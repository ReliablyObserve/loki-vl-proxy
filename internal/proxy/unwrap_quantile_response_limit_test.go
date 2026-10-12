package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: semantics/unwrap-quantile-response-limit
//
// Loki 3.7.7 answers quantile_over_time over an unwrapped JSON field whatever
// the volume of lines the range holds: it keeps the samples of one window per
// series and interpolates between adjacent ranked samples
// (pkg/logql/range_vector.go:449-487 quantileOverTime, Quantile). The proxy
// used to read every raw row of the range into one buffered response and failed
// with 502 "manual metric response exceeds" once the rows outgrew
// -backend-max-buffered-response-bytes (6h of generator data at the default).
// The test scales that down: the rows of a few minutes are several times a
// small body limit, and so would be counts per stream (the generator's pods
// rotate, so a stream holds a couple of lines), while the counts of the
// range aggregation's group fit it; the answer is Loki's per-window p95
// without a raw row read.
func TestUnwrapQuantileResponseLimit(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC() // minute-aligned
	const minutes, perMinute = 5, 40
	want := map[int64]float64{}
	emu := &vlEmulator{t: t, field: "duration_ms"}
	padding := strings.Repeat("x", 200)
	for m := 0; m < minutes; m++ {
		for i := 0; i < perMinute; i++ {
			value := (i*37)%perMinute + 1 + m
			emu.rows = append(emu.rows, emuRow{
				ts: base.Add(time.Duration(m)*time.Minute + time.Duration(i+1)*time.Second).UnixNano(),
				// The log generator's pods rotate every batch: one stream per two lines.
				stream: fmt.Sprintf(`{service_name="api-gateway",pod="api-gateway-%d-%d",level="info"}`, m, i/2),
				value:  float64(value),
				msg:    fmt.Sprintf(`{"duration_ms":%d,"pad":%q}`, value, padding),
			})
		}
	}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()

	// Window (t-1m, t] at t = base+(m+1)m holds the values 1+m .. 40+m once
	// each: Loki's p95 is rank 0.95*39 = 37.05 between 38+m and 39+m.
	for m := 0; m < minutes; m++ {
		want[base.Add(time.Duration(m+1)*time.Minute).Unix()] = 38.05 + float64(m)
	}

	p := newGapTestProxy(t, backend.URL)
	p.execLimits.BufferedBackendBodyBytes = 16 << 10 // the raw rows hold ~60 KiB
	params := url.Values{
		"query": {`quantile_over_time(0.95, {service_name="api-gateway"} | json | unwrap duration_ms [1m]) by (service_name)`},
		"start": {base.Add(time.Minute).Format(time.RFC3339Nano)},
		"end":   {base.Add(time.Duration(minutes) * time.Minute).Format(time.RFC3339Nano)},
		"step":  {"60"},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("Loki answers this range; proxy status %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(response.Data.Result) != 1 || response.Data.Result[0].Metric["service_name"] != "api-gateway" {
		t.Fatalf("want one api-gateway series, got %s", rec.Body.String())
	}
	got := response.Data.Result[0].Values
	if len(got) != len(want) {
		t.Fatalf("want %d points, got %d: %v", len(want), len(got), got)
	}
	for _, point := range got {
		ts := int64(point[0].(float64))
		value, _ := strconv.ParseFloat(point[1].(string), 64)
		// The last bit may differ: a platform may fuse the interpolation's multiply-add.
		if w, ok := want[ts]; !ok || math.Abs(value-w) > 1e-9 {
			t.Errorf("t=%d: want %v, got %v", ts, want[ts], value)
		}
	}
	if q := emu.endpointCalls("/select/logsql/query"); len(q) != 0 {
		t.Fatalf("quantile read raw rows: %q", q)
	}
}
