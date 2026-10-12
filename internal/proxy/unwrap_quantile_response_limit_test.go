package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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
// reads every raw row of the range into one buffered response and fails with
// 502 "manual metric response exceeds" once the rows outgrow
// -backend-max-buffered-response-bytes (6h of generator data at the default).
// The test scales that down: the rows of a few minutes exceed a small body
// limit, and the answer must still be Loki's per-window p95.
func TestUnwrapQuantileResponseLimit(t *testing.T) {
	if os.Getenv("LVP_OPEN_GAP_TESTS") == "" {
		t.Skip("semantics/unwrap-quantile-response-limit is open: VictoriaLogs' quantile is not Loki's (rank floor(phi*n), no interpolation, sampled above 10k values); set LVP_OPEN_GAP_TESTS=1 to reproduce")
	}
	base := time.Unix(1700000400, 0).UTC() // minute-aligned
	const minutes, perMinute = 5, 40
	padding := strings.Repeat("x", 200)
	want := map[int64]float64{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			http.Error(w, "unexpected endpoint "+r.URL.Path, http.StatusNotFound)
			return
		}
		for m := 0; m < minutes; m++ {
			for i := 0; i < perMinute; i++ {
				ts := base.Add(time.Duration(m)*time.Minute + time.Duration(i+1)*time.Second)
				value := (i*37)%perMinute + 1 + m
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"_time":        ts.Format(time.RFC3339Nano),
					"_stream":      `{service_name="api-gateway",level="info"}`,
					"service_name": "api-gateway",
					"_msg":         fmt.Sprintf(`{"duration_ms":%d,"pad":%q}`, value, padding),
					"duration_ms":  strconv.Itoa(value),
					"pad":          padding,
				})
			}
		}
	}))
	defer backend.Close()

	// Window (t-1m, t] at t = base+(m+1)m holds the values 1+m .. 40+m once
	// each: Loki's p95 is rank 0.95*39 = 37.05 between 38+m and 39+m.
	rank := 0.95 * (perMinute - 1)
	weight := rank - math.Floor(rank)
	for m := 0; m < minutes; m++ {
		want[base.Add(time.Duration(m+1)*time.Minute).Unix()] = float64(38+m)*(1-weight) + float64(39+m)*weight
	}

	p := newGapTestProxy(t, backend.URL)
	p.execLimits.BufferedBackendBodyBytes = 16 << 10 // the rows hold ~60 KiB
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
		// Tolerate the last bit: arm64 may fuse the interpolation's multiply-add.
		if w, ok := want[ts]; !ok || math.Abs(value-w) > 1e-9 {
			t.Errorf("t=%d: want %v, got %v", ts, want[ts], value)
		}
	}
}
