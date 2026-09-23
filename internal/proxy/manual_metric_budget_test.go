package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRawManualMetricLimitsFailClosedAndRecover(t *testing.T) {
	for _, bare := range []bool{false, true} {
		for _, capKind := range []string{"rows", "series"} {
			t.Run(fmt.Sprintf("bare=%v/%s", bare, capKind), func(t *testing.T) {
				rows := 3
				stamp := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.FormValue("limit") != "" || strings.Contains(r.FormValue("query"), "| sort") || !strings.Contains(r.FormValue("query"), " | limit ") {
						t.Errorf("raw exact metric must stream bounded input: %v", r.Form)
					}
					for i := rows; i > 0; i-- {
						row := map[string]string{"_time": stamp.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano), "_stream": fmt.Sprintf(`{app="api",pod="%d"}`, i), "latency": "2", "_msg": "sample"}
						encoded, _ := json.Marshal(row)
						fmt.Fprintln(w, string(encoded))
					}
				}))
				defer backend.Close()
				p := newGapTestProxy(t, backend.URL)
				p.rangeMetricRowLimit, p.maxStatsQuerySeries = 10, 10
				if capKind == "rows" {
					p.rangeMetricRowLimit = 2
				} else {
					p.maxStatsQuerySeries = 2
				}
				fetch := func() (int, error) {
					if bare {
						spec := bareParserMetricCompatSpec{baseQuery: `{app="api"}|json`, unwrapField: "latency", funcName: "avg_over_time", rangeWindow: time.Minute}
						result, err := p.fetchBareParserMetricSeries(t.Context(), "", spec, stamp.Format(time.RFC3339Nano), stamp.Add(time.Minute).Format(time.RFC3339Nano))
						return len(result), err
					}
					result, err := p.collectRangeMetricSamples(t.Context(), "*", nil, nil, false, "latency", "", stamp, stamp.Add(time.Minute))
					return len(result), err
				}
				if count, err := fetch(); err == nil || count != 0 {
					t.Fatalf("over-budget input returned partial metrics: count=%d err=%v", count, err)
				}
				rows = 2
				if count, err := fetch(); err != nil || count != 2 {
					t.Fatalf("subsequent exact-cap query failed: count=%d err=%v", count, err)
				}
			})
		}
	}
}

func TestBoundedBareParserMetricOutput(t *testing.T) {
	stamp := int64(time.Second)
	series := []bareParserMetricSeries{{metric: map[string]string{"app": "api"}, samples: []bareParserMetricSample{{tsNanos: stamp, value: 2}, {tsNanos: stamp * 2, value: 4}}}}
	spec := bareParserMetricCompatSpec{funcName: "avg_over_time", rangeWindow: time.Minute}
	for _, matrix := range []bool{false, true} {
		start := stamp
		if !matrix {
			start = stamp * 2
		}
		body, err := buildBoundedBareParserMetric(t.Context(), series, start, stamp*2, stamp, spec, matrix)
		if err != nil {
			t.Fatal(err)
		}
		old := buildBareParserMetricVector(series, stamp*2, spec)
		if matrix {
			old = buildBareParserMetricMatrix(series, start, stamp*2, stamp, spec)
		}
		oldBytes, _ := json.Marshal(old)
		var got, want any
		_ = json.Unmarshal(body, &got)
		_ = json.Unmarshal(oldBytes, &want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("successful response changed: got=%s want=%s", body, oldBytes)
		}
	}
	for _, budget := range []string{"samples", "bytes"} {
		ctx := binaryEvaluationContext(t.Context())
		state := ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState)
		if budget == "samples" {
			state.budget.outputSamples = 1
		} else {
			state.budget.bytes = 16
		}
		if body, err := buildBoundedBareParserMetric(ctx, series, stamp, stamp*2, stamp, spec, true); err == nil || body != nil {
			t.Fatalf("%s budget returned partial response: %s %v", budget, body, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if body, err := buildBoundedBareParserMetric(ctx, series, stamp, stamp*2, stamp, spec, true); err == nil || body != nil {
		t.Fatalf("cancelled response returned output: %s %v", body, err)
	}
	if err := checkManualMetricRead(t.Context(), &io.LimitedReader{N: 0}); err == nil {
		t.Fatal("exhausted input byte budget accepted")
	}
}

// Loki's unwrap duration()/bytes() accept unit strings; the raw parser path
// must convert them instead of dropping the sample (regression: every
// duration()/bytes() unwrap over a bare parser returned no series while Loki
// returned the converted values).
func TestBareParserRawSampleWeightAppliesUnwrapConversion(t *testing.T) {
	cases := []struct {
		name  string
		conv  string
		value interface{}
		want  float64
		ok    bool
	}{
		{"duration ms", "duration", "15ms", 0.015, true},
		{"duration compound", "duration", "2m30s", 150, true},
		{"bytes unit", "bytes", "1024B", 1024, true},
		{"bytes kib", "bytes", "1.5KiB", 1536, true},
		{"plain float", "", "12.5", 12.5, true},
		{"plain float rejects unit", "", "15ms", 0, false},
		{"duration rejects garbage", "duration", "soon", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := bareParserMetricCompatSpec{funcName: "sum_over_time", unwrapField: "v", unwrapConv: tc.conv}
			got, ok := bareParserRawSampleWeight(map[string]interface{}{"v": tc.value}, spec)
			if ok != tc.ok || (ok && math.Abs(got-tc.want) > 1e-9) {
				t.Fatalf("weight(%v, %q) = %v, %v; want %v, %v", tc.value, tc.conv, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestAddSeriesLimitWarning(t *testing.T) {
	const want500 = `{"warnings":["maximum number of series (500) reached for a single query; returning partial results"]`
	truncated := &seriesLimitScope{drilldown: true}
	truncated.truncated.Store(500)

	cases := []struct {
		name  string
		body  string
		scope *seriesLimitScope
		want  string
	}{
		{"object with fields", `{"status":"success","data":{}}`, truncated, want500 + `,"status":"success","data":{}}`},
		{"empty object", `{}`, truncated, want500 + `}`},
		{"not truncated", `{"status":"success"}`, &seriesLimitScope{drilldown: true}, `{"status":"success"}`},
		{"no scope", `{"status":"success"}`, nil, `{"status":"success"}`},
		{"already warned", `{"warnings":["x"],"status":"success"}`, truncated, `{"warnings":["x"],"status":"success"}`},
		{"not an object", `[1,2]`, truncated, `[1,2]`},
		{"empty body", ``, truncated, ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(addSeriesLimitWarning([]byte(tc.body), tc.scope)); got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}
