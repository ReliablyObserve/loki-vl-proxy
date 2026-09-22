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

// The window is Loki's (t-range, t]: a sample exactly on the lower edge is
// out, one exactly on the evaluation timestamp is in. Samples sit on both
// edges so an inclusive lower bound or an exclusive upper bound changes the
// answer.
func TestBoundedBareParserMetricOutput(t *testing.T) {
	stamp := int64(time.Second)
	series := []bareParserMetricSeries{{metric: map[string]string{"app": "api"}, samples: []bareParserMetricSample{{tsNanos: stamp, value: 2}, {tsNanos: stamp * 2, value: 4}}}}
	spec := bareParserMetricCompatSpec{funcName: "avg_over_time", rangeWindow: time.Second}
	for _, tc := range []struct {
		matrix bool
		start  int64
		want   string
	}{
		// eval 2s, window (1s, 2s]: the 1s sample is on the open edge.
		{matrix: false, start: stamp * 2, want: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api"},"value":[2,"4"]}]}}`},
		// eval 1s, window (0, 1s] holds 2; eval 2s, window (1s, 2s] holds 4.
		{matrix: true, start: stamp, want: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[1,"2"],[2,"4"]]}]}}`},
	} {
		body, err := buildBoundedBareParserMetric(t.Context(), series, tc.start, stamp*2, stamp, spec, tc.matrix)
		if err != nil {
			t.Fatal(err)
		}
		var got, want any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("response is not JSON: %v: %s", err, body)
		}
		_ = json.Unmarshal([]byte(tc.want), &want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("matrix=%v: got=%s want=%s", tc.matrix, body, tc.want)
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
