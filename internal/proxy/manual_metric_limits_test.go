package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestGroupedQuantileRawRowLimit(t *testing.T) {
	for _, rows := range []int{2, 3} {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(fmt.Sprintf("%s/rows=%d", endpoint, rows), func(t *testing.T) {
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/select/logsql/query" {
						http.NotFound(w, r)
						return
					}
					if got := r.FormValue("limit"); got != "" {
						t.Errorf("limit argument would force backend sorting: %s", got)
					}
					if got := r.FormValue("query"); !strings.HasSuffix(got, " | limit 3") {
						t.Errorf("expected streaming overflow probe limit 3, got %s", got)
					}
					for i := 0; i < rows; i++ {
						fmt.Fprintf(w, "{\"_time\":\"2026-09-14T12:00:00Z\",\"_msg\":\"sample\",\"latency\":%d,\"_stream\":\"{app=\\\"api\\\"}\"}\n", i+1)
					}
				}))
				defer backend.Close()
				p := newGapTestProxy(t, backend.URL)
				p.rangeMetricRowLimit = 2
				params := url.Values{
					"query": {`quantile_over_time(0.5, {app="api"} | unwrap latency [5m]) by ()`},
					"time":  {"2026-09-14T12:01:00Z"}, "start": {"2026-09-14T12:01:00Z"}, "end": {"2026-09-14T12:01:00Z"}, "step": {"60"},
				}
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+endpoint+"?"+params.Encode(), nil)
				if endpoint == "query" {
					p.handleQuery(rec, req)
				} else {
					p.handleQueryRange(rec, req)
				}
				if rows == 2 {
					if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"1.5"`) {
						t.Fatalf("complete exact-cap input rejected: %d %s", rec.Code, rec.Body.String())
					}
				} else if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "row limit exceeded") || strings.Contains(rec.Body.String(), `"result"`) {
					t.Fatalf("truncation must return an error without partial samples: %d %s", rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestManualMetricEvaluationCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stamp := time.Now()
	series := map[string]manualSeriesSamples{"x": {Metric: map[string]string{"app": "api"}, Samples: []rangeMetricSample{{ts: stamp.UnixNano(), value: 1}}}}
	result, err := buildManualRangeMetricMatrixContext(ctx, "quantile", 0.5, series, stamp, stamp.Add(time.Hour), time.Second, time.Minute, 500)
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("matrix must return cancellation without partial data, got %s %v", result, err)
	}
	result, err = buildManualRangeMetricVectorContext(ctx, "quantile", 0.5, series, stamp, time.Minute)
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("vector must return cancellation without partial data, got %s %v", result, err)
	}
}

func TestManualMetricRowLimitCountsSkippedRows(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Neither row is a usable sample, but both consumed the backend row limit.
		fmt.Fprint(w, "{\"latency\":\"not numeric\"}\n{\"latency\":\"not numeric\"}\n")
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	p.rangeMetricRowLimit = 1
	stamp := time.Now()
	series, err := p.collectRangeMetricSamples(context.Background(), "*", nil, nil, true, "latency", "", stamp.Add(-time.Minute), stamp)
	if err == nil || !strings.Contains(err.Error(), "row limit exceeded") || series != nil {
		t.Fatalf("skipped rows must not hide truncation: series=%v err=%v", series, err)
	}
}

func TestManualMetricStreamingLimitSortsCompleteInputLocally(t *testing.T) {
	stamp := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.FormValue("limit") != "" || r.FormValue("query") != `* | limit 4` {
			t.Errorf("expected streaming cap without backend timestamp sort: %v", r.Form)
		}
		for _, seconds := range []int{3, 1, 2} {
			fmt.Fprintf(w, "{\"_time\":%q,\"latency\":%d,\"_stream\":\"{app=\\\"api\\\"}\"}\n", stamp.Add(time.Duration(seconds)*time.Second).Format(time.RFC3339Nano), seconds)
		}
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	p.rangeMetricRowLimit = 3
	series, err := p.collectRangeMetricSamples(t.Context(), "*", nil, nil, true, "latency", "", stamp, stamp.Add(time.Minute))
	if err != nil || len(series) != 1 {
		t.Fatalf("complete unordered input rejected: %v %v", series, err)
	}
	for _, entry := range series {
		if len(entry.Samples) != 3 {
			t.Fatalf("lost raw samples: %v", entry.Samples)
		}
		for i, sample := range entry.Samples {
			if sample.ts != stamp.Add(time.Duration(i+1)*time.Second).UnixNano() || sample.value != float64(i+1) {
				t.Fatalf("samples must be sorted locally with values preserved: %v", entry.Samples)
			}
		}
	}
}
