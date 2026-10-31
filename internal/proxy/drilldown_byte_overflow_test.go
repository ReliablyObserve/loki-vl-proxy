package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// A byte bound can stop the initial read before its series counter sees the
// tenant's limit+1st series. Drilldown may retry the same bounded ranking as a
// series overflow; other clients retain the resource error.
// conformance: limits/drilldown-partial-with-warning, limits/per-tenant-override, operator-configurable-limits
func TestDrilldownBreakdown_ByteOverflowRanking(t *testing.T) {
	const small = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000400,"9"]]},{"metric":{"pod":"b"},"values":[[1700000400,"7"]]},{"metric":{"pod":"c"},"values":[[1700000400,"1"]]}]}}`
	const exact = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000400,"9"]]}]}}`
	oversized := strings.Repeat(" ", boundsTestCap+1) + small
	for _, tc := range []struct {
		name                           string
		drilldown, firstFits, rankFits bool
		wantStatus, wantCalls, series  int
		warning                        bool
	}{
		{"drilldown ranks after byte overflow", true, false, true, 200, 2, 2, true},
		{"byte retry preserves under-limit exact result", true, false, true, 200, 2, 1, false},
		{"plain client retains byte error", false, false, true, 500, 1, 0, false},
		{"under limit stays exact", true, true, true, 200, 1, 1, false},
		{"oversized ranking stays bounded", true, false, false, 500, 2, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var calls []string
			vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/select/logsql/stats_query_range" {
					t.Errorf("unexpected backend path %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				query := r.FormValue("query")
				mu.Lock()
				calls = append(calls, query)
				mu.Unlock()
				switch {
				case tc.firstFits:
					fmt.Fprint(w, exact)
				case tc.rankFits && strings.Contains(query, "__lvp_rank"):
					if tc.series == 1 {
						fmt.Fprint(w, exact)
					} else {
						fmt.Fprint(w, small)
					}
				default:
					fmt.Fprint(w, oversized)
				}
			}))
			t.Cleanup(vl.Close)
			_, mux := newTenantLimitsProxy(t, vl.URL, Config{
				MaxStatsQuerySeries: 9,
				TenantLimits:        map[string]map[string]any{"tenant-a": {"max_query_series": 2.0}},
				ExecutionLimits:     ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: boundsTestCap},
			})
			start := time.Unix(1700000400, 0)
			params := tumblingRangeParams(`sum by (pod) (count_over_time({app="tumble"}[5m]))`, start, start.Add(time.Hour), 5*time.Minute)
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "tenant-a")
			if tc.drilldown {
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d, want %d: %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus == 200 {
				if got := lokiResultSeriesCount(rec.Body.Bytes()); got != tc.series {
					t.Errorf("series=%d, want %d: %s", got, tc.series, rec.Body)
				}
				if got := strings.Contains(rec.Body.String(), `"warnings"`); got != tc.warning {
					t.Errorf("warning=%v, want %v: %s", got, tc.warning, rec.Body)
				}
				if tc.warning && !strings.Contains(rec.Body.String(), "maximum number of series (2) reached for a single query; returning partial results") {
					t.Errorf("missing tenant-specific Loki warning: %s", rec.Body)
				}
				if !strings.Contains(rec.Body.String(), `"pod":"a"`) || !strings.Contains(rec.Body.String(), `"9"`) {
					t.Errorf("exact busiest sample missing: %s", rec.Body)
				}
			} else if !strings.Contains(rec.Body.String(), fmt.Sprintf("manual metric response exceeds %d bytes", boundsTestCap)) {
				t.Errorf("missing unchanged byte limit error: %s", rec.Body)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(calls) != tc.wantCalls {
				t.Fatalf("backend calls=%d, want %d: %v", len(calls), tc.wantCalls, calls)
			}
			if tc.wantCalls == 2 && (!strings.Contains(calls[1], "| limit 3 |") || strings.Contains(calls[0], "__lvp_rank")) {
				t.Errorf("ranking must retry once with tenant limit+1: %v", calls)
			}
		})
	}
}

// Cancellation while waiting for the bounded stats slot must not send either
// the ordinary query or its ranked retry to VictoriaLogs.
func TestDrilldownBreakdown_CanceledBeforeSlot(t *testing.T) {
	p := newGuardTestProxy(t, Config{BackendURL: "http://127.0.0.1:1"})
	p.statsQueryRangeSem = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/?query=x", nil).WithContext(ctx)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	rec := httptest.NewRecorder()
	p.proxyStatsQueryRangeDirectAnchored(rec, req, `app:="x" | stats by (pod) count()`, 0)
	if rec.Code != 499 || !strings.Contains(rec.Body.String(), `"errorType":"canceled"`) {
		t.Fatalf("expected cancellation, got %d: %s", rec.Code, rec.Body)
	}
}

type cancelStatsReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (r cancelStatsReader) Read(dst []byte) (int, error) {
	n, err := r.Reader.Read(dst)
	r.cancel()
	return n, err
}

func TestDrilldownBreakdown_CanceledOverflowDoesNotRetry(t *testing.T) {
	p := newGuardTestProxy(t, Config{BackendURL: "http://stats.invalid", ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: boundsTestCap}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	p.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(cancelStatsReader{
			Reader: strings.NewReader(strings.Repeat(" ", boundsTestCap+1)), cancel: cancel,
		})}, nil
	})
	req := httptest.NewRequest(http.MethodGet, "/?query=x", nil).WithContext(ctx)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	rec := httptest.NewRecorder()
	p.proxyStatsQueryRangeDirectAnchored(rec, req, `app:="x" | stats by (pod) count()`, 0)
	if rec.Code != 499 || !strings.Contains(rec.Body.String(), `"errorType":"canceled"`) || calls != 1 {
		t.Fatalf("expected one canceled read, got %d calls, status %d: %s", calls, rec.Code, rec.Body)
	}
}

func TestBuildVLInFilter_QuotedValues(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{`plain"quote`, `pod:in("plain\"quote")`},
		{`back\slash`, `pod:in("back\\slash")`},
		{`end\`, `pod:in("end\\")`},
		{"line\n\ttab\r\x00", `pod:in("line\n\ttab\r\x00")`},
		{`x\") | delete pod`, `pod:in("x\\\") | delete pod")`},
	} {
		if got := buildVLInFilter("pod", []string{tc.value}); got != tc.want {
			t.Errorf("value %q: got %q, want %q", tc.value, got, tc.want)
		}
	}
}
