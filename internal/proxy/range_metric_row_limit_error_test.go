package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const rowLimitErrorText = "limit reached while evaluating the query"

// rowLimitBackend answers every VictoriaLogs call but the raw row fetch with
// an error carrying text no client may read, which keeps the metric on the
// raw-row evaluator, and serves rows lines to the fetch. A rows count of 0
// streams rows until the proxy hangs up.
func rowLimitBackend(t *testing.T, rows int, hungUp chan<- struct{}) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			http.Error(w, "vl internal: select/logsql stats failed on 10.1.2.3:9428", http.StatusInternalServerError)
			return
		}
		line := func(i int) string {
			return fmt.Sprintf(`{"_time":"2023-11-14T22:15:%02d.000000000Z","_msg":"a=b","_stream":"{env=\"production\"}"}`+"\n", i%60)
		}
		flusher, _ := w.(http.Flusher)
		for i := 0; rows == 0 || i < rows; i++ {
			if _, err := fmt.Fprint(w, line(i)); err != nil || r.Context().Err() != nil {
				break
			}
			if rows == 0 && flusher != nil {
				flusher.Flush()
			}
		}
		if rows == 0 {
			<-r.Context().Done()
			close(hungUp)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func rowLimitRequest(p *Proxy, endpoint, query string) *httptest.ResponseRecorder {
	at := time.Unix(1700000400, 0).UTC()
	params := url.Values{
		"query": {query},
		"start": {at.Format(time.RFC3339)},
		"end":   {at.Add(6 * time.Minute).Format(time.RFC3339)},
		"time":  {at.Add(6 * time.Minute).Format(time.RFC3339)},
		"step":  {"60"},
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+endpoint+"?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "team-a")
	rec := httptest.NewRecorder()
	if endpoint == "query" {
		p.handleQuery(rec, req)
	} else {
		p.handleQueryRange(rec, req)
	}
	return rec
}

// A metric read by the raw-row evaluator past -manual-range-metric-row-limit
// answers the limit error on every route, range and instant, for each of the
// two evaluators (ordered JSON and the bare parser): status 400 and errorType
// bad_data as Loki answers a query over one of its limits (logqlmodel.ErrLimit,
// pkg/util/server/error.go:110-118), and the text names the flag. The
// client reads nothing of VictoriaLogs or Go.
// conformance: heavy-metric-fetch-bounds, limits/row-limit-answers-loki-limit-text
func TestRangeMetricRowLimitErrorIsLokiShaped(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		// The Logs Drilldown level breakdown over `| json`, read by the ordered JSON evaluator.
		{"ordered json", `sum by (detected_level) (count_over_time({env="production"} | json | __error__!="" [5m]))`},
		// A bare logfmt metric, read by the bare-parser evaluator.
		{"bare parser", `count_over_time({env="production"} | logfmt [5m])`},
		// rate_counter always reads raw rows through the manual range metric evaluator.
		{"manual range metric", `sum(rate_counter({env="production"} | logfmt | unwrap n [5m]))`},
	} {
		for _, endpoint := range []string{"query_range", "query"} {
			t.Run(tc.name+"/"+endpoint, func(t *testing.T) {
				srv := rowLimitBackend(t, 50, nil)
				p := newFilterPushdownProxy(t, srv.URL)
				p.rangeMetricRowLimit = 3
				rec := rowLimitRequest(p, endpoint, tc.query)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status %d, want 400: %s", rec.Code, rec.Body.String())
				}
				// Loki's WriteError: text/plain, nosniff and the message alone.
				if body := readErrorRec(t, rec); rec.Code != http.StatusBadRequest || body.Error != rowLimitErrorText {
					t.Fatalf("body %q, want %q alone", rec.Body.String(), rowLimitErrorText)
				}
				// Nothing of VictoriaLogs, LogsQL or Go in what the client reads.
				host := strings.TrimPrefix(srv.URL, "http://")
				for _, leak := range []string{host, "10.1.2.3", "select/logsql", "vl internal", "_stream", "limit 4", "|", "context", "EOF", "Internal Server Error", "&{", "*"} {
					if strings.Contains(rec.Body.String(), leak) {
						t.Errorf("body leaks %q: %s", leak, rec.Body.String())
					}
				}
			})
		}
	}
}

// Exactly -manual-range-metric-row-limit rows is within the limit.
// conformance: heavy-metric-fetch-bounds, limits/row-limit-answers-loki-limit-text
func TestRangeMetricRowLimitAdmitsRowsAtTheLimit(t *testing.T) {
	srv := rowLimitBackend(t, 3, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	p.rangeMetricRowLimit = 3
	if rec := rowLimitRequest(p, "query_range", `count_over_time({env="production"} | logfmt [5m])`); rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 at the limit: %s", rec.Code, rec.Body.String())
	}
}

// The raw fetch is cut off as the limit is hit: the proxy hangs up on a
// VictoriaLogs that keeps sending rows, so no further row is read or buffered.
// conformance: heavy-metric-fetch-bounds, limits/row-limit-answers-loki-limit-text
func TestRangeMetricRowLimitStopsReadingUpstream(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"ordered json", `sum by (detected_level) (count_over_time({env="production"} | json | __error__!="" [5m]))`},
		{"bare parser", `count_over_time({env="production"} | logfmt [5m])`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hungUp := make(chan struct{})
			srv := rowLimitBackend(t, 0, hungUp)
			p := newFilterPushdownProxy(t, srv.URL)
			p.rangeMetricRowLimit = 3
			rec := rowLimitRequest(p, "query_range", tc.query)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "-manual-range-metric-row-limit") {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			select {
			case <-hungUp:
			case <-time.After(5 * time.Second):
				t.Fatal("VictoriaLogs was still being read after the row limit was hit")
			}
		})
	}
}
