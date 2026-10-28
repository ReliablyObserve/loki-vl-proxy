package proxy

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// queryStepsGetBehindMiddleware is queryStepsGet with the headers the
// middleware has set on the writer before the handler runs (the security
// headers: nosniff, no-store), as every request through the server has.
func queryStepsGetBehindMiddleware(t testing.TB, p *Proxy, query string, w extentWindow, late bool) (int, []byte, http.Header) {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(w.start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(w.end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(w.step.Seconds(), 'f', -1, 64)},
	}
	rec := httptest.NewRecorder()
	rec.Header().Set("X-Content-Type-Options", "nosniff")
	rec.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	rec.Header().Set("X-Frame-Options", "DENY")
	var out http.ResponseWriter = rec
	if late {
		out = &lateHeaderWriter{ResponseWriter: rec}
	}
	p.handleQueryRange(out, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	p.statsExtentStores.wait()
	return rec.Code, rec.Body.Bytes(), rec.Header()
}

// lateHeaderWriter adds, as the compression middleware does for a client that
// accepts gzip, the headers of its encoding when the answer goes out.
type lateHeaderWriter struct {
	http.ResponseWriter
	sent bool
}

func (l *lateHeaderWriter) addHeaders() {
	if !l.sent {
		l.sent = true
		l.Header().Set("Content-Encoding", "gzip")
		l.Header().Set("Vary", "Accept-Encoding")
		l.Header().Set("Content-Length", "1")
	}
}

func (l *lateHeaderWriter) WriteHeader(code int) {
	l.addHeaders()
	l.ResponseWriter.WriteHeader(code)
}

func (l *lateHeaderWriter) Write(b []byte) (int, error) {
	l.addHeaders()
	return l.ResponseWriter.Write(b)
}

// TestQueryRangeStepsBehindMiddleware: the evaluator of a missing range gives
// the headers the middleware set (nosniff among them) as the evaluation of
// the whole window does, so a moved window is answered from the steps it holds
// (partial, hit) and not evaluated whole (fallback), for every shape, with the
// answer and headers of the uncached proxy. A missing range evaluated with
// bare headers had no nosniff where the stored steps had it, and the merge
// refused the two (found by the fleet proof: the sum without fold, which
// writes no nosniff of its own, was evaluated whole on every refresh). A
// client that accepts gzip gets Content-Encoding, Vary and Content-Length from
// the writer below the handler when the answer goes out; those are not the
// evaluator's headers, and an answer carrying them was never kept (the same
// proof: a Go client, like Grafana, asks for gzip).
//
// conformance: quality/query-range-settled-steps, loki_api_v1_query_range
func TestQueryRangeStepsBehindMiddleware(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "security headers", true: "headers added when the answer goes out"}[late], func(t *testing.T) {
			s0 := time.Unix(1_700_001_000, 0).UTC()
			rows := queryStepsRows(s0, 4*time.Hour, nil)
			end := s0.Add(3 * time.Hour)
			onSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
			offSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
			on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
			off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
			windows := queryStepsWindows(end)[:4] // the first window and three refreshes
			for _, query := range queryStepsQueries {
				for _, w := range windows {
					wantCode, want, wantHeader := queryStepsGetBehindMiddleware(t, off, query, w, late)
					gotCode, got, gotHeader := queryStepsGetBehindMiddleware(t, on, query, w, late)
					if gotCode != wantCode || !sameQueryStepsAnswer(got, want) {
						t.Fatalf("%s [%s] step %s: the settled steps changed the answer\n with: %d %s\nwithout: %d %s", query, w.start.Format(time.RFC3339Nano), w.step, gotCode, got, wantCode, want)
					}
					for _, name := range []string{"Content-Type", "X-Content-Type-Options", "Cache-Control", "X-Frame-Options", "Warning"} {
						if gotHeader.Get(name) != wantHeader.Get(name) {
							t.Fatalf("%s: header %s %q, without the steps %q", query, name, gotHeader.Get(name), wantHeader.Get(name))
						}
					}
					if !bytes.Contains(got, []byte(`"status":"success"`)) && gotCode == http.StatusOK {
						t.Fatalf("%s: %s", query, got)
					}
				}
			}
			outcomes := queryStepsOutcomes(t, on)
			t.Logf("outcomes %v", outcomes)
			if outcomes["fallback"] != 0 {
				t.Fatalf("%d windows were evaluated whole although their steps were held: %v", outcomes["fallback"], outcomes)
			}
			if outcomes["partial"]+outcomes["hit"] < len(queryStepsQueries)*2 {
				t.Fatalf("the refreshes were not answered from settled steps: %v", outcomes)
			}
		})
	}
}
