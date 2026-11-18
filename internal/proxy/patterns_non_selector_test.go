package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// Loki reads the /patterns query as a stream selector alone: a line filter, a parser or a metric expression is a
// 400 `only label matchers are supported`, before any pattern is mined. The route answers it the same way, with no
// VictoriaLogs call. Loki 3.7.9 answered the first query below as listed in the live differential of 2026-11-14
// (build 726dfb48f, VictoriaLogs v1.54.0): HTTP 400, text/plain, that text.
//
// conformance: drilldown/patterns-non-selector-query, loki_api_v1_patterns
func TestPatternsRouteRejectsAQueryThatIsNotASelector(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, query := range []string{
		`{env="production"} |= "error"`,
		`{env="production"} | logfmt`,
		`{env="production"} | json | level="error"`,
		`count_over_time({env="production"}[5m])`,
	} {
		params := url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700003600"}}
		w := doCompatProxyRequest(p, "/loki/api/v1/patterns?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if body := readErrorRec(t, w); w.Code != http.StatusBadRequest || body.Error != "only label matchers are supported" {
			t.Errorf("%s: status=%d body=%s, want 400 %q", query, w.Code, w.Body, "only label matchers are supported")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected patterns requests reached the backend %d times", calls.Load())
	}
}
