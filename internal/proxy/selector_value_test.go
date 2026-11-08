package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// A stream-selector matcher value that is not a string is Loki's 400 parse
// error on query_range and query, written as Loki writes it (text/plain, the
// message alone), before any VictoriaLogs call. The proxy read ip("...") as a
// string value and answered 200 with no streams.
// conformance: semantics/selector-values-are-strings, status-400, loki_api_v1_query_range, loki_api_v1_query
func TestSelectorValueNotString_LokiParseError(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stream_field_names" {
			calls.Add(1)
		}
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, tc := range []struct{ query, want string }{
		{`{a=ip("1.2.3.4")}`, "parse error at line 1, col 4: syntax error: unexpected ip, expecting STRING"},
		{`{a="1", addr!=ip("1.1.1.1")}`, "parse error at line 1, col 15: syntax error: unexpected ip, expecting STRING"},
		{`{a=foo}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=1}`, "parse error at line 1, col 4: syntax error: unexpected NUMBER, expecting STRING"},
		{`count_over_time({a="1", addr!=ip("1.1.1.1")}[5m])`, "parse error at line 1, col 31: syntax error: unexpected ip, expecting STRING"},
	} {
		for _, route := range []string{"query_range", "query"} {
			before := calls.Load()
			params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}}
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+route+"?"+params.Encode(), nil)
			if route == "query" {
				p.handleQuery(w, r)
			} else {
				p.handleQueryRange(w, r)
			}
			if w.Code != http.StatusBadRequest || w.Body.String() != tc.want {
				t.Errorf("%s %s: %d %q, want 400 %q", route, tc.query, w.Code, w.Body.String(), tc.want)
			}
			if ct := w.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
				t.Errorf("%s %s: Content-Type %q", route, tc.query, ct)
			}
			if n := calls.Load() - before; n != 0 {
				t.Errorf("%s %s: %d VictoriaLogs calls, want none", route, tc.query, n)
			}
		}
	}
}
