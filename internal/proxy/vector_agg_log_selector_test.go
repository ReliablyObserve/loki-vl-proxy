package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// A vector aggregation over a log selector is Loki's 400 parse error on
// query_range and query, written as Loki writes it (text/plain, the message
// alone), before any VictoriaLogs call. The proxy answered query_range with a
// 500 (VictoriaLogs rejected the query with no stats pipe) and query with its
// own "unsupported instant aggregation target".
// conformance: semantics/vector-aggregation-over-log-selector-status, status-400, loki_api_v1_query_range, loki_api_v1_query
func TestVectorAggregationOverLogSelector_LokiParseError(t *testing.T) {
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
		{`topk(2, {app="api-gateway",env="production"})`, "parse error at line 1, col 45: syntax error: unexpected )"},
		{`bottomk(1, {app="api-gateway",env="production"})`, "parse error at line 1, col 48: syntax error: unexpected )"},
		{`sort({app="api-gateway"})`, "parse error at line 1, col 25: syntax error: unexpected )"},
		{`sort_desc({app="api-gateway",env="production"})`, "parse error at line 1, col 47: syntax error: unexpected )"},
		{`stddev({namespace="prod"})`, "parse error at line 1, col 26: syntax error: unexpected )"},
		{`sum by (level) ({app="api-gateway"} | json)`, "parse error at line 1, col 43: syntax error: unexpected )"},
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
