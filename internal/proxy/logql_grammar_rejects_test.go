package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// logqlGrammarRejects are queries Loki's parser rejects that the proxy once read and sent to VictoriaLogs: a blank
// Loki's lexer does not skip between two tokens, a stage after an unwrap, an unwrap in a log query and a range
// Loki does not read as a duration. Loki's texts are recorded with the Loki oracle check command
// (internal/logql testdata whitespace_loki.jsonl.gz and range-unwrap-grammar-loki.jsonl).
var logqlGrammarRejects = []struct{ query, loki string }{
	{"sum\v(rate({a=\"b\"}[5m])) by (a)", `parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting ( or by or without`},
	{"sum by\u00a0(a) (rate({a=\"b\"}[5m]))", `parse error at line 1, col 7: syntax error: unexpected IDENTIFIER, expecting (`},
	{"topk(2, sum by (a)\u2003(rate({a=\"b\"}[5m])))", `parse error at line 1, col 19: syntax error: unexpected IDENTIFIER, expecting (`},
	{"topk(2,\u0085sum\u0085by\u0085(a)\u0085(rate({a=\"b\"}[5m])))", `parse error at line 1, col 8: syntax error: unexpected IDENTIFIER`},
	{"{a=\"b\"}\f|\fline_format\f\"{{.a}}\"\f|\flabel_format\fx=y\f|\fdrop\fz", `parse error at line 1, col 8: syntax error: unexpected IDENTIFIER`},
	{"{a=\"b\"} | line_format\u00a0\"{{.a}}\" | label_format x=y | drop z", `parse error at line 1, col 22: syntax error: unexpected IDENTIFIER, expecting STRING`},
	{`quantile_over_time(.5, {a="b"} | json | unwrap c | drop __error__[5m])`, `parse error at line 1, col 52: syntax error: unexpected drop, expecting IDENTIFIER or (`},
	{`sum_over_time({a="b"} | unwrap c |= "x" [5m])`, `parse error at line 0, col 34: syntax error: unexpected |=, expecting RANGE or |`},
	{`{a="b"} | unwrap a | label_format c=d != ip("10.0.0.0/8")`, `parse error at line 1, col 11: syntax error: unexpected unwrap`},
	{`{a="b"} | json | unwrap a`, `parse error at line 1, col 18: syntax error: unexpected unwrap`},
	{`quantile_over_time(.5, {a="b"} | json | unwrap c[1µs])`, `parse error at line 0, col 49: unknown unit "µs" in duration "1µs"`},
	{`quantile_over_time(.5, {a="b"} | json | unwrap c[5m5m])`, `parse error at line 0, col 49: not a valid duration string: "5m5m"`},
	{`rate({a="b"}[1us])`, `parse error at line 0, col 13: unknown unit "us" in duration "1us"`},
	{`rate({a="b"}[1.5m])`, `parse error at line 0, col 13: unknown unit "." in duration "1.5m"`},
	{`rate({a="b"}[9999999999h])`, `parse error at line 0, col 13: duration out of range`},
	{`rate({a="b"}[5m]) / on rate({a="b"}[5m])`, `parse error at line 1, col 24: syntax error: unexpected RATE, expecting (`},
}

// A query Loki's parser rejects is answered with Loki's 400 and text on query_range and query before any request to
// VictoriaLogs, whatever the cache holds; its neighbours Loki accepts still reach VictoriaLogs.
//
// conformance: semantics/parser-reads-unicode-blank-as-white-space, semantics/stage-after-unwrap-accepted, semantics/unwrap-in-log-query, semantics/range-duration-grammar-wider-than-loki, semantics/binary-on-without-label-list-accepted
func TestLogQLGrammarRejectsBeforeVictoriaLogs(t *testing.T) {
	var backendCalls atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer vl.Close()
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(0, 0), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	get := func(path, query string) *httptest.ResponseRecorder {
		params := url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}}
		if path == "/loki/api/v1/query" {
			params = url.Values{"query": {query}, "time": {"1700000600"}}
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil))
		return w
	}
	for _, tc := range logqlGrammarRejects {
		paths := []string{"/loki/api/v1/query_range"}
		if !strings.HasPrefix(tc.query, "{") {
			paths = append(paths, "/loki/api/v1/query") // a log query is another error on the instant endpoint
		}
		for _, path := range paths {
			backendCalls.Store(0)
			w := get(path, tc.query)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %q: status %d, want 400: %s", path, tc.query, w.Code, w.Body.String())
			}
			if got := readErrorRec(t, w).Error; got != tc.loki {
				t.Errorf("%s %q:\n proxy %q\n Loki  %q", path, tc.query, got, tc.loki)
			}
			if n := backendCalls.Load(); n != 0 {
				t.Errorf("%s %q: %d VictoriaLogs calls before Loki's 400", path, tc.query, n)
			}
		}
	}
	for _, q := range []string{
		"sum by (a)\t(rate({a=\"b\"}[5m]))", "sum by (a)\n(rate({a=\"b\"}[5m]))", "sum by (a)\r\n(rate({a=\"b\"}[5m]))",
		`sum_over_time({a="b"} | unwrap c | __error__="" [5m])`, `sum_over_time({a="b"} | unwrap c | x > -1 [5m])`,
		`rate({a="b"}[1ms])`, `rate({a="b"}[5m30s])`, `rate({a="b"}[5m] offset 1us)`, `{a="b"} | json | unwrap [1s]`,
	} {
		backendCalls.Store(0)
		if w := get("/loki/api/v1/query_range", q); w.Code != http.StatusOK || backendCalls.Load() == 0 {
			t.Errorf("%q: status %d and %d VictoriaLogs calls, want 200 from VictoriaLogs: %s", q, w.Code, backendCalls.Load(), w.Body.String())
		}
	}
}
