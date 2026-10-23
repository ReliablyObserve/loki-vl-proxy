package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// Loki v3.7.7 has no count_values (pkg/logql/syntax/lex.go:114-125 has no token
// for it, syntax.y:477-490 and ast.go:1243-1254 no vector operation), so every form is a
// 400 parse error at the position of the name, before any storage work. The
// proxy must answer the same message on both endpoints without a backend call;
// it used to answer "count_values is not translatable to LogsQL" (or a bare
// "unexpected IDENT" without a position for the by-before-arguments form).
// Errors carry no labels, so both response encodings answer the same body.
// conformance: semantics/count-values-unsupported
func TestCountValues_RejectedLikeLoki(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	const inner = `sum by (level) (count_over_time({env="production"}[5m]))`
	const unexpected = ": syntax error: unexpected IDENTIFIER"
	cases := []struct{ query, want string }{
		{`count_values("v", ` + inner + `)`, "parse error at line 1, col 1" + unexpected},
		{`count_values("v", count_over_time({env="production"}[5m]))`, "parse error at line 1, col 1" + unexpected},
		{`count_values by (level) ("v", ` + inner + `)`, "parse error at line 1, col 1" + unexpected},
		{`count_values without (level) ("v", ` + inner + `)`, "parse error at line 1, col 1" + unexpected},
		{`count_values("v", ` + inner + `) by (level)`, "parse error at line 1, col 1" + unexpected},
		{`count_values("1 bad", ` + inner + `)`, "parse error at line 1, col 1" + unexpected},
		{`sum(count_values("v", ` + inner + `))`, "parse error at line 1, col 5" + unexpected},
		{`topk(1, count_values("v", ` + inner + `))`, "parse error at line 1, col 9" + unexpected},
		{inner + ` + count_values("v", ` + inner + `)`, "parse error at line 1, col 60" + unexpected},
		{"sum(\n  count_values(\"v\", " + inner + "))", "parse error at line 2, col 3" + unexpected},
	}
	for _, tc := range cases {
		for _, target := range []string{
			"/loki/api/v1/query?time=1609462800&query=" + url.QueryEscape(tc.query),
			"/loki/api/v1/query_range?start=1609459200&end=1609462800&step=60&query=" + url.QueryEscape(tc.query),
		} {
			for _, flags := range []string{"", "categorize-labels"} {
				t.Run(target+"/"+flags, func(t *testing.T) {
					headers := map[string]string{"X-Scope-OrgID": "0"}
					if flags != "" {
						headers["X-Loki-Response-Encoding-Flags"] = flags
					}
					w := doCompatProxyRequest(p, target, headers)
					if w.Code != http.StatusBadRequest {
						t.Fatalf("status=%d body=%s", w.Code, w.Body)
					}
					var body struct{ Status, ErrorType, Error string }
					if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Status != "error" || body.ErrorType != "bad_data" || body.Error != tc.want {
						t.Fatalf("body=%s, want bad_data error %q", w.Body, tc.want)
					}
				})
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected count_values reached the backend %d times", calls.Load())
	}
}
