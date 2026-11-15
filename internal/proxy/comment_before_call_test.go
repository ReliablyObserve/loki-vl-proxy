package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// A comment between a vector aggregation's name and its parenthesis or `by (` grouping makes the name an identifier
// to Loki: query and query_range answer Loki's 400 with its exact text (the grammar sweep's recordings, Loki 3.7.9)
// before any VictoriaLogs call, also on the second request; the same query without the comment is answered, and a
// comment after the grouping keyword is removed as before.
//
// conformance: semantics/comment-between-function-and-parenthesis-accepted
func TestCommentBeforeVectorAggregationCallAnswersLikeLoki(t *testing.T) {
	var upstream atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer vl.Close()
	for _, tc := range []struct{ query, lokiErr string }{
		{"sum # c\n(rate({app=\"a\"}[5m])) by (app)", "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"},
		{"sum#c\nby (app) (rate({app=\"a\"}[5m]))", "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"},
		{"SUM by # c\n(app) (rate({app=\"a\"}[5m]))", "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"},
		{"topk(2, sum # c\nby (app) (rate({app=\"a\"}[5m])))", "parse error at line 1, col 9: syntax error: unexpected IDENTIFIER"},
	} {
		for _, rt := range failFastRoutes {
			if !rt.metric {
				continue
			}
			t.Run(rt.name+" "+tc.query, func(t *testing.T) {
				p := newCompatTestProxy(t, vl.URL)
				params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}}
				before := upstream.Load()
				for i := 0; i < 2; i++ {
					rec := doCompatProxyRequest(p, rt.path+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
					if rec.Code != http.StatusBadRequest || strings.TrimSpace(rec.Body.String()) != tc.lokiErr {
						t.Errorf("request %d: %d %q, want Loki's 400 %q", i+1, rec.Code, rec.Body, tc.lokiErr)
					}
				}
				if calls := upstream.Load() - before; calls != 0 {
					t.Errorf("%d requests reached VictoriaLogs", calls)
				}
			})
		}
	}
	// The query without the comment, and with a comment where Loki's lexer does not look, is answered.
	for _, q := range []string{"sum (rate({app=\"a\"}[5m])) by (app)", "sum (rate({app=\"a\"}[5m])) by # c\n(app)"} {
		if msg := cachedQueryValidation(logqlpkg.StripComments(q)).msg; msg != "" {
			t.Errorf("%q: rejected with %q", q, msg)
		}
	}
}
