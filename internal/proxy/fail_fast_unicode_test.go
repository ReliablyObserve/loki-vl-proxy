package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// failFastRoutes are the routes that take a LogQL query, and the parameter that holds it.
var failFastRoutes = []struct {
	name, path, param string
	metric            bool // takes a metric query; the others take a log query or a selector
}{
	{"query", "/loki/api/v1/query", "query", true},
	{"query_range", "/loki/api/v1/query_range", "query", true},
	{"series", "/loki/api/v1/series", "match[]", false},
	{"labels", "/loki/api/v1/labels", "query", false},
	{"label_values", "/loki/api/v1/label/app/values", "query", false},
	{"index_stats", "/loki/api/v1/index/stats", "query", false},
	{"volume", "/loki/api/v1/index/volume", "query", false},
	{"volume_range", "/loki/api/v1/index/volume_range", "query", false},
	{"patterns", "/loki/api/v1/patterns", "query", false},
	{"detected_fields", "/loki/api/v1/detected_fields", "query", false},
	{"detected_field_values", "/loki/api/v1/detected_field/app/values", "query", false},
	{"detected_labels", "/loki/api/v1/detected_labels", "query", false},
	{"tail", "/loki/api/v1/tail", "query", false},
}

// lateValidation lists the routes that reach VictoriaLogs before they reject the query: a follow-up to move each one's
// validation ahead of its first lookup. A route listed here that validates first fails the test, so the list only
// shrinks. Empty: every route rejects with a cold cache without a request. query and query_range still look their
// compat cache up before the handler validates (compatCacheMiddleware runs inside the chain that has no validation for
// them), and format_query echoes its query without parsing it (Loki parses it: a follow-up). The first only matters for an answer cached before a change of the parser: the rune fence covers it.
var lateValidation = map[string]bool{}

// A query Loki rejects fast is rejected at the first step of the route, with Loki's status and text, before any
// cache lookup, plan, translation or VictoriaLogs call: the queries that once stopped the parser from returning,
// on every route that takes a query. The fake VictoriaLogs counts its requests.
//
// conformance: semantics/logql-unicode-runes, semantics/label-list-trailing-comma
func TestRejectedQueryNeverReachesVictoriaLogs(t *testing.T) {
	var upstream atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer vl.Close()
	for _, tc := range []struct{ log, metric, text string }{
		{"{app=\"a\"} | x ٣", "sum(rate({app=\"a\"}[5m])) > ٣", "syntax error: unexpected IDENTIFIER"},
		{"{app=\"a\"} | x > ٣1", "sum(rate({app=\"a\"} | x > ٣1 [5m]))", "syntax error: unexpected IDENTIFIER"},
		{"{app=\"a\"} | json | x == ०", "rate({app=\"a\"}[٣5m])", ""},
		// A label list with a trailing, doubled or leading comma (semantics/label-list-trailing-comma).
		{"{app=\"a\"} | drop a,", "sum by (a,) (rate({app=\"a\"}[5m]))", "expecting IDENTIFIER"},
		{"{app=\"a\"} | keep ,a", "sum(rate({app=\"a\"}[5m])) / on(a,) sum(rate({app=\"a\"}[5m]))", "expecting IDENTIFIER"},
		{"{app=\"a\"} | json a,,b", "sum without (a,,b) (rate({app=\"a\"}[5m]))", "expecting IDENTIFIER"},
	} {
		for _, rt := range failFastRoutes {
			query := tc.log
			if rt.metric {
				query = tc.metric
			}
			t.Run(rt.name+" "+query, func(t *testing.T) {
				p := newCompatTestProxy(t, vl.URL)
				params := url.Values{rt.param: {query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}}
				if rt.name == "tail" {
					params.Set("limit", "10")
				}
				before := upstream.Load()
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					done <- doCompatProxyRequest(p, rt.path+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
				}()
				var rec *httptest.ResponseRecorder
				select {
				case rec = <-done:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s does not answer", rt.path)
				}
				calls := upstream.Load() - before
				body := rec.Body.String()
				if rec.Code != http.StatusBadRequest || !strings.Contains(body, "parse error") || (tc.text != "" && !strings.Contains(body, tc.text)) {
					t.Errorf("%s: %d %s, want Loki's 400 with a parse error", rt.path, rec.Code, body)
				}
				if calls != 0 && !lateValidation[rt.name] {
					t.Errorf("%s: %d requests reached VictoriaLogs", rt.path, calls)
				}
				if calls == 0 && lateValidation[rt.name] {
					t.Errorf("%s validates first now: take it out of lateValidation", rt.name)
				}
			})
		}
	}
}

// A query that was answered before the scanner read its characters as Loki's lexer does, and is a 400 now (a stage name
// followed by a character that starts no token), is not served the answer an older binary cached: the compat edge key
// of a rejected query carries a version, and the key of a valid query does not change.
//
// conformance: semantics/logql-unicode-runes, semantics/label-list-trailing-comma, cache-response-key-versions
func TestCompatEdgeDoesNotServeAnAnswerCachedBeforeTheRuneFix(t *testing.T) {
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[]}}`))
	}))
	defer vl.Close()
	cached := `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"a"},"values":[["1700000000000000000","old"]]}]}}`
	for _, endpoint := range []string{"query_range", "query"} {
		for _, tc := range []struct {
			query    string
			rejected bool
		}{
			{"{app=\"a\"} | x \u00b2", true},
			{"{app=\"a\"} | x \u2153", true},
			{"{app=\"a\"} | x \u2163", true},
			{"{app=\"a\"} | x $", true},
			{"sum by (a,) (count_over_time({app=\"a\"}[1m]))", true},
			{"{app=\"a\"} | drop a,", true},
			{"sum by (a) (count_over_time({app=\"a\"}[1m]))", false},
			{"{app=\"a\"} |= \"x\u00b2\"", false},
		} {
			p := newCompatTestProxy(t, vl.URL)
			params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}}
			target := "/loki/api/v1/" + endpoint + "?" + params.Encode()
			key, ok := p.compatCacheKey(endpoint, httptest.NewRequest(http.MethodGet, target, nil))
			if !ok {
				t.Fatalf("%s %q: no compat key", endpoint, tc.query)
			}
			if got := strings.Contains(key, runeResponseVersion); got != tc.rejected {
				t.Fatalf("%s %q: key %q carries the fence = %v, want %v", endpoint, tc.query, key, got, tc.rejected)
			}
			// The key a binary without the fence built: the same key with the fence taken out.
			p.compatCache.SetWithTTL(strings.Replace(key, runeResponseVersion, "", 1), []byte(cached), 5*time.Minute)
			rec := doCompatProxyRequest(p, target, nil)
			if tc.rejected {
				if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), "old") {
					t.Errorf("%s %q: got %d %s, want Loki's 400 and no cached answer", endpoint, tc.query, rec.Code, rec.Body.String())
				}
			} else if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "old") {
				t.Errorf("%s %q: got %d %s, want the cached answer", endpoint, tc.query, rec.Code, rec.Body.String())
			}
		}
	}
}
