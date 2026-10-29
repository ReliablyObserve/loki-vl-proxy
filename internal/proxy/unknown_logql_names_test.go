package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// recordingBackend is a fake VictoriaLogs that answers every request with an
// empty result and keeps the requests it saw, as path and sorted parameters.
type recordingBackend struct {
	*httptest.Server
	mu    sync.Mutex
	calls []string
}

func newRecordingBackend(t *testing.T) *recordingBackend {
	t.Helper()
	b := &recordingBackend{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		b.mu.Lock()
		b.calls = append(b.calls, r.URL.Path+"?"+r.Form.Encode())
		b.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"hits":[],"values":[]}`))
	}))
	t.Cleanup(b.Close)
	return b
}

// dataCalls are the requests that read log data or metric results, not the
// label and stream-field lookups the proxy makes on its own.
func (b *recordingBackend) dataCalls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, c := range b.calls {
		if strings.HasPrefix(c, "/select/logsql/query?") || strings.HasPrefix(c, "/select/logsql/stats_query") || strings.HasPrefix(c, "/select/logsql/hits?") {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

func queryURL(endpoint, query string) string {
	params := url.Values{"query": {query}, "start": {"1775322000"}, "end": {"1775325600"}, "time": {"1775325600"}, "step": {"60"}, "limit": {"10"}}
	return "/loki/api/v1/" + endpoint + "?" + params.Encode()
}

// Loki v3.7.7 has no group() and no label_join(), and none of the PromQL math,
// time or quantile functions: a name outside lex.go functionTokens is an
// IDENTIFIER the grammar cannot place (syntax.y vectorOp, LABEL_REPLACE,
// vectorExpr). Both query endpoints answer 400 with Loki's text and the column
// of the name, before any backend call. The messages are syntax.ParseExpr's.
//
// conformance: semantics/proxy-accepts-operators-not-in-loki
func TestUnknownLogQLNamesRejectedLikeLoki(t *testing.T) {
	const identifier = "parse error at line 1, col %d: syntax error: unexpected IDENTIFIER"
	cases := []struct {
		query string
		col   int
	}{
		{`group(count_over_time({app="a"}[5m]))`, 1},
		{`GROUP(count_over_time({app="a"}[5m]))`, 1},
		{`group by (app) (count_over_time({app="a"}[5m]))`, 1},
		{`sum(group(rate({app="a"}[5m])))`, 5},
		{`label_join(rate({app="a"}[5m]), "d", ",", "app", "pod")`, 1},
		{`LABEL_JOIN(rate({app="a"}[5m]), "d", ",", "app")`, 1},
		{`label_replace(label_join(rate({app="a"}[5m]), "d", ",", "app"), "a", "b", "c", "d")`, 15},
		{`sum(rate({app="a"}[5m])) + clamp_min(rate({app="a"}[5m]), 1)`, 28},
		{`abs(rate({app="a"}[5m]))`, 1},
		{`time()`, 1},
		{`count_values("x", count_over_time({app="a"}[5m]))`, 1},
	}
	backend := newRecordingBackend(t)
	p := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeTranslated, false)
	for _, endpoint := range []string{"query_range", "query"} {
		for _, tc := range cases {
			t.Run(endpoint+"/"+tc.query, func(t *testing.T) {
				want := strings.Replace(identifier, "%d", strconv.Itoa(tc.col), 1)
				w := serve(p, http.MethodGet, queryURL(endpoint, tc.query), nil)
				var got struct{ ErrorType, Error, Message string }
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusBadRequest || got.ErrorType != "bad_data" || got.Error != want || got.Message != want {
					t.Fatalf("status=%d body=%.300s, want 400 %q", w.Code, w.Body, want)
				}
			})
		}
	}
	if calls := backend.dataCalls(); len(calls) != 0 {
		t.Fatalf("rejected queries reached the backend: %v", calls)
	}
}

// Loki lower-cases every word before its lexer looks it up (lex.go), so SUM,
// TOPK(3, ...), RATE, BY, OFFSET, UNWRAP and the like are the keywords they
// spell. The proxy answers them with the same backend requests as the
// lower-case spelling.
//
// conformance: semantics/proxy-accepts-operators-not-in-loki
func TestUppercaseKeywordsSendTheLowercaseBackendRequests(t *testing.T) {
	cases := []struct{ upper, lower string }{
		{`TOPK(3, RATE({app="a"}[5m]))`, `topk(3, rate({app="a"}[5m]))`},
		{`SUM BY (app) (COUNT_OVER_TIME({app="a"} |= "ERROR" [5m]))`, `sum by (app) (count_over_time({app="a"} |= "ERROR" [5m]))`},
		{`SUM(RATE({app="a"}[5m])) BY (app)`, `sum(rate({app="a"}[5m])) by (app)`},
		{`SUM WITHOUT (pod) (RATE({app="a"}[5m]))`, `sum without (pod) (rate({app="a"}[5m]))`},
		{`SUM(RATE({app="a"} | JSON | UNWRAP bytes [5m]))`, `sum(rate({app="a"} | json | unwrap bytes [5m]))`},
		{`SUM(RATE({app="a"}[5m])) / ON (app) SUM(RATE({app="b"}[5m]))`, `sum(rate({app="a"}[5m])) / on (app) sum(rate({app="b"}[5m]))`},
		{`SUM(RATE({app="a"}[5m])) > BOOL 1`, `sum(rate({app="a"}[5m])) > bool 1`},
		{`MAX(COUNT_OVER_TIME({app="a"}[5m] OFFSET 1m))`, `max(count_over_time({app="a"}[5m] offset 1m))`},
		{`{app="a"} | JSON | DROP pod`, `{app="a"} | json | drop pod`},
	}
	// The same request three ways: a GET, a form-encoded POST and a multi-tenant
	// GET that fans out per tenant.
	run := func(t *testing.T, endpoint, variant, query string) []string {
		backend := newRecordingBackend(t)
		p := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeTranslated, false)
		var w *httptest.ResponseRecorder
		switch variant {
		case "post":
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			target := queryURL(endpoint, query)
			form := target[strings.Index(target, "?")+1:]
			req := httptest.NewRequest(http.MethodPost, target[:strings.Index(target, "?")], strings.NewReader(form))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("X-Scope-OrgID", "0")
			w = httptest.NewRecorder()
			mux.ServeHTTP(w, req)
		case "multitenant":
			w = serve(p, http.MethodGet, queryURL(endpoint, query), map[string]string{"X-Scope-OrgID": "0|1"})
		default:
			w = serve(p, http.MethodGet, queryURL(endpoint, query), nil)
		}
		if w.Code >= 500 || w.Code == http.StatusBadRequest {
			t.Fatalf("%s %s %s: status %d %.300s", variant, endpoint, query, w.Code, w.Body)
		}
		return backend.dataCalls()
	}
	for _, endpoint := range []string{"query_range", "query"} {
		for _, variant := range []string{"get", "post", "multitenant"} {
			for _, tc := range cases {
				if endpoint == "query" && strings.HasPrefix(tc.lower, "{") {
					continue // a log query is not an instant query
				}
				t.Run(endpoint+"/"+variant+"/"+tc.upper, func(t *testing.T) {
					lower := run(t, endpoint, variant, tc.lower)
					if len(lower) == 0 {
						t.Fatalf("the lower-case form %q reached no backend call, the comparison proves nothing", tc.lower)
					}
					upper := run(t, endpoint, variant, tc.upper)
					if variant == "multitenant" {
						// Tenant fanout can issue the same translated request once or twice
						// depending on coalescing. Compare translated URLs here; tenant
						// fanout counts are covered by the tenant tests.
						lower = slices.Compact(lower)
						upper = slices.Compact(upper)
					}
					if strings.Join(upper, "\n") != strings.Join(lower, "\n") {
						t.Fatalf("backend requests differ\n upper %q\n lower %q", upper, lower)
					}
				})
			}
		}
	}
}
