package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// A label filter after `| json` names Loki's sanitized label (http_method) while
// VictoriaLogs' unpack_json keeps the original key (http.method): the query sent
// upstream must accept every spelling, for log and metric queries alike.
//
// conformance: profiles/sanitized-json-key-filter
func TestSanitizedJSONKeyFilterReachesVictoriaLogs(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		queries = append(queries, r.Form.Get("query"))
		mu.Unlock()
	}))
	defer backend.Close()

	for _, tc := range []struct{ name, path, query string }{
		{"log query", "/loki/api/v1/query_range", `{app="web"} | json | http_method="GET"`},
		{"metric query", "/loki/api/v1/query_range", `sum(count_over_time({app="web"} | json | http_method="GET" | http_status_code>=400 [5m]))`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			queries = nil
			mu.Unlock()
			p := newTestProxy(t, backend.URL)
			q := url.Values{"query": {tc.query}, "start": {"1767225600"}, "end": {"1767229200"}, "step": {"60"}, "time": {"1767229200"}}
			if res := doCompatProxyRequest(p, tc.path+"?"+q.Encode(), nil); res.Code != http.StatusOK {
				t.Fatalf("%s: %d %s", tc.query, res.Code, res.Body)
			}
			mu.Lock()
			defer mu.Unlock()
			var sawSpelling bool
			for _, vl := range queries {
				if strings.Contains(vl, `"http.method"`) && strings.Contains(vl, `"http-method"`) {
					sawSpelling = true
				}
			}
			if !sawSpelling {
				t.Fatalf("no upstream query accepts the original keys of http_method: %q", queries)
			}
		})
	}
}
