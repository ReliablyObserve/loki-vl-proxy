package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// fanoutLineOwners answers a multi-tenant log query through the routes and returns, for every tenant of the merged
// answer, the owner marks of its lines (the fake below writes the AccountID a line was read for into the line).
func fanoutLineOwners(t *testing.T, mux *http.ServeMux, method, orgID, rawQuery string) map[string][]string {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+rawQuery, nil)
	if method == http.MethodPost {
		r = httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(rawQuery))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	r.Header.Set("X-Scope-OrgID", orgID)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("answer %q: %v", rec.Body.String(), err)
	}
	owners := map[string][]string{}
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			owners[s.Stream["__tenant_id__"]] = append(owners[s.Stream["__tenant_id__"]], v[1])
		}
	}
	for _, lines := range owners {
		sort.Strings(lines)
	}
	return owners
}

// Every tenant of a multi-tenant request is answered from its own scope: a tenant's part of the merged answer holds
// that tenant's lines only, also when the per-tenant answers come from the response cache. The query_range
// response cache key is built before the handler sets its request scope, so a tenant's sub-request that kept the
// multi-tenant request's scope wrote and read one key for every tenant. A repeat whose merged entry misses (here an
// extra parameter) then answered every tenant with the lines of whichever tenant stored last. The edge cache keeps a
// GET from that cache, not a POST, and not a GET when the edge cache is off; a window of its own changes nothing.
//
// conformance: tenant-numeric-routing
func TestMultiTenantFanout_EachTenantAnswersFromItsOwnScope(t *testing.T) {
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintf(w, `{"_time":"2023-11-14T22:13:20Z","_stream":"{app=\"x\"}","_msg":"read for %s","app":"x"}`+"\n", r.Header.Get("AccountID"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"values": []any{}})
	}))
	defer vl.Close()
	want := map[string][]string{"7": {"read for 7"}, "8": {"read for 8"}, "teamA": {"read for 42"}}
	for _, tc := range []struct {
		name, method string
		edgeCache    bool
		window       string
	}{
		{"GET, no edge cache", http.MethodGet, false, ""},
		{"GET, no edge cache, a window", http.MethodGet, false, "&start=1699999000&end=1700001000"},
		{"POST, edge cache", http.MethodPost, true, ""},
		{"POST, no edge cache", http.MethodPost, false, ""},
		{"GET, edge cache", http.MethodGet, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProxyWithOptions(t, tenantRoutingConfigs["default"], func(c *Config) {
				c.BackendURL = vl.URL
				if tc.edgeCache {
					c.CompatCache = cache.New(60*time.Second, 1000)
				}
			})
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			query := "query=" + url.QueryEscape(`{app="x"}`) + tc.window
			for _, raw := range []string{query, query + "&merged_entry_misses=1", query + "&merged_entry_misses=2"} {
				got := fanoutLineOwners(t, mux, tc.method, "7|8|teamA", raw)
				for tenant, lines := range want {
					if strings.Join(got[tenant], ",") != strings.Join(lines, ",") {
						t.Errorf("%s: tenant %s answered %q, want %q", raw, tenant, got[tenant], lines)
					}
				}
			}
		})
	}
}
