//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// Each tenant of a multi-tenant query_range is answered from its own VictoriaLogs account and its own cached answer:
// two accounts hold one distinct line each, and a POST query_range for both (the request, then two repeats whose
// extra parameter misses the merged answer, so each tenant's answer comes from the per-tenant response cache)
// answers every __tenant_id__ with its own account's line only. The proxy runs in process against the stack's
// VictoriaLogs, with the response cache on and the compatibility edge cache on (a POST bypasses it) and off.
//
// conformance: tenant-numeric-routing
func TestFeature_Multitenancy_FanoutTenantsAnswerFromTheirOwnScope(t *testing.T) {
	unique := fmt.Sprintf("fanoutscope%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	accounts := map[string]string{"4201": unique + "-acct4201", "4202": unique + "-acct4202"}
	for account, msg := range accounts {
		tenantLabelIngest(t, map[string]string{"_time": stamp, "_msg": msg, "app": unique}, "app", account)
	}
	if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}
	// Searchable in both accounts, asked of VictoriaLogs itself: a probe through the proxy would cache its answers.
	deadline := time.Now().Add(30 * time.Second)
	for account, msg := range accounts {
		for {
			_, body := hardeningRequest(t, "GET", vlURL+"/select/logsql/query?query="+url.QueryEscape(`app:="`+unique+`"`), "",
				map[string]string{"AccountID": account, "ProjectID": "0"})
			if strings.Contains(string(body), msg) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("account %s's row did not become searchable", account)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	form := url.Values{
		"query": {`{app="` + unique + `"}`},
		"limit": {"100"},
		"start": {fmt.Sprint(time.Now().Add(-15 * time.Minute).UnixNano())},
		"end":   {fmt.Sprint(time.Now().Add(time.Minute).UnixNano())},
	}
	for _, edgeCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("edge cache %v", edgeCache), func(t *testing.T) {
			cfg := proxy.Config{BackendURL: vlURL, Cache: cache.New(time.Minute, 1000), LogLevel: "error"}
			if edgeCache {
				cfg.CompatCache = cache.New(time.Minute, 1000)
			}
			p, err := proxy.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterProxyRoutes(mux)
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)

			read := func(org string, extra string) (int, map[string][]string) {
				body := form.Encode() + extra
				status, raw := hardeningRequest(t, "POST", server.URL+"/loki/api/v1/query_range", body,
					map[string]string{"X-Scope-OrgID": org, "Content-Type": "application/x-www-form-urlencoded"})
				var resp struct {
					Data struct {
						Result []struct {
							Stream map[string]string `json:"stream"`
							Values [][]string        `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if status == http.StatusOK {
					if err := json.Unmarshal(raw, &resp); err != nil {
						t.Fatalf("answer %.300s: %v", raw, err)
					}
				}
				lines := map[string][]string{}
				for _, s := range resp.Data.Result {
					for _, v := range s.Values {
						lines[s.Stream["__tenant_id__"]] = append(lines[s.Stream["__tenant_id__"]], v[1])
					}
				}
				return status, lines
			}

			for _, extra := range []string{"", "&merged_entry_misses=1", "&merged_entry_misses=2"} {
				status, got := read("4201|4202", extra)
				if status != http.StatusOK {
					t.Fatalf("%q: status %d", extra, status)
				}
				for account, msg := range accounts {
					if strings.Join(got[account], ",") != msg {
						t.Errorf("%q: tenant %s answered %q, want only %q", extra, account, got[account], msg)
					}
				}
			}
		})
	}
}
