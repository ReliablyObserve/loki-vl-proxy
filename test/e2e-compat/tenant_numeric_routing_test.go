//go:build e2e

package e2e_compat

import (
	"context"
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

func numericRoutingServer(t *testing.T, mutate func(*proxy.Config)) *httptest.Server {
	t.Helper()
	cfg := proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error"}
	mutate(&cfg)
	p, err := proxy.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

// Numeric orgs are read from the VictoriaLogs account of the same number only
// in their canonical spelling, and strict routing serves only mapped orgs. The
// rows live in real VictoriaLogs accounts; a leak (007 reading account 7, a
// strict proxy reading an unmapped account) shows up as that account's row in
// the answer.
//
// conformance: tenant-numeric-routing, semantics/tenant-numeric-canonical
func TestFeature_Multitenancy_NumericRoutingIsCanonicalAndStrictModeServesOnlyMappedOrgs(t *testing.T) {
	unique := fmt.Sprintf("numroute%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	accounts := map[string]string{"4101": unique + "-acct4101", "4102": unique + "-acct4102", "4294967295": unique + "-acctmax"}
	for account, msg := range accounts {
		tenantLabelIngest(t, map[string]string{"_time": stamp, "_msg": msg, "app": unique}, "app", account)
	}
	if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}

	defaults := numericRoutingServer(t, func(c *proxy.Config) {
		c.TenantMap = map[string]proxy.TenantMapping{"mapped-ten": {AccountID: "4102", ProjectID: "0"}}
	})
	strict := numericRoutingServer(t, func(c *proxy.Config) {
		c.StrictTenantRouting = true
		c.TenantMap = map[string]proxy.TenantMapping{"mapped-ten": {AccountID: "4102", ProjectID: "0"}, "4101": {AccountID: "4101", ProjectID: "0"}}
	})

	path := "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="`+unique+`"}`) +
		"&limit=100&start=" + fmt.Sprint(time.Now().Add(-15*time.Minute).UnixNano()) + "&end=" + fmt.Sprint(time.Now().Add(time.Minute).UnixNano())
	read := func(server *httptest.Server, org string) (int, string) {
		headers := map[string]string{}
		if org != "-" {
			headers["X-Scope-OrgID"] = org
		}
		status, body := hardeningRequest(t, "GET", server.URL+path, "", headers)
		return status, string(body)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, body := read(defaults, "4101"); strings.Contains(body, accounts["4101"]) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ingested rows did not become searchable")
		}
		time.Sleep(500 * time.Millisecond)
	}

	type want struct {
		status int
		sees   string // the one account row the answer must hold ("" = none required)
	}
	for _, tc := range []struct {
		name   string
		server *httptest.Server
		org    string
		want   want
	}{
		{"default: canonical number reads its account", defaults, "4101", want{200, accounts["4101"]}},
		{"default: largest account", defaults, "4294967295", want{200, accounts["4294967295"]}},
		{"default: mapped name reads its mapped account", defaults, "mapped-ten", want{200, accounts["4102"]}},
		{"default: leading zeros are refused, not merged into 4101", defaults, "04101", want{403, ""}},
		{"default: sign is refused", defaults, "+4101", want{403, ""}},
		{"default: negative is refused", defaults, "-1", want{403, ""}},
		{"default: above uint32 is refused", defaults, "4294967296", want{403, ""}},
		{"default: unknown name is refused", defaults, "nobody", want{403, ""}},
		{"default: multi-tenant with a non-canonical part is refused", defaults, "4101|04101", want{403, ""}},
		{"strict: mapped number reads its account", strict, "4101", want{200, accounts["4101"]}},
		{"strict: mapped name reads its mapped account", strict, "mapped-ten", want{200, accounts["4102"]}},
		{"strict: unmapped canonical number is refused", strict, "4294967295", want{403, ""}},
		{"strict: non-canonical number is refused", strict, "04101", want{403, ""}},
		{"strict: default alias is refused", strict, "0", want{403, ""}},
		{"strict: missing org is refused", strict, "-", want{403, ""}},
		{"strict: multi-tenant with an unmapped part is refused", strict, "4101|4294967295", want{403, ""}},
	} {
		status, body := read(tc.server, tc.org)
		if status != tc.want.status {
			t.Errorf("%s: status %d, want %d: %.200s", tc.name, status, tc.want.status, body)
			continue
		}
		for account, msg := range accounts {
			holds := strings.Contains(body, msg)
			if tc.want.status != http.StatusOK && holds {
				t.Errorf("%s: a refused request answered with account %s's row", tc.name, account)
			}
			if tc.want.status == http.StatusOK && holds != (msg == tc.want.sees) {
				t.Errorf("%s: account %s's row present=%v, want only %q", tc.name, account, holds, tc.want.sees)
			}
		}
	}
}
