//go:build e2e

package e2e_compat

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// TestCompat_TenantHeaderLikeAuthEnabledLoki asks a live Loki with
// auth_enabled: true and an in-process proxy with -auth.enabled (over the
// stack's VictoriaLogs) the same requests that differ only in X-Scope-OrgID,
// and compares status, Content-Type and body of every refusal and the status
// of every admitted request. The stack's Loki runs with auth_enabled: false
// and ignores the header, so this test needs a second Loki:
//
//	loki -config.file=internal/proxy/testdata/tenant_auth_loki_config.yaml &
//	LOKI_AUTH_URL=http://127.0.0.1:23100 go test -tags=e2e ./test/e2e-compat -run TestCompat_TenantHeaderLikeAuthEnabledLoki -count=1 -v
//
// It skips without LOKI_AUTH_URL. The recorded copy of these answers runs on
// every PR as internal/proxy TestTenantAuth_AnswersLikeLoki.
//
// conformance: tenant-auth-follows-loki
func TestCompat_TenantHeaderLikeAuthEnabledLoki(t *testing.T) {
	lokiAuth := os.Getenv("LOKI_AUTH_URL")
	if lokiAuth == "" {
		t.Skip("LOKI_AUTH_URL (a Loki with auth_enabled: true) is not set")
	}
	headers := map[string]*string{
		"absent": nil, "empty": ptr(""), "single": ptr("team-a"), "multi": ptr("team-a|team-b"),
		"trailing-bar": ptr("team-a|"), "duplicate": ptr("team-a|team-a"), "invalid-member": ptr("team-a|a b"),
		"dot": ptr("."), "dotdot": ptr(".."), "long": ptr(strings.Repeat("a", 151)), "space": ptr("a b"),
		"slash": ptr("../x"), "non-ascii": ptr("zé"), "plus": ptr("+7"), "colon": ptr("x:y"),
	}
	tenants := map[string]proxy.TenantMapping{}
	for _, h := range headers {
		if h == nil {
			continue
		}
		for _, id := range strings.Split(*h, "|") {
			if id != "" {
				tenants[id] = proxy.TenantMapping{AccountID: "7" + string(rune('0'+len(tenants)%10)), ProjectID: "0"}
			}
		}
	}
	p, err := proxy.New(proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error", AuthEnabled: true, TenantMap: tenants})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	sel := url.QueryEscape(`{app=~".+"}`)
	win := "&start=1762860000&end=1762863600"
	routes := []string{
		"/loki/api/v1/query_range?query=" + sel + win,
		"/loki/api/v1/query?query=" + url.QueryEscape(`count_over_time({app=~".+"}[1h])`) + "&time=1762863600",
		"/loki/api/v1/series?match%5B%5D=" + sel + win,
		"/loki/api/v1/labels?start=1762860000&end=1762863600",
		"/loki/api/v1/label/app/values?start=1762860000&end=1762863600",
		"/loki/api/v1/index/volume?query=" + sel + win,
		"/loki/api/v1/detected_fields?query=" + sel + win,
		"/loki/api/v1/tail?query=" + sel,
	}
	ask := func(base, target string, h *string) (int, string, string) {
		req, _ := http.NewRequest(http.MethodGet, base+target, nil)
		if h != nil {
			req.Header["X-Scope-Orgid"] = []string{*h}
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s%s: %v", base, target, err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
	}
	for _, target := range routes {
		for name, h := range headers {
			ls, lct, lbody := ask(lokiAuth, target, h)
			ps, pct, pbody := ask(srv.URL, target, h)
			if ls != ps {
				t.Errorf("%s %s: proxy %d %q, Loki %d %q", strings.SplitN(target, "?", 2)[0], name, ps, pbody, ls, lbody)
				continue
			}
			if ls >= 300 && (lbody != pbody || lct != pct) {
				t.Errorf("%s %s: proxy %q (%s), Loki %q (%s)", strings.SplitN(target, "?", 2)[0], name, pbody, pct, lbody, lct)
			}
		}
	}
}

func ptr(s string) *string { return &s }
