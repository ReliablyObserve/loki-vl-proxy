package proxy

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// /metrics and /config on the main listener reveal no secret and no more than
// Loki's do. Loki 3.7.9 serves both without authentication: /config is its
// whole configuration with every secret field masked, /metrics holds per-tenant
// series (loki_distributor_lines_received_total{tenant=...} and others) and its
// gRPC address. The proxy's /config is a stub, and its /metrics carries no
// token, backend credential, tenant mapping, peer address or backend address,
// and no tenant name unless -metrics.export-sensitive-labels asks for it.
//
// conformance: instrumentation-reveals-no-secrets
func TestInstrumentation_RevealsNoSecrets(t *testing.T) {
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
	}))
	defer vl.Close()
	vlHost := strings.TrimPrefix(vl.URL, "http://")
	const (
		adminToken  = "admin-secret-token-31337"
		deleteToken = "delete-secret-token-31337"
		peerToken   = "peer-secret-token-31337"
		backendPw   = "vl-secret-password-31337"
		backendUsr  = "vl-secret-user-31337"
		org         = "team-secret-org"
		account     = "918273"
		project     = "645"
		peerA       = "10.99.88.77:3100"
		peerB       = "10.99.88.78:3100"
		clientAuth  = "client-secret-bearer-31337"
	)
	for _, sensitive := range []bool{false, true} {
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: peerA, DiscoveryType: "static", StaticPeers: peerA + "," + peerB, AuthToken: peerToken, Timeout: 50 * time.Millisecond})
		on := true
		p, err := New(Config{
			BackendURL: vl.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error",
			RegisterInstrumentation: &on, AdminAuthToken: adminToken, DeleteAPIEnabled: true, DeleteAPIAuthToken: deleteToken,
			BackendBasicAuth: backendUsr + ":" + backendPw, BackendHeaders: map[string]string{"X-Static-Secret": backendPw},
			PeerCache: pc, PeerAuthToken: peerToken,
			TenantMap:                    map[string]TenantMapping{org: {AccountID: account, ProjectID: project}},
			ForwardHeaders:               []string{"Authorization"},
			MetricsExportSensitiveLabels: sensitive,
		})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		for _, target := range []string{"/loki/api/v1/labels?start=1762000000&end=1762003600", "/loki/api/v1/label/app/values?start=1762000000&end=1762003600", "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="x"}`) + "&start=1762000000&end=1762003600"} {
			r := httptest.NewRequest(http.MethodGet, target, nil)
			r.Header.Set("X-Scope-OrgID", org)
			r.Header.Set("Authorization", "Bearer "+clientAuth)
			mux.ServeHTTP(httptest.NewRecorder(), r)
		}
		secrets := []string{adminToken, deleteToken, peerToken, backendPw, backendUsr, base64.StdEncoding.EncodeToString([]byte(backendUsr + ":" + backendPw)),
			account, project + `"`, peerA, peerB, "10.99.88.", vlHost, clientAuth}
		if !sensitive {
			secrets = append(secrets, org)
		}
		for _, path := range []string{"/metrics", "/config"} {
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			body, _ := io.ReadAll(w.Result().Body)
			if w.Code != http.StatusOK || len(body) == 0 {
				t.Fatalf("%s: %d", path, w.Code)
			}
			for _, s := range secrets {
				if strings.Contains(string(body), s) {
					i := strings.Index(string(body), s)
					t.Fatalf("export-sensitive-labels=%v: %s reveals %q: ...%s...", sensitive, path, s, string(body)[max(0, i-200):min(len(body), i+100)])
				}
			}
			if path == "/metrics" && sensitive && !strings.Contains(string(body), org) {
				t.Fatalf("-metrics.export-sensitive-labels did not export the tenant (the test would not see a tenant leak)")
			}
		}
		pc.Close()
	}
}
