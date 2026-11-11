package proxy

import (
	"bytes"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// grafanaCredentials is every credential or identity Grafana's Loki data
// source can send (Grafana docs, Loki data source "Authentication" and
// "Custom HTTP headers"; Grafana server send_user_header), each with values
// that appear nowhere else. TLS client certificates end at the proxy's
// listener (-tls-client-ca-file, TestBuildHTTPServer_WithTLSClientCA): the
// proxy never sees them as headers.
func grafanaCredentials(user string) http.Header {
	h := http.Header{}
	h.Set("X-Scope-OrgID", "team-a")
	switch user {
	case "basic":
		// Basic authentication.
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("basic-secret-user:basic-secret-pass")))
	case "oauth":
		// Forward OAuth Identity: the user's access token and OIDC ID token.
		h.Set("Authorization", "Bearer oauth-secret-access-token")
		h.Set("X-Id-Token", "oauth-secret-id-token")
	case "oauth2":
		h.Set("Authorization", "Bearer oauth-secret-other-token")
		h.Set("X-Id-Token", "oauth-secret-other-id-token")
	case "custom":
		// Custom HTTP headers.
		h.Set("X-Custom-Auth", "custom-secret-value")
	case "cookies":
		// Allowed cookies (and With credentials).
		h.Set("Cookie", "grafana_session=cookie-secret-session; other=cookie-secret-other")
	case "user":
		// send_user_header: the signed-in Grafana user.
		h.Set("X-Grafana-User", "grafana-secret-user")
	}
	return h
}

var grafanaCredentialSecrets = []string{
	"basic-secret-user", "basic-secret-pass", base64.StdEncoding.EncodeToString([]byte("basic-secret-user:basic-secret-pass")),
	"oauth-secret-access-token", "oauth-secret-id-token", "oauth-secret-other-token", "oauth-secret-other-id-token",
	"custom-secret-value", "cookie-secret-session", "cookie-secret-other", "grafana-secret-user",
}

// Each credential Grafana sends is forwarded to VictoriaLogs only when a flag
// says so, and then only the configured names; it never reaches the log, the
// cache key text or the disk cache; and a request whose answer depends on a
// forwarded credential is never answered from another user's cache entry.
//
// conformance: credential-passthrough-as-configured
func TestCredentialPassThrough_AsConfigured(t *testing.T) {
	type forwarded map[string]string // VictoriaLogs header -> value expected
	for _, tc := range []struct {
		name   string
		config func(*Config)
		user   string
		want   forwarded
		// sharedCache: a second user's identical request may reuse the first
		// one's answer (nothing that differs between them is forwarded).
		sharedCache bool
	}{
		{"basic, nothing forwarded", nil, "basic", nil, true},
		{"oauth, nothing forwarded", nil, "oauth", nil, true},
		{"custom header, nothing forwarded", nil, "custom", nil, true},
		{"cookies, nothing forwarded", nil, "cookies", nil, true},
		{"grafana user, not trusted", nil, "user", nil, true},
		{"basic, -forward-authorization", func(c *Config) { c.ForwardHeaders = []string{"Authorization"} }, "basic",
			forwarded{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("basic-secret-user:basic-secret-pass"))}, false},
		{"oauth, -forward-authorization", func(c *Config) { c.ForwardHeaders = []string{"Authorization"} }, "oauth",
			forwarded{"Authorization": "Bearer oauth-secret-access-token"}, false},
		{"oauth, -forward-headers=Authorization,X-Id-Token", func(c *Config) { c.ForwardHeaders = []string{"Authorization", "X-Id-Token"} }, "oauth",
			forwarded{"Authorization": "Bearer oauth-secret-access-token", "X-Id-Token": "oauth-secret-id-token"}, false},
		{"custom, -forward-headers=X-Custom-Auth", func(c *Config) { c.ForwardHeaders = []string{"X-Custom-Auth"} }, "custom",
			forwarded{"X-Custom-Auth": "custom-secret-value"}, false},
		{"cookies, -forward-cookies=grafana_session", func(c *Config) { c.ForwardCookies = []string{"grafana_session"} }, "cookies",
			forwarded{"Cookie": "grafana_session=cookie-secret-session"}, false},
		{"grafana user, -metrics.trust-proxy-headers", func(c *Config) { c.MetricsTrustProxyHeaders = true }, "user",
			forwarded{"X-Grafana-User": "grafana-secret-user", "X-Loki-Vl-Client-Id": "grafana-secret-user"}, true},
		{"basic, -metrics.trust-proxy-headers names the user only", func(c *Config) { c.MetricsTrustProxyHeaders = true }, "basic",
			forwarded{"X-Loki-Vl-Auth-User": "basic-secret-user"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var seen []http.Header
			var calls atomic.Int64
			vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				mu.Lock()
				seen = append(seen, r.Header.Clone())
				mu.Unlock()
				_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
			}))
			defer vl.Close()
			dir := t.TempDir()
			dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: dir + "/cache.db", MinTTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			c := cache.New(5*time.Minute, 1000)
			c.SetL2(dc)
			var logs bytes.Buffer
			var logMu sync.Mutex
			cfg := Config{BackendURL: vl.URL, Cache: c, LogLevel: "debug",
				TenantMap: map[string]TenantMapping{"team-a": {AccountID: "1", ProjectID: "0"}}}
			if tc.config != nil {
				tc.config(&cfg)
			}
			p, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			p.log = slog.New(slog.NewJSONHandler(lockedWriter{&logMu, &logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			ask := func(user string) {
				r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?start=1762000000&end=1762003600", nil)
				r.Header = grafanaCredentials(user)
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, r)
				if w.Code != http.StatusOK {
					t.Fatalf("%d %s", w.Code, w.Body.String())
				}
			}
			ask(tc.user)
			first := calls.Load()
			if first == 0 {
				t.Fatal("VictoriaLogs was not asked")
			}
			mu.Lock()
			for _, h := range seen {
				for name, value := range tc.want {
					if got := h.Get(name); got != value {
						t.Fatalf("VictoriaLogs %s = %q, want %q", name, got, value)
					}
				}
				for name, values := range h {
					if _, ok := tc.want[name]; ok {
						continue
					}
					for _, v := range values {
						for _, secret := range grafanaCredentialSecrets {
							if strings.Contains(v, secret) {
								t.Fatalf("VictoriaLogs received %s: %q without a flag forwarding it", name, v)
							}
						}
					}
				}
			}
			mu.Unlock()

			// A second user with other credentials of the same kind.
			other := map[string]string{"basic": "oauth2", "oauth": "oauth2", "custom": "oauth2", "cookies": "oauth2", "user": "oauth2"}[tc.user]
			ask(other)
			if reused := calls.Load() == first; reused && !tc.sharedCache {
				t.Fatal("a user was answered from another user's cache entry although the answer depends on a forwarded credential")
			}

			dc.Flush()
			_ = dc.Close()
			c.Close()
			raw, err := os.ReadFile(dir + "/cache.db")
			if err != nil {
				t.Fatal(err)
			}
			logMu.Lock()
			out := logs.String()
			logMu.Unlock()
			for _, secret := range grafanaCredentialSecrets {
				// The trusted Grafana user is the request log's enduser.id by
				// design (identity, not a credential).
				trustedUser := secret == "grafana-secret-user" && cfg.MetricsTrustProxyHeaders
				if strings.Contains(out, secret) && !trustedUser {
					t.Fatalf("the log holds %q: %s", secret, out)
				}
				if bytes.Contains(raw, []byte(secret)) {
					t.Fatalf("the disk cache holds %q", secret)
				}
			}
		})
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
