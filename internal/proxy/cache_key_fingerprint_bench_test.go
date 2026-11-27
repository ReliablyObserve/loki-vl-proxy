package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkForwardedAuthFingerprint measures the per-request fingerprint on
// its three paths: nothing forwarded (tenant scope only), trusted proxy
// identity (a user name, no credential) and a forwarded credential.
func BenchmarkForwardedAuthFingerprint(b *testing.B) {
	for _, bc := range []struct {
		name    string
		cfg     Config
		headers map[string]string
	}{
		{name: "scope-only", cfg: Config{}, headers: map[string]string{"X-Scope-OrgID": "team-a"}},
		{name: "trusted-identity", cfg: Config{MetricsTrustProxyHeaders: true}, headers: map[string]string{"X-Scope-OrgID": "team-a", "X-Grafana-User": "alice"}},
		{name: "credential", cfg: Config{ForwardHeaders: []string{"Authorization"}, ForwardCookies: []string{"session"}}, headers: map[string]string{
			"X-Scope-OrgID": "team-a", "Authorization": "Basic YWxpY2U6Y29ycmVjdC1ob3JzZQ==", "Cookie": "session=s3ss10n; other=x",
		}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			cfg := bc.cfg
			cfg.BackendURL = "http://vl.example:9428"
			cfg.LogLevel = "error"
			cfg.Cache = cache.New(30*time.Second, 100)
			p, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
			for k, v := range bc.headers {
				r.Header.Set(k, v)
			}
			r = withOrgID(r)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = p.forwardedAuthFingerprint(r)
			}
		})
	}
}
