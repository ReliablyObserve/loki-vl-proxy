package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkTenantAdmissionAndHeaders covers what every request pays for its
// org: admission plus the AccountID/ProjectID headers of the backend request.
func BenchmarkTenantAdmissionAndHeaders(b *testing.B) {
	for _, tc := range []struct {
		name  string
		org   string
		label string
	}{
		{"numeric", "7", ""},
		{"mapped", "teamA", ""},
		{"alias", "0", ""},
		{"label", "teamA", "org_id"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p, err := New(Config{
				BackendURL:        "http://unused",
				Cache:             cache.New(60*time.Second, 1000),
				LogLevel:          "error",
				TenantLabel:       tc.label,
				AllowGlobalTenant: true,
				TenantMap:         map[string]TenantMapping{"teamA": {AccountID: "42", ProjectID: "0"}},
			})
			if err != nil {
				b.Fatal(err)
			}
			base := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
			base.Header.Set("X-Scope-OrgID", tc.org)
			r := p.withRequestScope(base)
			backend := httptest.NewRequest(http.MethodGet, "http://vl/select/logsql/query", nil).WithContext(r.Context())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.validateTenantHeader(r); err != nil {
					b.Fatal(err)
				}
				p.forwardTenantHeaders(backend)
			}
		})
	}
}
