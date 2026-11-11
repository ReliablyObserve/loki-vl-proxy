package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkRequestLogger is the cost of the request wrapper around a handler that does nothing: caller
// resolution, telemetry, metrics and the request line (JSON, to io.Discard). "plain" is the default
// configuration; "user" sends X-Grafana-User and X-Query-Tags with -metrics.trust-proxy-headers and
// -server.enable-query-analytics on.
func BenchmarkRequestLogger(b *testing.B) {
	for _, tc := range []struct {
		name string
		user bool
	}{{"plain", false}, {"user", true}} {
		b.Run(tc.name, func(b *testing.B) {
			p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			p.log = slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
			p.requestSampler = nil
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D", nil)
			req.Header.Set("X-Scope-OrgID", "team-a")
			if tc.user {
				p.metricsTrustProxyHeaders, p.enableQueryAnalytics = true, true
				req.Header.Set("X-Grafana-User", "alice@example.com")
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
			w := httptest.NewRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				h.ServeHTTP(w, req)
			}
		})
	}
}
