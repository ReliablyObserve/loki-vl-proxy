package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkRequestLogger is the cost of the request wrapper: caller resolution, telemetry, metrics and the log
// lines (JSON at INFO, to io.Discard). "plain" is the default configuration around a handler that does nothing;
// "user" sends X-Grafana-User and X-Query-Tags with -metrics.trust-proxy-headers and
// -server.enable-query-analytics on; the "vl4" variants make 4 VictoriaLogs calls, each logging its
// upstream_request line.
func BenchmarkRequestLogger(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"_time":"2026-11-07T10:00:00Z","_msg":"x"}`+"\n")
	}))
	b.Cleanup(backend.Close)
	for _, tc := range []struct {
		name    string
		user    bool
		vlCalls int
	}{{"plain", false, 0}, {"user", true, 0}, {"plain_vl4", false, 4}, {"user_vl4", true, 4}} {
		b.Run(tc.name, func(b *testing.B) {
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
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
			params := url.Values{"query": {`app:="api"`}, "start": {"1"}, "end": {"2"}}
			h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
				for range tc.vlCalls {
					resp, err := p.vlGet(r.Context(), "/select/logsql/query", params)
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
				}
				w.WriteHeader(http.StatusOK)
			})
			w := httptest.NewRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				h.ServeHTTP(w, req)
			}
		})
	}
}
