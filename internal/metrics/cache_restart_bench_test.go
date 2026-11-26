package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkMetricsRenderCacheStats renders /metrics with a cache behind it, so
// the restart hand-over's series are written; cache-series counts them.
func BenchmarkMetricsRenderCacheStats(b *testing.B) {
	m := populatedMetrics(false)
	m.SetCacheStatsProvider(func() cache.StatsSnapshot { return cache.StatsSnapshot{} })
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	b.ReportAllocs()
	var body string
	for b.Loop() {
		rec := httptest.NewRecorder()
		m.Handler(rec, req)
		body = rec.Body.String()
	}
	b.ReportMetric(float64(len(body)), "body-bytes")
	b.ReportMetric(float64(strings.Count(body, "\nloki_vl_proxy_cache_restart_")), "restart-series")
}
