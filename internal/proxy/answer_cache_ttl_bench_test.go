package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkAnswerCacheTTLReadCacheKey measures the canonical read cache key of
// a fenced class (index_stats) on a memo hit and on the computed path (two
// extra parts bypass the memo), with the class unset and shortened.
func BenchmarkAnswerCacheTTLReadCacheKey(b *testing.B) {
	for _, tc := range []struct {
		name string
		ttls map[string]time.Duration
	}{{"unset", nil}, {"shortened", map[string]time.Duration{"index_stats": 5 * time.Second}}} {
		p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", AnswerCacheTTLs: tc.ttls})
		if err != nil {
			b.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=1700000000000000000&end=1700003600000000000", nil)
		b.Run(tc.name+"/memo_hit", func(b *testing.B) {
			p.canonicalReadCacheKey("index_stats", "0", r)
			b.ReportAllocs()
			for b.Loop() {
				_ = p.canonicalReadCacheKey("index_stats", "0", r)
			}
		})
		b.Run(tc.name+"/computed", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.canonicalReadCacheKey("index_stats", "0", r, "a", "b")
			}
		})
		_ = p.Shutdown(context.Background())
	}
}
