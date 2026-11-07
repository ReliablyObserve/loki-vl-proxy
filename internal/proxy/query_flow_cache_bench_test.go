package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// A cache hit read raw, through the flow wrapper without a request, and through it with a request each (the
// wrapper's lock on the shared lookup histogram is the cost under contention).
func BenchmarkCacheHitParallel(b *testing.B) {
	p := &Proxy{metrics: metrics.NewMetrics()}
	c := cache.New(time.Minute, 1000)
	defer c.Close()
	c.SetWithTTL("k", []byte("value"), time.Minute)
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Get("k")
			}
		})
	})
	b.Run("wrapped_no_request", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				p.cacheGet(context.Background(), c, "k")
			}
		})
	})
	b.Run("wrapped_in_request", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			ctx, _ := flowCtx()
			for pb.Next() {
				p.cacheGet(ctx, c, "k")
			}
		})
	})
}
