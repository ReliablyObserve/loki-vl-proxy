package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkCacheReadWrapper is one cache read of a client request, through the proxy's wrapper that records it in
// the request's flow: a memory hit, a miss of memory and disk, and a memory-only peek.
func BenchmarkCacheReadWrapper(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Hour, 100), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: b.TempDir() + "/l2.db", FlushInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dc.Close() })
	disk := cache.New(time.Hour, 100)
	b.Cleanup(disk.Close)
	disk.SetL2(dc)
	p.cache.SetWithTTL("query_range:tenant:hit", []byte("result"), time.Hour)
	ctx := context.WithValue(context.Background(), requestTelemetryKey, newRequestTelemetry())
	for _, tc := range []struct {
		name string
		read func()
	}{
		{"MemoryHit", func() { p.cacheGet(ctx, p.cache, "query_range:tenant:hit") }},
		{"MemoryDiskMiss", func() { p.cacheGet(ctx, disk, "query_range:tenant:miss") }},
		{"Peek", func() { p.cachePeek(ctx, p.cache, "query_range:tenant:hit") }},
		{"NoRequest", func() { p.cacheGet(context.Background(), p.cache, "query_range:tenant:hit") }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tc.read()
			}
		})
	}
}
