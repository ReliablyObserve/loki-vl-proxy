package proxy

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newOpt1Proxy(t testing.TB) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:        "http://unused",
		Cache:             cache.New(30*time.Second, 100),
		LogLevel:          "error",
		MetadataFieldMode: MetadataFieldModeHybrid,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

// TestOPT1_ExposureCachePopulatesOnFirstCall verifies that calling
// metadataFieldExposuresCached with a non-nil map stores the result so
// subsequent calls return the cached value without recomputing.
func TestOPT1_ExposureCachePopulatesOnFirstCall(t *testing.T) {
	p := newOpt1Proxy(t)
	cache := make(map[string][]metadataFieldExposure, 4)

	// First call — cache miss, must populate.
	got1 := p.metadataFieldExposuresCached("service.name", cache)
	if len(cache) == 0 {
		t.Fatal("cache not populated after first call — OPT-1 regression: nil-check is still len()==0")
	}
	cached, ok := cache["service.name"]
	if !ok {
		t.Fatal("service.name not stored in cache after first call")
	}

	// Second call — must hit cache (same pointer/content).
	got2 := p.metadataFieldExposuresCached("service.name", cache)
	if len(got1) != len(got2) {
		t.Fatalf("cache hit returned different length: first=%d second=%d", len(got1), len(got2))
	}
	for i := range got1 {
		if got1[i].name != got2[i].name {
			t.Fatalf("cache hit returned different exposure[%d]: first=%q second=%q", i, got1[i].name, got2[i].name)
		}
	}
	_ = cached
}

// TestOPT1_NilCacheBypassesStore verifies that a nil cache falls through to
// direct computation without panicking (used by callers that opt out of caching).
func TestOPT1_NilCacheBypassesStore(t *testing.T) {
	p := newOpt1Proxy(t)
	got := p.metadataFieldExposuresCached("service.name", nil)
	// Should not panic and should return non-empty exposures in hybrid mode.
	if len(got) == 0 {
		t.Fatal("nil-cache path returned empty exposures for service.name in hybrid mode")
	}
}

// TestOPT1_NCallsSameFieldOnlyCompputesOnce verifies that N calls with the same
// field and the same non-nil cache result in exactly 1 unique cache entry (O(1)
// computation after the first call).
func TestOPT1_NCallsSameFieldOnlyComputesOnce(t *testing.T) {
	p := newOpt1Proxy(t)
	c := make(map[string][]metadataFieldExposure, 4)
	const N = 100
	for i := 0; i < N; i++ {
		p.metadataFieldExposuresCached("level", c)
	}
	if len(c) != 1 {
		t.Fatalf("expected exactly 1 cache entry after %d calls with same field, got %d", N, len(c))
	}
}

// BenchmarkOPT1_ExposureCache_CacheHit measures the O(1) map-lookup fast path.
// Threshold: should be well under 500 ns/op on any modern hardware.
func BenchmarkOPT1_ExposureCache_CacheHit(b *testing.B) {
	p := newOpt1Proxy(b)
	c := make(map[string][]metadataFieldExposure, 4)
	_ = p.metadataFieldExposuresCached("service.name", c) // warm
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.metadataFieldExposuresCached("service.name", c)
	}
}

// BenchmarkOPT1_ExposureCache_CacheMiss measures the slow path (nil cache = pre-fix baseline).
func BenchmarkOPT1_ExposureCache_CacheMiss(b *testing.B) {
	p := newOpt1Proxy(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.metadataFieldExposuresCached("service.name", nil)
	}
}
