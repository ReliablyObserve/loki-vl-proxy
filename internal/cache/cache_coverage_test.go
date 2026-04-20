package cache

import (
	"fmt"
	"testing"
	"time"
)

// =============================================================================
// GetWithTTL — remaining TTL accuracy
// =============================================================================

func TestGetWithTTL_ReturnsRemainingTime(t *testing.T) {
	c := NewWithMaxBytes(10*time.Second, 100, 1024*1024)
	defer c.Close()

	c.SetWithTTL("key1", []byte("val"), 5*time.Second)

	val, ttl, ok := c.GetWithTTL("key1")
	if !ok || string(val) != "val" {
		t.Fatalf("expected hit, got ok=%v val=%q", ok, string(val))
	}
	if ttl <= 0 || ttl > 5*time.Second {
		t.Errorf("expected TTL 0-5s, got %v", ttl)
	}
}

func TestGetWithTTL_Expired(t *testing.T) {
	c := NewWithMaxBytes(1*time.Millisecond, 100, 1024*1024)
	defer c.Close()

	c.Set("expire-me", []byte("val"))
	time.Sleep(5 * time.Millisecond)

	_, ttl, ok := c.GetWithTTL("expire-me")
	if ok {
		t.Errorf("should miss for expired key, ttl=%v", ttl)
	}
}

func TestGetWithTTL_Miss(t *testing.T) {
	c := New(10*time.Second, 100)
	defer c.Close()

	_, ttl, ok := c.GetWithTTL("nonexistent")
	if ok || ttl != 0 {
		t.Errorf("miss should return 0 TTL, got ok=%v ttl=%v", ok, ttl)
	}
}

func TestGetStaleWithTTL_ReturnsExpiredEntry(t *testing.T) {
	c := NewWithMaxBytes(1*time.Millisecond, 100, 1024*1024)
	defer c.Close()

	c.Set("stale", []byte("payload"))
	time.Sleep(5 * time.Millisecond)

	val, ttl, ok := c.GetStaleWithTTL("stale")
	if !ok {
		t.Fatal("expected stale entry to be returned")
	}
	if string(val) != "payload" {
		t.Fatalf("unexpected stale payload %q", string(val))
	}
	if ttl >= 0 {
		t.Fatalf("expected negative remaining TTL for stale entry, got %v", ttl)
	}
}

func TestGetSharedWithTTL_PromotesFreshDiskHitIntoL1(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	c := NewWithMaxBytes(10*time.Second, 100, 1024*1024)
	defer c.Close()
	c.SetL2(dc)

	dc.Set("shared", []byte("payload"), 5*time.Second)
	dc.Flush()

	value, ttl, tier, ok := c.GetSharedWithTTL("shared")
	if !ok {
		t.Fatal("expected shared disk hit")
	}
	if string(value) != "payload" {
		t.Fatalf("unexpected payload %q", value)
	}
	if tier != "l2_disk" {
		t.Fatalf("expected l2_disk tier, got %q", tier)
	}
	if ttl <= 0 {
		t.Fatalf("expected positive TTL, got %v", ttl)
	}
	if promoted, _, ok := c.GetWithTTL("shared"); !ok || string(promoted) != "payload" {
		t.Fatal("expected L2 hit to promote payload into local L1")
	}
	stats := c.Stats()
	if stats.L2.Hits != 1 || stats.L1.Misses != 1 {
		t.Fatalf("unexpected cache tier stats: %+v", stats)
	}
}

func TestGetRecoverableStaleWithTTL_ReturnsDiskStaleValue(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	c := NewWithMaxBytes(10*time.Second, 100, 1024*1024)
	defer c.Close()
	c.SetL2(dc)

	dc.Set("disk-stale", []byte("payload"), 10*time.Millisecond)
	dc.Flush()
	time.Sleep(20 * time.Millisecond)

	value, ttl, tier, ok := c.GetRecoverableStaleWithTTL("disk-stale")
	if !ok {
		t.Fatal("expected stale disk payload")
	}
	if string(value) != "payload" {
		t.Fatalf("unexpected payload %q", value)
	}
	if tier != "l2_disk" {
		t.Fatalf("expected l2_disk tier, got %q", tier)
	}
	if ttl >= 0 {
		t.Fatalf("expected negative TTL, got %v", ttl)
	}
	if stats := c.Stats(); stats.L2.StaleHits != 1 {
		t.Fatalf("expected one stale L2 hit, got %+v", stats)
	}
}

func TestCache_MaxEntrySizeBytes(t *testing.T) {
	c := NewWithMaxBytes(10*time.Second, 100, 1000)
	defer c.Close()

	if got := c.MaxEntrySizeBytes(); got != 100 {
		t.Fatalf("expected 10%% admission cap, got %d", got)
	}
}

// =============================================================================
// Shadow copy TTL preservation
// =============================================================================

func TestShadowCopy_UsesOwnerTTL(t *testing.T) {
	// Owner cache: 60s default TTL
	ownerCache := NewWithMaxBytes(60*time.Second, 100, 1024*1024)
	defer ownerCache.Close()
	ownerCache.SetWithTTL("shared", []byte("data"), 20*time.Second)

	// Non-owner cache: 120s default TTL
	localCache := NewWithMaxBytes(120*time.Second, 100, 1024*1024)
	defer localCache.Close()

	// Simulate L3 fetch: owner returns 20s remaining, local stores with that TTL
	val, remaining, ok := ownerCache.GetWithTTL("shared")
	if !ok {
		t.Fatal("owner should have key")
	}

	// Store shadow with owner's TTL
	localCache.SetWithTTL("shared", val, remaining)

	// Verify shadow TTL is close to owner's (not local's 120s default)
	_, shadowTTL, _ := localCache.GetWithTTL("shared")
	if shadowTTL > 21*time.Second {
		t.Errorf("shadow TTL should be ~20s (from owner), got %v", shadowTTL)
	}
}

// =============================================================================
// LRU eviction edge cases
// =============================================================================

func TestEviction_ByteLimit(t *testing.T) {
	// Max 1000 bytes, 10% guard = 100 bytes max per entry
	c := NewWithMaxBytes(60*time.Second, 1000, 1000)
	defer c.Close()

	c.Set("k1", make([]byte, 90)) // 90 < 100 (10% of 1000)
	c.Set("k2", make([]byte, 90))
	// k1 + k2 = 180 bytes, fits

	// Fill to trigger eviction
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("fill-%d", i), make([]byte, 90))
	}
	// Total would be 1080 > 1000, early entries evicted

	entries, bytes := c.Size()
	if bytes > 1000 {
		t.Errorf("bytes %d exceeds max 1000", bytes)
	}
	_ = entries
}

func TestEviction_PromotePreventsEviction(t *testing.T) {
	c := NewWithMaxBytes(60*time.Second, 3, 1024*1024)
	defer c.Close()

	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))

	// Access "a" to promote it
	c.Get("a")

	// Add "d" — should evict "b" (LRU, since "a" was promoted)
	c.Set("d", []byte("4"))

	if _, ok := c.Get("a"); !ok {
		t.Error("'a' was promoted and should survive eviction")
	}
	if _, ok := c.Get("b"); ok {
		t.Error("'b' should be evicted (LRU)")
	}
}

func TestEviction_ExpiredRemovedFirst(t *testing.T) {
	c := NewWithMaxBytes(1*time.Millisecond, 2, 1024*1024)
	defer c.Close()

	c.Set("old", []byte("1"))
	time.Sleep(5 * time.Millisecond) // let it expire

	c.SetWithTTL("new1", []byte("2"), 60*time.Second)
	c.SetWithTTL("new2", []byte("3"), 60*time.Second) // triggers eviction

	if _, ok := c.Get("old"); ok {
		t.Error("expired 'old' should be evicted first")
	}
}

// =============================================================================
// Invalidation
// =============================================================================

func TestInvalidatePrefix_RemovesMatching(t *testing.T) {
	c := New(60*time.Second, 100)
	defer c.Close()

	c.Set("query:a", []byte("1"))
	c.Set("query:b", []byte("2"))
	c.Set("labels:x", []byte("3"))

	c.InvalidatePrefix("query:")

	if _, ok := c.Get("query:a"); ok {
		t.Error("query:a should be invalidated")
	}
	if _, ok := c.Get("labels:x"); !ok {
		t.Error("labels:x should survive")
	}
}

func TestInvalidate_NonExistent(t *testing.T) {
	c := New(60*time.Second, 100)
	defer c.Close()
	c.Invalidate("nope") // should not panic
}

// =============================================================================
// Size tracking
// =============================================================================

func TestSize_TracksCorrectly(t *testing.T) {
	c := New(60*time.Second, 100)
	defer c.Close()

	c.Set("k1", []byte("hello")) // 5 bytes
	c.Set("k2", []byte("world")) // 5 bytes

	entries, bytes := c.Size()
	if entries != 2 {
		t.Errorf("expected 2 entries, got %d", entries)
	}
	if bytes != 10 {
		t.Errorf("expected 10 bytes, got %d", bytes)
	}

	c.Invalidate("k1")
	entries, bytes = c.Size()
	if entries != 1 || bytes != 5 {
		t.Errorf("after invalidate: entries=%d bytes=%d (expected 1, 5)", entries, bytes)
	}
}

// =============================================================================
// Cleanup goroutine
// =============================================================================

func TestCleanup_RemovesExpired(t *testing.T) {
	// Short TTL + manual trigger
	c := NewWithMaxBytes(10*time.Millisecond, 100, 1024*1024)
	c.Set("temp", []byte("data"))

	time.Sleep(50 * time.Millisecond)

	// After cleanup tick (30s default, but we can check manually)
	_, ok := c.Get("temp")
	if ok {
		t.Error("expired entry should be gone after Get")
	}
	c.Close()
}

func TestClose_Idempotent(t *testing.T) {
	c := New(60*time.Second, 100)
	c.Close()
	c.Close() // should not panic
}

// =============================================================================
// Large entries rejected
// =============================================================================

func TestSet_RejectsOversized(t *testing.T) {
	// Max 100 bytes, entry > 10% = 10 bytes
	c := NewWithMaxBytes(60*time.Second, 100, 100)
	defer c.Close()

	c.Set("big", make([]byte, 50)) // 50 > 10 (10% of 100)

	if _, ok := c.Get("big"); ok {
		t.Error("oversized entry should be rejected (>10% of maxBytes)")
	}
}

// =============================================================================
// Replace existing key
// =============================================================================

func TestSet_ReplaceExisting(t *testing.T) {
	c := New(60*time.Second, 100)
	defer c.Close()

	c.Set("k", []byte("v1"))
	c.Set("k", []byte("v2-longer"))

	val, ok := c.Get("k")
	if !ok || string(val) != "v2-longer" {
		t.Errorf("expected v2-longer, got %q", string(val))
	}

	_, bytes := c.Size()
	if bytes != len("v2-longer") {
		t.Errorf("bytes should reflect replacement, got %d", bytes)
	}
}

// =============================================================================
// Stats
// =============================================================================

func TestStats_HitMiss(t *testing.T) {
	c := New(60*time.Second, 100)
	defer c.Close()

	c.Set("x", []byte("1"))
	c.Get("x")    // hit
	c.Get("nope") // miss

	if c.Hits.Load() < 1 {
		t.Error("expected at least 1 hit")
	}
	if c.Misses.Load() < 1 {
		t.Error("expected at least 1 miss")
	}
}
