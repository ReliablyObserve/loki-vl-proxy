package cache

import (
	"testing"
	"time"
)

func TestCache_SetGet(t *testing.T) {
	c := New(1*time.Second, 100)
	c.Set("key1", []byte("value1"))
	v, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(v) != "value1" {
		t.Errorf("expected value1, got %s", v)
	}
}

func TestCache_Miss(t *testing.T) {
	c := New(1*time.Second, 100)
	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected cache miss")
	}
}

func TestCache_TTLExpiry(t *testing.T) {
	c := New(50*time.Millisecond, 100)
	c.Set("key1", []byte("value1"))

	// Should hit
	_, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected hit before expiry")
	}

	time.Sleep(60 * time.Millisecond)

	// Should miss after TTL
	_, ok = c.Get("key1")
	if ok {
		t.Error("expected miss after TTL expiry")
	}
}

func TestCache_SetWithTTL(t *testing.T) {
	c := New(1*time.Hour, 100) // long default TTL
	c.SetWithTTL("short", []byte("data"), 50*time.Millisecond)

	_, ok := c.Get("short")
	if !ok {
		t.Fatal("expected hit")
	}

	time.Sleep(60 * time.Millisecond)
	_, ok = c.Get("short")
	if ok {
		t.Error("expected miss after custom TTL")
	}
}

func TestCache_MaxEntries_Eviction(t *testing.T) {
	c := New(1*time.Hour, 5)

	for i := range 10 {
		c.Set(string(rune('a'+i)), []byte("data"))
	}

	entries, _ := c.Size()
	if entries > 5 {
		t.Errorf("expected <=5 entries after eviction, got %d", entries)
	}
}

func TestCache_MaxBytes_Eviction(t *testing.T) {
	c := NewWithMaxBytes(1*time.Hour, 1000, 100) // 100 bytes max

	// Insert 50-byte values
	c.Set("a", make([]byte, 50))
	c.Set("b", make([]byte, 50))

	_, bytes := c.Size()
	if bytes > 100 {
		t.Errorf("expected <=100 bytes, got %d", bytes)
	}
}

func TestCache_Invalidate(t *testing.T) {
	c := New(1*time.Hour, 100)
	c.Set("key1", []byte("data"))
	c.Set("key2", []byte("data"))

	c.Invalidate("key1")
	_, ok := c.Get("key1")
	if ok {
		t.Error("expected miss after invalidate")
	}

	_, ok = c.Get("key2")
	if !ok {
		t.Error("expected hit for non-invalidated key")
	}
}

func TestCache_InvalidatePrefix(t *testing.T) {
	c := New(1*time.Hour, 100)
	c.Set("labels:start=1", []byte("data"))
	c.Set("labels:start=2", []byte("data"))
	c.Set("series:match=x", []byte("data"))

	c.InvalidatePrefix("labels:")

	_, ok := c.Get("labels:start=1")
	if ok {
		t.Error("expected miss after prefix invalidation")
	}
	_, ok = c.Get("labels:start=2")
	if ok {
		t.Error("expected miss after prefix invalidation")
	}
	_, ok = c.Get("series:match=x")
	if !ok {
		t.Error("expected hit for non-matching prefix")
	}
}

func TestCache_Size(t *testing.T) {
	c := New(1*time.Hour, 100)
	c.Set("a", []byte("12345"))
	c.Set("b", []byte("67890"))

	entries, bytes := c.Size()
	if entries != 2 {
		t.Errorf("expected 2 entries, got %d", entries)
	}
	if bytes != 10 {
		t.Errorf("expected 10 bytes, got %d", bytes)
	}
}

func TestCache_Stats(t *testing.T) {
	c := New(1*time.Hour, 100)
	c.Set("key", []byte("val"))

	c.Get("key")    // hit
	c.Get("miss")   // miss

	if c.Hits.Load() != 1 {
		t.Errorf("expected 1 hit, got %d", c.Hits.Load())
	}
	if c.Misses.Load() != 1 {
		t.Errorf("expected 1 miss, got %d", c.Misses.Load())
	}
}

func TestCache_LargeValueRejected(t *testing.T) {
	c := NewWithMaxBytes(1*time.Hour, 100, 1000) // 1KB max

	// Value >10% of max (>100 bytes) should be rejected
	c.Set("large", make([]byte, 200))
	_, ok := c.Get("large")
	if ok {
		t.Error("expected large value to be rejected (>10% of maxBytes)")
	}
}

func TestCache_EvictionStats(t *testing.T) {
	c := New(50*time.Millisecond, 3)

	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))

	// Wait for expiry
	time.Sleep(60 * time.Millisecond)

	// This should trigger eviction of expired entries
	c.Set("d", []byte("4"))
	c.Set("e", []byte("5"))
	c.Set("f", []byte("6"))

	entries, _ := c.Size()
	if entries > 3 {
		t.Errorf("expected <=3, got %d", entries)
	}
}

func TestCache_EvictWhenFullNoExpired(t *testing.T) {
	c := New(1*time.Hour, 3) // long TTL, small max

	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))

	// Force eviction — no expired entries, so random eviction happens
	c.Set("d", []byte("4"))

	entries, _ := c.Size()
	if entries > 3 {
		t.Errorf("expected <=3 after forced eviction, got %d", entries)
	}

	// New key should be present
	_, ok := c.Get("d")
	if !ok {
		t.Error("expected new key 'd' to be present")
	}
}

func TestCache_ReplaceExistingKey(t *testing.T) {
	c := New(1*time.Hour, 100)
	c.Set("key", []byte("old"))
	c.Set("key", []byte("new"))

	v, ok := c.Get("key")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(v) != "new" {
		t.Errorf("expected 'new', got %q", v)
	}

	entries, bytes := c.Size()
	if entries != 1 {
		t.Errorf("expected 1 entry (replaced), got %d", entries)
	}
	if bytes != 3 {
		t.Errorf("expected 3 bytes (len('new')), got %d", bytes)
	}
}

func TestCache_SetL2_Fallback(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// L1 has short TTL (50ms), L2 has long TTL (1h via DiskCache.Set)
	c := New(50*time.Millisecond, 100)
	defer c.Close()
	c.SetL2(dc)

	// Write directly to L2 with a long TTL to simulate L1 eviction
	dc.Set("key1", []byte("hello"), 1*time.Hour)
	dc.Flush()

	// L1 doesn't have it, L2 should
	v, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected L2 fallback hit")
	}
	if string(v) != "hello" {
		t.Errorf("expected hello, got %q", v)
	}
}

func TestCache_Close(t *testing.T) {
	c := New(1*time.Hour, 100)
	c.Set("k", []byte("v"))
	c.Close()
	// After close, cache still works for reads (just cleanup goroutine stopped)
	v, ok := c.Get("k")
	if !ok || string(v) != "v" {
		t.Error("expected cache to still be readable after Close")
	}
	// Double close should not panic
	c.Close()
}
