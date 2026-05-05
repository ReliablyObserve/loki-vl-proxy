package cache

import (
	"fmt"
	"testing"
	"time"
)

func TestLRU_EvictsOldestAccessed(t *testing.T) {
	c := New(60*time.Second, 3) // max 3 entries

	c.SetWithTTL("a", []byte("1"), 60*time.Second)
	c.SetWithTTL("b", []byte("2"), 60*time.Second)
	c.SetWithTTL("c", []byte("3"), 60*time.Second)

	// Access "a" to make it recently used
	c.Get("a")
	c.drainPromotions() // flush deferred LRU before triggering eviction

	// Add "d" — should evict "b" (least recently used), not "a"
	c.SetWithTTL("d", []byte("4"), 60*time.Second)

	if _, ok := c.Get("a"); !ok {
		t.Error("'a' should survive (recently accessed)")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("'d' should exist (just added)")
	}
	// "b" should be evicted (LRU)
	if _, ok := c.Get("b"); ok {
		t.Error("'b' should be evicted (least recently used)")
	}
}

func TestLRU_GetPromotesToFront(t *testing.T) {
	c := New(60*time.Second, 3)

	c.SetWithTTL("a", []byte("1"), 60*time.Second)
	c.SetWithTTL("b", []byte("2"), 60*time.Second)
	c.SetWithTTL("c", []byte("3"), 60*time.Second)

	// Access all in order: a, b, c — "a" is now most recently used at last access
	c.Get("a")
	c.drainPromotions() // flush deferred LRU before triggering eviction

	// Insert 2 new entries to force 2 evictions
	c.SetWithTTL("d", []byte("4"), 60*time.Second)
	c.SetWithTTL("e", []byte("5"), 60*time.Second)

	// "a" was accessed last among the original 3, should still be present
	if _, ok := c.Get("a"); !ok {
		t.Error("'a' should survive (most recently accessed of originals)")
	}
}

func TestLRU_SetUpdatesAccessTime(t *testing.T) {
	c := New(60*time.Second, 3)

	c.SetWithTTL("a", []byte("1"), 60*time.Second)
	c.SetWithTTL("b", []byte("2"), 60*time.Second)
	c.SetWithTTL("c", []byte("3"), 60*time.Second)

	// Re-set "a" with new value — promotes it
	c.SetWithTTL("a", []byte("updated"), 60*time.Second)

	// Add "d" — should evict "b" (oldest untouched)
	c.SetWithTTL("d", []byte("4"), 60*time.Second)

	if v, ok := c.Get("a"); !ok {
		t.Error("'a' should survive (re-set promotes)")
	} else if string(v) != "updated" {
		t.Errorf("'a' should have updated value, got %q", string(v))
	}
}

func TestLRU_ManyEntries_StableMemory(t *testing.T) {
	c := New(60*time.Second, 100) // max 100

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		c.SetWithTTL(key, []byte(fmt.Sprintf("value-%d", i)), 60*time.Second)
	}

	// Should have at most ~100 entries (with some eviction overhead)
	size, _ := c.Size()
	if size > 120 {
		t.Errorf("cache should have ~100 entries, got %d", size)
	}
	if size < 80 {
		t.Errorf("cache should have ~100 entries, got %d (too aggressive eviction)", size)
	}
}
