package cache

import (
	"testing"
	"time"
)

// TestPeekReadsFreshMemoryEntriesOnly: Peek answers a fresh memory entry and
// misses an absent or expired one.
func TestPeekReadsFreshMemoryEntriesOnly(t *testing.T) {
	c := New(time.Minute, 10)
	defer c.Close()
	c.SetWithTTL("a", []byte("1"), time.Minute)
	c.SetWithTTL("b", []byte("2"), time.Second)
	if v, ok := c.Peek("a"); !ok || string(v) != "1" {
		t.Fatalf("fresh entry: %q %t", v, ok)
	}
	if _, ok := c.Peek("missing"); ok {
		t.Fatal("absent entry found")
	}
	restore := AdvanceClockForTesting(2 * time.Second)
	defer restore()
	if _, ok := c.Peek("b"); ok {
		t.Fatal("expired entry found")
	}
	var nilCache *Cache
	if _, ok := nilCache.Peek("a"); ok {
		t.Fatal("nil cache found an entry")
	}
}
