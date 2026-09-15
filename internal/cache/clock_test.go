package cache

import (
	"testing"
	"time"
)

// A write whose TTL equals the disk minimum TTL is persisted and replaces the
// previous value for the key, in memory and on disk, and both expire with the
// cache clock.
func TestSetWithTTL_AtDiskMinTTLOverwritesAndExpires(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: tempDBPath(t), MinTTL: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c := New(time.Minute, 100)
	defer c.Close()
	c.SetL2(dc)

	c.SetWithTTL("k", []byte("old"), 5*time.Minute)
	dc.Flush()
	c.SetWithTTL("k", []byte("new"), 30*time.Second)
	if v, ok := dc.Get("k"); !ok || string(v) != "new" {
		t.Fatalf("buffered disk value = %q, %v; want new", v, ok)
	}
	dc.Flush()
	if v, ok := dc.Get("k"); !ok || string(v) != "new" {
		t.Fatalf("flushed disk value = %q, %v; want new", v, ok)
	}
	if v, ok := c.Get("k"); !ok || string(v) != "new" {
		t.Fatalf("memory value = %q, %v; want new", v, ok)
	}

	restore := AdvanceClockForTesting(31 * time.Second)
	defer restore()
	if v, ok := c.Get("k"); ok {
		t.Fatalf("value %q still served after its TTL", v)
	}
}
