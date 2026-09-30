package cache

import (
	"path/filepath"
	"testing"
	"time"
)

func TestGetSharedWithAge_ReportsTimeSinceStoreAndUnknownForDisk(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(t.TempDir(), "c.db"), FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dc.Close() })
	c := New(time.Minute, 100)
	c.SetL2(dc)
	t.Cleanup(c.Close)

	// A 30s entry stored now is 0s old whatever TTL the caller expects.
	c.SetLocalAndDiskWithTTL("k", []byte("v"), 30*time.Second)
	restore := AdvanceClockForTesting(7 * time.Second)
	defer restore()
	_, remaining, age, tier, ok := c.GetSharedWithAge("k")
	if !ok || tier != "l1_memory" || age < 7*time.Second || age > 8*time.Second || remaining > 24*time.Second {
		t.Fatalf("l1: ok=%v tier=%s age=%v remaining=%v", ok, tier, age, remaining)
	}

	// A replica with an empty memory tier reads the disk copy: the store time is
	// not kept, so the age is unknown (-1); it stays unknown once promoted.
	other := New(time.Minute, 100)
	other.SetL2(dc)
	t.Cleanup(other.Close)
	_, _, age, tier, ok = other.GetSharedWithAge("k")
	if !ok || tier != "l2_disk" || age != -1 {
		t.Fatalf("disk: ok=%v tier=%s age=%v", ok, tier, age)
	}
	_, _, age, tier, ok = other.GetSharedWithAge("k")
	if !ok || tier != "l1_memory" || age != -1 {
		t.Fatalf("promoted disk copy: ok=%v tier=%s age=%v", ok, tier, age)
	}

	// A value stored locally only (a fresh fill) is stamped.
	other.SetLocalOnlyWithTTL("fresh", []byte("v"), 30*time.Second)
	if _, _, age, _, ok := other.GetSharedWithAge("fresh"); !ok || age < 0 || age > time.Second {
		t.Fatalf("local fill: ok=%v age=%v", ok, age)
	}

	// A read-ahead copy of an owner's value carries only what is left of the
	// owner's TTL: its age is unknown, not zero.
	other.SetShadowWithTTL("readahead", []byte("v"), 30*time.Second)
	if _, _, age, _, ok := other.GetSharedWithAge("readahead"); !ok || age != -1 {
		t.Fatalf("read-ahead copy: ok=%v age=%v, want unknown", ok, age)
	}
}
