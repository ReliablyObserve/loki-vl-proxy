package cache

import (
	"fmt"
	"testing"
	"time"
)

// TestDiskCache_OverwriteAccountingDoesNotDoubleCountBytes guards the
// fix for Flush() charging currentBytes without deducting the old entry
// size on overwrites. Before the fix, overwriting the same key would
// count its bytes twice, causing the cap to trigger prematurely.
func TestDiskCache_OverwriteAccountingDoesNotDoubleCountBytes(t *testing.T) {
	// Use 64 KiB cap; each payload is 8 KiB, so the cap comfortably
	// holds 8 fresh entries but would choke after 1-2 overwrites if
	// the old fix was in place.
	const maxBytes = 64 * 1024
	const payloadSize = 8 * 1024
	payload := make([]byte, payloadSize)

	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// Write and flush the same key 6 times. Net disk usage = 1 entry
	// (payloadSize + bbolt overhead). Without the fix, Flush() would
	// accumulate 6×payloadSize towards the cap and start rejecting writes.
	for i := range 6 {
		dc.Set("overwrite-me", payload, time.Hour)
		dc.Flush()
		_ = i
	}

	// Now write 4 distinct keys that each fit within the cap.
	for i := range 4 {
		dc.Set(fmt.Sprintf("fresh-%d", i), payload, time.Hour)
	}
	dc.Flush()

	// All 4 fresh keys must be admitted.
	admitted := 0
	for i := range 4 {
		if _, ok := dc.Get(fmt.Sprintf("fresh-%d", i)); ok {
			admitted++
		}
	}
	if admitted < 4 {
		t.Errorf("overwrite accounting bug: only %d/4 fresh entries admitted after repeated overwrites; cap was poisoned", admitted)
	}
}

// TestDiskCache_ExpiredEntryReclaimedOnRead guards the lazy-eviction fix:
// reading an expired disk entry should delete it from bolt so subsequent
// Size() calls reflect real (live) usage instead of dead bytes.
func TestDiskCache_ExpiredEntryReclaimedOnRead(t *testing.T) {
	const maxBytes = 32 * 1024
	const payloadSize = 12 * 1024

	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// Write a key that will expire soon.
	dc.Set("expires-soon", make([]byte, payloadSize), 40*time.Millisecond)
	dc.Flush()

	// Wait for expiry, then trigger the lazy delete by reading the key.
	time.Sleep(60 * time.Millisecond)
	_, ok := dc.Get("expires-soon")
	if ok {
		t.Fatal("expected miss for expired entry")
	}

	// Give the background goroutine time to delete from bolt.
	time.Sleep(50 * time.Millisecond)

	// Now write two fresh keys. Without the fix the dead bytes from the
	// expired entry remain and the cap triggers prematurely.
	dc.Set("fresh-a", make([]byte, payloadSize), time.Hour)
	dc.Set("fresh-b", make([]byte, payloadSize), time.Hour)
	dc.Flush()

	hitA, _ := dc.Get("fresh-a")
	hitB, _ := dc.Get("fresh-b")
	if hitA == nil || hitB == nil {
		t.Errorf("expired entry bytes not reclaimed: fresh-a=%v fresh-b=%v — cap was poisoned by dead space", hitA != nil, hitB != nil)
	}
}

// TestDiskCache_MaxBytesSteadyStateLongRun simulates a long-running deployment
// writing the same working set repeatedly and verifies the cache remains
// operational (does not stop admitting entries) across many flush cycles.
func TestDiskCache_MaxBytesSteadyStateLongRun(t *testing.T) {
	const (
		maxBytes    = 128 * 1024
		payloadSize = 4 * 1024
		numKeys     = 8  // 8×4KB = 32KB working set, fits within 128KB cap
		numCycles   = 20 // simulate 20 reload cycles
	)

	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	payload := make([]byte, payloadSize)

	for cycle := range numCycles {
		for i := range numKeys {
			dc.Set(fmt.Sprintf("key-%d", i), payload, time.Hour)
		}
		dc.Flush()

		// After every flush, all working-set keys must still be readable.
		missing := 0
		for i := range numKeys {
			if _, ok := dc.Get(fmt.Sprintf("key-%d", i)); !ok {
				missing++
			}
		}
		if missing > 0 {
			t.Fatalf("cycle %d: %d/%d working-set entries missing — cap self-poisoned after repeated overwrites", cycle, missing, numKeys)
		}
	}
}
