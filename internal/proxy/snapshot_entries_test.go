package proxy

import (
	"sync"
	"testing"
)

// TestSnapshotEntriesForPatterns_BreaksAliasing locks in the fix for the
// production race fatal:
//
//	concurrent map read and map write
//	extractLogPatternsFromWindowEntriesWithStats (postprocess.go:390)
//	maybeAutodetectPatternsFromWindowEntries     (patterns.go:1207)
//	proxyLogQueryWindowed                        (query_range_windowing.go:235)
//
// applyStreamLabelMutations aliases the descriptor cache's translatedLabels
// map when no drop/keep change applies, so the spawned autodetect goroutine
// read entry.Stream["detected_level"] while the main thread's
// applyDerivedFields wrote into the same map. snapshotEntriesForPatterns
// must produce a slice whose Stream maps are SEPARATE allocations — mutating
// the originals or the snapshots in isolation must not bleed into the other.
func TestSnapshotEntriesForPatterns_BreaksAliasing(t *testing.T) {
	original := []queryRangeWindowEntry{
		{
			Stream: map[string]string{"detected_level": "error", "level": "warn", "app": "api"},
			Ts:     "1700000000000000000",
			Msg:    "boom",
		},
		{
			Stream: map[string]string{"level": "info", "app": "api"},
			Ts:     "1700000060000000000",
			Msg:    "ok",
		},
	}

	snap := snapshotEntriesForPatterns(original)
	if len(snap) != len(original) {
		t.Fatalf("snapshot length: want %d, got %d", len(original), len(snap))
	}

	// Mutating the original Stream maps must NOT leak into the snapshot.
	for i := range original {
		original[i].Stream["detected_level"] = "MUTATED"
		original[i].Stream["level"] = "MUTATED"
		original[i].Stream["new_key"] = "INJECTED"
	}

	if got := snap[0].Stream["detected_level"]; got != "error" {
		t.Errorf("snapshot[0].Stream[detected_level]: want %q (pre-mutation), got %q", "error", got)
	}
	if got := snap[1].Stream["level"]; got != "info" {
		t.Errorf("snapshot[1].Stream[level]: want %q (pre-mutation), got %q", "info", got)
	}
	if _, exists := snap[0].Stream["new_key"]; exists {
		t.Error("snapshot[0].Stream must not see post-snapshot inserts")
	}

	// Conversely, mutating snapshot must NOT corrupt the original
	// (it shouldn't matter for the goroutine, but locks in the boundary).
	snap[0].Stream["detected_level"] = "SNAPSHOT_MUTATED"
	if got := original[0].Stream["detected_level"]; got == "SNAPSHOT_MUTATED" {
		t.Error("snapshot mutation leaked back into original Stream map")
	}

	// Verify only the keys the autodetect path reads are present. The full
	// label set (e.g. "app") must NOT bloat the snapshot — that's the whole
	// reason this helper exists instead of cloning the full map.
	for i, e := range snap {
		for k := range e.Stream {
			if k != "detected_level" && k != "level" {
				t.Errorf("snap[%d].Stream contains unexpected key %q", i, k)
			}
		}
	}
}

// TestSnapshotEntriesForPatterns_RaceFreeUnderConcurrentMutation reproduces
// the production race condition locally — without the fix, this test fails
// with `go test -race` ("concurrent map read and map write"). With the fix,
// the snapshot is a fresh allocation so the original map can be freely
// mutated by a sibling goroutine.
func TestSnapshotEntriesForPatterns_RaceFreeUnderConcurrentMutation(t *testing.T) {
	// Build entries that ALL share the same underlying Stream map — exactly
	// the production aliasing pattern (descriptor cache returns the same map
	// for every entry from the same _stream within a window).
	sharedStream := map[string]string{
		"detected_level": "error",
		"level":          "warn",
		"app":            "api",
	}
	entries := make([]queryRangeWindowEntry, 200)
	for i := range entries {
		entries[i] = queryRangeWindowEntry{
			Stream: sharedStream,
			Ts:     "1700000000000000000",
			Msg:    "line",
		}
	}

	// Take the snapshot BEFORE spawning the mutator (this is what
	// proxyLogQueryWindowed now does). The pattern-autodetect goroutine sees
	// the snapshot; the main thread mutates the shared original map.
	snap := snapshotEntriesForPatterns(entries)

	var wg sync.WaitGroup
	wg.Add(2)

	// Reader goroutine: reads the snapshot map (no race possible — fresh alloc).
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			for j := range snap {
				_ = snap[j].Stream["detected_level"]
				_ = snap[j].Stream["level"]
			}
		}
	}()

	// Writer goroutine: simulates applyDerivedFields writing into the
	// descriptor cache's map (the original aliased map).
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			sharedStream["traceID"] = "abc"
			sharedStream["spanID"] = "def"
			delete(sharedStream, "traceID")
		}
	}()

	wg.Wait()
}
