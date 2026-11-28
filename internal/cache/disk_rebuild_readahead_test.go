package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

// rebuildState is what the start-up index rebuild leaves: every indexed key with
// its charged size, expiry and recency order, the byte total, the evictions and
// the records still in the file.
type rebuildState struct {
	entries   []string
	bytes     int64
	evictions int64
	stored    int
}

func readRebuildState(t *testing.T, path string, readahead bool) rebuildState {
	t.Helper()
	dc, err := NewDiskCache(DiskCacheConfig{Path: path, Compression: true, Codec: "s2", FlushInterval: time.Hour, MaxEntries: 150, RebuildReadahead: readahead})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()
	var s rebuildState
	dc.writeMu.Lock()
	for e := dc.newest; e != nil; e = e.next {
		s.entries = append(s.entries, fmt.Sprintf("%s/%d/%d", e.key, e.size, e.expiresAt))
	}
	s.bytes = dc.logicalBytes
	dc.writeMu.Unlock()
	s.evictions = dc.Evictions.Load()
	s.stored, _ = dc.Size()
	return s
}

// The read-ahead only reads: a rebuild with it leaves exactly the index, byte
// total, evictions (expired and over capacity) and stored records a rebuild
// without it leaves, from identical copies of one file.
//
// conformance: cache-restart-handover
func TestRebuildReadahead_LeavesTheSameIndex(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dc, err := NewDiskCache(DiskCacheConfig{Path: src, Compression: true, Codec: "s2", FlushInterval: time.Hour, MaxEntries: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		ttl := time.Hour
		if i%7 == 0 {
			ttl = time.Minute // expired at the rebuild below
		}
		dc.Set(fmt.Sprintf("stats-extent:v1:%03d:%d", i%40, i), make([]byte, 512+(i*997)%(64<<10)), ttl)
	}
	dc.Flush()
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	restore := AdvanceClockForTesting(10 * time.Minute)
	defer restore()
	var states []rebuildState
	for _, readahead := range []bool{false, true} {
		path := filepath.Join(dir, fmt.Sprintf("copy-%t.db", readahead))
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		states = append(states, readRebuildState(t, path, readahead))
	}
	off, on := states[0], states[1]
	// 29 of 200 expired, then the capacity of 150 drops the rest beyond it.
	if len(off.entries) != 150 || off.evictions != 50 || off.stored != 150 {
		t.Fatalf("without read-ahead: %d entries, %d evictions, %d stored", len(off.entries), off.evictions, off.stored)
	}
	if !slices.Equal(off.entries, on.entries) || off.bytes != on.bytes || off.evictions != on.evictions || off.stored != on.stored {
		t.Fatalf("read-ahead changed the rebuild: %d/%d/%d/%d entries/bytes/evictions/stored, without %d/%d/%d/%d",
			len(on.entries), on.bytes, on.evictions, on.stored, len(off.entries), off.bytes, off.evictions, off.stored)
	}
}

// The read-ahead reads the whole file when it is given the time, stops at once
// when stopped, joins every goroutine, and is a no-op for a file it cannot open.
//
// conformance: cache-restart-handover
func TestRebuildReadahead_ReadsTheFileAndStops(t *testing.T) {
	before := runtime.NumGoroutine()
	path := filepath.Join(t.TempDir(), "f")
	size := int64(5*restoreReadaheadBuffer + 123)
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	stop := startRebuildReadahead(path)
	deadline := time.Now().Add(10 * time.Second)
	// Wait for the producer and readers to finish on their own.
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := stop(); got != size {
		t.Fatalf("read %d of %d bytes", got, size)
	}
	big := filepath.Join(t.TempDir(), "big")
	if err := os.WriteFile(big, make([]byte, 256*restoreReadaheadBuffer), 0o600); err != nil {
		t.Fatal(err)
	}
	stop = startRebuildReadahead(big)
	if got := stop(); got > int64(2*restoreReadaheadReaders)*restoreReadaheadBuffer {
		t.Fatalf("stopped at once but read %d bytes", got)
	}
	if got := startRebuildReadahead(filepath.Join(t.TempDir(), "missing"))(); got != 0 {
		t.Fatalf("missing file: read %d bytes", got)
	}
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, %d before the read-ahead", runtime.NumGoroutine(), before)
		}
	}
}
