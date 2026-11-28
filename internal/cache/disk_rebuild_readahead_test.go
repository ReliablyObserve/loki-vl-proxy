package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
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

// walking stands for the rebuild's walk: it passes perMillisecond stored bytes
// every millisecond until the test ends.
func walking(t *testing.T, perMillisecond int64) *atomic.Int64 {
	t.Helper()
	var walked atomic.Int64
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				walked.Add(perMillisecond)
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() { close(done); <-stopped })
	return &walked
}

// The read-ahead reads the whole file when it is given the time, stops at once
// when stopped, joins every goroutine, and is a no-op for a file it cannot open.
//
// conformance: cache-restart-handover
func TestRebuildReadahead_ReadsTheFileAndStops(t *testing.T) {
	walked := walking(t, 1)
	before := runtime.NumGoroutine()
	path := filepath.Join(t.TempDir(), "f")
	size := int64(5*restoreReadaheadBuffer + 123)
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	ra := startRebuildReadahead(path, walked)
	<-ra.done // ends on its own once the file is read
	if got, early := ra.stop(); got != size || early {
		t.Fatalf("read %d of %d bytes, stopped early %t", got, size, early)
	}
	big := filepath.Join(t.TempDir(), "big")
	if err := os.WriteFile(big, make([]byte, 256*restoreReadaheadBuffer), 0o600); err != nil {
		t.Fatal(err)
	}
	// Each reader finishes at most the read it is in when stop is called;
	// stopped at once, it reads far less than the file.
	if got, _ := startRebuildReadahead(big, walked).stop(); got >= 256*restoreReadaheadBuffer {
		t.Fatalf("stopped at once but read the whole file (%d bytes)", got)
	}
	if got, _ := startRebuildReadahead(filepath.Join(t.TempDir(), "missing"), walked).stop(); got != 0 {
		t.Fatalf("missing file: read %d bytes", got)
	}
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, %d before the read-ahead", runtime.NumGoroutine(), before)
		}
	}
}

// Once it has read its probe the read-ahead decides once: it goes on to the end
// of the file while the walk alone passed stored bytes slower than it reads,
// and stops, reading little more than the probe, when the walk alone is as fast
// (large records, or pages reclaimed under memory pressure).
//
// conformance: cache-restart-handover
func TestRebuildReadahead_StopsWhenTheWalkIsAhead(t *testing.T) {
	// A local test file is read within the probe time, which never stops the
	// read-ahead (TestRebuildReadahead_FastDeviceNeverStops); decide at once,
	// after a short walk probe.
	walkProbe, probeTime := rebuildReadaheadWalkProbe, rebuildReadaheadProbeTime
	rebuildReadaheadWalkProbe, rebuildReadaheadProbeTime = 20*time.Millisecond, 0 // several walk ticks
	t.Cleanup(func() { rebuildReadaheadWalkProbe, rebuildReadaheadProbeTime = walkProbe, probeTime })
	path := filepath.Join(t.TempDir(), "f")
	size := int64(3 * rebuildReadaheadProbeMin)
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	ra := startRebuildReadahead(path, walking(t, 1)) // a byte a millisecond
	<-ra.done
	if got, early := ra.stop(); got != size || early {
		t.Fatalf("walk behind: read %d of %d bytes, stopped early %t", got, size, early)
	}
	ra = startRebuildReadahead(path, walking(t, 1<<40)) // far faster than any read
	<-ra.done
	got, early := ra.stop()
	limit := int64(rebuildReadaheadProbeMin) + int64(2*restoreReadaheadReaders+1)*restoreReadaheadBuffer
	if !early || got < rebuildReadaheadProbeMin || got > limit {
		t.Fatalf("walk ahead: read %d bytes (probe %d, at most %d), stopped early %t", got, rebuildReadaheadProbeMin, limit, early)
	}
}

// A device that reads the whole file within the probe time is never stopped,
// however far ahead the walk is.
//
// conformance: cache-restart-handover
func TestRebuildReadahead_FastDeviceNeverStops(t *testing.T) {
	probeTime := rebuildReadaheadProbeTime
	rebuildReadaheadProbeTime = time.Hour
	t.Cleanup(func() { rebuildReadaheadProbeTime = probeTime })
	path := filepath.Join(t.TempDir(), "f")
	size := int64(3 * rebuildReadaheadProbeMin)
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	ra := startRebuildReadahead(path, walking(t, 1<<40))
	<-ra.done
	if got, early := ra.stop(); got != size || early {
		t.Fatalf("read %d of %d bytes, stopped early %t", got, size, early)
	}
}
