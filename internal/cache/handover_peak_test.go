package cache

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"
)

// The shutdown flush holds at most about one 4 MiB chunk of encoded copies of
// the memory-only answers it keeps, not the whole -cache-startup-warm-bytes
// budget: 32 MiB of answers flush with a live-heap peak well under 32 MiB over
// the cache's own memory.
// conformance: cache-restart-handover
func TestHandover_FlushPeakMemoryIsOneChunk(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 512<<20, DiskCacheConfig{})
	value := make([]byte, 512<<10)
	for i := range value {
		value[i] = byte(i * 7)
	}
	for j := 0; j < 64; j++ {
		f.c.SetLocalOnlyRestorableWithTTL(fmt.Sprintf("compat:t:%02d:routing:x", j), value, time.Hour)
	}
	// Collect garbage early, so the sampled heap is close to what is live.
	defer debug.SetGCPercent(debug.SetGCPercent(5))
	runtime.GC()
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	base := sample[0].Value.Uint64()
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			metrics.Read(s)
			if v := s[0].Value.Uint64(); v > peak.Load() {
				peak.Store(v)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	report, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 64 << 20})
	close(stop)
	<-done
	if err != nil || report.MemoryEntries != 64 {
		t.Fatalf("flush %+v %v", report, err)
	}
	grown := int64(peak.Load()) - int64(base)
	t.Logf("live heap peak %d MiB over the cache's own %d MiB", grown>>20, base>>20)
	if grown > 24<<20 {
		t.Fatalf("flush peak %d MiB: the encoded copies must be bounded by a chunk, not the 32 MiB kept", grown>>20)
	}
}
