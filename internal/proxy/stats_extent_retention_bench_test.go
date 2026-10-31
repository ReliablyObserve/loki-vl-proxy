package proxy

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Run with -benchtime=1x in a fresh process. The custom heap metric is a
// post-GC retained delta, not allocation throughput or process RSS. RAM and
// the disk queue hold the same value, not two independent payload copies.
func BenchmarkStatsExtentRetainedCache(b *testing.B) {
	for _, queue := range []bool{false, true} {
		name := "ram"
		if queue {
			name = "ram-and-disk-queue"
		}
		b.Run(name, func(b *testing.B) {
			const entries = 256
			fixture := statsRetentionFixture(10000, false)
			c := cache.NewWithMaxBytes(time.Hour, 1000, 64<<20)
			defer c.Close()
			if queue {
				disk, err := cache.NewDiskCache(cache.DiskCacheConfig{
					Path: filepath.Join(b.TempDir(), "cache.db"), MaxBytes: 64 << 20,
					FlushInterval: time.Hour, FlushSize: 10000, FlushBytes: 64 << 20,
					MaxBufferBytes: 128 << 20,
				})
				if err != nil {
					b.Fatal(err)
				}
				c.SetL2(disk)
				defer func() {
					if err := disk.Close(); err != nil {
						b.Error(err)
					}
				}()
			}
			var before, after runtime.MemStats
			runtime.GC()
			runtime.GC()
			runtime.ReadMemStats(&before)
			b.ResetTimer()
			var retained, encoded int
			for range b.N {
				retained, encoded = 0, 0
				for i := 0; i < entries; i++ {
					value, err := encodeStatsExtentChunk(fixture)
					if err != nil {
						b.Fatal(err)
					}
					c.SetWithTTL(fmt.Sprintf("%sretention:%d", statsExtentKeyPrefix, i), value, time.Hour)
					retained += cap(value)
					encoded += len(value)
				}
			}
			b.StopTimer()
			runtime.GC()
			runtime.GC()
			runtime.ReadMemStats(&after)
			count, charged := c.Size()
			if count != entries || c.Evictions.Load() != 0 {
				b.Fatalf("retention fixture did not fit: entries=%d evictions=%d", count, c.Evictions.Load())
			}
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/entries, "heap-B/entry")
			b.ReportMetric(float64(retained)/entries, "retained-cap-B/entry")
			b.ReportMetric(float64(encoded)/entries, "encoded-B/entry")
			b.ReportMetric(float64(charged)/entries, "charged-B/entry")
			if dir := os.Getenv("S2_HEAP_PROFILE_DIR"); dir != "" {
				file, err := os.Create(filepath.Join(dir, name+".pprof"))
				if err != nil {
					b.Fatal(err)
				}
				err = pprof.WriteHeapProfile(file)
				closeErr := file.Close()
				if err != nil || closeErr != nil {
					b.Fatalf("heap profile: %v, close: %v", err, closeErr)
				}
			}
			runtime.KeepAlive(c)
			runtime.KeepAlive(fixture)
		})
	}
}
