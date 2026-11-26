package cache

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// These benchmarks use only APIs that predate the eviction signals, so the
// same file runs against a baseline checkout; benchScrape is the one helper
// each checkout defines for what its metrics scrape reads.

// Every Set past the warm-up evicts the least recently used entry.
func BenchmarkCacheSetEvictCapacity(b *testing.B) {
	c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
	defer c.Close()
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "query:evict-" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		c.SetWithTTL(keys[i%len(keys)], value, time.Hour)
		i++
	}
}

// Every Set past the warm-up meets a full cache of expired entries: the expiry
// sweep runs, then the insert.
func BenchmarkCacheSetEvictExpired(b *testing.B) {
	value := []byte(`{"data":"test"}`)
	b.ReportAllocs()
	c := NewWithMaxBytes(time.Hour, 64, 1<<30)
	defer c.Close()
	i := 0
	for b.Loop() {
		c.SetWithTTL("query:exp-"+strconv.Itoa(i%128), value, time.Nanosecond)
		i++
	}
}

// Replacing hot keys in place.
func BenchmarkCacheSetReplace(b *testing.B) {
	c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
	defer c.Close()
	value := []byte(`{"data":"test"}`)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		c.SetWithTTL("query:replace-"+strconv.Itoa(i%256), value, time.Hour)
		i++
	}
}

func benchHitCache(b *testing.B) (*Cache, []string) {
	c := NewWithMaxBytes(time.Hour, 4096, 1<<30)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "query:hit-" + strconv.Itoa(i)
		c.SetWithTTL(keys[i], []byte(`{"status":"success"}`), time.Hour)
	}
	return c, keys
}

// Parallel memory hits; with_scrape adds a goroutine that reads the cache's
// metrics snapshots in a loop, as a scrape does, to expose any contention the
// snapshot adds to the read path.
func BenchmarkCacheGetHitParallel(b *testing.B) {
	for _, scrape := range []bool{false, true} {
		b.Run(fmt.Sprintf("with_scrape=%v", scrape), func(b *testing.B) {
			c, keys := benchHitCache(b)
			defer c.Close()
			var stop atomic.Bool
			done := make(chan struct{})
			if scrape {
				go func() {
					defer close(done)
					for !stop.Load() {
						benchScrape(c)
					}
				}()
			} else {
				close(done)
			}
			var next atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(next.Add(1)) * 7919
				for pb.Next() {
					c.Get(keys[i%len(keys)])
					i++
				}
			})
			b.StopTimer()
			stop.Store(true)
			<-done
		})
	}
}

// Parallel writers that keep evicting, with and without a concurrent scrape.
func BenchmarkCacheSetEvictParallel(b *testing.B) {
	for _, scrape := range []bool{false, true} {
		b.Run(fmt.Sprintf("with_scrape=%v", scrape), func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			keys := make([]string, 8192)
			for i := range keys {
				keys[i] = "query:par-" + strconv.Itoa(i)
			}
			var stop atomic.Bool
			done := make(chan struct{})
			if scrape {
				go func() {
					defer close(done)
					for !stop.Load() {
						benchScrape(c)
					}
				}()
			} else {
				close(done)
			}
			value := []byte(`{"data":"test"}`)
			var next atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(next.Add(1)) * 104729
				for pb.Next() {
					c.SetWithTTL(keys[i%len(keys)], value, time.Hour)
					i++
				}
			})
			b.StopTimer()
			stop.Store(true)
			<-done
		})
	}
}

// One flush that commits 64 records into a full index: 64 capacity evictions.
func BenchmarkDiskCacheFlushEvict(b *testing.B) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(b.TempDir(), "cache.db"), MaxEntries: 256, FlushInterval: time.Hour, FlushSize: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = dc.Close() }()
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	n := 0
	put := func() {
		for j := 0; j < 64; j++ {
			dc.Set("query:flush-"+strconv.Itoa(n), value, time.Hour)
			n++
		}
		dc.Flush()
	}
	for i := 0; i < 8; i++ {
		put()
	}
	b.ReportAllocs()
	for b.Loop() {
		put()
	}
}

// A disk record read (bbolt read and decode), the L2 read histogram's path.
func BenchmarkDiskCacheGetCommitted(b *testing.B) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(b.TempDir(), "cache.db"), FlushInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = dc.Close() }()
	dc.Set("query:read", benchmarkDiskPayload(), time.Hour)
	dc.Flush()
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := dc.Get("query:read"); !ok {
			b.Fatal("miss")
		}
	}
}
