package cache

import (
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

// scrapeModes are the scrapers the parallel benchmarks run beside the
// workload: none; layer_stats, the layer snapshots both checkouts have; full,
// what this checkout's metrics scrape reads (benchScrape), in a tight loop
// (the worst case: one of the Ps does nothing else); full_paced, the same every
// millisecond (still about 15,000 times a 15 s scrape interval).
var scrapeModes = []string{"none", "layer_stats", "full", "full_paced"}

func startScraper(c *Cache, mode string) (stop func()) {
	if mode == "none" {
		return func() {}
	}
	var halt atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !halt.Load() {
			switch mode {
			case "layer_stats":
				_ = c.LayerStats()
			case "full":
				benchScrape(c)
			case "full_paced":
				benchScrape(c)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	return func() { halt.Store(true); <-done }
}

// Parallel memory hits beside each scraper: a scrape that takes a lock the
// read path needs, or writes a line it reads, slows the hits.
func BenchmarkCacheGetHitParallel(b *testing.B) {
	for _, mode := range scrapeModes {
		b.Run("scrape="+mode, func(b *testing.B) {
			c, keys := benchHitCache(b)
			defer c.Close()
			stop := startScraper(c, mode)
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
			stop()
		})
	}
}

// Parallel writers that keep evicting, beside each scraper.
func BenchmarkCacheSetEvictParallel(b *testing.B) {
	for _, mode := range scrapeModes {
		b.Run("scrape="+mode, func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			keys := make([]string, 8192)
			for i := range keys {
				keys[i] = "query:par-" + strconv.Itoa(i)
			}
			stop := startScraper(c, mode)
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
			stop()
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
