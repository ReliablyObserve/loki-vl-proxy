package cache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Cost of the restart hand-over's own work (a shutdown flush and a start-up
// warm-up of 1,000 entries of 1 KiB), and of the memory read path with the
// hand-over's hit counting on: the read path is unchanged, the promoter counts.

func handoverBenchEntries(c *Cache, n int) {
	value := make([]byte, 1024)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("query_range:t:bench-%05d", i)
		if i%10 == 0 {
			c.SetLocalOnlyRestorableWithTTL("compat:t:"+key+":routing:x", value, time.Hour)
			continue
		}
		c.SetWithTTL(key, value, time.Hour)
	}
}

func BenchmarkHandoverFlush(b *testing.B) {
	dir := b.TempDir()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		path := filepath.Join(dir, fmt.Sprintf("flush-%d.db", i))
		dc, err := NewDiskCache(DiskCacheConfig{Path: path, FlushInterval: time.Hour})
		if err != nil {
			b.Fatal(err)
		}
		c := NewWithMaxBytes(time.Hour, 2000, 64<<20)
		c.SetL2(dc)
		handoverBenchEntries(c, 1000)
		dc.Flush()
		b.StartTimer()
		if _, err := c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 64 << 20}); err != nil {
			b.Fatal(err)
		}
		_ = dc.Close()
		b.StopTimer()
		c.Close()
	}
}

func BenchmarkHandoverWarm(b *testing.B) {
	dir := b.TempDir()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		path := filepath.Join(dir, fmt.Sprintf("warm-%d.db", i))
		dc, err := NewDiskCache(DiskCacheConfig{Path: path, FlushInterval: time.Hour})
		if err != nil {
			b.Fatal(err)
		}
		c := NewWithMaxBytes(time.Hour, 2000, 64<<20)
		c.SetL2(dc)
		handoverBenchEntries(c, 1000)
		dc.Flush()
		if _, err := c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 64 << 20}); err != nil {
			b.Fatal(err)
		}
		_ = dc.Close()
		c.Close()
		b.StartTimer()
		dc, err = NewDiskCache(DiskCacheConfig{Path: path, FlushInterval: time.Hour})
		if err != nil {
			b.Fatal(err)
		}
		c = NewWithMaxBytes(time.Hour, 2000, 64<<20)
		c.SetL2(dc)
		report := c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 64 << 20})
		b.StopTimer()
		if report.DiskEntries+report.MemoryEntries != 1000 {
			b.Fatalf("warmed %+v", report)
		}
		c.Close()
		_ = dc.Close()
	}
}

func BenchmarkCache_Get_Hit_HandoverHits(b *testing.B) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(b.TempDir(), "hits.db"), FlushInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	c := New(60*time.Second, 10000)
	c.SetL2(dc)
	defer func() { c.Close(); _ = dc.Close() }()
	c.EnableHandoverHits()
	c.Set("bench-key", []byte("bench-value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("bench-key")
	}
}

// BenchmarkHandoverFlushLargeMemory flushes 32 MiB of memory-only answers
// (64 of 512 KiB): the flush's own allocation stays near one 4 MiB chunk of
// encoded copies plus the records, not the whole budget.
func BenchmarkHandoverFlushLargeMemory(b *testing.B) {
	dir := b.TempDir()
	value := make([]byte, 512<<10)
	for i := range value {
		value[i] = byte(i * 7) // poorly compressible
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(dir, fmt.Sprintf("large-%d.db", i)), FlushInterval: time.Hour})
		if err != nil {
			b.Fatal(err)
		}
		c := NewWithMaxBytes(time.Hour, 1000, 512<<20)
		c.SetL2(dc)
		for j := 0; j < 64; j++ {
			c.SetLocalOnlyRestorableWithTTL(fmt.Sprintf("compat:t:%02d:routing:x", j), value, time.Hour)
		}
		b.StartTimer()
		report, err := c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 64 << 20})
		if err != nil || report.MemoryEntries != 64 {
			b.Fatalf("flush %+v %v", report, err)
		}
		b.StopTimer()
		_ = dc.Close()
		c.Close()
	}
}
