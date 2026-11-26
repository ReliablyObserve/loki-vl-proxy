package cache

import (
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Retention policy costs: the same workloads with the policy off (plain LRU)
// and on, so benchstat shows what protection and demotion add per operation.

var retentionBenchPolicies = []struct {
	name   string
	policy RetentionPolicy
}{
	{"lru", RetentionPolicy{}},
	{"protect20", RetentionPolicy{ProtectPercent: 20}},
	{"protect20+demote", RetentionPolicy{ProtectPercent: 20, DemoteOnEvict: true}},
}

func retentionBenchKeys(n int, prefix string) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i)
	}
	return keys
}

// Every Set past the warm-up evicts; one key in four is read before the next
// round, so the protected segment fills and turns over.
func BenchmarkRetentionSetEvict(b *testing.B) {
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	keys := retentionBenchKeys(4096, "query:evict-")
	for _, p := range retentionBenchPolicies {
		b.Run("policy="+p.name, func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			if err := c.SetRetention(p.policy); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				k := keys[i%len(keys)]
				c.SetWithTTL(k, value, time.Hour)
				if i%4 == 0 {
					c.Get(k)
				}
				i++
			}
		})
	}
}

// Hits on a resident set with the promoter applied every 256 lookups: the
// promoter's segment work on top of the LRU move.
func BenchmarkRetentionGetHitPromote(b *testing.B) {
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	keys := retentionBenchKeys(2048, "query:hit-")
	for _, p := range retentionBenchPolicies {
		b.Run("policy="+p.name, func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 4096, 1<<30)
			defer c.Close()
			if err := c.SetRetention(p.policy); err != nil {
				b.Fatal(err)
			}
			for _, k := range keys {
				c.SetWithTTL(k, value, time.Hour)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				c.Get(keys[i%len(keys)])
				if i%256 == 255 {
					c.drainPromotions()
				}
				i++
			}
		})
	}
}

// Replacing hot keys in place.
func BenchmarkRetentionReplace(b *testing.B) {
	value := []byte(`{"data":"test"}`)
	keys := retentionBenchKeys(256, "query:replace-")
	for _, p := range retentionBenchPolicies {
		b.Run("policy="+p.name, func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			if err := c.SetRetention(p.policy); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				c.SetWithTTL(keys[i%len(keys)], value, time.Hour)
				i++
			}
		})
	}
}

// Capacity evictions with a disk below: each evicted entry was read once, so
// demotion hands it to the disk (kept, as the disk already holds it).
func BenchmarkRetentionEvictWithDisk(b *testing.B) {
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	keys := retentionBenchKeys(4096, "query:disk-")
	for _, p := range retentionBenchPolicies {
		b.Run("policy="+p.name, func(b *testing.B) {
			dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(b.TempDir(), "bench.db"), FlushInterval: time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = dc.Close() }()
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			if err := c.SetRetention(p.policy); err != nil {
				b.Fatal(err)
			}
			c.SetL2(dc)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				k := keys[i%len(keys)]
				c.SetWithTTL(k, value, time.Hour)
				c.Get(k)
				i++
			}
		})
	}
}

// Parallel hits on a resident set larger than the protected share: every
// hit's deferred promotion may move entries between segments under the lock
// the readers share.
func BenchmarkRetentionGetHitParallel(b *testing.B) {
	value := []byte(`{"status":"success"}`)
	keys := retentionBenchKeys(2048, "query:par-hit-")
	for _, p := range retentionBenchPolicies {
		b.Run("policy="+p.name, func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 4096, 1<<30)
			defer c.Close()
			if err := c.SetRetention(p.policy); err != nil {
				b.Fatal(err)
			}
			for _, k := range keys {
				c.SetWithTTL(k, value, time.Hour)
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
		})
	}
}

// Parallel writers that keep evicting, one lookup in four a hit.
func BenchmarkRetentionSetEvictParallel(b *testing.B) {
	value := []byte(`{"data":"test"}`)
	keys := retentionBenchKeys(8192, "query:par-evict-")
	for _, p := range retentionBenchPolicies {
		b.Run("policy="+p.name, func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			if err := c.SetRetention(p.policy); err != nil {
				b.Fatal(err)
			}
			var next atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(next.Add(1)) * 104729
				for pb.Next() {
					k := keys[i%len(keys)]
					c.SetWithTTL(k, value, time.Hour)
					if i%4 == 0 {
						c.Get(k)
					}
					i++
				}
			})
		})
	}
}
