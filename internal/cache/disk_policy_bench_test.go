package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	gzip "github.com/klauspost/compress/gzip"
	bolt "go.etcd.io/bbolt"
)

// These benchmarks intentionally use the DiskCacheConfig fields shared with the
// pre-optimization cache, so the same fixtures can measure both implementations.
// writecalls/op counts bbolt software write calls, not device IOPS or fsyncs.
const diskPolicyFixtureKeys = 1024

func diskPolicyPayload(size int, random bool) []byte {
	payload := make([]byte, size)
	if random {
		_, _ = rand.New(rand.NewSource(20261030)).Read(payload)
		return payload
	}
	pattern := []byte(`{"level":"info","service":"checkout","message":"completed request"}`)
	for offset := 0; offset < len(payload); {
		offset += copy(payload[offset:], pattern)
	}
	return payload
}

func diskPolicyOpen(b *testing.B, flushSize int) *DiskCache {
	return diskPolicyOpenAt(b, flushSize, filepath.Join(b.TempDir(), "cache.db"))
}

func diskPolicyOpenAt(b *testing.B, flushSize int, path string) *DiskCache {
	b.Helper()
	dc, err := NewDiskCache(DiskCacheConfig{
		Path: path, Compression: true,
		FlushSize: flushSize, FlushInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := dc.Close(); err != nil {
			b.Error(err)
		}
	})
	return dc
}

func diskPolicyKeys() []string {
	keys := make([]string, diskPolicyFixtureKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("disk-policy-%04d", i)
	}
	return keys
}

func diskPolicyPopulate(b *testing.B, dc *DiskCache, keys []string, payload []byte) {
	b.Helper()
	for i, key := range keys {
		dc.Set(key, payload, 24*time.Hour)
		// Bound fixture queue size independently of implementation defaults.
		// In particular, 1024 random 64KiB records must not overflow a bounded
		// asynchronous queue before the read benchmark starts.
		if (i+1)%64 == 0 {
			dc.Flush()
		}
	}
	dc.Flush()
	if got, ok := dc.Get(keys[0]); !ok || !bytes.Equal(got, payload) {
		b.Fatal("disk fixture did not round-trip")
	}
}

func BenchmarkDiskPolicyHit(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 64 << 10} {
		for _, random := range []bool{false, true} {
			kind := "compressible"
			if random {
				kind = "random"
			}
			b.Run(fmt.Sprintf("%dB/%s", size, kind), func(b *testing.B) {
				dc := diskPolicyOpen(b, 1000)
				keys, payload := diskPolicyKeys(), diskPolicyPayload(size, random)
				diskPolicyPopulate(b, dc, keys, payload)
				before := dc.db.Stats()
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, ok := dc.Get(keys[i%len(keys)])
					if !ok || len(got) != size || got[0] != payload[0] || got[len(got)-1] != payload[len(payload)-1] {
						b.Fatal("invalid disk hit")
					}
				}
				b.StopTimer()
				after := dc.db.Stats()
				writeCalls := after.TxStats.GetWrite() - before.TxStats.GetWrite()
				if writeCalls != 0 {
					b.Fatalf("disk hits unexpectedly performed %d software writes", writeCalls)
				}
				b.ReportMetric(float64(writeCalls)/float64(b.N), "writecalls/op")
			})
		}
	}
}

// Flush timing includes draining the final partial batch, so a short benchmark
// cannot report free writes just because they remain buffered when it ends.
func BenchmarkDiskPolicyBatch(b *testing.B) {
	for _, flushSize := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("batch%d", flushSize), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "cache.db")
			// Build the stable working set with bounded batched writes even
			// for batch1, then reopen with the policy under measurement.
			seed, err := NewDiskCache(DiskCacheConfig{
				Path: path, Compression: true,
				FlushSize: 1000, FlushInterval: time.Hour,
			})
			if err != nil {
				b.Fatal(err)
			}
			keys, payload := diskPolicyKeys(), diskPolicyPayload(4<<10, false)
			diskPolicyPopulate(b, seed, keys, payload)
			if err := seed.Close(); err != nil {
				b.Fatal(err)
			}
			dc := diskPolicyOpenAt(b, flushSize, path)
			before := dc.db.Stats()
			flushBefore := dc.FlushCount.Load()
			writesBefore := dc.Writes.Load()
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dc.Set(keys[i%len(keys)], payload, 24*time.Hour)
			}
			dc.Flush()
			b.StopTimer()
			after := dc.db.Stats()
			b.ReportMetric(float64(after.TxStats.GetWrite()-before.TxStats.GetWrite())/float64(b.N), "writecalls/op")
			b.ReportMetric(float64(dc.FlushCount.Load()-flushBefore)/float64(b.N), "flushes/op")
			b.ReportMetric(float64(dc.Writes.Load()-writesBefore)/float64(b.N), "committedvalues/op")
			if got, ok := dc.Get(keys[0]); !ok || !bytes.Equal(got, payload) {
				b.Fatal("batch fixture did not round-trip")
			}
		})
	}
}

// Each operation commits exactly 100 distinct records before the next operation
// begins. Unlike the burst benchmark, this holds persistence work constant and
// prevents asynchronous same-key coalescing from explaining the improvement.
func BenchmarkDiskPolicyCommittedBatch(b *testing.B) {
	const recordsPerBatch = 100
	dc := diskPolicyOpen(b, 100000)
	keys, payload := diskPolicyKeys(), diskPolicyPayload(4<<10, false)
	diskPolicyPopulate(b, dc, keys, payload)
	before := dc.db.Stats()
	writesBefore := dc.Writes.Load()
	b.ReportAllocs()
	b.SetBytes(recordsPerBatch * int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < recordsPerBatch; j++ {
			dc.Set(keys[(i*recordsPerBatch+j)%len(keys)], payload, 24*time.Hour)
		}
		dc.Flush()
	}
	b.StopTimer()
	after := dc.db.Stats()
	committed := dc.Writes.Load() - writesBefore
	if committed != int64(b.N)*recordsPerBatch {
		b.Fatalf("committed %d records, want %d", committed, int64(b.N)*recordsPerBatch)
	}
	b.ReportMetric(float64(committed)/float64(b.N), "committedrecords/op")
	b.ReportMetric(float64(after.TxStats.GetWrite()-before.TxStats.GetWrite())/float64(committed), "writecalls/record")
}

// Legacy entries place the expiration inside the gzip stream. Seed them before
// opening DiskCache, so a newer implementation can discover them while rebuilding
// its index. No keys expire, so every iteration sees a stable keyspace. This
// measures one bounded maintenance invocation, not necessarily a full scan:
// implementations may apply different per-invocation scan/time budgets.
func BenchmarkDiskPolicyLegacyExpirySweep(b *testing.B) {
	path := filepath.Join(b.TempDir(), "cache.db")
	seedDB, err := bolt.Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	payload := diskPolicyPayload(4<<10, false)
	legacy := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(legacy[:8], uint64(time.Now().Add(24*time.Hour).UnixNano()))
	copy(legacy[8:], payload)
	var encoded bytes.Buffer
	w := gzip.NewWriter(&encoded)
	if _, err := w.Write(legacy); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	keys := diskPolicyKeys()
	if err := seedDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("cache"))
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := bucket.Put([]byte(key), encoded.Bytes()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = seedDB.Close()
		b.Fatal(err)
	}
	if err := seedDB.Close(); err != nil {
		b.Fatal(err)
	}
	dc := diskPolicyOpenAt(b, 1000, path)
	before := dc.db.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dc.Flush()
	}
	b.StopTimer()
	after := dc.db.Stats()
	b.ReportMetric(float64(after.TxStats.GetWrite()-before.TxStats.GetWrite())/float64(b.N), "writecalls/op")
	b.ReportMetric(diskPolicyFixtureKeys, "fixturekeys")
}
