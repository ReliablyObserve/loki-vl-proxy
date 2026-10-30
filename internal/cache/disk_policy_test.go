package cache

import (
	"bytes"
	"encoding/binary"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
	"testing"
	"time"
)

func policyDisk(t *testing.T, cfg DiskCacheConfig) *DiskCache {
	t.Helper()
	if cfg.Path == "" {
		cfg.Path = filepath.Join(t.TempDir(), "cache.db")
	}
	cfg.FlushInterval = time.Hour
	dc, err := NewDiskCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dc.Close() })
	return dc
}
func policyWant(t *testing.T, dc *DiskCache, key string, want []byte) {
	t.Helper()
	got, ok := dc.Get(key)
	if want == nil {
		if ok {
			t.Fatalf("key %.64q unexpectedly present", key)
		}
		return
	}
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("%s = %q, %v; want %q", key, got, ok, want)
	}
}
func TestDiskPolicyLRUEvictsColdEntry(t *testing.T) {
	for _, limit := range []string{"bytes", "entries", "index"} {
		t.Run(limit, func(t *testing.T) {
			cfg := DiskCacheConfig{MaxBytes: 140, MaxEntries: 100, MaxIndexBytes: 1 << 20}
			if limit == "entries" {
				cfg.MaxBytes = 0
				cfg.MaxEntries = 2
			}
			if limit == "index" {
				cfg.MaxBytes = 0
				cfg.MaxIndexBytes = 258
			}
			dc := policyDisk(t, cfg)
			value := bytes.Repeat([]byte("x"), 32)
			dc.Set("a", value, time.Hour)
			dc.Flush()
			dc.Set("b", value, time.Hour)
			dc.Flush()
			stats := dc.db.Stats()
			before := stats.TxStats.GetWrite()
			policyWant(t, dc, "a", value)
			stats = dc.db.Stats()
			if after := stats.TxStats.GetWrite(); after != before {
				t.Fatalf("cache hit wrote to disk: %d -> %d", before, after)
			}
			dc.Set("c", value, time.Hour)
			dc.Flush()
			policyWant(t, dc, "b", nil)
			policyWant(t, dc, "a", value)
			policyWant(t, dc, "c", value)
		})
	}
}
func TestDiskPolicyRestartKeepsValuesAndBudget(t *testing.T) {
	cfg := DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), MaxEntries: 2}
	dc := policyDisk(t, cfg)
	dc.Set("a", []byte("a"), time.Hour)
	dc.Flush()
	dc.Set("b", []byte("b"), time.Hour)
	dc.Flush()
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	dc = policyDisk(t, cfg)
	policyWant(t, dc, "b", []byte("b"))
	policyWant(t, dc, "a", []byte("a"))
	dc.Set("c", []byte("c"), time.Hour)
	dc.Flush()
	policyWant(t, dc, "b", nil)
	policyWant(t, dc, "a", []byte("a"))
	policyWant(t, dc, "c", []byte("c"))
}
func TestDiskPolicyRejectedReplacementCannotResurrect(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntryBytes: 64, MaxBufferBytes: 1024})
	dc.Set("a", []byte("old"), time.Hour)
	dc.Flush()
	dc.Set("a", bytes.Repeat([]byte("x"), 65), time.Hour)
	policyWant(t, dc, "a", nil)
	dc.Flush()
	policyWant(t, dc, "a", nil)
	dc.Set("b", []byte("small"), time.Hour)
	dc.Flush()
	policyWant(t, dc, "b", []byte("small"))
}
func TestDiskPolicyNativeCorruptionNeverUsesLegacy(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{})
	dc.Set("a", []byte("new"), time.Hour)
	dc.Flush()
	err := dc.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(legacyDataBucket)
		if err != nil {
			return err
		}
		old, valid := encodeDiskEntry(diskEntry{Value: []byte("old"), ExpiresAt: time.Now().Add(time.Hour).UnixNano()})
		if !valid {
			t.Fatal("legacy encode failed")
		}
		if err := b.Put([]byte("a"), old); err != nil {
			return err
		}
		native := tx.Bucket(dataBucket)
		damaged := bytes.Clone(native.Get([]byte("a")))
		damaged[0] ^= 1
		return native.Put([]byte("a"), damaged)
	})
	if err != nil {
		t.Fatal(err)
	}
	policyWant(t, dc, "a", nil)
}

// bbolt admits one writer. A blocked disk writer must not turn Set into a
// synchronous I/O operation while the primary cache holds its own mutex.
func TestDiskPolicyThresholdDoesNotBlockSet(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{FlushSize: 1, FlushBytes: 256, MaxBufferBytes: 4096})
	tx, err := dc.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			_ = tx.Rollback()
		}
	}()
	done := make(chan struct{})
	go func() { dc.Set("a", []byte("first"), time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Set blocked behind bbolt writer")
	}
	// Buffered data remains visible even if the asynchronous flusher took it.
	got := make(chan []byte, 1)
	go func() { v, _ := dc.Get("a"); got <- v }()
	select {
	case v := <-got:
		if string(v) != "first" {
			t.Fatalf("pending read = %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending read blocked behind bbolt writer")
	}
	_ = tx.Rollback()
	released = true
	dc.Flush()
	policyWant(t, dc, "a", []byte("first"))
}
func TestDiskPolicyFlushPurgeReplacementOrdering(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{FlushSize: 1})
	for i := 0; i < 30; i++ {
		dc.Set("a", []byte("before"), time.Hour)
		// Purge is a barrier for all previously admitted pending/flushing data.
		dc.Purge()
		dc.Flush()
		policyWant(t, dc, "a", nil)
		dc.Set("a", []byte("after"), time.Hour)
		dc.Flush()
		policyWant(t, dc, "a", []byte("after"))
	}
}
func TestDiskPolicyInvalidBudgets(t *testing.T) {
	for _, cfg := range []DiskCacheConfig{
		{MaxBytes: -1}, {MaxEntries: -1}, {MaxIndexBytes: -1}, {MaxBufferBytes: -1}, {FlushBytes: -1}, {FlushSize: -1}, {MaxEntryBytes: -1}, {Codec: "unknown"},
	} {
		cfg.Path = filepath.Join(t.TempDir(), "cache.db")
		dc, err := NewDiskCache(cfg)
		if err == nil {
			_ = dc.Close()
			t.Errorf("accepted invalid config: %+v", cfg)
		}
	}
}

func TestDiskPolicyLegacyUpgrade(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "gzip"}[compressed], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache.db")
			db, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			old, valid := encodeDiskEntry(diskEntry{Value: []byte("legacy"), ExpiresAt: time.Now().Add(time.Hour).UnixNano()})
			if !valid {
				t.Fatal("encode failed")
			}
			if compressed {
				old, err = compress(old)
				if err != nil {
					t.Fatal(err)
				}
			}
			err = db.Update(func(tx *bolt.Tx) error {
				b, e := tx.CreateBucketIfNotExists(legacyDataBucket)
				if e != nil {
					return e
				}
				return b.Put([]byte("a"), old)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			dc := policyDisk(t, DiskCacheConfig{Path: path, Compression: compressed})
			policyWant(t, dc, "a", []byte("legacy"))
			dc.Set("a", []byte("new"), time.Hour)
			dc.Flush()
			policyWant(t, dc, "a", []byte("new"))
			err = dc.db.View(func(tx *bolt.Tx) error {
				if v := tx.Bucket(legacyDataBucket).Get([]byte("a")); v != nil {
					t.Error("new write retained legacy fallback")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDiskPolicyQueueBoundUnderBlockedStorage(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxBufferBytes: 512, FlushSize: 1})
	tx, err := dc.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 100; i++ {
		dc.Set(string(rune(i+1)), bytes.Repeat([]byte("x"), 32), time.Hour)
	}
	dc.writeMu.Lock()
	used := dc.bufferBytes
	dc.writeMu.Unlock()
	if used > 512 {
		t.Fatalf("queue exceeds budget: %d", used)
	}
	if dc.RejectedWrites.Load() == 0 {
		t.Fatal("expected bounded queue admission rejection")
	}
	_ = tx.Rollback()
	dc.Flush()
}
func TestDiskPolicyCompressionFalseOverridesExplicitCodec(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{Compression: false, Codec: "s2"})
	dc.Set("a", bytes.Repeat([]byte("x"), 100), time.Hour)
	dc.Flush()
	err := dc.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(dataBucket).Get([]byte("a"))
		if len(raw) < diskRecordHeaderSize || diskCodec(raw[5]) != diskCodecRaw {
			t.Fatal("compression=false did not select raw")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiskPolicyOversizeKeyCannotPoisonBatch(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{})
	valid := string(bytes.Repeat([]byte("v"), bolt.MaxKeySize))
	invalid := string(bytes.Repeat([]byte("x"), bolt.MaxKeySize+1))
	dc.Set(invalid, []byte("bad"), time.Hour)
	dc.Set(valid, []byte("boundary"), time.Hour)
	dc.Set("good", []byte("good"), time.Hour)
	dc.Flush()
	dc.Flush()
	policyWant(t, dc, "good", []byte("good"))
	policyWant(t, dc, valid, []byte("boundary"))
	policyWant(t, dc, invalid, nil)
	if dc.RejectedWrites.Load() == 0 {
		t.Fatal("oversize key admission not rejected")
	}
}

func TestDiskPolicyStartupRejectsOversizedRecords(t *testing.T) {
	for _, kind := range []string{"native_encoded", "native_decoded", "legacy_raw", "legacy_gzip"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache.db")
			expiry := time.Now().Add(time.Hour).UnixNano()
			raw, err := encodeDiskRecord(diskEntry{Value: []byte("x"), ExpiresAt: expiry}, diskCodecRaw)
			if err != nil {
				t.Fatal(err)
			}
			bucket := dataBucket
			switch kind {
			case "native_encoded":
				raw = append(raw, make([]byte, 8192)...)
			case "native_decoded":
				binary.BigEndian.PutUint32(raw[16:20], 65)
			case "legacy_raw", "legacy_gzip":
				bucket = legacyDataBucket
				raw = make([]byte, 8192)
				binary.BigEndian.PutUint64(raw[:8], uint64(expiry))
			}
			db, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				b, e := tx.CreateBucketIfNotExists(bucket)
				if e != nil {
					return e
				}
				return b.Put([]byte("oversized"), raw)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			dc := policyDisk(t, DiskCacheConfig{Path: path, MaxEntryBytes: 64, Compression: kind == "legacy_gzip"})
			if n, _ := dc.Size(); n != 0 {
				t.Fatalf("oversized stored record entered index: %d", n)
			}
			policyWant(t, dc, "oversized", nil)
		})
	}
}

func TestDiskPolicyBatchRecencyFollowsUse(t *testing.T) {
	for _, touch := range []bool{false, true} {
		t.Run(map[bool]string{false: "write_order", true: "pending_hit"}[touch], func(t *testing.T) {
			dc := policyDisk(t, DiskCacheConfig{MaxEntries: 2})
			dc.Set("c", []byte("c"), time.Hour)
			dc.Set("b", []byte("b"), time.Hour)
			dc.Set("a", []byte("a"), time.Hour)
			if touch {
				policyWant(t, dc, "c", []byte("c"))
			}
			dc.Flush()
			policyWant(t, dc, "a", []byte("a"))
			if touch {
				policyWant(t, dc, "b", nil)
				policyWant(t, dc, "c", []byte("c"))
			} else {
				policyWant(t, dc, "c", nil)
				policyWant(t, dc, "b", []byte("b"))
			}
		})
	}
}
