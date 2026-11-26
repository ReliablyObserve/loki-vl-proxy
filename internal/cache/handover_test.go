package cache

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Restart hand-over tests: a graceful shutdown keeps the memory tier's hot
// set through the disk tier and the next start restores it, within budgets,
// and never restores an entry past its expiry, for another build or
// configuration, after a later write to the file, a purge, or with a clock
// that reads earlier than the flush.

type handoverFixture struct {
	path string
	c    *Cache
	dc   *DiskCache
}

func openHandoverFixture(t *testing.T, path string, maxBytes int, diskCfg DiskCacheConfig) *handoverFixture {
	t.Helper()
	diskCfg.Path = path
	if diskCfg.FlushInterval == 0 {
		diskCfg.FlushInterval = time.Hour
	}
	dc, err := NewDiskCache(diskCfg)
	if err != nil {
		t.Fatal(err)
	}
	c := NewWithMaxBytes(time.Minute, 1000, maxBytes)
	c.SetL2(dc)
	c.EnableHandoverHits()
	f := &handoverFixture{path: path, c: c, dc: dc}
	t.Cleanup(f.close)
	return f
}

func (f *handoverFixture) close() {
	if f.c != nil {
		f.c.Close()
		_ = f.dc.Close()
		f.c, f.dc = nil, nil
	}
}

// restart flushes with opts (when non-nil), closes, and opens the same file.
func (f *handoverFixture) restart(t *testing.T, opts *HandoverFlushOptions) (*handoverFixture, HandoverFlushReport) {
	t.Helper()
	var report HandoverFlushReport
	if opts != nil {
		f.c.drainPromotions()
		var err error
		report, err = f.c.FlushHandover(context.Background(), *opts)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	f.close()
	return openHandoverFixture(t, f.path, 1<<20, DiskCacheConfig{}), report
}

func memoryKeys(c *Cache) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var keys []string
	for e := c.lruList.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*lruEntry).key)
	}
	return keys
}

func (c *Cache) memoryEntry(key string) (entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	return e, ok
}

// conformance: cache-restart-handover
func TestHandover_RestoresHotSetInRecencyOrderWithHits(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	for i := 0; i < 5; i++ {
		f.c.SetWithTTL(fmt.Sprintf("query_range:t:k%d", i), []byte(fmt.Sprintf("v%d", i)), time.Minute)
	}
	f.c.SetLocalOnlyRestorableWithTTL("compat:t:big:routing:x", []byte("oversized"), time.Minute)
	f.c.SetLocalOnlyWithTTL("query_range:t:marker:confirmed", []byte("1"), time.Minute)
	f.c.SetShadowWithTTL("query_range:t:peercopy", []byte("p"), time.Minute)
	f.c.SetWithTTL("patterns:t:p", []byte("pat"), time.Minute)
	f.dc.Flush()
	// k1 is the hottest (read three times, last), then the memory-only answer.
	f.c.drainPromotions()
	for i := 0; i < 3; i++ {
		f.c.GetWithTTL("compat:t:big:routing:x")
	}
	f.c.drainPromotions()
	for i := 0; i < 3; i++ {
		f.c.GetWithTTL("query_range:t:k1")
	}

	next, report := f.restart(t, &HandoverFlushOptions{Scope: "build-a", MemoryBytes: 1 << 20})
	if !report.Complete || report.MemoryEntries != 1 || report.Ranked != 6 {
		t.Fatalf("flush report %+v: want complete, 1 memory entry, 6 ranked (5 + the shadow written on disk)", report)
	}
	// The disk LRU keeps the order on its own: the hottest ranked record is newest.
	if got := next.dc.newest.key; got != "query_range:t:k1" {
		t.Fatalf("disk LRU newest = %q, want the hottest key", got)
	}
	warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "build-a", MaxBytes: 1 << 20})
	if warm.Reason != "done" || warm.MemoryEntries != 1 || warm.DiskEntries != 6 {
		t.Fatalf("warm report %+v", warm)
	}
	keys := memoryKeys(next.c)
	if len(keys) != 7 || keys[0] != "query_range:t:k1" || keys[1] != "compat:t:big:routing:x" {
		t.Fatalf("restored LRU order %q: want the hottest first", keys)
	}
	for _, absent := range []string{"query_range:t:marker:confirmed", "patterns:t:p"} {
		if _, ok := next.c.memoryEntry(absent); ok {
			t.Fatalf("%s restored: memory-only markers and patterns stay with their process", absent)
		}
	}
	big, ok := next.c.memoryEntry("compat:t:big:routing:x")
	if !ok || !big.memOnly || !big.restorable || string(big.value) != "oversized" {
		t.Fatalf("restorable answer restored as %+v", big)
	}
	if _, _, memOnly, ok := next.c.getForPeer("compat:t:big:routing:x"); !ok || !memOnly {
		t.Fatal("a restored memory-only answer must stay memory-only for peers")
	}
	next.c.mu.RLock()
	got := next.c.hits["query_range:t:k1"]
	next.c.mu.RUnlock()
	if got < 3 {
		t.Fatalf("restored hits %d, want the 3 counted before the restart", got)
	}
	if next.c.HasHandover() {
		t.Fatal("the hand-over must be taken once")
	}
	if again := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "build-a", MaxBytes: 1 << 20}); again.Found {
		t.Fatal("a second warm-up must find nothing")
	}
	stats := next.c.HandoverStats()
	if !stats.PreviousFlushFound || !stats.PreviousFlushComplete || stats.PreviousFlushMemoryEntries != 1 || !stats.WarmDone || stats.WarmDiskEntries != 6 {
		t.Fatalf("hand-over stats %+v", stats)
	}
	// The records are deleted after the warm-up read them.
	if err := next.dc.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(handoverBucket) != nil {
			return fmt.Errorf("hand-over bucket still present")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// conformance: cache-restart-handover
func TestHandover_OtherScopeRestoresOnlyDiskEntries(t *testing.T) {
	for _, scope := range []string{"build-b", ""} {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), time.Minute)
		next, _ := f.restart(t, &HandoverFlushOptions{Scope: "build-a", MemoryBytes: 1 << 20})
		warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: scope, MaxBytes: 1 << 20})
		if warm.DiskEntries != 1 || warm.MemoryEntries != 0 || warm.Skipped != 1 {
			t.Fatalf("scope %q: warm %+v, want the disk entry only", scope, warm)
		}
		if _, ok := next.c.memoryEntry("compat:t:b:routing:x"); ok {
			t.Fatalf("scope %q: memory-only answer of another build restored", scope)
		}
	}
}

// conformance: cache-restart-handover
func TestHandover_WriteAfterManifestDropsEverything(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
	f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), time.Minute)
	f.c.drainPromotions()
	if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	f.close()
	// An older binary (or anything else) writes to the file after the
	// manifest: it does not know the bucket, so it neither reads nor deletes it.
	db, err := bolt.Open(f.path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return tx.Bucket(dataBucket).Delete([]byte("query:t:a")) }); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	next := openHandoverFixture(t, f.path, 1<<20, DiskCacheConfig{})
	if next.c.HasHandover() {
		t.Fatal("a hand-over followed by another write must be dropped")
	}
	if _, ok := next.dc.PreviousFlush(); ok {
		t.Fatal("a dropped hand-over must not be reported as the previous flush")
	}
	if err := next.dc.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(handoverBucket) != nil {
			return fmt.Errorf("dropped hand-over bucket kept")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// conformance: cache-restart-handover
func TestHandover_CrashBeforeCloseLeavesNoManifest(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), time.Minute)
	f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
	if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	// The process dies after the records and before the disk tier's Close.
	f.c.Close()
	_ = f.dc.db.Close()
	f.c, f.dc = nil, nil
	next := openHandoverFixture(t, f.path, 1<<20, DiskCacheConfig{})
	if next.c.HasHandover() {
		t.Fatal("records without a manifest must be ignored")
	}
	if v, ok := next.dc.Get("query:t:a"); !ok || string(v) != "a" {
		t.Fatal("the data bucket must stay usable")
	}
}

// conformance: cache-restart-handover
func TestHandover_CorruptOrOldFormatIsIgnored(t *testing.T) {
	cases := map[string]func(tx *bolt.Tx) error{
		"corrupt manifest": func(tx *bolt.Tx) error {
			b := tx.Bucket(handoverBucket)
			raw := bytes.Clone(b.Get([]byte(handoverManifestKey)))
			raw[20] ^= 0xff
			return b.Put([]byte(handoverManifestKey), raw)
		},
		"unknown manifest version": func(tx *bolt.Tx) error {
			b := tx.Bucket(handoverBucket)
			raw := bytes.Clone(b.Get([]byte(handoverManifestKey)))
			raw[4] = 9
			return b.Put([]byte(handoverManifestKey), raw)
		},
		"no hand-over bucket (older writer)": func(tx *bolt.Tx) error {
			return tx.DeleteBucket(handoverBucket)
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
			f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
			f.c.drainPromotions()
			if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s"}); err != nil {
				t.Fatal(err)
			}
			f.close()
			// Damage the file in place, then make the manifest current again,
			// so only the damage decides.
			db, err := bolt.Open(f.path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *bolt.Tx) error {
				if err := damage(tx); err != nil {
					return err
				}
				return refreshManifestTx(tx)
			}); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
			next := openHandoverFixture(t, f.path, 1<<20, DiskCacheConfig{})
			if next.c.HasHandover() {
				t.Fatal("a damaged or absent hand-over must be ignored")
			}
			if v, ok := next.dc.Get("query:t:a"); !ok || string(v) != "a" {
				t.Fatal("the data bucket must stay usable")
			}
		})
	}
}

// refreshManifestTx rewrites a decodable manifest with this transaction's id,
// as if it had just been written; an undecodable one is left alone.
func refreshManifestTx(tx *bolt.Tx) error {
	b := tx.Bucket(handoverBucket)
	if b == nil {
		return nil
	}
	m, err := decodeHandoverManifest(b.Get([]byte(handoverManifestKey)))
	if err != nil {
		return nil
	}
	m.txID = uint64(tx.ID())
	raw, err := encodeHandoverManifest(m)
	if err != nil {
		return err
	}
	return b.Put([]byte(handoverManifestKey), raw)
}

// conformance: cache-restart-handover
func TestHandover_CorruptRecordIsSkipped(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	f.c.SetLocalOnlyRestorableWithTTL("compat:t:good:routing:x", []byte("good"), time.Minute)
	f.c.SetLocalOnlyRestorableWithTTL("compat:t:bad:routing:x", []byte("bad"), time.Minute)
	f.c.SetLocalOnlyRestorableWithTTL("compat:t:badvalue:routing:x", []byte("badvalue"), time.Minute)
	f.c.drainPromotions()
	if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	f.close()
	db, err := bolt.Open(f.path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(handoverBucket)
		badKey := handoverBucketKey(handoverClassMemory, "compat:t:bad:routing:x")
		raw := bytes.Clone(b.Get(badKey))
		raw[5] ^= 0xff // rank: header CRC fails
		if err := b.Put(badKey, raw); err != nil {
			return err
		}
		valueKey := handoverBucketKey(handoverClassMemory, "compat:t:badvalue:routing:x")
		raw = bytes.Clone(b.Get(valueKey))
		raw[len(raw)-1] ^= 0xff // value: the record CRC fails at decode
		if err := b.Put(valueKey, raw); err != nil {
			return err
		}
		return refreshManifestTx(tx)
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	next := openHandoverFixture(t, f.path, 1<<20, DiskCacheConfig{})
	warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
	if warm.MemoryEntries != 1 || next.dc.handoverCorrupt != 1 {
		t.Fatalf("warm %+v corrupt %d: want only the intact record", warm, next.dc.handoverCorrupt)
	}
	if e, ok := next.c.memoryEntry("compat:t:good:routing:x"); !ok || string(e.value) != "good" {
		t.Fatal("intact record not restored")
	}
	for _, key := range []string{"compat:t:bad:routing:x", "compat:t:badvalue:routing:x"} {
		if _, ok := next.c.memoryEntry(key); ok {
			t.Fatalf("damaged record %s restored", key)
		}
	}
}

// conformance: cache-restart-handover
func TestHandover_ClockSkewNeverExtendsAnEntry(t *testing.T) {
	t.Run("clock behind the flush restores nothing", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		restore := AdvanceClockForTesting(time.Hour)
		f.c.SetWithTTL("query:t:a", []byte("a"), 2*time.Hour)
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), 2*time.Hour)
		next, _ := f.restart(t, &HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20})
		restore() // the next process's clock reads an hour earlier
		warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
		if warm.Reason != "clock_behind_flush" || warm.DiskEntries+warm.MemoryEntries != 0 {
			t.Fatalf("warm %+v: a clock behind the flush must restore nothing", warm)
		}
		if _, ok := next.c.memoryEntry("compat:t:b:routing:x"); ok {
			t.Fatal("memory-only answer restored under a clock behind the flush")
		}
	})
	t.Run("clock ahead expires entries, never extends them", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetWithTTL("query:t:short", []byte("s"), 40*time.Second)
		f.c.SetWithTTL("query:t:long", []byte("l"), 10*time.Minute)
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), 40*time.Second)
		next, _ := f.restart(t, &HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20})
		restore := AdvanceClockForTesting(time.Minute)
		defer restore()
		warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
		if warm.DiskEntries != 1 || warm.MemoryEntries != 0 {
			t.Fatalf("warm %+v: only the long entry is still fresh", warm)
		}
		_, ttl, ok := next.c.GetWithTTL("query:t:long")
		if !ok || ttl > 9*time.Minute {
			t.Fatalf("restored ttl %v: must not exceed what was left (under 9m)", ttl)
		}
	})
}

// conformance: cache-restart-handover
func TestHandover_PurgeSupersedesFlushAndWarm(t *testing.T) {
	t.Run("purge after the flush", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), time.Minute)
		f.c.drainPromotions()
		if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20}); err != nil {
			t.Fatal(err)
		}
		f.c.PurgeAll()
		next, _ := f.restart(t, nil)
		if next.c.HasHandover() {
			t.Fatal("a purge must remove the hand-over")
		}
	})
	t.Run("purge during the flush", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		gen, err := f.dc.beginHandover()
		if err != nil {
			t.Fatal(err)
		}
		f.dc.Purge()
		f.dc.set("query:t:captured", []byte("x"), time.Minute, 0, gen)
		if f.dc.holds("query:t:captured") {
			t.Fatal("a value captured before a purge was written after it")
		}
		if err := f.dc.putHandoverRecords(gen, []handoverRecord{{key: "query:t:captured", class: handoverClassDisk}}); err == nil {
			t.Fatal("hand-over records written after a purge")
		}
		if f.dc.setHandoverManifest(gen, HandoverManifest{Scope: "s"}) {
			t.Fatal("a manifest accepted after a purge")
		}
	})
	t.Run("purge during the warm-up", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
		next, _ := f.restart(t, &HandoverFlushOptions{Scope: "s"})
		next.c.mu.Lock()
		next.c.diskPurges++ // a purge is running
		next.c.mu.Unlock()
		warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
		if warm.Reason != "purged" || warm.DiskEntries != 0 {
			t.Fatalf("warm %+v during a purge", warm)
		}
	})
}

// conformance: cache-restart-handover
func TestHandover_BudgetsDeadlineAndLiveEntries(t *testing.T) {
	t.Run("byte budget keeps the hottest", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		for i := 0; i < 10; i++ {
			f.c.SetWithTTL(fmt.Sprintf("query:t:%d", i), bytes.Repeat([]byte{'x'}, 100), time.Minute)
		}
		next, _ := f.restart(t, &HandoverFlushOptions{Scope: "s"})
		warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 350})
		if warm.DiskEntries != 3 {
			t.Fatalf("warm %+v: 350 bytes hold three entries", warm)
		}
		for _, k := range memoryKeys(next.c) {
			if k != "query:t:9" && k != "query:t:8" && k != "query:t:7" {
				t.Fatalf("restored %s: the budget must keep the most recent", k)
			}
		}
	})
	t.Run("memory-only budget at flush", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:1:routing:x", bytes.Repeat([]byte{'a'}, 600), time.Minute)
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:2:routing:x", bytes.Repeat([]byte{'b'}, 600), time.Minute)
		_, report := f.restart(t, &HandoverFlushOptions{Scope: "s", MemoryBytes: 1000})
		if report.MemoryEntries != 1 || report.MemoryBytes != 600 {
			t.Fatalf("flush %+v: the budget holds one 600-byte answer", report)
		}
	})
	t.Run("expired deadline writes no record", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:1:routing:x", []byte("a"), time.Minute)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		report, err := f.c.FlushHandover(ctx, HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20})
		if err != nil || report.Complete || report.MemoryEntries != 0 {
			t.Fatalf("flush %+v %v under an expired deadline", report, err)
		}
		next, _ := f.restart(t, nil)
		if next.c.HasHandover() {
			t.Fatal("an empty hand-over restores nothing")
		}
	})
	t.Run("warm-up deadline", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
		next, _ := f.restart(t, &HandoverFlushOptions{Scope: "s"})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		warm := next.c.WarmFromHandover(ctx, HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
		if warm.Reason != "deadline" || warm.DiskEntries != 0 || !next.c.HandoverStats().WarmDone {
			t.Fatalf("warm %+v under an expired deadline", warm)
		}
	})
	t.Run("never evicts or overwrites live entries", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetWithTTL("query:t:a", []byte("old"), time.Minute)
		f.c.SetWithTTL("query:t:b", []byte("b"), time.Minute)
		f.dc.Flush()
		f.c.drainPromotions()
		if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s"}); err != nil {
			t.Fatal(err)
		}
		f.close()
		dc, err := NewDiskCache(DiskCacheConfig{Path: f.path, FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		// Room for exactly one more entry; a live request stores a newer "a".
		c := NewWithMaxBytes(time.Minute, 2, 1<<20)
		c.SetL2(dc)
		defer func() { c.Close(); _ = dc.Close() }()
		c.SetLocalOnlyWithTTL("query:t:a", []byte("new"), time.Minute)
		c.SetLocalOnlyWithTTL("query:t:live", []byte("live"), time.Minute)
		warm := c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
		if warm.DiskEntries != 0 {
			t.Fatalf("warm %+v into a full memory tier", warm)
		}
		if v, _, ok := c.GetWithTTL("query:t:a"); !ok || string(v) != "new" {
			t.Fatal("the warm-up overwrote a fresher live entry")
		}
		if _, _, ok := c.GetWithTTL("query:t:live"); !ok {
			t.Fatal("the warm-up evicted a live entry")
		}
	})
}

// conformance: cache-restart-handover
func TestHandover_FlushWritesWhatTheDiskLacksAndHonoursItsBudget(t *testing.T) {
	t.Run("a refused write is written at shutdown", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
		f.dc.Flush()
		// The disk evicted the record while the memory tier kept the entry.
		f.dc.writeMu.Lock()
		f.dc.index["query:t:a"].invalid = true
		f.dc.writeMu.Unlock()
		next, report := f.restart(t, &HandoverFlushOptions{Scope: "s"})
		if report.DiskWrites != 1 || report.Ranked != 1 {
			t.Fatalf("flush %+v: want the missing record written and ranked", report)
		}
		if v, ok := next.dc.Get("query:t:a"); !ok || string(v) != "a" {
			t.Fatal("record not written at shutdown")
		}
	})
	t.Run("a full disk ranks only what it holds", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{MaxEntries: 2})
		for i := 0; i < 5; i++ {
			f.c.SetLocalOnlyWithTTL(fmt.Sprintf("query:t:%d", i), []byte("v"), time.Minute)
		}
		// Promote them to disk-eligible entries without writing the disk.
		f.c.mu.Lock()
		for k, e := range f.c.entries {
			e.memOnly = false
			f.c.entries[k] = e
		}
		f.c.mu.Unlock()
		next, report := f.restart(t, &HandoverFlushOptions{Scope: "s"})
		if report.Ranked != 2 {
			t.Fatalf("flush %+v: a two-entry disk ranks two", report)
		}
		warm := next.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20})
		if warm.DiskEntries != 2 {
			t.Fatalf("warm %+v", warm)
		}
		for _, want := range []string{"query:t:4", "query:t:3"} {
			if _, ok := next.c.memoryEntry(want); !ok {
				t.Fatalf("%s missing: a full disk must keep the hottest", want)
			}
		}
	})
	t.Run("a failing disk leaves no manifest", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		f.c.SetLocalOnlyRestorableWithTTL("compat:t:b:routing:x", []byte("b"), time.Minute)
		_ = f.dc.db.Close()
		if _, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s", MemoryBytes: 1 << 20}); err == nil {
			t.Fatal("a flush to a closed file must fail")
		}
		f.dc.writeMu.Lock()
		pending := f.dc.pendingManifest
		f.dc.writeMu.Unlock()
		if pending != nil {
			t.Fatal("a failed flush must leave no manifest")
		}
		f.c.Close()
		f.c, f.dc = nil, nil
	})
}

// conformance: cache-restart-handover
func TestHandover_StartupWindowFreezes(t *testing.T) {
	c := NewWithMaxBytes(time.Minute, 100, 1<<20)
	defer c.Close()
	if c.HandoverStats().StartupTracked {
		t.Fatal("untracked by default")
	}
	c.TrackStartupWindow()
	c.SetWithTTL("query:t:a", []byte("a"), time.Minute)
	c.GetSharedWithTTL("query:t:a")
	c.GetSharedWithTTL("query:t:missing")
	c.freezeStartupWindow()
	c.GetSharedWithTTL("query:t:a")
	s := c.HandoverStats()
	if !s.StartupTracked || s.StartupL1Requests != 2 || s.StartupL1Hits != 1 {
		t.Fatalf("startup window %+v: want 2 lookups and 1 hit, frozen", s)
	}
}

func TestHandover_ManifestAndRecordCodec(t *testing.T) {
	m := HandoverManifest{Scope: "v1|rev|cfg", FlushedAt: 42, Duration: time.Second, Complete: true, Ranked: 3, DiskWrites: 1, MemoryEntries: 2, MemoryBytes: 99, txID: 7}
	raw, err := encodeHandoverManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeHandoverManifest(raw)
	if err != nil || got != m {
		t.Fatalf("manifest round trip %+v %v", got, err)
	}
	if _, err := encodeHandoverManifest(HandoverManifest{Scope: strings.Repeat("x", handoverMaxScopeBytes+1)}); err == nil {
		t.Fatal("an oversized scope must be refused")
	}
	r := handoverRecord{key: "query:t:a", class: handoverClassDisk, rank: 5, hits: 9}
	class, key, rank, hits, payload, err := decodeHandoverRecordHeader(handoverBucketKey(r.class, r.key), encodeHandoverRecord(r))
	if err != nil || class != r.class || key != r.key || rank != 5 || hits != 9 || len(payload) != 0 {
		t.Fatalf("record round trip %c %q %d %d %v", class, key, rank, hits, err)
	}
	if _, _, _, _, _, err := decodeHandoverRecordHeader(handoverBucketKey(handoverClassMemory, r.key), encodeHandoverRecord(r)); err == nil {
		t.Fatal("a record under another class's key must be refused")
	}
}

func FuzzHandoverManifestDecode(f *testing.F) {
	raw, _ := encodeHandoverManifest(HandoverManifest{Scope: "s", FlushedAt: 1, Ranked: 1})
	f.Add(raw)
	f.Add([]byte(handoverMagic))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := decodeHandoverManifest(data)
		if err != nil {
			return
		}
		again, err := encodeHandoverManifest(m)
		if err != nil || !bytes.Equal(again, data) {
			t.Fatalf("decoded manifest does not re-encode to its input")
		}
	})
}

func FuzzHandoverRecordDecode(f *testing.F) {
	f.Add([]byte("dquery:t:a"), encodeHandoverRecord(handoverRecord{key: "query:t:a", class: handoverClassDisk, rank: 1}))
	payload, _ := encodeDiskRecord(diskEntry{Value: []byte("v"), ExpiresAt: 1}, diskCodecS2)
	f.Add([]byte("mcompat:x"), encodeHandoverRecord(handoverRecord{key: "compat:x", class: handoverClassMemory, payload: payload}))
	f.Add([]byte{}, []byte{})
	f.Fuzz(func(t *testing.T, bucketKey, raw []byte) {
		class, key, rank, hits, payload, err := decodeHandoverRecordHeader(bucketKey, raw)
		if err != nil {
			return
		}
		again := encodeHandoverRecord(handoverRecord{key: key, class: class, rank: rank, hits: hits, payload: payload})
		if !bytes.Equal(again, raw) {
			t.Fatalf("decoded record does not re-encode to its input")
		}
		if class == handoverClassMemory {
			_, _ = decodeNativeDiskRecordLimit(payload, 1<<20)
		}
	})
}
