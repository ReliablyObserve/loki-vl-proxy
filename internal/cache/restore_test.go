package cache

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// The batched restore's safety and bounds: batches are bounded, a purge wins,
// something fresher than the disk record wins, the disk order is left as ranked,
// the memory in flight is one batch plus the read-ahead buffers, the progress
// reads right while it runs, and every goroutine returns.

// hookCtx runs a hook before the n-th Err call (the warm-up checks its context
// once before each batch).
type hookCtx struct {
	context.Context
	calls, at int
	hook      func()
}

func (c *hookCtx) Err() error {
	if c.calls++; c.calls == c.at {
		c.hook()
	}
	return nil
}

// warmFixture leaves a hand-over of n disk entries and opens it again; the
// hottest is the last stored.
func warmFixture(t *testing.T, n, valueBytes, maxBytes int) *handoverFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "c.db")
	f := openHandoverFixture(t, path, maxBytes, DiskCacheConfig{MaxBufferBytes: 1 << 30, FlushBytes: 1 << 30})
	f.c.maxEntries = n + 10
	r := rand.New(rand.NewPCG(5, 6))
	for i := 0; i < n; i++ {
		value := make([]byte, valueBytes)
		for j := 0; j+8 <= len(value); j += 8 {
			v := r.Uint64()
			for k := 0; k < 8; k++ {
				value[j+k] = byte(v >> (8 * k))
			}
		}
		f.c.SetWithTTL(restoreKey(i), value, time.Hour)
	}
	f.dc.Flush()
	f.c.drainPromotions()
	if report, err := f.c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s"}); err != nil || report.Ranked != n {
		t.Fatalf("flush %+v %v", report, err)
	}
	f.close()
	next := openHandoverFixture(t, path, maxBytes, DiskCacheConfig{MaxBufferBytes: 1 << 30, FlushBytes: 1 << 30})
	next.c.maxEntries = n + 10
	return next
}

func plannedReads(keys ...string) []*diskRestoreRead {
	out := make([]*diskRestoreRead, len(keys))
	for i, key := range keys {
		out[i] = &diskRestoreRead{restoreEntry: restoreEntry{key: key}}
	}
	return out
}

func wideLimits() restoreLimits {
	return restoreLimits{budget: 1 << 40, room: 1 << 40, maxEntry: 1 << 30, bytes: restoreBatchBytes, workers: 4}
}

// conformance: cache-restart-handover
func TestRestore_BatchesAreBoundedByBytes(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 256<<20, DiskCacheConfig{})
	var keys []string
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("query:t:%d", i)
		size := 1 << 20
		if i == 7 {
			size = 6 << 20 // above one batch: a batch of its own
		}
		f.dc.Set(key, bytes.Repeat([]byte{byte(i)}, size), time.Hour)
		keys = append(keys, key)
		f.dc.Flush()
	}
	var batches []int
	for at := 0; at < len(keys); {
		reads := plannedReads(keys[at:]...)
		f.dc.planRestore(reads)
		n := f.dc.readRestoreBatch(reads, wideLimits())
		var decoded int
		for i, r := range reads[:n] {
			if r.skip != restoreKeep || len(r.value) == 0 || r.value[0] != byte(at+i) || r.raw != nil {
				t.Fatalf("read %d of batch at %d: skip %v, %d bytes", i, at, r.skip, len(r.value))
			}
			decoded += len(r.value)
		}
		if n < 1 || (n > 1 && decoded > restoreBatchBytes) {
			t.Fatalf("batch of %d entries holds %d bytes", n, decoded)
		}
		batches = append(batches, n)
		at += n
	}
	if fmt.Sprint(batches) != "[3 3 1 1 2]" {
		t.Fatalf("batches %v", batches)
	}
}

// conformance: cache-restart-handover
func TestRestore_BudgetRoomAndSizeLimitsSkipWithoutLosingLaterEntries(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	sizes := []int{100, 50_000, 700, 100}
	var keys []string
	for i, size := range sizes {
		key := fmt.Sprintf("query:t:%d", i)
		f.dc.Set(key, make([]byte, size), time.Hour)
		keys = append(keys, key)
	}
	f.dc.Flush()
	for name, tc := range map[string]struct {
		limits restoreLimits
		want   []RestoreSkip
	}{
		"too large": {restoreLimits{budget: 1 << 20, room: 1 << 20, maxEntry: 10_000}, []RestoreSkip{0, RestoreSkipTooLarge, 0, 0}},
		"budget":    {restoreLimits{budget: 150, room: 1 << 20, maxEntry: 1 << 20}, []RestoreSkip{0}},
		"room":      {restoreLimits{budget: 1 << 20, room: 50, maxEntry: 1 << 20}, []RestoreSkip{RestoreSkipMemoryFull, RestoreSkipMemoryFull, RestoreSkipMemoryFull, RestoreSkipMemoryFull}},
	} {
		tc.limits.bytes, tc.limits.workers = restoreBatchBytes, 2
		reads := plannedReads(keys...)
		f.dc.planRestore(reads)
		n := f.dc.readRestoreBatch(reads, tc.limits)
		if n != len(tc.want) {
			t.Fatalf("%s: consumed %d, want %d", name, n, len(tc.want))
		}
		for i, want := range tc.want {
			if reads[i].skip != want {
				t.Fatalf("%s: entry %d skip %v, want %v", name, i, reads[i].skip, want)
			}
		}
	}
	// A planned batch that ends at the budget leaves the rest for the next one,
	// which sees what was really restored.
	reads := plannedReads(keys[1], keys[2])
	f.dc.planRestore(reads)
	if n := f.dc.readRestoreBatch(reads, restoreLimits{budget: 50_100, room: 1 << 20, maxEntry: 1 << 20, bytes: restoreBatchBytes, workers: 1}); n != 1 || reads[1].skip != restoreKeep {
		t.Fatalf("consumed %d, second skip %v", n, reads[1].skip)
	}
}

// conformance: cache-restart-handover
func TestRestore_QueuedAndRewrittenValuesWinOverTheBatch(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	for _, k := range []string{"queued", "rewritten", "committed", "removed", "stable"} {
		f.dc.Set("query:t:"+k, []byte("old"), time.Hour)
	}
	f.dc.Flush()
	f.dc.Set("query:t:queued", []byte("queued-new"), time.Hour)
	reads := plannedReads("query:t:queued", "query:t:rewritten", "query:t:committed", "query:t:removed", "query:t:stable")
	f.dc.planRestore(reads)
	if n := f.dc.readRestoreBatch(reads, wideLimits()); n != 5 {
		t.Fatalf("consumed %d", n)
	}
	if string(reads[0].value) != "queued-new" {
		t.Fatalf("a queued value is fresher than its disk record: read %q", reads[0].value)
	}
	// Writes that land between the read and the admission.
	f.dc.Set("query:t:rewritten", []byte("newer"), time.Hour) // still queued
	f.dc.Set("query:t:committed", []byte("newer"), time.Hour)
	f.dc.writeMu.Lock()
	delete(f.dc.writeBuf, "query:t:rewritten")
	f.dc.writeMu.Unlock()
	f.dc.Set("query:t:rewritten", []byte("newest"), time.Hour)
	f.dc.writeMu.Lock()
	f.dc.index["query:t:removed"].invalid = true
	f.dc.writeMu.Unlock()
	generation, _ := f.c.diskPromotionGeneration()
	entries := make([]*restoreEntry, len(reads))
	for i := range reads {
		entries[i] = &reads[i].restoreEntry
	}
	var restored int64
	if !f.c.admitRestoreBatch(entries, generation, &restored, 1<<20, func([]*restoreEntry) { f.dc.restoreFence(reads) }) {
		t.Fatal("admission refused without a purge")
	}
	for key, want := range map[string]string{"queued": "queued-new", "stable": "old", "rewritten": "", "committed": "", "removed": ""} {
		e, ok := f.c.memoryEntry("query:t:" + key)
		if ok != (want != "") || (ok && string(e.value) != want) {
			t.Fatalf("%s: restored %v %q, want %q", key, ok, e.value, want)
		}
	}
	// After a commit the rewritten record has a new generation: still superseded.
	stale := plannedReads("query:t:stable")
	f.dc.planRestore(stale)
	f.dc.readRestoreBatch(stale, wideLimits())
	f.dc.Set("query:t:stable", []byte("replaced"), time.Hour)
	f.dc.Flush()
	f.dc.restoreFence(stale)
	if stale[0].skip != RestoreSkipSuperseded {
		t.Fatalf("a rewritten record: skip %v", stale[0].skip)
	}
}

// conformance: cache-restart-handover
func TestRestore_APurgeDuringTheWarmUpWins(t *testing.T) {
	t.Run("between batches", func(t *testing.T) {
		const n = 3 * restoreBatchKeys
		f := warmFixture(t, n, 64, 8<<20)
		ctx := &hookCtx{Context: context.Background(), at: 2, hook: f.c.PurgeAll}
		warm := f.c.WarmFromHandover(ctx, HandoverWarmOptions{Scope: "s", MaxBytes: 4 << 20})
		if warm.Reason != "purged" || warm.DiskEntries != restoreBatchKeys || warm.Progress.Skipped[RestoreSkipPurged] != n-restoreBatchKeys {
			t.Fatalf("warm %+v", warm)
		}
		if entries, bytes := f.c.Size(); entries != 0 || bytes != 0 {
			t.Fatalf("%d entries (%d bytes) survived the purge", entries, bytes)
		}
	})
	t.Run("between read and admission", func(t *testing.T) {
		f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
		generation, _ := f.c.diskPromotionGeneration()
		f.c.PurgeAll()
		var restored int64
		entry := &restoreEntry{key: "query:t:a", value: []byte("a"), expiry: time.Now().Add(time.Hour).UnixNano()}
		if f.c.admitRestoreBatch([]*restoreEntry{entry}, generation, &restored, 1<<20, nil) || restored != 0 {
			t.Fatal("a batch a purge crossed was admitted")
		}
		if _, ok := f.c.memoryEntry("query:t:a"); ok {
			t.Fatal("restored after the purge")
		}
	})
}

// conformance: cache-restart-handover
func TestRestore_LeavesTheDiskOrderRanked(t *testing.T) {
	const n = 40
	f := warmFixture(t, n, 64, 1<<20)
	order := func() []string {
		f.dc.writeMu.Lock()
		defer f.dc.writeMu.Unlock()
		var out []string
		for e := f.dc.newest; e != nil; e = e.next {
			out = append(out, e.key)
		}
		return out
	}
	before := order()
	if warm := f.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 1 << 20}); warm.DiskEntries != n {
		t.Fatalf("warm %+v", warm)
	}
	after := order()
	if fmt.Sprint(before) != fmt.Sprint(after) || after[0] != restoreKey(n-1) {
		t.Fatalf("the warm-up reordered the disk: hottest %q, newest %q", restoreKey(n-1), after[0])
	}
	if f.dc.Hits.Load() != 0 || f.dc.Misses.Load() != 0 {
		t.Fatal("the warm-up counted disk lookups")
	}
	f.c.mu.RLock()
	defer f.c.mu.RUnlock()
	i := n - 1
	for e := f.c.lruList.Front(); e != nil; e, i = e.Next(), i-1 {
		if e.Value.(*lruEntry).key != restoreKey(i) {
			t.Fatalf("memory order at rank %d: %q", n-1-i, e.Value.(*lruEntry).key)
		}
	}
}

// conformance: cache-restart-handover
func TestRestore_StopsAtAFullMemoryTier(t *testing.T) {
	const n = 3 * restoreBatchKeys
	f := warmFixture(t, n, 64, 8<<20)
	f.c.maxEntries = 100
	warm := f.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 4 << 20})
	if warm.Reason != RestoreDone || warm.DiskEntries != 100 || warm.Progress.Skipped[RestoreSkipMemoryFull] != n-100 {
		t.Fatalf("warm %+v", warm)
	}
}

// conformance: cache-restart-handover
func TestRestore_ProgressReadsRightWhileItRuns(t *testing.T) {
	const n = 3 * restoreBatchKeys
	f := warmFixture(t, n, 64, 8<<20)
	var plan, mid HandoverStats
	ctx := &hookCtx{Context: context.Background(), at: 2, hook: func() { mid = f.c.HandoverStats() }}
	deadline, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx.Context = deadline
	warm := f.c.WarmFromHandover(ctx, HandoverWarmOptions{Scope: "s", MaxBytes: 4 << 20, OnPlan: func(RestoreProgress) { plan = f.c.HandoverStats() }})
	p := plan.Restore
	if p.Outcome != RestoreRunning || p.Source != RestoreSourceDisk || p.PlannedEntries != n || p.PlannedBytes <= 0 || p.BudgetBytes != 4<<20 || p.ProcessedEntries != 0 || p.Timeout <= 50*time.Second {
		t.Fatalf("plan %+v", p)
	}
	m := mid.Restore
	if mid.WarmDone || m.Outcome != RestoreRunning || m.ProcessedEntries != restoreBatchKeys || mid.WarmDiskEntries != restoreBatchKeys ||
		m.RestoredBytes <= 0 || m.Fraction() < 0.3 || m.Fraction() > 0.4 || m.Elapsed <= 0 || m.BytesPerSecond() <= 0 {
		t.Fatalf("mid-way %+v fraction %v", mid, m.Fraction())
	}
	if eta, ok := m.Remaining(); !ok || eta < m.Elapsed {
		t.Fatalf("a third done: remaining %v of elapsed %v", eta, m.Elapsed)
	}
	end := f.c.HandoverStats()
	if !end.WarmDone || end.Restore.Outcome != RestoreDone || end.Restore.ProcessedEntries != n || end.Restore.ProcessedBytes != p.PlannedBytes ||
		end.Restore.Fraction() != 1 || end.WarmBytes != warm.Bytes || end.WarmDuration != warm.Duration || end.IndexRebuildEntries != n || end.IndexRebuildDuration <= 0 {
		t.Fatalf("end %+v", end)
	}
	if eta, ok := end.Restore.Remaining(); !ok || eta != 0 {
		t.Fatalf("done: remaining %v", eta)
	}
}

// conformance: cache-restart-handover
func TestRestore_ReadAheadCoversTheRecordsAndStops(t *testing.T) {
	const n = 300
	f := warmFixture(t, n, 4096, 8<<20)
	items := f.dc.handover.items
	ranges := f.dc.restoreRanges(items)
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		// The values are incompressible, so the record's last 64 bytes are the
		// value's: they lie inside one of the ranges.
		f.dc.writeMu.Lock()
		size := int(f.dc.index[item.key].size) - len(item.key)
		f.dc.writeMu.Unlock()
		reads := plannedReads(item.key)
		f.dc.planRestore(reads)
		f.dc.readRestoreBatch(reads, wideLimits())
		tail := reads[0].value[len(reads[0].value)-64:]
		found := false
		for _, r := range ranges {
			if at := bytes.Index(raw[r.from:r.to], tail); at >= 0 && r.to-r.from >= int64(size) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s: record outside the read-ahead ranges %v", item.key, ranges)
		}
	}
	before := runtime.NumGoroutine()
	sizes := f.dc.storedSizes(items)
	for _, cancelFirst := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if cancelFirst {
			cancel()
		}
		ahead := f.dc.startRestoreReadahead(ctx, items, sizes, 1<<40)
		ahead.advance(10, 1000)
		ahead.stop()
		cancel()
	}
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, %d before the read-ahead", runtime.NumGoroutine(), before)
		}
	}
}

func TestRestore_MergeFileRanges(t *testing.T) {
	got := mergeFileRanges([]fileRange{{10 << 20, 11 << 20}, {0, 100}, {50, 200}, {200 + restoreReadaheadGap, 200 + restoreReadaheadGap + 1}})
	want := []fileRange{{0, 200 + restoreReadaheadGap + 1}, {10 << 20, 11 << 20}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("merged %v, want %v", got, want)
	}
}

// The warm-up keeps no second copy of what it restores: beside the restored
// entries it holds the read-ahead's buffers and one batch's records. 64 MiB of
// incompressible values restore with a live-heap peak within 24 MiB of what the
// restored entries themselves take (a copy of every stored record would add 64).
// conformance: cache-restart-handover
func TestHandover_WarmPeakMemoryIsOneBatch(t *testing.T) {
	const n, size = 256, 256 << 10
	f := warmFixture(t, n, size, 256<<20)
	defer debug.SetGCPercent(debug.SetGCPercent(5))
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	stop := heapPeakSampler()
	warm := f.c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: 128 << 20})
	peak := stop()
	if warm.DiskEntries != n {
		t.Fatalf("warm %+v", warm)
	}
	over := int64(peak) - int64(before.HeapAlloc) - warm.Bytes
	t.Logf("restored %d MiB, live heap peak %d MiB above them", warm.Bytes>>20, over>>20)
	if over > 24<<20 {
		t.Fatalf("warm-up peak %d MiB above the restored entries: bounded by a batch and the read-ahead buffers", over>>20)
	}
}

// One batch is admitted under one hold of the memory lock: its duration bounds
// how long a request can wait.
// conformance: cache-restart-handover
func TestRestore_AdmissionHoldIsShort(t *testing.T) {
	c := NewWithMaxBytes(time.Minute, 10*restoreBatchKeys, 64<<20)
	defer c.Close()
	generation, _ := c.diskPromotionGeneration()
	var worst time.Duration
	var restored int64
	for batch := 0; batch < 10; batch++ {
		entries := make([]*restoreEntry, restoreBatchKeys)
		for i := range entries {
			entries[i] = &restoreEntry{key: restoreKey(batch*restoreBatchKeys + i), value: []byte("v"), expiry: time.Now().Add(time.Hour).UnixNano()}
		}
		start := time.Now()
		c.admitRestoreBatch(entries, generation, &restored, 1<<30, nil)
		worst = max(worst, time.Since(start))
	}
	t.Logf("longest admission of %d entries: %v", restoreBatchKeys, worst)
	if worst > 250*time.Millisecond {
		t.Fatalf("a batch held the memory lock for %v", worst)
	}
}

// The restore's throughput gate, off by default (it needs the disk and memory
// of a real-size run): with LVP_RESTORE_GATE=1 it restores
// LVP_RESTORE_BENCH_MB MiB (default 2048) of decoded values and fails below
// 400 MiB/s, which is 2 GiB in 5 s. LVP_RESTORE_COLD=1 drops the file from the
// page cache first.
//
//	LVP_RESTORE_GATE=1 GOMEMLIMIT=6GiB GOMAXPROCS=4 go test -run TestRestoreWarm_ThroughputGate -v ./internal/cache/
//
// conformance: cache-restart-handover
func TestRestoreWarm_ThroughputGate(t *testing.T) {
	if os.Getenv("LVP_RESTORE_GATE") != "1" {
		t.Skip("set LVP_RESTORE_GATE=1 to run the real-size restore gate")
	}
	if os.Getenv("LVP_RESTORE_BENCH_MB") == "" {
		t.Setenv("LVP_RESTORE_BENCH_MB", "2048")
	}
	const floor = 400 << 20 // decoded bytes per second
	size := restoreBenchSize(t)
	cold := os.Getenv("LVP_RESTORE_COLD") == "1"
	for _, codec := range []string{"s2", "raw"} {
		f := openRestoreFixture(t, codec, size)
		path := filepath.Join(filepath.Dir(f.path), "restore-gate.db")
		copyRestoreFile(t, f.path, path, cold)
		dc, err := NewDiskCache(restoreDiskConfig(path, codec))
		if err != nil {
			t.Fatal(err)
		}
		c := NewWithMaxBytes(time.Hour, f.entries+10, int(2*f.decoded+(1<<20)))
		c.SetL2(dc)
		warm := c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: f.decoded})
		c.Close()
		_ = dc.Close()
		_ = os.Remove(path)
		rate := warm.Progress.BytesPerSecond()
		t.Logf("codec=%s cold=%v: %d entries, %d MiB (%d MiB on disk) in %v = %.0f MiB/s", codec, cold, warm.DiskEntries, warm.Bytes>>20, f.encoded>>20, warm.Duration, rate/(1<<20))
		if warm.DiskEntries != f.entries || rate < floor {
			t.Fatalf("codec=%s: %+v at %.0f MiB/s, want all %d entries at 400 MiB/s or more", codec, warm, rate/(1<<20), f.entries)
		}
	}
}

// Records of the legacy bucket, expired and damaged records, and a closed disk
// read as a lookup reads them: a value, or a skip that drops the bad record.
// conformance: cache-restart-handover
func TestRestore_LegacyExpiredDamagedAndClosed(t *testing.T) {
	f := openHandoverFixture(t, filepath.Join(t.TempDir(), "c.db"), 1<<20, DiskCacheConfig{})
	for _, k := range []string{"legacy", "expired", "damaged", "gone"} {
		f.dc.Set("query:t:"+k, []byte("value-"+k), time.Hour)
	}
	f.dc.Flush()
	rewrite := func(key string, change func(tx *bolt.Tx, k []byte) error) {
		if err := f.dc.db.Update(func(tx *bolt.Tx) error { return change(tx, []byte("query:t:"+key)) }); err != nil {
			t.Fatal(err)
		}
	}
	rewrite("legacy", func(tx *bolt.Tx, k []byte) error {
		old, ok := encodeDiskEntry(diskEntry{Value: []byte("old-format"), ExpiresAt: time.Now().Add(time.Hour).UnixNano()})
		if !ok {
			t.Fatal("legacy encoding")
		}
		if err := tx.Bucket(dataBucket).Delete(k); err != nil {
			return err
		}
		return tx.Bucket(legacyDataBucket).Put(k, old)
	})
	f.dc.compression = false
	rewrite("expired", func(tx *bolt.Tx, k []byte) error {
		raw, err := encodeDiskRecord(diskEntry{Value: []byte("x"), ExpiresAt: time.Now().Add(-time.Minute).UnixNano()}, diskCodecRaw)
		if err != nil {
			return err
		}
		return tx.Bucket(dataBucket).Put(k, raw)
	})
	rewrite("damaged", func(tx *bolt.Tx, k []byte) error {
		raw := bytes.Clone(tx.Bucket(dataBucket).Get(k))
		raw[len(raw)-1] ^= 1
		return tx.Bucket(dataBucket).Put(k, raw)
	})
	rewrite("gone", func(tx *bolt.Tx, k []byte) error { return tx.Bucket(dataBucket).Delete(k) })

	reads := plannedReads("query:t:legacy", "query:t:expired", "query:t:damaged", "query:t:gone", "query:t:never")
	f.dc.planRestore(reads)
	if n := f.dc.readRestoreBatch(reads, wideLimits()); n != len(reads) {
		t.Fatalf("consumed %d", n)
	}
	want := []RestoreSkip{restoreKeep, RestoreSkipExpired, RestoreSkipUnreadable, RestoreSkipUnreadable, RestoreSkipUnreadable}
	for i, r := range reads {
		if r.skip != want[i] {
			t.Fatalf("%s: skip %v, want %v", r.key, r.skip, want[i])
		}
	}
	if string(reads[0].value) != "old-format" {
		t.Fatalf("legacy value %q", reads[0].value)
	}
	f.dc.writeMu.Lock()
	for _, k := range []string{"expired", "damaged", "gone"} {
		if e := f.dc.index["query:t:"+k]; e != nil && !e.invalid {
			f.dc.writeMu.Unlock()
			t.Fatalf("%s: the bad record stayed in the index", k)
		}
	}
	f.dc.writeMu.Unlock()

	// A queued value that already expired is not restored.
	f.dc.writeMu.Lock()
	f.dc.writeBuf["query:t:stale"] = diskEntry{Value: []byte("v"), ExpiresAt: time.Now().Add(-time.Second).UnixNano(), state: &diskEntryState{}}
	f.dc.writeMu.Unlock()
	stale := plannedReads("query:t:stale")
	f.dc.planRestore(stale)
	if stale[0].skip != RestoreSkipExpired {
		t.Fatalf("expired queued value: skip %v", stale[0].skip)
	}

	f.c.Close()
	if err := f.dc.Close(); err != nil {
		t.Fatal(err)
	}
	closed := plannedReads("query:t:legacy")
	f.dc.planRestore(closed)
	failed := plannedReads("query:t:legacy")
	if closed[0].skip != RestoreSkipUnreadable || f.dc.readRestoreBatch(failed, wideLimits()) != 1 || failed[0].skip != RestoreSkipUnreadable {
		t.Fatalf("closed disk: %v %v", closed[0].skip, failed[0].skip)
	}
	if ranges := f.dc.restoreRanges([]handoverItem{{key: "query:t:legacy"}}); len(ranges) != 0 {
		t.Fatalf("read-ahead ranges of a closed disk: %v", ranges)
	}
	f.c, f.dc = nil, nil
}

// The exporters' label values are fixed.
// conformance: cache-restart-handover
func TestRestore_ReasonsAndOutcomesAreFixedLabels(t *testing.T) {
	var names []string
	for _, reason := range RestoreSkipReasons() {
		names = append(names, reason.String())
	}
	if got := fmt.Sprint(names); got != "[expired superseded too_large over_budget memory_full other_scope unreadable deadline purged clock_behind_flush peer_busy]" {
		t.Fatalf("reasons %s", got)
	}
	if got := fmt.Sprint(RestoreOutcomes()); got != "[running done deadline purged clock_behind_flush none]" {
		t.Fatalf("outcomes %s", got)
	}
	if RestoreSkip(200).String() != "unknown" {
		t.Fatal("unknown reason")
	}
	var idle RestoreProgress
	if _, ok := idle.Remaining(); ok || idle.Fraction() != 0 || idle.BytesPerSecond() != 0 {
		t.Fatal("no restore yet: nothing to estimate")
	}
}
