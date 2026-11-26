package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// fillFleet writes n keys on node from (pushed to their replicas) and waits
// for every replica copy to reach its disk. It returns the keys.
func fillFleet(t *testing.T, f *replFleet, from, n, size int, ttl time.Duration) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("bulk-key-%04d", i)
		value := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		f.nodes[from].c.SetWithTTL(keys[i], value, ttl)
	}
	waitFor(t, "every push", func() bool {
		ok, _ := f.pushes()
		want := 0
		for _, key := range keys {
			for _, idx := range f.replicaIdx(key) {
				if idx != from {
					want++
				}
			}
		}
		return ok >= int64(want)
	})
	for _, nd := range f.nodes {
		nd.dc.Flush()
	}
	return keys
}

func ownedBy(f *replFleet, keys []string, node int) (owned, other []string) {
	for _, key := range keys {
		mine := false
		for _, idx := range f.replicaIdx(key) {
			mine = mine || idx == node
		}
		if mine {
			owned = append(owned, key)
		} else {
			other = append(other, key)
		}
	}
	return owned, other
}

func inMemory(c *Cache, key string) ([]byte, entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	return e.value, e, ok
}

func restoreCtx(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func checkProgressSums(t *testing.T, p RestoreProgress) {
	t.Helper()
	if p.ProcessedEntries != p.PlannedEntries || p.PlannedEntries != p.RestoredEntries+p.SkippedEntries() {
		t.Fatalf("planned %d, processed %d, restored %d + skipped %d (%v)", p.PlannedEntries, p.ProcessedEntries, p.RestoredEntries, p.SkippedEntries(), p.Skipped)
	}
}

// A replica restarted with an empty disk pulls every key it replicates from
// its peers, into memory and its disk, with the remaining TTL, and nothing it
// does not replicate.
//
// conformance: cache-restart-handover
func TestPeerRestore_WipedDiskRefillsReplicatedKeys(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	keys := fillFleet(t, f, 0, 300, 2048, 10*time.Minute)
	f.restart(2, true)
	nd := f.nodes[2]
	if !nd.c.PeerRestoreWanted() {
		t.Fatal("an empty disk and peers should want a peer restore")
	}
	report := nd.c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Outcome != RestoreDone {
		t.Fatalf("outcome %q, peers %+v", report.Outcome, report.Peers)
	}
	owned, other := ownedBy(f, keys, 2)
	if len(owned) == 0 || len(other) == 0 {
		t.Fatalf("owned %d, other %d", len(owned), len(other))
	}
	for _, key := range owned {
		value, e, ok := inMemory(nd.c, key)
		if !ok || len(value) != 2048 {
			t.Fatalf("%s not restored", key)
		}
		if remaining := time.Until(e.expiresAt); remaining > 10*time.Minute || remaining < 9*time.Minute {
			t.Fatalf("%s restored with %v left", key, remaining)
		}
		if e.memOnly || e.shadow {
			t.Fatalf("%s restored memory-only %v shadow %v", key, e.memOnly, e.shadow)
		}
	}
	for _, key := range other {
		if _, _, ok := inMemory(nd.c, key); ok {
			t.Fatalf("%s restored on a replica that does not hold it", key)
		}
	}
	nd.dc.Flush()
	for _, key := range owned {
		if _, ok := nd.dc.Get(key); !ok {
			t.Fatalf("%s not written to the new disk", key)
		}
	}
	p := report.Progress
	checkProgressSums(t, p)
	if p.RestoredEntries != int64(len(owned)) || report.DiskWrites != int64(len(owned)) {
		t.Fatalf("restored %d, disk writes %d, want %d", p.RestoredEntries, report.DiskWrites, len(owned))
	}
	if stats := nd.c.HandoverStats().PeerRestore; stats.Outcome != RestoreDone || stats.Source != RestoreSourcePeer || stats.RestoredEntries != p.RestoredEntries {
		t.Fatalf("published progress %+v", stats)
	}
	if nd.c.PeerRestoreRunning() {
		t.Fatal("still marked restoring")
	}
}

// At a replication factor of 3 every key is held by both other replicas; each
// is sent once, by the first of them in ring order.
//
// conformance: cache-restart-handover
func TestPeerRestore_EachKeyCrossesTheNetworkOnce(t *testing.T) {
	f := newReplFleet(t, 3, 3, true, time.Second)
	keys := fillFleet(t, f, 0, 200, 512, 10*time.Minute)
	f.restart(1, true)
	report := f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Outcome != RestoreDone || report.Progress.RestoredEntries != int64(len(keys)) {
		t.Fatalf("outcome %q restored %d of %d", report.Outcome, report.Progress.RestoredEntries, len(keys))
	}
	sent := f.nodes[0].pc.bulk.records.Load() + f.nodes[2].pc.bulk.records.Load()
	if sent != int64(len(keys)) || f.nodes[0].pc.bulk.records.Load() == 0 || f.nodes[2].pc.bulk.records.Load() == 0 {
		t.Fatalf("sent %d + %d records for %d keys", f.nodes[0].pc.bulk.records.Load(), f.nodes[2].pc.bulk.records.Load(), len(keys))
	}
	if report.Progress.Skipped[RestoreSkipSuperseded] != 0 {
		t.Fatalf("%d keys arrived twice", report.Progress.Skipped[RestoreSkipSuperseded])
	}
}

// Serving a bulk stream is no lookup: the serving replica's memory and disk
// hit counters, its LRU order and its disk recency stay as they were.
//
// conformance: cache-restart-handover
func TestPeerRestore_ServingTouchesNoCounterOrRecency(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 100, 1024, 10*time.Minute)
	server := f.nodes[0]
	lru := func() []string {
		server.c.mu.RLock()
		defer server.c.mu.RUnlock()
		var out []string
		for e := server.c.lruList.Front(); e != nil; e = e.Next() {
			out = append(out, e.Value.(*lruEntry).key)
		}
		return out
	}
	diskOrder := func() []string {
		server.dc.writeMu.Lock()
		defer server.dc.writeMu.Unlock()
		var out []string
		for e := server.dc.newest; e != nil; e = e.next {
			out = append(out, e.key)
		}
		return out
	}
	beforeLRU, beforeDisk := lru(), diskOrder()
	beforeStats := server.c.Stats()
	beforeDiskHits := server.dc.Hits.Load()
	f.restart(1, true)
	report := f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Outcome != RestoreDone || report.Progress.RestoredEntries != 100 {
		t.Fatalf("outcome %q restored %d", report.Outcome, report.Progress.RestoredEntries)
	}
	if fmt.Sprint(lru()) != fmt.Sprint(beforeLRU) || fmt.Sprint(diskOrder()) != fmt.Sprint(beforeDisk) {
		t.Fatal("serving moved the LRU or the disk recency")
	}
	if after := server.c.Stats(); after.L1 != beforeStats.L1 || after.L2 != beforeStats.L2 || server.dc.Hits.Load() != beforeDiskHits {
		t.Fatalf("serving counted lookups: L1 %+v -> %+v, L2 %+v -> %+v", beforeStats.L1, after.L1, beforeStats.L2, after.L2)
	}
}

// What the restoring replica already holds wins over what a peer sends, and
// so does a value written while the restore runs.
//
// conformance: cache-restart-handover
func TestPeerRestore_NeverOverwritesFresherLocalData(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	keys := fillFleet(t, f, 0, 50, 256, 10*time.Minute)
	f.restart(1, true)
	nd := f.nodes[1]
	nd.c.SetLocalAndDiskWithTTL(keys[3], []byte("fresher"), time.Minute)
	report := nd.c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Progress.Skipped[RestoreSkipSuperseded] != 1 || report.Progress.RestoredEntries != 49 {
		t.Fatalf("restored %d, superseded %d", report.Progress.RestoredEntries, report.Progress.Skipped[RestoreSkipSuperseded])
	}
	if v, _, ok := inMemory(nd.c, keys[3]); !ok || string(v) != "fresher" {
		t.Fatalf("memory holds %q", v)
	}
	nd.dc.Flush()
	if v, ok := nd.dc.Get(keys[3]); !ok || string(v) != "fresher" {
		t.Fatalf("disk holds %q", v)
	}
}

// A memory-only answer reaches a replica of the same scope and stays
// memory-only there; another scope gets none; entries the push filter keeps
// on their replica (forwarded-credential keys) never leave it.
//
// conformance: cache-restart-handover
func TestPeerRestore_MemoryOnlyScopeAndPushFilter(t *testing.T) {
	for _, tc := range []struct {
		name            string
		server, client  string
		wantMemoryValue bool
	}{
		{"same scope", "build-a", "build-a", true},
		{"other scope", "build-a", "build-b", false},
		{"no scope", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplFleet(t, 2, 2, true, time.Second)
			for _, nd := range f.nodes {
				nd.pc.SetPushFilter(func(key string) bool { return !strings.Contains(key, ":auth:user.") })
			}
			server := f.nodes[0]
			server.pc.SetBulkScope(func() string { return tc.server })
			server.c.SetLocalOnlyRestorableWithTTL("mem-answer", []byte("big answer"), 10*time.Minute)
			server.c.SetLocalOnlyWithTTL("mem-marker", []byte("helper"), 10*time.Minute)
			server.c.SetLocalAndDiskWithTTL("q:auth:user.alice", []byte("secret"), 10*time.Minute)
			server.c.SetLocalAndDiskWithTTL("plain", []byte("value"), 10*time.Minute)
			server.dc.Flush()
			f.restart(1, true)
			nd := f.nodes[1]
			report := nd.c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{Scope: tc.client, MaxBytes: 32 << 20})
			if report.Outcome != RestoreDone {
				t.Fatalf("outcome %q %+v", report.Outcome, report.Peers)
			}
			if _, _, ok := inMemory(nd.c, "plain"); !ok {
				t.Fatal("the plain entry was not restored")
			}
			for _, key := range []string{"mem-marker", "q:auth:user.alice"} {
				if _, _, ok := inMemory(nd.c, key); ok {
					t.Fatalf("%s left its replica", key)
				}
			}
			_, e, ok := inMemory(nd.c, "mem-answer")
			if ok != tc.wantMemoryValue {
				t.Fatalf("memory-only answer restored %v, want %v", ok, tc.wantMemoryValue)
			}
			if ok && (!e.memOnly || !e.restorable) {
				t.Fatal("the memory-only answer was not kept memory-only")
			}
			nd.dc.Flush()
			if _, ok := nd.dc.Get("mem-answer"); ok {
				t.Fatal("a memory-only answer was written to disk")
			}
		})
	}
}

// A purge on the restoring replica while the stream runs stops the restore,
// and nothing is admitted after it.
//
// conformance: cache-restart-handover
func TestPeerRestore_PurgeDuringTheRestoreWins(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 400, 4096, 10*time.Minute)
	f.nodes[0].pc.bulk.rate = newByteRate(256 << 10) // about 6 s for the set
	f.restart(1, true)
	nd := f.nodes[1]
	done := make(chan PeerRestoreReport)
	go func() {
		done <- nd.c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	}()
	waitFor(t, "a first batch", func() bool { return nd.c.HandoverStats().PeerRestore.ProcessedEntries > 0 })
	nd.c.PurgeAll()
	report := <-done
	if report.Outcome != "purged" {
		t.Fatalf("outcome %q", report.Outcome)
	}
	if n := len(nd.c.entries); n != 0 {
		t.Fatalf("%d entries after the purge", n)
	}
	nd.dc.Flush()
	if entries, _ := nd.dc.Size(); entries != 0 {
		t.Fatalf("%d disk records after the purge", entries)
	}
	checkProgressSums(t, report.Progress)
}

// The restore stops at its deadline with what arrived, and returns promptly.
//
// conformance: cache-restart-handover
func TestPeerRestore_DeadlineStopsAndKeepsTheHottest(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 400, 4096, 10*time.Minute)
	f.nodes[0].pc.bulk.rate = newByteRate(256 << 10)
	f.restart(1, true)
	nd := f.nodes[1]
	start := time.Now()
	report := nd.c.RestoreFromPeers(restoreCtx(t, 700*time.Millisecond), PeerRestoreOptions{MaxBytes: 32 << 20})
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %v past a 700 ms deadline", took)
	}
	if report.Outcome != "deadline" || report.Progress.RestoredEntries == 0 || report.Progress.Skipped[RestoreSkipDeadline] == 0 {
		t.Fatalf("outcome %q restored %d skipped %v", report.Outcome, report.Progress.RestoredEntries, report.Progress.Skipped)
	}
	checkProgressSums(t, report.Progress)
}

// The byte budget ends the restore with the hottest entries in, and the
// serving peer is asked to stop.
//
// conformance: cache-restart-handover
func TestPeerRestore_BudgetKeepsTheHottest(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	keys := fillFleet(t, f, 0, 200, 4096, 10*time.Minute)
	// Touch the last keys so they are the server's hottest.
	for _, key := range keys[150:] {
		f.nodes[0].c.Get(key)
	}
	f.nodes[0].c.drainPromotions()
	f.restart(1, true)
	nd := f.nodes[1]
	report := nd.c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 50*int64(entrySize(keys[0], make([]byte, 4096))) + 100})
	if report.Outcome != RestoreDone || report.Progress.RestoredEntries != 50 {
		t.Fatalf("outcome %q restored %d", report.Outcome, report.Progress.RestoredEntries)
	}
	for _, key := range keys[150:] {
		if _, _, ok := inMemory(nd.c, key); !ok {
			t.Fatalf("hot key %s not restored first", key)
		}
	}
	checkProgressSums(t, report.Progress)
}

// Peers that do not know the route (an older build), refuse it, or are not
// there are left out at once: the restore ends quickly with nothing.
//
// conformance: cache-restart-handover
func TestPeerRestore_OldUnreachableAndRestoringPeersFallBackFast(t *testing.T) {
	old := httptest.NewServer(http.NotFoundHandler())
	defer old.Close()
	restoring := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(peerBulkVersionHeader, peerBulkVersion)
		w.Header().Set(peerBulkStateHeader, "restoring")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer restoring.Close()
	dead := httptest.NewServer(http.NotFoundHandler())
	deadAddr := dead.Listener.Addr().String()
	dead.Close()
	self := httptest.NewServer(http.NotFoundHandler())
	defer self.Close()
	selfAddr := self.Listener.Addr().String()
	peers := []string{selfAddr, old.Listener.Addr().String(), restoring.Listener.Addr().String(), deadAddr}
	c := NewWithMaxBytes(time.Hour, 1000, 64<<20)
	defer c.Close()
	pc := NewPeerCache(PeerConfig{SelfAddr: selfAddr, DiscoveryType: "static", StaticPeers: strings.Join(peers, ","), Timeout: time.Second})
	defer pc.Close()
	c.SetL3(pc)
	start := time.Now()
	report := c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 1 << 20})
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v", took)
	}
	got := map[string]string{}
	for _, p := range report.Peers {
		got[p.Addr] = p.Outcome
	}
	want := map[string]string{peers[1]: PeerRestoreUnsupported, peers[2]: PeerRestoreRestoring, peers[3]: PeerRestoreUnreachable}
	if fmt.Sprint(got) != fmt.Sprint(want) || report.Outcome != RestoreNone {
		t.Fatalf("outcome %q peers %v, want %v", report.Outcome, got, want)
	}
}

// Replicas that all restore at once answer each other "restoring" and all end
// at once: none waits on another.
//
// conformance: cache-restart-handover
func TestPeerRestore_AllRestartingTogetherNoDeadlock(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	for i := range f.nodes {
		f.restart(i, true)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for _, nd := range f.nodes {
		wg.Add(1)
		go func(c *Cache) {
			defer wg.Done()
			c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 1 << 20})
		}(nd.c)
	}
	wg.Wait()
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("three empty replicas took %v", took)
	}
}

// A serving replica streams to at most its stream limit at once; a requester
// beyond it is answered busy and served after a retry.
//
// conformance: cache-restart-handover
func TestPeerRestore_BusyPeerIsRetried(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 20, 512, 10*time.Minute)
	server := f.nodes[0].pc
	server.bulk.slots = make(chan struct{}, 1)
	server.bulk.slots <- struct{}{} // one stream in use
	go func() {
		time.Sleep(1500 * time.Millisecond)
		<-server.bulk.slots
	}()
	f.restart(1, true)
	report := f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Outcome != RestoreDone || report.Progress.RestoredEntries != 20 || report.Peers[0].Attempts < 2 {
		t.Fatalf("outcome %q restored %d peers %+v", report.Outcome, report.Progress.RestoredEntries, report.Peers)
	}
	if server.bulk.rejected[bulkRejectBusy].Load() == 0 {
		t.Fatal("no busy answer counted")
	}
}

// A replica that restores from peers refuses to serve a bulk stream, and one
// that stops serving (its HTTP server shuts down) ends its streams.
//
// conformance: cache-restart-handover
func TestPeerRestore_ServerRefusesWhileRestoringAndStopsOnShutdown(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 400, 4096, 10*time.Minute)
	f.nodes[0].c.handoverStats.peerRestoring.Store(true)
	f.restart(1, true)
	report := f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Outcome != RestoreNone || report.Peers[0].Outcome != PeerRestoreRestoring {
		t.Fatalf("outcome %q peers %+v", report.Outcome, report.Peers)
	}
	f.nodes[0].c.handoverStats.peerRestoring.Store(false)
	f.nodes[0].pc.bulk.rate = newByteRate(256 << 10)
	done := make(chan PeerRestoreReport)
	go func() {
		done <- f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	}()
	waitFor(t, "a first batch", func() bool { return f.nodes[1].c.HandoverStats().PeerRestore.RestoredEntries > 0 })
	f.nodes[0].pc.StopBulk()
	select {
	case report = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream did not end after StopBulk")
	}
	if report.Peers[0].Outcome != PeerRestoreTruncated || report.Progress.RestoredEntries == 0 {
		t.Fatalf("peer outcome %q, restored %d", report.Peers[0].Outcome, report.Progress.RestoredEntries)
	}
	checkProgressSums(t, report.Progress)
	if f.nodes[0].pc.bulk.streams[bulkEndStopped].Load() != 1 {
		t.Fatal("the stopped stream was not counted")
	}
}

// Every goroutine of a restore and of the streams it pulled has returned.
//
// conformance: cache-restart-handover
func TestPeerRestore_LeavesNoGoroutines(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	fillFleet(t, f, 0, 50, 512, 10*time.Minute)
	f.restart(2, true)
	before := runtime.NumGoroutine()
	f.nodes[2].c.RestoreFromPeers(restoreCtx(t, 10*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	f.nodes[2].pc.bulkHTTP().CloseIdleConnections()
	waitFor(t, "goroutines to return", func() bool { return runtime.NumGoroutine() <= before+2 })
}

var _ = io.Discard
