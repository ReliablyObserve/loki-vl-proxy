package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
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
	f.nodes[0].pc.bulk.gov.fixed = 256 << 10 // about 6 s for the set
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
	f.nodes[0].pc.bulk.gov.fixed = 256 << 10
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
	f.nodes[0].pc.bulk.gov.fixed = 256 << 10
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

// The requester's ring built by a serving peer places every key as the
// requester's own peer cache does.
//
// conformance: cache-restart-handover
func TestPeerBulkRequest_RingMatchesThePeerCacheRing(t *testing.T) {
	peers := []string{"10.0.0.3:3100", "10.0.0.1:3100", "10.0.0.2:3100", "10.0.0.4:3100"}
	pc := NewPeerCache(PeerConfig{SelfAddr: peers[0], DiscoveryType: "static", StaticPeers: strings.Join(peers, ","), ReplicationFactor: 3})
	defer pc.Close()
	req := bulkRequest{Version: 1, For: peers[0], Peers: pc.Peers(), Replication: 3}
	ring := req.ringFor(peers[1])
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if fmt.Sprint(ring.ring.getN(key, 3)) != fmt.Sprint(pc.replicasOf(key)) {
			t.Fatalf("%s: %v vs %v", key, ring.ring.getN(key, 3), pc.replicasOf(key))
		}
	}
}

// Bulk requests outside the bounds are refused before any work.
//
// conformance: cache-restart-handover
func TestPeerBulkRequest_Validation(t *testing.T) {
	many := make([]string, peerBulkMaxPeers+1)
	for i := range many {
		many[i] = fmt.Sprintf("10.0.%d.%d:3100", i/250, i%250)
	}
	for name, body := range map[string]string{
		"version":     `{"version":2,"for":"a:1","peers":["a:1","b:1"],"replication":2}`,
		"replication": `{"version":1,"for":"a:1","peers":["a:1","b:1"],"replication":9}`,
		"no peers":    `{"version":1,"for":"a:1","peers":[],"replication":2}`,
		"bad addr":    `{"version":1,"for":"a:1","peers":["a:1","nohost"],"replication":2}`,
		"negative":    `{"version":1,"for":"a:1","peers":["a:1","b:1"],"replication":2,"max_bytes":-1}`,
		"not json":    `{`,
		"too many":    fmt.Sprintf(`{"version":1,"for":"a:1","peers":["%s"],"replication":2}`, strings.Join(many, `","`)),
	} {
		if _, err := decodeBulkRequest(strings.NewReader(body)); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	ok, err := decodeBulkRequest(strings.NewReader(`{"version":1,"for":"a:1","peers":["a:1","b:1"],"replication":2}`))
	if err != nil || !ok.onRing("b:1") || ok.onRing("c:1") || ok.onRing("a:1") {
		t.Fatalf("valid request: %v", err)
	}
}

// discardWriter is a response writer that drops the body, so a test measures
// what the serving side holds and not the body.
type discardWriter struct {
	header http.Header
	bytes  int64
}

func (w *discardWriter) Header() http.Header         { return w.header }
func (w *discardWriter) WriteHeader(int)             {}
func (w *discardWriter) Write(p []byte) (int, error) { w.bytes += int64(len(p)); return len(p), nil }

// Serving a stream holds about one batch, whatever the size of what it sends:
// 64 MiB of disk records stream with a live heap below 24 MiB above the
// cache's own.
//
// conformance: cache-restart-handover
func TestPeerBulkServe_PeakMemoryIsOneBatch(t *testing.T) {
	quietRestoreLogs(t)
	f := newReplFleet(t, 2, 2, true, time.Second)
	server := f.nodes[0]
	value := restoreCorpus(1 << 20)
	for i := 0; i < 64; i++ {
		server.dc.Set(fmt.Sprintf("big-%03d", i), value, time.Hour)
		server.dc.Flush()
	}
	if entries, _ := server.dc.Size(); entries != 64 {
		t.Fatalf("the disk holds %d records", entries)
	}
	req := bulkRequest{Version: 1, For: f.addr(1), Peers: []string{f.addr(0), f.addr(1)}, Replication: 2}
	ring := req.ringFor(f.addr(0))
	debug.FreeOSMemory()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	previous := debug.SetGCPercent(5)
	defer debug.SetGCPercent(previous)
	stop := heapPeakSampler()
	w := &discardWriter{header: http.Header{}}
	server.pc.bulk.gov.fixed = math.Inf(1)
	server.pc.bulk.gov.begin()
	outcome := server.pc.streamBulk(context.Background(), w, server.c, req, ring, false)
	server.pc.bulk.gov.end()
	peak := stop()
	if outcome != bulkEndDone || w.bytes < 64<<20 {
		t.Fatalf("outcome %s, %d bytes", bulkEndName(outcome), w.bytes)
	}
	if grew := int64(peak) - int64(base.HeapAlloc); grew > 24<<20 {
		t.Fatalf("serving 64 MiB grew the live heap by %d MiB", grew>>20)
	}
}

// persist queues restored values to the local disk: a full write queue is
// committed once and tried again, the rest is counted as skipped; a value a
// live write replaced since its admission, or anything after a purge, is never
// written.
//
// conformance: cache-restart-handover
func TestPeerRestore_PersistRetriesAFullQueueAndKeepsLiveWrites(t *testing.T) {
	quietRestoreLogs(t)
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/c.db", MaxBufferBytes: 64 << 10, FlushSize: 1 << 20, FlushBytes: 1 << 30, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c := NewWithMaxBytes(time.Hour, 1000, 64<<20)
	defer c.Close()
	c.SetL2(dc)
	r := newTestRestore(c, "")
	defer r.cancel()
	base := dc.reserveAccess(1001)
	r.access.Store(int64(base))
	r.accessFloor = int64(base) - 1000
	admit := func(n int, prefix string) []*restoreEntry {
		batch := make([]*restoreEntry, n)
		for i := range batch {
			batch[i] = &restoreEntry{key: fmt.Sprintf("%s-%02d", prefix, i), value: bytes.Repeat([]byte{'v'}, 4096), expiry: time.Now().Add(time.Hour).UnixNano()}
		}
		restored := int64(0)
		if !c.admitRestoreBatch(batch, r.generation, &restored, 1<<30, nil) {
			t.Fatal("admission refused")
		}
		return batch
	}

	batch := admit(40, "full")
	r.persist(batch)
	written, skipped := r.diskWrites.Load(), r.diskSkipped.Load()
	if written+skipped != 40 || written <= 16 || skipped == 0 {
		t.Fatalf("written %d, skipped %d: want a retry after the commit and the rest skipped", written, skipped)
	}

	// A live write since the admission wins, in memory and on disk.
	live := admit(2, "live")
	c.SetLocalAndDiskWithTTL(live[0].key, []byte("fresher"), time.Minute)
	dc.Flush()
	r.diskWrites.Store(0)
	r.persist(live)
	dc.Flush()
	if v, ok := dc.Get(live[0].key); !ok || string(v) != "fresher" || r.diskWrites.Load() != 1 {
		t.Fatalf("disk holds %q, %d writes: the restored value replaced a live write", v, r.diskWrites.Load())
	}

	// After a purge nothing is written.
	late := admit(3, "late")
	c.PurgeAll()
	r.diskWrites.Store(0)
	r.persist(late)
	dc.Flush()
	if entries, _ := dc.Size(); entries != 0 || r.diskWrites.Load() != 0 {
		t.Fatalf("%d records, %d writes after a purge", entries, r.diskWrites.Load())
	}
}
