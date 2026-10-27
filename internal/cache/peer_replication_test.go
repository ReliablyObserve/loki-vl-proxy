package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// replNode is one replica of an in-process peer ring: memory, a disk and a
// peer cache behind an HTTP server whose handler a test can swap (to stop
// the node or slow it down).
type replNode struct {
	c       *Cache
	dc      *DiskCache
	pc      *PeerCache
	srv     *httptest.Server
	handler atomic.Value // http.Handler
	disk    string
	wire    atomic.Int64 // request body bytes received
}

type replFleet struct {
	t       testing.TB
	nodes   []*replNode
	timeout time.Duration
	wt      bool
	rf      int
}

func newReplFleet(t testing.TB, n, rf int, writeThrough bool, timeout time.Duration) *replFleet {
	t.Helper()
	f := &replFleet{t: t, timeout: timeout, wt: writeThrough, rf: rf}
	for i := 0; i < n; i++ {
		nd := &replNode{disk: filepath.Join(t.TempDir(), "l2.bolt")}
		nd.handler.Store(http.Handler(http.NotFoundHandler()))
		nd.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nd.wire.Add(r.ContentLength)
			nd.handler.Load().(http.Handler).ServeHTTP(w, r)
		}))
		f.nodes = append(f.nodes, nd)
	}
	for i := range f.nodes {
		f.start(i)
	}
	t.Cleanup(f.close)
	return f
}

func (f *replFleet) addr(i int) string { return f.nodes[i].srv.Listener.Addr().String() }

// start starts node i with empty memory and the disk at its path.
func (f *replFleet) start(i int) {
	f.t.Helper()
	nd := f.nodes[i]
	peers := make([]string, len(f.nodes))
	for k := range f.nodes {
		peers[k] = f.addr(k)
	}
	nd.c = NewWithMaxBytes(time.Hour, 10000, 64<<20)
	dc, err := NewDiskCache(DiskCacheConfig{Path: nd.disk, FlushSize: 1, FlushInterval: 10 * time.Millisecond})
	if err != nil {
		f.t.Fatal(err)
	}
	nd.dc = dc
	nd.c.SetL2(dc)
	nd.pc = NewPeerCache(PeerConfig{
		SelfAddr: f.addr(i), DiscoveryType: "static", StaticPeers: strings.Join(peers, ","),
		Timeout: f.timeout, WriteThrough: f.wt, WriteThroughMinTTL: time.Second, ReplicationFactor: f.rf,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	nd.c.SetL3(nd.pc)
	c, pc := nd.c, nd.pc
	nd.handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pc.ServeHTTP(w, r, c)
	})))
}

// stopNode closes node i's caches; its address drops every connection.
func (f *replFleet) stopNode(i int) {
	nd := f.nodes[i]
	nd.handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			_ = conn.Close()
		}
	})))
	nd.pc.Close()
	nd.c.Close()
	_ = nd.dc.Close()
}

// restart restarts node i with empty memory, its disk kept or wiped.
func (f *replFleet) restart(i int, wipeDisk bool) {
	f.stopNode(i)
	if wipeDisk {
		f.nodes[i].disk = filepath.Join(f.t.TempDir(), "l2.bolt")
	}
	f.start(i)
}

func (f *replFleet) close() {
	for _, nd := range f.nodes {
		nd.srv.Close()
		nd.pc.Close()
		nd.c.Close()
		_ = nd.dc.Close()
	}
}

// replicaIdx returns the key's replicas as node indexes, owner first.
func (f *replFleet) replicaIdx(key string) []int {
	var out []int
	for _, addr := range f.nodes[0].pc.replicasOf(key) {
		for i := range f.nodes {
			if f.addr(i) == addr {
				out = append(out, i)
			}
		}
	}
	return out
}

// keyWith returns a key whose replicas are exactly want, in that order.
func (f *replFleet) keyWith(want ...int) string {
	f.t.Helper()
	return f.keysWith(1, want...)[0]
}

// keysWith returns n distinct keys whose replicas are exactly want.
func (f *replFleet) keysWith(n int, want ...int) []string {
	f.t.Helper()
	var keys []string
	for i := 0; i < 100000 && len(keys) < n; i++ {
		key := fmt.Sprintf("repl-key-%d", i)
		if fmt.Sprint(f.replicaIdx(key)) == fmt.Sprint(want) {
			keys = append(keys, key)
		}
	}
	if len(keys) < n {
		f.t.Fatalf("no %d keys with replicas %v", n, want)
	}
	return keys
}

func (f *replFleet) pushes() (ok, failed int64) {
	for _, nd := range f.nodes {
		ok += nd.pc.WTPushes.Load()
		failed += nd.pc.WTErrors.Load()
	}
	return ok, failed
}

func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func onDisk(nd *replNode, key string) bool {
	nd.dc.Flush()
	_, ok := nd.dc.Get(key)
	return ok
}

// A write is pushed once to each replica other than the writer; a replica
// keeps the copy in memory and on disk with the full TTL and pushes it no
// further, whoever wrote it.
//
// conformance: peer-cache-replication
func TestPeerReplication_WriteReachesEveryReplicaOnce(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	owner, backup, other := 0, 1, 2
	keys := f.keysWith(2, owner, backup)
	key, key2 := keys[0], keys[1]

	// Written on a replica that is neither: pushed to both.
	f.nodes[other].c.SetWithTTL(key, []byte("v1"), 10*time.Minute)
	waitFor(t, "two pushes", func() bool { ok, _ := f.pushes(); return ok == 2 })
	for _, i := range []int{owner, backup} {
		_, ttl, ok := f.nodes[i].c.GetWithTTL(key)
		if !ok || ttl < 9*time.Minute {
			t.Fatalf("replica %d: ok %v ttl %v, want the full TTL", i, ok, ttl)
		}
		if !onDisk(f.nodes[i], key) {
			t.Fatalf("replica %d kept no disk copy", i)
		}
	}
	if _, ttl, ok := f.nodes[other].c.GetWithTTL(key); !ok || ttl > nonOwnerShadowMaxTTL {
		t.Fatalf("the writer that is not a replica keeps %v, want a shadow of at most %v", ttl, nonOwnerShadowMaxTTL)
	}
	if onDisk(f.nodes[other], key) {
		t.Fatal("the writer that is not a replica wrote its disk")
	}

	// Written on the owner: pushed to the backup only.
	f.nodes[owner].c.SetWithTTL(key2, []byte("v2"), 10*time.Minute)
	waitFor(t, "one more push", func() bool { ok, _ := f.pushes(); return ok == 3 })
	time.Sleep(50 * time.Millisecond) // a bounced push would land by now
	if ok, failed := f.pushes(); ok != 3 || failed != 0 {
		t.Fatalf("pushes %d, failed %d: want 3 and 0 (no push loops)", ok, failed)
	}
}

// A replica that lost its disk gets its keys back from the other replica,
// the owner included (it asks its backup), with no recompute; the copy goes
// back to its memory and disk without being pushed again.
//
// conformance: peer-cache-replication
func TestPeerReplication_LostDiskRefillsFromReplica(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	owner, backup := 0, 1
	key := f.keyWith(owner, backup)
	f.nodes[owner].c.SetWithTTL(key, []byte("value"), 10*time.Minute)
	waitFor(t, "the backup copy", func() bool { return onDisk(f.nodes[backup], key) })

	// Restart with the disk: the owner serves from its own disk.
	f.restart(owner, false)
	if _, _, _, tier, ok := f.nodes[owner].c.GetSharedWithAge(key); !ok || tier != "l2_disk" {
		t.Fatalf("restarted with its disk: ok %v tier %q", ok, tier)
	}

	// The owner loses its disk: the backup serves it.
	f.restart(owner, true)
	pushesBefore, _ := f.pushes()
	v, _, _, tier, ok := f.nodes[owner].c.GetSharedWithAge(key)
	if !ok || string(v) != "value" || tier != "l3_peer" {
		t.Fatalf("after losing its disk the owner got %q %v from %q", v, ok, tier)
	}
	if f.nodes[owner].pc.BackupHits.Load() != 1 {
		t.Fatalf("backup hits %d", f.nodes[owner].pc.BackupHits.Load())
	}
	if !onDisk(f.nodes[owner], key) {
		t.Fatal("the owner did not refill its disk")
	}
	time.Sleep(50 * time.Millisecond)
	if ok, _ := f.pushes(); ok != pushesBefore {
		t.Fatalf("the refill pushed %d copies", ok-pushesBefore)
	}

	// The backup loses its disk: the owner serves it.
	f.restart(backup, true)
	if v, _, _, tier, ok := f.nodes[backup].c.GetSharedWithAge(key); !ok || string(v) != "value" || tier != "l3_peer" {
		t.Fatalf("after losing its disk the backup got %q %v from %q", v, ok, tier)
	}
	if f.nodes[backup].pc.BackupHits.Load() != 0 {
		t.Fatal("the backup's lookup did not go to the owner first")
	}
}

// With the owner down, a lookup goes on to the backup within the peer fetch
// timeout; with the owner slow, it gives up on it after the timeout.
//
// conformance: peer-cache-replication
func TestPeerReplication_OwnerDownOrSlowBackupServes(t *testing.T) {
	const timeout = 200 * time.Millisecond
	f := newReplFleet(t, 3, 2, true, timeout)
	owner, backup, other := 0, 1, 2
	key := f.keyWith(owner, backup)
	f.nodes[owner].c.SetWithTTL(key, []byte("value"), 10*time.Minute)
	waitFor(t, "the backup copy", func() bool { _, _, ok := f.nodes[backup].c.GetWithTTL(key); return ok })

	f.stopNode(owner)
	started := time.Now()
	v, ok := f.nodes[other].c.Get(key)
	down := time.Since(started)
	if !ok || string(v) != "value" {
		t.Fatalf("owner down: got %q %v", v, ok)
	}
	if f.nodes[other].pc.BackupHits.Load() != 1 || f.nodes[other].pc.PeerErrors.Load() == 0 {
		t.Fatalf("backup hits %d, errors %d", f.nodes[other].pc.BackupHits.Load(), f.nodes[other].pc.PeerErrors.Load())
	}
	if down > timeout {
		t.Fatalf("owner down (connection refused) took %v, above the fetch timeout %v", down, timeout)
	}

	// A slow owner: the lookup waits at most the timeout for it.
	key2 := f.keyWith(backup, owner)
	f.restart(owner, true)
	f.nodes[backup].c.SetWithTTL(key2, []byte("v2"), 10*time.Minute)
	waitFor(t, "the owner copy", func() bool { _, _, ok := f.nodes[owner].c.GetWithTTL(key2); return ok })
	release := make(chan struct{})
	defer close(release)
	slow := f.nodes[backup].handler.Load().(http.Handler)
	f.nodes[backup].handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(5 * timeout):
		}
		slow.ServeHTTP(w, r)
	})))
	started = time.Now()
	v, ok = f.nodes[other].c.Get(key2)
	took := time.Since(started)
	if !ok || string(v) != "v2" {
		t.Fatalf("slow owner: got %q %v", v, ok)
	}
	if took < timeout || took > timeout+150*time.Millisecond {
		t.Fatalf("slow owner: %v, want the fetch timeout %v plus one backup round trip", took, timeout)
	}
	t.Logf("owner down: %v; slow owner: %v (timeout %v)", down, took, timeout)
}

// An expired copy is never served by a replica, from memory or disk, and a
// lookup never extends a copy's TTL.
//
// conformance: peer-cache-replication
func TestPeerReplication_ExpiredCopiesNeverServed(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	owner, backup, other := 0, 1, 2
	key := f.keyWith(owner, backup)
	f.nodes[owner].c.SetWithTTL(key, []byte("value"), 20*time.Second)
	waitFor(t, "the backup copy", func() bool { return onDisk(f.nodes[backup], key) })
	f.nodes[other].c.Get(key)
	_, ttl, ok := f.nodes[other].c.GetWithTTL(key)
	if !ok || ttl > 20*time.Second {
		t.Fatalf("the fetched copy has %v, more than the owner's TTL", ttl)
	}
	f.restart(owner, true) // only the backup holds it now, on disk and in memory
	f.restart(other, true)
	restore := AdvanceClockForTesting(16 * time.Second) // under MinUsableTTL left
	defer restore()
	if _, ok := f.nodes[other].c.Get(key); ok {
		t.Fatal("a copy about to expire was served")
	}
	f.restart(backup, false) // memory gone, disk kept
	if _, ok := f.nodes[other].c.Get(key); ok {
		t.Fatal("a copy about to expire was served from a replica's disk")
	}
}

// A value its writer keeps in memory only stays off the fetching replica's
// disk too.
//
// conformance: peer-cache-replication
func TestPeerReplication_MemoryOnlyStaysOffDisk(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	owner, backup := 0, 1
	key := f.keyWith(owner, backup)
	f.nodes[owner].c.SetLocalOnlyWithTTL(key, []byte("window"), 10*time.Minute)
	if v, _, _, tier, ok := f.nodes[backup].c.GetSharedWithAge(key); !ok || string(v) != "window" || tier != "l3_peer" {
		t.Fatalf("got %q %v from %q", v, ok, tier)
	}
	if onDisk(f.nodes[backup], key) {
		t.Fatal("a memory-only value was written to the replica's disk")
	}
	if ok, _ := f.pushes(); ok != 0 {
		t.Fatalf("%d pushes of a memory-only value", ok)
	}
}

// Keys the push filter rejects (entries computed with a user's forwarded
// credentials) and values above the peers' body limit are never pushed, and
// a value too large to push costs the peers' breakers nothing.
//
// conformance: peer-cache-replication
func TestPeerReplication_FilteredAndOversizedNotPushed(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	for _, nd := range f.nodes {
		nd.pc.SetPushFilter(func(key string) bool { return !strings.Contains(key, ":auth:user.") })
	}
	owner, backup, other := 0, 1, 2
	key := f.keyWith(owner, backup)
	f.nodes[other].c.SetWithTTL(key+":auth:user.a", []byte("secret"), 10*time.Minute)
	f.nodes[other].c.SetReplicatedWithTTL(key+":auth:user.b", []byte("secret"), 10*time.Minute)
	big := make([]byte, maxPeerSetBodyBytes+1)
	f.nodes[owner].c.Close()
	f.nodes[owner].c = NewWithMaxBytes(time.Hour, 10, 1<<30) // room for the value
	f.nodes[owner].c.SetL2(f.nodes[owner].dc)
	f.nodes[owner].c.SetL3(f.nodes[owner].pc)
	f.nodes[owner].c.SetReplicatedWithTTL(key, big, 10*time.Minute)
	time.Sleep(100 * time.Millisecond)
	if ok, failed := f.pushes(); ok != 0 || failed != 0 {
		t.Fatalf("pushes %d, failed %d", ok, failed)
	}
	if f.nodes[owner].pc.PushTooLarge.Load() != 1 {
		t.Fatalf("too-large pushes %d", f.nodes[owner].pc.PushTooLarge.Load())
	}
	if !f.nodes[owner].pc.peerAllowed(f.addr(backup)) {
		t.Fatal("the skipped push opened the backup's breaker")
	}
}

// SetReplicatedWithTTL pushes to the replicas with write-through off; a
// plain SetWithTTL does not.
//
// conformance: peer-cache-replication
func TestPeerReplication_ForcedReplicationWithoutWriteThrough(t *testing.T) {
	f := newReplFleet(t, 3, 2, false, time.Second)
	owner, backup := 0, 1
	key := f.keyWith(owner, backup)
	f.nodes[owner].c.SetWithTTL(key+"-plain", []byte("v"), 10*time.Minute)
	f.nodes[owner].c.SetReplicatedWithTTL(key, []byte("v"), 10*time.Minute)
	waitFor(t, "the forced push", func() bool { return onDisk(f.nodes[backup], key) })
	time.Sleep(50 * time.Millisecond)
	if ok, _ := f.pushes(); ok != 1 {
		t.Fatalf("pushes %d, want 1", ok)
	}
}

// Every goroutine the replication starts (pushes, lookups) ends once the
// fleet is closed.
//
// conformance: peer-cache-replication
func TestPeerReplication_CloseLeavesNoGoroutines(t *testing.T) {
	const label = "TestPeerReplication_CloseLeavesNoGoroutines"
	pprof.Do(context.Background(), pprof.Labels("cache-test", label), func(context.Context) {
		f := newReplFleet(t, 3, 2, true, time.Second)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("leak-%d", i)
			f.nodes[i%3].c.SetWithTTL(key, []byte("v"), time.Minute)
			f.nodes[(i+1)%3].c.Get(key)
		}
		waitFor(t, "the pushes", func() bool { ok, failed := f.pushes(); return ok+failed >= 20 })
		f.restart(0, true)
		f.close()
	})
	deadline := time.Now().Add(5 * time.Second)
	for labelledGoroutines(label) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d replication goroutines left", labelledGoroutines(label))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
