package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
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

// pause makes node i accept requests and never answer them until the
// returned function is called (a paused or hung process).
func (f *replFleet) pause(i int) (resume func()) {
	release := make(chan struct{})
	prev := f.nodes[i].handler.Load().(http.Handler)
	f.nodes[i].handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})))
	var once sync.Once
	return func() {
		once.Do(func() {
			close(release)
			f.nodes[i].handler.Store(prev)
		})
	}
}

// A paused owner costs a request of many lookups one peer timeout in all,
// not one per lookup: the first lookup that times out suspends the owner for
// the breaker cooldown and cancels the lookups still waiting on it, which go
// on to the backup; later lookups skip the owner. After the cooldown one
// lookup probes the owner while the others skip it.
//
// conformance: peer-cache-replication
func TestPeerReplication_PausedOwnerCostsOneTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	const margin = 150 * time.Millisecond
	f := newReplFleet(t, 3, 2, true, timeout)
	owner, backup, other := 0, 1, 2
	keys := f.keysWith(12, owner, backup)
	for _, key := range keys {
		f.nodes[owner].c.SetWithTTL(key, []byte("v-"+key), 10*time.Minute)
	}
	waitFor(t, "the backup copies", func() bool { ok, _ := f.pushes(); return ok == int64(len(keys)) })
	resume := f.pause(owner)
	defer resume()

	// Sequential lookups of one request.
	started := time.Now()
	for _, key := range keys[:6] {
		if v, ok := f.nodes[other].c.Get(key); !ok || string(v) != "v-"+key {
			t.Fatalf("%s: %q %v", key, v, ok)
		}
	}
	sequential := time.Since(started)
	if sequential > timeout+margin {
		t.Fatalf("6 sequential lookups with the owner paused took %s, want at most one timeout (%s) plus %s", sequential, timeout, margin)
	}

	// Concurrent lookups on a fresh replica, before any suspension.
	f.restart(other, true)
	pb := f.nodes[other].pc.breaker(f.addr(owner))
	if pb.suspendedUntil.Load() != 0 {
		t.Fatal("a fresh replica starts with the owner suspended")
	}
	var wg sync.WaitGroup
	started = time.Now()
	for _, key := range keys[6:] {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			if v, ok := f.nodes[other].c.Get(key); !ok || string(v) != "v-"+key {
				t.Errorf("%s: %q %v", key, v, ok)
			}
		}(key)
	}
	wg.Wait()
	concurrent := time.Since(started)
	if concurrent > timeout+margin {
		t.Fatalf("6 concurrent lookups with the owner paused took %s, want at most one timeout (%s) plus %s", concurrent, timeout, margin)
	}

	// After the cooldown: one probe, the others skip the owner.
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	probeDone := make(chan time.Duration)
	go func() {
		t0 := time.Now()
		f.nodes[other].pc.Get(keys[0])
		probeDone <- time.Since(t0)
	}()
	time.Sleep(20 * time.Millisecond) // the probe is waiting on the owner
	t0 := time.Now()
	if _, _, ok := f.nodes[other].pc.Get(keys[1]); !ok {
		t.Fatal("the lookup beside the probe missed")
	}
	if beside := time.Since(t0); beside > 50*time.Millisecond {
		t.Fatalf("a lookup beside the probe waited %s on the paused owner", beside)
	}
	if probe := <-probeDone; probe < timeout {
		t.Fatalf("the probe took %s, want the timeout: it did not ask the owner", probe)
	}
	if pb.suspendedUntil.Load() <= time.Now().UnixNano() {
		t.Fatal("the failed probe did not suspend the owner again")
	}

	// The owner answers again: the next probe lifts the suspension.
	resume()
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	if _, _, ok := f.nodes[other].pc.Get(keys[2]); !ok {
		t.Fatal("the probe of a resumed owner missed")
	}
	if pb.suspendedUntil.Load() != 0 {
		t.Fatal("a successful probe left the owner suspended")
	}
	t.Logf("owner paused, timeout %s: 6 sequential lookups %s, 6 concurrent %s", timeout, sequential, concurrent)
}

// Cache.Get (the extent, window and inventory path) counts its tiers like
// GetSharedWithAge, and every replica fetch is timed by the replica's role
// and the outcome.
//
// conformance: peer-cache-replication
func TestPeerReplication_GetTierCountersAndFetchDurations(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	owner, backup, other := 0, 1, 2
	keys := f.keysWith(2, owner, backup)
	f.nodes[owner].c.SetWithTTL(keys[0], []byte("v"), 10*time.Minute)
	waitFor(t, "the backup copy", func() bool { ok, _ := f.pushes(); return ok == 1 })

	c := f.nodes[other].c
	c.Get(keys[0])    // l1 miss, l2 miss, l3 hit from the owner
	c.Get(keys[0])    // l1 hit
	c.Get("absent")   // misses everywhere: computed
	f.stopNode(owner) // the next lookup errors on the owner, then hits the backup
	f.nodes[backup].c.SetWithTTL(keys[1], []byte("w"), 10*time.Minute)
	c.Get(keys[1])

	st := c.Stats()
	if st.L1.Requests != 4 || st.L1.Hits != 1 || st.L3.Hits != 2 || st.BackendFallthrough != 1 {
		t.Fatalf("tier counters: l1 %+v, l2 %+v, l3 %+v, fall-through %d", st.L1, st.L2, st.L3, st.BackendFallthrough)
	}
	text := f.nodes[other].pc.FetchDurationText()
	for _, want := range []string{
		`loki_vl_proxy_peer_cache_fetch_duration_seconds_count{replica="owner",outcome="hit"} 1`,
		`loki_vl_proxy_peer_cache_fetch_duration_seconds_count{replica="owner",outcome="error"} 1`,
		`loki_vl_proxy_peer_cache_fetch_duration_seconds_count{replica="backup",outcome="hit"} 1`,
		`loki_vl_proxy_peer_cache_fetch_duration_seconds_bucket{replica="owner",outcome="hit",le="+Inf"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

// Replicas of a fleet start within milliseconds of each other, so a replica
// dials peers that do not listen yet. A refused connection to a peer that
// never answered is not a failure: the peer is skipped for a moment and
// tried again, its breaker never opens, and the first lookups after every
// replica listens are peer hits. Pushes made while a replica did not listen
// are kept (bounded) and sent once it answers, so no copy is lost.
//
// conformance: peer-cache-replication
func TestPeerReplication_StaggeredStartOpensNoBreaker(t *testing.T) {
	const n = 3
	addrs := make([]string, n)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = l.Addr().String()
		_ = l.Close() // nothing listens until the node starts: dials are refused
	}
	caches := make([]*Cache, n)
	pcs := make([]*PeerCache, n)
	for i := range caches {
		caches[i] = NewWithMaxBytes(time.Hour, 10000, 64<<20)
		pcs[i] = NewPeerCache(PeerConfig{SelfAddr: addrs[i], DiscoveryType: "static", StaticPeers: strings.Join(addrs, ","),
			Timeout: time.Second, WriteThrough: true, WriteThroughMinTTL: time.Second, ReplicationFactor: 2,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		caches[i].SetL3(pcs[i])
	}
	var servers []*http.Server
	t.Cleanup(func() {
		for _, srv := range servers {
			_ = srv.Close()
		}
		for i := range caches {
			pcs[i].Close()
			caches[i].Close()
		}
	})
	start := func(i int) {
		var l net.Listener
		var err error
		for tries := 0; tries < 50; tries++ {
			if l, err = net.Listen("tcp", addrs[i]); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("listen %s: %v", addrs[i], err)
		}
		c, pc := caches[i], pcs[i]
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { pc.ServeHTTP(w, r, c) })}
		servers = append(servers, srv)
		go func() { _ = srv.Serve(l) }()
	}
	replicaIdx := func(key string) []int {
		var out []int
		for _, addr := range pcs[0].replicasOf(key) {
			for i := range addrs {
				if addrs[i] == addr {
					out = append(out, i)
				}
			}
		}
		return out
	}
	up := []bool{true, false, false}

	// Node 0 starts and works while the others are not listening yet.
	start(0)
	var keys []string
	wantDropped := 0
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("stagger-%d", i)
		keys = append(keys, key)
		caches[0].SetWithTTL(key, []byte("v-"+key), 10*time.Minute)
		for _, r := range replicaIdx(key) {
			if r != 0 && !up[r] {
				wantDropped++
			}
		}
		caches[0].Get(fmt.Sprintf("absent-%d", i)) // lookups that dial the others
	}
	time.Sleep(150 * time.Millisecond)
	start(1)
	up[1] = true
	time.Sleep(350 * time.Millisecond)
	start(2)
	up[2] = true
	time.Sleep(2 * peerStartupRetry) // past the retry window

	for i, pc := range pcs {
		for j, addr := range addrs {
			if i == j {
				continue
			}
			pb := pc.breaker(addr)
			if pb.failures.Load() >= pb.threshold || pb.suspendedUntil.Load() != 0 || !pc.peerAllowed(addr) {
				t.Fatalf("node %d's breaker for node %d is open after the staggered start (failures %d)", i, j, pb.failures.Load())
			}
		}
	}
	waitFor(t, "the kept pushes", func() bool { return pcs[0].WTPushes.Load() >= int64(wantDropped) })
	if got := pcs[0].PushPeerNotUp.Load(); got != 0 || pcs[0].WTErrors.Load() != 0 {
		t.Fatalf("pushes dropped %d, errors %d (pushes made before their replica listened: %d)", got, pcs[0].WTErrors.Load(), wantDropped)
	}

	// The first lookups after every replica listens are answered by a peer
	// whenever node 0 (the writer) is one of the key's replicas.
	hits := 0
	for _, key := range keys {
		if v, ok := caches[2].Get(key); !ok || string(v) != "v-"+key {
			t.Fatalf("%s (replicas %v): the first lookup after start missed", key, replicaIdx(key))
		}
		hits++
	}
	// Writes after every replica listens are pushed with no loss.
	dropped := pcs[0].PushPeerNotUp.Load()
	for i := 0; i < 10; i++ {
		caches[0].SetWithTTL(fmt.Sprintf("after-%d", i), []byte("v"), 10*time.Minute)
	}
	time.Sleep(100 * time.Millisecond)
	if pcs[0].PushPeerNotUp.Load() != dropped || pcs[0].WTErrors.Load() != 0 {
		t.Fatal("a push after every replica started was lost")
	}
	t.Logf("staggered start: %d of %d keys found by the first lookups, %d pushes made before their replica listened, all sent later (%d dropped), %d refused dials counted as starting", hits, len(keys), wantDropped, dropped, pcs[0].PeerNotUp.Load())
}

// Pushes kept for a peer that never starts are bounded and dropped after the
// wait, each counted, and the retrier ends with the cache.
//
// conformance: peer-cache-replication
func TestPeerReplication_PendingPushesBounded(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := l.Addr().String()
	_ = l.Close()
	pc := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1," + dead,
		Timeout: time.Second, WriteThrough: true, ReplicationFactor: 2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	defer pc.Close()
	for i := 0; i < pendingPushMaxEntries+10; i++ {
		pc.Replicate(fmt.Sprintf("k-%d", i), []byte("v"), time.Minute)
	}
	waitFor(t, "the first refusal", func() bool { return pc.PeerNotUp.Load() > 0 })
	time.Sleep(50 * time.Millisecond)
	pb := pc.breaker(dead)
	pb.mu.Lock()
	kept := len(pb.pending)
	pb.mu.Unlock()
	if kept > pendingPushMaxEntries {
		t.Fatalf("%d pushes kept, bound %d", kept, pendingPushMaxEntries)
	}
	if dropped := pc.PushPeerNotUp.Load(); dropped+int64(kept) < 10 {
		t.Fatalf("kept %d, dropped %d of %d", kept, dropped, pendingPushMaxEntries+10)
	}
	if pb.failures.Load() != 0 {
		t.Fatalf("refusals of a peer that never started counted %d failures", pb.failures.Load())
	}
	pc.Close()
	waitFor(t, "the retrier to end", func() bool {
		pb.mu.Lock()
		defer pb.mu.Unlock()
		return !pb.retrying
	})
}

// A burst of writes (a cold build storing every hour of a range) is pushed
// in full: pushes beyond the running ones wait in the queue instead of being
// dropped, and the queue drains to empty.
//
// conformance: peer-cache-replication
func TestPeerReplication_PushBurstQueued(t *testing.T) {
	f := newReplFleet(t, 3, 2, true, time.Second)
	owner, backup := 0, 1
	keys := f.keysWith(500, owner, backup)
	for _, key := range keys {
		f.nodes[owner].c.SetWithTTL(key, []byte("v"), 10*time.Minute)
	}
	waitFor(t, "every push", func() bool { ok, _ := f.pushes(); return ok == int64(len(keys)) })
	pc := f.nodes[owner].pc
	if pc.PushThrottled.Load() != 0 {
		t.Fatalf("%d pushes of a burst dropped", pc.PushThrottled.Load())
	}
	waitFor(t, "the workers to stop", func() bool { return pc.pushers.Load() == 0 && pc.pushQueued.Load() == 0 })
}
