package cache

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shortCooldown makes node from's breaker for node to open for d instead of
// the 10s cooldown, so a test sees the half-open probe without waiting.
func (f *replFleet) shortCooldown(from, to int, d time.Duration) *peerBreaker {
	pb := &peerBreaker{threshold: 5, cooldown: d, wake: make(chan struct{}, 1)}
	f.nodes[from].pc.breakers.Store(f.addr(to), pb)
	return pb
}

// recordSets wraps node i's handler and records every /_cache/set it gets:
// the key, the TTL sent, and whether another set was in flight.
type setLog struct {
	mu       sync.Mutex
	events   []string // "start key" / "end key", in order
	ttls     map[string]int64
	inFlight atomic.Int32
	maxIn    atomic.Int32
}

func (f *replFleet) recordSets(i int) *setLog {
	l := &setLog{ttls: map[string]int64{}}
	inner := f.nodes[i].handler.Load().(http.Handler)
	f.nodes[i].handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cache/set" {
			inner.ServeHTTP(w, r)
			return
		}
		key := r.URL.Query().Get("key")
		ttl, _ := strconv.ParseInt(r.URL.Query().Get("ttl_ms"), 10, 64)
		n := l.inFlight.Add(1)
		for {
			m := l.maxIn.Load()
			if n <= m || l.maxIn.CompareAndSwap(m, n) {
				break
			}
		}
		l.mu.Lock()
		l.events = append(l.events, "start "+key)
		l.ttls[key] = ttl
		l.mu.Unlock()
		inner.ServeHTTP(w, r)
		l.mu.Lock()
		l.events = append(l.events, "end "+key)
		l.mu.Unlock()
		l.inFlight.Add(-1)
	})))
	return l
}

func (l *setLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// A replica that stops answering misses no write: the writer's pushes to it
// wait in its backlog while its breaker is open (reads skip it meanwhile and
// stay fast), and once it answers again the half-open probe is the first
// replayed push, the breaker closes, and every key reaches it with the TTL it
// has left. The replay starts slowly: the probe goes alone, then rounds grow.
//
// conformance: peer-cache-push-replay, peer-cache-replication
func TestPeerPushReplay_OpenBufferHalfOpenReplayClosed(t *testing.T) {
	const timeout = 200 * time.Millisecond
	f := newReplFleet(t, 3, 2, true, timeout)
	writer, peer := 0, 1
	pb := f.shortCooldown(writer, peer, 400*time.Millisecond)
	keys := f.keysWith(40, writer, peer)
	sets := f.recordSets(peer)
	resume := f.pause(peer)
	defer resume()

	// Open: the first push times out and suspends the peer; it and every
	// later push wait in the backlog.
	pc := f.nodes[writer].pc
	for _, key := range keys {
		f.nodes[writer].c.SetWithTTL(key, []byte("v-"+key), 10*time.Minute)
	}
	waitFor(t, "every push in the backlog", func() bool { return pb.buffered.Load() == int64(len(keys)) })
	if pb.suspendedUntil.Load() == 0 {
		t.Fatal("a peer that timed out is not suspended")
	}
	if got := pc.PushBuffered[bufferedPushFailed].Load() + pc.PushBuffered[bufferedPeerUnavailable].Load() + pc.PushBuffered[bufferedBacklog].Load(); got != int64(len(keys)) {
		t.Fatalf("%d pushes buffered, want %d", got, len(keys))
	}
	// Reads stay transparent: a lookup on the writer skips the open peer.
	started := time.Now()
	if _, _, ok := pc.Get(f.keyWith(peer, writer)); ok {
		t.Fatal("a key no replica holds was found")
	}
	if took := time.Since(started); took > timeout/2 {
		t.Fatalf("a lookup beside the open breaker took %s", took)
	}

	// Half open, then closed: the peer answers again.
	resume()
	waitFor(t, "the replay", func() bool { return pc.PushReplayed.Load() == int64(len(keys)) && !pc.hasBacklog(f.addr(peer)) })
	if pb.suspendedUntil.Load() != 0 || pb.failures.Load() != 0 || !pc.peerAllowed(f.addr(peer)) {
		t.Fatal("the breaker did not close after the replay")
	}
	for _, key := range keys {
		if v, ok := f.nodes[peer].c.Peek(key); !ok || string(v) != "v-"+key {
			t.Fatalf("%s did not reach the replica: %q %v", key, v, ok)
		}
	}
	if n := pc.PushBufferDropped[droppedFull].Load() + pc.PushBufferDropped[droppedExpired].Load(); n != 0 {
		t.Fatalf("%d buffered pushes dropped", n)
	}
	// The TTL sent is what the writer's copy had left, never more.
	sets.mu.Lock()
	for _, key := range keys {
		if ttl := sets.ttls[key]; ttl <= 0 || ttl > (10*time.Minute).Milliseconds() {
			t.Errorf("%s replayed with ttl_ms %d", key, ttl)
		}
	}
	sets.mu.Unlock()
	// Slow start: the probe ran alone, and the replay never had more than
	// replayMaxInFlight pushes in flight.
	ev := sets.snapshot()
	if len(ev) < 2 || !strings.HasPrefix(ev[0], "start ") || ev[1] != "end "+strings.TrimPrefix(ev[0], "start ") {
		t.Fatalf("the probe did not go alone: %v", ev[:min(len(ev), 4)])
	}
	if m := sets.maxIn.Load(); m > replayMaxInFlight {
		t.Fatalf("%d replayed pushes in flight, bound %d", m, replayMaxInFlight)
	}
	// Writes after the recovery go the normal way again.
	before := pc.PushBuffered[bufferedBacklog].Load()
	f.nodes[writer].c.SetWithTTL(keys[0], []byte("after"), 10*time.Minute)
	waitFor(t, "the push after the recovery", func() bool {
		v, ok := f.nodes[peer].c.Peek(keys[0])
		return ok && string(v) == "after"
	})
	if pc.PushBuffered[bufferedBacklog].Load() != before {
		t.Fatal("a push after the recovery went through the backlog")
	}
	t.Logf("%d pushes buffered while the replica was paused, %d replayed, max %d in flight", len(keys), pc.PushReplayed.Load(), sets.maxIn.Load())
}

// The backlog keeps one entry per key, the newest value: a burst of writes of
// one key to an open peer costs one buffered entry and lands as the last
// value; an older value routed to the backlog late never replaces it.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_CoalescesLatestValueWins(t *testing.T) {
	b := newPushBuffer(10, 1<<20, nil)
	now := time.Now().UnixNano()
	exp := now + int64(time.Minute)
	b.add("p", &bufferedPush{key: "k", value: []byte("v1"), seq: 1, expiresAt: exp}, now)
	if c, _, _ := b.add("p", &bufferedPush{key: "k", value: []byte("v2-longer"), seq: 3, expiresAt: exp}, now); !c {
		t.Fatal("a newer value of a buffered key is not coalesced")
	}
	if c, _, _ := b.add("p", &bufferedPush{key: "k", value: []byte("v0"), seq: 2, expiresAt: exp}, now); !c {
		t.Fatal("an older value of a buffered key is not coalesced")
	}
	if entries, bytes := b.size(); entries != 1 || bytes != int64(len("v2-longer")) {
		t.Fatalf("%d entries, %d bytes after coalescing", entries, bytes)
	}
	batch, _ := b.take("p", 10, now)
	if len(batch) != 1 || string(batch[0].value) != "v2-longer" {
		t.Fatalf("took %v", batch)
	}

	f := newReplFleet(t, 3, 2, true, 200*time.Millisecond)
	writer, peer := 0, 1
	f.shortCooldown(writer, peer, 300*time.Millisecond)
	key := f.keyWith(writer, peer)
	resume := f.pause(peer)
	pc := f.nodes[writer].pc
	for i := 0; i < 50; i++ {
		f.nodes[writer].c.SetWithTTL(key, []byte(fmt.Sprintf("v%02d", i)), 10*time.Minute)
	}
	waitFor(t, "the burst in the backlog", func() bool { return pc.PushCoalesced.Load() >= 48 })
	if n := pc.breaker(f.addr(peer)).buffered.Load(); n != 1 {
		t.Fatalf("%d entries buffered for one key", n)
	}
	resume()
	waitFor(t, "the replay", func() bool { return !pc.hasBacklog(f.addr(peer)) })
	if v, ok := f.nodes[peer].c.Peek(key); !ok || string(v) != "v49" {
		t.Fatalf("the replica holds %q %v, want the last write", v, ok)
	}
}

// Above the budget the oldest entry of the largest backlog goes, counted, with
// one log line however many go; entries and bytes never exceed the bounds.
//
// conformance: peer-cache-push-replay
func TestPeerPushBuffer_FullDropsOldestOfLargest(t *testing.T) {
	b := newPushBuffer(4, 1<<20, nil)
	now := time.Now().UnixNano()
	exp := now + int64(time.Minute)
	for i, a := range []string{"a1", "a2", "a3"} {
		b.add("A", &bufferedPush{key: a, value: []byte("x"), seq: uint64(i), expiresAt: exp}, now)
	}
	b.add("B", &bufferedPush{key: "b1", value: []byte("x"), seq: 10, expiresAt: exp}, now)
	_, _, drops := b.add("B", &bufferedPush{key: "b2", value: []byte("x"), seq: 11, expiresAt: exp}, now)
	if len(drops) != 1 || drops[0].entry.key != "a1" || drops[0].reason != droppedFull {
		t.Fatalf("dropped %+v, want a1 (oldest of the largest backlog)", drops)
	}
	// An expired entry is counted as expired, not as a full buffer.
	_, _, drops = b.add("A", &bufferedPush{key: "a4", value: []byte("x"), seq: 12, expiresAt: exp}, exp)
	if len(drops) != 1 || drops[0].entry.key != "a2" || drops[0].reason != droppedExpired {
		t.Fatalf("dropped %+v, want a2 as expired", drops)
	}
	// The byte bound holds as well.
	bb := newPushBuffer(100, 10, nil)
	for i := 0; i < 5; i++ {
		bb.add("A", &bufferedPush{key: fmt.Sprint(i), value: []byte("abcd"), seq: uint64(i), expiresAt: exp}, now)
		if _, bytes := bb.size(); bytes > 10 {
			t.Fatalf("%d bytes buffered, bound 10", bytes)
		}
	}

	// Through the peer cache: drops are counted and logged once.
	var logs bytes.Buffer
	pc := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1,peer:1",
		WriteThrough: true, ReplicationFactor: 2, PushBufferMaxEntries: 8, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	defer pc.Close()
	pb := pc.breaker("peer:1")
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(time.Hour).UnixNano())
	for i := 0; i < 100; i++ {
		pc.Replicate(fmt.Sprintf("k-%d", i), []byte("v"), time.Minute)
	}
	if got := pc.PushBufferDropped[droppedFull].Load(); got != 92 || pb.buffered.Load() != 8 {
		t.Fatalf("%d dropped, %d kept", got, pb.buffered.Load())
	}
	if n := strings.Count(logs.String(), "push buffer full"); n != 1 {
		t.Fatalf("%d buffer-full log lines:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "-peer-push-buffer-max-entries") {
		t.Fatalf("the log line does not name the flag:\n%s", logs.String())
	}
}

// Entries whose TTL ran out while the peer was down are not sent.
//
// conformance: peer-cache-push-replay
func TestPeerPushBuffer_ExpiredEntriesAreNotSent(t *testing.T) {
	b := newPushBuffer(10, 1<<20, nil)
	now := time.Now().UnixNano()
	b.add("p", &bufferedPush{key: "old", value: []byte("x"), seq: 1, expiresAt: now + int64(2*time.Second)}, now)
	b.add("p", &bufferedPush{key: "new", value: []byte("x"), seq: 2, expiresAt: now + int64(time.Minute)}, now)
	batch, drops := b.take("p", 10, now)
	if len(batch) != 1 || batch[0].key != "new" || len(drops) != 1 || drops[0].reason != droppedExpired {
		t.Fatalf("took %v, dropped %v", batch, drops)
	}
	if !b.finish("p") {
		t.Fatal("an empty backlog did not finish")
	}
}

// Close stops every replayer at once, including a push waiting on a hung
// peer, and every goroutine the backlog started comes back.
//
// conformance: peer-cache-push-replay, shutdown-releases-background-work
func TestPeerPushReplay_CloseStopsReplay(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()
	hung := make(chan struct{})
	srv := newHungPeer(t, hung)
	pc := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1," + srv,
		Timeout: 30 * time.Second, WriteThrough: true, ReplicationFactor: 2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	pb := pc.breaker(srv)
	pb.seenUp.Store(true)
	for i := 0; i < 20; i++ {
		pc.bufferPush(srv, pushJob{addr: srv, key: fmt.Sprintf("k-%d", i), value: []byte("v"), ttl: time.Minute, seq: uint64(i)}, bufferedPeerUnavailable)
	}
	<-hung // the probe is waiting on the peer
	started := time.Now()
	pc.Close()
	waitFor(t, "the replayer to stop", func() bool { return pc.replayers.Load() == 0 })
	if took := time.Since(started); took > time.Second {
		t.Fatalf("Close took %s to stop a replay waiting on a hung peer (timeout 30s)", took)
	}
	if pb.buffered.Load() != 0 || pc.PushBufferDropped[droppedClosed].Load() == 0 {
		t.Fatalf("%d entries left, %d counted closed", pb.buffered.Load(), pc.PushBufferDropped[droppedClosed].Load())
	}
	// A push after Close is not buffered.
	pc.bufferPush(srv, pushJob{addr: srv, key: "late", value: []byte("v"), ttl: time.Minute}, bufferedPeerUnavailable)
	if pc.replayers.Load() != 0 || pb.buffered.Load() != 0 {
		t.Fatal("a push after Close started a replayer")
	}
	pc.client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) { // +2: the hung test server
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseline+2 {
		buf := make([]byte, 1<<20)
		t.Fatalf("%d goroutines after Close, baseline %d:\n%s", n, baseline, buf[:runtime.Stack(buf, true)])
	}
}

// A queued push to a peer whose breaker holds calls back goes to the backlog
// without a call, and checking the breaker never takes the half-open probe.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_QueuedPushToOpenPeerIsBufferedWithoutACall(t *testing.T) {
	var calls atomic.Int32
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	pb := pc.breaker(addr)
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pc.sendJob(pushJob{addr: addr, key: "k", value: []byte("v"), ttl: time.Minute, seq: 1})
	if calls.Load() != 0 || pc.PushBuffered[bufferedPeerUnavailable].Load() != 1 || pb.buffered.Load() != 1 {
		t.Fatalf("calls %d, buffered %d", calls.Load(), pc.PushBuffered[bufferedPeerUnavailable].Load())
	}
	if pb.probeAt.Load() != 0 {
		t.Fatal("routing a push took the half-open probe")
	}
	// The cooldown ends: the replayer sends the backlog.
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	waitFor(t, "the replay", func() bool { return pc.PushReplayed.Load() == 1 })
	if calls.Load() != 1 || pb.suspendedUntil.Load() != 0 {
		t.Fatalf("calls %d, suspended %d", calls.Load(), pb.suspendedUntil.Load())
	}
}

// A replayer waiting out a peer's cooldown starts as soon as another call
// (a lookup's half-open probe) finds the peer answering, not when the
// cooldown it slept on ends.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_WakesWhenThePeerAnswers(t *testing.T) {
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	pb := pc.breaker(addr)
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pc.bufferPush(addr, pushJob{addr: addr, key: "k", value: []byte("v"), ttl: time.Minute, seq: 1}, bufferedPeerUnavailable)
	time.Sleep(20 * time.Millisecond) // the replayer sleeps on the suspension
	pc.recordPeerSuccess(addr)        // a lookup's probe got an answer
	started := time.Now()
	waitFor(t, "the replay after the peer answered", func() bool { return pc.PushReplayed.Load() == 1 })
	if took := time.Since(started); took > time.Second {
		t.Fatalf("the replay started %s after the peer answered", took)
	}
}

// A push that takes the half-open probe but goes to the backlog (the queue
// was full) hands the probe back, so the replayer probes at once instead of
// the peer staying suspended for another cooldown.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_UnusedProbeIsHandedBack(t *testing.T) {
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	pc.replication = 2
	pc.updatePeers([]string{"self:3100", addr})
	pb := pc.breaker(addr)
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano()) // half open
	pc.pushQueued.Store(maxQueuedPushBytes)                               // the queue is full
	pc.Replicate("k", []byte("v"), time.Minute)
	waitFor(t, "the replay", func() bool { return pc.PushReplayed.Load() == 1 })
	if pb.suspendedUntil.Load() != 0 {
		t.Fatal("the replayed probe did not close the breaker")
	}
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	pb.probeAt.Store(time.Now().UnixNano())
	pb.releaseProbe()
	if pb.probeAt.Load() != 0 {
		t.Fatal("a half-open probe was not handed back")
	}
	pb.suspendedUntil.Store(0)
	pb.probeAt.Store(7)
	if pb.releaseProbe(); pb.probeAt.Load() != 7 {
		t.Fatal("releaseProbe changed a closed breaker")
	}
}

// A push the queue has no room for waits in the backlog and is sent from
// there; with buffering off it is dropped and counted as before.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_QueueFullIsBuffered(t *testing.T) {
	var calls atomic.Int32
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	pc.pushQueued.Store(maxQueuedPushBytes) // the queue is full
	pc.enqueuePush(pushJob{addr: addr, key: "k", value: []byte("v"), ttl: time.Minute, seq: 1})
	if pc.PushBuffered[bufferedQueueFull].Load() != 1 || pc.PushThrottled.Load() != 0 {
		t.Fatalf("buffered %d, throttled %d", pc.PushBuffered[bufferedQueueFull].Load(), pc.PushThrottled.Load())
	}
	waitFor(t, "the replay", func() bool { return pc.PushReplayed.Load() == 1 && calls.Load() == 1 })

	off := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1," + addr,
		WriteThrough: true, ReplicationFactor: 2, PushBufferMaxEntries: -1})
	defer off.Close()
	off.pushQueued.Store(maxQueuedPushBytes)
	off.enqueuePush(pushJob{addr: addr, key: "k", value: []byte("v"), ttl: time.Minute, seq: 1})
	if off.PushThrottled.Load() != 1 || off.replayers.Load() != 0 {
		t.Fatalf("throttled %d with buffering off", off.PushThrottled.Load())
	}
}

// With buffering off a push to an open peer is dropped as before this
// buffer existed: counted as an error and as a disabled drop.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_DisabledDropsAsBefore(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1,peer:1",
		WriteThrough: true, ReplicationFactor: 2, PushBufferMaxBytes: -1})
	defer pc.Close()
	pb := pc.breaker("peer:1")
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pc.Replicate("k", []byte("v"), time.Minute)
	if pc.WTErrors.Load() != 1 || pc.PushBufferDropped[droppedDisabled].Load() != 1 || pb.buffered.Load() != 0 || pc.replayers.Load() != 0 {
		t.Fatalf("errors %d, disabled drops %d, buffered %d", pc.WTErrors.Load(), pc.PushBufferDropped[droppedDisabled].Load(), pb.buffered.Load())
	}
	if st := pc.Stats(); st["push_buffer_entries"] != 0 {
		t.Fatalf("stats: %v", st)
	}
}

// The backlog of a peer that left the ring is dropped, and a buffered key
// whose replicas no longer include the peer is not sent to it.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_RingChangesDropBacklog(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1,peer:1",
		WriteThrough: true, ReplicationFactor: 2})
	defer pc.Close()
	pb := pc.breaker("peer:1")
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(time.Hour).UnixNano())
	pc.Replicate("k", []byte("v"), time.Minute)
	if pb.buffered.Load() != 1 {
		t.Fatal("not buffered")
	}
	pc.updatePeers([]string{"self:1"})
	if pb.buffered.Load() != 0 || pc.PushBufferDropped[droppedPeerRemoved].Load() != 1 {
		t.Fatalf("buffered %d, removed %d", pb.buffered.Load(), pc.PushBufferDropped[droppedPeerRemoved].Load())
	}
	if pc.replicaOf("k", "peer:1") {
		t.Fatal("a removed peer is still a replica")
	}
	sent, retry, called := pc.sendReplay("peer:1", []*bufferedPush{{key: "k", value: []byte("v"), expiresAt: time.Now().Add(time.Minute).UnixNano()}})
	if sent != 0 || len(retry) != 0 || called || pc.PushBufferDropped[droppedNotReplica].Load() != 1 {
		t.Fatalf("sent %d, retry %d, not-replica drops %d", sent, len(retry), pc.PushBufferDropped[droppedNotReplica].Load())
	}
}

// A peer that refuses a value (4xx) does not get it again; a 5xx is retried.
//
// conformance: peer-cache-push-replay
func TestPeerPushReplay_RejectedIsDroppedServerErrorRetried(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusBadRequest)
	pc, addr := clientPeer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(int(status.Load())) })
	items := []*bufferedPush{{key: "k", value: []byte("v"), expiresAt: time.Now().Add(time.Minute).UnixNano()}}
	if sent, retry, called := pc.sendReplay(addr, items); sent != 0 || len(retry) != 0 || !called || pc.PushBufferDropped[droppedRejected].Load() != 1 {
		t.Fatalf("400: sent %d, retry %d", sent, len(retry))
	}
	status.Store(http.StatusServiceUnavailable)
	if sent, retry, _ := pc.sendReplay(addr, items); sent != 0 || len(retry) != 1 {
		t.Fatalf("503: sent %d, retry %d", sent, len(retry))
	}
}

// newHungPeer serves a peer whose /_cache/set never answers until the client
// gives up; hung gets a value when the first set arrives.
func newHungPeer(t *testing.T, hung chan<- struct{}) string {
	t.Helper()
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // the server notices a closed connection only after the body
		once.Do(func() { close(hung) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// One write-through Replicate to a replica whose breaker is open: the push
// joins its backlog (one key in eight is new, the others coalesce), which
// before this buffer was a dropped push.
func BenchmarkPeerPushBuffer_ReplicateToOpenPeer(b *testing.B) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1,peer:1",
		WriteThrough: true, ReplicationFactor: 2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	defer pc.Close()
	pb := pc.breaker("peer:1")
	pb.seenUp.Store(true)
	pb.suspendedUntil.Store(time.Now().Add(time.Hour).UnixNano())
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}
	value := make([]byte, 4096)
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		pc.Replicate(keys[n%len(keys)], value, time.Minute)
		n++
	}
}

// The buffer alone: add, take a round, requeue it.
func BenchmarkPeerPushBuffer_AddTakeRequeue(b *testing.B) {
	buf := newPushBuffer(DefaultPushBufferMaxEntries, DefaultPushBufferMaxBytes, nil)
	now := time.Now().UnixNano()
	value := make([]byte, 4096)
	keys := make([]string, 256)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		buf.add("p", &bufferedPush{key: keys[n%len(keys)], value: value, seq: uint64(n), expiresAt: now + int64(time.Hour)}, now)
		if n%32 == 31 {
			items, _ := buf.take("p", 32, now)
			buf.requeue("p", items, now)
		}
		n++
	}
}
