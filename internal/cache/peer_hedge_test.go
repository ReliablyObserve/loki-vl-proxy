package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hedgeStub is a peer whose /_cache/get answers with value, or, while paused,
// accepts the request and never answers until the test resumes it or the
// caller goes away (a SIGSTOPped or hung process).
type hedgeStub struct {
	srv    *httptest.Server
	paused atomic.Bool
	calls  atomic.Int64
	// headerThenStall sends the headers, then waits stall before the body.
	headerThenStall time.Duration
	release         chan struct{}
	once            sync.Once
}

func newHedgeStub(t *testing.T, value string) *hedgeStub {
	t.Helper()
	s := &hedgeStub{release: make(chan struct{})}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		if s.paused.Load() {
			select {
			case <-s.release:
			case <-r.Context().Done():
				return
			}
		}
		if r.URL.Path == "/_cache/has" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.Header().Set("X-Cache-Ttl-Ms", "60000")
		w.WriteHeader(http.StatusOK)
		if s.headerThenStall > 0 {
			w.(http.Flusher).Flush()
			time.Sleep(s.headerThenStall)
		}
		_, _ = w.Write([]byte(value))
	}))
	// Released before the server closes: Close waits for open handlers.
	t.Cleanup(s.srv.Close)
	t.Cleanup(s.resume)
	return s
}

func (s *hedgeStub) addr() string { return s.srv.Listener.Addr().String() }

func (s *hedgeStub) resume() { s.once.Do(func() { close(s.release) }) }

// hedgeKey returns a key whose replicas on pc are want, in that order.
func hedgeKey(t *testing.T, pc *PeerCache, want ...string) string {
	t.Helper()
	for i := 0; i < 20000; i++ {
		key := fmt.Sprintf("hedge-key-%d", i)
		got := pc.replicasOf(key)
		if len(got) >= len(want) && strings.Join(got[:len(want)], ",") == strings.Join(want, ",") {
			return key
		}
	}
	t.Fatalf("no key with replicas %v", want)
	return ""
}

func hedgePeerCache(t *testing.T, rf int, timeout, hedgeMax time.Duration, peers ...string) *PeerCache {
	t.Helper()
	pc := NewPeerCache(PeerConfig{
		SelfAddr:          "127.0.0.1:1",
		DiscoveryType:     "static",
		StaticPeers:       strings.Join(append([]string{"127.0.0.1:1"}, peers...), ","),
		ReplicationFactor: rf,
		Timeout:           timeout,
		ReadHedgeMaxDelay: hedgeMax,
	})
	t.Cleanup(pc.Close)
	return pc
}

// Loose bounds: -race and a busy machine slow everything, but a paused
// replica must never cost a read the peer timeout.
const (
	hedgeTestTimeout = 2 * time.Second
	hedgeTestMax     = 100 * time.Millisecond
	hedgeTestSlack   = 400 * time.Millisecond
)

// A lookup whose owner is paused asks the backup once the owner has not sent
// its response headers within the hedge delay, and answers from it long
// before the peer timeout; the owner's call runs on in the background, where
// its timeout suspends the owner.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_PausedOwnerAnswersFromBackup(t *testing.T) {
	owner, backup := newHedgeStub(t, "from-owner"), newHedgeStub(t, "from-backup")
	pc := hedgePeerCache(t, 3, hedgeTestTimeout, hedgeTestMax, owner.addr(), backup.addr())
	key := hedgeKey(t, pc, owner.addr(), backup.addr())
	owner.paused.Store(true)

	started := time.Now()
	v, _, ok := pc.Get(key)
	took := time.Since(started)
	if !ok || string(v) != "from-backup" {
		t.Fatalf("got %q %v, want the backup's value", v, ok)
	}
	if took > hedgeTestMax+hedgeTestSlack {
		t.Fatalf("the lookup took %s with its owner paused, want at most the hedge delay %s plus %s", took, hedgeTestMax, hedgeTestSlack)
	}
	next, gaveUp, _ := pc.ReadHedgeStats()
	if next != 1 || gaveUp != 0 || pc.BackupHits.Load() != 1 {
		t.Fatalf("hedges next=%d gave_up=%d, backup hits %d; want 1, 0, 1", next, gaveUp, pc.BackupHits.Load())
	}
	if owner.calls.Load() != 1 || backup.calls.Load() != 1 {
		t.Fatalf("owner called %d times, backup %d; want once each", owner.calls.Load(), backup.calls.Load())
	}
	// The owner's call still runs: it lags, so the next lookups ask the
	// backup first and wait on nothing.
	if !pc.peerLagging(owner.addr()) {
		t.Fatal("the owner whose call lost the race is not lagging")
	}
	for i := 0; i < 5; i++ {
		other := hedgeKey(t, pc, owner.addr(), backup.addr()) + fmt.Sprint("-", i)
		pc.mu.RLock()
		replicas := pc.ring.getN(other, 3)
		pc.mu.RUnlock()
		if replicas[0] != owner.addr() {
			continue
		}
		started := time.Now()
		if v, _, ok := pc.Get(other); !ok || string(v) != "from-backup" {
			t.Fatalf("lookup beside the lagging owner: %q %v", v, ok)
		}
		if d := time.Since(started); d > hedgeTestMax/2 {
			t.Fatalf("a lookup beside the lagging owner took %s, want no hedge delay", d)
		}
	}
	if owner.calls.Load() != 1 {
		t.Fatalf("the lagging owner was asked %d times, want once", owner.calls.Load())
	}
}

// A lookup whose only other replica is paused stops waiting after the
// give-up delay and goes on as a miss (the caller reads VictoriaLogs); until
// the call it left ends, lookups skip that peer without a call, and the
// call's timeout then suspends it. Concurrent lookups of the key (singleflight
// followers) wait no longer than the leader.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_PausedOnlyReplicaGivesUp(t *testing.T) {
	const timeout = 600 * time.Millisecond
	owner := newHedgeStub(t, "v")
	pc := hedgePeerCache(t, 1, timeout, hedgeTestMax, owner.addr())
	key := hedgeKey(t, pc, owner.addr())
	owner.paused.Store(true)

	var wg sync.WaitGroup
	took := make([]time.Duration, 4)
	for i := range took {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			if _, _, ok := pc.Get(key); ok {
				t.Error("a paused replica answered")
			}
			took[i] = time.Since(started)
		}()
	}
	wg.Wait()
	for i, d := range took {
		if d > hedgeTestMax+hedgeTestSlack || d >= timeout {
			t.Fatalf("lookup %d took %s, want at most the give-up delay %s plus %s, below the peer timeout %s", i, d, hedgeTestMax, hedgeTestSlack, timeout)
		}
	}
	if _, gaveUp, _ := pc.ReadHedgeStats(); gaveUp != 1 {
		t.Fatalf("gave_up=%d, want 1 (the followers share the leader's read)", gaveUp)
	}
	// The call still runs: the next lookup skips the slow peer at once.
	if pc.peerSlow(owner.addr()) {
		started := time.Now()
		if _, _, ok := pc.Get(key + "-other"); ok {
			t.Fatal("a slow peer answered")
		}
		if d := time.Since(started); d > 50*time.Millisecond {
			t.Fatalf("a lookup beside the slow call took %s, want no wait", d)
		}
	}
	// The call left running times out and suspends the owner.
	pb := pc.breaker(owner.addr())
	waitFor(t, "the owner suspension", func() bool { return pb.suspendedUntil.Load() > time.Now().UnixNano() })
	waitFor(t, "the slow call to end", func() bool { return !pc.peerSlow(owner.addr()) })
	calls := owner.calls.Load()
	started := time.Now()
	if _, _, ok := pc.Get(key); ok {
		t.Fatal("a suspended peer answered")
	}
	if d := time.Since(started); d > 50*time.Millisecond || owner.calls.Load() != calls {
		t.Fatalf("a lookup of a suspended peer took %s and %d calls", d, owner.calls.Load()-calls)
	}

	// After the cooldown the half-open probe is a lookup: it pays at most the
	// give-up delay, never the timeout, and its call suspends the owner again.
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	started = time.Now()
	if _, _, ok := pc.Get(key); ok {
		t.Fatal("a paused peer answered the probe")
	}
	if d := time.Since(started); d > hedgeTestMax+hedgeTestSlack || d >= timeout {
		t.Fatalf("the probing lookup took %s, want at most the give-up delay %s plus %s", d, hedgeTestMax, hedgeTestSlack)
	}
	waitFor(t, "the probe to suspend the owner again", func() bool { return pb.suspendedUntil.Load() > time.Now().UnixNano() })

	// The owner answers again: the next probe lifts the suspension.
	owner.resume()
	owner.paused.Store(false)
	waitFor(t, "the slow probe to end", func() bool { return !pc.peerSlow(owner.addr()) })
	pb.suspendedUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	if v, _, ok := pc.Get(key); !ok || string(v) != "v" {
		t.Fatalf("the resumed owner: %q %v", v, ok)
	}
	if pb.suspendedUntil.Load() != 0 {
		t.Fatal("a successful probe left the owner suspended")
	}
}

// A replica that sent its headers is answering: the read waits for its body
// (within the peer timeout) and asks no other replica.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_AnsweringReplicaIsNotHedged(t *testing.T) {
	owner, backup := newHedgeStub(t, "from-owner"), newHedgeStub(t, "from-backup")
	owner.headerThenStall = 3 * hedgeTestMax
	pc := hedgePeerCache(t, 3, hedgeTestTimeout, hedgeTestMax, owner.addr(), backup.addr())
	key := hedgeKey(t, pc, owner.addr(), backup.addr())
	if v, _, ok := pc.Get(key); !ok || string(v) != "from-owner" {
		t.Fatalf("got %q %v, want the owner's value", v, ok)
	}
	if next, gaveUp, _ := pc.ReadHedgeStats(); next != 0 || gaveUp != 0 || backup.calls.Load() != 0 {
		t.Fatalf("hedges next=%d gave_up=%d, backup calls %d; want none", next, gaveUp, backup.calls.Load())
	}
}

// A healthy owner answers every read: no hedge, no second call, and the
// measured header latencies bring the delay down from the maximum.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_HealthyFleetAsksOneReplica(t *testing.T) {
	owner, backup := newHedgeStub(t, "from-owner"), newHedgeStub(t, "from-backup")
	pc := hedgePeerCache(t, 3, hedgeTestTimeout, time.Second, owner.addr(), backup.addr())
	key := hedgeKey(t, pc, owner.addr(), backup.addr())
	if d := pc.readHedgeDelay(); d != time.Second {
		t.Fatalf("delay before any measurement %s, want the maximum", d)
	}
	for i := 0; i < peerReadHedgeMinSamples; i++ {
		if v, _, ok := pc.Get(key); !ok || string(v) != "from-owner" {
			t.Fatalf("read %d: %q %v", i, v, ok)
		}
	}
	if backup.calls.Load() != 0 {
		t.Fatalf("the backup was asked %d times", backup.calls.Load())
	}
	if d := pc.readHedgeDelay(); d >= time.Second || d < peerReadHedgeFloor {
		t.Fatalf("delay after %d loopback reads %s, want it between the floor and the maximum", peerReadHedgeMinSamples, d)
	}
	if got := pc.readGiveUpDelay(); got != time.Second {
		t.Fatalf("give-up delay %s, want the maximum", got)
	}
}

// With hedging off a read waits on a paused replica up to the peer timeout,
// as before hedged reads.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_OffWaitsThePeerTimeout(t *testing.T) {
	const timeout = 250 * time.Millisecond
	owner := newHedgeStub(t, "v")
	pc := hedgePeerCache(t, 1, timeout, -1, owner.addr())
	key := hedgeKey(t, pc, owner.addr())
	owner.paused.Store(true)
	started := time.Now()
	if _, _, ok := pc.Get(key); ok {
		t.Fatal("a paused replica answered")
	}
	if d := time.Since(started); d < timeout {
		t.Fatalf("with hedging off the lookup took %s, want the peer timeout %s", d, timeout)
	}
	if d := pc.readHedgeDelay(); d != 0 {
		t.Fatalf("delay %s with hedging off", d)
	}
}

// The /patterns owner presence check waits at most the give-up delay on a
// paused peer, and the peer is then skipped without a call.
//
// conformance: peer-cache-read-hedge, peer-cache-patterns
func TestPeerReadHedge_ReachableIsBoundedByTheGiveUpDelay(t *testing.T) {
	owner := newHedgeStub(t, "v")
	pc := hedgePeerCache(t, 1, hedgeTestTimeout, hedgeTestMax, owner.addr())
	owner.paused.Store(true)
	started := time.Now()
	if pc.Reachable(owner.addr(), "k") {
		t.Fatal("a paused peer is reachable")
	}
	if d := time.Since(started); d > hedgeTestMax+hedgeTestSlack {
		t.Fatalf("the check took %s, want at most the give-up delay %s plus %s", d, hedgeTestMax, hedgeTestSlack)
	}
	calls := owner.calls.Load()
	if pc.Reachable(owner.addr(), "k") || owner.calls.Load() != calls {
		t.Fatal("a slow peer was checked again")
	}
	if pc.PeerErrorReasons()["peer_slow"] < 2 {
		t.Fatalf("peer_slow reasons %d, want 2", pc.PeerErrorReasons()["peer_slow"])
	}
}

// Close cancels the calls reads left running on a paused peer: no goroutine
// outlives the peer cache.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_CloseStopsBackgroundCalls(t *testing.T) {
	owner := newHedgeStub(t, "v")
	owner.paused.Store(true)
	before := runtime.NumGoroutine()
	pc := NewPeerCache(PeerConfig{
		SelfAddr:          "127.0.0.1:1",
		DiscoveryType:     "static",
		StaticPeers:       "127.0.0.1:1," + owner.addr(),
		Timeout:           30 * time.Second,
		ReadHedgeMaxDelay: 20 * time.Millisecond,
	})
	// Concurrent reads of distinct keys owned by the paused peer: each
	// leaves a call running when it gives up.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = pc.Get(fmt.Sprintf("close-key-%d", i))
		}()
	}
	wg.Wait()
	if pc.breaker(owner.addr()).slowCalls.Load() == 0 {
		t.Fatal("no read left a call running on the paused peer")
	}
	pc.Close()
	pc.client.CloseIdleConnections()
	waitFor(t, "the background calls to stop", func() bool { return runtime.NumGoroutine() <= before+2 })
}

// The delay is twice the measured header p99, between the floor and the
// give-up delay, the maximum until enough latencies are measured; the
// histogram halves itself so the estimate follows the current latency.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedge_DelayFollowsHeaderLatency(t *testing.T) {
	pc := &PeerCache{hedgeMax: 200 * time.Millisecond, client: &http.Client{Timeout: 2 * time.Second}}
	if d := pc.readHedgeDelay(); d != 200*time.Millisecond {
		t.Fatalf("unmeasured delay %s, want the maximum", d)
	}
	for i := 0; i < 200; i++ {
		pc.headerLatency.observe(2 * time.Millisecond) // bucket le=2.5ms
	}
	if d := pc.readHedgeDelay(); d != 5*time.Millisecond {
		t.Fatalf("delay %s, want twice the 2.5ms p99 bucket", d)
	}
	for i := 0; i < 2; i++ {
		pc.headerLatency.observe(3 * time.Second) // 1% outliers do not move the p99
	}
	if d := pc.readHedgeDelay(); d != 5*time.Millisecond {
		t.Fatalf("delay %s after 1%% outliers, want unchanged", d)
	}
	for i := 0; i < 50; i++ {
		pc.headerLatency.observe(40 * time.Millisecond) // le=50ms
	}
	if d := pc.readHedgeDelay(); d != 100*time.Millisecond {
		t.Fatalf("delay %s, want twice the 50ms p99 bucket", d)
	}
	for i := 0; i < 100; i++ {
		pc.headerLatency.observe(400 * time.Millisecond) // le=0.5s: capped
	}
	if d := pc.readHedgeDelay(); d != 200*time.Millisecond {
		t.Fatalf("delay %s, want the maximum", d)
	}
	// Half the peer timeout caps it too.
	pc.client.Timeout = 100 * time.Millisecond
	if d, g := pc.readHedgeDelay(), pc.readGiveUpDelay(); d != 50*time.Millisecond || g != 50*time.Millisecond {
		t.Fatalf("delay %s, give-up %s, want half the 100ms timeout", d, g)
	}
	// A floor of 5ms.
	fast := &PeerCache{hedgeMax: time.Second, client: &http.Client{Timeout: 2 * time.Second}}
	for i := 0; i < 200; i++ {
		fast.headerLatency.observe(100 * time.Microsecond)
	}
	if d := fast.readHedgeDelay(); d != peerReadHedgeFloor {
		t.Fatalf("delay %s for sub-millisecond peers, want the floor", d)
	}
	// Decay: the window halves, and newer latencies take over.
	for i := 0; i < peerHeaderLatencyDecayAt; i++ {
		fast.headerLatency.observe(20 * time.Millisecond)
	}
	if total := fast.headerLatency.total.Load(); total >= peerHeaderLatencyDecayAt {
		t.Fatalf("histogram holds %d latencies, want it halved below %d", total, peerHeaderLatencyDecayAt)
	}
	if d := fast.readHedgeDelay(); d != 50*time.Millisecond {
		t.Fatalf("delay %s after the fleet slowed to 20ms, want twice the 25ms bucket", d)
	}
	off := &PeerCache{}
	if off.readHedgeDelay() != 0 || off.readGiveUpDelay() != 0 {
		t.Fatal("a peer cache without a maximum hedges")
	}
	text := pc.readHedgeText()
	for _, want := range []string{`loki_vl_proxy_peer_cache_read_hedges_total{action="next_replica"} 0`, `loki_vl_proxy_peer_cache_read_hedges_total{action="gave_up"} 0`, "loki_vl_proxy_peer_cache_read_hedge_delay_seconds 0.05"} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics lack %q:\n%s", want, text)
		}
	}
}
