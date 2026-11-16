package cache

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Hedged peer reads. A read never waits on a replica that does not answer:
// a lookup asks the key's replicas in ring order, and when the replica asked
// last has not sent its response headers within the hedge delay, the lookup
// also asks the next replica and takes the first answer from either. With no
// replica left to ask it waits at most the give-up delay (the configured
// maximum) since the last one was asked, then goes on as a miss: the caller
// reads VictoriaLogs. The calls it stopped waiting for keep running in the
// background under the peer's context: their own timeout still suspends a
// paused or hung peer (recordPeerCallError) and cancels every call waiting
// on it, so the breaker trips as before, while the request paid the give-up
// delay instead of -peer-timeout. Until such a call ends, reads skip its peer
// (peerSlow), and a peer whose call is still running after another replica
// answered first is asked after the key's other replicas (peerLagging), so
// one read per proxy pays the delay.
//
// The hedge delay follows the peers' measured time to response headers:
// twice the p99 of recent header latencies, at least peerReadHedgeFloor and
// at most the give-up delay. A healthy fleet therefore asks a second replica
// for about one read in a hundred, which costs a peer read, never a
// VictoriaLogs read; going on as a miss waits the longer give-up delay.

const (
	// DefaultPeerReadHedgeMaxDelay bounds how long a read waits on one
	// replica's response headers before it asks the next replica or goes on as
	// a miss, until enough header latencies are measured, and above them.
	DefaultPeerReadHedgeMaxDelay = 200 * time.Millisecond
	// peerReadHedgeFloor is the shortest hedge delay: a peer's GC pause or a
	// scheduling hiccup of a few milliseconds does not send a second read.
	peerReadHedgeFloor = 5 * time.Millisecond
	// peerReadHedgeMinSamples header latencies are measured before the delay
	// follows them; until then it is the maximum.
	peerReadHedgeMinSamples = 64
	// peerHeaderLatencyDecayAt halves the measured counts once the window
	// holds this many, so the estimate follows the fleet's current latency.
	peerHeaderLatencyDecayAt = 4096
)

// Hedge outcomes, the action label of
// loki_vl_proxy_peer_cache_read_hedges_total.
const (
	hedgeNextReplica = iota // the next replica was asked as well
	hedgeGaveUp             // no replica left: the read went on as a miss
	hedgeOutcomes
)

var hedgeOutcomeNames = [hedgeOutcomes]string{"next_replica", "gave_up"}

// headerLatency is a decaying histogram of the time peers took to send their
// response headers, over peerFetchBuckets plus an overflow bucket.
type headerLatency struct {
	counts [len(peerFetchBuckets) + 1]atomic.Int64
	total  atomic.Int64
}

func (h *headerLatency) observe(d time.Duration) {
	sec := d.Seconds()
	i := len(peerFetchBuckets)
	for j, le := range peerFetchBuckets {
		if sec <= le {
			i = j
			break
		}
	}
	h.counts[i].Add(1)
	if h.total.Add(1) >= peerHeaderLatencyDecayAt {
		// Halve every bucket. Concurrent observations may lose a count to
		// the halving, which only blurs an estimate.
		var sum int64
		for j := range h.counts {
			for {
				old := h.counts[j].Load()
				if h.counts[j].CompareAndSwap(old, old/2) {
					sum += old / 2
					break
				}
			}
		}
		h.total.Store(sum)
	}
}

// p99 is the upper bound of the bucket holding the 99th percentile, and false
// while fewer than peerReadHedgeMinSamples latencies are measured.
func (h *headerLatency) p99() (time.Duration, bool) {
	var counts [len(peerFetchBuckets) + 1]int64
	var total int64
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
		total += counts[i]
	}
	if total < peerReadHedgeMinSamples {
		return 0, false
	}
	rank := total - total/100 // the smallest count covering 99%
	var seen int64
	for i, n := range counts {
		seen += n
		if seen >= rank {
			if i == len(peerFetchBuckets) {
				return time.Duration(1<<63 - 1), true
			}
			return time.Duration(peerFetchBuckets[i] * float64(time.Second)), true
		}
	}
	return time.Duration(1<<63 - 1), true
}

// resolveReadHedgeMax maps PeerConfig.ReadHedgeMaxDelay to the maximum in
// force: 0 is the default, a negative value turns hedging off.
func resolveReadHedgeMax(configured time.Duration) time.Duration {
	switch {
	case configured == 0:
		return DefaultPeerReadHedgeMaxDelay
	case configured < 0:
		return 0
	default:
		return configured
	}
}

// readHedgeDelay is how long a read waits on one replica's response headers
// before it asks the next replica or goes on as a miss; 0 when hedging is off.
func (pc *PeerCache) readHedgeDelay() time.Duration {
	if pc.hedgeMax <= 0 {
		return 0
	}
	ceiling := pc.readGiveUpDelay()
	d := ceiling
	if p99, ok := pc.headerLatency.p99(); ok && p99 < ceiling/2 {
		d = 2 * p99
	}
	return max(min(d, ceiling), min(peerReadHedgeFloor, ceiling))
}

// readGiveUpDelay is how long a read waits on the last replica it can ask
// before it goes on as a miss: the configured maximum, at most half the peer
// timeout. A miss costs a VictoriaLogs read, so it is not taken at the
// adaptive delay that only asks another replica.
func (pc *PeerCache) readGiveUpDelay() time.Duration {
	if pc.hedgeMax <= 0 {
		return 0
	}
	ceiling := pc.hedgeMax
	if pc.client != nil && pc.client.Timeout > 0 && pc.client.Timeout/2 < ceiling {
		ceiling = pc.client.Timeout / 2
	}
	return ceiling
}

// peerCall tracks one peer call a read may stop waiting for. While it runs
// after another replica answered the read first, its peer is lagging
// (peerBreaker.laggingCalls): reads ask it after the key's other replicas.
// While it runs after a read gave up on it, its peer is slow
// (peerBreaker.slowCalls): reads skip it.
type peerCall struct {
	pb     *peerBreaker
	state  atomic.Int32 // callRunning, callLagging, callSlow or callFinished
	answer replicaAnswer
}

const (
	callRunning = iota
	callLagging
	callSlow
	callFinished
)

func (c *peerCall) markLagging() {
	if c.state.CompareAndSwap(callRunning, callLagging) {
		c.pb.laggingCalls.Add(1)
	}
}

func (c *peerCall) markSlow() {
	for {
		switch s := c.state.Load(); s {
		case callRunning, callLagging:
			if !c.state.CompareAndSwap(s, callSlow) {
				continue
			}
			if s == callLagging {
				c.pb.laggingCalls.Add(-1)
			}
			c.pb.slowCalls.Add(1)
		}
		return
	}
}

func (c *peerCall) finish() {
	switch c.state.Swap(callFinished) {
	case callLagging:
		c.pb.laggingCalls.Add(-1)
	case callSlow:
		c.pb.slowCalls.Add(-1)
	}
}

// peerSlow reports whether a read gave up on a call to addr that is still
// running: reads skip the peer until that call ends (an answer clears it, a
// timeout suspends the peer).
func (pc *PeerCache) peerSlow(addr string) bool {
	return pc.breaker(addr).slowCalls.Load() > 0
}

// peerLagging reports whether another replica answered a read before a call
// to addr that is still running: reads ask the peer after the key's other
// replicas until that call ends.
func (pc *PeerCache) peerLagging(addr string) bool {
	return pc.breaker(addr).laggingCalls.Load() > 0
}

// peerReadAllowed is peerAllowed for a read: a peer that is slow right now is
// skipped as well, without taking the breaker's half-open probe.
func (pc *PeerCache) peerReadAllowed(addr string) bool {
	if pc.peerSlow(addr) {
		pc.RecordPeerErrorReason("peer_slow")
		return false
	}
	if !pc.peerAllowed(addr) {
		pc.RecordPeerErrorReason("breaker_open")
		return false
	}
	return true
}

// replicaAnswer is what one replica answered a read.
type replicaAnswer struct {
	index   int
	body    []byte
	ttl     time.Duration
	memOnly bool
	status  int
}

// A hedged read hears of each call through events: 2i when replica i sent
// its response headers, 2i+1 when its answer (peerCall.answer) is in.
func headersEvent(i int) int { return 2 * i }
func answerEvent(i int) int  { return 2*i + 1 }

// headerSink tells a hedged read that replica index sent its response
// headers; the zero value tells nobody.
type headerSink struct {
	events chan<- int
	index  int
}

func (h headerSink) arrived() {
	if h.events != nil {
		h.events <- headersEvent(h.index)
	}
}

// hedgedRead is one read across a key's replicas: one allocation holds its
// state and the calls it may stop waiting for.
type hedgedRead struct {
	pc        *PeerCache
	key       string
	replicas  []string
	events    chan int
	calls     [MaxReplicationFactor]peerCall
	order     [MaxReplicationFactor]int8 // replica indexes in the order asked
	n         int                        // replicas in order
	next      int                        // next position in order
	pending   int
	waitingOn int // the replica asked last, or -1
	asked     bool
}

// plan orders the replicas to ask: ring order, lagging peers last.
func (r *hedgedRead) plan() {
	pc := r.pc
	for pass := 0; pass < 2; pass++ {
		for i, addr := range r.replicas[:min(len(r.replicas), MaxReplicationFactor)] {
			if addr == pc.selfAddr || pc.peerLagging(addr) != (pass == 1) {
				continue
			}
			r.order[r.n] = int8(i)
			r.n++
		}
	}
}

// launch asks the next replica this read may ask; false when none is left.
func (r *hedgedRead) launch() bool {
	pc := r.pc
	for r.next < r.n {
		i := int(r.order[r.next])
		addr := r.replicas[i]
		r.next++
		if !pc.peerReadAllowed(addr) {
			pc.PeerErrors.Add(1)
			continue
		}
		r.asked = true
		r.pending++
		r.calls[i].pb = pc.breaker(addr)
		r.waitingOn = i
		go r.fetch(i, addr)
		return true
	}
	return false
}

func (r *hedgedRead) fetch(i int, addr string) {
	defer r.calls[i].finish()
	started := time.Now()
	b, t, m, status := r.pc.fetchFrom(addr, r.key, headerSink{events: r.events, index: i})
	r.pc.observeReplicaFetch(i, status, time.Since(started))
	r.calls[i].answer = replicaAnswer{index: i, body: b, ttl: t, memOnly: m, status: status}
	r.events <- answerEvent(i) // publishes the answer to the reader
}

// giveUp leaves the calls still running to finish in the background (their
// timeout suspends a paused peer); reads skip their peers until then.
func (r *hedgedRead) giveUp() {
	for i := range r.calls {
		if r.calls[i].pb != nil {
			r.calls[i].markSlow()
		}
	}
	r.pc.readHedges[hedgeGaveUp].Add(1)
}

// hedgeTimers reuses the timers of hedged reads.
var hedgeTimers = sync.Pool{New: func() any {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return t
}}

// askReplicas asks the key's replicas in ring order and returns the first
// hit, hedging every call by the read hedge delay (see the top of this file).
func (pc *PeerCache) askReplicas(key string, replicas []string) (answer replicaAnswer, ok, asked bool) {
	r := &hedgedRead{pc: pc, key: key, replicas: replicas, waitingOn: -1}
	r.plan()
	// Every call sends at most its headers and its answer, so no sender
	// ever blocks on a read that stopped waiting.
	r.events = make(chan int, 2*min(len(replicas), MaxReplicationFactor))
	if !r.launch() {
		return replicaAnswer{}, false, r.asked
	}
	hedge, giveUp := pc.readHedgeDelay(), pc.readGiveUpDelay()
	lastLaunch := time.Now()
	var timer *time.Timer
	var expired <-chan time.Time
	arm := func(d time.Duration) {
		if hedge <= 0 {
			return // hedging off: wait on each replica up to the peer timeout
		}
		if timer == nil {
			timer = hedgeTimers.Get().(*time.Timer)
		}
		timer.Reset(d)
		expired = timer.C
	}
	disarm := func() {
		if timer != nil {
			timer.Stop()
		}
		expired = nil
	}
	defer func() {
		if timer != nil {
			timer.Stop()
			hedgeTimers.Put(timer)
		}
	}()
	// relaunch asks the next replica and arms the hedge delay for it
	// (launched), or, with none left, arms what is left of the give-up delay
	// since the last launch for the calls still running (waiting).
	relaunch := func() (launched, waiting bool) {
		if r.launch() {
			lastLaunch = time.Now()
			arm(hedge)
			return true, true
		}
		if r.pending == 0 {
			return false, false
		}
		left := giveUp - time.Since(lastLaunch)
		if left <= 0 {
			return false, false
		}
		arm(left)
		return false, true
	}
	arm(hedge)
	for {
		select {
		case e := <-r.events:
			i := e / 2
			if e == headersEvent(i) {
				if i == r.waitingOn {
					// The replica asked last is answering: its body follows
					// within the peer timeout, so no second replica is asked.
					disarm()
				}
				continue
			}
			r.pending--
			a := r.calls[i].answer
			if a.status == http.StatusOK {
				for j := range r.calls {
					if j != i && r.calls[j].pb != nil {
						r.calls[j].markLagging() // a call still running lost the race
					}
				}
				return a, true, r.asked
			}
			if a.index != r.waitingOn && r.pending > 0 {
				continue // a replica hedged away from: the one waited on still runs
			}
			// The replica waited on answered a miss or failed: ask the next
			// one now, as an unhedged read would.
			disarm()
			r.waitingOn = -1
			if _, waiting := relaunch(); waiting {
				continue
			}
			if r.pending > 0 {
				r.giveUp() // only replicas past the give-up delay are left
			}
			return replicaAnswer{}, false, r.asked
		case <-expired:
			expired = nil
			launched, waiting := relaunch()
			if launched {
				pc.readHedges[hedgeNextReplica].Add(1)
			}
			if waiting {
				continue
			}
			// No replica left to ask and none answered within the give-up
			// delay: this read goes on as a miss.
			r.giveUp()
			return replicaAnswer{}, false, r.asked
		}
	}
}

// observeReplicaFetch records one replica fetch in the fetch duration
// histogram by the replica's role and the outcome.
func (pc *PeerCache) observeReplicaFetch(index, status int, d time.Duration) {
	role, outcome := 0, 0
	if index > 0 {
		role = 1
	}
	switch status {
	case http.StatusNotFound:
		outcome = 1
	case 0:
		outcome = 2
	}
	pc.fetchDurations[role][outcome].observe(d)
}

// boundedPeerCheck runs check (a call to addr) and waits for it at most the
// give-up delay: when the peer has not answered by then it counts as slow
// and the result is false, while the call finishes in the background.
func (pc *PeerCache) boundedPeerCheck(addr string, check func() bool) bool {
	delay := pc.readGiveUpDelay()
	if delay <= 0 {
		return check()
	}
	call := &peerCall{pb: pc.breaker(addr)}
	done := make(chan bool, 1)
	go func() {
		defer call.finish()
		done <- check()
	}()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case ok := <-done:
		return ok
	case <-timer.C:
		call.markSlow()
		pc.readHedges[hedgeGaveUp].Add(1)
		pc.RecordPeerErrorReason("peer_slow")
		return false
	}
}

// ReadHedgeStats returns the hedge counters by action and the current delay.
func (pc *PeerCache) ReadHedgeStats() (nextReplica, gaveUp int64, delay time.Duration) {
	if pc == nil {
		return 0, 0, 0
	}
	return pc.readHedges[hedgeNextReplica].Load(), pc.readHedges[hedgeGaveUp].Load(), pc.readHedgeDelay()
}

// readHedgeText renders the hedge metrics in the Prometheus text format.
func (pc *PeerCache) readHedgeText() string {
	var b strings.Builder
	b.WriteString("# HELP loki_vl_proxy_peer_cache_read_hedges_total Peer-cache reads whose replica had not sent response headers within the hedge delay, by action: next_replica (the next replica was asked as well) or gave_up (no replica left; the read went on as a miss).\n")
	b.WriteString("# TYPE loki_vl_proxy_peer_cache_read_hedges_total counter\n")
	for i, name := range hedgeOutcomeNames {
		fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_read_hedges_total{action=%q} %d\n", name, pc.readHedges[i].Load())
	}
	b.WriteString("# HELP loki_vl_proxy_peer_cache_read_hedge_delay_seconds How long a peer-cache read waits on one replica's response headers before it asks the next replica or goes on as a miss (0: hedging off).\n")
	b.WriteString("# TYPE loki_vl_proxy_peer_cache_read_hedge_delay_seconds gauge\n")
	fmt.Fprintf(&b, "loki_vl_proxy_peer_cache_read_hedge_delay_seconds %g\n", pc.readHedgeDelay().Seconds())
	return b.String()
}
