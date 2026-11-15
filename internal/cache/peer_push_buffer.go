package cache

import (
	"container/list"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Pushes a replica cannot take right now (its breaker is open, it is still
// starting, the push queue is full, or the push failed) wait in a bounded
// backlog per peer and are replayed once the peer answers, so the fleet
// returns to full replication on its own after a peer outage. Reads never
// wait for any of this: a lookup skips a peer whose breaker is open, as
// before.
//
// The backlog of a peer is FIFO and coalesced by key: a newer value of a key
// replaces the older one (the newest write wins; each push carries the
// sequence number of its Replicate call, so an older value routed to the
// backlog late never replaces a newer one). While a peer has a backlog, every
// new push to it joins the backlog, so a replayed older value never lands
// after a newer one.
//
// All backlogs share one budget per replica, -peer-push-buffer-max-entries
// and -peer-push-buffer-max-bytes. Above it the oldest entry of the largest
// backlog is dropped and counted (a peer down for long cannot crowd out one
// that just went down), with one log line at most every
// pushBufferDropLogEvery. Nothing on the request path ever waits on the
// backlog.
//
// One replayer per peer with a backlog sends it once the peer's breaker lets a
// call through, slowly first: one push (the half-open probe), then rounds that
// double from 1 to replayMaxBatch pushes, at most replayMaxInFlight at once,
// replayPace apart. A failed round puts its pushes back and waits
// replayBackoffMin doubling to replayBackoffMax, jittered, so many replicas
// replaying to one recovered peer do not hit it in step. The replayer stops on
// Close, and with it every push it has in flight.
const (
	// DefaultPushBufferMaxEntries and DefaultPushBufferMaxBytes are the
	// defaults of -peer-push-buffer-max-entries and -peer-push-buffer-max-bytes.
	DefaultPushBufferMaxEntries       = 8192
	DefaultPushBufferMaxBytes   int64 = 32 << 20

	replayMaxBatch    = 32
	replayMaxInFlight = 8 // below the transport's 20 connections per peer, so reads keep room
	replayPace        = 10 * time.Millisecond
	replayBackoffMin  = peerStartupRetry
	replayBackoffMax  = 10 * time.Second // the breaker cooldown
	replayWakeJitter  = 250 * time.Millisecond
	// A replica never serves a copy with less than 5s left, so a buffered push
	// with less than that is not worth sending.
	replayMinRemainingTTL = 5 * time.Second
	// The TTL a replica gives a push sent without one (serveSet).
	replicaDefaultTTL      = 30 * time.Second
	pushBufferDropLogEvery = 30 * time.Second
)

// Why a push went to the backlog (loki_vl_proxy_peer_cache_push_buffered_total{reason}).
const (
	bufferedPeerUnavailable = iota // the peer's breaker is open or it is suspended
	bufferedPeerNotUp              // the peer never answered since this process started
	bufferedQueueFull              // the push queue was full
	bufferedPushFailed             // the push failed (transport error, timeout, 429 or 5xx)
	bufferedBacklog                // the peer already had a backlog
	bufferedReasons
)

var bufferedReasonNames = [bufferedReasons]string{"peer_unavailable", "peer_not_up", "queue_full", "push_failed", "backlog"}

// Why a buffered push was dropped (loki_vl_proxy_peer_cache_push_buffer_dropped_total{reason}).
const (
	droppedFull        = iota // the buffer was full: the oldest entry of the largest backlog went
	droppedExpired            // less than replayMinRemainingTTL left
	droppedPeerRemoved        // the peer left the ring
	droppedNotReplica         // the peer is no longer one of the key's replicas
	droppedRejected           // the peer refused it (a 4xx other than 429)
	droppedClosed             // the cache was closed
	droppedDisabled           // buffering is off (-peer-push-buffer-max-entries or -max-bytes 0)
	droppedReasons
)

var droppedReasonNames = [droppedReasons]string{"full", "expired", "peer_removed", "not_replica", "rejected", "closed", "disabled"}

type bufferedPush struct {
	key       string
	value     []byte
	expiresAt int64 // Unix ns
	seq       uint64
	notUp     bool // the peer never answered when it was buffered (counted in PushPeerNotUp when dropped)
}

type pushBacklog struct {
	addr      string
	order     list.List // of *bufferedPush, oldest first
	byKey     map[string]*list.Element
	bytes     int64
	replaying bool
	count     *atomic.Int64 // mirrors order.Len() for lock-free reads
}

func (b *pushBacklog) setCount() { b.count.Store(int64(b.order.Len())) }

// pushBuffer holds the backlogs of every peer under one budget. Its methods
// do no I/O, so its invariants are fuzzed on their own.
type pushBuffer struct {
	mu         sync.Mutex
	peers      map[string]*pushBacklog
	entries    int
	bytes      int64
	maxEntries int
	maxBytes   int64
	// counter returns the atomic mirroring a peer's backlog length (the
	// peer's breaker); nil gives each backlog its own.
	counter func(addr string) *atomic.Int64
}

type pushDrop struct {
	addr   string // the peer whose backlog it was in
	entry  *bufferedPush
	reason int
}

func newPushBuffer(maxEntries int, maxBytes int64, counter func(string) *atomic.Int64) *pushBuffer {
	return &pushBuffer{peers: map[string]*pushBacklog{}, maxEntries: maxEntries, maxBytes: maxBytes, counter: counter}
}

func (b *pushBuffer) backlog(addr string) *pushBacklog {
	bl := b.peers[addr]
	if bl == nil {
		bl = &pushBacklog{addr: addr, byKey: map[string]*list.Element{}}
		if b.counter != nil {
			bl.count = b.counter(addr)
		} else {
			bl.count = new(atomic.Int64)
		}
		b.peers[addr] = bl
	}
	return bl
}

// add buffers p for addr. It reports whether p replaced or lost to an entry
// of the same key (coalesced), whether the caller must start the peer's
// replayer, and the entries dropped to stay within the budget.
func (b *pushBuffer) add(addr string, p *bufferedPush, now int64) (coalesced, startReplay bool, drops []pushDrop) {
	b.mu.Lock()
	defer b.mu.Unlock()
	bl := b.backlog(addr)
	if el, ok := bl.byKey[p.key]; ok {
		coalesced = true
		old := el.Value.(*bufferedPush)
		if p.seq >= old.seq {
			delta := int64(len(p.value) - len(old.value))
			bl.bytes += delta
			b.bytes += delta
			p.notUp = p.notUp || old.notUp
			el.Value = p
			bl.order.MoveToBack(el)
		}
	} else {
		bl.byKey[p.key] = bl.order.PushBack(p)
		bl.bytes += int64(len(p.value))
		b.entries++
		b.bytes += int64(len(p.value))
	}
	if !bl.replaying {
		bl.replaying = true
		startReplay = true
	}
	drops = b.evict(now, nil)
	bl.setCount()
	return coalesced, startReplay, drops
}

// evict drops the oldest entry of the largest backlog until the buffer is
// within its budget.
func (b *pushBuffer) evict(now int64, drops []pushDrop) []pushDrop {
	for b.entries > b.maxEntries || b.bytes > b.maxBytes {
		var victim *pushBacklog
		for _, bl := range b.peers {
			if bl.order.Len() == 0 {
				continue
			}
			if victim == nil || bl.bytes > victim.bytes || (bl.bytes == victim.bytes && bl.order.Len() > victim.order.Len()) {
				victim = bl
			}
		}
		if victim == nil {
			break
		}
		p := b.remove(victim, victim.order.Front())
		reason := droppedFull
		if p.expiresAt-now < int64(replayMinRemainingTTL) {
			reason = droppedExpired
		}
		drops = append(drops, pushDrop{victim.addr, p, reason})
		victim.setCount()
	}
	return drops
}

func (b *pushBuffer) remove(bl *pushBacklog, el *list.Element) *bufferedPush {
	p := bl.order.Remove(el).(*bufferedPush)
	delete(bl.byKey, p.key)
	bl.bytes -= int64(len(p.value))
	b.entries--
	b.bytes -= int64(len(p.value))
	return p
}

// take removes up to n of addr's oldest entries that are still worth sending
// and returns them, with the expired ones it removed on the way.
func (b *pushBuffer) take(addr string, n int, now int64) (batch []*bufferedPush, drops []pushDrop) {
	b.mu.Lock()
	defer b.mu.Unlock()
	bl := b.peers[addr]
	if bl == nil {
		return nil, nil
	}
	for len(batch) < n && bl.order.Len() > 0 {
		p := b.remove(bl, bl.order.Front())
		if p.expiresAt-now < int64(replayMinRemainingTTL) {
			drops = append(drops, pushDrop{addr, p, droppedExpired})
			continue
		}
		batch = append(batch, p)
	}
	bl.setCount()
	return batch, drops
}

// requeue puts pushes that could not be sent back at the front of addr's
// backlog, in their order; a key keeps whichever value is newer.
func (b *pushBuffer) requeue(addr string, items []*bufferedPush, now int64) (drops []pushDrop) {
	b.mu.Lock()
	defer b.mu.Unlock()
	bl := b.backlog(addr)
	for i := len(items) - 1; i >= 0; i-- {
		p := items[i]
		if el, ok := bl.byKey[p.key]; ok {
			if el.Value.(*bufferedPush).seq >= p.seq {
				continue // a newer value of the key waits already
			}
			b.remove(bl, el)
		}
		bl.byKey[p.key] = bl.order.PushFront(p)
		bl.bytes += int64(len(p.value))
		b.entries++
		b.bytes += int64(len(p.value))
	}
	drops = b.evict(now, nil)
	bl.setCount()
	return drops
}

// finish ends addr's replayer when its backlog is empty (it reports true);
// otherwise the replayer goes on.
func (b *pushBuffer) finish(addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	bl := b.peers[addr]
	if bl == nil {
		return true
	}
	if bl.order.Len() > 0 {
		return false
	}
	bl.replaying = false
	bl.setCount()
	delete(b.peers, addr)
	return true
}

// removePeer drops addr's backlog and returns its entries.
func (b *pushBuffer) removePeer(addr string) []*bufferedPush {
	b.mu.Lock()
	defer b.mu.Unlock()
	bl := b.peers[addr]
	if bl == nil {
		return nil
	}
	out := make([]*bufferedPush, 0, bl.order.Len())
	for bl.order.Len() > 0 {
		out = append(out, b.remove(bl, bl.order.Front()))
	}
	bl.setCount()
	return out
}

// absentPeers returns the peers with a backlog that keep is not true for.
func (b *pushBuffer) absentPeers(keep func(string) bool) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for addr := range b.peers {
		if !keep(addr) {
			out = append(out, addr)
		}
	}
	return out
}

func (b *pushBuffer) size() (entries int, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entries, b.bytes
}

// --- PeerCache side ----------------------------------------------------------

// bufferPush keeps a push addr could not take; it never blocks on the peer.
func (pc *PeerCache) bufferPush(addr string, job pushJob, reason int) {
	// A push to a peer that never answered since this process started counts
	// in PushPeerNotUp when it is dropped, whatever sent it here.
	p := &bufferedPush{key: job.key, value: job.value, seq: job.seq, notUp: reason == bufferedPeerNotUp || !pc.breaker(addr).seenUp.Load()}
	if pc.pushBuf == nil {
		pc.countDrops([]pushDrop{{addr, p, droppedDisabled}})
		return
	}
	select {
	case <-pc.done:
		pc.countDrops([]pushDrop{{addr, p, droppedClosed}})
		return
	default:
	}
	now := time.Now()
	ttl := job.ttl
	if ttl <= 0 {
		ttl = replicaDefaultTTL
	}
	p.expiresAt = now.Add(ttl).UnixNano()
	coalesced, start, drops := pc.pushBuf.add(addr, p, now.UnixNano())
	pc.PushBuffered[reason].Add(1)
	if coalesced {
		pc.PushCoalesced.Add(1)
	}
	pc.countDrops(drops)
	if start {
		pc.replayers.Add(1)
		go pc.replay(addr)
	}
}

func (pc *PeerCache) countDrops(drops []pushDrop) {
	full := int64(0)
	for _, d := range drops {
		pc.PushBufferDropped[d.reason].Add(1)
		if d.entry.notUp {
			pc.PushPeerNotUp.Add(1)
		}
		if d.reason == droppedFull {
			full++
		}
	}
	if full == 0 {
		return
	}
	pc.dropsSinceLog.Add(full)
	now := time.Now().UnixNano()
	last := pc.dropLoggedAt.Load()
	if now-last < int64(pushBufferDropLogEvery) || !pc.dropLoggedAt.CompareAndSwap(last, now) {
		return
	}
	entries, bytes := pc.pushBuf.size()
	pc.log.Warn("peer cache push buffer full: dropping the oldest buffered pushes; the replicas that miss them fetch them from another replica or VictoriaLogs on first lookup",
		"dropped", pc.dropsSinceLog.Swap(0), "buffered_entries", entries, "buffered_bytes", bytes,
		"max_entries_flag", "-peer-push-buffer-max-entries", "max_entries", pc.pushBuf.maxEntries,
		"max_bytes_flag", "-peer-push-buffer-max-bytes", "max_bytes", pc.pushBuf.maxBytes)
}

// blockedFor is how long calls to the peer are still held back by its
// breaker (0 when a call may go). It changes no breaker state, unlike
// peerAllowed, which claims the half-open probe.
func (pb *peerBreaker) blockedFor(now int64) time.Duration {
	var until int64
	if !pb.seenUp.Load() {
		until = pb.notUpUntil.Load()
	}
	if s := pb.suspendedUntil.Load(); s > until {
		until = s
	}
	if pb.failures.Load() >= pb.threshold {
		if open := time.UnixMilli(pb.lastFail.Load()).Add(pb.cooldown).UnixNano(); open > until {
			until = open
		}
	}
	if until <= now {
		return 0
	}
	return time.Duration(until - now)
}

// replay sends addr's backlog once the peer takes calls again, slowly first.
func (pc *PeerCache) replay(addr string) {
	defer pc.replayers.Add(-1)
	pb := pc.breaker(addr)
	timer := time.NewTimer(0)
	defer timer.Stop()
	batch, backoff := 1, replayBackoffMin
	outage, replayed, started := false, 0, time.Now()
	failed := func() {
		batch = 1
		timer.Reset(jittered(backoff))
		backoff = min(2*backoff, replayBackoffMax)
	}
	for {
		select {
		case <-pc.done:
			pc.dropBacklog(addr, droppedClosed)
			return
		case <-timer.C:
		case <-pb.wake: // another call found the peer answering again
		}
		now := time.Now()
		if wait := pb.blockedFor(now.UnixNano()); wait > 0 {
			outage, batch = true, 1
			timer.Reset(wait + time.Duration(rand.Int63n(int64(replayWakeJitter))))
			continue
		}
		halfOpen := pb.suspendedUntil.Load() != 0
		if !pc.peerAllowed(addr) { // another call holds the half-open probe
			outage = true
			failed()
			continue
		}
		if halfOpen {
			batch = 1 // this round is the probe
		}
		items, drops := pc.pushBuf.take(addr, batch, now.UnixNano())
		pc.countDrops(drops)
		if len(items) == 0 {
			if halfOpen {
				pb.releaseProbe() // no call made: hand the probe back
			}
			if pc.pushBuf.finish(addr) {
				if outage && replayed > 0 {
					pc.log.Info("peer cache replayed the pushes buffered for a peer", "peer", addr, "replayed", replayed, "took", time.Since(started).Round(time.Millisecond).String())
				}
				return
			}
			timer.Reset(replayPace)
			continue
		}
		sent, retry, called := pc.sendReplay(addr, items)
		if halfOpen && !called {
			pb.releaseProbe()
		}
		replayed += sent
		pc.PushReplayed.Add(int64(sent))
		if len(retry) > 0 {
			pc.countDrops(pc.pushBuf.requeue(addr, retry, time.Now().UnixNano()))
			outage = true
			failed()
			continue
		}
		backoff = replayBackoffMin
		batch = min(2*batch, replayMaxBatch)
		timer.Reset(replayPace)
	}
}

// sendReplay pushes items to addr, at most replayMaxInFlight at once, and
// returns how many landed, the ones to try again and whether any call was
// made.
func (pc *PeerCache) sendReplay(addr string, items []*bufferedPush) (sent int, retry []*bufferedPush, called bool) {
	outcomes := make([]pushOutcome, len(items))
	send := func(i int) {
		p := items[i]
		if !pc.replicaOf(p.key, addr) {
			outcomes[i] = pushNotReplica
			return
		}
		remaining := time.Until(time.Unix(0, p.expiresAt))
		if remaining < replayMinRemainingTTL {
			outcomes[i] = pushExpired
			return
		}
		outcomes[i] = pc.pushOnce(addr, p.key, p.value, remaining)
	}
	if len(items) == 1 {
		send(0)
	} else {
		var wg sync.WaitGroup
		sem := make(chan struct{}, replayMaxInFlight)
		for i := range items {
			sem <- struct{}{}
			wg.Add(1)
			go func(i int) {
				defer func() { <-sem; wg.Done() }()
				send(i)
			}(i)
		}
		wg.Wait()
	}
	var drops []pushDrop
	for i, o := range outcomes {
		switch o {
		case pushDelivered:
			sent++
		case pushRejected:
			drops = append(drops, pushDrop{addr, items[i], droppedRejected})
		case pushNotReplica:
			drops = append(drops, pushDrop{addr, items[i], droppedNotReplica})
		case pushExpired:
			drops = append(drops, pushDrop{addr, items[i], droppedExpired})
		default:
			retry = append(retry, items[i])
		}
		called = called || (o != pushNotReplica && o != pushExpired)
	}
	pc.countDrops(drops)
	return sent, retry, called
}

// replicaOf reports whether addr is still one of key's replicas.
func (pc *PeerCache) replicaOf(key, addr string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return len(pc.peers) > 0 && pc.ring.has(key, pc.replication, addr)
}

func (pc *PeerCache) dropBacklog(addr string, reason int) {
	if pc.pushBuf == nil {
		return
	}
	items := pc.pushBuf.removePeer(addr)
	drops := make([]pushDrop, len(items))
	for i, p := range items {
		drops[i] = pushDrop{addr, p, reason}
	}
	pc.countDrops(drops)
}

// releaseProbe hands back the half-open probe a caller took with peerAllowed
// but made no call with, so the next call probes the peer instead of the
// suspension lasting another cooldown. Outside half-open it does nothing.
func (pb *peerBreaker) releaseProbe() {
	if pb.suspendedUntil.Load() != 0 {
		pb.probeAt.Store(0)
	}
}

// jittered spreads d over [d/2, 3d/2).
func jittered(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}

// pushOutcome is what one push attempt came to.
type pushOutcome uint8

const (
	pushDelivered  pushOutcome = iota
	pushRetry                  // transport error, timeout, 429 or 5xx: worth sending again
	pushPeerNotUp              // the peer is still starting
	pushRejected               // the peer refused the value (a 4xx): never sent again
	pushNotReplica             // the peer is no longer a replica of the key
	pushExpired                // too little TTL left
)

func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// hasBacklog reports whether addr has a backlog, including pushes of it in
// flight (the backlog goes only once its replayer finished).
func (pc *PeerCache) hasBacklog(addr string) bool {
	if pc.pushBuf == nil {
		return false
	}
	pc.pushBuf.mu.Lock()
	defer pc.pushBuf.mu.Unlock()
	return pc.pushBuf.peers[addr] != nil
}
