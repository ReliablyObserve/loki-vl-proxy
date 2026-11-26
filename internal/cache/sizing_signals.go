package cache

import (
	"hash/maphash"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Sizing signals (#1767): would a larger cache or a longer TTL have answered
// more, how large is what users ask for, how long until a key is asked again,
// and how many requested bytes the cache answered. Off unless
// EnableSizingSignals is called; a cache without them pays one nil check per
// lookup and store.
//
// The ghost caches, the working set and the reuse intervals follow a spatial
// sample of the keys (SHARDS): a key is sampled when the low bits of its hash
// are zero, one key in sizingSampleRate, and every sampled access of a key is
// seen. The sampled events go through a lossy buffer to one goroutine, so a
// lookup never waits for it; under bursts events drop and the figures are a
// lower bound. Counts and sizes from the sample are scaled by the rate. The
// byte hit counters are exact.

// sizingSampleRate is one key in 16 (the hash's low four bits are zero).
const (
	sizingSampleShift = 4
	sizingSampleRate  = 1 << sizingSampleShift
	sizingSampleMask  = sizingSampleRate - 1
)

// sizingMaxKeys bounds the sampled keys the working set remembers (24h) and
// each ghost cache holds; past it the oldest are dropped first.
const sizingMaxKeys = 1 << 14

// sizingPendingMax bounds the keys between a full miss and the store that
// fills it (byte hit ratio); past it a miss is not matched to its fill.
const sizingPendingMax = 4096

// GhostScenario is a what-if cache a ghost simulates.
type GhostScenario uint8

const (
	// GhostDoubleBytes: the tier with twice its entry and byte budgets.
	GhostDoubleBytes GhostScenario = iota
	// GhostDoubleTTL: the tier with its budgets and every TTL doubled.
	GhostDoubleTTL
	GhostScenarioCount
)

var ghostScenarioNames = [GhostScenarioCount]string{"2x_bytes", "2x_ttl"}

func (g GhostScenario) String() string {
	if g < GhostScenarioCount {
		return ghostScenarioNames[g]
	}
	return "unknown"
}

// Sizing tiers: the ghosts simulate L1 and L2.
const (
	sizingTierL1 = iota
	sizingTierL2
	sizingTierCount
)

// SizingTiers are the tier label values of the ghost hits, in index order.
var SizingTiers = [sizingTierCount]string{"l1_memory", "l2_disk"}

// WorkingSetWindows are the windows the working set is estimated over.
var WorkingSetWindows = [...]time.Duration{5 * time.Minute, time.Hour, 24 * time.Hour}

// WorkingSetWindowNames are their label values.
var WorkingSetWindowNames = [len(WorkingSetWindows)]string{"5m", "1h", "24h"}

// ReuseIntervalBounds are the reuse interval histogram's upper bounds.
var ReuseIntervalBounds = EvictionAgeBounds

// EndpointClass is the answer class of a key, from its namespace.
type EndpointClass uint8

const (
	ClassQueryRange EndpointClass = iota
	ClassQuery
	ClassSeries
	ClassLabels
	ClassVolume
	ClassDetected
	ClassPatterns
	ClassOther
	EndpointClassCount
)

var endpointClassNames = [EndpointClassCount]string{"query_range", "query", "series", "labels", "volume", "detected", "patterns", "other"}

func (e EndpointClass) String() string {
	if e < EndpointClassCount {
		return endpointClassNames[e]
	}
	return "unknown"
}

// EndpointClassForKey maps a cache key to its answer class from its
// namespace, never from its text after the prefix.
func EndpointClassForKey(key string) EndpointClass {
	if strings.HasPrefix(key, "query-steps:") {
		return ClassQueryRange
	}
	prefix, _, _ := strings.Cut(key, ":")
	switch prefix {
	case "query_range", "query_range_window", "query_range_window_has_hits":
		return ClassQueryRange
	case "query":
		return ClassQuery
	case "series":
		return ClassSeries
	case "labels", "label_values", "label_inventory", "vlinv-v1", "sfn", "afn":
		return ClassLabels
	case "volume", "volume_range", "index_stats":
		return ClassVolume
	case "detected_fields", "detected_labels", "detected_field_values", "detect_fields", "detect_labels":
		return ClassDetected
	case "patterns", "patterns_window":
		return ClassPatterns
	}
	return ClassOther
}

// Lookup tiers for the byte hit counters.
const (
	hitTierL1 = iota
	hitTierL2
	hitTierL3
	hitTierCount
)

// HitTiers are the tier label values of the hit bytes, in index order.
var HitTiers = [hitTierCount]string{"l1_memory", "l2_disk", "l3_peer"}

type sizingEvent struct {
	hash  uint64
	at    int64 // unix nanoseconds
	ttl   int64 // nanoseconds, stores
	size  int32
	tier  uint8 // sizingTierL1 or sizingTierL2
	store bool  // a store, else a lookup
	hit   bool  // lookups: the tier answered
	layer Layer
}

// sizingSignals are a cache's sizing signals.
type sizingSignals struct {
	seed maphash.Seed

	hitBytes       [EndpointClassCount][hitTierCount]atomic.Int64
	requestedBytes [EndpointClassCount]atomic.Int64

	pendingMu sync.Mutex
	pending   map[string]EndpointClass

	events  chan sizingEvent
	flushCh chan chan struct{}
	done    chan struct{}
	stopped chan struct{}

	// Published by the goroutine, read by scrapes.
	ghostHits      [sizingTierCount][GhostScenarioCount]atomic.Int64
	workingKeys    [len(WorkingSetWindows)]atomic.Int64
	workingBytes   [len(WorkingSetWindows)]atomic.Int64
	reuse          [LayerCount]atomicHistogram
	dropped        [sizingTierCount]atomic.Int64
	sampledLookups atomic.Int64

	// Owned by the goroutine.
	ghosts [sizingTierCount][GhostScenarioCount]*ghostLRU
	seen   map[uint64]seenKey
}

type seenKey struct {
	last int64
	size int32
}

// EnableSizingSignals starts the sizing signals; the ghosts take the memory
// tier's budgets and, when one is attached (SetL2 first), the disk tier's.
// Call it once, before the cache serves traffic; Close stops it.
func (c *Cache) EnableSizingSignals() {
	if c == nil || c.disabled || c.sizing != nil {
		return
	}
	s := &sizingSignals{
		seed:    maphash.MakeSeed(),
		pending: make(map[string]EndpointClass),
		events:  make(chan sizingEvent, 1024),
		flushCh: make(chan chan struct{}),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
		seen:    make(map[uint64]seenKey),
	}
	budgets := [sizingTierCount][2]int64{{int64(c.maxEntries), int64(c.maxBytes)}}
	if c.l2 != nil {
		budgets[sizingTierL2] = [2]int64{int64(c.l2.maxEntries), c.l2.maxBytes}
	}
	for t, b := range budgets {
		if b[0] <= 0 {
			continue
		}
		s.ghosts[t][GhostDoubleBytes] = newGhostLRU(2*b[0], 2*b[1], 1)
		s.ghosts[t][GhostDoubleTTL] = newGhostLRU(b[0], b[1], 2)
	}
	go s.run()
	if c.l2 != nil {
		c.l2.sizing = s
	}
	c.sizing = s // before the cache is shared: read without a lock
}

func (s *sizingSignals) stop() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	<-s.stopped
}

// sampled reports whether key is in the sample, and its hash.
func (s *sizingSignals) sampled(key string) (uint64, bool) {
	h := maphash.String(s.seed, key)
	return h, h&sizingSampleMask == 0
}

func (s *sizingSignals) send(e sizingEvent) {
	select {
	case s.events <- e:
	default:
		s.dropped[e.tier].Add(1)
	}
}

// lookup records a request-level lookup: tier is the tier that answered
// ("" on a full miss) and size the answer's bytes.
func (s *sizingSignals) lookup(key, tier string, size int) {
	class := EndpointClassForKey(key)
	hitL1 := false
	switch tier {
	case "l1_memory":
		hitL1 = true
		s.hitBytes[class][hitTierL1].Add(int64(size))
		s.requestedBytes[class].Add(int64(size))
	case "l2_disk":
		s.hitBytes[class][hitTierL2].Add(int64(size))
		s.requestedBytes[class].Add(int64(size))
	case "l3_peer":
		s.hitBytes[class][hitTierL3].Add(int64(size))
		s.requestedBytes[class].Add(int64(size))
	default:
		s.pendingMu.Lock()
		if len(s.pending) < sizingPendingMax {
			s.pending[key] = class
		}
		s.pendingMu.Unlock()
	}
	if h, ok := s.sampled(key); ok {
		s.sampledLookups.Add(1)
		s.send(sizingEvent{hash: h, at: clockNow().UnixNano(), size: int32(min(size, 1<<31-1)), tier: sizingTierL1, hit: hitL1, layer: LayerForKey(key)})
	}
}

// storeL1 records a value of valueBytes (size with its key) stored in memory:
// the fill of a pending miss, and for a sampled key the ghosts' store.
func (s *sizingSignals) storeL1(key string, valueBytes, size int, ttl time.Duration) {
	s.pendingMu.Lock()
	class, filled := s.pending[key]
	if filled {
		delete(s.pending, key)
	}
	s.pendingMu.Unlock()
	if filled {
		s.requestedBytes[class].Add(int64(valueBytes))
	}
	if h, ok := s.sampled(key); ok {
		s.send(sizingEvent{hash: h, at: clockNow().UnixNano(), ttl: int64(ttl), size: int32(min(size, 1<<31-1)), tier: sizingTierL1, store: true, layer: LayerForKey(key)})
	}
}

// lookupL2 records an L2 lookup (L1 missed) for a sampled key.
func (s *sizingSignals) lookupL2(key string, hit bool) {
	if h, ok := s.sampled(key); ok {
		s.send(sizingEvent{hash: h, at: clockNow().UnixNano(), tier: sizingTierL2, hit: hit, layer: LayerForKey(key)})
	}
}

// storeL2 records a value the L2 write queue admitted for a sampled key.
func (s *sizingSignals) storeL2(key string, size int, ttl time.Duration) {
	if h, ok := s.sampled(key); ok {
		s.send(sizingEvent{hash: h, at: clockNow().UnixNano(), ttl: int64(ttl), size: int32(min(size, 1<<31-1)), tier: sizingTierL2, store: true, layer: LayerForKey(key)})
	}
}

// flush waits until every event sent before it is applied and the working
// set is republished (tests).
func (s *sizingSignals) flush() {
	done := make(chan struct{})
	s.flushCh <- done
	<-done
}

func (s *sizingSignals) run() {
	defer close(s.stopped)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case e := <-s.events:
			s.apply(e)
		case <-ticker.C:
			s.publishWorkingSet()
		case done := <-s.flushCh:
			for drained := false; !drained; {
				select {
				case e := <-s.events:
					s.apply(e)
				default:
					drained = true
				}
			}
			s.publishWorkingSet()
			close(done)
		}
	}
}

func (s *sizingSignals) apply(e sizingEvent) {
	for g, ghost := range s.ghosts[e.tier] {
		if ghost == nil {
			continue
		}
		if e.store {
			ghost.store(e.hash, int64(e.size), e.ttl, e.at)
			continue
		}
		if ghost.lookup(e.hash, e.at) && !e.hit {
			s.ghostHits[e.tier][g].Add(sizingSampleRate)
		}
	}
	if e.tier != sizingTierL1 {
		return
	}
	prev, ok := s.seen[e.hash]
	if ok && !e.store {
		s.reuse[e.layer].observe(bucketOf(ReuseIntervalBounds[:], time.Duration(e.at-prev.last)), e.at-prev.last)
	}
	next := seenKey{last: e.at, size: prev.size}
	if e.size > 0 {
		next.size = e.size
	}
	if !ok && len(s.seen) >= sizingMaxKeys {
		s.pruneSeen(e.at)
		if len(s.seen) >= sizingMaxKeys {
			return
		}
	}
	s.seen[e.hash] = next
}

// pruneSeen drops keys last seen beyond the largest window, then, while still
// full, the oldest eighth.
func (s *sizingSignals) pruneSeen(now int64) {
	horizon := now - int64(WorkingSetWindows[len(WorkingSetWindows)-1])
	for h, k := range s.seen {
		if k.last < horizon {
			delete(s.seen, h)
		}
	}
	if len(s.seen) < sizingMaxKeys {
		return
	}
	oldest := now
	for _, k := range s.seen {
		oldest = min(oldest, k.last)
	}
	cut := oldest + (now-oldest)/8
	for h, k := range s.seen {
		if k.last <= cut {
			delete(s.seen, h)
		}
	}
}

func (s *sizingSignals) publishWorkingSet() {
	now := clockNow().UnixNano()
	var keys, bytes [len(WorkingSetWindows)]int64
	horizon := now - int64(WorkingSetWindows[len(WorkingSetWindows)-1])
	for h, k := range s.seen {
		if k.last < horizon {
			delete(s.seen, h)
			continue
		}
		for w, d := range WorkingSetWindows {
			if k.last >= now-int64(d) {
				keys[w]++
				bytes[w] += int64(k.size)
			}
		}
	}
	for w := range WorkingSetWindows {
		s.workingKeys[w].Store(keys[w] * sizingSampleRate)
		s.workingBytes[w].Store(bytes[w] * sizingSampleRate)
	}
}

// ghostLRU is a key-only LRU of sampled keys with sizes and expiries, its
// budgets scaled to the sample.
type ghostLRU struct {
	maxEntries, maxBytes int64 // 0 bytes: no byte budget
	ttlFactor            int64
	bytes                int64
	index                map[uint64]*ghostNode
	newest, oldest       *ghostNode
}

type ghostNode struct {
	hash       uint64
	size       int64
	expires    int64
	prev, next *ghostNode
}

func newGhostLRU(entries, bytes, ttlFactor int64) *ghostLRU {
	g := &ghostLRU{
		maxEntries: min(max(entries/sizingSampleRate, 1), sizingMaxKeys),
		maxBytes:   bytes / sizingSampleRate,
		ttlFactor:  ttlFactor,
		index:      make(map[uint64]*ghostNode),
	}
	if bytes > 0 && g.maxBytes == 0 {
		g.maxBytes = 1
	}
	return g
}

// lookup reports whether the ghost holds an unexpired key, and makes it the
// newest.
func (g *ghostLRU) lookup(hash uint64, now int64) bool {
	n := g.index[hash]
	if n == nil {
		return false
	}
	if n.expires <= now {
		g.remove(n)
		return false
	}
	g.unlink(n)
	g.pushNewest(n)
	return true
}

func (g *ghostLRU) store(hash uint64, size, ttl, now int64) {
	if n := g.index[hash]; n != nil {
		g.remove(n)
	}
	if g.maxBytes > 0 && size > g.maxBytes {
		return
	}
	n := &ghostNode{hash: hash, size: size, expires: now + ttl*g.ttlFactor}
	g.index[hash] = n
	g.bytes += size
	g.pushNewest(n)
	for int64(len(g.index)) > g.maxEntries || (g.maxBytes > 0 && g.bytes > g.maxBytes) {
		g.remove(g.oldest)
	}
}

func (g *ghostLRU) remove(n *ghostNode) {
	g.unlink(n)
	delete(g.index, n.hash)
	g.bytes -= n.size
}

func (g *ghostLRU) unlink(n *ghostNode) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		g.newest = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		g.oldest = n.prev
	}
	n.prev, n.next = nil, nil
}

func (g *ghostLRU) pushNewest(n *ghostNode) {
	n.next = g.newest
	if g.newest != nil {
		g.newest.prev = n
	}
	g.newest = n
	if g.oldest == nil {
		g.oldest = n
	}
}

// SizingSnapshot is the sizing signals' state.
type SizingSnapshot struct {
	Enabled bool
	// GhostHits are the estimated lookups each tier missed that its scenario
	// would have answered, by sizing tier and scenario.
	GhostHits [sizingTierCount][GhostScenarioCount]int64
	// WorkingSetKeys and WorkingSetBytes estimate the distinct keys looked up
	// or stored in memory, and their answer bytes, per window.
	WorkingSetKeys, WorkingSetBytes [len(WorkingSetWindows)]int64
	// ReuseInterval is the time between two lookups of a key, by layer.
	ReuseInterval [LayerCount]HistogramSnapshot
	// HitBytes are the answer bytes each tier returned to request-level
	// lookups, by class; RequestedBytes add the bytes stored after a full miss.
	HitBytes       [EndpointClassCount][hitTierCount]int64
	RequestedBytes [EndpointClassCount]int64
	// Dropped are sampled events the buffer had no room for, by sizing tier.
	Dropped [sizingTierCount]int64
}

// SizingStats returns the sizing signals without the cache's locks.
func (c *Cache) SizingStats() (out SizingSnapshot) {
	if c == nil || c.disabled {
		return
	}
	s := c.sizing
	if s == nil {
		return
	}
	out.Enabled = true
	for t := range out.GhostHits {
		for g := range out.GhostHits[t] {
			out.GhostHits[t][g] = s.ghostHits[t][g].Load()
		}
	}
	for w := range WorkingSetWindows {
		out.WorkingSetKeys[w] = s.workingKeys[w].Load()
		out.WorkingSetBytes[w] = s.workingBytes[w].Load()
	}
	for l := range out.ReuseInterval {
		out.ReuseInterval[l] = s.reuse[l].snapshot(len(ReuseIntervalBounds) + 1)
	}
	for k := range out.HitBytes {
		for t := range out.HitBytes[k] {
			out.HitBytes[k][t] = s.hitBytes[k][t].Load()
		}
		out.RequestedBytes[k] = s.requestedBytes[k].Load()
	}
	for t := range out.Dropped {
		out.Dropped[t] = s.dropped[t].Load()
	}
	return
}

// sizingLookup records a request-level lookup when the sizing signals are on.
func (c *Cache) sizingLookup(key, tier string, v []byte) {
	if s := c.sizing; s != nil {
		s.lookup(key, tier, len(v))
	}
}
