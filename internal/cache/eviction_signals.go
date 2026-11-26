package cache

import (
	"sync/atomic"
	"time"
)

// EvictionReason says why an entry left a tier. The set is fixed, so it can be
// a metric label without query, tenant or key text.
type EvictionReason uint8

const (
	// EvictionExpired: the entry's TTL ran out (cleanup, the expiry sweep before
	// a capacity eviction, an L2 expiry delete or the startup scan).
	EvictionExpired EvictionReason = iota
	// EvictionCapacity: the entry was the least recently used one when a new
	// entry needed room (entry count, byte budget, non-owner shadow budget, or the
	// L2 index/byte budget, including the startup scan).
	EvictionCapacity
	// EvictionReplaced: a newer value for the same key took its place.
	EvictionReplaced
	// EvictionInvalidated: L1, an explicit Invalidate or InvalidatePrefix; L2, a
	// record whose newer write was refused or that could not be read back.
	EvictionInvalidated
	// EvictionFlush: a purge of the tier (PurgeAll, DiskCache.Purge).
	EvictionFlush
	EvictionReasonCount
)

var evictionReasonNames = [EvictionReasonCount]string{"expired", "capacity", "replaced", "invalidated", "flush"}

func (r EvictionReason) String() string {
	if r < EvictionReasonCount {
		return evictionReasonNames[r]
	}
	return "unknown"
}

// RejectionReason says why a value was not stored in a tier.
type RejectionReason uint8

const (
	// RejectionSingleEntryBudget: an L1 value above a tenth of the memory budget.
	RejectionSingleEntryBudget RejectionReason = iota
	// RejectionTooLarge: an L2 value above the disk's entry limit or budget.
	RejectionTooLarge
	// RejectionQueueFull: the L2 write queue had no room.
	RejectionQueueFull
	// RejectionTTLBelowMinimum: an L2 write with a TTL under -disk-cache-min-ttl
	// (or no TTL at all).
	RejectionTTLBelowMinimum
	// RejectionSourceExpired: the value's source deadline passed before it was
	// stored (a peer or disk copy, or a queued L2 write that expired before its
	// flush).
	RejectionSourceExpired
	// RejectionShadowBudget: no room in the L1 non-owner shadow budget.
	RejectionShadowBudget
	// RejectionCapacity: a queued L2 write lost to entries used after it was
	// queued when the batch did not fit.
	RejectionCapacity
	// RejectionInvalidKey: an L2 key that is empty or longer than bbolt allows.
	RejectionInvalidKey
	// RejectionEncodeFailed: the L2 record could not be encoded.
	RejectionEncodeFailed
	// RejectionClosed: an L2 write after the disk cache started closing.
	RejectionClosed
	// RejectionMemoryOnly and RejectionNotReplica are placement skips, not refused
	// writes, and are not part of the layer rejections counter: a memory-only
	// answer (SetLocalOnlyWithTTL, a peer's memory-only copy, a patterns entry)
	// and a non-owner shadow copy are kept off L2 by design.
	RejectionMemoryOnly
	RejectionNotReplica
	RejectionReasonCount
)

var rejectionReasonNames = [RejectionReasonCount]string{"single_entry_budget", "too_large", "queue_full", "ttl_below_minimum", "source_expired", "shadow_budget", "capacity", "invalid_key", "encode_failed", "closed", "memory_only", "not_replica"}

func (r RejectionReason) String() string {
	if r < RejectionReasonCount {
		return rejectionReasonNames[r]
	}
	return "unknown"
}

// PlacementSkip reports a reason the layer rejections counter leaves out.
func (r RejectionReason) PlacementSkip() bool {
	return r == RejectionMemoryOnly || r == RejectionNotReplica
}

// Promotion is one copy between tiers on a lower-tier hit.
type Promotion uint8

const (
	// PromotionDiskToMemory: an L2 hit copied into L1.
	PromotionDiskToMemory Promotion = iota
	// PromotionPeerToMemory: an L3 peer hit copied into L1.
	PromotionPeerToMemory
	// PromotionPeerToDisk: an L3 peer hit handed to the local L2 write queue.
	PromotionPeerToDisk
	PromotionCount
)

var promotionTiers = [PromotionCount][2]string{{"l2_disk", "l1_memory"}, {"l3_peer", "l1_memory"}, {"l3_peer", "l2_disk"}}

// Tiers returns the from and to tier names.
func (p Promotion) Tiers() (from, to string) {
	if p < PromotionCount {
		return promotionTiers[p][0], promotionTiers[p][1]
	}
	return "unknown", "unknown"
}

// Only expiry and capacity evictions are observed by the age and hit
// histograms: they answer whether the cache is too small or the TTL too short.
const evictionObservedCount = 2

func observedEviction(r EvictionReason) bool { return r == EvictionExpired || r == EvictionCapacity }

// ObservedEvictionReasons are the reasons the age and hit histograms record, in
// index order.
var ObservedEvictionReasons = [evictionObservedCount]EvictionReason{EvictionExpired, EvictionCapacity}

// Histogram bounds. Age is the time since the value was admitted to the tier on
// this replica; hits are the lookups that delivered it from the tier.
var (
	EvictionAgeBounds  = [...]time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, 6 * time.Hour, 24 * time.Hour}
	EvictionHitsBounds = [...]int64{0, 1, 2, 3, 5, 10, 25, 100}
	// DiskReadSecondsBounds and DiskReadBytesBounds cover an L2 lookup that read a
	// record from bbolt: tens of microseconds for a page-cache hit to tens of
	// milliseconds for a cold device read of a large record.
	DiskReadSecondsBounds = [...]time.Duration{50 * time.Microsecond, 100 * time.Microsecond, 250 * time.Microsecond, 500 * time.Microsecond, time.Millisecond, 2500 * time.Microsecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}
	DiskReadBytesBounds   = [...]int64{256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20}
)

// maxHistogramBuckets holds the widest bound set above plus its overflow bucket.
const maxHistogramBuckets = len(DiskReadSecondsBounds) + 1

// HistogramSnapshot is a fixed-bucket histogram: Counts()[i] holds the
// observations at or below bound i and above the previous bound (not
// cumulative), the last element the observations above every bound. Sum is in
// the bound's unit: nanoseconds for durations, units for counts. A value, so a
// snapshot allocates nothing.
type HistogramSnapshot struct {
	counts  [maxHistogramBuckets]int64
	buckets int
	Sum     int64
	Count   int64
}

// Counts returns the per-bucket counts (empty for a zero snapshot).
func (h *HistogramSnapshot) Counts() []int64 { return h.counts[:h.buckets] }

// NewHistogramSnapshot builds a snapshot from per-bucket counts (at most
// one more than the widest bound set) and a sum, as an exporter test needs.
func NewHistogramSnapshot(counts []int64, sum int64) HistogramSnapshot {
	h := HistogramSnapshot{buckets: min(len(counts), maxHistogramBuckets), Sum: sum}
	for i := 0; i < h.buckets; i++ {
		h.counts[i] = counts[i]
		h.Count += counts[i]
	}
	return h
}

// atomicHistogram records without a lock: one bucket add and one sum add; the
// count is the buckets' sum.
type atomicHistogram struct {
	counts [maxHistogramBuckets]atomic.Int64
	sum    atomic.Int64
}

func bucketOf[N ~int64](bounds []N, v N) int {
	i := 0
	for i < len(bounds) && v > bounds[i] {
		i++
	}
	return i
}

func (h *atomicHistogram) observe(bucket int, v int64) {
	h.counts[bucket].Add(1)
	h.sum.Add(v)
}

func (h *atomicHistogram) snapshot(buckets int) HistogramSnapshot {
	out := HistogramSnapshot{buckets: buckets, Sum: h.sum.Load()}
	for i := 0; i < buckets; i++ {
		out.counts[i] = h.counts[i].Load()
		out.Count += out.counts[i]
	}
	return out
}

// tierSignals holds one tier's reasons and eviction observations for every
// semantic layer. All fields are atomics: they are read on scrape without the
// cache's mutex.
type tierSignals struct {
	evictions  [LayerCount][EvictionReasonCount]atomic.Int64
	rejections [LayerCount][RejectionReasonCount]atomic.Int64
	age        [LayerCount][evictionObservedCount]atomicHistogram
	hits       [LayerCount][evictionObservedCount]atomicHistogram
}

func (s *tierSignals) reject(layer Layer, reason RejectionReason) {
	s.rejections[layer][reason].Add(1)
}

// evict counts one removal; age < 0 means the admission time is unknown (an L2
// record found by the startup scan), so the histograms skip it.
func (s *tierSignals) evict(layer Layer, reason EvictionReason, age time.Duration, hits uint32) {
	s.evictions[layer][reason].Add(1)
	if observedEviction(reason) && age >= 0 {
		s.age[layer][reason].observe(bucketOf(EvictionAgeBounds[:], age), int64(age))
		s.hits[layer][reason].observe(bucketOf(EvictionHitsBounds[:], int64(hits)), int64(hits))
	}
}

// TierSignalsSnapshot is one tier's reasons and eviction histograms for one
// semantic layer. AgeAtEviction and HitsAtEviction are indexed like
// ObservedEvictionReasons.
type TierSignalsSnapshot struct {
	Evictions      [EvictionReasonCount]int64
	Rejections     [RejectionReasonCount]int64
	AgeAtEviction  [evictionObservedCount]HistogramSnapshot
	HitsAtEviction [evictionObservedCount]HistogramSnapshot
}

func (s *tierSignals) snapshot(layer Layer) (out TierSignalsSnapshot) {
	for r := range out.Evictions {
		out.Evictions[r] = s.evictions[layer][r].Load()
	}
	for r := range out.Rejections {
		out.Rejections[r] = s.rejections[layer][r].Load()
	}
	for r := range out.AgeAtEviction {
		out.AgeAtEviction[r] = s.age[layer][r].snapshot(len(EvictionAgeBounds) + 1)
		out.HitsAtEviction[r] = s.hits[layer][r].snapshot(len(EvictionHitsBounds) + 1)
	}
	return
}

// LayerSignalsSnapshot is why entries left or never entered L1 and L2, the
// copies between tiers, and the L2 record reads, for one semantic layer.
type LayerSignalsSnapshot struct {
	L1, L2     TierSignalsSnapshot
	Promotions [PromotionCount]int64
	// DiskReadSeconds and DiskReadBytes observe each L2 lookup that read a record
	// from bbolt (the index lock, the read and the decode) and the record's
	// encoded size; L2 hits served from the pending write queue are not reads.
	DiskReadSeconds, DiskReadBytes HistogramSnapshot
}

// diskReadSignals are an L2 store's record-read histograms.
type diskReadSignals struct {
	seconds [LayerCount]atomicHistogram
	bytes   [LayerCount]atomicHistogram
}

func (s *diskReadSignals) observe(layer Layer, elapsed time.Duration, bytes int) {
	s.seconds[layer].observe(bucketOf(DiskReadSecondsBounds[:], elapsed), int64(elapsed))
	s.bytes[layer].observe(bucketOf(DiskReadBytesBounds[:], int64(bytes)), int64(bytes))
}

// SignalStats returns the eviction and rejection reasons, eviction histograms,
// promotions and L2 record reads by semantic layer. It reads atomics only and
// takes no cache lock.
func (c *Cache) SignalStats() (out [LayerCount]LayerSignalsSnapshot) {
	if c == nil || c.disabled {
		return
	}
	for l := LayerUnknown; l < LayerCount; l++ {
		out[l].L1 = c.signalsL1.snapshot(l)
		for p := range out[l].Promotions {
			out[l].Promotions[p] = c.promotions[l][p].Load()
		}
	}
	if c.l2 != nil {
		disk := c.l2.SignalStats()
		for l := LayerUnknown; l < LayerCount; l++ {
			out[l].L2 = disk[l].L2
			out[l].DiskReadSeconds = disk[l].DiskReadSeconds
			out[l].DiskReadBytes = disk[l].DiskReadBytes
		}
	}
	return
}

// SignalStats returns the L2 part of Cache.SignalStats, without a lock.
func (dc *DiskCache) SignalStats() (out [LayerCount]LayerSignalsSnapshot) {
	if dc == nil {
		return
	}
	for l := LayerUnknown; l < LayerCount; l++ {
		out[l].L2 = dc.signals.snapshot(l)
		out[l].DiskReadSeconds = dc.reads.seconds[l].snapshot(len(DiskReadSecondsBounds) + 1)
		out[l].DiskReadBytes = dc.reads.bytes[l].snapshot(len(DiskReadBytesBounds) + 1)
	}
	return
}

// age is the time since the node's value entered L1, to the second.
func (data *lruEntry) age(now time.Time) time.Duration {
	return time.Duration(uint32(now.Unix())-data.admitted) * time.Second
}

// countEvictionLocked counts why an L1 entry of layer leaves; data is its LRU
// node, nil when unknown. Called with c.mu held, before removeEntryLocked.
func (c *Cache) countEvictionLocked(layer Layer, reason EvictionReason, data *lruEntry, now time.Time) {
	if data == nil || !observedEviction(reason) {
		c.signalsL1.evictions[layer][reason].Add(1)
		return
	}
	c.signalsL1.evict(layer, reason, data.age(now), data.hits)
}

// lruNodeLocked is key's LRU node, nil when it has none.
func (c *Cache) lruNodeLocked(key string) *lruEntry {
	if elem, ok := c.lruIndex[key]; ok {
		return elem.Value.(*lruEntry)
	}
	return nil
}

// skipDisk counts a write kept off an attached L2 by placement. It is
// attributed like every other L2 counter, by the key's namespace.
func (c *Cache) skipDisk(key string, reason RejectionReason) {
	if c.l2 != nil {
		c.l2.signals.reject(LayerForKey(key), reason)
	}
}
