package cache

import (
	"strings"
	"sync/atomic"
	"time"
)

// Layer identifies a semantic contract, independently of storage tier.
type Layer uint8

const (
	LayerUnknown Layer = iota
	LayerLoki
	LayerVL
	LayerCount
)

func (l Layer) String() string {
	switch l {
	case LayerLoki:
		return "loki"
	case LayerVL:
		return "vl"
	default:
		return "unknown"
	}
}

// LayerForKey deliberately recognizes only audited namespaces. Arbitrary keys
// stay unknown rather than turning query or tenant text into metric labels.
func LayerForKey(key string) Layer {
	if strings.HasPrefix(key, "query-steps:v1:") {
		return LayerLoki
	}
	if strings.HasPrefix(key, "stats-extent:v1:") {
		return LayerVL
	}
	prefix, _, ok := strings.Cut(key, ":")
	if !ok {
		return LayerUnknown
	}
	switch prefix {
	case "vlinv-v1", "sfn", "afn", "query_range_window_has_hits":
		return LayerVL
	case "query_range", "query_range_window", "query", "series", "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_labels", "detected_field_values", "detect_fields", "detect_labels", "patterns", "compat", "mt", "label_inventory":
		return LayerLoki
	case "__patterns_snapshot", "__label_values_index_snapshot":
		if key == prefix+":v1" {
			return LayerLoki
		}
	}
	return LayerUnknown
}

// LayerTierStatsSnapshot separates logical lookup traffic from retained charges.
// Admissions include accepted replacements and promotions. Evictions count
// capacity and expiry removals, excluding explicit invalidation and replacement.
// A staged replacement rejected under disk capacity pressure can also evict its
// original committed record: those are one rejection and one eviction.
// Hits and decoded read bytes describe values delivered by a storage tier; a
// caller's later usability/semantic check may still reject that candidate.
type LayerTierStatsSnapshot struct {
	Requests, Hits, Misses, StaleHits int64
	ReadBytes, WriteBytes             int64 // decoded value bytes, excluding keys
	Admissions, Rejections, Evictions int64
	Entries, Bytes                    int64 // L1 key+value; L2 committed key+record
	LookupNanoseconds                 int64 // elapsed logical L2/L3 lookup time; zero for L1
}

// LayerStatsSnapshot contains bounded semantic-layer statistics. The disk index
// and queue are accounting charges, not measured heap; encoded bytes represent
// bbolt record copies/commits, not device throughput, file size or page I/O.
type LayerStatsSnapshot struct {
	L1, L2, L3                                                                  LayerTierStatsSnapshot
	DiskIndexBytes, DiskQueueBytes, DiskEncodedReadBytes, DiskEncodedWriteBytes int64
}
type layerTierStats struct {
	requests, hits, misses, staleHits atomic.Int64
	readBytes, writeBytes             atomic.Int64
	admissions, rejections, evictions atomic.Int64
	entries, bytes                    atomic.Int64
	lookupNanoseconds                 atomic.Int64
}

func (s *layerTierStats) snapshot() LayerTierStatsSnapshot {
	return LayerTierStatsSnapshot{
		Requests: s.requests.Load(), Hits: s.hits.Load(), Misses: s.misses.Load(), StaleHits: s.staleHits.Load(),
		ReadBytes: s.readBytes.Load(), WriteBytes: s.writeBytes.Load(),
		Admissions: s.admissions.Load(), Rejections: s.rejections.Load(), Evictions: s.evictions.Load(),
		Entries: s.entries.Load(), Bytes: s.bytes.Load(), LookupNanoseconds: s.lookupNanoseconds.Load(),
	}
}
func (s *layerTierStats) lookup(value []byte, ok, stale bool) {
	s.requests.Add(1)
	if ok {
		s.hits.Add(1)
		s.readBytes.Add(int64(len(value)))
		if stale {
			s.staleHits.Add(1)
		}
	} else {
		s.misses.Add(1)
	}
}
func (c *Cache) layer(key string) Layer {
	if c.semanticLayer > LayerUnknown && c.semanticLayer < LayerCount {
		return c.semanticLayer
	}
	return LayerForKey(key)
}
func NewForLayer(ttl time.Duration, maxEntries int, layer Layer) *Cache {
	return NewWithMaxBytesForLayer(ttl, maxEntries, 256*1024*1024, layer)
}
func NewWithMaxBytesForLayer(ttl time.Duration, maxEntries, maxBytes int, layer Layer) *Cache {
	if layer >= LayerCount {
		layer = LayerUnknown
	}
	return newWithMaxBytesForLayer(ttl, maxEntries, maxBytes, layer)
}

// DiskCache returns the optional attached disk instance for callers that must
// deduplicate shared physical cache counters. Attachments are configured before
// serving requests, as with SetL2.
func (c *Cache) DiskCache() *DiskCache {
	if c == nil || c.disabled {
		return nil
	}
	return c.l2
}

// LayerStats returns bounded O(1) snapshots. Disk counters include queue hits;
// encoded record bytes are logical bbolt traffic, not physical device I/O.
func (c *Cache) LayerStats() (out [LayerCount]LayerStatsSnapshot) {
	if c == nil || c.disabled {
		return
	}
	c.mu.RLock()
	for l := LayerUnknown; l < LayerCount; l++ {
		out[l].L1 = c.layerL1[l].snapshot()
		out[l].L3 = c.layerL3[l].snapshot()
	}
	c.mu.RUnlock()
	if c.l2 != nil {
		disk := c.l2.LayerStats()
		for l := LayerUnknown; l < LayerCount; l++ {
			out[l].L2 = disk[l].L2
			out[l].DiskIndexBytes = disk[l].DiskIndexBytes
			out[l].DiskQueueBytes = disk[l].DiskQueueBytes
			out[l].DiskEncodedReadBytes = disk[l].DiskEncodedReadBytes
			out[l].DiskEncodedWriteBytes = disk[l].DiskEncodedWriteBytes
		}
	}
	return
}
func (dc *DiskCache) LayerStats() (out [LayerCount]LayerStatsSnapshot) {
	if dc == nil {
		return
	}
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	for l := LayerUnknown; l < LayerCount; l++ {
		out[l].L2 = dc.layerL2[l].snapshot()
		out[l].DiskIndexBytes = dc.layerIndexBytes[l]
		out[l].DiskQueueBytes = dc.layerQueueBytes[l]
		out[l].DiskEncodedReadBytes = dc.layerEncodedReadBytes[l].Load()
		out[l].DiskEncodedWriteBytes = dc.layerEncodedWriteBytes[l].Load()
	}
	return
}
func (c *Cache) getPeerReplica(key string) (value []byte, ttl time.Duration, memOnly, ok bool) {
	return c.getPeerReplicaFrom(c.l3, key)
}

// Read-ahead can attach a local cache without configuring its L3 fallback.
// Attribute the same logical peer lookup to that cache, using the caller's peer
// instance rather than assuming c.l3 is configured.
func (c *Cache) getPeerReplicaFrom(peer *PeerCache, key string) (value []byte, ttl time.Duration, memOnly, ok bool) {
	start := time.Now()
	value, ttl, memOnly, ok = peer.getReplica(key)
	s := &c.layerL3[c.layer(key)]
	s.lookup(value, ok, false)
	s.lookupNanoseconds.Add(time.Since(start).Nanoseconds())
	return
}
