package cache

import (
	"container/list"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type entry struct {
	value     []byte
	expiresAt time.Time
	sizeBytes int
}

type tierStats struct {
	requests  atomic.Int64
	hits      atomic.Int64
	misses    atomic.Int64
	staleHits atomic.Int64
}

type TierStatsSnapshot struct {
	Requests  int64
	Hits      int64
	Misses    int64
	StaleHits int64
}

type StatsSnapshot struct {
	L1                TierStatsSnapshot
	L2                TierStatsSnapshot
	L3                TierStatsSnapshot
	BackendFallthrough int64
	Entries           int
	Bytes             int
	DiskEntries       int
	DiskBytes         int64
}

// Cache is an in-memory TTL cache with per-key TTL override, max entries, and max bytes.
// Supports optional L2 disk cache and L3 peer cache for distributed fleet operation.
// Lookup order: L1 (in-memory) → L2 (disk) → L3 (peer) → VL backend.
type Cache struct {
	mu         sync.RWMutex
	entries    map[string]entry
	defaultTTL time.Duration
	maxEntries int
	maxBytes   int
	curBytes   int
	disabled   bool
	l2         *DiskCache    // optional L2 disk cache
	l3         *PeerCache    // optional L3 peer fleet cache
	done       chan struct{} // signals cleanup goroutine to stop

	// LRU ordering: front = most recently used, back = least recently used
	lruList  *list.List               // doubly-linked list of *lruEntry
	lruIndex map[string]*list.Element // key → list element for O(1) lookup
	hot      map[string]hotStat       // lightweight per-key hotness index for peer read-ahead
	hotMax   int                      // cap hot index growth
	hotOn    bool                     // only enabled when peer hot read-ahead is enabled

	// Stats
	Hits      atomic.Int64
	Misses    atomic.Int64
	Evictions atomic.Int64

	l1Stats            tierStats
	l2Stats            tierStats
	l3Stats            tierStats
	backendFallthrough atomic.Int64
}

// MaxEntrySizeBytes returns the largest object this cache will retain.
// Entries larger than this threshold are rejected to protect the cache from
// a single oversized response consuming a disproportionate share of memory.
func (c *Cache) MaxEntrySizeBytes() int {
	if c == nil || c.disabled || c.maxBytes <= 0 {
		return 0
	}
	return c.maxBytes / 10
}

type lruEntry struct {
	key string
}

type hotStat struct {
	score      uint64
	lastAccess int64
	tenant     string
}

// HotKeySnapshot captures a hot key candidate for bounded peer read-ahead.
type HotKeySnapshot struct {
	Key            string
	Tenant         string
	Score          uint64
	SizeBytes      int
	RemainingTTLMS int64
}

const nonOwnerShadowMaxTTL = 30 * time.Second
const defaultHotIndexMaxEntries = 50000
const maxHotIndexQueryLimit = 2000

// SetL2 attaches an L2 disk cache. On L1 miss, L2 is checked.
// On L1 set, values are also written to L2.
func (c *Cache) SetL2(dc *DiskCache) {
	if c == nil || c.disabled {
		return
	}
	c.l2 = dc
}

// SetL3 attaches an L3 peer cache for distributed fleet operation.
// On L1+L2 miss, the owning peer is queried before hitting VL.
func (c *Cache) SetL3(pc *PeerCache) {
	if c == nil || c.disabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.l3 = pc
	c.hotOn = pc != nil && pc.readAheadEnabled
	if !c.hotOn {
		c.hot = make(map[string]hotStat)
	}
	if pc != nil {
		pc.AttachLocalCache(c)
	}
}

func NewDisabled() *Cache {
	c := &Cache{disabled: true, done: make(chan struct{})}
	close(c.done)
	return c
}

func New(ttl time.Duration, maxEntries int) *Cache {
	return NewWithMaxBytes(ttl, maxEntries, 256*1024*1024) // 256MB default
}

func NewWithMaxBytes(ttl time.Duration, maxEntries, maxBytes int) *Cache {
	c := &Cache{
		entries:    make(map[string]entry),
		defaultTTL: ttl,
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		done:       make(chan struct{}),
		lruList:    list.New(),
		lruIndex:   make(map[string]*list.Element),
		hot:        make(map[string]hotStat),
		hotMax:     defaultHotIndexMaxEntries,
	}
	go c.cleanup()
	return c
}

// Close stops the background cleanup goroutine.
func (c *Cache) Close() {
	if c == nil || c.disabled {
		return
	}
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

// GetWithTTL returns the value and remaining TTL for a key.
// Returns (nil, 0, false) on miss.
func (c *Cache) GetWithTTL(key string) ([]byte, time.Duration, bool) {
	if c == nil || c.disabled {
		return nil, 0, false
	}
	c.mu.Lock()
	e, ok := c.entries[key]
	if ok {
		remaining := time.Until(e.expiresAt)
		if remaining > 0 {
			if elem, found := c.lruIndex[key]; found {
				c.lruList.MoveToFront(elem)
			}
			if c.hotOn {
				c.recordHotLocked(key)
			}
			c.mu.Unlock()
			c.Hits.Add(1)
			return e.value, remaining, true
		}
	}
	c.mu.Unlock()
	return nil, 0, false
}

// GetSharedWithTTL returns the value and remaining TTL across the full fresh-cache stack.
// Lookup order is L1 memory -> L2 disk -> L3 peer. Hits from lower tiers are promoted
// into local L1 only so read reuse improves without forcing extra disk or peer writes.
func (c *Cache) GetSharedWithTTL(key string) ([]byte, time.Duration, string, bool) {
	if c == nil || c.disabled {
		return nil, 0, "", false
	}
	if v, ttl, ok := c.getL1WithTTL(key); ok {
		c.recordTierRequest("l1")
		c.recordTierHit("l1")
		c.Hits.Add(1)
		return v, ttl, "l1_memory", true
	}
	c.recordTierRequest("l1")
	c.recordTierMiss("l1")

	if c.l2 != nil {
		c.recordTierRequest("l2")
		if v, ttl, ok := c.l2.GetWithTTL(key); ok {
			c.SetLocalOnlyWithTTL(key, v, ttl)
			c.recordTierHit("l2")
			c.Hits.Add(1)
			return v, ttl, "l2_disk", true
		}
		c.recordTierMiss("l2")
	}

	if c.l3 != nil {
		c.recordTierRequest("l3")
		if v, remainingTTL, ok := c.l3.Get(key); ok {
			shadowTTL := remainingTTL
			if shadowTTL <= 0 {
				shadowTTL = 30 * time.Second
			}
			c.SetLocalOnlyWithTTL(key, v, shadowTTL)
			c.recordTierHit("l3")
			c.Hits.Add(1)
			return v, shadowTTL, "l3_peer", true
		}
		c.recordTierMiss("l3")
	}

	c.backendFallthrough.Add(1)
	c.Misses.Add(1)
	return nil, 0, "", false
}

// GetStaleWithTTL returns a locally retained value even if its TTL has expired.
// The returned TTL may be negative when the entry is stale. This never falls
// back to L2/L3 because stale-on-error is only intended to reuse the serving
// replica's last known-good payload.
func (c *Cache) GetStaleWithTTL(key string) ([]byte, time.Duration, bool) {
	if c == nil || c.disabled {
		return nil, 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[key]
	if !ok {
		return nil, 0, false
	}
	return e.value, time.Until(e.expiresAt), true
}

// GetRecoverableStaleWithTTL returns the last known-good value from local memory or local disk.
// It never reads peers for stale data, which keeps degraded-path fallback local-first.
func (c *Cache) GetRecoverableStaleWithTTL(key string) ([]byte, time.Duration, string, bool) {
	if c == nil || c.disabled {
		return nil, 0, "", false
	}
	if v, ttl, ok := c.GetStaleWithTTL(key); ok {
		c.recordTierStaleHit("l1")
		return v, ttl, "l1_memory", true
	}
	if c.l2 != nil {
		if v, ttl, ok := c.l2.GetStaleWithTTL(key); ok {
			c.recordTierStaleHit("l2")
			return v, ttl, "l2_disk", true
		}
	}
	return nil, 0, "", false
}

func (c *Cache) Get(key string) ([]byte, bool) {
	if c == nil || c.disabled {
		return nil, false
	}
	c.mu.Lock()
	e, ok := c.entries[key]
	if ok && !time.Now().After(e.expiresAt) {
		// Promote to front of LRU list (most recently used)
		if elem, found := c.lruIndex[key]; found {
			c.lruList.MoveToFront(elem)
		}
		if c.hotOn {
			c.recordHotLocked(key)
		}
		c.mu.Unlock()
		c.Hits.Add(1)
		return e.value, true
	}
	c.mu.Unlock()

	// L2 fallback (disk)
	if c.l2 != nil {
		if v, ok := c.l2.Get(key); ok {
			c.Set(key, v) // promote to L1
			c.Hits.Add(1)
			return v, true
		}
	}

	// L3 fallback (peer fleet — sharded)
	if c.l3 != nil {
		if v, remainingTTL, ok := c.l3.Get(key); ok {
			// Use the owner's remaining TTL for the shadow copy — never extend.
			// If TTL is unknown (0), use a short default.
			shadowTTL := remainingTTL
			if shadowTTL <= 0 {
				shadowTTL = 30 * time.Second
			}
			c.SetWithTTL(key, v, shadowTTL)
			c.Hits.Add(1)
			return v, true
		}
	}

	c.Misses.Add(1)
	return nil, false
}

// Set stores a value with the default TTL.
func (c *Cache) Set(key string, value []byte) {
	if c == nil || c.disabled {
		return
	}
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetShadowWithTTL stores a shadow value locally without re-propagating it to peers.
func (c *Cache) SetShadowWithTTL(key string, value []byte, ttl time.Duration) {
	if c == nil || c.disabled {
		return
	}
	c.setWithTTL(key, value, ttl, false)
}

// SetLocalOnlyWithTTL stores a value only in local L1 memory.
// It skips L2 disk writes, L3 peer propagation, and non-owner TTL clamping.
func (c *Cache) SetLocalOnlyWithTTL(key string, value []byte, ttl time.Duration) {
	if c == nil || c.disabled {
		return
	}
	size := len(value)
	if size > c.maxBytes/10 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[key]; ok {
		c.curBytes -= old.sizeBytes
		if elem, found := c.lruIndex[key]; found {
			c.lruList.MoveToFront(elem)
		}
	}

	c.evictIfNeeded(size)

	c.entries[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		sizeBytes: size,
	}
	c.curBytes += size

	if _, found := c.lruIndex[key]; !found {
		elem := c.lruList.PushFront(&lruEntry{key: key})
		c.lruIndex[key] = elem
	}
}

// SetLocalAndDiskWithTTL stores a value in local L1 and the local L2 disk cache,
// but skips peer write-through. This is the preferred mode for helper/read caches
// that should survive pod churn without adding east-west write amplification.
func (c *Cache) SetLocalAndDiskWithTTL(key string, value []byte, ttl time.Duration) {
	if c == nil || c.disabled {
		return
	}
	size := len(value)
	if size > c.maxBytes/10 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.storeLocalLocked(key, value, ttl, size)
	if c.l2 != nil && !skipL2WriteForKey(key) {
		c.l2.Set(key, value, ttl)
	}
}

// SetWithTTL stores a value with a specific TTL.
func (c *Cache) SetWithTTL(key string, value []byte, ttl time.Duration) {
	if c == nil || c.disabled {
		return
	}
	c.setWithTTL(key, value, ttl, true)
}

func (c *Cache) setWithTTL(key string, value []byte, ttl time.Duration, propagateL3 bool) {
	size := len(value)
	// Don't cache entries larger than 10% of max bytes
	if size > c.maxBytes/10 {
		return
	}

	ownerLocal := true
	writeThroughEnabled := false
	if c.l3 != nil {
		ownerLocal = c.l3.IsOwner(key)
		writeThroughEnabled = c.l3.WriteThroughEnabled()
		// When write-through is enabled and this replica isn't owner, keep only
		// a short-lived shadow copy locally. Owner peer keeps full TTL.
		if writeThroughEnabled && !ownerLocal && ttl > nonOwnerShadowMaxTTL {
			ttl = nonOwnerShadowMaxTTL
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.storeLocalLocked(key, value, ttl, size)

	// Write-through to L2 (disk), except keys that already have dedicated
	// persistence paths and would otherwise double-write to disk.
	shouldWriteL2 := c.l2 != nil && !skipL2WriteForKey(key)
	// Avoid concentrating disk writes on non-owner pods when write-through is active.
	if shouldWriteL2 && writeThroughEnabled && !ownerLocal {
		shouldWriteL2 = false
	}
	if shouldWriteL2 {
		c.l2.Set(key, value, ttl)
	}

	// L3 optional owner write-through for better cache distribution in skewed
	// traffic scenarios (for example a single hot client pinned to one pod).
	if propagateL3 && c.l3 != nil {
		c.l3.SetWithTTL(key, value, ttl)
	}
}

func (c *Cache) storeLocalLocked(key string, value []byte, ttl time.Duration, size int) {
	if old, ok := c.entries[key]; ok {
		c.curBytes -= old.sizeBytes
		if elem, found := c.lruIndex[key]; found {
			c.lruList.MoveToFront(elem)
		}
	}

	c.evictIfNeeded(size)

	c.entries[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		sizeBytes: size,
	}
	c.curBytes += size

	if _, found := c.lruIndex[key]; !found {
		elem := c.lruList.PushFront(&lruEntry{key: key})
		c.lruIndex[key] = elem
	}
}

func skipL2WriteForKey(key string) bool {
	return len(key) >= len("patterns:") && key[:len("patterns:")] == "patterns:"
}

// Invalidate removes a specific key.
func (c *Cache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		c.curBytes -= e.sizeBytes
		delete(c.entries, key)
		delete(c.hot, key)
		if elem, found := c.lruIndex[key]; found {
			c.lruList.Remove(elem)
			delete(c.lruIndex, key)
		}
	}
}

// InvalidatePrefix removes all keys with the given prefix.
func (c *Cache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			c.curBytes -= e.sizeBytes
			delete(c.entries, k)
			delete(c.hot, k)
			if elem, found := c.lruIndex[k]; found {
				c.lruList.Remove(elem)
				delete(c.lruIndex, k)
			}
		}
	}
}

// Size returns current number of entries and bytes.
func (c *Cache) Size() (entries int, bytes int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries), c.curBytes
}

func (c *Cache) evictIfNeeded(incomingSize int) {
	now := time.Now()
	// First pass: remove expired entries
	if len(c.entries) >= c.maxEntries || c.curBytes+incomingSize > c.maxBytes {
		for k, v := range c.entries {
			if now.After(v.expiresAt) {
				c.curBytes -= v.sizeBytes
				delete(c.entries, k)
				delete(c.hot, k)
				if elem, found := c.lruIndex[k]; found {
					c.lruList.Remove(elem)
					delete(c.lruIndex, k)
				}
				c.Evictions.Add(1)
			}
		}
	}
	// Second pass: LRU eviction — remove from back (least recently used)
	for len(c.entries) >= c.maxEntries || c.curBytes+incomingSize > c.maxBytes {
		back := c.lruList.Back()
		if back == nil {
			break // empty list, nothing to evict
		}
		lruE := back.Value.(*lruEntry)
		if e, ok := c.entries[lruE.key]; ok {
			c.curBytes -= e.sizeBytes
			delete(c.entries, lruE.key)
			delete(c.hot, lruE.key)
			c.Evictions.Add(1)
		}
		c.lruList.Remove(back)
		delete(c.lruIndex, lruE.key)
	}
}

func (c *Cache) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.entries {
			if now.After(v.expiresAt) {
				c.curBytes -= v.sizeBytes
				delete(c.entries, k)
				delete(c.hot, k)
				if elem, found := c.lruIndex[k]; found {
					c.lruList.Remove(elem)
					delete(c.lruIndex, k)
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Cache) recordHotLocked(key string) {
	hs := c.hot[key]
	hs.score++
	hs.lastAccess = time.Now().UnixNano()
	if hs.tenant == "" {
		hs.tenant = tenantFromCacheKey(key)
	}
	c.hot[key] = hs
	if len(c.hot) <= c.hotMax {
		return
	}
	c.pruneHotLocked(len(c.hot) - c.hotMax)
}

func (c *Cache) pruneHotLocked(drop int) {
	if drop <= 0 || len(c.hot) == 0 {
		return
	}
	type candidate struct {
		key        string
		score      uint64
		lastAccess int64
	}
	cands := make([]candidate, 0, len(c.hot))
	for k, v := range c.hot {
		cands = append(cands, candidate{key: k, score: v.score, lastAccess: v.lastAccess})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].score == cands[j].score {
			return cands[i].lastAccess < cands[j].lastAccess
		}
		return cands[i].score < cands[j].score
	})
	if drop > len(cands) {
		drop = len(cands)
	}
	for i := 0; i < drop; i++ {
		delete(c.hot, cands[i].key)
	}
}

// TopHotKeys returns hot cache keys sorted by descending score.
// It is used by peer hot-index read-ahead and intentionally bounded.
func (c *Cache) TopHotKeys(limit int, minRemainingTTL time.Duration, maxObjectBytes int) []HotKeySnapshot {
	if limit <= 0 {
		return nil
	}
	if limit > maxHotIndexQueryLimit {
		limit = maxHotIndexQueryLimit
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.hotOn {
		return nil
	}

	now := time.Now()
	capHint := len(c.hot)
	if capHint > maxHotIndexQueryLimit {
		capHint = maxHotIndexQueryLimit
	}
	out := make([]HotKeySnapshot, 0, capHint)
	for key, stat := range c.hot {
		e, ok := c.entries[key]
		if !ok {
			continue
		}
		remaining := time.Until(e.expiresAt)
		if remaining <= 0 || now.After(e.expiresAt) {
			continue
		}
		if minRemainingTTL > 0 && remaining < minRemainingTTL {
			continue
		}
		if maxObjectBytes > 0 && e.sizeBytes > maxObjectBytes {
			continue
		}
		out = append(out, HotKeySnapshot{
			Key:            key,
			Tenant:         stat.tenant,
			Score:          stat.score,
			SizeBytes:      e.sizeBytes,
			RemainingTTLMS: remaining.Milliseconds(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].RemainingTTLMS > out[j].RemainingTTLMS
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (c *Cache) getL1WithTTL(key string) ([]byte, time.Duration, bool) {
	c.mu.Lock()
	e, ok := c.entries[key]
	if ok {
		remaining := time.Until(e.expiresAt)
		if remaining > 0 {
			if elem, found := c.lruIndex[key]; found {
				c.lruList.MoveToFront(elem)
			}
			if c.hotOn {
				c.recordHotLocked(key)
			}
			c.mu.Unlock()
			return e.value, remaining, true
		}
	}
	c.mu.Unlock()
	return nil, 0, false
}

func (c *Cache) recordTierRequest(tier string) {
	switch tier {
	case "l1":
		c.l1Stats.requests.Add(1)
	case "l2":
		c.l2Stats.requests.Add(1)
	case "l3":
		c.l3Stats.requests.Add(1)
	}
}

func (c *Cache) recordTierHit(tier string) {
	switch tier {
	case "l1":
		c.l1Stats.hits.Add(1)
	case "l2":
		c.l2Stats.hits.Add(1)
	case "l3":
		c.l3Stats.hits.Add(1)
	}
}

func (c *Cache) recordTierMiss(tier string) {
	switch tier {
	case "l1":
		c.l1Stats.misses.Add(1)
	case "l2":
		c.l2Stats.misses.Add(1)
	case "l3":
		c.l3Stats.misses.Add(1)
	}
}

func (c *Cache) recordTierStaleHit(tier string) {
	switch tier {
	case "l1":
		c.l1Stats.staleHits.Add(1)
	case "l2":
		c.l2Stats.staleHits.Add(1)
	case "l3":
		c.l3Stats.staleHits.Add(1)
	}
}

func (c *Cache) Stats() StatsSnapshot {
	entries, bytes := c.Size()
	var diskEntries int
	var diskBytes int64
	if c.l2 != nil {
		diskEntries, diskBytes = c.l2.Size()
	}
	return StatsSnapshot{
		L1: TierStatsSnapshot{
			Requests:  c.l1Stats.requests.Load(),
			Hits:      c.l1Stats.hits.Load(),
			Misses:    c.l1Stats.misses.Load(),
			StaleHits: c.l1Stats.staleHits.Load(),
		},
		L2: TierStatsSnapshot{
			Requests:  c.l2Stats.requests.Load(),
			Hits:      c.l2Stats.hits.Load(),
			Misses:    c.l2Stats.misses.Load(),
			StaleHits: c.l2Stats.staleHits.Load(),
		},
		L3: TierStatsSnapshot{
			Requests:  c.l3Stats.requests.Load(),
			Hits:      c.l3Stats.hits.Load(),
			Misses:    c.l3Stats.misses.Load(),
			StaleHits: c.l3Stats.staleHits.Load(),
		},
		BackendFallthrough: c.backendFallthrough.Load(),
		Entries:            entries,
		Bytes:              bytes,
		DiskEntries:        diskEntries,
		DiskBytes:          diskBytes,
	}
}

func tenantFromCacheKey(key string) string {
	if strings.HasPrefix(key, "__") {
		return ""
	}
	parts := strings.SplitN(key, ":", 3)
	if len(parts) < 3 {
		return ""
	}
	switch parts[0] {
	case "query_range",
		"query_range_window",
		"query_range_window_has_hits",
		"query",
		"series",
		"labels",
		"label_values",
		"volume",
		"volume_range",
		"detected_fields",
		"detected_labels",
		"detected_field_values",
		"patterns",
		"label_inventory":
		return parts[1]
	default:
		return ""
	}
}
