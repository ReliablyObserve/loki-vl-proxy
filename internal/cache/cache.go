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
	l2         *DiskCache    // optional L2 disk cache
	l3         *PeerCache    // optional L3 peer fleet cache
	done       chan struct{} // signals cleanup goroutine to stop

	// LRU ordering: front = most recently used, back = least recently used
	lruList  *list.List               // doubly-linked list of *lruEntry
	lruIndex map[string]*list.Element // key → list element for O(1) lookup
	hot      map[string]hotStat       // lightweight per-key hotness index for peer read-ahead
	hotMax   int                      // cap hot index growth

	// Stats
	Hits      atomic.Int64
	Misses    atomic.Int64
	Evictions atomic.Int64
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

// SetL2 attaches an L2 disk cache. On L1 miss, L2 is checked.
// On L1 set, values are also written to L2.
func (c *Cache) SetL2(dc *DiskCache) {
	c.l2 = dc
}

// SetL3 attaches an L3 peer cache for distributed fleet operation.
// On L1+L2 miss, the owning peer is queried before hitting VL.
func (c *Cache) SetL3(pc *PeerCache) {
	c.l3 = pc
	if pc != nil {
		pc.AttachLocalCache(c)
	}
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
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

// GetWithTTL returns the value and remaining TTL for a key.
// Returns (nil, 0, false) on miss.
func (c *Cache) GetWithTTL(key string) ([]byte, time.Duration, bool) {
	c.mu.Lock()
	e, ok := c.entries[key]
	if ok {
		remaining := time.Until(e.expiresAt)
		if remaining > 0 {
			if elem, found := c.lruIndex[key]; found {
				c.lruList.MoveToFront(elem)
			}
			c.recordHotLocked(key)
			c.mu.Unlock()
			c.Hits.Add(1)
			return e.value, remaining, true
		}
	}
	c.mu.Unlock()
	return nil, 0, false
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	e, ok := c.entries[key]
	if ok && !time.Now().After(e.expiresAt) {
		// Promote to front of LRU list (most recently used)
		if elem, found := c.lruIndex[key]; found {
			c.lruList.MoveToFront(elem)
		}
		c.recordHotLocked(key)
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
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetShadowWithTTL stores a shadow value locally without re-propagating it to peers.
func (c *Cache) SetShadowWithTTL(key string, value []byte, ttl time.Duration) {
	c.setWithTTL(key, value, ttl, false)
}

// SetWithTTL stores a value with a specific TTL.
func (c *Cache) SetWithTTL(key string, value []byte, ttl time.Duration) {
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

	// If replacing existing entry, subtract old size and promote in LRU
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

	// Add to LRU list if not already there
	if _, found := c.lruIndex[key]; !found {
		elem := c.lruList.PushFront(&lruEntry{key: key})
		c.lruIndex[key] = elem
	}

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
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	out := make([]HotKeySnapshot, 0, min(limit, len(c.hot)))
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
