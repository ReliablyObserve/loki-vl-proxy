package cache

import (
	"container/list"
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
	l2         *DiskCache  // optional L2 disk cache
	l3         *PeerCache  // optional L3 peer fleet cache
	done       chan struct{} // signals cleanup goroutine to stop

	// LRU ordering: front = most recently used, back = least recently used
	lruList  *list.List            // doubly-linked list of *lruEntry
	lruIndex map[string]*list.Element // key → list element for O(1) lookup

	// Stats
	Hits   atomic.Int64
	Misses atomic.Int64
	Evictions atomic.Int64
}

type lruEntry struct {
	key string
}

// SetL2 attaches an L2 disk cache. On L1 miss, L2 is checked.
// On L1 set, values are also written to L2.
func (c *Cache) SetL2(dc *DiskCache) {
	c.l2 = dc
}

// SetL3 attaches an L3 peer cache for distributed fleet operation.
// On L1+L2 miss, the owning peer is queried before hitting VL.
func (c *Cache) SetL3(pc *PeerCache) {
	c.l3 = pc
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

// SetWithTTL stores a value with a specific TTL.
func (c *Cache) SetWithTTL(key string, value []byte, ttl time.Duration) {
	size := len(value)
	// Don't cache entries larger than 10% of max bytes
	if size > c.maxBytes/10 {
		return
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
	if c.l2 != nil && !skipL2WriteForKey(key) {
		c.l2.Set(key, value, ttl)
	}

	// L3 optional owner write-through for better cache distribution in skewed
	// traffic scenarios (for example a single hot client pinned to one pod).
	if c.l3 != nil {
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
				if elem, found := c.lruIndex[k]; found {
					c.lruList.Remove(elem)
					delete(c.lruIndex, k)
				}
			}
		}
		c.mu.Unlock()
	}
}
