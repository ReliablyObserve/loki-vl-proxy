package cache

import (
	"container/list"
	"time"
)

// Frontend shadows share the primary L1 ceiling and are additionally capped at
// ten percent of its bytes and entries. They cannot evict owned/local entries.
const nonOwnerShadowBudgetDivisor = 10

func (c *Cache) removeEntryLocked(key string) {
	if old, ok := c.entries[key]; ok {
		c.curBytes -= old.sizeBytes
		if old.shadow {
			c.shadowBytes -= old.sizeBytes
			c.shadowEntries--
		}
		delete(c.entries, key)
	}
	delete(c.hot, key)
	if elem, ok := c.lruIndex[key]; ok {
		data := elem.Value.(*lruEntry)
		if data.shadowElem != nil {
			c.shadowList.Remove(data.shadowElem)
		}
		c.lruList.Remove(elem)
		delete(c.lruIndex, key)
	}
}
func (c *Cache) shadowRoomLocked(size int) bool {
	maxEntries, maxBytes := c.maxEntries/nonOwnerShadowBudgetDivisor, c.maxBytes/nonOwnerShadowBudgetDivisor
	if maxEntries == 0 || size > maxBytes {
		return false
	}
	for c.shadowEntries >= maxEntries || c.shadowBytes+size > maxBytes || len(c.entries) >= c.maxEntries || c.curBytes+size > c.maxBytes {
		if c.shadowList == nil || c.shadowList.Back() == nil {
			return false
		}
		key := c.shadowList.Back().Value.(*lruEntry).key
		c.removeEntryLocked(key)
		c.Evictions.Add(1)
	}
	return true
}

// diskPromotionGeneration snapshots eligibility before starting a disk read.
func (c *Cache) diskPromotionGeneration() (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.diskGeneration, c.diskPurges == 0
}

// getDiskWithTTL fences both the returned snapshot and L1 promotion against a
// local purge. Remote peers are deliberately outside this local purge contract.
func (c *Cache) getDiskWithTTL(key string) ([]byte, time.Duration, bool) {
	start := clockNow()
	generation, allowed := c.diskPromotionGeneration()
	if !allowed {
		return nil, 0, false
	}
	value, ttl, ok := c.l2.GetWithTTL(key)
	// Starting before the read conservatively charges lookup/scheduling time
	// against the remaining TTL reported by disk.
	deadline := start.Add(ttl)
	if !ok || !c.promoteDiskWithDeadline(key, value, deadline, generation) {
		return nil, 0, false
	}
	remaining := deadline.Sub(clockNow())
	if remaining <= 0 {
		return nil, 0, false
	}
	return value, remaining, true
}

// promoteDiskWithDeadline preserves a source deadline without overwriting newer L1
// data. False means a local purge or expired deadline invalidated the snapshot;
// a valid oversized snapshot can still be served without admitting it into L1.
func (c *Cache) promoteDiskWithDeadline(key string, value []byte, deadline time.Time, generation uint64) bool {
	if c == nil || c.disabled {
		return false
	}
	expiry := deadline.UnixNano()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.diskPurges != 0 || c.diskGeneration != generation {
		return false
	}
	remaining := deadline.Sub(clockNow())
	if remaining <= 0 {
		return false
	}
	size := entrySize(key, value)
	if size > c.maxBytes/10 {
		return true
	}
	if old, ok := c.entries[key]; ok && old.expiresAt.After(clockNow()) {
		return true
	}
	c.storeEntryLocked(key, value, remaining, size, false, false, false, expiry)
	return true
}
func (c *Cache) storeEntryLocked(key string, value []byte, ttl time.Duration, size int, stamped, memOnly, shadow bool, sourceExpiry int64) bool {
	now := clockNow()
	if sourceExpiry != 0 {
		remaining := time.Unix(0, sourceExpiry).Sub(now)
		if remaining <= 0 {
			return false
		}
		ttl = min(ttl, remaining)
	}
	e := entry{value: value, expiresAt: now.Add(ttl), sizeBytes: size, memOnly: memOnly, shadow: shadow, sourceExpiresAt: sourceExpiry}
	if stamped {
		e.storedAt = now
	}
	if c.replaceEntryLocked(key, e) {
		return true
	}
	c.removeEntryLocked(key)
	if shadow {
		if !c.shadowRoomLocked(size) {
			return false
		}
		if c.shadowList == nil {
			c.shadowList = list.New()
		}
	} else {
		c.evictIfNeeded(size)
	}
	c.entries[key] = e
	c.curBytes += size
	data := &lruEntry{key: key}
	c.lruIndex[key] = c.lruList.PushFront(data)
	if shadow {
		data.shadowElem = c.shadowList.PushFront(data)
		c.shadowBytes += size
		c.shadowEntries++
	}
	return true
}

// Replacing a hot entry in the same storage class reuses its list nodes.
func (c *Cache) replaceEntryLocked(key string, next entry) bool {
	old, ok := c.entries[key]
	if !ok || old.shadow != next.shadow || old.memOnly != next.memOnly {
		return false
	}
	delta := next.sizeBytes - old.sizeBytes
	if c.curBytes+delta > c.maxBytes {
		return false
	}
	if next.shadow && c.shadowBytes+delta > c.maxBytes/nonOwnerShadowBudgetDivisor {
		return false
	}
	elem, ok := c.lruIndex[key]
	if !ok {
		return false
	}
	c.entries[key] = next
	c.curBytes += delta
	c.lruList.MoveToFront(elem)
	if next.shadow {
		c.shadowBytes += delta
		c.shadowList.MoveToFront(elem.Value.(*lruEntry).shadowElem)
	}
	return true
}
func (c *Cache) setPlacedWithTTL(key string, value []byte, ttl time.Duration, propagate, stamped, force bool) {
	size := entrySize(key, value)
	if size > c.maxBytes/10 {
		return
	}
	publishable := c.l3 != nil && c.l3.publicationEligible(key, value, ttl, force)
	shadow := publishable && !c.l3.IsReplica(key)
	storedTTL := ttl
	var sourceExpiry int64
	if shadow {
		storedTTL = min(ttl, nonOwnerShadowMaxTTL)
		if !stamped {
			sourceExpiry = clockNow().Add(ttl).UnixNano()
		}
	}
	c.mu.Lock()
	c.storeEntryLocked(key, value, storedTTL, size, stamped, false, shadow, sourceExpiry)
	if !shadow && c.l2 != nil && !skipL2WriteForKey(key) {
		c.l2.Set(key, value, ttl)
	}
	c.mu.Unlock()
	if propagate && c.l3 != nil {
		// Preserve the existing push admission/error counters. The pure
		// eligibility check above decides storage without changing counters.
		if force {
			c.l3.Replicate(key, value, ttl)
		} else {
			c.l3.SetWithTTL(key, value, ttl)
		}
	}
}
