package cache

import (
	"time"
)

// Frontend shadows share the primary L1 ceiling and are additionally capped at
// ten percent of its bytes and entries. They cannot evict owned/local entries.
const nonOwnerShadowBudgetDivisor = 10

func (c *Cache) removeEntryLocked(key string) {
	size := 0
	if old, ok := c.entries[key]; ok {
		size = old.sizeBytes
		c.curBytes -= old.sizeBytes
		stats := &c.layerL1[c.layer(key)]
		stats.entries.Add(-1)
		stats.bytes.Add(-int64(old.sizeBytes))
		if old.shadow {
			c.shadowBytes -= old.sizeBytes
			c.shadowEntries--
		}
		delete(c.entries, key)
	}
	delete(c.hot, key)
	if c.hitsOn.Load() {
		c.hitsMu.Lock()
		delete(c.hits, key)
		c.hitsMu.Unlock()
	}
	if elem, ok := c.lruIndex[key]; ok {
		data := elem.Value.(*lruEntry)
		c.leaveSegmentLocked(data, size)
		c.lruList.Remove(elem)
		delete(c.lruIndex, key)
	}
}
func (c *Cache) shadowRoomLocked(size int, now time.Time) bool {
	maxEntries, maxBytes := c.maxEntries/nonOwnerShadowBudgetDivisor, c.maxBytes/nonOwnerShadowBudgetDivisor
	if maxEntries == 0 || size > maxBytes {
		return false
	}
	for c.shadowEntries >= maxEntries || c.shadowBytes+size > maxBytes || len(c.entries) >= c.maxEntries || c.curBytes+size > c.maxBytes {
		back := c.shadowList.back
		if back == nil {
			return false
		}
		data := back.data
		layer := c.layer(data.key)
		c.layerL1[layer].evictions.Add(1)
		c.countEvictionLocked(layer, EvictionCapacity, data, now)
		c.removeEntryLocked(data.key)
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
	if s := c.sizing; s != nil {
		s.lookupL2(key, ok)
	}
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
		c.layerL1[c.layer(key)].rejections.Add(1)
		c.signals.reject(c.layer(key), RejectionSingleEntryBudget)
		return true
	}
	if old, ok := c.entries[key]; ok && old.expiresAt.After(clockNow()) {
		return true
	}
	if c.storeEntryLocked(key, value, remaining, size, false, false, false, expiry) {
		c.signals.promote(c.layer(key), PromotionDiskToMemory)
	}
	return true
}
func (c *Cache) storeEntryLocked(key string, value []byte, ttl time.Duration, size int, stamped, memOnly, shadow bool, sourceExpiry int64) bool {
	now := clockNow()
	if sourceExpiry != 0 {
		remaining := time.Unix(0, sourceExpiry).Sub(now)
		if remaining <= 0 {
			c.layerL1[c.layer(key)].rejections.Add(1)
			c.signals.reject(c.layer(key), RejectionSourceExpired)
			return false
		}
		ttl = min(ttl, remaining)
	}
	e := entry{value: value, expiresAt: now.Add(ttl), sizeBytes: size, memOnly: memOnly, shadow: shadow, sourceExpiresAt: sourceExpiry}
	if stamped {
		e.storedAt = now
	}
	replaced, existed := c.replaceEntryLocked(key, e, now)
	if replaced {
		if s := c.sizing; s != nil {
			s.storeL1(key, len(value), size, ttl)
		}
		return true
	}
	if existed { // a value of another storage class
		c.countEvictionLocked(c.layer(key), EvictionReplaced, nil, now)
	}
	c.removeEntryLocked(key)
	if shadow {
		if !c.shadowRoomLocked(size, now) {
			c.layerL1[c.layer(key)].rejections.Add(1)
			c.signals.reject(c.layer(key), RejectionShadowBudget)
			return false
		}
	} else {
		c.evictIfNeeded(size)
	}
	c.entries[key] = e
	c.curBytes += size
	stats := &c.layerL1[c.layer(key)]
	stats.entries.Add(1)
	stats.bytes.Add(int64(size))
	stats.admissions.Add(1)
	stats.writeBytes.Add(int64(len(value)))
	data := &lruEntry{key: key, admitted: uint32(now.Unix())}
	c.lruIndex[key] = c.lruList.PushFront(data)
	if s := c.sizing; s != nil {
		s.storeL1(key, len(value), size, ttl)
	}
	if shadow {
		data.seg, data.node = segShadow, c.newSegNodeLocked(data)
		c.shadowList.pushFront(data.node)
		c.shadowBytes += size
		c.shadowEntries++
	} else if c.retention != nil {
		c.enterProbationLocked(data)
	}
	return true
}

// Replacing a hot entry in the same storage class reuses its list nodes.
// existed says the key held a value, replaced or not.
func (c *Cache) replaceEntryLocked(key string, next entry, now time.Time) (replaced, existed bool) {
	old, ok := c.entries[key]
	if !ok {
		return false, false
	}
	if old.shadow != next.shadow || old.memOnly != next.memOnly {
		return false, true
	}
	delta := next.sizeBytes - old.sizeBytes
	if c.curBytes+delta > c.maxBytes {
		return false, true
	}
	if next.shadow && c.shadowBytes+delta > c.maxBytes/nonOwnerShadowBudgetDivisor {
		return false, true
	}
	elem, ok := c.lruIndex[key]
	if !ok {
		return false, true
	}
	data := elem.Value.(*lruEntry)
	data.admitted, data.hits = uint32(now.Unix()), 0
	c.entries[key] = next
	c.curBytes += delta
	layer := c.layer(key)
	c.signals.count(layer, EvictionReplaced)
	stats := &c.layerL1[layer]
	stats.bytes.Add(int64(delta))
	stats.admissions.Add(1)
	stats.writeBytes.Add(int64(len(next.value)))
	c.lruList.MoveToFront(elem)
	if next.shadow {
		c.shadowBytes += delta
		c.shadowList.moveToFront(data.node)
	} else if c.retention != nil {
		c.resizedSegmentLocked(data, delta)
	}
	return true, true
}
func (c *Cache) setPlacedWithTTL(key string, value []byte, ttl time.Duration, propagate, stamped, force bool) {
	size := entrySize(key, value)
	if size > c.maxBytes/10 {
		c.layerL1[c.layer(key)].rejections.Add(1)
		c.signals.reject(c.layer(key), RejectionSingleEntryBudget)
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
	switch {
	case c.l2 == nil:
	case shadow:
		c.skipDisk(key, RejectionNotReplica)
	case skipL2WriteForKey(key):
		c.skipDisk(key, RejectionMemoryOnly)
	default:
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
