package cache

import (
	"container/list"
	"fmt"
	"time"
)

// Reuse-aware retention for the memory tier (L1), off unless SetRetention is
// called. It changes only which entry a capacity eviction removes and where it
// goes: never an entry's TTL, its value, or whether a lookup may serve it. An
// entry still leaves when its TTL ends, so expiry-driven evictions are the same
// with the policy on or off.
//
// Protection is a segmented LRU kept beside the cache's one recency list (the
// list the restart hand-over reads): an entry starts on probation and moves to
// the protected segment on its first hit; the protected segment holds at most
// ProtectPercent of the entry and byte budgets, its least recently used entry
// going back to probation when it is full. A capacity eviction takes the least
// recently used probation entry first, so a flood of entries read once does
// not evict the entries that are read again.
//
// Demotion: a capacity eviction of an entry read at least once keeps its disk
// copy (moved to the newest end of the disk's recency order), or writes it to
// the disk with the TTL it has left when the disk holds none.

// segment is the list an lruEntry's segElem belongs to.
type segment uint8

const (
	segNone segment = iota
	segShadow
	segProbation
	segProtected
)

// MaxRetentionProtectPercent is the largest protected share: probation keeps
// at least a tenth of the budgets, so a new entry has room to be read again.
const MaxRetentionProtectPercent = 90

// RetentionPolicy is what a memory cache keeps under capacity pressure. The
// zero value is plain LRU.
type RetentionPolicy struct {
	// ProtectPercent is the share of the entry and byte budgets the protected
	// segment may hold (1 to MaxRetentionProtectPercent); 0 turns protection off.
	ProtectPercent int
	// DemoteOnEvict keeps or writes the disk copy of an entry read at least
	// once when a capacity eviction removes it from memory.
	DemoteOnEvict bool
}

type retentionState struct {
	policy RetentionPolicy
	// on is protection; probation and protected are its segments.
	on                         bool
	probation, protected       segList
	maxProtBytes, maxProtCount int
	protBytes, protCount       int
	demote                     bool
}

// SetRetention sets the retention policy. It must be called before the cache
// stores an entry; the zero policy keeps plain LRU.
func (c *Cache) SetRetention(p RetentionPolicy) error {
	if c == nil || c.disabled {
		return nil
	}
	if p.ProtectPercent < 0 || p.ProtectPercent > MaxRetentionProtectPercent {
		return fmt.Errorf("cache reuse protection percent %d outside 0..%d", p.ProtectPercent, MaxRetentionProtectPercent)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != 0 {
		return fmt.Errorf("cache retention policy set after the cache stored entries")
	}
	if p.ProtectPercent == 0 && !p.DemoteOnEvict {
		c.retention = nil
		return nil
	}
	c.retention = &retentionState{
		policy:       p,
		on:           p.ProtectPercent > 0,
		maxProtBytes: c.maxBytes * p.ProtectPercent / 100,
		maxProtCount: c.maxEntries * p.ProtectPercent / 100,
		demote:       p.DemoteOnEvict,
	}
	return nil
}

// Retention returns the cache's retention policy (the zero policy for plain
// LRU).
func (c *Cache) Retention() RetentionPolicy {
	if c == nil || c.disabled {
		return RetentionPolicy{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.retention == nil {
		return RetentionPolicy{}
	}
	return c.retention.policy
}

// enterProbationLocked puts a new non-shadow node on probation when
// protection is on.
func (c *Cache) enterProbationLocked(data *lruEntry) {
	if r := c.retention; r.on {
		data.seg, data.node = segProbation, c.newSegNodeLocked(data)
		r.probation.pushFront(data.node)
	}
}

// segFreeMax bounds the reusable nodes a cache keeps (24 bytes each).
const segFreeMax = 1024

// newSegNodeLocked returns a node for data, reusing a freed one.
func (c *Cache) newSegNodeLocked(data *lruEntry) *segNode {
	n := c.segFree
	if n == nil {
		return &segNode{data: data}
	}
	c.segFree, c.segFreeN = n.next, c.segFreeN-1
	n.next, n.data = nil, data
	return n
}

// freeSegNodeLocked keeps an unlinked node for reuse.
func (c *Cache) freeSegNodeLocked(n *segNode) {
	if c.segFreeN >= segFreeMax {
		return
	}
	n.data, n.prev, n.next = nil, nil, c.segFree
	c.segFree = n
	c.segFreeN++
}

// leaveSegmentLocked takes the node of an entry of size bytes out of its
// segment list.
func (c *Cache) leaveSegmentLocked(data *lruEntry, size int) {
	switch data.seg {
	case segShadow:
		c.shadowList.remove(data.node)
	case segProbation:
		c.retention.probation.remove(data.node)
	case segProtected:
		c.retention.protected.remove(data.node)
		c.retention.protBytes -= size
		c.retention.protCount--
	}
	if data.node != nil {
		c.freeSegNodeLocked(data.node)
	}
	data.seg, data.node = segNone, nil
}

// hitSegmentLocked applies a hit to the node's segment: most recently used in
// its list, and a first hit moves a probation entry (or one the restart
// warm-up admitted, in no segment) to the protected segment.
func (c *Cache) hitSegmentLocked(data *lruEntry) {
	switch data.seg {
	case segShadow:
		c.shadowList.moveToFront(data.node)
		return
	case segProtected:
		c.retention.protected.moveToFront(data.node)
		return
	}
	r := c.retention
	if r == nil || !r.on {
		return
	}
	e, ok := c.entries[data.key]
	if !ok || e.shadow {
		return
	}
	if data.seg == segProbation {
		r.probation.remove(data.node)
	} else {
		data.node = c.newSegNodeLocked(data)
	}
	data.seg = segProtected
	r.protected.pushFront(data.node)
	r.protBytes += e.sizeBytes
	r.protCount++
	c.balanceProtectedLocked()
}

// resizedSegmentLocked applies an in-place replacement that changed an
// entry's size by delta and made it most recently used. A replaced entry in no
// segment (admitted by the warm-up) is a new admission: it starts on probation.
func (c *Cache) resizedSegmentLocked(data *lruEntry, delta int) {
	switch data.seg {
	case segNone:
		c.enterProbationLocked(data)
	case segProbation:
		c.retention.probation.moveToFront(data.node)
	case segProtected:
		c.retention.protected.moveToFront(data.node)
		c.retention.protBytes += delta
		c.balanceProtectedLocked()
	}
}

// balanceProtectedLocked moves the least recently used protected entries back
// to probation (most recently used there) until the segment fits its share.
func (c *Cache) balanceProtectedLocked() {
	r := c.retention
	for r.protBytes > r.maxProtBytes || r.protCount > r.maxProtCount {
		back := r.protected.back
		if back == nil {
			return
		}
		r.protected.remove(back)
		r.protBytes -= c.entries[back.data.key].sizeBytes
		r.protCount--
		back.data.seg = segProbation
		r.probation.pushFront(back)
	}
}

// retentionVictimLocked is the capacity victim under the policy, given the
// least recently used entry overall: that entry when it is in no segment (a
// warm-up entry never read), else the least recently used probation entry,
// else the least recently used protected one.
func (c *Cache) retentionVictimLocked(back *list.Element) *list.Element {
	r := c.retention
	if !r.on || (back != nil && back.Value.(*lruEntry).seg == segNone) {
		return back
	}
	for _, l := range [...]*segList{&r.probation, &r.protected} {
		if b := l.back; b != nil {
			return c.lruIndex[b.data.key]
		}
	}
	return back
}

// segNode links an lruEntry into one segment list.
type segNode struct {
	prev, next *segNode
	data       *lruEntry
}

// segList is an intrusive doubly linked list of segNodes, most recently used
// at front. The zero value is empty.
type segList struct {
	front, back *segNode
	n           int
}

func (l *segList) pushFront(n *segNode) {
	n.prev, n.next = nil, l.front
	if l.front != nil {
		l.front.prev = n
	} else {
		l.back = n
	}
	l.front = n
	l.n++
}

func (l *segList) remove(n *segNode) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.front = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.back = n.prev
	}
	n.prev, n.next = nil, nil
	l.n--
}

func (l *segList) moveToFront(n *segNode) {
	if l.front == n {
		return
	}
	l.remove(n)
	l.pushFront(n)
}

// demoteLocked hands the disk an entry a capacity eviction is about to remove
// from memory, when it was read at least once and the disk may hold it: not a
// shadow or memory-only value, not during a purge, and with at least the
// disk's minimum TTL left. A disk that holds a current copy keeps it (newest
// in its recency order); otherwise the value is written with the TTL it has
// left, never more.
func (c *Cache) demoteLocked(data *lruEntry, now time.Time) {
	if c.l2 == nil || data.hits == 0 || data.seg == segShadow || c.diskPurges != 0 {
		return
	}
	e, ok := c.entries[data.key]
	if !ok || e.shadow || e.memOnly || skipL2WriteForKey(data.key) {
		return
	}
	expiry := e.expiresAt
	if e.sourceExpiresAt != 0 {
		if source := time.Unix(0, e.sourceExpiresAt); source.Before(expiry) {
			expiry = source
		}
	}
	remaining := expiry.Sub(now)
	if remaining <= 0 || remaining < c.l2.MinTTL() {
		return
	}
	layer := c.layer(data.key)
	if c.l2.retain(data.key) {
		c.signals.demote(layer, DemotionRetained)
		return
	}
	if c.l2.set(data.key, e.value, remaining, 0, 0) {
		c.signals.demote(layer, DemotionWritten)
	}
}

// retain moves key's current disk copy to the newest end of the recency
// order, as a lookup would without counting a hit. It reports whether the
// disk holds a current, unexpired copy (committed or queued).
func (dc *DiskCache) retain(key string) bool {
	now := clockNow().UnixNano()
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	if dc.closed || dc.closing {
		return false
	}
	for _, queue := range [...]map[string]diskEntry{dc.writeBuf, dc.flushing} {
		if entry, ok := queue[key]; ok {
			if (entry.state != nil && entry.state.cancelled.Load()) || entry.ExpiresAt <= now {
				return false
			}
			if entry.state == nil {
				return true
			}
			dc.sequence++
			entry.state.lastAccess = dc.sequence
			return true
		}
	}
	item := dc.index[key]
	if item == nil || item.invalid || item.expiresAt <= now {
		return false
	}
	dc.sequence++
	item.lastAccess = dc.sequence
	dc.touch(item)
	return true
}
