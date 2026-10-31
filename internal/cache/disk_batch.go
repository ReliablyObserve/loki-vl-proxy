package cache

import (
	"container/heap"
	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
	"sort"
)

type diskMutation struct {
	size, expiry int64
	entry        diskEntry
	remove       bool
	// originalRemoved tracks deletion of a record present before this batch,
	// including expiry/invalidation before a new value for the same key.
	originalRemoved bool
}
type diskVictim struct {
	key    string
	access uint64
}
type diskVictims []diskVictim

func (h diskVictims) Len() int { return len(h) }
func (h diskVictims) Less(i, j int) bool {
	if h[i].access == h[j].access {
		return h[i].key < h[j].key
	}
	return h[i].access < h[j].access
}
func (h diskVictims) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *diskVictims) Push(value any) { *h = append(*h, value.(diskVictim)) }
func (h *diskVictims) Pop() any {
	old := *h
	n := len(old)
	value := old[n-1]
	old[n-1] = diskVictim{}
	*h = old[:n-1]
	return value
}

type diskEncodedEntry struct {
	key    string
	entry  diskEntry
	record []byte
	access uint64
}
type diskBatch struct {
	cache                      *DiskCache
	pending                    map[string]diskEntry
	expired                    []*diskLRUEntry
	records                    []diskEncodedEntry
	changes                    map[string]diskMutation
	order                      []string
	victims                    *diskVictims
	plannedBytes, plannedIndex int64
	plannedCount               int
	writes, evictions          int64
	layerEvictions             [LayerCount]int64
	layerRejections            [LayerCount]int64
	layerEncodedWrites         [LayerCount]int64
}

// flushLocked is called with flushMu held. No live index mutation precedes a
// successful bbolt commit, so rollback preserves the old index and recency.
func (dc *DiskCache) flushLocked() bool {
	batch, open := dc.takeDiskBatch()
	if batch == nil {
		return open
	}
	batch.encode()
	err := dc.db.Update(batch.commit)
	batch.finish(err)
	return err == nil
}

// Snapshot maps remain immutable and visible until commit; newer pending values
// always take precedence. Compression and transaction commits hold no queue lock.
func (dc *DiskCache) takeDiskBatch() (*diskBatch, bool) {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	if dc.closed {
		return nil, false
	}
	expired := dc.expiryCandidatesLocked()
	if len(dc.writeBuf) == 0 && len(expired) == 0 {
		return nil, true
	}
	pending := dc.writeBuf
	dc.writeBuf = make(map[string]diskEntry)
	dc.flushingBytes = 0
	dc.layerQueueBytes = [LayerCount]int64{}
	missing := 0
	for _, entry := range pending {
		if entry.state == nil {
			missing++
		}
	}
	// Existing expiry tests inject values directly; production Set supplies state.
	states := make([]diskEntryState, missing)
	stateIndex := 0
	for key, entry := range pending {
		if entry.state == nil {
			dc.sequence++
			entry.state = &states[stateIndex]
			stateIndex++
			entry.state.lastAccess = dc.sequence
			pending[key] = entry
		}
		dc.flushingBytes += diskQueueCharge(key, entry.Value)
		dc.layerQueueBytes[LayerForKey(key)] += diskQueueCharge(key, entry.Value)
	}
	dc.bufferBytes = dc.flushingBytes
	dc.flushing = pending
	return &diskBatch{cache: dc, pending: pending, expired: expired}, true
}
func (batch *diskBatch) encode() {
	batch.records = make([]diskEncodedEntry, 0, len(batch.pending))
	for key, entry := range batch.pending {
		if entry.state.cancelled.Load() {
			continue
		}
		if entry.ExpiresAt <= clockNow().UnixNano() {
			batch.records = append(batch.records, diskEncodedEntry{key: key, entry: entry})
			continue
		}
		raw, err := encodeDiskRecord(entry, batch.cache.codec)
		if err != nil {
			batch.cache.writeMu.Lock()
			batch.cache.rejectFlushingLocked(key, entry)
			batch.cache.writeMu.Unlock()
			continue
		}
		batch.records = append(batch.records, diskEncodedEntry{key: key, entry: entry, record: raw})
	}
	batch.changes = make(map[string]diskMutation, len(batch.records)+len(batch.expired))
	batch.order = make([]string, 0, len(batch.records))
}
func (batch *diskBatch) commit(tx *bolt.Tx) error {
	dc := batch.cache
	dc.writeMu.Lock()
	batch.plannedBytes, batch.plannedIndex, batch.plannedCount = dc.logicalBytes, dc.indexBytes, len(dc.index)
	// Stable snapshots order pending/flushing hits; heap victims refresh any
	// subsequent hit lazily when that candidate reaches the heap root.
	for i := range batch.records {
		batch.records[i].access = batch.records[i].entry.state.lastAccess
	}
	sort.Slice(batch.records, func(i, j int) bool {
		if batch.records[i].access == batch.records[j].access {
			return batch.records[i].key < batch.records[j].key
		}
		return batch.records[i].access < batch.records[j].access
	})
	dc.writeMu.Unlock()
	if err := batch.deleteExpired(tx); err != nil {
		return err
	}
	for _, record := range batch.records {
		if err := batch.putRecord(tx, record); err != nil {
			return err
		}
	}
	return nil
}
func (batch *diskBatch) deleteExpired(tx *bolt.Tx) error {
	dc := batch.cache
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	for _, e := range batch.expired {
		if current := dc.index[e.key]; current == e && (current.invalid || current.expiresAt <= clockNow().UnixNano()) {
			if e.expiresAt <= clockNow().UnixNano() {
				batch.layerEvictions[LayerForKey(e.key)]++
			}
			if err := batch.remove(tx, e.key); err != nil {
				return err
			}
		}
	}
	return nil
}

// The methods below run with writeMu held. bbolt Put/Delete stage node changes;
// the disk commit itself runs after this mutex is released.
func (batch *diskBatch) existing(key string) (int64, uint64, bool) {
	if change, ok := batch.changes[key]; ok {
		if change.remove {
			return 0, 0, false
		}
		return change.size, change.entry.state.lastAccess, true
	}
	if e := batch.cache.index[key]; e != nil {
		return e.size, e.lastAccess, true
	}
	return 0, 0, false
}
func (batch *diskBatch) remove(tx *bolt.Tx, key string) error {
	originalRemoved := batch.changes[key].originalRemoved || batch.cache.index[key] != nil
	if size, _, ok := batch.existing(key); ok {
		batch.plannedBytes -= size
		batch.plannedIndex -= diskIndexCharge(key)
		batch.plannedCount--
		batch.evictions++
	}
	if err := deleteDiskKey(tx, key); err != nil {
		return err
	}
	batch.changes[key] = diskMutation{remove: true, originalRemoved: originalRemoved}
	return nil
}
func (batch *diskBatch) putRecord(tx *bolt.Tx, record diskEncodedEntry) error {
	dc := batch.cache
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	key, entry, raw := record.key, record.entry, record.record
	if entry.state.cancelled.Load() || raw == nil {
		return batch.remove(tx, key)
	}
	size := int64(len(key)) + int64(len(raw))
	if !dc.fitsEntry(key, size) {
		dc.RejectedWrites.Add(1)
		dc.layerL2[LayerForKey(key)].rejections.Add(1)
		return batch.remove(tx, key)
	}
	if oldSize, _, ok := batch.existing(key); ok {
		batch.plannedBytes -= oldSize
		batch.plannedIndex -= diskIndexCharge(key)
		batch.plannedCount--
	}
	originalRemoved := batch.changes[key].originalRemoved
	batch.changes[key] = diskMutation{size: size, expiry: entry.ExpiresAt, entry: entry, originalRemoved: originalRemoved}
	batch.order = append(batch.order, key)
	batch.plannedBytes += size
	batch.plannedIndex += diskIndexCharge(key)
	batch.plannedCount++
	if batch.victims != nil {
		heap.Push(batch.victims, diskVictim{key: key, access: entry.state.lastAccess})
	}
	if err := batch.evictOverflow(tx); err != nil {
		return err
	}
	if batch.changes[key].remove {
		return nil
	}
	if err := tx.Bucket(dataBucket).Put([]byte(key), raw); err != nil {
		return err
	}
	if err := tx.Bucket(legacyDataBucket).Delete([]byte(key)); err != nil {
		return err
	}
	batch.writes++
	batch.layerEncodedWrites[LayerForKey(key)] += int64(len(raw))
	return nil
}
func (batch *diskBatch) fits() bool {
	dc := batch.cache
	return batch.plannedCount <= dc.maxEntries && batch.plannedIndex <= dc.maxIndexBytes && (dc.maxBytes == 0 || batch.plannedBytes <= dc.maxBytes)
}
func (batch *diskBatch) buildVictims() {
	// Allocate only under pressure. Metadata is bounded by the index plus the
	// bounded batch, and heap operations avoid quadratic oldest-key rescans.
	h := make(diskVictims, 0, len(batch.cache.index)+len(batch.changes))
	for key, e := range batch.cache.index {
		if _, changed := batch.changes[key]; !changed {
			h = append(h, diskVictim{key: key, access: e.lastAccess})
		}
	}
	for key, change := range batch.changes {
		if !change.remove {
			h = append(h, diskVictim{key: key, access: change.entry.state.lastAccess})
		}
	}
	heap.Init(&h)
	batch.victims = &h
}
func (batch *diskBatch) evictOverflow(tx *bolt.Tx) error {
	if batch.fits() {
		return nil
	}
	if batch.victims == nil {
		batch.buildVictims()
	}
	for !batch.fits() {
		if batch.victims.Len() == 0 {
			return boltErrors.ErrInvalid
		}
		victim := heap.Pop(batch.victims).(diskVictim)
		_, access, ok := batch.existing(victim.key)
		if !ok {
			continue
		}
		if access != victim.access {
			victim.access = access
			heap.Push(batch.victims, victim)
			continue
		}
		// A queued value can lose to entries touched after its admission.
		// Reject its staged value and also count the original committed
		// record if this removal deletes one not already removed by the batch.
		change, staged := batch.changes[victim.key]
		if staged {
			batch.layerRejections[LayerForKey(victim.key)]++
		}
		if batch.cache.index[victim.key] != nil && !change.originalRemoved {
			batch.layerEvictions[LayerForKey(victim.key)]++
		}
		if err := batch.remove(tx, victim.key); err != nil {
			return err
		}
	}
	return nil
}
func (batch *diskBatch) apply() {
	dc := batch.cache
	for key := range batch.changes {
		dc.removeIndex(key)
	}
	nodes := make([]*diskLRUEntry, 0, len(batch.order))
	for _, key := range batch.order {
		change := batch.changes[key]
		if change.remove {
			continue
		}
		node := dc.newIndexEntry(key, change.size, change.expiry, change.entry.state.lastAccess)
		if change.entry.state.cancelled.Load() {
			node.invalid = true
			dc.dirty = true
		}
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].lastAccess > nodes[j].lastAccess })
	dc.mergeIndexEntries(nodes)
}
func (batch *diskBatch) restore() {
	dc := batch.cache
	for key, entry := range batch.pending {
		if _, newer := dc.writeBuf[key]; !newer && !entry.state.cancelled.Load() {
			dc.writeBuf[key] = entry
		}
	}
	if len(batch.expired) > 0 {
		dc.dirty = true
	}
}
func (batch *diskBatch) finish(err error) {
	dc := batch.cache
	dc.writeMu.Lock()
	if err == nil {
		batch.apply()
		for l := LayerUnknown; l < LayerCount; l++ {
			dc.layerL2[l].evictions.Add(batch.layerEvictions[l])
			dc.layerL2[l].rejections.Add(batch.layerRejections[l])
			dc.layerEncodedWriteBytes[l].Add(batch.layerEncodedWrites[l])
		}
	} else {
		batch.restore()
	}
	dc.flushing = nil
	dc.flushingBytes = 0
	dc.bufferBytes = 0
	dc.layerQueueBytes = [LayerCount]int64{}
	for key, entry := range dc.writeBuf {
		dc.bufferBytes += diskQueueCharge(key, entry.Value)
		dc.layerQueueBytes[LayerForKey(key)] += diskQueueCharge(key, entry.Value)
	}
	more := dc.dirty
	dc.writeMu.Unlock()
	dc.lastFlushErr = err
	if err != nil {
		dc.log.Warn("disk cache flush failed", "error", err)
	} else {
		dc.Writes.Add(batch.writes)
		dc.Evictions.Add(batch.evictions)
	}
	dc.FlushCount.Add(1)
	if err == nil && more {
		dc.requestFlush()
	}
}
func (dc *DiskCache) rejectFlushingLocked(key string, entry diskEntry) {
	entry.state.cancelled.Store(true)
	dc.RejectedWrites.Add(1)
	dc.layerL2[LayerForKey(key)].rejections.Add(1)
	if item := dc.index[key]; item != nil {
		item.invalid = true
		dc.dirty = true
	}
	dc.requestFlush()
}
