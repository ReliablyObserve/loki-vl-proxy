package cache

import (
	"runtime"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// Batched disk reads of the start-up warm-up. A batch takes the disk lock once
// to see what the write queue and the index hold, reads and decodes its records
// in one read transaction with a bounded pool of goroutines, and checks under
// the memory lock (restoreFence) that nothing fresher was written meanwhile.
// The reads count no disk hit or miss and leave the disk recency as the start-up
// rebuild ordered it from the hand-over's ranks.

// diskRestoreRead is one ranked key of a batch.
type diskRestoreRead struct {
	restoreEntry
	// queued is the write queue's state when the value came from it (it is
	// fresher than any disk record); generation is the index entry's when the
	// value is read from disk.
	queued     *diskEntryState
	generation uint64
	// raw aliases the read transaction's pages; valid only inside it.
	raw    []byte
	native bool
}

// planRestore resolves each key against the write queue and the index under one
// hold of the disk lock. Memory-only hand-over records are only in their bucket.
func (dc *DiskCache) planRestore(reads []*diskRestoreRead) {
	now := clockNow().UnixNano()
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	for _, r := range reads {
		switch {
		case r.skip != restoreKeep:
		case dc.closed || dc.closing:
			r.skip = RestoreSkipUnreadable
		case r.memOnly:
		default:
			entry, ok := dc.writeBuf[r.key]
			if !ok {
				entry, ok = dc.flushing[r.key]
			}
			switch {
			case ok && (entry.state == nil || entry.state.cancelled.Load()):
				r.skip = RestoreSkipUnreadable
			case ok && entry.ExpiresAt <= now:
				r.skip = RestoreSkipExpired
			case ok:
				r.value, r.expiry, r.queued = entry.Value, entry.ExpiresAt, entry.state
			default:
				item := dc.index[r.key]
				if item == nil || item.invalid {
					r.skip = RestoreSkipUnreadable
					continue
				}
				r.generation = item.generation
			}
		}
	}
}

// restoreLimits bounds what one batch may read.
type restoreLimits struct {
	// budget and room are the decoded bytes (keys included) left of the
	// warm-up's byte budget and free in the memory tier.
	budget, room int64
	// maxEntry is the largest entry the memory tier takes.
	maxEntry int64
	bytes    int64
	workers  int
}

// locate finds r's record in tx and returns the memory it will take.
// key is a buffer reused between calls: Get keeps no reference to it.
func (dc *DiskCache) locate(tx *bolt.Tx, r *diskRestoreRead, key *[]byte) (int64, bool) {
	if r.memOnly {
		b := tx.Bucket(handoverBucket)
		if b == nil {
			return 0, false
		}
		bucketKey := handoverBucketKey(handoverClassMemory, r.key)
		v := b.Get(bucketKey)
		if v == nil {
			return 0, false
		}
		if _, _, _, _, _, err := decodeHandoverRecordHeader(bucketKey, v); err != nil {
			return 0, false
		}
		r.raw, r.native = v[handoverRecordHeaderSize:], true
	} else {
		*key = append((*key)[:0], r.key...)
		if b := tx.Bucket(dataBucket); b != nil {
			if v := b.Get(*key); v != nil {
				r.raw, r.native = v, true
			}
		}
		if r.raw == nil {
			if b := tx.Bucket(legacyDataBucket); b != nil {
				r.raw = b.Get(*key)
			}
		}
		if r.raw == nil || dc.validateStoredDiskRecord(r.raw, r.native) != nil {
			return 0, false
		}
	}
	// A native record's header holds the decoded length; a legacy record is
	// sized by what is stored.
	size := int64(len(r.raw))
	if r.native {
		_, decoded, _, err := diskRecordHeader(r.raw, dc.maxEntryBytes)
		if err != nil {
			return 0, false
		}
		size = int64(decoded)
	}
	return size + int64(len(r.key)), true
}

// readRestoreBatch reads reads in order until the batch's byte bound or the
// budget ends it, and decodes what it took in one read transaction, with at
// most limits.workers goroutines (all joined before it returns). It returns how
// many of reads it consumed, at least one; the rest belong to the next batch.
// The decoded values are owned memory: nothing refers to the file afterwards.
func (dc *DiskCache) readRestoreBatch(reads []*diskRestoreRead, limits restoreLimits) int {
	consumed := 0
	var failed []*diskRestoreRead
	err := dc.db.View(func(tx *bolt.Tx) error {
		var planned int64
		work := make([]*diskRestoreRead, 0, len(reads))
		var key []byte
		for _, r := range reads {
			if r.skip != restoreKeep {
				consumed++
				continue
			}
			var size int64
			if r.queued != nil {
				size = int64(entrySize(r.key, r.value))
			} else {
				var ok bool
				if size, ok = dc.locate(tx, r, &key); !ok {
					r.skip = RestoreSkipUnreadable
					failed = append(failed, r)
					consumed++
					continue
				}
			}
			if size > limits.maxEntry {
				r.skip = RestoreSkipTooLarge
				consumed++
				continue
			}
			if planned > 0 && (planned+size > limits.bytes || planned+size > min(limits.budget, limits.room)) {
				r.raw = nil
				break
			}
			if size > limits.budget {
				r.skip = RestoreSkipOverBudget
				consumed++
				continue
			}
			if size > limits.room {
				r.skip = RestoreSkipMemoryFull
				consumed++
				continue
			}
			planned += size
			consumed++
			if r.queued == nil {
				work = append(work, r)
			}
		}
		decodeRestoreReads(work, limits.workers, dc.maxEntryBytes, dc.compression)
		for _, r := range work {
			if !r.memOnly {
				dc.layerEncodedReadBytes[LayerForKey(r.key)].Add(int64(len(r.raw)))
			}
			r.raw = nil
			if r.skip != restoreKeep {
				failed = append(failed, r)
			}
		}
		return nil
	})
	if err != nil {
		dc.log.Warn("cache restore read failed", "error", err)
		for _, r := range reads {
			r.skip = max(r.skip, RestoreSkipUnreadable)
		}
		return len(reads)
	}
	dc.invalidateRestored(failed)
	return consumed
}

// decodeRestoreReads decodes the located records, sharing them among at most
// workers goroutines including the caller's. Reading the mapped pages happens
// here too, so a cold file is read with that much parallelism.
func decodeRestoreReads(work []*diskRestoreRead, workers, maxDecoded int, legacyCompression bool) {
	now := clockNow().UnixNano()
	decode := func(r *diskRestoreRead) {
		var decoded diskEntry
		var err error
		if r.native {
			decoded, err = decodeNativeDiskRecordLimit(r.raw, maxDecoded)
		} else {
			decoded, err = decodeLegacyDiskRecord(r.raw, legacyCompression, maxDecoded)
		}
		switch {
		case err != nil:
			r.skip = RestoreSkipUnreadable
		case decoded.ExpiresAt <= now:
			r.skip = RestoreSkipExpired
		default:
			r.value, r.expiry = decoded.Value, decoded.ExpiresAt
		}
	}
	var next atomic.Int64
	run := func() {
		for {
			i := int(next.Add(1) - 1)
			if i >= len(work) {
				return
			}
			decode(work[i])
		}
	}
	var wg sync.WaitGroup
	for n := 1; n < min(workers, len(work)); n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run()
		}()
	}
	run()
	wg.Wait()
}

func restoreWorkers() int { return min(runtime.GOMAXPROCS(0), restoreMaxWorkers) }

// invalidateRestored drops disk records the warm-up found corrupt or expired,
// as a lookup would, unless they were rewritten since the batch was planned.
func (dc *DiskCache) invalidateRestored(failed []*diskRestoreRead) {
	dirty := false
	dc.writeMu.Lock()
	for _, r := range failed {
		if r.memOnly || r.queued != nil {
			continue
		}
		if current := dc.index[r.key]; current != nil && current.generation == r.generation {
			current.invalid, dc.dirty, dirty = true, true, true
		}
	}
	dc.writeMu.Unlock()
	if dirty {
		dc.requestFlush()
	}
}

// restoreFence marks the entries of a batch that something fresher replaced
// since they were planned: a queued write, a rewrite or a removal of the disk
// record. It runs under the memory lock, which every cache write holds while it
// queues its disk write (lock order: memory, then disk).
func (dc *DiskCache) restoreFence(reads []*diskRestoreRead) {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	for _, r := range reads {
		if r.skip != restoreKeep || r.memOnly {
			continue
		}
		pending, buffered := dc.writeBuf[r.key]
		flushing, inFlight := dc.flushing[r.key]
		current := false
		switch {
		case r.queued != nil:
			current = !r.queued.cancelled.Load() &&
				((buffered && pending.state == r.queued) || (!buffered && inFlight && flushing.state == r.queued))
		case !buffered && (!inFlight || (flushing.state != nil && flushing.state.cancelled.Load())):
			item := dc.index[r.key]
			current = item != nil && !item.invalid && item.generation == r.generation
		}
		if !current {
			r.skip = RestoreSkipSuperseded
		}
	}
}

// storedSizes returns each key's stored bytes (key and encoded value) from the
// index, 0 when it does not hold it, taking the lock once per batch.
func (dc *DiskCache) storedSizes(keys []handoverItem) []int64 {
	out := make([]int64, len(keys))
	for start := 0; start < len(keys); start += restoreBatchKeys {
		dc.writeMu.Lock()
		for i := start; i < min(start+restoreBatchKeys, len(keys)); i++ {
			switch item := keys[i]; {
			case item.memory:
				out[i] = int64(item.size + len(item.key))
			default:
				if e := dc.index[item.key]; e != nil {
					out[i] = e.size - diskRecordHeaderSize
				}
			}
		}
		dc.writeMu.Unlock()
	}
	return out
}
