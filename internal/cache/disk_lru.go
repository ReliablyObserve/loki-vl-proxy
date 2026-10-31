package cache

import (
	"encoding/binary"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"io"
)

// Intrusive links avoid an extra list.Element allocation per index entry.
// Charge128 conservatively covers this node, map slots and key bookkeeping;
// this is an accounting bound rather than a claim about allocator RSS.
type diskLRUEntry struct {
	key        string
	size       int64
	expiresAt  int64
	lastAccess uint64
	generation uint64
	invalid    bool
	prev, next *diskLRUEntry
}

func diskIndexCharge(key string) int64 { return 128 + int64(len(key)) }
func (dc *DiskCache) touch(e *diskLRUEntry) {
	if dc.newest == e {
		return
	}
	if e.prev != nil {
		e.prev.next = e.next
	} else if dc.newest == e {
		dc.newest = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if dc.oldest == e {
		dc.oldest = e.prev
	}
	e.prev = nil
	e.next = dc.newest
	if dc.newest != nil {
		dc.newest.prev = e
	}
	dc.newest = e
	if dc.oldest == nil {
		dc.oldest = e
	}
}
func (dc *DiskCache) removeIndex(key string) {
	e := dc.index[key]
	if e == nil {
		return
	}
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		dc.newest = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		dc.oldest = e.prev
	}
	delete(dc.index, key)
	dc.indexBytes -= diskIndexCharge(key)
	dc.logicalBytes -= e.size
	layer := LayerForKey(key)
	dc.layerL2[layer].entries.Add(-1)
	dc.layerL2[layer].bytes.Add(-e.size)
	dc.layerIndexBytes[layer] -= diskIndexCharge(key)
	e.prev, e.next = nil, nil
}

func (dc *DiskCache) newIndexEntry(key string, size, expiry int64, access uint64) *diskLRUEntry {
	dc.removeIndex(key)
	dc.sequence++
	if access == 0 {
		access = dc.sequence
	}
	e := &diskLRUEntry{key: key, size: size, expiresAt: expiry, generation: dc.sequence, lastAccess: access}
	dc.index[key] = e
	dc.indexBytes += diskIndexCharge(key)
	dc.logicalBytes += size
	layer := LayerForKey(key)
	dc.layerL2[layer].entries.Add(1)
	dc.layerL2[layer].bytes.Add(size)
	dc.layerIndexBytes[layer] += diskIndexCharge(key)
	if dc.nextExpiry == 0 || expiry < dc.nextExpiry {
		dc.nextExpiry = expiry
	}
	return e
}
func (dc *DiskCache) addIndex(key string, size, expiry int64) {
	dc.touch(dc.newIndexEntry(key, size, expiry, 0))
}

// New nodes arrive sorted by descending access; merge them with the existing
// sorted LRU in one pass, preserving hits that occurred during a disk commit.
func (dc *DiskCache) mergeIndexEntries(nodes []*diskLRUEntry) {
	position := dc.newest
	for _, e := range nodes {
		for position != nil && position.lastAccess > e.lastAccess {
			position = position.next
		}
		if position == nil {
			e.prev = dc.oldest
			if dc.oldest != nil {
				dc.oldest.next = e
			} else {
				dc.newest = e
			}
			dc.oldest = e
		} else {
			e.prev = position.prev
			e.next = position
			if position.prev != nil {
				position.prev.next = e
			} else {
				dc.newest = e
			}
			position.prev = e
		}
	}
}
func (dc *DiskCache) fitsEntry(key string, size int64) bool {
	return diskIndexCharge(key) <= dc.maxIndexBytes && (dc.maxBytes == 0 || size <= dc.maxBytes)
}
func (dc *DiskCache) fitsIndex(key string, size int64) bool {
	return len(dc.index) < dc.maxEntries && dc.indexBytes+diskIndexCharge(key) <= dc.maxIndexBytes && (dc.maxBytes == 0 || dc.logicalBytes+size <= dc.maxBytes)
}
func deleteDiskKey(tx *bolt.Tx, key string) error {
	for _, name := range [][]byte{dataBucket, legacyDataBucket} {
		if b := tx.Bucket(name); b != nil {
			if err := b.Delete([]byte(key)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Startup order is deterministic bucket/key order, not persisted recency. Scan
// with bounded in-memory metadata, pruning overflow and expired/malformed frames.
func (dc *DiskCache) rebuildIndex() error {
	return dc.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{dataBucket, legacyDataBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		for _, name := range [][]byte{dataBucket, legacyDataBucket} {
			b := tx.Bucket(name)
			native := string(name) == string(dataBucket)
			cursor := b.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				// The native bucket always wins, including malformed native
				// data: delete a shadow legacy copy before validating native.
				if native {
					if err := tx.Bucket(legacyDataBucket).Delete(key); err != nil {
						return err
					}
				}
				var expiry int64
				err := dc.validateStoredDiskRecord(value, native)
				if err == nil {
					if native {
						expiry, err = nativeDiskRecordExpiry(value)
					} else {
						expiry, err = legacyDiskExpiry(value, dc.compression)
					}
				}
				size := int64(len(key)) + int64(len(value))
				k := string(key)
				if err != nil || expiry <= clockNow().UnixNano() || !dc.fitsEntry(k, size) || !dc.fitsIndex(k, size) {
					if err := cursor.Delete(); err != nil {
						return err
					}
					dc.Evictions.Add(1)
					if err == nil {
						dc.layerL2[LayerForKey(k)].evictions.Add(1)
					}
					continue
				}
				dc.addIndex(k, size, expiry)
			}
		}
		return nil
	})
}

// Select expiry/deletion work from RAM; no gzip initialization, full bucket
// statistics or writable transaction is needed when the cache is idle/fresh.
// The index is bounded and at most1024 deletions enter one transaction.
func (dc *DiskCache) expiryCandidatesLocked() []*diskLRUEntry {
	now := clockNow().UnixNano()
	if !dc.dirty && (dc.nextExpiry == 0 || dc.nextExpiry > now) {
		return nil
	}
	dc.dirty = false
	dc.nextExpiry = 0
	var work []*diskLRUEntry
	for _, e := range dc.index {
		if e.invalid || e.expiresAt <= now {
			if len(work) < 1024 {
				work = append(work, e)
			} else {
				dc.dirty = true
			}
		}
		if dc.nextExpiry == 0 || e.expiresAt < dc.nextExpiry {
			dc.nextExpiry = e.expiresAt
		}
	}
	return work
}

// The bucket identifies legacy data, so magic-like expiry bytes must never
// trigger native record parsing here.
func legacyDiskExpiry(raw []byte, compressed bool) (int64, error) {
	if !compressed {
		if len(raw) < 8 {
			return 0, fmt.Errorf("truncated legacy disk cache expiry")
		}
		return int64(binary.BigEndian.Uint64(raw[:8])), nil
	}
	reader, input, err := acquireDiskGzip(raw)
	if err != nil {
		return 0, err
	}
	defer releaseDiskGzip(reader, input)
	var expiry [8]byte
	if _, err = io.ReadFull(reader, expiry[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(expiry[:])), nil
}

// Check storage bounds and the native header before copying an mmap-backed
// value. Native writers fall back to raw whenever compression does not shrink
// data, so even a falsely small decoded header cannot justify a huge payload.
func (dc *DiskCache) validateStoredDiskRecord(raw []byte, native bool) error {
	if native {
		if len(raw) > diskRecordHeaderSize+dc.maxEntryBytes || len(raw) < 4 || string(raw[:4]) != diskRecordMagic {
			return fmt.Errorf("invalid native disk cache storage size or magic")
		}
		codec, size, _, err := diskRecordHeader(raw, dc.maxEntryBytes)
		if err != nil {
			return err
		}
		payloadSize := len(raw) - diskRecordHeaderSize
		if payloadSize > size || (codec == diskCodecRaw && payloadSize != size) {
			return fmt.Errorf("disk cache encoded length exceeds declared value")
		}
		return nil
	}
	limit := dc.maxEntryBytes + 8
	if dc.compression {
		limit = 2*limit + 1024
	}
	if len(raw) > limit || len(raw) < 8 {
		return fmt.Errorf("legacy disk cache encoded size exceeds limit")
	}
	return nil
}
