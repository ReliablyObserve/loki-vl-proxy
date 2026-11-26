package cache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
)

// Restart hand-over records. A graceful shutdown (Cache.FlushHandover) leaves,
// in a bucket of its own in the disk cache's bbolt file, the memory tier's
// recency order and hit counts and the memory-only values that may outlive the
// process; the next process (Cache.WarmFromHandover) loads them into its memory
// tier before it reports ready.
//
// The bucket is never read by a lookup, a stale lookup, a peer read or the
// index rebuild: its values reach memory only through the warm-up, which keeps
// each one memory-only. A manifest written as the very last transaction before
// the file is closed makes the records valid. It holds the transaction id it
// was written in, so any later write to the file (an older binary that does
// not know the bucket, a purge, a process that crashed after start) leaves a
// manifest whose id is not the file's current one, and the next start drops
// every record.
var handoverBucket = []byte("cache_handover_v1")

const (
	handoverManifestKey = "\x00manifest"
	handoverMagic       = "LVHO"
	handoverVersion     = 1
	// handoverClassDisk ranks a record of the data bucket; handoverClassMemory
	// holds a memory-only value encoded like a disk record (codec, expiry, CRC).
	handoverClassDisk   byte = 'd'
	handoverClassMemory byte = 'm'
	// Record value: version[1] class[1] reserved[2] rank[4] hits[4] CRC32C[4],
	// then for a memory record the encoded value. The CRC covers the first 12
	// bytes and the key; a memory value carries its own record CRC, checked when
	// the warm-up decodes it, so loading the index never reads values.
	handoverRecordHeaderSize = 16
	// Manifest: magic[4] version[1] flags[1] reserved[2] txid[8] flushedAt[8]
	// duration[8] ranked[4] diskWrites[4] memoryEntries[4] reserved[4]
	// memoryBytes[8] scopeLength[2] scope CRC32C[4].
	handoverManifestFixedSize = 58
	handoverMaxScopeBytes     = 1024
	handoverFlagComplete      = 1
	// One hand-over transaction holds at most this many records or bytes, so
	// the flush checks its deadline between bounded commits.
	handoverChunkRecords = 512
	handoverChunkBytes   = 4 << 20
)

// HandoverManifest describes the hand-over a graceful shutdown left.
type HandoverManifest struct {
	// Scope is the writer's build and configuration identity; memory-only
	// values are restored only by a process with the same scope.
	Scope string
	// FlushedAt is the cache clock (UnixNano) when the flush started. A
	// process whose clock reads earlier restores nothing.
	FlushedAt int64
	Duration  time.Duration
	// Complete is false when the flush stopped at its deadline: the records
	// written are whole, only fewer.
	Complete      bool
	Ranked        int
	DiskWrites    int
	MemoryEntries int
	MemoryBytes   int64
	txID          uint64
}

type handoverItem struct {
	key    string
	rank   uint32
	hits   uint32
	memory bool
	// size is a memory record's decoded value length (0 for a disk rank).
	size int
}

type handoverState struct {
	manifest HandoverManifest
	items    []handoverItem // ascending rank: hottest first
}

type handoverRecord struct {
	key     string
	class   byte
	rank    uint32
	hits    uint32
	payload []byte
	size    int // a memory record's decoded value length
}

var errHandoverSuperseded = errors.New("cache hand-over superseded by a purge or close")

// handoverKeyFits reports whether a key with its class byte fits a bbolt key.
func handoverKeyFits(key string) bool { return key != "" && len(key)+1 <= bolt.MaxKeySize }

func handoverBucketKey(class byte, key string) []byte {
	out := make([]byte, 0, 1+len(key))
	out = append(out, class)
	return append(out, key...)
}

func encodeHandoverRecord(r handoverRecord) []byte {
	out := make([]byte, handoverRecordHeaderSize+len(r.payload))
	out[0], out[1] = handoverVersion, r.class
	binary.BigEndian.PutUint32(out[4:8], r.rank)
	binary.BigEndian.PutUint32(out[8:12], r.hits)
	sum := crc32.Update(0, diskRecordCRC, out[:12])
	sum = crc32.Update(sum, diskRecordCRC, []byte(r.key))
	binary.BigEndian.PutUint32(out[12:16], sum)
	copy(out[handoverRecordHeaderSize:], r.payload)
	return out
}

// decodeHandoverRecordHeader validates a record's header against its bucket
// key and returns its class, rank, hits and payload (aliasing raw).
func decodeHandoverRecordHeader(bucketKey, raw []byte) (byte, string, uint32, uint32, []byte, error) {
	if len(bucketKey) < 2 || len(raw) < handoverRecordHeaderSize {
		return 0, "", 0, 0, nil, fmt.Errorf("truncated cache hand-over record")
	}
	class := bucketKey[0]
	if raw[0] != handoverVersion || raw[1] != class || raw[2] != 0 || raw[3] != 0 {
		return 0, "", 0, 0, nil, fmt.Errorf("unsupported cache hand-over record version or class")
	}
	if class != handoverClassDisk && class != handoverClassMemory {
		return 0, "", 0, 0, nil, fmt.Errorf("unknown cache hand-over record class")
	}
	key := bucketKey[1:]
	sum := crc32.Update(0, diskRecordCRC, raw[:12])
	sum = crc32.Update(sum, diskRecordCRC, key)
	if sum != binary.BigEndian.Uint32(raw[12:16]) {
		return 0, "", 0, 0, nil, fmt.Errorf("cache hand-over record checksum mismatch")
	}
	payload := raw[handoverRecordHeaderSize:]
	if class == handoverClassDisk && len(payload) != 0 {
		return 0, "", 0, 0, nil, fmt.Errorf("cache hand-over disk rank carries a value")
	}
	return class, string(key), binary.BigEndian.Uint32(raw[4:8]), binary.BigEndian.Uint32(raw[8:12]), payload, nil
}

func encodeHandoverManifest(m HandoverManifest) ([]byte, error) {
	if len(m.Scope) > handoverMaxScopeBytes {
		return nil, fmt.Errorf("cache hand-over scope exceeds %d bytes", handoverMaxScopeBytes)
	}
	out := make([]byte, handoverManifestFixedSize+len(m.Scope)+4)
	copy(out, handoverMagic)
	out[4] = handoverVersion
	if m.Complete {
		out[5] = handoverFlagComplete
	}
	binary.BigEndian.PutUint64(out[8:16], m.txID)
	binary.BigEndian.PutUint64(out[16:24], uint64(m.FlushedAt))
	binary.BigEndian.PutUint64(out[24:32], uint64(m.Duration))
	binary.BigEndian.PutUint32(out[32:36], uint32(m.Ranked))
	binary.BigEndian.PutUint32(out[36:40], uint32(m.DiskWrites))
	binary.BigEndian.PutUint32(out[40:44], uint32(m.MemoryEntries))
	binary.BigEndian.PutUint64(out[48:56], uint64(m.MemoryBytes))
	binary.BigEndian.PutUint16(out[56:58], uint16(len(m.Scope)))
	copy(out[58:], m.Scope)
	end := handoverManifestFixedSize + len(m.Scope)
	binary.BigEndian.PutUint32(out[end:], crc32.Update(0, diskRecordCRC, out[:end]))
	return out, nil
}

func decodeHandoverManifest(raw []byte) (HandoverManifest, error) {
	if len(raw) < handoverManifestFixedSize+4 || string(raw[:4]) != handoverMagic {
		return HandoverManifest{}, fmt.Errorf("invalid cache hand-over manifest")
	}
	if raw[4] != handoverVersion || raw[5]&^handoverFlagComplete != 0 || raw[6] != 0 || raw[7] != 0 {
		return HandoverManifest{}, fmt.Errorf("unsupported cache hand-over manifest version or flags")
	}
	scopeLen := int(binary.BigEndian.Uint16(raw[56:58]))
	if scopeLen > handoverMaxScopeBytes || len(raw) != handoverManifestFixedSize+scopeLen+4 {
		return HandoverManifest{}, fmt.Errorf("cache hand-over manifest length mismatch")
	}
	end := handoverManifestFixedSize + scopeLen
	if crc32.Update(0, diskRecordCRC, raw[:end]) != binary.BigEndian.Uint32(raw[end:]) {
		return HandoverManifest{}, fmt.Errorf("cache hand-over manifest checksum mismatch")
	}
	m := HandoverManifest{
		txID:          binary.BigEndian.Uint64(raw[8:16]),
		FlushedAt:     int64(binary.BigEndian.Uint64(raw[16:24])),
		Duration:      time.Duration(binary.BigEndian.Uint64(raw[24:32])),
		Complete:      raw[5]&handoverFlagComplete != 0,
		Ranked:        int(binary.BigEndian.Uint32(raw[32:36])),
		DiskWrites:    int(binary.BigEndian.Uint32(raw[36:40])),
		MemoryEntries: int(binary.BigEndian.Uint32(raw[40:44])),
		MemoryBytes:   int64(binary.BigEndian.Uint64(raw[48:56])),
		Scope:         string(raw[58:end]),
	}
	if m.Duration < 0 || m.MemoryBytes < 0 {
		return HandoverManifest{}, fmt.Errorf("cache hand-over manifest holds a negative value")
	}
	return m, nil
}

// loadHandoverLocked reads the hand-over inside the start-up index rebuild,
// before any other write: a manifest is valid only when the file's last
// committed transaction is the one that wrote it (this writable transaction
// is the next id). It returns nil when there is no valid hand-over, and
// whether the bucket must be deleted. Records are bounded by the disk index
// budgets (entry count and key bytes), so a damaged or crafted file cannot
// make the start-up hold more than the index could.
func (dc *DiskCache) loadHandoverLocked(tx *bolt.Tx) (*handoverState, bool) {
	b := tx.Bucket(handoverBucket)
	if b == nil {
		return nil, false
	}
	raw := b.Get([]byte(handoverManifestKey))
	if raw == nil {
		return nil, true
	}
	manifest, err := decodeHandoverManifest(raw)
	if err != nil || tx.ID() < 1 || manifest.txID != uint64(tx.ID()-1) {
		if err != nil {
			dc.log.Warn("cache hand-over manifest rejected", "error", err)
		} else {
			dc.log.Info("cache hand-over dropped: the disk cache was written after the shutdown flush")
		}
		return nil, true
	}
	dc.previousFlush, dc.previousFlushFound = manifest, true
	state := &handoverState{manifest: manifest}
	var keyBytes int64
	cursor := b.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if string(k) == handoverManifestKey {
			continue
		}
		class, key, rank, hits, payload, err := decodeHandoverRecordHeader(k, v)
		if err != nil {
			dc.handoverCorrupt++
			continue
		}
		if len(state.items) >= dc.maxEntries || keyBytes+diskIndexCharge(key) > dc.maxIndexBytes {
			dc.handoverOverflow++
			break
		}
		item := handoverItem{key: key, rank: rank, hits: hits, memory: class == handoverClassMemory}
		if item.memory {
			_, size, _, err := diskRecordHeader(payload, dc.maxEntryBytes)
			if err != nil {
				dc.handoverCorrupt++
				continue
			}
			item.size = size
		}
		keyBytes += diskIndexCharge(key)
		state.items = append(state.items, item)
	}
	if len(state.items) == 0 {
		return nil, true
	}
	sort.SliceStable(state.items, func(i, j int) bool { return state.items[i].rank < state.items[j].rank })
	return state, false
}

// applyHandoverRanksLocked moves the ranked records of the data bucket to the
// recent end of the disk LRU, coldest first, so the hottest is the newest and
// the last to be evicted: the recency order the memory tier had survives the
// restart on disk too. Record recency and list order stay consistent.
func (dc *DiskCache) applyHandoverRanksLocked(state *handoverState) {
	for i := len(state.items) - 1; i >= 0; i-- {
		item := state.items[i]
		if item.memory {
			continue
		}
		e := dc.index[item.key]
		if e == nil || e.invalid {
			continue
		}
		dc.sequence++
		e.lastAccess = dc.sequence
		dc.touch(e)
	}
}

// takeHandover hands the loaded hand-over to the warm-up, once.
func (dc *DiskCache) takeHandover() *handoverState {
	if dc == nil {
		return nil
	}
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	state := dc.handover
	dc.handover = nil
	return state
}

// PreviousFlush returns the manifest of the shutdown flush this process
// started from, and whether one was found and valid.
func (dc *DiskCache) PreviousFlush() (HandoverManifest, bool) {
	if dc == nil {
		return HandoverManifest{}, false
	}
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	return dc.previousFlush, dc.previousFlushFound
}

// DropHandover deletes the hand-over records (after the warm-up read them, or
// when the warm-up is off).
func (dc *DiskCache) DropHandover() {
	if dc == nil {
		return
	}
	dc.flushMu.Lock()
	defer dc.flushMu.Unlock()
	dc.writeMu.Lock()
	closed := dc.closed
	dc.handover = nil
	dc.writeMu.Unlock()
	if closed {
		return
	}
	if err := dc.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(handoverBucket); err != nil && err != boltErrors.ErrBucketNotFound {
			return err
		}
		return nil
	}); err != nil {
		dc.log.Warn("cache hand-over cleanup failed", "error", err)
	}
}

// beginHandover starts a shutdown flush: it clears any earlier records and any
// manifest waiting for Close, and returns the generation later writes of this
// flush must still match (a purge or close moves it).
func (dc *DiskCache) beginHandover() (uint64, error) {
	dc.flushMu.Lock()
	defer dc.flushMu.Unlock()
	dc.writeMu.Lock()
	if dc.closed || dc.closing {
		dc.writeMu.Unlock()
		return 0, errHandoverSuperseded
	}
	dc.handoverGen++
	gen := dc.handoverGen
	dc.pendingManifest = nil
	dc.handover = nil
	dc.writeMu.Unlock()
	err := dc.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(handoverBucket); err != nil && err != boltErrors.ErrBucketNotFound {
			return err
		}
		_, err := tx.CreateBucket(handoverBucket)
		return err
	})
	return gen, err
}

// putHandoverRecords commits one bounded chunk of hand-over records.
func (dc *DiskCache) putHandoverRecords(gen uint64, records []handoverRecord) error {
	dc.flushMu.Lock()
	defer dc.flushMu.Unlock()
	dc.writeMu.Lock()
	current := !dc.closed && !dc.closing && dc.handoverGen == gen
	dc.writeMu.Unlock()
	if !current {
		return errHandoverSuperseded
	}
	return dc.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(handoverBucket)
		if b == nil {
			return errHandoverSuperseded
		}
		for _, r := range records {
			if err := b.Put(handoverBucketKey(r.class, r.key), encodeHandoverRecord(r)); err != nil {
				return err
			}
		}
		return nil
	})
}

// setHandoverManifest leaves the manifest for Close, which writes it as the
// file's last transaction.
func (dc *DiskCache) setHandoverManifest(gen uint64, m HandoverManifest) bool {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	if dc.closed || dc.closing || dc.handoverGen != gen {
		return false
	}
	dc.pendingManifest = &m
	return true
}

// writeHandoverManifestLocked runs in Close after the last queue flush, with
// flushMu held. A failed last flush leaves no manifest: the records may then
// name values the file does not hold.
func (dc *DiskCache) writeHandoverManifestLocked() {
	dc.writeMu.Lock()
	m := dc.pendingManifest
	dc.pendingManifest = nil
	dc.writeMu.Unlock()
	if m == nil || dc.lastFlushErr != nil {
		return
	}
	err := dc.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(handoverBucket)
		if b == nil {
			return errHandoverSuperseded
		}
		manifest := *m
		manifest.txID = uint64(tx.ID())
		raw, err := encodeHandoverManifest(manifest)
		if err != nil {
			return err
		}
		return b.Put([]byte(handoverManifestKey), raw)
	})
	if err != nil {
		dc.log.Warn("cache hand-over manifest write failed", "error", err)
	}
}

// planHandover gives the memory tier's disk-eligible keys (hottest first) the
// disk recency of their rank: a key the disk holds (committed or queued) is
// touched, coldest first, so the hottest ends newest; a key it lacks gets the
// access its write must carry. Under capacity pressure during the flush the
// disk then evicts the coldest records first.
func (dc *DiskCache) planHandover(keys []string) (present []bool, access []uint64) {
	present = make([]bool, len(keys))
	access = make([]uint64, len(keys))
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		dc.sequence++
		access[i] = dc.sequence
		if entry, ok := dc.writeBuf[key]; ok && entry.state != nil && !entry.state.cancelled.Load() {
			entry.state.lastAccess = dc.sequence
			present[i] = true
		} else if entry, ok := dc.flushing[key]; ok && entry.state != nil && !entry.state.cancelled.Load() {
			entry.state.lastAccess = dc.sequence
			present[i] = true
		}
		if e := dc.index[key]; e != nil && !e.invalid {
			e.lastAccess = dc.sequence
			dc.touch(e)
			present[i] = true
		}
	}
	return present, access
}

// queueRoom reports whether the write queue admits key and value now.
func (dc *DiskCache) queueRoom(key string, value []byte) bool {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	charge := diskQueueCharge(key, value)
	used := dc.bufferBytes
	if old, ok := dc.writeBuf[key]; ok {
		used -= diskQueueCharge(key, old.Value)
	}
	return charge <= dc.maxBufferBytes && used <= dc.maxBufferBytes-charge
}

// holds reports whether the disk holds a current value for key (committed and
// valid, or queued).
func (dc *DiskCache) holds(key string) bool {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	if entry, ok := dc.writeBuf[key]; ok {
		return entry.state == nil || !entry.state.cancelled.Load()
	}
	if entry, ok := dc.flushing[key]; ok && (entry.state == nil || !entry.state.cancelled.Load()) {
		return true
	}
	e := dc.index[key]
	return e != nil && !e.invalid
}

// storedSize returns a committed record's key and encoded value bytes (0 when
// the index does not hold it).
func (dc *DiskCache) storedSize(key string) int64 {
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	if e := dc.index[key]; e != nil {
		return e.size - diskRecordHeaderSize
	}
	return 0
}

// readHandoverMemory reads and decodes one memory-only value of the
// hand-over. The encoded value is copied out before decoding so a large value
// does not extend the read transaction.
func (dc *DiskCache) readHandoverMemory(key string) ([]byte, int64, bool) {
	var raw []byte
	err := dc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(handoverBucket)
		if b == nil {
			return nil
		}
		bucketKey := handoverBucketKey(handoverClassMemory, key)
		v := b.Get(bucketKey)
		if v == nil {
			return nil
		}
		if _, _, _, _, _, err := decodeHandoverRecordHeader(bucketKey, v); err != nil {
			return err
		}
		raw = bytes.Clone(v[handoverRecordHeaderSize:])
		return nil
	})
	if err != nil || raw == nil {
		return nil, 0, false
	}
	decoded, err := decodeNativeDiskRecordLimit(raw, dc.maxEntryBytes)
	if err != nil {
		return nil, 0, false
	}
	return decoded.Value, decoded.ExpiresAt, true
}
