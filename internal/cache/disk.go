package cache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	gzip "github.com/klauspost/compress/gzip"
	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var dataBucket = []byte("cache_records_v1")
var legacyDataBucket = []byte("cache")
var gzipWriteBufferPool = sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) }}
var gzipWriterPool = sync.Pool{New: func() interface{} { return gzip.NewWriter(io.Discard) }}
var gzipReaderPool sync.Pool

// DiskCache batches bounded raw writes into bbolt and tracks disk recency in
// memory. Hits never persist recency; its ordering resets after a restart.
// Index and write queue have separate budgets (MaxIndexBytes + MaxBufferBytes).
// Codec output, bbolt pages and temporary transaction state are additional memory.
type DiskCache struct {
	layerL2                [LayerCount]layerTierStats
	layerIndexBytes        [LayerCount]int64
	layerQueueBytes        [LayerCount]int64
	layerEncodedReadBytes  [LayerCount]atomic.Int64
	layerEncodedWriteBytes [LayerCount]atomic.Int64

	db              *bolt.DB
	compression     bool // legacy bucket decoding only
	codec           diskCodec
	writeMu         sync.Mutex // queue, index and lifecycle state; never held over disk commits
	flushMu         sync.Mutex // orders flush, purge and close
	writeBuf        map[string]diskEntry
	flushing        map[string]diskEntry
	flushingBytes   int64
	bufferBytes     int64 // pending + flushing raw values, keys and conservative metadata
	flushSize       int
	flushBytes      int64
	maxBufferBytes  int64
	maxEntryBytes   int
	minTTL          time.Duration
	maxBytes        int64
	maxEntries      int
	maxIndexBytes   int64
	index           map[string]*diskLRUEntry
	indexBytes      int64
	logicalBytes    int64
	newest, oldest  *diskLRUEntry
	nextExpiry      int64
	dirty           bool
	sequence        uint64
	closing, closed bool
	closeOnce       sync.Once
	lastFlushErr    error
	closeErr        error
	done            chan struct{}
	workerDone      chan struct{}
	flushRequest    chan struct{}
	log             *slog.Logger
	Hits            atomic.Int64
	Misses          atomic.Int64
	Writes          atomic.Int64
	Evictions       atomic.Int64
	RejectedWrites  atomic.Int64
	FlushCount      atomic.Int64
	BytesOnDisk     atomic.Int64
}

type diskEntry struct {
	Value     []byte
	ExpiresAt int64
	state     *diskEntryState
}

type diskEntryState struct {
	cancelled  atomic.Bool
	lastAccess uint64 // protected by writeMu; shared by pending/flushing readers
}

type DiskCacheConfig struct {
	Path           string
	MaxBytes       int64         // logical encoded value + key bytes; physical bbolt file can be larger
	FlushInterval  time.Duration // default 5s
	FlushSize      int           // default 1000 unique pending writes
	FlushBytes     int64         // default 4MiB pending raw queue charge
	MaxBufferBytes int64         // default 16MiB pending + flushing queue charge
	MaxEntries     int           // default 100000 committed index entries
	MaxIndexBytes  int64         // default 16MiB conservative 128+len(key) index charge
	MaxEntryBytes  int           // default and hard ceiling 256MiB decoded value
	MinTTL         time.Duration
	Compression    bool   // legacy read flag; selects S2 for new writes if Codec is empty
	Codec          string // raw, s2, zstd or gzip; empty with Compression=false selects raw
}

func NewDiskCache(cfg DiskCacheConfig) (*DiskCache, error) {
	if cfg.MaxBytes < 0 || cfg.FlushInterval < 0 || cfg.FlushSize < 0 || cfg.FlushBytes < 0 || cfg.MaxBufferBytes < 0 || cfg.MaxEntries < 0 || cfg.MaxIndexBytes < 0 || cfg.MaxEntryBytes < 0 || cfg.MinTTL < 0 {
		return nil, fmt.Errorf("negative disk cache configuration")
	}
	codec, err := parseDiskCodec(cfg.Codec)
	if err != nil {
		return nil, err
	}
	if !cfg.Compression {
		codec = diskCodecRaw
	}
	if cfg.FlushSize == 0 {
		cfg.FlushSize = 1000
	}
	if cfg.FlushBytes == 0 {
		cfg.FlushBytes = 4 << 20
	}
	if cfg.MaxBufferBytes == 0 {
		cfg.MaxBufferBytes = 16 << 20
	}
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = 100000
	}
	if cfg.MaxIndexBytes == 0 {
		cfg.MaxIndexBytes = 16 << 20
	}
	if cfg.MaxEntryBytes == 0 || cfg.MaxEntryBytes > diskRecordMaxDecoded {
		cfg.MaxEntryBytes = diskRecordMaxDecoded
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	// DefaultOptions is shared globally. Mutating it races unrelated bbolt users.
	opts := *bolt.DefaultOptions
	opts.NoSync = true
	opts.NoGrowSync = true
	opts.FreelistType = bolt.FreelistMapType
	db, err := bolt.Open(cfg.Path, 0600, &opts)
	if err != nil {
		return nil, fmt.Errorf("open disk cache: %w", err)
	}
	dc := &DiskCache{db: db, compression: cfg.Compression, codec: codec, writeBuf: make(map[string]diskEntry),
		flushSize: cfg.FlushSize, flushBytes: cfg.FlushBytes, maxBufferBytes: cfg.MaxBufferBytes,
		maxEntryBytes: cfg.MaxEntryBytes, minTTL: cfg.MinTTL, maxBytes: cfg.MaxBytes,
		maxEntries: cfg.MaxEntries, maxIndexBytes: cfg.MaxIndexBytes, index: make(map[string]*diskLRUEntry),
		done: make(chan struct{}), workerDone: make(chan struct{}), flushRequest: make(chan struct{}, 1),
		log: slog.Default().With("component", "disk_cache")}
	if err = dc.rebuildIndex(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize disk cache: %w", err)
	}
	go dc.backgroundFlush(cfg.FlushInterval)
	return dc, nil
}

func (dc *DiskCache) Get(key string) ([]byte, bool) { v, _, ok := dc.GetWithTTL(key); return v, ok }
func (dc *DiskCache) GetWithTTL(key string) ([]byte, time.Duration, bool) {
	return dc.getWithTTL(key, false)
}
func (dc *DiskCache) GetStaleWithTTL(key string) ([]byte, time.Duration, bool) {
	return dc.getWithTTL(key, true)
}
func (dc *DiskCache) getWithTTL(key string, allowStale bool) (value []byte, ttl time.Duration, found bool) {
	if dc == nil {
		return nil, 0, false
	}
	start := time.Now()
	defer func() {
		s := &dc.layerL2[LayerForKey(key)]
		s.lookup(value, found, found && ttl <= 0)
		s.lookupNanoseconds.Add(time.Since(start).Nanoseconds())
	}()
	return dc.lookupWithTTL(key, allowStale)
}

// lookupWithTTL implements storage lookup without the observation wrapper.
func (dc *DiskCache) lookupWithTTL(key string, allowStale bool) ([]byte, time.Duration, bool) {
	dc.writeMu.Lock()
	if dc.closed || dc.closing {
		dc.writeMu.Unlock()
		dc.Misses.Add(1)
		return nil, 0, false
	}
	entry, ok := dc.writeBuf[key]
	if !ok {
		entry, ok = dc.flushing[key]
	}
	if ok {
		cancelled := entry.state != nil && entry.state.cancelled.Load()
		if entry.state != nil && !cancelled {
			dc.sequence++
			entry.state.lastAccess = dc.sequence
		}
		dc.writeMu.Unlock()
		remaining := time.Unix(0, entry.ExpiresAt).Sub(clockNow())
		if cancelled || (remaining <= 0 && !allowStale) {
			dc.Misses.Add(1)
			return nil, 0, false
		}
		dc.Hits.Add(1)
		return entry.Value, remaining, true
	}
	item := dc.index[key]
	if item == nil || item.invalid {
		dc.writeMu.Unlock()
		dc.Misses.Add(1)
		return nil, 0, false
	}
	generation := item.generation
	dc.writeMu.Unlock()
	// Copy out before decoding so compression work does not extend the bbolt
	// reader transaction and delay a writer that needs to grow its mmap.
	var raw []byte
	native := false
	err := dc.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(dataBucket); b != nil {
			if value := b.Get([]byte(key)); value != nil {
				if err := dc.validateStoredDiskRecord(value, true); err != nil {
					return err
				}
				raw = bytes.Clone(value)
				dc.layerEncodedReadBytes[LayerForKey(key)].Add(int64(len(value)))
				native = true
				return nil
			}
		}
		if b := tx.Bucket(legacyDataBucket); b != nil {
			if value := b.Get([]byte(key)); value != nil {
				if err := dc.validateStoredDiskRecord(value, false); err != nil {
					return err
				}
				raw = bytes.Clone(value)
				dc.layerEncodedReadBytes[LayerForKey(key)].Add(int64(len(value)))
			}
		}
		return nil
	})
	var decoded diskEntry
	if err == nil && raw != nil {
		if native {
			decoded, err = decodeNativeDiskRecordLimit(raw, dc.maxEntryBytes)
		} else {
			decoded, err = decodeLegacyDiskRecord(raw, dc.compression, dc.maxEntryBytes)
		}
	}
	remaining := time.Unix(0, decoded.ExpiresAt).Sub(clockNow())
	if err != nil || raw == nil || (remaining <= 0 && !allowStale) {
		dc.writeMu.Lock()
		if current := dc.index[key]; current != nil && current.generation == generation {
			current.invalid = true
			dc.dirty = true
		}
		dc.writeMu.Unlock()
		dc.requestFlush()
		dc.Misses.Add(1)
		return nil, 0, false
	}
	dc.writeMu.Lock()
	if current := dc.index[key]; current != nil && current.generation == generation {
		dc.sequence++
		current.lastAccess = dc.sequence
		dc.touch(current)
	}
	dc.writeMu.Unlock()
	dc.Hits.Add(1)
	return decoded.Value, remaining, true
}

func (dc *DiskCache) MinTTL() time.Duration {
	if dc == nil {
		return 0
	}
	return dc.minTTL
}

// Set only admits to the bounded raw queue. A single worker performs threshold
// flushes, so calls under the L1 mutex never commit disk transactions.
func (dc *DiskCache) Set(key string, value []byte, ttl time.Duration) {
	layer := LayerForKey(key)
	dc.writeMu.Lock()
	if dc.closed || dc.closing {
		dc.layerL2[layer].rejections.Add(1)
		dc.writeMu.Unlock()
		return
	}
	charge := diskQueueCharge(key, value)
	used := dc.bufferBytes
	if old, ok := dc.writeBuf[key]; ok {
		used -= diskQueueCharge(key, old.Value)
	}
	reject := key == "" || len(key) > bolt.MaxKeySize || ttl <= 0 || (dc.minTTL > 0 && ttl < dc.minTTL) || len(value) > dc.maxEntryBytes || charge > dc.maxBufferBytes || used > dc.maxBufferBytes-charge
	if !reject && dc.codec == diskCodecRaw && dc.maxBytes > 0 {
		reject = int64(len(key))+int64(diskRecordHeaderSize)+int64(len(value)) > dc.maxBytes
	}
	if reject {
		dc.rejectLocked(key)
		dc.writeMu.Unlock()
		dc.requestFlush()
		return
	}
	if old, ok := dc.writeBuf[key]; ok {
		dc.layerQueueBytes[layer] -= diskQueueCharge(key, old.Value)
	}
	dc.layerQueueBytes[layer] += charge
	dc.layerL2[layer].admissions.Add(1)
	dc.layerL2[layer].writeBytes.Add(int64(len(value)))
	dc.sequence++
	dc.writeBuf[key] = diskEntry{Value: value, ExpiresAt: clockNow().Add(ttl).UnixNano(), state: &diskEntryState{lastAccess: dc.sequence}}
	dc.bufferBytes = used + charge
	if item := dc.index[key]; item != nil {
		item.invalid = false
	}
	shouldFlush := len(dc.writeBuf) >= dc.flushSize || dc.pendingChargeLocked() >= dc.flushBytes
	dc.writeMu.Unlock()
	if shouldFlush {
		dc.requestFlush()
	}
}

func diskQueueCharge(key string, value []byte) int64 {
	return 128 + int64(len(key)) + int64(len(value))
}
func (dc *DiskCache) pendingChargeLocked() int64 { return dc.bufferBytes - dc.flushingBytes }
func (dc *DiskCache) rejectLocked(key string) {
	dc.RejectedWrites.Add(1)
	dc.layerL2[LayerForKey(key)].rejections.Add(1)
	if old, ok := dc.writeBuf[key]; ok {
		dc.bufferBytes -= diskQueueCharge(key, old.Value)
		dc.layerQueueBytes[LayerForKey(key)] -= diskQueueCharge(key, old.Value)
		delete(dc.writeBuf, key)
	}
	if entry, ok := dc.flushing[key]; ok && entry.state != nil {
		entry.state.cancelled.Store(true)
	}
	if item := dc.index[key]; item != nil {
		item.invalid = true
		dc.dirty = true
	}
}
func (dc *DiskCache) requestFlush() {
	select {
	case dc.flushRequest <- struct{}{}:
	default:
	}
}

func encodeDiskEntry(entry diskEntry) ([]byte, bool) {
	if len(entry.Value) > math.MaxInt-8 {
		return nil, false
	}
	encoded := make([]byte, 8+len(entry.Value))
	binary.BigEndian.PutUint64(encoded[:8], uint64(entry.ExpiresAt))
	copy(encoded[8:], entry.Value)
	return encoded, true
}

func (dc *DiskCache) Flush() { dc.flushMu.Lock(); defer dc.flushMu.Unlock(); dc.flushLocked() }

func (dc *DiskCache) Purge() {
	dc.flushMu.Lock()
	defer dc.flushMu.Unlock()
	dc.writeMu.Lock()
	if dc.closed {
		dc.writeMu.Unlock()
		return
	}
	dc.writeBuf = make(map[string]diskEntry)
	dc.flushing = nil
	dc.bufferBytes = 0
	dc.flushingBytes = 0
	dc.index = make(map[string]*diskLRUEntry)
	dc.newest, dc.oldest = nil, nil
	dc.indexBytes, dc.logicalBytes, dc.nextExpiry = 0, 0, 0
	dc.dirty = false
	for l := LayerUnknown; l < LayerCount; l++ {
		dc.layerL2[l].entries.Store(0)
		dc.layerL2[l].bytes.Store(0)
		dc.layerIndexBytes[l] = 0
		dc.layerQueueBytes[l] = 0
	}
	dc.writeMu.Unlock()
	if err := dc.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{dataBucket, legacyDataBucket} {
			if err := tx.DeleteBucket(name); err != nil && err != boltErrors.ErrBucketNotFound {
				return err
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		dc.log.Warn("disk cache purge failed", "error", err)
	}
}

func (dc *DiskCache) Close() error {
	dc.closeOnce.Do(func() {
		dc.writeMu.Lock()
		dc.closing = true
		dc.writeMu.Unlock()
		close(dc.done)
		<-dc.workerDone
		dc.flushMu.Lock()
		for dc.flushLocked() {
			dc.writeMu.Lock()
			more := len(dc.writeBuf) > 0 || dc.dirty || (dc.nextExpiry != 0 && dc.nextExpiry <= clockNow().UnixNano())
			dc.writeMu.Unlock()
			if !more {
				break
			}
		}
		dc.writeMu.Lock()
		dc.closed = true
		dc.writeMu.Unlock()
		dc.closeErr = errors.Join(dc.lastFlushErr, dc.db.Close())
		dc.flushMu.Unlock()
	})
	return dc.closeErr
}

// Size reports indexed committed entries and physical file bytes. MaxBytes caps
// logical encoded key+value bytes; free bbolt pages keep the file high-water mark.
func (dc *DiskCache) Size() (entries int, bytes int64) {
	dc.writeMu.Lock()
	entries = len(dc.index)
	dc.writeMu.Unlock()
	if fi, err := os.Stat(dc.db.Path()); err == nil {
		bytes = fi.Size()
	}
	dc.BytesOnDisk.Store(bytes)
	return entries, bytes
}
func (dc *DiskCache) backgroundFlush(interval time.Duration) {
	defer close(dc.workerDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-dc.done:
			return
		case <-ticker.C:
			dc.Flush()
		case <-dc.flushRequest:
			dc.Flush()
		}
	}
}
func compress(data []byte) ([]byte, error) {
	buf := gzipWriteBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	w := gzipWriterPool.Get().(*gzip.Writer)
	w.Reset(buf)
	if _, err := w.Write(data); err != nil {
		gzipWriterPool.Put(w)
		gzipWriteBufferPool.Put(buf)
		return nil, err
	}
	if err := w.Close(); err != nil {
		gzipWriterPool.Put(w)
		gzipWriteBufferPool.Put(buf)
		return nil, err
	}
	out := append([]byte(nil), buf.Bytes()...)
	gzipWriterPool.Put(w)
	if buf.Cap() <= 1<<20 {
		buf.Reset()
		gzipWriteBufferPool.Put(buf)
	}
	return out, nil
}

func decompress(data []byte) ([]byte, error) {
	var (
		r   *gzip.Reader
		ok  bool
		err error
	)
	if pooled := gzipReaderPool.Get(); pooled != nil {
		r, ok = pooled.(*gzip.Reader)
		if ok {
			err = r.Reset(bytes.NewReader(data))
		}
	}
	if !ok || err != nil {
		r, err = gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
	}
	out, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if closeErr == nil {
		gzipReaderPool.Put(r)
	}
	if readErr != nil {
		return nil, readErr
	}
	return out, closeErr
}
