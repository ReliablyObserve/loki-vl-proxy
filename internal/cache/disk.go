package cache

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

var dataBucket = []byte("cache")

var gzipWriteBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(io.Discard)
	},
}

var gzipReaderPool sync.Pool

// DiskCache is an L2 on-disk cache backed by bbolt (B+ tree).
// Optimized for sequential I/O patterns (AWS ST1 HDD-friendly):
//   - Write-back buffer: batches writes to reduce IOPS
//   - Gzip compression: reduces disk I/O volume
//   - Sequential B+ tree layout for disk-friendly access
type DiskCache struct {
	db          *bolt.DB
	compression bool
	writeBuf    map[string]diskEntry
	writeMu     sync.Mutex
	flushSize   int // flush buffer after this many entries
	minTTL      time.Duration
	maxBytes    int64
	log         *slog.Logger
	done        chan struct{} // signals background flusher to stop

	// Stats
	Hits        atomic.Int64
	Misses      atomic.Int64
	Writes      atomic.Int64
	Evictions   atomic.Int64
	FlushCount  atomic.Int64
	BytesOnDisk atomic.Int64
}

type diskEntry struct {
	Value     []byte
	ExpiresAt int64 // unix nano
}

// DiskCacheConfig configures the L2 disk cache.
type DiskCacheConfig struct {
	Path          string        // Path to bbolt database file
	MaxBytes      int64         // Max disk usage (0 = unlimited)
	FlushInterval time.Duration // How often to flush write buffer (default: 5s)
	FlushSize     int           // Flush after N buffered writes (default: 100)
	MinTTL        time.Duration // Skip disk writes for entries with TTL below this threshold
	Compression   bool          // Gzip compress values (default: true)
}

// NewDiskCache creates an L2 disk cache.
func NewDiskCache(cfg DiskCacheConfig) (*DiskCache, error) {
	opts := bolt.DefaultOptions
	opts.NoSync = true     // Async sync — faster writes, safe with write buffer
	opts.NoGrowSync = true // Skip grow sync for sequential write performance
	opts.FreelistType = bolt.FreelistMapType

	db, err := bolt.Open(cfg.Path, 0600, opts)
	if err != nil {
		return nil, fmt.Errorf("open disk cache: %w", err)
	}

	// Create bucket
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dataBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	flushSize := cfg.FlushSize
	if flushSize == 0 {
		flushSize = 100
	}

	logger := slog.Default().With("component", "disk_cache")

	dc := &DiskCache{
		db:          db,
		compression: cfg.Compression,
		writeBuf:    make(map[string]diskEntry),
		flushSize:   flushSize,
		minTTL:      cfg.MinTTL,
		maxBytes:    cfg.MaxBytes,
		log:         logger,
		done:        make(chan struct{}),
	}

	// Start background flusher
	interval := cfg.FlushInterval
	if interval == 0 {
		interval = 5 * time.Second
	}
	go dc.backgroundFlush(interval)

	return dc, nil
}

// Get retrieves a value from disk cache.
func (dc *DiskCache) Get(key string) ([]byte, bool) {
	value, _, ok := dc.GetWithTTL(key)
	return value, ok
}

// GetWithTTL retrieves a fresh value from disk cache with its remaining TTL.
func (dc *DiskCache) GetWithTTL(key string) ([]byte, time.Duration, bool) {
	return dc.getWithTTL(key, false)
}

// GetStaleWithTTL retrieves the last retained value from disk, even if expired.
// The returned TTL may be negative when the entry is stale.
func (dc *DiskCache) GetStaleWithTTL(key string) ([]byte, time.Duration, bool) {
	return dc.getWithTTL(key, true)
}

func (dc *DiskCache) getWithTTL(key string, allowStale bool) ([]byte, time.Duration, bool) {
	// Check write buffer first
	dc.writeMu.Lock()
	if entry, ok := dc.writeBuf[key]; ok {
		dc.writeMu.Unlock()
		remaining := time.Until(time.Unix(0, entry.ExpiresAt))
		if remaining <= 0 && !allowStale {
			dc.Misses.Add(1)
			return nil, 0, false
		}
		dc.Hits.Add(1)
		return entry.Value, remaining, true
	}
	dc.writeMu.Unlock()

	// Check disk
	var raw []byte
	_ = dc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		if v := b.Get([]byte(key)); v != nil {
			raw = make([]byte, len(v))
			copy(raw, v)
		}
		return nil
	})

	if raw == nil {
		dc.Misses.Add(1)
		return nil, 0, false
	}

	// Decompress
	if dc.compression {
		var err error
		raw, err = decompress(raw)
		if err != nil {
			dc.Misses.Add(1)
			return nil, 0, false
		}
	}

	// Check expiry (first 8 bytes = expiry timestamp)
	if len(raw) < 8 {
		dc.Misses.Add(1)
		return nil, 0, false
	}
	expiresAt := int64(binary.BigEndian.Uint64(raw[:8]))
	remaining := time.Until(time.Unix(0, expiresAt))
	if remaining <= 0 && !allowStale {
		dc.Misses.Add(1)
		dc.Evictions.Add(1)
		go func() {
			_ = dc.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(dataBucket).Delete([]byte(key))
			})
		}()
		return nil, 0, false
	}

	dc.Hits.Add(1)
	return raw[8:], remaining, true
}

// Set stores a value in the write buffer (will be flushed to disk).
func (dc *DiskCache) Set(key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	if dc.minTTL > 0 && ttl < dc.minTTL {
		return
	}

	dc.writeMu.Lock()
	dc.writeBuf[key] = diskEntry{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl).UnixNano(),
	}
	shouldFlush := len(dc.writeBuf) >= dc.flushSize
	dc.writeMu.Unlock()

	if shouldFlush {
		dc.Flush()
	}
}

// Flush writes buffered entries to disk in a single batch transaction.
func (dc *DiskCache) Flush() {
	dc.writeMu.Lock()
	if len(dc.writeBuf) == 0 {
		dc.writeMu.Unlock()
		return
	}
	buf := dc.writeBuf
	dc.writeBuf = make(map[string]diskEntry)
	dc.writeMu.Unlock()

	currentBytes := int64(0)
	if dc.maxBytes > 0 {
		if _, bytesOnDisk := dc.Size(); bytesOnDisk > 0 {
			currentBytes = bytesOnDisk
		}
	}

	_ = dc.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		for key, entry := range buf {
			encoded, ok := encodeDiskEntry(entry)
			if !ok {
				continue
			}

			// Compress
			if dc.compression {
				var err error
				encoded, err = compress(encoded)
				if err != nil {
					continue
				}
			}
			if dc.maxBytes > 0 {
				if existing := b.Get([]byte(key)); existing != nil {
					currentBytes -= int64(len(existing)) // reclaim overwrite space
				}
				if currentBytes+int64(len(encoded)) > dc.maxBytes {
					dc.Evictions.Add(1)
					continue
				}
			}

			_ = b.Put([]byte(key), encoded)
			dc.Writes.Add(1)
			currentBytes += int64(len(encoded))
		}
		return nil
	})

	dc.FlushCount.Add(1)
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

// Close stops the background flusher, flushes pending writes, and closes the database.
func (dc *DiskCache) Close() error {
	select {
	case <-dc.done:
	default:
		close(dc.done)
	}
	dc.Flush()
	return dc.db.Close()
}

// Size returns the number of entries and bytes on disk.
func (dc *DiskCache) Size() (entries int, bytes int64) {
	_ = dc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		entries = b.Stats().KeyN
		return nil
	})
	fi, err := os.Stat(dc.db.Path())
	if err == nil {
		bytes = fi.Size()
	}
	dc.BytesOnDisk.Store(bytes)
	return entries, bytes
}

func (dc *DiskCache) backgroundFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-dc.done:
			return
		case <-ticker.C:
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
