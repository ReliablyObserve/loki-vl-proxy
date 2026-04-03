package cache

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

var dataBucket = []byte("cache")

// DiskCache is an L2 on-disk cache backed by bbolt (B+ tree).
// Optimized for sequential I/O patterns (AWS ST1 HDD-friendly):
//   - Write-back buffer: batches writes to reduce IOPS
//   - Gzip compression: reduces disk I/O volume
//   - Optional AES-256-GCM encryption at rest
//   - Sequential B+ tree layout for disk-friendly access
type DiskCache struct {
	db          *bolt.DB
	compression bool
	gcm         cipher.AEAD // nil if encryption disabled
	writeBuf    map[string]diskEntry
	writeMu     sync.Mutex
	flushSize   int // flush buffer after this many entries
	log         *slog.Logger

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
	Compression   bool          // Gzip compress values (default: true)
	EncryptionKey []byte        // 32-byte AES-256 key (nil = no encryption)
}

// NewDiskCache creates an L2 disk cache.
func NewDiskCache(cfg DiskCacheConfig) (*DiskCache, error) {
	opts := bolt.DefaultOptions
	opts.NoSync = true        // Async sync — faster writes, safe with write buffer
	opts.NoGrowSync = true    // Skip grow sync for sequential write performance
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
		db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	flushSize := cfg.FlushSize
	if flushSize == 0 {
		flushSize = 100
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dc := &DiskCache{
		db:          db,
		compression: cfg.Compression,
		writeBuf:    make(map[string]diskEntry),
		flushSize:   flushSize,
		log:         logger,
	}

	// Set up encryption if key provided
	if len(cfg.EncryptionKey) == 32 {
		block, err := aes.NewCipher(cfg.EncryptionKey)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("aes cipher: %w", err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("gcm cipher: %w", err)
		}
		dc.gcm = gcm
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
	// Check write buffer first
	dc.writeMu.Lock()
	if entry, ok := dc.writeBuf[key]; ok {
		dc.writeMu.Unlock()
		if time.Now().UnixNano() > entry.ExpiresAt {
			dc.Misses.Add(1)
			return nil, false
		}
		dc.Hits.Add(1)
		return entry.Value, true
	}
	dc.writeMu.Unlock()

	// Check disk
	var raw []byte
	dc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		if v := b.Get([]byte(key)); v != nil {
			raw = make([]byte, len(v))
			copy(raw, v)
		}
		return nil
	})

	if raw == nil {
		dc.Misses.Add(1)
		return nil, false
	}

	// Decrypt
	if dc.gcm != nil {
		var err error
		raw, err = dc.decrypt(raw)
		if err != nil {
			dc.Misses.Add(1)
			return nil, false
		}
	}

	// Decompress
	if dc.compression {
		var err error
		raw, err = decompress(raw)
		if err != nil {
			dc.Misses.Add(1)
			return nil, false
		}
	}

	// Check expiry (first 8 bytes = expiry timestamp)
	if len(raw) < 8 {
		dc.Misses.Add(1)
		return nil, false
	}
	expiresAt := int64(binary.BigEndian.Uint64(raw[:8]))
	if time.Now().UnixNano() > expiresAt {
		dc.Misses.Add(1)
		dc.Evictions.Add(1)
		return nil, false
	}

	dc.Hits.Add(1)
	return raw[8:], true
}

// Set stores a value in the write buffer (will be flushed to disk).
func (dc *DiskCache) Set(key string, value []byte, ttl time.Duration) {
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

	dc.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		for key, entry := range buf {
			// Encode: [8 bytes expiry][value]
			encoded := make([]byte, 8+len(entry.Value))
			binary.BigEndian.PutUint64(encoded[:8], uint64(entry.ExpiresAt))
			copy(encoded[8:], entry.Value)

			// Compress
			if dc.compression {
				var err error
				encoded, err = compress(encoded)
				if err != nil {
					continue
				}
			}

			// Encrypt
			if dc.gcm != nil {
				encoded = dc.encrypt(encoded)
			}

			b.Put([]byte(key), encoded)
			dc.Writes.Add(1)
		}
		return nil
	})

	dc.FlushCount.Add(1)
}

// Close flushes pending writes and closes the database.
func (dc *DiskCache) Close() error {
	dc.Flush()
	return dc.db.Close()
}

// Size returns the number of entries and bytes on disk.
func (dc *DiskCache) Size() (entries int, bytes int64) {
	dc.db.View(func(tx *bolt.Tx) error {
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
	for range ticker.C {
		dc.Flush()
	}
}

func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (dc *DiskCache) encrypt(plaintext []byte) []byte {
	nonce := make([]byte, dc.gcm.NonceSize())
	rand.Read(nonce)
	return dc.gcm.Seal(nonce, nonce, plaintext, nil)
}

func (dc *DiskCache) decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := dc.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return dc.gcm.Open(nil, nonce, ciphertext, nil)
}
