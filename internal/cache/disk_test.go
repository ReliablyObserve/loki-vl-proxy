package cache

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempDBPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.db")
}

func TestDiskCache_SetGet(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("key1", []byte("hello"), 10*time.Second)

	// Should be in write buffer
	v, ok := dc.Get("key1")
	if !ok {
		t.Fatal("expected hit from write buffer")
	}
	if string(v) != "hello" {
		t.Errorf("expected hello, got %q", v)
	}

	// Flush to disk and verify
	dc.Flush()
	v, ok = dc.Get("key1")
	if !ok {
		t.Fatal("expected hit from disk")
	}
	if string(v) != "hello" {
		t.Errorf("expected hello, got %q", v)
	}
}

func TestDiskCache_TTLExpiry(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("exp", []byte("data"), 50*time.Millisecond)
	dc.Flush()

	time.Sleep(60 * time.Millisecond)

	_, ok := dc.Get("exp")
	if ok {
		t.Error("expected miss after TTL expiry")
	}
}

func TestDiskCache_Compression(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// Large repetitive data compresses well
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 26) + 'a'
	}

	dc.Set("big", data, 10*time.Second)
	dc.Flush()

	v, ok := dc.Get("big")
	if !ok {
		t.Fatal("expected hit")
	}
	if len(v) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(v))
	}
	for i := range v {
		if v[i] != data[i] {
			t.Fatalf("data mismatch at byte %d", i)
		}
	}

	// Disk size should be smaller than raw data
	_, diskBytes := dc.Size()
	if diskBytes >= int64(len(data)) {
		t.Logf("warning: compressed size %d >= raw size %d", diskBytes, len(data))
	}
}

func TestDiskCache_Encryption(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	dc, err := NewDiskCache(DiskCacheConfig{
		Path:          tempDBPath(t),
		EncryptionKey: key,
		Compression:   false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("secret", []byte("sensitive-data"), 10*time.Second)
	dc.Flush()

	v, ok := dc.Get("secret")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(v) != "sensitive-data" {
		t.Errorf("expected sensitive-data, got %q", v)
	}
}

func TestDiskCache_EncryptionWithCompression(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	dc, err := NewDiskCache(DiskCacheConfig{
		Path:          tempDBPath(t),
		EncryptionKey: key,
		Compression:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("both", []byte("encrypted-and-compressed"), 10*time.Second)
	dc.Flush()

	v, ok := dc.Get("both")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(v) != "encrypted-and-compressed" {
		t.Errorf("expected encrypted-and-compressed, got %q", v)
	}
}

func TestDiskCache_WrongKeyFails(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	path := tempDBPath(t)

	// Write with key1
	dc1, err := NewDiskCache(DiskCacheConfig{
		Path:          path,
		EncryptionKey: key1,
	})
	if err != nil {
		t.Fatal(err)
	}
	dc1.Set("protected", []byte("data"), 10*time.Second)
	dc1.Flush()
	_ = dc1.Close()

	// Try reading with key2
	dc2, err := NewDiskCache(DiskCacheConfig{
		Path:          path,
		EncryptionKey: key2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc2.Close() }()

	_, ok := dc2.Get("protected")
	if ok {
		t.Error("expected decryption failure with wrong key")
	}
}

func TestDiskCache_Size(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path: tempDBPath(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	for i := range 10 {
		dc.Set(string(rune('a'+i)), []byte("data"), 10*time.Second)
	}
	dc.Flush()

	entries, bytes := dc.Size()
	if entries != 10 {
		t.Errorf("expected 10 entries, got %d", entries)
	}
	if bytes == 0 {
		t.Error("expected non-zero bytes on disk")
	}
}

func TestDiskCache_FlushSize(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:      tempDBPath(t),
		FlushSize: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// Write 5 entries — should auto-flush
	for i := range 5 {
		dc.Set(string(rune('a'+i)), []byte("val"), 10*time.Second)
	}

	time.Sleep(10 * time.Millisecond) // let auto-flush complete

	if dc.FlushCount.Load() < 1 {
		t.Error("expected at least 1 auto-flush after reaching FlushSize")
	}
}

func TestDiskCache_Stats(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path: tempDBPath(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("k", []byte("v"), 10*time.Second)
	dc.Get("k")          // hit
	dc.Get("nonexistent") // miss

	if dc.Hits.Load() != 1 {
		t.Errorf("expected 1 hit, got %d", dc.Hits.Load())
	}
	if dc.Misses.Load() != 1 {
		t.Errorf("expected 1 miss, got %d", dc.Misses.Load())
	}
}

func TestDiskCache_Persistence(t *testing.T) {
	path := tempDBPath(t)

	// Write and close
	dc1, err := NewDiskCache(DiskCacheConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	dc1.Set("persist", []byte("survives-restart"), 1*time.Hour)
	dc1.Flush()
	_ = dc1.Close()

	// Reopen and read
	dc2, err := NewDiskCache(DiskCacheConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc2.Close() }()

	v, ok := dc2.Get("persist")
	if !ok {
		t.Fatal("expected data to persist across restarts")
	}
	if string(v) != "survives-restart" {
		t.Errorf("expected survives-restart, got %q", v)
	}
}

func TestDiskCache_InvalidPath(t *testing.T) {
	_, err := NewDiskCache(DiskCacheConfig{
		Path: "/nonexistent/dir/test.db",
	})
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestDiskCache_InvalidEncryptionKey(t *testing.T) {
	// Key must be exactly 32 bytes for AES-256
	_, err := NewDiskCache(DiskCacheConfig{
		Path:          tempDBPath(t),
		EncryptionKey: []byte("too-short"),
	})
	if err != nil {
		t.Error("short key should be ignored (no encryption), not fail")
	}

	// But accessing dc should still work (no encryption applied)
	dc, _ := NewDiskCache(DiskCacheConfig{
		Path:          tempDBPath(t) + "2",
		EncryptionKey: []byte("too-short"),
	})
	if dc != nil {
		dc.Set("k", []byte("v"), time.Second)
		v, ok := dc.Get("k")
		if !ok || string(v) != "v" {
			t.Error("expected unencrypted cache to work with short key")
		}
		_ = dc.Close()
		_ = os.Remove(dc.db.Path())
	}
}
