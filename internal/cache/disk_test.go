package cache

import (
	"math"
	"path/filepath"
	"testing"
	"time"
	"unsafe"
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

func TestDiskCache_GetWithTTL_ReturnsRemainingTTL(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("ttl", []byte("value"), 5*time.Second)
	dc.Flush()

	value, ttl, ok := dc.GetWithTTL("ttl")
	if !ok {
		t.Fatal("expected fresh disk hit")
	}
	if string(value) != "value" {
		t.Fatalf("expected value, got %q", value)
	}
	if ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("expected TTL between 0 and 5s, got %v", ttl)
	}
}

func TestDiskCache_GetStaleWithTTL_ReturnsExpiredValue(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("stale", []byte("value"), 20*time.Millisecond)
	dc.Flush()
	time.Sleep(30 * time.Millisecond)

	value, ttl, ok := dc.GetStaleWithTTL("stale")
	if !ok {
		t.Fatal("expected stale disk value to be returned")
	}
	if string(value) != "value" {
		t.Fatalf("expected value, got %q", value)
	}
	if ttl >= 0 {
		t.Fatalf("expected negative TTL for stale disk value, got %v", ttl)
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
		data[i] = byte(i%26) + 'a'
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
	dc.Get("k")           // hit
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

func TestEncodeDiskEntryRejectsOverflowSizedValue(t *testing.T) {
	entry := diskEntry{
		Value:     make([]byte, 0),
		ExpiresAt: time.Now().UnixNano(),
	}
	headerSize := 8
	hugeLen := math.MaxInt - headerSize + 1

	// #nosec G103 -- test-only slice-header mutation to force an overflow-sized len without allocating it.
	valueHeader := (*[3]uintptr)(unsafe.Pointer(&entry.Value))
	valueHeader[1] = uintptr(hugeLen)
	valueHeader[2] = uintptr(hugeLen)

	if encoded, ok := encodeDiskEntry(entry); ok || encoded != nil {
		t.Fatalf("expected overflow-sized entry to be rejected, got ok=%v len=%d", ok, len(encoded))
	}
}

func TestDiskCache_MaxBytesSkipsFlushEntriesBeyondBudget(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
		MaxBytes:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("small", []byte("ok"), 10*time.Second)
	dc.Set("large", make([]byte, 2<<20), 10*time.Second)
	dc.Flush()

	if _, ok := dc.Get("small"); !ok {
		t.Fatal("expected small entry to remain cacheable")
	}
	if _, ok := dc.Get("large"); ok {
		t.Fatal("expected oversized entry to be skipped")
	}
	if dc.Evictions.Load() == 0 {
		t.Fatal("expected max-bytes skip to increment evictions")
	}
}

func TestDiskCache_MinTTLSkipsShortLivedWrites(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:     tempDBPath(t),
		MinTTL:   30 * time.Second,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("short", []byte("skip-me"), 5*time.Second)
	dc.Set("long", []byte("keep-me"), time.Minute)
	dc.Flush()

	if _, ok := dc.Get("short"); ok {
		t.Fatal("expected short-lived entry to be skipped for disk cache")
	}
	if v, ok := dc.Get("long"); !ok || string(v) != "keep-me" {
		t.Fatalf("expected long-lived entry to be stored, got %q ok=%v", string(v), ok)
	}
}
