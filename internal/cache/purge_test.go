package cache

import (
	"testing"
	"time"
)

// TestCache_PurgeAll_ClearsL1 confirms that PurgeAll evicts all in-memory entries.
func TestCache_PurgeAll_ClearsL1(t *testing.T) {
	c := New(60*time.Second, 1000)
	c.Set("key1", []byte("value1"))
	c.Set("key2", []byte("value2"))

	if _, ok := c.Get("key1"); !ok {
		t.Fatal("expected key1 to be in cache before purge")
	}

	c.PurgeAll()

	if _, ok := c.Get("key1"); ok {
		t.Error("expected key1 to be gone after PurgeAll")
	}
	if _, ok := c.Get("key2"); ok {
		t.Error("expected key2 to be gone after PurgeAll")
	}
}

// TestCache_PurgeAll_ClearsHotIndex confirms the L0 hot-key index is also cleared,
// so hot entries don't survive a flush.
func TestCache_PurgeAll_ClearsHotIndex(t *testing.T) {
	c := New(60*time.Second, 1000)
	c.Set("hot-key", []byte("data"))
	// Promote to hot index by recording multiple accesses
	for i := 0; i < 5; i++ {
		c.Get("hot-key")
	}
	c.PurgeAll()

	if _, ok := c.Get("hot-key"); ok {
		t.Error("expected hot-key to be gone from L0 after PurgeAll")
	}
}

// TestCache_PurgeAll_IdempotentOnEmptyCache verifies no panic when purging an
// already-empty cache.
func TestCache_PurgeAll_IdempotentOnEmptyCache(t *testing.T) {
	c := New(60*time.Second, 1000)
	c.PurgeAll()
	c.PurgeAll() // second purge must not panic
}

// TestCache_PurgeAll_WritableAfterPurge confirms the cache accepts new entries
// after being purged (not permanently broken).
func TestCache_PurgeAll_WritableAfterPurge(t *testing.T) {
	c := New(60*time.Second, 1000)
	c.Set("before", []byte("v1"))
	c.PurgeAll()

	c.Set("after", []byte("v2"))
	v, ok := c.Get("after")
	if !ok {
		t.Fatal("expected hit for entry written after purge")
	}
	if string(v) != "v2" {
		t.Errorf("expected v2, got %q", v)
	}
}

// TestDiskCache_Purge_ClearsWriteBuffer confirms that Purge clears buffered (unflushed)
// writes — entries that haven't been committed to bbolt yet must also be gone.
func TestDiskCache_Purge_ClearsWriteBuffer(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("buffered-key", []byte("buffered-value"), 60*time.Second)

	// Verify it's in the write buffer (not yet flushed)
	if _, ok := dc.Get("buffered-key"); !ok {
		t.Fatal("expected buffered hit before purge")
	}

	dc.Purge()

	if _, ok := dc.Get("buffered-key"); ok {
		t.Error("expected buffered-key to be gone after Purge")
	}
}

// TestDiskCache_Purge_ClearsDiskEntries confirms that Purge removes entries
// that have been flushed to the bbolt database.
func TestDiskCache_Purge_ClearsDiskEntries(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("disk-key", []byte("disk-value"), 60*time.Second)
	dc.Flush() // commit to bbolt

	// Verify it's on disk
	if _, ok := dc.Get("disk-key"); !ok {
		t.Fatal("expected disk hit before purge")
	}

	dc.Purge()

	if _, ok := dc.Get("disk-key"); ok {
		t.Error("expected disk-key to be gone after Purge")
	}
}

// TestDiskCache_Purge_WritableAfterPurge confirms the disk cache accepts new
// entries after being purged.
func TestDiskCache_Purge_WritableAfterPurge(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("before", []byte("old"), 60*time.Second)
	dc.Flush()
	dc.Purge()

	dc.Set("after", []byte("new"), 60*time.Second)
	v, ok := dc.Get("after")
	if !ok {
		t.Fatal("expected hit for entry written after Purge")
	}
	if string(v) != "new" {
		t.Errorf("expected new, got %q", v)
	}
}

// TestDiskCache_Purge_Compressed checks purge works correctly with gzip compression.
func TestDiskCache_Purge_Compressed(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        tempDBPath(t),
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	dc.Set("compressed-key", []byte("compressed-value"), 60*time.Second)
	dc.Flush()

	if _, ok := dc.Get("compressed-key"); !ok {
		t.Fatal("expected hit before purge")
	}

	dc.Purge()

	if _, ok := dc.Get("compressed-key"); ok {
		t.Error("expected compressed-key to be gone after Purge")
	}
}
