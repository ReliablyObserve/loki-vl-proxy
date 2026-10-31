package cache

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestDiskBatchCloseDrainsRejectedReplacements(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	cfg := DiskCacheConfig{Path: path, MaxEntryBytes: 1, FlushSize: 5000, FlushInterval: time.Hour}
	dc, err := NewDiskCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dc.Close() })
	for i := 0; i < 2200; i++ {
		dc.Set(fmt.Sprintf("k%d", i), []byte("v"), time.Hour)
	}
	dc.Flush()
	// Keep the worker behind this barrier while every old value is rejected.
	// Close must drain all1024-entry deletion batches, even when the worker
	// exits after at most one batch before Close takes over.
	dc.flushMu.Lock()
	for i := 0; i < 2200; i++ {
		dc.Set(fmt.Sprintf("k%d", i), []byte("xx"), time.Hour)
	}
	done := make(chan error, 1)
	go func() { done <- dc.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		dc.writeMu.Lock()
		closing := dc.closing
		dc.writeMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			dc.flushMu.Unlock()
			t.Fatal("Close did not start")
		}
		time.Sleep(time.Millisecond)
	}
	dc.flushMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDiskCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if entries, _ := reopened.Size(); entries != 0 {
		t.Fatalf("%d rejected old values survived Close/restart", entries)
	}
}

func TestDiskBatchIdleFlushDoesNotWrite(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	dc.Set("a", []byte("value"), time.Hour)
	dc.Flush()
	stats := dc.db.Stats()
	before := stats.TxStats.GetWrite()
	for i := 0; i < 10; i++ {
		dc.Flush()
	}
	stats = dc.db.Stats()
	if writes := stats.TxStats.GetWrite() - before; writes != 0 {
		t.Fatalf("idle flushes performed %d writes", writes)
	}
}
