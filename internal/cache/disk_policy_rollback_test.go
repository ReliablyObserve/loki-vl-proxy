package cache

import (
	bolt "go.etcd.io/bbolt"
	"path/filepath"
	"testing"
	"time"
)

func TestDiskPolicyRollbackPreservesNewerPending(t *testing.T) {
	dc, e := NewDiskCache(DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), MaxEntries: 1, FlushInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	defer dc.Close()
	dc.Set("old", []byte("old"), time.Hour)
	dc.Flush()
	e = dc.db.Update(func(tx *bolt.Tx) error { _, e := tx.Bucket(dataBucket).CreateBucket([]byte("bad")); return e })
	if e != nil {
		t.Fatal(e)
	}
	tx, e := dc.db.Begin(true)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	dc.Set("bad", []byte("first"), time.Hour)
	done := make(chan struct{})
	go func() { dc.Flush(); close(done) }()
	deadline := time.After(time.Second)
	for {
		dc.writeMu.Lock()
		_, ready := dc.flushing["bad"]
		dc.writeMu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			t.Fatal("snapshot not taken")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	dc.Set("bad", []byte("newer"), time.Hour)
	tx.Rollback()
	<-done
	dc.writeMu.Lock()
	old := dc.index["old"]
	newer := dc.writeBuf["bad"]
	count := len(dc.index)
	dc.writeMu.Unlock()
	if old == nil || count != 1 || string(newer.Value) != "newer" {
		t.Fatalf("rollback corrupted state: old=%v count=%d pending=%s", old, count, newer.Value)
	}
	e = dc.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(dataBucket).DeleteBucket([]byte("bad")) })
	if e != nil {
		t.Fatal(e)
	}
	dc.Flush()
	got, ok := dc.Get("bad")
	if !ok || string(got) != "newer" {
		t.Fatalf("retry lost newer value: %q %v", got, ok)
	}
	if _, ok := dc.Get("old"); ok {
		t.Fatal("old not evicted after successful retry")
	}
}
