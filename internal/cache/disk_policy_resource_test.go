package cache

import (
	"runtime"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestDiskPolicyRejectsDamagedRecordBeforeCopy(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntryBytes: 1024})
	dc.Set("damaged", []byte("valid"), time.Hour)
	dc.Flush()
	raw, err := encodeDiskRecord(diskEntry{Value: []byte("x"), ExpiresAt: time.Now().Add(time.Hour).UnixNano()}, diskCodecRaw)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, make([]byte, 8<<20)...)
	if err := dc.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(dataBucket).Put([]byte("damaged"), raw) }); err != nil {
		t.Fatal(err)
	}
	// Keep the asynchronous cleanup transaction out of the allocation sample.
	tx, err := dc.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, hit := dc.Get("damaged")
	runtime.ReadMemStats(&after)
	if hit {
		t.Fatal("damaged record was returned")
	}
	// A generous allowance absorbs runtime bookkeeping; the failing path copied
	// more than 8 MiB despite a configured decoded-entry limit of 1 KiB.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("oversized record allocated %d bytes before rejection", allocated)
	}
}

func TestDiskPolicyDiskHitAfterPendingWriteKeepsRecency(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 2})
	dc.Set("a", []byte("a"), time.Hour)
	dc.Flush()
	dc.Set("b", []byte("b"), time.Hour)
	policyWant(t, dc, "a", []byte("a"))
	dc.Set("c", []byte("c"), time.Hour)
	dc.Flush()
	policyWant(t, dc, "b", nil)
	policyWant(t, dc, "a", []byte("a"))
	policyWant(t, dc, "c", []byte("c"))
}
