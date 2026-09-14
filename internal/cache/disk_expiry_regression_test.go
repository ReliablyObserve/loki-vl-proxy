package cache

import (
	"strings"
	"testing"
	"time"
)

func TestDiskCacheUnreadExpiryAllowsFreshAdmission(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db", MaxBytes: 65536, FlushSize: 100, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	dc.Set("old", []byte(strings.Repeat("x", 50000)), time.Hour)
	dc.Flush()
	// Set an expired value without relying on scheduler/timer precision.
	dc.writeMu.Lock()
	dc.writeBuf["old"] = diskEntry{Value: []byte(strings.Repeat("x", 50000)), ExpiresAt: time.Now().Add(-time.Hour).UnixNano()}
	dc.writeMu.Unlock()
	dc.Flush()
	dc.Set("new", []byte(strings.Repeat("y", 20000)), time.Hour)
	dc.Flush()
	if _, ok := dc.Get("new"); !ok {
		t.Fatal("unread expired entry blocked fresh admission")
	}
}

func TestDiskCacheExpirySweepContinuesWithoutWrites(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db", Compression: compressed, FlushSize: 2000, FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1100; i++ {
			dc.Set(strings.Repeat("k", i+1), []byte("value"), time.Hour)
		}
		dc.Flush()
		// Verify sweeping makes progress independently of reads of expired keys.
		dc.writeMu.Lock()
		for i := 0; i < 1100; i++ {
			dc.writeBuf[strings.Repeat("k", i+1)] = diskEntry{Value: []byte("value"), ExpiresAt: 1}
		}
		dc.writeMu.Unlock()
		dc.Flush()
		for i := 0; i < 30; i++ {
			dc.Flush()
		}
		if got, _ := dc.Size(); got != 0 {
			t.Errorf("compressed=%v expired keys remain: %d", compressed, got)
		}
		dc.Close()
	}
}
