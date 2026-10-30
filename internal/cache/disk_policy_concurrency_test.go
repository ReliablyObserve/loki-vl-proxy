package cache

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDiskPolicyConcurrentLifecycle(t *testing.T) {
	dc, e := NewDiskCache(DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), MaxBufferBytes: 8192, MaxEntries: 20, MaxIndexBytes: 4096, MaxBytes: 4096, FlushSize: 3, FlushInterval: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				k := fmt.Sprintf("k%d", (i+g)%37)
				switch (i + g) % 7 {
				case 0:
					dc.Flush()
				case 1:
					dc.Purge()
				case 2:
					dc.Get(k)
				case 3:
					dc.GetStaleWithTTL(k)
				case 4:
					dc.Set(k, bytes.Repeat([]byte("a"), 9000), time.Hour)
				default:
					dc.Set(k, bytes.Repeat([]byte("x"), 100), time.Hour)
				}
			}
		}(g)
	}
	wg.Wait()
	dc.Flush()
	dc.writeMu.Lock()
	if dc.bufferBytes > dc.maxBufferBytes || dc.indexBytes > dc.maxIndexBytes || len(dc.index) > dc.maxEntries || dc.logicalBytes > dc.maxBytes {
		t.Error("budget exceeded")
	}
	seen := 0
	for p := dc.newest; p != nil; p = p.next {
		seen++
		if p.next != nil && p.next.prev != p {
			t.Error("broken LRU")
		}
		if seen > len(dc.index) {
			t.Error("LRU cycle")
			break
		}
	}
	if seen != len(dc.index) {
		t.Error("missing LRU nodes")
	}
	dc.writeMu.Unlock()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				switch (i + g) % 5 {
				case 0:
					dc.Close()
				case 1:
					dc.Set("x", []byte("x"), time.Hour)
				case 2:
					dc.Purge()
				case 3:
					dc.Flush()
				case 4:
					dc.Get("x")
				}
			}
		}(g)
	}
	wg.Wait()
	dc.Close()
}
