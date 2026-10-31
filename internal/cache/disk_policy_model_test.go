package cache

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

func TestDiskPolicyLRUMatchesReference(t *testing.T) {
	dc, e := NewDiskCache(DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), MaxEntries: 4, FlushSize: 100000, FlushBytes: 1 << 20, FlushInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	defer dc.Close()
	for seed := int64(0); seed < 10; seed++ {
		dc.Purge()
		r := rand.New(rand.NewSource(seed))
		access := map[string]int{}
		step := 0
		for i := 0; i < 1000; i++ {
			step++
			key := fmt.Sprintf("k%d", r.Intn(12))
			switch r.Intn(4) {
			case 0, 1:
				dc.Set(key, []byte(key), time.Hour)
				access[key] = step
			case 2:
				_, ok := dc.Get(key)
				_, want := access[key]
				if ok != want {
					t.Fatalf("seed%d iter%d hit=%v want=%v key%s", seed, i, ok, want, key)
				}
				if ok {
					access[key] = step
				}
			case 3:
				dc.Flush()
				for len(access) > 4 {
					old := ""
					ts := int(^uint(0) >> 1)
					for k, v := range access {
						if v < ts {
							old = k
							ts = v
						}
					}
					delete(access, old)
				}
				dc.writeMu.Lock()
				if len(dc.index) != len(access) {
					t.Errorf("count mismatch seed%d iter%d", seed, i)
				}
				prev := ^uint64(0)
				seen := 0
				for p := dc.newest; p != nil; p = p.next {
					seen++
					if p.lastAccess > prev {
						t.Errorf("unsorted LRU seed%d iter%d", seed, i)
					}
					prev = p.lastAccess
					if _, ok := access[p.key]; !ok {
						t.Errorf("wrong victim seed%d iter%d kept%s", seed, i, p.key)
					}
					if seen > len(access) {
						break
					}
				}
				dc.writeMu.Unlock()
				if t.Failed() {
					return
				}
			}
		}
	}
}
