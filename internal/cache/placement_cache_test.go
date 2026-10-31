package cache

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func placementLocal(t *testing.T, entries, maxBytes int) (*Cache, *PeerCache) {
	t.Helper()
	c := NewWithMaxBytes(time.Minute, entries, maxBytes)
	pc := NewPeerCache(PeerConfig{SelfAddr: "a:3100", DiscoveryType: "static", StaticPeers: "a:3100,b:3100,c:3100", WriteThrough: true, WriteThroughMinTTL: 30 * time.Second, ReplicationFactor: 1})
	c.SetL3(pc)
	t.Cleanup(func() { pc.Close(); c.Close() })
	return c, pc
}
func placementKeys(pc *PeerCache, owner string, count int) []string {
	keys := make([]string, 0, count)
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprintf("placement-%d", i)
		pc.mu.RLock()
		got := pc.ring.get(key)
		pc.mu.RUnlock()
		if got == owner {
			keys = append(keys, key)
		}
	}
	return keys
}
func TestPlacementPeerShadowResidencyPreservesFreshness(t *testing.T) {
	c, pc := placementLocal(t, 100, 10000)
	key := placementKeys(pc, "b:3100", 1)[0]
	sourceTTL := 240 * time.Second
	c.storePeerCopy(key, []byte("value"), sourceTTL, false)
	_, resident, ok := c.GetWithTTL(key)
	if !ok || resident > nonOwnerShadowMaxTTL {
		t.Fatalf("resident TTL=%s, want <=%s", resident, nonOwnerShadowMaxTTL)
	}
	_, source, age, tier, ok := c.GetSharedWithAge(key)
	if !ok || source < sourceTTL-time.Second || source > sourceTTL || age != -1 || tier != "l1_memory" {
		t.Fatalf("source freshness TTL=%s age=%s tier=%s hit=%v", source, age, tier, ok)
	}
	restore := AdvanceClockForTesting(31 * time.Second)
	defer restore()
	if _, _, ok := c.GetWithTTL(key); ok {
		t.Fatal("shadow residency extended past30seconds")
	}
}
func TestPlacementShadowBudgets(t *testing.T) {
	for _, mode := range []string{"entries", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			entries, payload := 100, 40
			if mode == "bytes" {
				entries = 1000
				payload = 600
			}
			c, pc := placementLocal(t, entries, 10000)
			for _, key := range placementKeys(pc, "b:3100", 100) {
				c.storePeerCopy(key, bytes.Repeat([]byte("x"), payload), time.Minute, false)
			}
			count, size := c.Size()
			if count > entries/10 || size > 10000/10 {
				t.Fatalf("shadows retained %d entries/%d bytes, budget %d/%d", count, size, entries/10, 10000/10)
			}
		})
	}
}
func TestPlacementShadowsNeverEvictOwnedEntries(t *testing.T) {
	c, pc := placementLocal(t, 100, 10000)
	owned := placementKeys(pc, "a:3100", 100)
	for _, key := range owned {
		c.SetWithTTL(key, []byte("owned"), time.Minute)
	}
	for _, key := range placementKeys(pc, "b:3100", 100) {
		c.storePeerCopy(key, []byte("shadow"), time.Minute, false)
	}
	for _, key := range owned {
		if _, _, ok := c.GetWithTTL(key); !ok {
			t.Fatalf("shadow admission evicted owned key %q", key)
		}
	}
}
func TestPlacementUnpublishableWritesStayLocal(t *testing.T) {
	for _, kind := range []string{"filter", "oversize", "short_ttl", "disabled"} {
		t.Run(kind, func(t *testing.T) {
			c, pc := placementLocal(t, 1000, 256<<20)
			dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db", FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			defer dc.Close()
			c.SetL2(dc)
			key := placementKeys(pc, "b:3100", 1)[0]
			value := []byte("value")
			ttl := time.Minute
			switch kind {
			case "filter":
				pc.SetPushFilter(func(string) bool { return false })
			case "oversize":
				value = make([]byte, maxPeerSetBodyBytes+1)
			case "short_ttl":
				pc.wtMinTTL = 2 * time.Minute
			case "disabled":
				pc.writeThrough = false
			}
			c.SetWithTTL(key, value, ttl)
			_, remaining, ok := c.GetWithTTL(key)
			if !ok || remaining < ttl-time.Second {
				t.Fatalf("local fallback lost fullTTL: %s hit%v", remaining, ok)
			}
			dc.Flush()
			got, ok := dc.Get(key)
			if !ok || !bytes.Equal(got, value) {
				t.Fatal("unpublishable write lost localdisk fallback")
			}
		})
	}
}
func TestPlacementForcedReplicationUsesExactlyRDisks(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf%d", rf), func(t *testing.T) {
			f := newReplFleet(t, 3, rf, true, time.Second)
			want := []int{0}
			if rf == 2 {
				want = append(want, 1)
			}
			key := f.keyWith(want...)
			f.nodes[2].c.SetReplicatedWithTTL(key, []byte("value"), 10*time.Minute)
			waitFor(t, "forced replica copies", func() bool { n, _ := f.pushes(); return n >= int64(rf) })
			copies := 0
			for _, node := range f.nodes {
				if onDisk(node, key) {
					copies++
				}
			}
			if copies != rf {
				t.Fatalf("persistentcopies=%d wantR%d", copies, rf)
			}
		})
	}
}
func TestPlacementPeerDiskPromotionDoesNotRewrite(t *testing.T) {
	c, pc := placementLocal(t, 100, 10000)
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db", FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c.SetL2(dc)
	key := placementKeys(pc, "a:3100", 1)[0]
	c.SetLocalAndDiskWithTTL(key, []byte("value"), time.Minute)
	dc.Flush()
	c.Invalidate(key)
	beforeHits, writes, pushes := dc.Hits.Load(), dc.Writes.Load(), pc.WTPushes.Load()
	for i := 0; i < 2; i++ {
		if _, _, _, ok := c.getForPeer(key); !ok {
			t.Fatal("peer cache miss")
		}
	}
	if dc.Hits.Load()-beforeHits != 1 {
		t.Fatal("peer disk result was not reused in ownerL1")
	}
	if dc.Writes.Load() != writes || pc.WTPushes.Load() != pushes {
		t.Fatal("promotion rewrote orreplicated cachedata")
	}
}

func TestPlacementShadowLRUTracksHits(t *testing.T) {
	c, pc := placementLocal(t, 20, 10000)
	keys := placementKeys(pc, "b:3100", 3)
	c.storePeerCopy(keys[0], []byte("first"), time.Minute, false)
	c.storePeerCopy(keys[1], []byte("second"), time.Minute, false)
	c.GetWithTTL(keys[0])
	c.drainPromotions()
	c.storePeerCopy(keys[2], []byte("third"), time.Minute, false)
	if _, _, ok := c.GetWithTTL(keys[0]); !ok {
		t.Fatal("hot shadow was evicted")
	}
	if _, _, ok := c.GetWithTTL(keys[1]); ok {
		t.Fatal("cold shadow survived budget eviction")
	}
}

func TestPlacementDiskPromotionPreservesConcurrentMemoryWrite(t *testing.T) {
	c, _ := placementLocal(t, 100, 10000)
	c.SetLocalOnlyWithTTL("newer", []byte("new"), time.Minute)
	generation, _ := c.diskPromotionGeneration()
	c.promoteDiskWithDeadline("newer", []byte("old"), clockNow().Add(10*time.Second), generation)
	got, ttl, ok := c.GetWithTTL("newer")
	if !ok || string(got) != "new" || ttl < 59*time.Second {
		t.Fatalf("promotion replaced newer memory: %q %s", got, ttl)
	}
}

func TestPlacementOverwriteReusesLRUNodes(t *testing.T) {
	for _, shadow := range []bool{false, true} {
		t.Run(fmt.Sprint(shadow), func(t *testing.T) {
			c, pc := placementLocal(t, 100, 10000)
			owner := "a:3100"
			if shadow {
				owner = "b:3100"
			}
			key := placementKeys(pc, owner, 1)[0]
			value := []byte("value")
			store := func() { c.storePeerCopy(key, value, time.Minute, false) }
			store()
			if allocs := testing.AllocsPerRun(100, store); allocs != 0 {
				t.Fatalf("hot overwrite allocs=%g want0", allocs)
			}
		})
	}
}
func TestPlacementExpiredIncomingKeepsExisting(t *testing.T) {
	c, _ := placementLocal(t, 100, 10000)
	c.SetLocalOnlyWithTTL("key", []byte("fresh"), time.Minute)
	c.mu.Lock()
	c.storeEntryLocked("key", []byte("old"), time.Second, 20, false, false, true, time.Now().Add(-time.Second).UnixNano())
	c.mu.Unlock()
	got, _, ok := c.GetWithTTL("key")
	if !ok || string(got) != "fresh" {
		t.Fatal("expired incoming evicted fresh existing value")
	}
}
func TestPlacementWrongTargetPushDoesNotAddPersistentCopy(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprint(rf), func(t *testing.T) {
			f := newReplFleet(t, 3, rf, true, time.Second)
			owners := []int{0}
			if rf == 2 {
				owners = append(owners, 1)
			}
			key := f.keyWith(owners...)
			req, err := http.NewRequest(http.MethodPost, f.nodes[2].srv.URL+"/_cache/set?key="+url.QueryEscape(key)+"&ttl_ms=600000", bytes.NewBufferString("value"))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("status%d", resp.StatusCode)
			}
			if onDisk(f.nodes[2], key) {
				t.Fatal("wrong-target push persisted nonreplica")
			}
			_, ttl, ok := f.nodes[2].c.GetWithTTL(key)
			if !ok || ttl > nonOwnerShadowMaxTTL {
				t.Fatalf("shadowhit=%v TTL=%s", ok, ttl)
			}
		})
	}
}
func BenchmarkPlacementOverwrite(b *testing.B) {
	for _, shadow := range []bool{false, true} {
		b.Run(fmt.Sprint(shadow), func(b *testing.B) {
			c := NewWithMaxBytes(time.Minute, 100, 10000)
			defer c.Close()
			key := "key"
			value := []byte("value")
			c.mu.Lock()
			c.storeEntryLocked(key, value, time.Minute, entrySize(key, value), false, false, shadow, 0)
			c.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.mu.Lock()
				c.storeEntryLocked(key, value, time.Minute, entrySize(key, value), false, false, shadow, 0)
				c.mu.Unlock()
			}
		})
	}
}

func TestPlacementForcedPushSurvivesRequesterAdmissionLimit(t *testing.T) {
	f := newReplFleet(t, 3, 1, true, time.Second)
	key := f.keyWith(0)
	// The requester has a tighter local entry limit than its designated owner.
	f.nodes[2].c.maxBytes = 10000
	value := bytes.Repeat([]byte("x"), 2000)
	f.nodes[2].c.SetReplicatedWithTTL(key, value, time.Minute)
	waitFor(t, "owner receives forced value", func() bool { n, _ := f.pushes(); return n >= 1 })
	got, _, ok := f.nodes[0].c.GetWithTTL(key)
	if !ok || !bytes.Equal(got, value) {
		t.Fatal("requester L1 admission prevented owner publication")
	}
	if _, _, ok := f.nodes[2].c.GetWithTTL(key); ok {
		t.Fatal("requester exceeded local entry admission cap")
	}
}

// Model a disk lookup suspended immediately before promoting its copied result.
func TestPlacementPurgeFencesCapturedDiskResult(t *testing.T) {
	c := NewWithMaxBytes(time.Minute, 100, 10000)
	defer c.Close()
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db", FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c.SetL2(dc)
	dc.Set("old", []byte("before purge"), time.Minute)
	dc.Flush()
	generation, _ := c.diskPromotionGeneration()
	value, ttl, ok := dc.GetWithTTL("old")
	if !ok {
		t.Fatal("missing fixture")
	}
	c.PurgeAll()
	if c.promoteDiskWithDeadline("old", value, clockNow().Add(ttl), generation) {
		t.Fatal("purged disk snapshot remained eligible")
	}
	if _, _, ok := c.GetWithTTL("old"); ok {
		t.Fatal("inflight disk promotion resurrected purged data")
	}
}

func TestPlacementOverlappingPurgesDisableDiskPromotion(t *testing.T) {
	c := NewWithMaxBytes(time.Minute, 100, 10000)
	defer c.Close()
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/cache.db", FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c.SetL2(dc)
	dc.Set("old", []byte("before purge"), time.Minute)
	dc.Flush()
	value, ttl, ok := dc.GetWithTTL("old")
	if !ok {
		t.Fatal("missing fixture")
	}

	// Both PurgeAll calls can clear L1, then wait here without holding c.mu.
	dc.flushMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			dc.flushMu.Unlock()
		}
	}()
	finished := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() { c.PurgeAll(); finished <- struct{}{} }()
	}
	waitFor(t, "both local purges reached disk", func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.diskPurges == 2
	})
	generation, allowed := c.diskPromotionGeneration()
	if allowed {
		t.Fatal("disk snapshot admitted during purge")
	}
	if c.promoteDiskWithDeadline("old", value, clockNow().Add(ttl), generation) {
		t.Fatal("promotion admitted during purge")
	}
	hits := dc.Hits.Load()
	if _, ok := c.Get("old"); ok {
		t.Fatal("Get read disk during purge")
	}
	if _, _, _, _, ok := c.GetSharedWithAge("old"); ok {
		t.Fatal("shared Get read disk during purge")
	}
	if _, _, _, ok := c.getForPeer("old"); ok {
		t.Fatal("peer Get read disk during purge")
	}
	if dc.Hits.Load() != hits {
		t.Fatal("purge-in-progress reached disk lookup")
	}
	dc.flushMu.Unlock()
	unlocked = true
	for i := 0; i < 2; i++ {
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("purge deadlock")
		}
	}
	if _, allowed := c.diskPromotionGeneration(); !allowed {
		t.Fatal("disk promotion remained disabled after both purges")
	}
	if _, _, ok := c.GetWithTTL("old"); ok {
		t.Fatal("old value reappeared")
	}
	dc.Set("new", []byte("after purge"), time.Minute)
	if value, ok := c.Get("new"); !ok || string(value) != "after purge" {
		t.Fatal("new post-purge disk lookup failed")
	}
	if _, _, ok := c.GetWithTTL("new"); !ok {
		t.Fatal("new post-purge disk result was not promoted")
	}
}

func TestPlacementDiskPromotionRejectsDeadlineExpiredDuringWait(t *testing.T) {
	for _, kind := range []string{"new", "oversize", "existing"} {
		t.Run(kind, func(t *testing.T) {
			c := NewWithMaxBytes(time.Minute, 100, 10000)
			defer c.Close()
			value := []byte("old")
			if kind == "oversize" {
				value = make([]byte, 2000)
			}
			if kind == "existing" {
				c.SetLocalOnlyWithTTL("key", []byte("new"), time.Minute)
			}
			generation, _ := c.diskPromotionGeneration()
			deadline := clockNow().Add(time.Second)
			c.mu.Lock()
			attempted := make(chan struct{})
			result := make(chan bool, 1)
			go func() { close(attempted); result <- c.promoteDiskWithDeadline("key", value, deadline, generation) }()
			<-attempted
			restore := AdvanceClockForTesting(2 * time.Second)
			defer restore()
			c.mu.Unlock()
			select {
			case admitted := <-result:
				if admitted {
					t.Fatal("expired disk snapshot remained serveable")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("promotion deadlock")
			}
		})
	}
}
