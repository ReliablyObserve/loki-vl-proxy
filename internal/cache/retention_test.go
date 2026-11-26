package cache

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func retentionCache(t *testing.T, maxEntries, maxBytes int, p RetentionPolicy) *Cache {
	t.Helper()
	c := NewWithMaxBytes(time.Hour, maxEntries, maxBytes)
	t.Cleanup(c.Close)
	if err := c.SetRetention(p); err != nil {
		t.Fatal(err)
	}
	return c
}

func holdsKeys(c *Cache, keys ...string) (held []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, k := range keys {
		if _, ok := c.entries[k]; ok {
			held = append(held, k)
		}
	}
	return held
}

// checkSegments holds the segment lists to the entries: every node is in the
// list its seg names (shadows in the shadow list, others on probation, in the
// protected segment, or in none), the protected counters are the sum of its
// entries, and the protected segment fits its share.
func checkSegments(t *testing.T, c *Cache) {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()
	walk := func(name string, l *segList, want segment) map[*lruEntry]bool {
		seen := map[*lruEntry]bool{}
		n := 0
		var prev *segNode
		for node := l.front; node != nil; node = node.next {
			if node.prev != prev || node.data == nil || node.data.node != node || node.data.seg != want {
				t.Fatalf("%s list: broken node %+v", name, node)
			}
			seen[node.data] = true
			prev, n = node, n+1
		}
		if l.back != prev || l.n != n {
			t.Fatalf("%s list: back/len %v/%d, walked %d", name, l.back == prev, l.n, n)
		}
		return seen
	}
	shadow := walk("shadow", &c.shadowList, segShadow)
	var probation, protected map[*lruEntry]bool
	r := c.retention
	if r != nil {
		probation, protected = walk("probation", &r.probation, segProbation), walk("protected", &r.protected, segProtected)
	}
	protBytes := 0
	for key, elem := range c.lruIndex {
		data := elem.Value.(*lruEntry)
		e, ok := c.entries[key]
		if !ok || data.key != key {
			t.Fatalf("%s: lru node without its entry", key)
		}
		switch {
		case e.shadow:
			if !shadow[data] {
				t.Fatalf("%s: shadow not in the shadow list (seg %d)", key, data.seg)
			}
		case data.seg == segProbation:
			if !probation[data] {
				t.Fatalf("%s: probation node not in its list", key)
			}
		case data.seg == segProtected:
			if !protected[data] {
				t.Fatalf("%s: protected node not in its list", key)
			}
			protBytes += e.sizeBytes
		case data.seg != segNone || data.node != nil:
			t.Fatalf("%s: seg %d node %v", key, data.seg, data.node != nil)
		case r != nil && r.on && data.admitted != 0:
			t.Fatalf("%s: stored under protection but in no segment", key)
		}
	}
	if len(shadow)+len(probation)+len(protected) > len(c.lruIndex) {
		t.Fatalf("segment lists hold %d nodes for %d entries", len(shadow)+len(probation)+len(protected), len(c.lruIndex))
	}
	if r != nil && r.on {
		if r.protBytes != protBytes || r.protCount != len(protected) {
			t.Fatalf("protected counters %d bytes/%d, entries hold %d/%d", r.protBytes, r.protCount, protBytes, len(protected))
		}
		if r.protBytes > r.maxProtBytes || r.protCount > r.maxProtCount {
			t.Fatalf("protected %d bytes/%d over its share %d/%d", r.protBytes, r.protCount, r.maxProtBytes, r.maxProtCount)
		}
	}
	if c.segFreeN > segFreeMax {
		t.Fatalf("%d free nodes kept", c.segFreeN)
	}
}

// conformance: cache-reuse-retention
func TestRetentionPolicyValidation(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 10, 1<<20)
	defer c.Close()
	for _, pct := range []int{-1, MaxRetentionProtectPercent + 1} {
		if err := c.SetRetention(RetentionPolicy{ProtectPercent: pct}); err == nil {
			t.Fatalf("percent %d accepted", pct)
		}
	}
	if err := c.SetRetention(RetentionPolicy{ProtectPercent: 20}); err != nil || c.retention == nil || !c.retention.on {
		t.Fatalf("policy not set: %v", err)
	}
	if err := c.SetRetention(RetentionPolicy{}); err != nil || c.retention != nil {
		t.Fatalf("zero policy keeps plain LRU: %v %+v", err, c.retention)
	}
	c.Set("query:a", []byte("a"))
	if err := c.SetRetention(RetentionPolicy{ProtectPercent: 20}); err == nil {
		t.Fatal("policy changed after the cache stored entries")
	}
	if err := NewDisabled().SetRetention(RetentionPolicy{ProtectPercent: 20}); err != nil {
		t.Fatalf("disabled cache: %v", err)
	}
}

// A capacity flood of entries stored once evicts the entries read again under
// LRU; with protection they stay, and the flood evicts itself.
//
// conformance: cache-reuse-retention
func TestRetentionProtectsReusedEntriesFromAFlood(t *testing.T) {
	hot := []string{"query:h0", "query:h1", "query:h2", "query:h3"}
	for _, tc := range []struct {
		name   string
		policy RetentionPolicy
		kept   int
	}{{"lru", RetentionPolicy{}, 0}, {"protect50", RetentionPolicy{ProtectPercent: 50}, len(hot)}} {
		t.Run(tc.name, func(t *testing.T) {
			c := retentionCache(t, 10, 1<<20, tc.policy)
			for _, k := range hot {
				c.Set(k, []byte("v"))
				c.Get(k)
			}
			c.drainPromotions()
			for i := 0; i < 30; i++ {
				c.Set(fmt.Sprintf("query:flood%d", i), []byte("v"))
			}
			if got := holdsKeys(c, hot...); len(got) != tc.kept {
				t.Fatalf("kept %v, want %d of the read entries", got, tc.kept)
			}
			checkSegments(t, c)
			assertSignalsBalance(t, c, nil)
			if s := signalsL1(c, LayerLoki); s.Evictions[EvictionCapacity] != 4+30-10 {
				t.Fatalf("capacity evictions %+v", s.Evictions)
			}
		})
	}
}

// The protected segment holds at most its share: the least recently read
// protected entry goes back to probation, and is evicted before newer ones.
//
// conformance: cache-reuse-retention
func TestRetentionProtectedShareIsBounded(t *testing.T) {
	c := retentionCache(t, 10, 1<<20, RetentionPolicy{ProtectPercent: 30}) // 3 entries
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("query:r%d", i)
		c.Set(k, []byte("v"))
		c.Get(k)
		c.drainPromotions()
	}
	checkSegments(t, c)
	c.mu.RLock()
	got := c.retention.protCount
	firstSeg := c.lruIndex["query:r0"].Value.(*lruEntry).seg
	c.mu.RUnlock()
	if got != 3 || firstSeg != segProbation {
		t.Fatalf("protected %d, r0 in segment %d", got, firstSeg)
	}
	for i := 0; i < 5; i++ { // fills the cache: r0, then r1 leave first
		c.Set(fmt.Sprintf("query:new%d", i), []byte("v"))
	}
	c.Set("query:new5", []byte("v"))
	c.Set("query:new6", []byte("v"))
	if got := holdsKeys(c, "query:r0", "query:r1", "query:r2", "query:r3", "query:r4"); fmt.Sprint(got) != "[query:r2 query:r3 query:r4]" {
		t.Fatalf("held %v", got)
	}
	checkSegments(t, c)
}

// The byte share binds as well as the entry share.
//
// conformance: cache-reuse-retention
func TestRetentionProtectedByteShare(t *testing.T) {
	c := retentionCache(t, 100, 10000, RetentionPolicy{ProtectPercent: 20}) // 2000 bytes
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("query:b%d", i)
		c.Set(k, bytes.Repeat([]byte("v"), 900))
		c.Get(k)
		c.drainPromotions()
	}
	checkSegments(t, c)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.retention.protCount != 2 || c.retention.protBytes > 2000 {
		t.Fatalf("protected %d entries, %d bytes", c.retention.protCount, c.retention.protBytes)
	}
}

// Expiry is the same with the policy on: a protected entry is not served past
// its TTL and leaves as expired.
//
// conformance: cache-reuse-retention
func TestRetentionNeverExtendsTTL(t *testing.T) {
	for _, p := range []RetentionPolicy{{}, {ProtectPercent: 50}, {ProtectPercent: 50, DemoteOnEvict: true}} {
		c := retentionCache(t, 2, 1<<20, p)
		c.SetWithTTL("query:a", []byte("a"), time.Second)
		c.Get("query:a")
		c.drainPromotions()
		restore := AdvanceClockForTesting(2 * time.Second)
		if _, ok := c.Get("query:a"); ok {
			t.Fatalf("%+v: served past its TTL", p)
		}
		c.Set("query:b", []byte("b"))
		c.Set("query:c", []byte("c"))
		restore()
		if s := signalsL1(c, LayerLoki); s.Evictions[EvictionExpired] != 1 || s.Evictions[EvictionCapacity] != 0 {
			t.Fatalf("%+v: evictions %+v", p, s.Evictions)
		}
		checkSegments(t, c)
	}
}

// Entries the restart warm-up admits are in no segment: never read, they are
// evicted first (they sit at the back of the recency list); read, they join
// the protected segment.
//
// conformance: cache-reuse-retention
func TestRetentionRestoredEntries(t *testing.T) {
	c := retentionCache(t, 4, 1<<20, RetentionPolicy{ProtectPercent: 50})
	generation, _ := c.diskPromotionGeneration()
	var restored int64
	entries := []*restoreEntry{
		{key: "query:w0", value: []byte("v"), expiry: clockNow().Add(time.Hour).UnixNano()},
		{key: "query:w1", value: []byte("v"), expiry: clockNow().Add(time.Hour).UnixNano()},
	}
	if !c.admitRestoreBatch(entries, generation, &restored, 1<<20, nil) {
		t.Fatal("restore not admitted")
	}
	c.Get("query:w1")
	c.drainPromotions()
	c.Set("query:a", []byte("v"))
	c.Set("query:b", []byte("v"))
	checkSegments(t, c)
	c.Set("query:c", []byte("v")) // full: w0 (no segment, oldest) leaves
	if got := holdsKeys(c, "query:w0", "query:w1"); fmt.Sprint(got) != "[query:w1]" {
		t.Fatalf("held %v", got)
	}
	c.mu.RLock()
	seg := c.lruIndex["query:w1"].Value.(*lruEntry).seg
	c.mu.RUnlock()
	if seg != segProtected {
		t.Fatalf("read warm-up entry in segment %d", seg)
	}
	checkSegments(t, c)
}

// A capacity eviction of an entry read once keeps its disk copy newest in the
// disk's recency order, so the disk evicts an older record instead.
//
// conformance: cache-reuse-retention
func TestRetentionDemoteKeepsTheDiskCopy(t *testing.T) {
	for _, demote := range []bool{false, true} {
		t.Run(fmt.Sprint(demote), func(t *testing.T) {
			c := retentionCache(t, 2, 1<<20, RetentionPolicy{DemoteOnEvict: demote})
			dc := policyDisk(t, DiskCacheConfig{MaxEntries: 3})
			c.SetL2(dc)
			c.SetWithTTL("query:a", []byte("a"), time.Hour)
			dc.Flush()
			c.Get("query:a")
			c.drainPromotions()
			c.SetWithTTL("query:b", []byte("b"), time.Hour)
			dc.Flush()
			c.SetWithTTL("query:c", []byte("c"), time.Hour) // evicts a, read once
			dc.Flush()
			c.SetWithTTL("query:d", []byte("d"), time.Hour) // evicts b, never read; the disk is full: its oldest record leaves
			dc.Flush()
			_, aOnDisk := dc.Get("query:a")
			_, bOnDisk := dc.Get("query:b")
			if aOnDisk != demote || bOnDisk == demote {
				t.Fatalf("demote=%v: a on disk %v, b on disk %v", demote, aOnDisk, bOnDisk)
			}
			want := int64(0)
			if demote {
				want = 1
			}
			if s := c.SignalStats()[LayerLoki]; s.Demotions[DemotionRetained] != want || s.Demotions[DemotionWritten] != 0 {
				t.Fatalf("demotions %+v", s.Demotions)
			}
			assertSignalsBalance(t, c, dc)
		})
	}
}

// When the disk holds no copy (its own capacity evicted it), the demotion
// writes the value with the TTL it has left in memory, never more.
//
// conformance: cache-reuse-retention
func TestRetentionDemoteWritesWithTheRemainingTTL(t *testing.T) {
	c := retentionCache(t, 2, 1<<20, RetentionPolicy{DemoteOnEvict: true})
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 1, MinTTL: 30 * time.Second})
	c.SetL2(dc)
	c.SetWithTTL("query:a", []byte("A"), 10*time.Minute)
	dc.Flush()
	c.SetWithTTL("query:b", []byte("B"), 10*time.Minute)
	dc.Flush() // the disk keeps b only
	if _, ok := dc.Get("query:a"); ok {
		t.Fatal("disk still holds a")
	}
	c.Get("query:a")
	c.drainPromotions()
	restore := AdvanceClockForTesting(4 * time.Minute)
	defer restore()
	c.Get("query:b")
	c.drainPromotions()
	c.SetWithTTL("query:c", []byte("C"), 10*time.Minute) // evicts a, read once, 6m left
	v, ttl, ok := dc.GetWithTTL("query:a")
	if !ok || string(v) != "A" || ttl > 6*time.Minute || ttl < 5*time.Minute {
		t.Fatalf("demoted copy %q %v %v", v, ttl, ok)
	}
	if s := c.SignalStats()[LayerLoki]; s.Demotions[DemotionWritten] != 1 {
		t.Fatalf("demotions %+v", s.Demotions)
	}
	restore2 := AdvanceClockForTesting(6*time.Minute + time.Second)
	defer restore2()
	if _, ok := dc.Get("query:a"); ok {
		t.Fatal("the demoted copy outlived the original TTL")
	}
}

// Demotion leaves alone what the disk may not hold or need not: entries never
// read, memory-only values, patterns, shadows, entries with less than the
// disk's minimum TTL left, and everything during a purge.
//
// conformance: cache-reuse-retention
func TestRetentionDemoteSkips(t *testing.T) {
	type setup func(c *Cache, key string)
	read := func(c *Cache, key string) { c.Get(key); c.drainPromotions() }
	for _, tc := range []struct {
		name  string
		key   string
		store setup
	}{
		{"never read", "query:x", func(c *Cache, key string) { c.SetWithTTL(key, []byte("v"), time.Hour) }},
		{"memory only", "query:x", func(c *Cache, key string) { c.SetLocalOnlyWithTTL(key, []byte("v"), time.Hour); read(c, key) }},
		{"patterns", "patterns:x", func(c *Cache, key string) { c.SetWithTTL(key, []byte("v"), time.Hour); read(c, key) }},
		{"below minimum TTL", "query:x", func(c *Cache, key string) { c.SetWithTTL(key, []byte("v"), 10*time.Second); read(c, key) }},
		{"source deadline below minimum TTL", "query:x", func(c *Cache, key string) {
			c.mu.Lock()
			c.storeEntryLocked(key, []byte("v"), time.Hour, entrySize(key, []byte("v")), false, false, false, clockNow().Add(10*time.Second).UnixNano())
			c.mu.Unlock()
			read(c, key)
		}},
		{"shadow", "query:x", func(c *Cache, key string) {
			c.mu.Lock()
			c.storeEntryLocked(key, []byte("v"), time.Hour, entrySize(key, []byte("v")), false, false, true, 0)
			c.mu.Unlock()
			read(c, key)
		}},
		{"purge", "query:x", func(c *Cache, key string) {
			c.SetWithTTL(key, []byte("v"), time.Hour)
			read(c, key)
			c.mu.Lock()
			c.diskPurges++
			c.mu.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := retentionCache(t, 20, 1<<20, RetentionPolicy{DemoteOnEvict: true})
			dc := policyDisk(t, DiskCacheConfig{MinTTL: 30 * time.Second})
			c.SetL2(dc)
			tc.store(c, tc.key)
			dc.Purge() // the disk holds nothing: a demotion would write
			c.mu.Lock()
			now := clockNow()
			data := c.lruNodeLocked(tc.key)
			if data == nil {
				c.mu.Unlock()
				t.Fatal("not stored")
			}
			c.demoteLocked(data, now)
			c.mu.Unlock()
			if s := c.SignalStats()[LayerForKey(tc.key)]; s.Demotions != [DemotionCount]int64{} {
				t.Fatalf("demoted: %+v", s.Demotions)
			}
			if _, ok := dc.Get(tc.key); ok {
				t.Fatal("written to disk")
			}
		})
	}
}

// Random operations over every write path, with shadows, restores, purges,
// replacements, invalidations and clock moves, keep the segments consistent,
// the protected share bounded and the eviction counters balanced.
//
// conformance: cache-reuse-retention
func TestRetentionRandomOperations(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			c := retentionCache(t, 16, 4096, RetentionPolicy{ProtectPercent: 10 + 15*int(seed%5), DemoteOnEvict: seed%2 == 0})
			dc := policyDisk(t, DiskCacheConfig{MaxEntries: 12, MaxBytes: 2048, MinTTL: 5 * time.Second, MaxEntryBytes: 256})
			c.SetL2(dc)
			var restores []func()
			defer func() {
				for i := len(restores) - 1; i >= 0; i-- {
					restores[i]()
				}
			}()
			prefixes := []string{"query:", "stats-extent:v1:", "other:", "patterns:"}
			for i := 0; i < 3000; i++ {
				key := prefixes[rng.Intn(len(prefixes))] + fmt.Sprint(rng.Intn(40))
				value := bytes.Repeat([]byte("v"), rng.Intn(500))
				ttl := time.Duration(rng.Intn(60)) * time.Second
				switch op := rng.Intn(16); {
				case op < 4:
					c.SetWithTTL(key, value, ttl)
				case op < 5:
					c.SetLocalOnlyWithTTL(key, value, ttl)
				case op < 6:
					c.mu.Lock()
					c.storeEntryLocked(key, value, ttl, entrySize(key, value), false, false, true, 0)
					c.mu.Unlock()
				case op < 7:
					generation, _ := c.diskPromotionGeneration()
					var restored int64
					c.admitRestoreBatch([]*restoreEntry{{key: key, value: value, expiry: clockNow().Add(ttl).UnixNano()}}, generation, &restored, 1<<20, nil)
				case op < 11:
					c.Get(key)
					if rng.Intn(2) == 0 {
						c.drainPromotions()
					}
				case op < 12:
					c.Invalidate(key)
				case op < 13:
					dc.Flush()
				case op < 14:
					restores = append(restores, AdvanceClockForTesting(time.Duration(rng.Intn(20))*time.Second))
				case op < 15:
					if rng.Intn(30) == 0 {
						c.PurgeAll()
					}
				default:
					c.InvalidatePrefix(prefixes[rng.Intn(len(prefixes))])
				}
				if i%50 == 0 {
					c.drainPromotions()
					checkSegments(t, c)
				}
			}
			c.drainPromotions()
			dc.Flush()
			checkSegments(t, c)
			assertSignalsBalance(t, c, dc)
			c.mu.RLock()
			defer c.mu.RUnlock()
			if len(c.lruIndex) != len(c.entries) || c.lruList.Len() != len(c.entries) {
				t.Fatalf("entry metadata leaked: lru=%d list=%d entries=%d", len(c.lruIndex), c.lruList.Len(), len(c.entries))
			}
		})
	}
}

// Concurrent readers, writers, flushes and invalidations with the policy on
// (run with -race).
//
// conformance: cache-reuse-retention
func TestRetentionConcurrent(t *testing.T) {
	c := retentionCache(t, 32, 1<<16, RetentionPolicy{ProtectPercent: 20, DemoteOnEvict: true})
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 16})
	c.SetL2(dc)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("query:%d", (i*7+w)%64)
				switch i % 6 {
				case 0, 1:
					c.SetWithTTL(key, []byte("value"), time.Minute)
				case 2, 3:
					c.Get(key)
				case 4:
					if w == 0 {
						dc.Flush()
					} else {
						c.Invalidate(key)
					}
				default:
					c.SetLocalOnlyWithTTL(key, []byte("v"), time.Minute)
				}
			}
		}(w)
	}
	wg.Wait()
	c.drainPromotions()
	dc.Flush()
	checkSegments(t, c)
	assertSignalsBalance(t, c, dc)
}
