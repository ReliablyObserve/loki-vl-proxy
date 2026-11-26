package cache

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// signalsL1 and signalsL2 read one layer's tier signals.
func signalsL1(c *Cache, layer Layer) TierSignalsSnapshot { return c.SignalStats()[layer].L1 }
func signalsL2(dc *DiskCache, layer Layer) TierSignalsSnapshot {
	return dc.SignalStats()[layer].L2
}

// assertSignalsBalance holds the reason counters to the layer counters they
// break down, for every layer: expired + capacity are the layer evictions, and
// every rejection reason but the placement skips sums to the layer rejections.
func assertSignalsBalance(t *testing.T, c *Cache, dc *DiskCache) {
	t.Helper()
	check := func(tier string, layer Layer, s TierSignalsSnapshot, stats LayerTierStatsSnapshot) {
		t.Helper()
		if got := s.Evictions[EvictionExpired] + s.Evictions[EvictionCapacity]; got != stats.Evictions {
			t.Fatalf("%s %s: expired+capacity=%d, layer evictions=%d (%+v)", tier, layer, got, stats.Evictions, s.Evictions)
		}
		rejected := int64(0)
		for r, n := range s.Rejections {
			if !RejectionReason(r).PlacementSkip() {
				rejected += n
			}
		}
		if rejected != stats.Rejections {
			t.Fatalf("%s %s: rejection reasons=%d, layer rejections=%d (%+v)", tier, layer, rejected, stats.Rejections, s.Rejections)
		}
		for i, h := range s.AgeAtEviction {
			if h.Count > s.Evictions[ObservedEvictionReasons[i]] || h.Count != s.HitsAtEviction[i].Count {
				t.Fatalf("%s %s: histogram count %d/%d over %d evictions", tier, layer, h.Count, s.HitsAtEviction[i].Count, s.Evictions[ObservedEvictionReasons[i]])
			}
		}
	}
	for l := LayerUnknown; l < LayerCount; l++ {
		if c != nil {
			check("l1", l, signalsL1(c, l), c.LayerStats()[l].L1)
		}
		if dc != nil {
			check("l2", l, signalsL2(dc, l), dc.LayerStats()[l].L2)
		}
	}
}

func histogramBucket(h HistogramSnapshot) int {
	for i, n := range h.Counts() {
		if n != 0 {
			return i
		}
	}
	return -1
}

// conformance: cache-eviction-signals
func TestEvictionSignalsMemoryReasons(t *testing.T) {
	const layer = LayerLoki
	t.Run("expired", func(t *testing.T) {
		c := NewWithMaxBytes(time.Hour, 2, 1<<20)
		defer c.Close()
		c.SetWithTTL("query:a", []byte("a"), time.Second)
		for i := 0; i < 3; i++ {
			if _, ok := c.Get("query:a"); !ok {
				t.Fatal("miss before expiry")
			}
		}
		c.drainPromotions()
		restore := AdvanceClockForTesting(2 * time.Second)
		defer restore()
		c.Set("query:b", []byte("b"))
		c.Set("query:c", []byte("c")) // full: the expiry sweep removes a
		s := signalsL1(c, layer)
		if s.Evictions[EvictionExpired] != 1 || s.Evictions[EvictionCapacity] != 0 {
			t.Fatalf("evictions %+v", s.Evictions)
		}
		if got := histogramBucket(s.AgeAtEviction[0]); got != 1 { // (1s, 5s]
			t.Fatalf("age bucket %d: %+v", got, s.AgeAtEviction[0])
		}
		if got := histogramBucket(s.HitsAtEviction[0]); got != 3 || s.HitsAtEviction[0].Sum != 3 { // (2, 3]
			t.Fatalf("hits bucket %d: %+v", got, s.HitsAtEviction[0])
		}
		assertSignalsBalance(t, c, nil)
	})
	t.Run("capacity", func(t *testing.T) {
		c := NewWithMaxBytes(time.Hour, 2, 1<<20)
		defer c.Close()
		c.Set("query:a", []byte("a"))
		c.Set("query:b", []byte("b"))
		for i := 0; i < 2; i++ {
			c.Get("query:b")
		}
		c.drainPromotions()
		c.Set("query:c", []byte("c")) // a is least recently used, never hit
		c.Get("query:c")
		c.drainPromotions()
		c.Set("query:d", []byte("d")) // b is now least recently used, 2 hits
		s := signalsL1(c, layer)
		if s.Evictions[EvictionCapacity] != 2 {
			t.Fatalf("evictions %+v", s.Evictions)
		}
		hits := s.HitsAtEviction[1]
		if hits.Count != 2 || hits.Sum != 2 || hits.Counts()[0] != 1 || hits.Counts()[2] != 1 {
			t.Fatalf("hits at capacity eviction %+v", hits)
		}
		assertSignalsBalance(t, c, nil)
	})
	t.Run("shadow_capacity", func(t *testing.T) {
		c := NewWithMaxBytes(time.Hour, 20, 1<<20)
		defer c.Close()
		c.mu.Lock()
		for i := 0; i < 3; i++ { // shadow budget: 20/10 = 2 entries
			c.storeEntryLocked(fmt.Sprintf("query:s%d", i), []byte("v"), time.Minute, 10, false, false, true, 0)
		}
		c.mu.Unlock()
		if s := signalsL1(c, layer); s.Evictions[EvictionCapacity] != 1 {
			t.Fatalf("shadow eviction %+v", s.Evictions)
		}
		assertSignalsBalance(t, c, nil)
	})
	t.Run("replaced", func(t *testing.T) {
		c := NewWithMaxBytes(time.Hour, 10, 1<<20)
		defer c.Close()
		c.Set("query:a", []byte("a"))
		c.Set("query:a", []byte("a2"))                            // in place
		c.SetLocalOnlyWithTTL("query:a", []byte("a3"), time.Hour) // another storage class
		if s := signalsL1(c, layer); s.Evictions[EvictionReplaced] != 2 || s.AgeAtEviction[0].Count+s.AgeAtEviction[1].Count != 0 {
			t.Fatalf("replacements %+v", s)
		}
		c.mu.RLock()
		data := c.lruIndex["query:a"].Value.(*lruEntry)
		c.mu.RUnlock()
		if data.hits != 0 || data.admitted == 0 {
			t.Fatalf("replacement keeps the old value's metadata: %+v", data)
		}
		assertSignalsBalance(t, c, nil)
	})
	t.Run("invalidated_and_flush", func(t *testing.T) {
		c := NewWithMaxBytes(time.Hour, 10, 1<<20)
		defer c.Close()
		for _, k := range []string{"query:a", "query:b1", "query:b2", "query:c", "query:d"} {
			c.Set(k, []byte("v"))
		}
		c.Invalidate("query:a")
		c.Invalidate("query:absent")
		c.InvalidatePrefix("query:b")
		c.PurgeAll()
		s := signalsL1(c, layer)
		if s.Evictions[EvictionInvalidated] != 3 || s.Evictions[EvictionFlush] != 2 {
			t.Fatalf("invalidations/flush %+v", s.Evictions)
		}
		assertSignalsBalance(t, c, nil)
	})
}

// conformance: cache-eviction-signals
func TestEvictionSignalsMemoryRejections(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 5, 1000)
	defer c.Close()
	big := bytes.Repeat([]byte("x"), 200) // over 1000/10
	c.Set("query:big", big)
	c.SetLocalOnlyWithTTL("query:big", big, time.Hour)
	c.SetLocalAndDiskWithTTL("query:big", big, time.Hour)
	c.storePeerCopy("query:big", big, time.Hour, false)
	c.mu.Lock()
	c.storeEntryLocked("query:late", []byte("v"), time.Minute, 10, false, false, false, clockNow().Add(-time.Second).UnixNano())
	c.storeEntryLocked("query:shadow", []byte("v"), time.Minute, 10, false, false, true, 0) // 5/10 = 0 shadow entries
	c.mu.Unlock()
	s := signalsL1(c, LayerLoki)
	if s.Rejections[RejectionSingleEntryBudget] != 4 || s.Rejections[RejectionSourceExpired] != 1 || s.Rejections[RejectionShadowBudget] != 1 {
		t.Fatalf("rejections %+v", s.Rejections)
	}
	assertSignalsBalance(t, c, nil)
}

// The reason split names the first condition Set rejected on before; the
// decision itself must not change for any combination.
//
// conformance: cache-eviction-signals
func TestDiskAdmissionRejectionKeepsTheDecision(t *testing.T) {
	legacy := func(dc *DiskCache, key string, value []byte, ttl time.Duration, charge, used int64) bool {
		reject := key == "" || len(key) > bolt.MaxKeySize || ttl <= 0 || (dc.minTTL > 0 && ttl < dc.minTTL) || len(value) > dc.maxEntryBytes || charge > dc.maxBufferBytes || used > dc.maxBufferBytes-charge
		if !reject && dc.codec == diskCodecRaw && dc.maxBytes > 0 {
			reject = int64(len(key))+int64(diskRecordHeaderSize)+int64(len(value)) > dc.maxBytes
		}
		return reject
	}
	keys := []string{"", "k", strings.Repeat("k", bolt.MaxKeySize+1)}
	values := [][]byte{nil, []byte("v"), bytes.Repeat([]byte("v"), 64)}
	ttls := []time.Duration{-time.Second, 0, time.Second, time.Minute}
	charges := []int64{0, 100, 1000}
	useds := []int64{0, 500, 900, 2000}
	checked := 0
	for _, codec := range []diskCodec{diskCodecRaw, diskCodecS2} {
		for _, minTTL := range []time.Duration{0, 30 * time.Second} {
			for _, maxBytes := range []int64{0, 40, 1 << 20} {
				dc := &DiskCache{codec: codec, minTTL: minTTL, maxBytes: maxBytes, maxEntryBytes: 32, maxBufferBytes: 1000}
				for _, key := range keys {
					for _, value := range values {
						for _, ttl := range ttls {
							for _, charge := range charges {
								for _, used := range useds {
									reason, got := dc.admissionRejection(key, value, ttl, charge, used)
									if want := legacy(dc, key, value, ttl, charge, used); got != want {
										t.Fatalf("decision changed for key=%d value=%d ttl=%s charge=%d used=%d codec=%s min=%s max=%d: %v (%s), want %v", len(key), len(value), ttl, charge, used, codec, minTTL, maxBytes, got, reason, want)
									}
									checked++
								}
							}
						}
					}
				}
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("checked %d combinations", checked)
	}
}

// conformance: cache-eviction-signals
func TestEvictionSignalsDiskRejections(t *testing.T) {
	const layer = LayerLoki
	dc := policyDisk(t, DiskCacheConfig{MinTTL: 30 * time.Second, MaxEntryBytes: 64, MaxBufferBytes: 512})
	dc.Set("query:short", []byte("v"), 10*time.Second)
	dc.Set("query:none", []byte("v"), 0)
	dc.Set("", []byte("v"), time.Hour)
	dc.Set("query:"+strings.Repeat("k", bolt.MaxKeySize), []byte("v"), time.Hour)
	dc.Set("query:big", bytes.Repeat([]byte("v"), 65), time.Hour)
	for i := 0; i < 6; i++ { // 128+key+value charges fill the 512-byte queue
		dc.Set(fmt.Sprintf("query:q%d", i), bytes.Repeat([]byte("v"), 40), time.Hour)
	}
	s := signalsL2(dc, layer)
	if s.Rejections[RejectionTTLBelowMinimum] != 2 || s.Rejections[RejectionTooLarge] != 1 || s.Rejections[RejectionQueueFull] == 0 {
		t.Fatalf("admission rejections %+v", s.Rejections)
	}
	if got := signalsL2(dc, LayerUnknown).Rejections[RejectionInvalidKey]; got != 1 {
		t.Fatalf("empty key %d", got)
	}
	if s.Rejections[RejectionInvalidKey] != 1 {
		t.Fatalf("long key %+v", s.Rejections)
	}
	dc.Flush()
	assertSignalsBalance(t, nil, dc)
	_ = dc.Close()
	dc.Set("query:closed", []byte("v"), time.Hour)
	if s := signalsL2(dc, layer); s.Rejections[RejectionClosed] != 1 {
		t.Fatalf("closed %+v", s.Rejections)
	}
	assertSignalsBalance(t, nil, dc)
}

// conformance: cache-eviction-signals
func TestEvictionSignalsDiskReasons(t *testing.T) {
	const layer = LayerLoki
	t.Run("expired_with_age_and_hits", func(t *testing.T) {
		dc := policyDisk(t, DiskCacheConfig{})
		dc.Set("query:a", []byte("a"), 40*time.Second)
		dc.Get("query:a") // queued hit
		dc.Flush()
		dc.Get("query:a") // committed hit
		restore := AdvanceClockForTesting(50 * time.Second)
		defer restore()
		dc.Flush()
		s := signalsL2(dc, layer)
		if s.Evictions[EvictionExpired] != 1 || histogramBucket(s.AgeAtEviction[0]) != 4 || s.HitsAtEviction[0].Sum != 2 {
			t.Fatalf("expiry %+v", s)
		}
		assertSignalsBalance(t, nil, dc)
	})
	t.Run("capacity_eviction_and_rejection", func(t *testing.T) {
		dc := policyDisk(t, DiskCacheConfig{MaxEntries: 1})
		dc.Set("query:old", []byte("v"), time.Hour)
		dc.Flush()
		dc.Set("query:new", []byte("v"), time.Hour)
		dc.Flush() // old is least recently used
		dc.Set("query:queued", []byte("v"), time.Hour)
		dc.Get("query:new") // touched after the queued write
		dc.Flush()
		s := signalsL2(dc, layer)
		if s.Evictions[EvictionCapacity] != 1 || s.Rejections[RejectionCapacity] != 1 || s.AgeAtEviction[1].Count != 1 {
			t.Fatalf("capacity %+v", s)
		}
		assertSignalsBalance(t, nil, dc)
	})
	t.Run("replaced_invalidated_too_large_flush", func(t *testing.T) {
		dc := policyDisk(t, DiskCacheConfig{MinTTL: 30 * time.Second, Compression: true, Codec: "s2", MaxBytes: 300})
		for _, k := range []string{"query:a", "query:b", "query:c", "query:d"} {
			dc.Set(k, []byte("v"), time.Hour)
		}
		dc.Flush()
		dc.Set("query:a", []byte("v2"), time.Hour) // replaced
		dc.Set("query:b", []byte("v2"), time.Second)
		dc.Flush() // the refused write invalidates b's record
		noise := make([]byte, 400)
		rand.New(rand.NewSource(1)).Read(noise)
		dc.Set("query:c", noise, time.Hour) // too large once encoded
		dc.Flush()
		s := signalsL2(dc, layer)
		if s.Evictions[EvictionReplaced] != 1 || s.Evictions[EvictionInvalidated] != 2 || s.Rejections[RejectionTooLarge] != 1 || s.Rejections[RejectionTTLBelowMinimum] != 1 {
			t.Fatalf("replace/invalidate/too large %+v / %+v", s.Evictions, s.Rejections)
		}
		dc.Purge()
		if s := signalsL2(dc, layer); s.Evictions[EvictionFlush] != 2 {
			t.Fatalf("flush %+v", s.Evictions)
		}
		assertSignalsBalance(t, nil, dc)
	})
	t.Run("startup_scan", func(t *testing.T) {
		path := t.TempDir() + "/cache.db"
		dc := policyDisk(t, DiskCacheConfig{Path: path})
		dc.Set("query:short", []byte("v"), time.Minute)
		dc.Set("query:long", []byte("v"), time.Hour)
		dc.Flush()
		_ = dc.Close()
		restore := AdvanceClockForTesting(2 * time.Minute)
		defer restore()
		dc = policyDisk(t, DiskCacheConfig{Path: path})
		s := signalsL2(dc, layer)
		if s.Evictions[EvictionExpired] != 1 || s.AgeAtEviction[0].Count != 0 {
			t.Fatalf("startup expiry %+v", s)
		}
		assertSignalsBalance(t, nil, dc)
	})
}

// conformance: cache-eviction-signals
func TestEvictionSignalsPlacementAndPromotion(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 100, 1<<20)
	defer c.Close()
	dc := policyDisk(t, DiskCacheConfig{})
	c.SetL2(dc)
	c.SetLocalOnlyWithTTL("query:mem", []byte("v"), time.Hour)
	c.Set("patterns:x", []byte("v"))
	c.SetLocalAndDiskWithTTL("patterns:y", []byte("v"), time.Hour)
	c.Set("query:disk", []byte("v"))
	dc.Flush()
	if s := signalsL2(dc, LayerLoki); s.Rejections[RejectionMemoryOnly] != 3 {
		t.Fatalf("memory-only skips %+v", s.Rejections)
	}
	c.mu.Lock()
	c.removeEntryLocked("query:disk")
	c.mu.Unlock()
	if _, tier, ok := c.GetWithTier("query:disk"); !ok || tier != "l2_disk" {
		t.Fatalf("disk hit %s %v", tier, ok)
	}
	signals := c.SignalStats()[LayerLoki]
	if signals.Promotions[PromotionDiskToMemory] != 1 || signals.DiskReadSeconds.Count != 1 || signals.DiskReadBytes.Count != 1 || signals.DiskReadBytes.Sum <= 1 {
		t.Fatalf("promotion/read %+v", signals)
	}
	assertSignalsBalance(t, c, dc)
}

// conformance: cache-eviction-signals
func TestEvictionSignalsPeerPromotion(t *testing.T) {
	pool := newCapacityFleet(t, 1, 20, capacityFleetBytes)
	reader := newNonMemberReader(t, pool)
	key := "query:signal-proof"
	owner := pool.nodes[pool.replicaIdx(key)[0]]
	owner.c.SetLocalAndDiskWithTTL(key, []byte("value"), time.Hour)
	owner.dc.Flush()
	if _, _, _, tier, ok := reader.cache.GetSharedWithAge(key); !ok || tier != "l3_peer" {
		t.Fatalf("peer answer %s %v", tier, ok)
	}
	s := reader.cache.SignalStats()[LayerLoki]
	if s.Promotions[PromotionPeerToMemory] != 1 || s.Promotions[PromotionPeerToDisk]+s.L2.Rejections[RejectionNotReplica] != 1 {
		t.Fatalf("peer promotion %+v", s)
	}
	assertSignalsBalance(t, reader.cache, reader.disk)
}

// Random operations over both tiers keep the reason counters equal to the
// layer counters they break down, and the per-entry metadata bounded by the
// entries.
//
// conformance: cache-eviction-signals
func TestEvictionSignalsRandomOperationsBalance(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			c := NewWithMaxBytes(time.Hour, 16, 4096)
			defer c.Close()
			dc := policyDisk(t, DiskCacheConfig{MaxEntries: 12, MaxBytes: 2048, MinTTL: 5 * time.Second, MaxEntryBytes: 256})
			c.SetL2(dc)
			var restores []func()
			defer func() {
				for i := len(restores) - 1; i >= 0; i-- {
					restores[i]()
				}
			}()
			prefixes := []string{"query:", "stats-extent:v1:", "other:", "patterns:"}
			for i := 0; i < 2000; i++ {
				key := prefixes[rng.Intn(len(prefixes))] + fmt.Sprint(rng.Intn(40))
				value := bytes.Repeat([]byte("v"), rng.Intn(500))
				ttl := time.Duration(rng.Intn(60)) * time.Second
				switch op := rng.Intn(12); {
				case op < 4:
					c.SetWithTTL(key, value, ttl)
				case op < 5:
					c.SetLocalOnlyWithTTL(key, value, ttl)
				case op < 8:
					c.Get(key)
				case op < 9:
					c.Invalidate(key)
				case op < 10:
					dc.Flush()
				case op < 11:
					restores = append(restores, AdvanceClockForTesting(time.Duration(rng.Intn(20))*time.Second))
				default:
					if rng.Intn(50) == 0 {
						c.PurgeAll()
					}
				}
			}
			c.drainPromotions()
			dc.Flush()
			assertSignalsBalance(t, c, dc)
			if l1 := signalsL1(c, LayerLoki); l1.Evictions[EvictionCapacity] == 0 || l1.Evictions[EvictionReplaced] == 0 || l1.Evictions[EvictionInvalidated] == 0 {
				t.Fatalf("the operations did not reach the reasons: %+v", l1.Evictions)
			}
			c.mu.RLock()
			defer c.mu.RUnlock()
			if len(c.lruIndex) != len(c.entries) || c.lruList.Len() != len(c.entries) {
				t.Fatalf("entry metadata leaked: lru=%d list=%d entries=%d", len(c.lruIndex), c.lruList.Len(), len(c.entries))
			}
		})
	}
}

// Concurrent readers, writers and flushes keep the counters balanced (run with
// -race for the data race check).
//
// conformance: cache-eviction-signals
func TestEvictionSignalsConcurrentBalance(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 32, 1<<16)
	defer c.Close()
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 16})
	c.SetL2(dc)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("query:%d", (i*7+w)%64)
				switch i % 5 {
				case 0, 1:
					c.SetWithTTL(key, []byte("value"), time.Minute)
				case 2, 3:
					c.Get(key)
				default:
					if w == 0 {
						dc.Flush()
					} else {
						c.Invalidate(key)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	c.drainPromotions()
	dc.Flush()
	assertSignalsBalance(t, c, dc)
}

// The signals add no goroutine, and a churned cache keeps no batch state.
//
// conformance: cache-eviction-signals
func TestEvictionSignalsNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		c := NewWithMaxBytes(time.Hour, 8, 1<<20)
		dc := policyDisk(t, DiskCacheConfig{MaxEntries: 4})
		c.SetL2(dc)
		for j := 0; j < 200; j++ {
			c.Set(fmt.Sprintf("query:%d", j), []byte("v"))
		}
		dc.Flush()
		c.Close()
		_ = dc.Close()
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d -> %d", before, after)
	}
}
