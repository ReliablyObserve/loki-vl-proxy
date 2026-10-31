package cache

import (
	"bytes"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLayerAccountingIsolation(t *testing.T) {
	c := NewWithMaxBytes(time.Minute, 2, 10000)
	defer c.Close()
	c.Set("query:a", []byte("abc"))
	c.Set("stats-extent:v1:b", []byte("de"))
	c.Get("query:a")
	c.Get("stats-extent:v1:missing")
	c.drainPromotions()
	s := c.LayerStats()
	if s[LayerLoki].L1.Hits != 1 || s[LayerVL].L1.Misses != 1 || s[LayerLoki].L1.ReadBytes != 3 {
		t.Fatalf("lookup accounting: %+v", s)
	}
	if s[LayerLoki].L1.Bytes != 10 || s[LayerVL].L1.Entries != 1 {
		t.Fatalf("occupancy: %+v", s)
	}
	c.Set("query:c", []byte("f"))
	if c.LayerStats()[LayerVL].L1.Evictions != 1 {
		t.Fatal("LRU removal not attributed")
	}
	c.PurgeAll()
	for _, s := range c.LayerStats() {
		if s.L1.Bytes != 0 || s.L1.Entries != 0 {
			t.Fatal("purge drift")
		}
	}
}
func TestLayerDiskQueueCommitRestart(t *testing.T) {
	cfg := DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), FlushInterval: time.Hour}
	dc, err := NewDiskCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	key := "stats-extent:v1:a"
	dc.Set(key, []byte("abc"), time.Minute)
	if s := dc.LayerStats()[LayerVL]; s.DiskQueueBytes != diskQueueCharge(key, []byte("abc")) || s.L2.Admissions != 1 {
		t.Fatalf("queue: %+v", s)
	}
	if allocations := testing.AllocsPerRun(100, func() { dc.Get(key) }); allocations != 0 {
		t.Fatalf("observed disk queue lookup allocated %v", allocations)
	}
	dc.Get(key)
	dc.Flush()
	s := dc.LayerStats()[LayerVL]
	if s.DiskQueueBytes != 0 || s.L2.Entries != 1 || s.L2.Bytes <= 3 || s.DiskEncodedWriteBytes <= 3 {
		t.Fatalf("commit: %+v", s)
	}
	dc.Get(key)
	if dc.LayerStats()[LayerVL].DiskEncodedReadBytes <= 3 {
		t.Fatal("no encoded read")
	}
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	dc, err = NewDiskCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	if after := dc.LayerStats()[LayerVL]; after.L2.Entries != 1 || after.L2.Bytes != s.L2.Bytes || after.DiskEncodedWriteBytes != 0 || after.DiskIndexBytes != diskIndexCharge(key) {
		t.Fatalf("restart: %+v", after)
	}
	dc.Purge()
	if s := dc.LayerStats()[LayerVL]; s.L2.Entries != 0 || s.L2.Bytes != 0 || s.DiskQueueBytes != 0 || s.DiskIndexBytes != 0 {
		t.Fatalf("purge: %+v", s)
	}
}
func TestLayerClassifierAndExplicitRawKeys(t *testing.T) {
	for key, want := range map[string]Layer{
		"query-steps:v1:a": LayerLoki, "query_range_window:a": LayerLoki,
		"detect_fields:tenant:q": LayerLoki, "detect_labels:tenant:q": LayerLoki,
		"index_stats:tenant:q": LayerLoki, "mt:query:tenant:q": LayerLoki,
		"__patterns_snapshot:v1": LayerLoki, "__label_values_index_snapshot:v1": LayerLoki,
		"stats-extent:v1:a": LayerVL, "sfn:a": LayerVL, "afn:a": LayerVL,
		"vlinv-v1:a": LayerVL, "query_range_window_has_hits:a": LayerVL,
		"secret:query:a": LayerUnknown, "query": LayerUnknown,
		"query-steps:v2:a": LayerUnknown, "stats-extent:v2:a": LayerUnknown,
		"__patterns_snapshot:v2": LayerUnknown, "vlinv:a": LayerUnknown,
		"stats-pushdown-risk:q": LayerUnknown, "level-presence:q": LayerUnknown,
		"level-needs-chain:q": LayerUnknown, "patternsnapshot:q": LayerUnknown,
	} {
		if got := LayerForKey(key); got != want {
			t.Fatalf("%q: %v != %v", key, got, want)
		}
	}
	c := NewForLayer(time.Minute, 2, LayerVL)
	defer c.Close()
	c.Set("raw query", []byte("x"))
	c.Get("raw query")
	if c.LayerStats()[LayerVL].L1.Hits != 1 {
		t.Fatal("explicit instance attribution")
	}
	if NewDisabled().LayerStats() != ([LayerCount]LayerStatsSnapshot{}) {
		t.Fatal("disabled counters")
	}
	var nilCache *Cache
	if nilCache.LayerStats() != ([LayerCount]LayerStatsSnapshot{}) {
		t.Fatal("nil counters")
	}
	if nilCache.DiskCache() != nil || NewDisabled().DiskCache() != nil || c.DiskCache() != nil {
		t.Fatal("nil, disabled or unattached disk accessor")
	}
	var nilDisk *DiskCache
	if nilDisk.LayerStats() != ([LayerCount]LayerStatsSnapshot{}) {
		t.Fatal("nil disk counters")
	}
	disk := policyDisk(t, DiskCacheConfig{})
	c.SetL2(disk)
	if c.DiskCache() != disk {
		t.Fatal("attached disk identity lost")
	}
	ordinary := NewWithMaxBytesForLayer(time.Minute, 2, 10000, Layer(255))
	defer ordinary.Close()
	ordinary.Set("unrecognized:raw", []byte("x"))
	ordinary.Get("unrecognized:raw")
	if ordinary.LayerStats()[LayerUnknown].L1.Hits != 1 || ordinary.LayerStats()[LayerLoki].L1.Entries != 0 || Layer(255).String() != "unknown" {
		t.Fatal("invalid/unknown layer was misattributed")
	}
}

func TestLayerAllMemoryLookupPathsCountOnce(t *testing.T) {
	getters := map[string]func(*Cache, string) bool{
		"get":         func(c *Cache, key string) bool { _, ok := c.Get(key); return ok },
		"ttl":         func(c *Cache, key string) bool { _, _, ok := c.GetWithTTL(key); return ok },
		"age":         func(c *Cache, key string) bool { _, _, _, ok := c.GetWithAge(key); return ok },
		"shared_ttl":  func(c *Cache, key string) bool { _, _, _, ok := c.GetSharedWithTTL(key); return ok },
		"shared_age":  func(c *Cache, key string) bool { _, _, _, _, ok := c.GetSharedWithAge(key); return ok },
		"stale":       func(c *Cache, key string) bool { _, _, ok := c.GetStaleWithTTL(key); return ok },
		"recoverable": func(c *Cache, key string) bool { _, _, _, ok := c.GetRecoverableStaleWithTTL(key); return ok },
		"peer":        func(c *Cache, key string) bool { _, _, _, ok := c.getForPeer(key); return ok },
	}
	for name, get := range getters {
		t.Run(name, func(t *testing.T) {
			c := New(time.Minute, 10)
			defer c.Close()
			c.Set("query:a", []byte("value"))
			if !get(c, "query:a") || get(c, "stats-extent:v1:missing") {
				t.Fatal("incorrect cache answers")
			}
			s := c.LayerStats()
			if s[LayerLoki].L1.Requests != 1 || s[LayerLoki].L1.Hits != 1 || s[LayerLoki].L1.ReadBytes != 5 || s[LayerVL].L1.Misses != 1 {
				t.Fatalf("duplicate/missing accounting: %+v", s)
			}
			if s[LayerLoki].L1.LookupNanoseconds != 0 {
				t.Fatal("L1 unexpectedly times lookups")
			}
			if allocations := testing.AllocsPerRun(100, func() { get(c, "query:a") }); allocations != 0 {
				t.Fatalf("lookup allocated %v", allocations)
			}
		})
	}
}

func TestLayerMemoryReplacementRejectionAndExpiry(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 10, 1000)
	defer c.Close()
	c.Set("query:a", []byte("old"))
	c.Set("query:a", []byte("replacement"))
	c.Set("query:large", bytes.Repeat([]byte("x"), 101))
	s := c.LayerStats()[LayerLoki].L1
	if s.Entries != 1 || s.Bytes != int64(len("query:a")+len("replacement")) || s.Admissions != 2 || s.WriteBytes != 14 || s.Rejections != 1 || s.Evictions != 0 {
		t.Fatalf("replace/reject: %+v", s)
	}
	c.Invalidate("query:a")
	if s := c.LayerStats()[LayerLoki].L1; s.Entries != 0 || s.Bytes != 0 || s.Evictions != 0 {
		t.Fatalf("invalidate: %+v", s)
	}
	c.SetWithTTL("query:a", []byte("stale"), time.Second)
	restore := AdvanceClockForTesting(2 * time.Second)
	defer restore()
	if _, ok := c.Get("query:a"); ok {
		t.Fatal("served expired fresh value")
	}
	if _, _, ok := c.GetStaleWithTTL("query:a"); !ok {
		t.Fatal("lost retained stale value")
	}
	if s := c.LayerStats()[LayerLoki].L1; s.StaleHits != 1 || s.ReadBytes != 5 {
		t.Fatalf("stale: %+v", s)
	}
	c.mu.Lock()
	c.evictIfNeeded(1000)
	c.mu.Unlock()
	if s := c.LayerStats()[LayerLoki].L1; s.Evictions != 1 || s.Entries != 0 || s.Bytes != 0 {
		t.Fatalf("expiry: %+v", s)
	}
}

func TestLayerRejectedStaleCandidatesCountStorageDeliveries(t *testing.T) {
	c := New(time.Hour, 10)
	defer c.Close()
	dc := policyDisk(t, DiskCacheConfig{})
	c.SetL2(dc)
	c.Set("query:stale", []byte("placeholder"))
	dc.Set("query:stale", []byte("disk-placeholder"), time.Hour)
	if _, _, _, ok := c.GetRecoverableStaleWithTTLMatching("query:stale", func([]byte) bool { return false }); ok {
		t.Fatal("returned unusable candidate")
	}
	s := c.LayerStats()[LayerLoki]
	if s.L1.Requests != 1 || s.L1.Misses != 0 || s.L1.Hits != 1 || s.L1.ReadBytes != int64(len("placeholder")) {
		t.Fatalf("RAM storage candidate accounting: %+v", s.L1)
	}
	if s.L2.Requests != 1 || s.L2.Misses != 0 || s.L2.Hits != 1 || s.L2.ReadBytes != int64(len("disk-placeholder")) {
		t.Fatalf("disk storage candidate accounting: %+v", s.L2)
	}
}

func TestLayerDiskExpiryAndRejectedReplacement(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntryBytes: 16})
	dc.Set("query:old", []byte("old"), time.Second)
	dc.Flush()
	dc.Set("stats-extent:v1:bad", []byte("good"), time.Hour)
	dc.Flush()
	dc.Set("stats-extent:v1:bad", bytes.Repeat([]byte("x"), 17), time.Hour)
	dc.Flush()
	s := dc.LayerStats()[LayerVL]
	if s.L2.Rejections != 1 || s.L2.Evictions != 0 || s.L2.Entries != 0 || s.DiskQueueBytes != 0 {
		t.Fatalf("rejected replacement: %+v", s)
	}
	restore := AdvanceClockForTesting(2 * time.Second)
	defer restore()
	dc.Flush()
	assertLayerDiskBalances(t, dc)
	if s := dc.LayerStats()[LayerLoki]; s.L2.Evictions != 1 || s.L2.Entries != 0 || s.L2.Bytes != 0 {
		t.Fatalf("expiry: %+v", s)
	}
}

func TestLayerPeerLookupsSeparateLogicalTraffic(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			pool := newCapacityFleet(t, rf, 20, capacityFleetBytes)
			reader := newNonMemberReader(t, pool)
			for _, key := range []string{"query:layer-proof", "stats-extent:v1:layer-proof"} {
				owner := pool.nodes[pool.replicaIdx(key)[0]]
				value := []byte("decoded-value")
				owner.c.SetLocalAndDiskWithTTL(key, value, time.Hour)
				owner.dc.Flush()
				for round := 0; round < 2; round++ {
					got, _, _, tier, ok := reader.cache.GetSharedWithAge(key)
					if !ok || tier != "l3_peer" || !bytes.Equal(got, value) {
						t.Fatalf("peer answer: %q %s %v", got, tier, ok)
					}
					reader.cache.Invalidate(key)
					owner.c.Invalidate(key)
				}
				layer := LayerForKey(key)
				s := reader.cache.LayerStats()[layer]
				if s.L1.Requests != 2 || s.L1.Misses != 2 || s.L2.Requests != 2 || s.L2.Misses != 2 || s.L3.Requests != 2 || s.L3.Hits != 2 || s.L3.ReadBytes != int64(2*len(value)) || s.L3.LookupNanoseconds <= 0 {
					t.Fatalf("reader accounting: %+v", s)
				}
				if owner.c.LayerStats()[layer].L1.Hits != 1 || owner.dc.LayerStats()[layer].L2.Hits != 1 {
					t.Fatalf("owner RAM/disk delegation: %+v", owner.c.LayerStats()[layer])
				}
				assertLayerDiskBalances(t, reader.disk)
			}
			if _, _, _, _, ok := reader.cache.GetSharedWithAge("stats-extent:v1:absent"); ok {
				t.Fatal("unexpected missing peer key")
			}
			if s := reader.cache.LayerStats()[LayerVL].L3; s.Requests != 3 || s.Hits != 2 || s.Misses != 1 {
				t.Fatalf("peer miss: %+v", s)
			}
		})
	}
}

func TestLayerPeerReadAheadCountsLogicalLookup(t *testing.T) {
	const key = "stats-extent:v1:read-ahead"
	value := []byte("prefetched backend extent")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("X-Cache-TTL-Ms", "60000")
		_, _ = w.Write(value)
	}))
	defer server.Close()
	peer := NewPeerCache(PeerConfig{SelfAddr: "non-member", DiscoveryType: "static", StaticPeers: server.Listener.Addr().String(), Timeout: time.Second, ReadAheadEnabled: true, ReadAheadInterval: time.Hour, ReadAheadMinTTL: time.Second})
	defer peer.Close()
	local := New(time.Hour, 10)
	defer local.Close()
	peer.AttachLocalCache(local)
	if local.l3 != nil {
		t.Fatal("test requires attach-only local cache")
	}
	entry := hotIndexEntry{Key: key, RemainingTTLMS: 60000, SizeBytes: len(value)}
	if !peer.prefetchHotKey(local, entry) {
		t.Fatal("prefetch missed real HTTP peer")
	}
	if !peer.prefetchHotKey(local, entry) {
		t.Fatal("prefetched value was not reusable")
	}
	s := local.LayerStats()[LayerVL]
	if s.L1.Requests != 2 || s.L1.Hits != 1 || s.L1.Misses != 1 || s.L3.Requests != 1 || s.L3.Hits != 1 || s.L3.ReadBytes != int64(len(value)) || s.L3.LookupNanoseconds <= 0 {
		t.Fatalf("read-ahead attribution/delegation: %+v", s)
	}
	if peer.prefetchHotKey(local, hotIndexEntry{Key: "stats-extent:v1:absent", RemainingTTLMS: 60000, SizeBytes: 1}) {
		t.Fatal("missing peer extent prefetched")
	}
	if s := local.LayerStats()[LayerVL].L3; s.Requests != 2 || s.Hits != 1 || s.Misses != 1 {
		t.Fatalf("read-ahead miss not attributed: %+v", s)
	}
}

// Reconcile public gauges against the actual bounded structures in tests only;
// production snapshots never enumerate an index or write queue.
func assertLayerDiskBalances(t *testing.T, dc *DiskCache) {
	t.Helper()
	dc.flushMu.Lock()
	defer dc.flushMu.Unlock()
	s := dc.LayerStats()
	dc.writeMu.Lock()
	defer dc.writeMu.Unlock()
	var entries, logical, index, queue [LayerCount]int64
	for key, e := range dc.index {
		l := LayerForKey(key)
		entries[l]++
		logical[l] += e.size
		index[l] += diskIndexCharge(key)
	}
	for key, e := range dc.writeBuf {
		queue[LayerForKey(key)] += diskQueueCharge(key, e.Value)
	}
	for key, e := range dc.flushing {
		queue[LayerForKey(key)] += diskQueueCharge(key, e.Value)
	}
	for l := LayerUnknown; l < LayerCount; l++ {
		if s[l].L2.Entries != entries[l] || s[l].L2.Bytes != logical[l] || s[l].DiskIndexBytes != index[l] || s[l].DiskQueueBytes != queue[l] {
			t.Fatalf("layer %s drift: %+v want entries=%d logical=%d index=%d queue=%d", l, s[l], entries[l], logical[l], index[l], queue[l])
		}
	}
}

func TestLayerDiskRollbackDoesNotPublishCommitStatistics(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 1})
	dc.Set("query:old", []byte("old"), time.Hour)
	dc.Flush()
	before := dc.LayerStats()
	key := "stats-extent:v1:blocked"
	if err := dc.db.Update(func(tx *bolt.Tx) error { _, err := tx.Bucket(dataBucket).CreateBucket([]byte(key)); return err }); err != nil {
		t.Fatal(err)
	}
	dc.Set(key, []byte("new"), time.Hour)
	dc.Flush()
	after := dc.LayerStats()
	if after[LayerLoki].L2.Evictions != before[LayerLoki].L2.Evictions || after[LayerVL].DiskEncodedWriteBytes != 0 || after[LayerLoki].L2.Entries != 1 || after[LayerVL].L2.Entries != 0 {
		t.Fatalf("rollback published: %+v", after)
	}
	assertLayerDiskBalances(t, dc)
	if err := dc.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(dataBucket).DeleteBucket([]byte(key)) }); err != nil {
		t.Fatal(err)
	}
	dc.Flush()
	assertLayerDiskBalances(t, dc)
	after = dc.LayerStats()
	if after[LayerLoki].L2.Evictions != 1 || after[LayerVL].L2.Entries != 1 || after[LayerVL].DiskEncodedWriteBytes == 0 || after[LayerVL].DiskQueueBytes != 0 {
		t.Fatalf("retry: %+v", after)
	}
}

func TestLayerDiskPendingReplacementWhileFlushing(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%v", rollback), func(t *testing.T) {
			dc := policyDisk(t, DiskCacheConfig{})
			key := "stats-extent:v1:queued"
			dc.Set(key, []byte("old"), time.Hour)
			dc.flushMu.Lock()
			batch, _ := dc.takeDiskBatch()
			if batch == nil {
				dc.flushMu.Unlock()
				t.Fatal("no batch")
			}
			dc.Set(key, []byte("newer"), time.Hour)
			if s := dc.LayerStats()[LayerVL]; s.DiskQueueBytes != diskQueueCharge(key, []byte("old"))+diskQueueCharge(key, []byte("newer")) {
				dc.flushMu.Unlock()
				t.Fatalf("pending and flushing must both be charged: %+v", s)
			}
			batch.encode()
			if rollback {
				batch.finish(errors.New("injected rollback"))
			} else {
				err := dc.db.Update(batch.commit)
				batch.finish(err)
				if err != nil {
					dc.flushMu.Unlock()
					t.Fatal(err)
				}
			}
			s := dc.LayerStats()[LayerVL]
			if s.DiskQueueBytes != diskQueueCharge(key, []byte("newer")) || s.L2.Admissions != 2 || s.L2.WriteBytes != 8 {
				dc.flushMu.Unlock()
				t.Fatalf("newer pending charge lost: %+v", s)
			}
			if rollback && (s.L2.Entries != 0 || s.DiskEncodedWriteBytes != 0) {
				dc.flushMu.Unlock()
				t.Fatalf("rollback committed counters: %+v", s)
			}
			dc.flushMu.Unlock()
			dc.Flush()
			assertLayerDiskBalances(t, dc)
			if got, ok := dc.Get(key); !ok || string(got) != "newer" {
				t.Fatalf("newer value lost: %q %v", got, ok)
			}
		})
	}
}

func TestLayerDiskUncommittedVictimIsRejectionNotEviction(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 1})
	dc.Set("query:hot", []byte("resident"), time.Hour)
	dc.Flush()
	dc.Set("stats-extent:v1:older-queued", []byte("queued"), time.Hour)
	if _, ok := dc.Get("query:hot"); !ok {
		t.Fatal("resident lost before flush")
	}
	dc.Flush()
	assertLayerDiskBalances(t, dc)
	s := dc.LayerStats()
	if s[LayerLoki].L2.Entries != 1 || s[LayerLoki].L2.Evictions != 0 || s[LayerVL].L2.Entries != 0 || s[LayerVL].L2.Rejections != 1 || s[LayerVL].L2.Evictions != 0 || s[LayerVL].DiskEncodedWriteBytes != 0 {
		t.Fatalf("uncommitted victim counted as eviction/write: %+v", s)
	}
	if dc.Evictions.Load() != 1 {
		t.Fatal("legacy eviction behavior changed")
	}
}

func TestLayerDiskStagedReplacementRemovalEvictsOriginal(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("original_expired=%v", expired), func(t *testing.T) {
			dc := policyDisk(t, DiskCacheConfig{MaxEntries: 2})
			ttl := time.Hour
			if expired {
				ttl = time.Second
			}
			dc.Set("query:replace", []byte("original"), ttl)
			dc.Flush()
			dc.Set("query:hot", []byte("resident"), time.Hour)
			dc.Flush()
			if expired {
				restore := AdvanceClockForTesting(2 * time.Second)
				defer restore()
			}
			// Both queued values precede the final hit. The replacement loses to
			// the newer queued key and the recently touched committed resident.
			dc.Set("query:replace", []byte("replacement"), time.Hour)
			dc.Set("stats-extent:v1:newer", []byte("new"), time.Hour)
			if _, ok := dc.Get("query:hot"); !ok {
				t.Fatal("resident lost before flush")
			}
			dc.Flush()
			assertLayerDiskBalances(t, dc)
			s := dc.LayerStats()
			if s[LayerLoki].L2.Evictions != 1 || s[LayerLoki].L2.Rejections != 1 || s[LayerLoki].L2.Entries != 1 || s[LayerVL].L2.Entries != 1 {
				t.Fatalf("original must be evicted exactly once and staged replacement rejected: %+v", s)
			}
			if _, ok := dc.Get("query:replace"); ok {
				t.Fatal("evicted original/replacement remained visible")
			}
			legacyEvictions := int64(1)
			if expired {
				legacyEvictions = 2
			}
			if dc.Evictions.Load() != legacyEvictions {
				t.Fatal("legacy eviction behavior changed")
			}
		})
	}
}

func TestLayerDiskCancelledFlushingChargeRetainedUntilRelease(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntryBytes: 8})
	key := "query:cancelled"
	dc.Set(key, []byte("old"), time.Hour)
	dc.flushMu.Lock()
	batch, _ := dc.takeDiskBatch()
	if batch == nil {
		dc.flushMu.Unlock()
		t.Fatal("no batch")
	}
	dc.Set(key, []byte("too-large"), time.Hour)
	s := dc.LayerStats()[LayerLoki]
	if s.DiskQueueBytes != diskQueueCharge(key, []byte("old")) || s.L2.Rejections != 1 {
		dc.flushMu.Unlock()
		t.Fatalf("cancelled data still held but not charged: %+v", s)
	}
	batch.encode()
	err := dc.db.Update(batch.commit)
	batch.finish(err)
	dc.flushMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	dc.Flush()
	assertLayerDiskBalances(t, dc)
	if s := dc.LayerStats()[LayerLoki]; s.L2.Entries != 0 || s.DiskQueueBytes != 0 || s.L2.Evictions != 0 || s.DiskEncodedWriteBytes != 0 {
		t.Fatalf("cancelled queue counted as committed/evicted: %+v", s)
	}
}

func TestLayerDiskMixedConcurrentMutations(t *testing.T) {
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 12, MaxEntryBytes: 32})
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				prefix := "query:"
				if worker%2 == 0 {
					prefix = "stats-extent:v1:"
				}
				key := fmt.Sprintf("%s%d-%d", prefix, worker, i%8)
				dc.Set(key, []byte("payload"), time.Hour)
				dc.Get(key)
				dc.LayerStats()
				if i%11 == 0 {
					dc.Flush()
				}
			}
		}(worker)
	}
	wg.Wait()
	dc.Flush()
	assertLayerDiskBalances(t, dc)
	s := dc.LayerStats()
	for _, l := range []Layer{LayerLoki, LayerVL} {
		if s[l].L2.Requests != 200 || s[l].L2.Admissions != 200 || s[l].L2.Requests != s[l].L2.Hits+s[l].L2.Misses {
			t.Fatalf("concurrent counters: %+v", s[l])
		}
	}
	dc.Purge()
	assertLayerDiskBalances(t, dc)
}
