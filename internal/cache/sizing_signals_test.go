package cache

import (
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"
)

func sizingCache(t *testing.T, maxEntries, maxBytes int) *Cache {
	t.Helper()
	c := NewWithMaxBytes(time.Hour, maxEntries, maxBytes)
	t.Cleanup(c.Close)
	c.EnableSizingSignals()
	return c
}

func sizingFlush(c *Cache) SizingSnapshot {
	c.sizing.flush()
	return c.SizingStats()
}

// sizingRequest is a request-level lookup that stores the answer on a full
// miss, as the handlers do.
func sizingRequest(c *Cache, key string, value []byte, ttl time.Duration) {
	if _, _, ok := c.GetWithTier(key); !ok {
		c.SetWithTTL(key, value, ttl)
	}
}

func near(got, want int64, tolerance float64) bool {
	return math.Abs(float64(got-want)) <= tolerance*float64(want)
}

// sampledKeys is how many of the keys fmt.Sprintf(format, i), i < n, the
// signals sample (the hash seed is random per process): the scaled figures
// are exact multiples of it.
func sampledKeys(c *Cache, format string, n int) int64 {
	k := int64(0)
	for i := 0; i < n; i++ {
		if _, ok := c.sizing.sampled(fmt.Sprintf(format, i)); ok {
			k++
		}
	}
	return k
}

// conformance: cache-sizing-signals
func TestSizingEndpointClassForKey(t *testing.T) {
	for key, want := range map[string]EndpointClass{
		"query_range:abc": ClassQueryRange, "query-steps:v1:x": ClassQueryRange, "query_range_window:x": ClassQueryRange,
		"query:x": ClassQuery, "series:x": ClassSeries, "labels:x": ClassLabels, "label_values:x": ClassLabels,
		"label_inventory:x": ClassLabels, "vlinv-v1:x": ClassLabels, "volume:x": ClassVolume, "volume_range:x": ClassVolume,
		"index_stats:x": ClassVolume, "detected_fields:x": ClassDetected, "detected_labels:x": ClassDetected,
		"patterns:x": ClassPatterns, "stats-extent:v1:x": ClassOther, "compat:x": ClassOther, "": ClassOther, "query_range": ClassQueryRange,
	} {
		if got := EndpointClassForKey(key); got != want {
			t.Errorf("%q: %s, want %s", key, got, want)
		}
	}
}

// Off by default: no signals and nothing exported.
//
// conformance: cache-sizing-signals
func TestSizingOffByDefault(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 10, 1<<20)
	defer c.Close()
	sizingRequest(c, "query:a", []byte("v"), time.Minute)
	sizingRequest(c, "query:a", []byte("v"), time.Minute)
	if s := c.SizingStats(); s.Enabled || s != (SizingSnapshot{}) {
		t.Fatalf("signals without EnableSizingSignals: %+v", s)
	}
	if s := NewDisabled().SizingStats(); s.Enabled {
		t.Fatal("disabled cache")
	}
}

// Hit bytes by tier and requested bytes: hits plus the store that fills a full
// miss; a store without a miss is not a request.
//
// conformance: cache-sizing-signals
func TestSizingByteHitRatio(t *testing.T) {
	c := NewWithMaxBytes(time.Hour, 100, 1<<20)
	defer c.Close()
	dc := policyDisk(t, DiskCacheConfig{})
	c.SetL2(dc)
	c.EnableSizingSignals()
	sizingRequest(c, "query_range:a", make([]byte, 100), time.Hour) // miss, fill 100
	sizingRequest(c, "query_range:a", make([]byte, 100), time.Hour) // L1 hit 100
	c.SetWithTTL("labels:b", make([]byte, 40), time.Hour)           // no request
	sizingRequest(c, "labels:b", nil, time.Hour)                    // L1 hit 40
	dc.Flush()
	c.Invalidate("query_range:a")
	sizingRequest(c, "query_range:a", nil, time.Hour) // L2 hit 100
	s := sizingFlush(c)
	if got := s.HitBytes[ClassQueryRange]; got != [hitTierCount]int64{100, 100, 0} {
		t.Fatalf("query_range hit bytes %v", got)
	}
	if got := s.HitBytes[ClassLabels]; got != [hitTierCount]int64{40, 0, 0} {
		t.Fatalf("labels hit bytes %v", got)
	}
	if s.RequestedBytes[ClassQueryRange] != 300 || s.RequestedBytes[ClassLabels] != 40 {
		t.Fatalf("requested %v", s.RequestedBytes)
	}
}

// The pending misses are bounded: a flood of misses never stored keeps at
// most sizingPendingMax keys.
//
// conformance: cache-sizing-signals
func TestSizingPendingMissesBounded(t *testing.T) {
	c := sizingCache(t, 10, 1<<20)
	for i := 0; i < 3*sizingPendingMax; i++ {
		c.GetWithTier(fmt.Sprintf("query:%d", i))
	}
	s := c.sizing
	s.pendingMu.Lock()
	n := len(s.pending)
	s.pendingMu.Unlock()
	if n > sizingPendingMax {
		t.Fatalf("%d pending misses", n)
	}
}

// A cycle over more keys than the memory tier holds misses every lookup under
// LRU; a tier twice as large would have answered them: the 2x_bytes ghost
// counts about that many hits, the 2x_ttl ghost none.
//
// conformance: cache-sizing-signals
func TestSizingGhostDoubleBytes(t *testing.T) {
	const entries, universe, rounds = 1600, 2400, 4
	c := sizingCache(t, entries, 1<<30)
	value := make([]byte, 64)
	for r := 0; r < rounds; r++ {
		for i := 0; i < universe; i++ {
			sizingRequest(c, fmt.Sprintf("query_range:k%d", i), value, time.Hour)
			if i%256 == 0 {
				c.sizing.flush()
			}
		}
	}
	s := sizingFlush(c)
	// Every sampled lookup after the first round, scaled; about 7200.
	want := sampledKeys(c, "query_range:k%d", universe) * sizingSampleRate * (rounds - 1)
	if got := s.GhostHits[sizingTierL1][GhostDoubleBytes]; got != want || !near(got, (rounds-1)*universe, 0.4) {
		t.Fatalf("2x_bytes ghost hits %d, want %d", got, want)
	}
	if got := s.GhostHits[sizingTierL1][GhostDoubleTTL]; got != 0 {
		t.Fatalf("2x_ttl ghost hits %d", got)
	}
}

// Keys asked again just after their TTL: a doubled TTL would have answered
// them, more room would not.
//
// conformance: cache-sizing-signals
func TestSizingGhostDoubleTTL(t *testing.T) {
	const universe, rounds = 2000, 4
	c := sizingCache(t, 10*universe, 1<<30)
	var advanced time.Duration
	defer func() { AdvanceClockForTesting(-advanced) }()
	for r := 0; r < rounds; r++ {
		for i := 0; i < universe; i++ {
			sizingRequest(c, fmt.Sprintf("query_range:t%d", i), make([]byte, 64), 10*time.Second)
		}
		c.sizing.flush()
		AdvanceClockForTesting(15 * time.Second)
		advanced += 15 * time.Second
	}
	s := sizingFlush(c)
	want := sampledKeys(c, "query_range:t%d", universe) * sizingSampleRate * (rounds - 1)
	if got := s.GhostHits[sizingTierL1][GhostDoubleTTL]; got != want || !near(got, (rounds-1)*universe, 0.4) {
		t.Fatalf("2x_ttl ghost hits %d, want %d", got, want)
	}
	if got := s.GhostHits[sizingTierL1][GhostDoubleBytes]; got != 0 {
		t.Fatalf("2x_bytes ghost hits %d", got)
	}
}

// The disk ghosts follow L2 lookups and writes: a disk that holds two thirds
// of the cycle misses what twice its budget would hold.
//
// conformance: cache-sizing-signals
func TestSizingGhostDisk(t *testing.T) {
	const universe, rounds = 2400, 3
	c := NewWithMaxBytes(time.Hour, 16, 1<<30) // memory holds almost nothing
	defer c.Close()
	dc := policyDisk(t, DiskCacheConfig{MaxEntries: 1600})
	c.SetL2(dc)
	c.EnableSizingSignals()
	for r := 0; r < rounds; r++ {
		for i := 0; i < universe; i++ {
			sizingRequest(c, fmt.Sprintf("query_range:d%d", i), make([]byte, 64), time.Hour)
			if i%200 == 0 {
				dc.Flush()
				c.sizing.flush()
			}
		}
		dc.Flush()
	}
	s := sizingFlush(c)
	want := sampledKeys(c, "query_range:d%d", universe) * sizingSampleRate * (rounds - 1)
	if got := s.GhostHits[sizingTierL2][GhostDoubleBytes]; got != want || !near(got, (rounds-1)*universe, 0.4) {
		t.Fatalf("L2 2x_bytes ghost hits %d, want %d", got, want)
	}
}

// The working set counts distinct keys and bytes per window, and forgets keys
// older than the largest window.
//
// conformance: cache-sizing-signals
func TestSizingWorkingSet(t *testing.T) {
	c := sizingCache(t, 100000, 1<<30)
	var advanced time.Duration
	defer func() { AdvanceClockForTesting(-advanced) }()
	advance := func(d time.Duration) { AdvanceClockForTesting(d); advanced += d }
	for i := 0; i < 4000; i++ {
		sizingRequest(c, fmt.Sprintf("query_range:old%04d", i), make([]byte, 1000), 48*time.Hour)
		if i%500 == 0 {
			c.sizing.flush()
		}
	}
	advance(30 * time.Minute)
	for i := 0; i < 2000; i++ {
		sizingRequest(c, fmt.Sprintf("query_range:new%04d", i), make([]byte, 500), 48*time.Hour)
		if i%500 == 0 {
			c.sizing.flush()
		}
	}
	s := sizingFlush(c)
	oldKeys, newKeys := sampledKeys(c, "query_range:old%04d", 4000), sampledKeys(c, "query_range:new%04d", 2000)
	want := [len(WorkingSetWindows)]int64{newKeys * sizingSampleRate, (oldKeys + newKeys) * sizingSampleRate, (oldKeys + newKeys) * sizingSampleRate}
	if s.WorkingSetKeys != want || !near(s.WorkingSetKeys[1], 6000, 0.4) {
		t.Fatalf("working set keys %v, want %v", s.WorkingSetKeys, want)
	}
	wantBytes := (oldKeys*int64(1000+len("query_range:old0000")) + newKeys*int64(500+len("query_range:new0000"))) * sizingSampleRate
	if s.WorkingSetBytes[1] != wantBytes {
		t.Fatalf("working set bytes %v, want %d", s.WorkingSetBytes, wantBytes)
	}
	advance(25 * time.Hour)
	if s := sizingFlush(c); s.WorkingSetKeys != [len(WorkingSetWindows)]int64{} {
		t.Fatalf("keys older than 24h kept: %v", s.WorkingSetKeys)
	}
	if n := len(c.sizing.seen); n != 0 {
		t.Fatalf("%d keys remembered past the horizon", n)
	}
}

// Keys asked every 25s land in the (10s, 30s] reuse bucket.
//
// conformance: cache-sizing-signals
func TestSizingReuseInterval(t *testing.T) {
	c := sizingCache(t, 100000, 1<<30)
	var advanced time.Duration
	defer func() { AdvanceClockForTesting(-advanced) }()
	for r := 0; r < 3; r++ {
		for i := 0; i < 1000; i++ {
			sizingRequest(c, fmt.Sprintf("query:r%d", i), make([]byte, 10), time.Hour)
		}
		c.sizing.flush()
		AdvanceClockForTesting(25 * time.Second)
		advanced += 25 * time.Second
	}
	h := sizingFlush(c).ReuseInterval[LayerLoki]
	counts := h.Counts()
	if h.Count == 0 || counts[3] < h.Count*9/10 { // bucket 3: (10s, 30s]
		t.Fatalf("reuse intervals %+v", h)
	}
}

// Memory is bounded: past sizingMaxKeys sampled keys the working set and the
// ghosts keep no more.
//
// conformance: cache-sizing-signals
func TestSizingBounded(t *testing.T) {
	c := sizingCache(t, 1<<20, 1<<40)
	for i := 0; i < 24*sizingMaxKeys; i++ {
		sizingRequest(c, fmt.Sprintf("query:b%d", i), make([]byte, 1), time.Hour)
		if i%4096 == 0 {
			c.sizing.flush()
		}
	}
	s := c.sizing
	s.flush()
	if len(s.seen) > sizingMaxKeys {
		t.Fatalf("%d keys remembered", len(s.seen))
	}
	for _, g := range s.ghosts[sizingTierL1] {
		if len(g.index) > sizingMaxKeys {
			t.Fatalf("ghost holds %d keys", len(g.index))
		}
	}
}

// Close stops the signals' goroutine; enabling twice starts one.
//
// conformance: cache-sizing-signals
func TestSizingCloseStopsTheGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		c := NewWithMaxBytes(time.Hour, 10, 1<<20)
		c.EnableSizingSignals()
		c.EnableSizingSignals()
		sizingRequest(c, "query:a", []byte("v"), time.Minute)
		c.Close()
		c.Close()
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines %d -> %d", before, n)
	}
}

// FuzzSizingGhostLRU drives a ghost cache with byte-coded stores, lookups and
// clock moves and holds its bounds and bookkeeping after every step.
func FuzzSizingGhostLRU(f *testing.F) {
	f.Add([]byte{0, 1, 10, 1, 1, 0, 0, 2, 200, 2, 5, 0, 1, 2, 0})
	f.Add([]byte{0, 9, 255, 0, 8, 255, 0, 7, 255, 1, 9, 0, 1, 8, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 || len(data) > 900 {
			return
		}
		g := newGhostLRU(int64(data[0]%8+1)*sizingSampleRate, int64(data[1])*sizingSampleRate*4, int64(data[2]%2+1))
		now := int64(0)
		for i := 3; i+2 < len(data); i += 3 {
			hash, arg := uint64(data[i+1]%32), int64(data[i+2])
			switch data[i] % 3 {
			case 0:
				g.store(hash, arg, (arg%16+1)*int64(time.Second), now)
			case 1:
				g.lookup(hash, now)
			default:
				now += arg * int64(time.Second) / 8
			}
			n, bytes := 0, int64(0)
			var prev *ghostNode
			for node := g.newest; node != nil; node = node.next {
				if node.prev != prev || g.index[node.hash] != node {
					t.Fatal("broken ghost list")
				}
				n, bytes, prev = n+1, bytes+node.size, node
			}
			if g.oldest != prev || n != len(g.index) || bytes != g.bytes {
				t.Fatalf("ghost bookkeeping: %d nodes, %d indexed, %d/%d bytes", n, len(g.index), bytes, g.bytes)
			}
			if int64(n) > g.maxEntries || (g.maxBytes > 0 && g.bytes > g.maxBytes) {
				t.Fatalf("ghost over its budget: %d/%d entries, %d/%d bytes", n, g.maxEntries, g.bytes, g.maxBytes)
			}
		}
	})
}
