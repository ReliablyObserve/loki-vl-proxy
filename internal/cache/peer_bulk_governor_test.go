package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func quietGovernor(ceiling int64, maxPause time.Duration) *bulkGovernor {
	g := newBulkGovernor(ceiling, maxPause, nil, nil)
	g.rate, g.state, g.last = g.startRate(), bulkStateRamping, time.Now()
	g.streams = 1
	return g
}

var noPressure [bulkPressureCount]bool

func pressureOf(reason int) [bulkPressureCount]bool {
	var r [bulkPressureCount]bool
	r[reason] = true
	return r
}

// The rate starts low, grows only after clean ticks, and never passes the
// ceiling.
//
// conformance: cache-restart-handover
func TestBulkGovernor_RampsFromLowToTheCeiling(t *testing.T) {
	g := quietGovernor(64<<20, time.Second)
	if g.rate != bulkStartRate {
		t.Fatalf("start rate %v", g.rate)
	}
	for i := 0; i < bulkCleanToGrow-1; i++ {
		g.step(noPressure, false)
	}
	if g.rate != bulkStartRate {
		t.Fatalf("grew before %d clean ticks: %v", bulkCleanToGrow, g.rate)
	}
	ticks := 0
	for ; g.state != bulkStateCeiling && ticks < 100; ticks++ {
		g.step(noPressure, false)
		if g.rate > 64<<20 {
			t.Fatalf("above the ceiling: %v", g.rate)
		}
	}
	if g.rate != 64<<20 || ticks > 40 {
		t.Fatalf("rate %v after %d ticks", g.rate, ticks)
	}
	// A ceiling below the start rate is the start rate.
	if low := quietGovernor(1<<20, time.Second); low.rate != 1<<20 {
		t.Fatalf("start above a 1 MiB/s ceiling: %v", low.rate)
	}
}

// Pressure on two ticks in a row halves the rate, down to the floor; one tick
// alone does not; new streams are refused meanwhile.
//
// conformance: cache-restart-handover
func TestBulkGovernor_HalvesUnderPressureWithDebounce(t *testing.T) {
	g := quietGovernor(64<<20, time.Second)
	g.rate, g.state = 32<<20, bulkStateRamping
	g.step(pressureOf(bulkPressureRequests), false)
	if g.rate != 32<<20 {
		t.Fatalf("one pressured tick changed the rate: %v", g.rate)
	}
	if ok, _ := g.admit(); ok {
		t.Fatal("a new stream was admitted under pressure")
	}
	g.step(pressureOf(bulkPressureSched), false)
	if g.rate != 16<<20 || g.state != bulkStateThrottled || g.decreases.Load() != 1 {
		t.Fatalf("rate %v state %s", g.rate, bulkStateNames[g.state])
	}
	for i := 0; i < 40; i++ {
		g.step(pressureOf(bulkPressureCPU), false)
	}
	if g.rate != bulkFloorRate {
		t.Fatalf("rate %v, want the floor", g.rate)
	}
	if g.pressure[bulkPressureCPU].Load() != 40 || g.pressure[bulkPressureRequests].Load() != 1 {
		t.Fatal("pressure counts by signal")
	}
	g.step(noPressure, false)
	if ok, _ := g.admit(); ok {
		t.Fatal("admitted while throttled")
	}
}

// A severe signal pauses at once: waiting streams hold until the pause ends,
// and a pause longer than the maximum ends them as throttled; a clean tick
// resumes at the floor.
//
// conformance: cache-restart-handover
func TestBulkGovernor_PausesResumesAndAborts(t *testing.T) {
	g := quietGovernor(64<<20, 300*time.Millisecond)
	g.step(noPressure, true)
	if g.state != bulkStatePaused || g.rate != 0 || g.pauses.Load() != 1 {
		t.Fatalf("state %s rate %v", bulkStateNames[g.state], g.rate)
	}
	if ok, retry := g.admit(); ok || retry < time.Second {
		t.Fatalf("admitted while paused (retry %v)", retry)
	}
	_, retry2 := g.admit()
	if retry2 <= time.Second {
		t.Fatalf("Retry-After does not grow: %v", retry2)
	}
	done := make(chan error, 1)
	go func() { done <- g.wait(context.Background(), 1000) }()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("waited through a pause: %v", err)
	default:
	}
	g.step(noPressure, false)
	if err := <-done; err != nil || g.rate != bulkFloorRate || g.state != bulkStateThrottled {
		t.Fatalf("resume: %v, rate %v, state %s", err, g.rate, bulkStateNames[g.state])
	}
	g.step(noPressure, true)
	start := time.Now()
	if err := g.wait(context.Background(), 1000); !errors.Is(err, errBulkThrottled) || time.Since(start) > 2*time.Second {
		t.Fatalf("a long pause: %v after %v", err, time.Since(start))
	}
	if g.aborts.Load() != 1 {
		t.Fatal("abort not counted")
	}
}

// Every stream of a replica shares one rate.
//
// conformance: cache-restart-handover
func TestBulkGovernor_StreamsShareOneRate(t *testing.T) {
	g := quietGovernor(1<<20, time.Second)
	g.rate = 1 << 20
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				_ = g.wait(context.Background(), 64<<10)
			}
		}()
	}
	wg.Wait()
	// 1 MiB at 1 MiB/s, less the 100 ms burst.
	if took := time.Since(start); took < 800*time.Millisecond || took > 2*time.Second {
		t.Fatalf("two streams sent 1 MiB in %v at 1 MiB/s", took)
	}
}

// The controller runs only while a stream does, restarts low, and every
// goroutine returns.
//
// conformance: cache-restart-handover
func TestBulkGovernor_LifecycleStartsLowAndStops(t *testing.T) {
	before := runtime.NumGoroutine()
	g := newBulkGovernor(64<<20, time.Second, newBulkSignals(nil, nil), nil)
	g.tick = 10 * time.Millisecond
	g.begin()
	g.begin()
	time.Sleep(50 * time.Millisecond)
	g.end()
	if g.stateNow.Load() == bulkStateIdle {
		t.Fatal("idle with a stream left")
	}
	g.end()
	if g.stateNow.Load() != bulkStateIdle || g.rateNow.Load() != 0 {
		t.Fatal("not idle after the last stream")
	}
	g.begin()
	if g.rate != bulkStartRate {
		t.Fatalf("restarted at %v, not low", g.rate)
	}
	g.end()
	waitFor(t, "the controller to stop", func() bool { return runtime.NumGoroutine() <= before })
}

// The signals: a query latency p99 over the last second far above its
// learned baseline is pressure, and well above that severe; too few requests
// are no sample.
//
// conformance: cache-restart-handover
func TestBulkSignals_LatencyAgainstItsBaseline(t *testing.T) {
	var hist LatencyHist
	s := newBulkSignals(nil, nil)
	fn := func() BulkLoad { return BulkLoad{Buckets: hist.Snapshot()} }
	s.load.Store(&fn)
	tick := func(n int, each time.Duration) ([bulkPressureCount]bool, bool) {
		for i := 0; i < n; i++ {
			hist.Observe(each)
		}
		return s.read()
	}
	tick(0, 0) // starts the deltas
	for i := 0; i < 10; i++ {
		if r, _ := tick(50, time.Millisecond); r[bulkPressureRequests] {
			t.Fatal("pressure at the baseline")
		}
	}
	// A tail: 5% of a second's requests at 4 ms.
	var r [bulkPressureCount]bool
	var severe bool
	for i := 0; i < 5; i++ {
		tick(47, time.Millisecond)
		r, severe = tick(3, 4*time.Millisecond)
	}
	if !r[bulkPressureRequests] || severe {
		t.Fatalf("a 4 ms tail over a 1 ms baseline: pressure %v severe %v", r[bulkPressureRequests], severe)
	}
	for i := 0; i < 5; i++ {
		r, severe = tick(50, 40*time.Millisecond)
	}
	if !severe {
		t.Fatal("a 40 ms p99 is not severe")
	}
	quiet := newBulkSignals(nil, nil)
	quiet.load.Store(&fn)
	quiet.read()
	if r, _ := quiet.read(); r[bulkPressureRequests] {
		t.Fatal("no requests are no sample")
	}
}

// The histogram buckets: bucket i holds latencies up to 64 µs << i.
//
// conformance: cache-restart-handover
func TestLatencyHist_Buckets(t *testing.T) {
	var h LatencyHist
	for _, d := range []time.Duration{0, 64 * time.Microsecond, 65 * time.Microsecond, 128 * time.Microsecond, 3 * time.Millisecond, time.Hour} {
		h.Observe(d)
	}
	got := h.Snapshot()
	if got[0] != 2 || got[1] != 2 || got[6] != 1 || got[LatencyBuckets-1] != 1 {
		t.Fatalf("buckets %v", got)
	}
}

// Waiting for the governor holds no cache lock and no disk transaction: a
// query's write and a disk commit go through while a stream is paused.
//
// conformance: cache-restart-handover
func TestPeerBulkServe_PausedStreamHoldsNoLock(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 200, 4096, 10*time.Minute)
	server := f.nodes[0]
	server.pc.bulk.gov.fixed = 64 << 10
	f.restart(1, true)
	done := make(chan PeerRestoreReport, 1)
	go func() {
		done <- f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	}()
	waitFor(t, "a stream", func() bool { return server.pc.bulk.active.Load() == 1 })
	g := server.pc.bulk.gov
	g.mu.Lock()
	g.rate, g.state, g.pausedAt = 0, bulkStatePaused, time.Now()
	g.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	for i := 0; i < 50; i++ {
		server.c.SetLocalAndDiskWithTTL(fmt.Sprintf("live-%d", i), []byte(strings.Repeat("x", 4096)), time.Minute)
	}
	server.dc.Flush()
	if _, ok := server.dc.Get("live-49"); !ok || time.Since(start) > time.Second {
		t.Fatalf("writes and a disk commit took %v while a stream was paused", time.Since(start))
	}
	g.mu.Lock()
	g.rate, g.state = 64<<10, bulkStateThrottled
	g.mu.Unlock()
	if report := <-done; report.Outcome != RestoreDone {
		t.Fatalf("outcome %q", report.Outcome)
	}
}

// A stream paused past the maximum pause ends as throttled; the restoring
// replica keeps what arrived and counts the rest as peer_busy.
//
// conformance: cache-restart-handover
func TestPeerRestore_ThrottledStreamEndsPartial(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 200, 4096, 10*time.Minute)
	server := f.nodes[0]
	server.pc.bulk.gov.fixed = 128 << 10
	server.pc.bulk.gov.maxPause = 200 * time.Millisecond
	f.restart(1, true)
	nd := f.nodes[1]
	done := make(chan PeerRestoreReport, 1)
	go func() {
		done <- nd.c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	}()
	waitFor(t, "a first batch", func() bool { return nd.c.HandoverStats().PeerRestore.RestoredEntries > 0 })
	g := server.pc.bulk.gov
	g.mu.Lock()
	g.rate, g.state, g.pausedAt = 0, bulkStatePaused, time.Now()
	g.mu.Unlock()
	report := <-done
	if report.Peers[0].Outcome != PeerRestoreThrottled || report.Progress.RestoredEntries == 0 || report.Progress.Skipped[RestoreSkipPeerBusy] == 0 {
		t.Fatalf("peer %+v, restored %d, skipped %v", report.Peers[0], report.Progress.RestoredEntries, report.Progress.Skipped)
	}
	checkProgressSums(t, report.Progress)
	if server.pc.bulk.streams[bulkEndThrottled].Load() != 1 {
		t.Fatal("the throttled stream was not counted")
	}
}

// A new stream is refused while the governor is paused; the requester asks
// again after the Retry-After.
//
// conformance: cache-restart-handover
func TestPeerBulkServe_RefusesNewStreamsWhilePaused(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 20, 512, 10*time.Minute)
	server := f.nodes[0]
	g := server.pc.bulk.gov
	g.mu.Lock()
	g.streams, g.state, g.rate, g.pausedAt = 1, bulkStatePaused, 0, time.Now()
	g.mu.Unlock()
	go func() {
		time.Sleep(1200 * time.Millisecond)
		g.mu.Lock()
		g.streams, g.state, g.rate = 0, bulkStateIdle, 0
		g.mu.Unlock()
	}()
	f.restart(1, true)
	report := f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 15*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20})
	if report.Outcome != RestoreDone || report.Progress.RestoredEntries != 20 || report.Peers[0].Attempts < 2 {
		t.Fatalf("outcome %q restored %d peers %+v", report.Outcome, report.Progress.RestoredEntries, report.Peers)
	}
	if server.pc.bulk.rejected[bulkRejectThrottled].Load() == 0 {
		t.Fatal("no throttled refusal counted")
	}
}

// The plan walks the memory tier and the disk index a chunk at a time and
// still lists every entry.
//
// conformance: cache-restart-handover
func TestPeerBulkServe_PlanInChunksListsEverything(t *testing.T) {
	quietRestoreLogs(t)
	dc, err := NewDiskCache(DiskCacheConfig{Path: t.TempDir() + "/c.db", FlushInterval: time.Hour, MaxBufferBytes: 64 << 20, FlushBytes: 64 << 20, FlushSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	c := NewWithMaxBytes(time.Hour, 3*bulkPlanChunk, 256<<20)
	defer c.Close()
	c.SetL2(dc)
	n := 2*bulkPlanChunk + 100
	for i := 0; i < n; i++ {
		c.SetLocalAndDiskWithTTL(fmt.Sprintf("k-%05d", i), []byte("v"), time.Hour)
	}
	dc.Flush()
	for i := 0; i < n; i += 2 { // half only on disk
		c.Invalidate(fmt.Sprintf("k-%05d", i))
	}
	items, _ := c.bulkPlan(func(string) bool { return true }, false)
	seen := map[string]bool{}
	for _, it := range items {
		if seen[it.key] {
			t.Fatalf("%s twice", it.key)
		}
		seen[it.key] = true
	}
	if len(seen) != n {
		t.Fatalf("planned %d of %d", len(seen), n)
	}
}

// The requester's intake cap holds its reads to the rate it sets.
//
// conformance: cache-restart-handover
func TestPeerRestore_IntakeCap(t *testing.T) {
	f := newReplFleet(t, 2, 2, true, time.Second)
	fillFleet(t, f, 0, 128, 8192, 10*time.Minute)
	f.nodes[0].pc.bulk.gov.fixed = 1 << 30
	f.restart(1, true)
	start := time.Now()
	report := f.nodes[1].c.RestoreFromPeers(restoreCtx(t, 20*time.Second), PeerRestoreOptions{MaxBytes: 32 << 20, MaxBytesPerSecond: 512 << 10})
	if report.Outcome != RestoreDone || report.Progress.RestoredEntries != 128 {
		t.Fatalf("outcome %q restored %d", report.Outcome, report.Progress.RestoredEntries)
	}
	// About 1 MiB at 512 KiB/s.
	if took := time.Since(start); took < 1500*time.Millisecond {
		t.Fatalf("1 MiB read in %v under a 512 KiB/s cap", took)
	}
}

// SetBulkLoad gives the governor's signals the client request counters.
//
// conformance: cache-restart-handover
func TestPeerBulkServe_SetBulkLoad(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "127.0.0.1:1", DiscoveryType: "static", StaticPeers: "127.0.0.1:1"})
	defer pc.Close()
	pc.SetBulkLoad(func() BulkLoad { return BulkLoad{Requests: 3, Nanos: 3e6} })
	if load := pc.bulk.gov.signals.load.Load(); load == nil || (*load)().Requests != 3 {
		t.Fatal("the load was not handed to the signals")
	}
	var nilPC *PeerCache
	nilPC.SetBulkLoad(nil)
}
