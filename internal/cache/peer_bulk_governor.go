package cache

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/bits"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

// The bulk governor: a serving replica sends peer bulk restore streams
// (peer_bulk_serve.go) at a lower priority than everything else it does. One
// governor per replica paces all its streams together:
//
//   - the rate starts low (bulkStartRate), grows by a quarter per tick
//     (200 ms) after three ticks without pressure, up to the configured
//     ceiling (-peer-bulk-serve-max-bytes-per-second), and halves after two
//     ticks with pressure (never below bulkFloorRate);
//   - a severe signal pauses every stream at once; a stream paused longer
//     than the maximum pause ends with outcome "throttled" and the requester
//     keeps what it received;
//   - while it is paused or under pressure, a new stream is refused (429)
//     with a growing, jittered Retry-After.
//
// Pressure is read each tick from signals the serving work could disturb,
// each against its own recent baseline and an absolute floor:
//
//   - CPU: the Go scheduler latency p99 of the tick (runnable goroutines
//     waiting for a thread) against its 30 s minimum, and the busy share of
//     the process's CPU budget (GOMAXPROCS);
//   - GC: the share of the CPU budget spent in GC;
//   - memory: the live heap against GOMEMLIMIT;
//   - client requests: the mean latency of the queries completed in the tick
//     (BulkLoad, without tails), against a baseline learned on clean ticks;
//   - peer requests: the same for the /_cache/get, /has and /set this replica
//     served;
//   - disk: the mean latency of the disk cache's lookups;
//   - network: a write to the requester that took far longer than the bytes
//     sent at the current rate allow.
//
// The controller goroutine runs only while a stream does. Waiting for the
// governor holds no cache lock and no disk transaction.

const (
	bulkTick         = 200 * time.Millisecond
	bulkStartRate    = 2 << 20   // bytes per second when a stream starts
	bulkFloorRate    = 256 << 10 // the lowest rate pressure halves to
	bulkCleanToGrow  = 5         // clean ticks before the rate grows
	bulkGrowFactor   = 1.15
	bulkHitsToShrink = 2 // pressured ticks before the rate halves
	// bulkBatchWindow sizes a batch to this much of the current rate, so the
	// reads and copies of the serving side are paced too.
	bulkBatchWindow   = 100 * time.Millisecond
	bulkBatchMinBytes = 64 << 10
	// bulkBatchBytesPerProc caps a batch per GOMAXPROCS, so one batch's reads
	// and copies are short bursts on a replica with few cores.
	bulkBatchBytesPerProc = 512 << 10
	// DefaultPeerBulkServeMaxPause is how long a paused stream waits before
	// it ends as throttled.
	DefaultPeerBulkServeMaxPause = 30 * time.Second
)

// BulkLoad is the serving replica's completed client requests, their total
// latency and their latency histogram, cumulative (long-lived requests such as
// tails left out).
type BulkLoad struct {
	Requests, Nanos int64
	Buckets         [LatencyBuckets]int64
}

// LatencyBuckets is the size of a LatencyHist: bucket i holds latencies up to
// latencyBucketBase << i (64 µs to about 2 s; the last one also everything
// above).
const LatencyBuckets = 16

const latencyBucketBase = 64 * time.Microsecond

// LatencyHist is a lock-free log2 latency histogram: one atomic add per
// observation.
type LatencyHist struct {
	b [LatencyBuckets]atomic.Int64
}

// Observe counts one latency.
func (h *LatencyHist) Observe(d time.Duration) {
	i := 0
	if d > latencyBucketBase {
		i = min(bits.Len64(uint64((d-1)/latencyBucketBase)), LatencyBuckets-1)
	}
	h.b[i].Add(1)
}

// Snapshot returns the counts.
func (h *LatencyHist) Snapshot() (out [LatencyBuckets]int64) {
	for i := range h.b {
		out[i] = h.b[i].Load()
	}
	return out
}

// Pressure reasons (loki_vl_proxy_peer_cache_bulk_serve_pressure_total).
const (
	bulkPressureSched = iota
	bulkPressureCPU
	bulkPressureGC
	bulkPressureMemory
	bulkPressureRequests
	bulkPressurePeer
	bulkPressureDisk
	bulkPressureNetwork
	bulkPressureCount
)

var bulkPressureNames = [bulkPressureCount]string{"scheduler_latency", "cpu", "gc", "memory", "request_latency", "peer_latency", "disk_latency", "network"}

// Governor states (loki_vl_proxy_peer_cache_bulk_serve_state).
const (
	bulkStateIdle = iota
	bulkStateRamping
	bulkStateCeiling
	bulkStateThrottled
	bulkStatePaused
	bulkStateCount
)

var bulkStateNames = [bulkStateCount]string{"idle", "ramping", "ceiling", "throttled", "paused"}

var errBulkThrottled = errors.New("peer bulk stream paused past its maximum")

type bulkGovernor struct {
	ceiling  float64 // bytes per second; +Inf without a ceiling
	maxPause time.Duration
	signals  *bulkSignals
	log      *slog.Logger
	tick     time.Duration

	mu        sync.Mutex
	rate      float64 // 0 while paused
	state     int
	tokens    float64
	last      time.Time
	clean     int
	pressured int
	pausedAt  time.Time
	streams   int
	stop      chan struct{}
	done      chan struct{}
	refusals  int // consecutive refused admissions, for the Retry-After

	// fixed, when set (tests and benchmarks), is the rate used instead of
	// the controller's (+Inf: no rate).
	fixed float64

	netStall atomic.Bool

	pressure  [bulkPressureCount]atomic.Int64
	decreases atomic.Int64
	pauses    atomic.Int64
	aborts    atomic.Int64
	pausedNs  atomic.Int64
	rateNow   atomic.Int64
	stateNow  atomic.Int64
}

func newBulkGovernor(ceiling int64, maxPause time.Duration, signals *bulkSignals, log *slog.Logger) *bulkGovernor {
	g := &bulkGovernor{ceiling: math.Inf(1), maxPause: maxPause, signals: signals, log: log, tick: bulkTick}
	if ceiling > 0 {
		g.ceiling = float64(ceiling)
	}
	if g.maxPause <= 0 {
		g.maxPause = DefaultPeerBulkServeMaxPause
	}
	return g
}

func (g *bulkGovernor) startRate() float64 { return min(float64(bulkStartRate), g.ceiling) }

// begin registers a stream; the first one starts the controller at the start
// rate.
func (g *bulkGovernor) begin() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.streams++
	if g.streams > 1 {
		return
	}
	g.rate, g.state, g.clean, g.pressured = g.startRate(), bulkStateRamping, 0, 0
	g.tokens, g.last = 0, time.Now()
	g.stop, g.done = make(chan struct{}), make(chan struct{})
	if g.fixed != 0 {
		g.rate, g.state = g.fixed, bulkStateCeiling
		close(g.done)
	} else {
		go g.control(g.stop, g.done)
	}
	g.publishLocked()
}

// end unregisters a stream; the last one stops the controller and waits for
// it.
func (g *bulkGovernor) end() {
	g.mu.Lock()
	g.streams--
	if g.streams > 0 {
		g.mu.Unlock()
		return
	}
	if g.state == bulkStatePaused {
		g.pausedNs.Add(int64(time.Since(g.pausedAt)))
	}
	g.state, g.rate = bulkStateIdle, 0
	g.publishLocked()
	stop, done := g.stop, g.done
	g.mu.Unlock()
	close(stop)
	<-done
}

func (g *bulkGovernor) publishLocked() {
	g.rateNow.Store(int64(g.rate))
	g.stateNow.Store(int64(g.state))
}

// admit reports whether a new stream may start now, and when to ask again.
func (g *bulkGovernor) admit() (bool, time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.streams == 0 || (g.state != bulkStatePaused && g.pressured == 0 && g.state != bulkStateThrottled) {
		g.refusals = 0
		return true, 0
	}
	g.refusals++
	return false, time.Duration(min(1<<min(g.refusals, 4), 10)) * time.Second
}

// batchBytes is the bytes a batch may hold at the current rate.
func (g *bulkGovernor) batchBytes() int64 {
	g.mu.Lock()
	rate := max(g.rate, float64(bulkFloorRate))
	g.mu.Unlock()
	limit := min(float64(restoreBatchBytes), float64(bulkBatchBytesPerProc*runtime.GOMAXPROCS(0)))
	return int64(min(max(rate*bulkBatchWindow.Seconds(), bulkBatchMinBytes), limit))
}

// workers is how many goroutines copy a batch: one, unless the governor runs
// at its ceiling with no pressure.
func (g *bulkGovernor) workers() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == bulkStateCeiling {
		return max(min(restoreWorkers(), runtime.GOMAXPROCS(0)/4), 1)
	}
	return 1
}

// wait takes n bytes at the current rate. While paused it waits, and returns
// errBulkThrottled once the pause has lasted the maximum.
func (g *bulkGovernor) wait(ctx context.Context, n int) error {
	for {
		g.mu.Lock()
		if g.rate <= 0 {
			pausedFor := time.Since(g.pausedAt)
			g.mu.Unlock()
			if pausedFor >= g.maxPause {
				g.aborts.Add(1)
				return errBulkThrottled
			}
			if !sleepCtx(ctx, min(50*time.Millisecond, g.maxPause-pausedFor)) {
				return ctx.Err()
			}
			continue
		}
		now := time.Now()
		if !math.IsInf(g.rate, 1) {
			burst := g.rate * bulkBatchWindow.Seconds()
			g.tokens = min(burst, g.tokens+now.Sub(g.last).Seconds()*g.rate)
		}
		g.last = now
		var delay time.Duration
		if !math.IsInf(g.rate, 1) {
			g.tokens -= float64(n)
			if g.tokens < 0 {
				delay = time.Duration(-g.tokens / g.rate * float64(time.Second))
			}
		}
		g.mu.Unlock()
		if delay <= 0 {
			return ctx.Err()
		}
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
		return nil
	}
}

// wrote reports a write of n bytes that took d: far longer than the current
// rate allows means the network or the requester is congested.
func (g *bulkGovernor) wrote(n int, d time.Duration) {
	g.mu.Lock()
	rate := g.rate
	g.mu.Unlock()
	if rate <= 0 || math.IsInf(rate, 1) {
		return
	}
	expected := time.Duration(float64(n) / rate * float64(time.Second))
	if d > 2*expected+50*time.Millisecond {
		g.netStall.Store(true)
	}
}

func (g *bulkGovernor) control(stop, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(g.tick)
	defer t.Stop()
	if g.signals != nil {
		g.signals.read() // the first delta starts here
	}
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		var reasons [bulkPressureCount]bool
		severe := false
		if g.signals != nil {
			reasons, severe = g.signals.read()
		}
		if g.netStall.Swap(false) {
			reasons[bulkPressureNetwork] = true
		}
		g.step(reasons, severe)
	}
}

// step applies one tick's signals.
func (g *bulkGovernor) step(reasons [bulkPressureCount]bool, severe bool) {
	pressured := false
	for i, on := range reasons {
		if on {
			pressured = true
			g.pressure[i].Add(1)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.streams == 0 {
		return // the last stream ended while this tick was read
	}
	before := g.state
	switch {
	case severe:
		if g.state != bulkStatePaused {
			g.rate, g.state, g.pausedAt = 0, bulkStatePaused, time.Now()
			g.pauses.Add(1)
		}
		g.clean, g.pressured = 0, 1
	case pressured:
		g.clean = 0
		g.pressured++
		if g.state == bulkStatePaused {
			g.pausedNs.Add(int64(time.Since(g.pausedAt)))
			g.rate, g.state, g.pressured = float64(bulkFloorRate), bulkStateThrottled, 1
			break
		}
		if g.pressured >= bulkHitsToShrink {
			g.rate = max(g.rate/2, min(float64(bulkFloorRate), g.ceiling))
			if math.IsInf(g.rate, 1) {
				g.rate = g.startRate()
			}
			g.state, g.pressured = bulkStateThrottled, 1
			g.decreases.Add(1)
		}
	default:
		g.pressured = 0
		g.clean++
		if g.state == bulkStatePaused {
			g.pausedNs.Add(int64(time.Since(g.pausedAt)))
			g.rate, g.state, g.clean = float64(bulkFloorRate), bulkStateThrottled, 0
			break
		}
		if g.clean >= bulkCleanToGrow && g.state != bulkStateCeiling {
			g.rate = min(g.rate*bulkGrowFactor, g.ceiling)
			g.state = bulkStateRamping
			if g.rate >= g.ceiling {
				g.state = bulkStateCeiling
			}
		}
	}
	g.publishLocked()
	if g.log != nil && before != g.state && (g.state == bulkStatePaused || before == bulkStatePaused || g.state == bulkStateThrottled && before != bulkStateThrottled) {
		var names []string
		for i, on := range reasons {
			if on {
				names = append(names, bulkPressureNames[i])
			}
		}
		g.log.Info("peer bulk restore serving throttled", "state", bulkStateNames[g.state], "previous", bulkStateNames[before], "bytes_per_second", int64(g.rate), "pressure", names)
	}
}

// bulkSignals reads the pressure signals once per tick, as deltas since the
// previous read.
type bulkSignals struct {
	samples []metrics.Sample
	load    atomic.Pointer[func() BulkLoad]
	peer    *PeerCache
	disk    func() *DiskCache

	prevSched                 []uint64
	prevGC, prevCPU, prevIdle float64
	reqTail, peerTail         tailWindow
	prevDiskN, prevDiskNs     int64
	schedMins                 [150]float64 // 30 s of tick p99s, a ring
	schedAt                   int
	schedSeen                 int
	diskBase                  float64 // EWMA of clean ticks' mean disk lookup latency, ns
	started                   bool
}

const (
	sigSched = iota
	sigGC
	sigCPU
	sigIdle
	sigHeap
	sigLimit
)

func newBulkSignals(peer *PeerCache, disk func() *DiskCache) *bulkSignals {
	s := &bulkSignals{peer: peer, disk: disk}
	s.samples = []metrics.Sample{
		{Name: "/sched/latencies:seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/gomemlimit:bytes"},
	}
	return s
}

// read returns this tick's pressure reasons and whether one is severe.
func (s *bulkSignals) read() (reasons [bulkPressureCount]bool, severe bool) {
	metrics.Read(s.samples)
	first := !s.started
	s.started = true

	// Scheduler latency p99 of this tick.
	if s.samples[sigSched].Value.Kind() == metrics.KindFloat64Histogram {
		h := s.samples[sigSched].Value.Float64Histogram()
		if len(s.prevSched) != len(h.Counts) {
			s.prevSched = make([]uint64, len(h.Counts))
			copy(s.prevSched, h.Counts)
		} else {
			var total uint64
			for i, c := range h.Counts {
				total += c - s.prevSched[i]
			}
			if total >= 20 {
				target, seen := total-total/100, uint64(0)
				p99 := 0.0
				for i, c := range h.Counts {
					seen += c - s.prevSched[i]
					if seen >= target {
						p99 = h.Buckets[i+1]
						break
					}
				}
				if math.IsInf(p99, 1) {
					p99 = h.Buckets[len(h.Buckets)-2]
				}
				base := p99
				for _, m := range s.schedMins[:min(s.schedSeen, len(s.schedMins))] {
					base = min(base, m)
				}
				s.schedMins[s.schedAt] = p99
				s.schedAt = (s.schedAt + 1) % len(s.schedMins)
				s.schedSeen++
				reasons[bulkPressureSched] = p99 > 0.0005 && p99 > 2*base
				severe = severe || (p99 > 0.005 && p99 > 4*base)
			}
			copy(s.prevSched, h.Counts)
		}
	}

	// CPU budget: busy share and GC share.
	gc, cpu, idle := s.samples[sigGC].Value.Float64(), s.samples[sigCPU].Value.Float64(), s.samples[sigIdle].Value.Float64()
	if !first {
		if dCPU := cpu - s.prevCPU; dCPU > 0 {
			busy := 1 - (idle-s.prevIdle)/dCPU
			gcShare := (gc - s.prevGC) / dCPU
			reasons[bulkPressureCPU] = busy > 0.85
			reasons[bulkPressureGC] = gcShare > 0.10
			severe = severe || busy > 0.95 || gcShare > 0.25
		}
	}
	s.prevGC, s.prevCPU, s.prevIdle = gc, cpu, idle

	// Memory against GOMEMLIMIT.
	if limit := s.samples[sigLimit].Value.Uint64(); limit > 0 && limit < math.MaxInt64 {
		share := float64(s.samples[sigHeap].Value.Uint64()) / float64(limit)
		reasons[bulkPressureMemory] = share > 0.80
		severe = severe || share > 0.90
	}

	if s.latencies(&reasons, &severe, first) {
		return reasons, false
	}
	return reasons, severe
}

// meanOver is the mean latency in nanoseconds of n operations, 0 under 3.
func meanOver(n, ns int64) float64 {
	if n < 3 || ns <= 0 {
		return 0
	}
	return float64(ns) / float64(n)
}

func ewma(base, sample float64) float64 {
	if sample <= 0 || base <= 0 {
		return base
	}
	return base + 0.05*(sample-base)
}

// latencies adds the pressure of the latencies of what this replica serves:
// the p99 of the queries and of the peer requests over the last second (5
// ticks), each against the p99 it had on clean ticks, and the mean disk
// lookup latency against its own. True on the first read (no delta yet).
func (s *bulkSignals) latencies(reasons *[bulkPressureCount]bool, severe *bool, first bool) bool {
	var reqP99, peerP99, diskMean float64
	if load := s.load.Load(); load != nil {
		reqP99 = s.reqTail.add((*load)().Buckets)
	}
	if s.peer != nil {
		peerP99 = s.peerTail.add(s.peer.served.Snapshot())
	}
	if s.disk != nil {
		if dc := s.disk(); dc != nil {
			var n, ns int64
			for i := range dc.layerL2 {
				n += dc.layerL2[i].requests.Load()
				ns += dc.layerL2[i].lookupNanoseconds.Load()
			}
			diskMean = meanOver(n-s.prevDiskN, ns-s.prevDiskNs)
			s.prevDiskN, s.prevDiskNs = n, ns
		}
	}
	if first {
		return true
	}
	for _, sig := range []struct {
		value, factor, floor, severeFactor, severeFloor float64
		base                                            *float64
		reason                                          int
	}{
		{reqP99, 1.5, 0.5e6, 3, 5e6, &s.reqTail.base, bulkPressureRequests},
		{peerP99, 1.5, 0.5e6, 3, 5e6, &s.peerTail.base, bulkPressurePeer},
		{diskMean, 2, 2e6, 5, 20e6, &s.diskBase, bulkPressureDisk},
	} {
		if sig.value <= 0 {
			continue
		}
		if *sig.base == 0 {
			*sig.base = sig.value
			continue
		}
		reasons[sig.reason] = sig.value > sig.factor**sig.base+sig.floor
		*severe = *severe || sig.value > sig.severeFactor**sig.base+sig.severeFloor
	}
	clean := true
	for _, on := range reasons {
		clean = clean && !on
	}
	if clean {
		s.reqTail.base = ewma(s.reqTail.base, reqP99)
		s.peerTail.base = ewma(s.peerTail.base, peerP99)
		s.diskBase = ewma(s.diskBase, diskMean)
	}
	return false
}

// tailWindow is the latency histogram of the last tailTicks ticks.
type tailWindow struct {
	prev    [LatencyBuckets]int64
	ring    [tailTicks][LatencyBuckets]int64
	at      int
	started bool
	// base is the EWMA of the p99 on clean ticks, ns.
	base float64
}

const (
	tailTicks   = 5  // one second
	tailSamples = 50 // fewer is no p99
)

// add takes the cumulative counts and returns the window's p99 (the upper
// bound of its bucket, ns), 0 under tailSamples.
func (w *tailWindow) add(cum [LatencyBuckets]int64) float64 {
	var d [LatencyBuckets]int64
	if w.started {
		for i := range cum {
			d[i] = max(cum[i]-w.prev[i], 0)
		}
	}
	w.prev, w.started = cum, true
	w.ring[w.at] = d
	w.at = (w.at + 1) % tailTicks
	var sum [LatencyBuckets]int64
	var n int64
	for _, r := range w.ring {
		for i, c := range r {
			sum[i] += c
			n += c
		}
	}
	if n < tailSamples {
		return 0
	}
	target, seen := n-n/100, int64(0)
	for i, c := range sum {
		if seen += c; seen >= target {
			return float64(latencyBucketBase << i)
		}
	}
	return float64(latencyBucketBase << (LatencyBuckets - 1))
}
