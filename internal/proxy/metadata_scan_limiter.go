package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Adaptive admission for long-range metadata scans.
//
// A long-range stream_field_names, stream_field_values, field_names,
// field_values or streams listing scans every row in its range, and what one
// scan costs VictoriaLogs depends on the data: 0.8 GiB and 9 s per 7-day
// scan on the e2e stack in September 2026 (35 s a day later), many times that
// on a large deployment, a few megabytes on a small one. A fixed concurrency
// is wrong in both directions, so each replica learns what the backend it
// talks to can take, from signals every replica sees on its own:
//
//   - Fleet share: VictoriaLogs exports how many selects it runs right now,
//     for all its clients. The selects it runs for others (other replicas,
//     other clients) count against this replica's limit, so replicas that
//     never talk to each other hold the backend near one replica's limit
//     rather than replicas x limit: a long scan is CPU-bound and VictoriaLogs
//     already spreads one over all its cores, so more at once only slow each
//     other down (seven at once took over 60 s each and OOM-killed the e2e
//     VictoriaLogs). A long scan waits a random fraction of a second before
//     deciding, so replicas that receive scans at the same moment still see
//     each other's selects.
//   - Latency (Vegas-style): each scan class (endpoint, range bucket and
//     tenant) tracks its no-load duration like Vegas tracks the base
//     round-trip time (a faster scan lowers it at once, slower ones raise it
//     slowly), per row when the caller knows the rows it scans. A scan that
//     ran beside others and took more than the tolerance times that baseline
//     shrinks the limit by a fifth (once per wave of slow scans); a scan that
//     used the whole limit within the tolerance grows it by 1/limit, between
//     the operator floor and ceiling.
//   - Distress: a transport failure, a timeout or a 5xx on a scan halves the
//     limit, and so does a rise of VictoriaLogs' queue-timeout counter
//     (vl_concurrent_select_limit_timeout_total); a rise of its queueing
//     counter (vl_concurrent_select_limit_reached_total) counts as a slow
//     wave. While VictoriaLogs runs as many selects as its
//     -search.maxConcurrentRequests allows, or stops answering /metrics after
//     having answered it, no scan is admitted.
//   - Memory: a scan is admitted only while VictoriaLogs' memory in use (the
//     anonymous resident set minus the idle heap it has not returned), plus
//     what this replica's scans in flight reserved and have not yet shown,
//     plus the selects others run counted at the class's cost, plus the
//     class's cost, stays under 1 - headroom of the memory available to it.
//     The cost is learned from the peak growth of memory in use while scans
//     of the class ran; a class nobody has measured runs alone, and on a
//     backend that is never idle escapes after a short wait (admitLocked).
//   - Brake: when memory in use passes a third of the way into the headroom
//     anyway, the replica stops its youngest scan (shedLocked).
//
// A backend whose /metrics lacks these series (a vmauth in front of
// VictoriaLogs, an older release) leaves the latency and distress feedback.
// Background inventory work never waits and never takes the last slot while
// a request could use it: it is skipped when the limiter is closed and
// retried on its own schedule. Synchronous requests wait the shared heavy
// queue wait and then fail like Loki's scheduler with a 429 that names the
// operator flags.

// Flag defaults for the adaptive metadata-scan limiter.
const (
	// DefaultBackendMaxConcurrentMetadataScans is the ceiling the adaptive
	// limit may grow to per replica. 0 disables the limiter.
	DefaultBackendMaxConcurrentMetadataScans = 8
	// DefaultBackendMinConcurrentMetadataScans is the floor the adaptive limit
	// may shrink to.
	DefaultBackendMinConcurrentMetadataScans = 1
	// DefaultBackendMetadataScanMemoryHeadroom is the fraction of the memory
	// available to VictoriaLogs that long-range scans must leave free.
	DefaultBackendMetadataScanMemoryHeadroom = 0.4
	// DefaultBackendMetadataScanLatencyTolerance is how many times its
	// no-load baseline a concurrent scan may take before the limit shrinks.
	DefaultBackendMetadataScanLatencyTolerance = 1.5
)

const (
	metadataScanInitialLimit = 2
	// metadataScanUnknownCostFraction is the share of the backend's available
	// memory reserved for a scan whose class was never measured: scans grow
	// with the data, and a backend's data with its memory, so a fraction of
	// the latter is the one prior that fits a small and a large backend.
	metadataScanUnknownCostFraction = 0.1
	// metadataScanMinCostFraction is the smallest cost a measured class is
	// given: a scan that happened to run while VictoriaLogs returned memory
	// would otherwise look free.
	metadataScanMinCostFraction = 1.0 / 256
	metadataScanBaselineWeight  = 0.3  // how far a slower solo scan pulls the no-load baseline up
	metadataScanBaselineDrift   = 0.02 // how far a slower concurrent scan pulls it up
	metadataScanDecrease        = 0.8  // limit multiplier on a slow concurrent scan
	metadataScanDistressFactor  = 0.5  // limit multiplier on backend distress
	// metadataScanPoll is how often VictoriaLogs' /metrics is read while
	// scans are in flight, to see the peak memory a scan builds up.
	metadataScanPoll = 500 * time.Millisecond
	// metadataScanDecisionAge is how old a reading may be for an admission
	// decision; waiters sharing a moment share one read.
	metadataScanDecisionAge = 100 * time.Millisecond
	// metadataScanRecheck is how often a waiting scan re-reads the backend
	// when nothing is released to wake it.
	metadataScanRecheck = 250 * time.Millisecond
	// metadataScanAdmissionJitter spreads the admission decisions of replicas
	// that receive scans at the same moment.
	metadataScanAdmissionJitter = 250 * time.Millisecond
	metadataScanMinFloorFlag    = "-backend-min-concurrent-metadata-scans"
	metadataScanHeadroomFlag    = "-backend-metadata-scan-memory-headroom"
)

// scanClass groups scans whose cost is comparable.
type scanClass struct {
	path   string
	bucket time.Duration // 1h, 6h, 24h, 7d, 30d; 0 means longer or unbounded
	tenant string
}

var scanRangeBuckets = []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour}

func classifyScan(path string, params url.Values, tenant string) scanClass {
	rng, ok := backendParamsRange(params)
	if ok {
		for _, bucket := range scanRangeBuckets {
			if rng <= bucket {
				return scanClass{path: path, bucket: bucket, tenant: tenant}
			}
		}
	}
	return scanClass{path: path, tenant: tenant}
}

func (c scanClass) String() string {
	rng := "unbounded"
	if c.bucket != 0 {
		rng = c.bucket.String()
	}
	return c.path + " (" + rng + ", tenant " + c.tenant + ")"
}

// scanClassStats is what the limiter has learned about one class.
type scanClassStats struct {
	baseline       float64 // no-load duration in ns (trackBaseline)
	baselinePerRow float64 // no-load ns per row, when rows were known
	costBytes      float64 // estimated VictoriaLogs memory growth per scan
	measured       bool
	// solo is set when costBytes was measured by a scan that ran with no
	// other select on VictoriaLogs: then it is the scan's own cost. Measured
	// beside others it is an upper bound (their growth lands in it too).
	solo       bool
	measuredAt time.Time
	lastUsed   time.Time
	shedAt     time.Time // when the brake last stopped a scan of the class
	samples    int
}

// backendSignals is one reading of VictoriaLogs' /metrics.
type backendSignals struct {
	inUse, available float64 // memory VictoriaLogs holds (see parseBackendSignals) and may use
	memOK            bool
	selectCurrent    float64
	selectCapacity   float64
	limitReached     float64
	limitTimeout     float64
	concOK           bool
	failed           bool    // /metrics did not answer 200
	others           float64 // selectCurrent minus this replica's own calls at the reading
	at               time.Time
}

// scanWorkKey carries the rows a scan will read, when the caller knows them
// (inventory buckets counted before their scan).
type scanWorkKey struct{}

func withScanWork(ctx context.Context, rows int64) context.Context {
	if rows <= 0 {
		return ctx
	}
	return context.WithValue(ctx, scanWorkKey{}, rows)
}

func scanWork(ctx context.Context) int64 {
	rows, _ := ctx.Value(scanWorkKey{}).(int64)
	return rows
}

// scanToken is one admitted scan.
type scanToken struct {
	class         scanClass
	start         time.Time
	rows          int64
	useAtStart    float64
	peakUse       float64
	memOK         bool
	concurrent    int // scans in flight on this replica while it ran, itself included (max)
	fleetLoad     int // long scans running anywhere when it was admitted, itself included
	reserved      float64
	distress      atomic.Bool
	released      bool
	background    bool
	decreaseEpoch int
	unknown       bool // admitted before its class had a measured cost
	// aborted is set when the call ended without a VictoriaLogs answer to
	// learn from: its client went away, or VictoriaLogs refused it (4xx).
	aborted   atomic.Bool
	maxOthers float64 // most selects VictoriaLogs ran for others while it ran
	// cancel stops the scan's VictoriaLogs call; shed is set when the limiter
	// used it to keep VictoriaLogs' memory below the shed mark.
	cancel  context.CancelFunc
	shed    atomic.Bool
	escaped bool   // admitted by an escape (memoryGateLocked)
	seq     uint64 // admission order, for a deterministic brake
}

type metadataScanLimiter struct {
	mu              sync.Mutex
	floor           int
	ceiling         int
	limit           float64
	inFlight        int
	unknownInFlight int // scans in flight whose class had no measured cost
	waiters         int // synchronous scans waiting for a slot
	queueWait       time.Duration
	tolerance       float64
	headroom        float64
	classes         map[scanClass]*scanClassStats
	sig             backendSignals
	backendSeen     bool      // /metrics has exported VictoriaLogs' select series at least once
	failingSince    time.Time // first /metrics failure of the current run of failures
	silenceWindow   time.Duration
	recentOthers    []othersReading
	recentUse       []useReading // memory in use over the last metadataScanSteadyWindow
	sampler         func(ctx context.Context) backendSignals
	localCalls      func() int    // this replica's select calls in VictoriaLogs right now; nil counts only scans
	sampling        chan struct{} // non-nil while a /metrics read is in flight
	tokens          map[*scanToken]struct{}
	polling         bool
	decreaseEpoch   int
	lastCounterCut  time.Time
	now             func() time.Time
	recheck         time.Duration
	poll            time.Duration
	decisionAge     time.Duration
	jitter          func() time.Duration
	changed         chan struct{} // closed and replaced whenever a slot may have opened
	lastShed        time.Time
	escapesInFlight int  // scans admitted by an escape, not yet released
	admitEscaped    bool // set by memoryGateLocked when it admitted an escape
	seq             uint64
	// counters for observability and tests
	increases, decreases, distressEvents, unknownRuns, admitted, rejected, shed int
}

func newMetadataScanLimiter(ceiling, floor int, queueWait time.Duration, headroom, tolerance float64, sampler func(ctx context.Context) backendSignals) *metadataScanLimiter {
	if ceiling <= 0 {
		return nil
	}
	// The package's tests define their own min, so the builtin is avoided.
	floor = max(floor, 1)
	if floor > ceiling {
		floor = ceiling
	}
	queueWait = max(queueWait, 0)
	tolerance = max(tolerance, 1)
	if headroom < 0 || headroom >= 1 {
		headroom = 0
	}
	initial := max(metadataScanInitialLimit, floor)
	if initial > ceiling {
		initial = ceiling
	}
	return &metadataScanLimiter{
		floor: floor, ceiling: ceiling, limit: float64(initial), queueWait: queueWait,
		tolerance: tolerance, headroom: headroom, classes: map[scanClass]*scanClassStats{},
		sampler: sampler, tokens: map[*scanToken]struct{}{}, now: time.Now,
		recheck: metadataScanRecheck, poll: metadataScanPoll, decisionAge: metadataScanDecisionAge, silenceWindow: metadataScanSilenceWindow,
		jitter: func() time.Duration {
			return time.Duration(rand.Int64N(int64(metadataScanAdmissionJitter)))
		},
		changed: make(chan struct{}),
	}
}

// signals returns a reading no older than maxAge, sharing one /metrics read
// among concurrent callers. The caller must not hold l.mu.
func (l *metadataScanLimiter) signals(ctx context.Context, maxAge time.Duration) backendSignals {
	if l.sampler == nil {
		return backendSignals{}
	}
	l.mu.Lock()
	for {
		if !l.sig.at.IsZero() && l.now().Sub(l.sig.at) <= maxAge {
			s := l.sig
			l.mu.Unlock()
			return s
		}
		if ch := l.sampling; ch != nil {
			l.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				l.mu.Lock()
				s := l.sig
				l.mu.Unlock()
				return s
			}
			l.mu.Lock()
			if !l.sig.at.IsZero() && l.now().Sub(l.sig.at) <= maxAge+l.poll {
				s := l.sig
				l.mu.Unlock()
				return s
			}
			continue
		}
		ch := make(chan struct{})
		l.sampling = ch
		l.mu.Unlock()
		s := l.sampler(ctx)
		l.mu.Lock()
		l.applySignalsLocked(s)
		l.sampling = nil
		close(ch)
		s = l.sig
		victim := l.shedLocked(s)
		l.mu.Unlock()
		if victim != nil {
			victim()
		}
		return s
	}
}

// metadataScanCounterCutEvery bounds how often a rise of VictoriaLogs' own
// queue counters may cut the limit: it is read up to ten times a second, and
// one burst of queueing is one signal.
const metadataScanCounterCutEvery = 5 * time.Second

// applySignalsLocked stores a reading, tracks in-flight scans' peak memory
// and turns rises of VictoriaLogs' queue counters into feedback.
func (l *metadataScanLimiter) applySignalsLocked(s backendSignals) {
	s.at = l.now()
	prev := l.sig
	switch {
	case s.concOK:
		l.backendSeen = true
		l.failingSince = time.Time{}
	case s.failed && l.failingSince.IsZero():
		l.failingSince = s.at
	}
	if s.concOK && prev.concOK && s.at.Sub(l.lastCounterCut) >= metadataScanCounterCutEvery {
		switch {
		case s.limitTimeout > prev.limitTimeout:
			l.lastCounterCut = s.at
			l.distressLocked()
		case s.limitReached > prev.limitReached && l.inFlight > 0:
			l.lastCounterCut = s.at
			l.slowLocked()
		}
	}
	if s.memOK {
		for t := range l.tokens {
			if s.inUse > t.peakUse {
				t.peakUse = s.inUse
			}
		}
	}
	if s.concOK {
		// This replica's calls still queued inside VictoriaLogs are not in
		// selectCurrent yet, so others can come out low under saturation;
		// the capacity check closes admission then anyway.
		mine := l.inFlight
		if l.localCalls != nil {
			mine = max(l.localCalls(), l.inFlight)
		}
		s.others = max(s.selectCurrent-float64(mine), 0)
		for t := range l.tokens {
			t.maxOthers = max(t.maxOthers, s.others)
		}
		if s.memOK {
			l.recentUse = append(l.recentUse, useReading{at: s.at, inUse: s.inUse})
			for len(l.recentUse) > 1 && s.at.Sub(l.recentUse[1].at) >= metadataScanSteadyWindow {
				l.recentUse = l.recentUse[1:]
			}
		}
		l.recentOthers = append(l.recentOthers, othersReading{at: s.at, others: s.others})
		for len(l.recentOthers) > 0 && s.at.Sub(l.recentOthers[0].at) > metadataScanFleetWindow {
			l.recentOthers = l.recentOthers[1:]
		}
	}
	l.sig = s
}

// shedLocked is the emergency brake behind every estimate: when VictoriaLogs'
// memory in use passes a third of the way into the headroom (73% of the
// available memory with the default headroom: the container also needs room
// for the page cache and the kernel), this replica stops its youngest scan in
// flight, one per poll interval while the memory stays above the mark. A
// scan's memory builds up over its run, so the youngest has the most left to
// grow and the least work lost; its request is answered with the 429. It
// returns the stopped scan's cancel function, for the caller to call without
// the lock.
func (l *metadataScanLimiter) shedLocked(s backendSignals) context.CancelFunc {
	if l.headroom <= 0 || !s.memOK || s.available <= 0 || s.inUse <= l.brakeMark(s) {
		return nil
	}
	if !l.lastShed.IsZero() && l.now().Sub(l.lastShed) < l.poll {
		return nil
	}
	var youngest *scanToken
	for t := range l.tokens {
		if !t.shed.Load() && t.cancel != nil && (youngest == nil || t.seq > youngest.seq) {
			youngest = t
		}
	}
	if youngest == nil {
		return nil
	}
	youngest.shed.Store(true)
	youngest.distress.Store(true)
	l.lastShed = l.now()
	l.shed++
	return youngest.cancel
}

// attachCancel gives an admitted scan the function that stops its call, so
// shedLocked can stop it. A scan shed before its call started stops at once.
func (l *metadataScanLimiter) attachCancel(t *scanToken, cancel context.CancelFunc) {
	if l == nil || t == nil {
		return
	}
	l.mu.Lock()
	t.cancel = cancel
	shed := t.shed.Load()
	l.mu.Unlock()
	if shed {
		cancel()
	}
}

// detachCancel takes the scan out of the brake's reach once VictoriaLogs has
// answered: it computes a listing before it sends it, so stopping the call
// while its body is read would free nothing and fail an answered request.
func (l *metadataScanLimiter) detachCancel(t *scanToken) {
	if l == nil || t == nil {
		return
	}
	l.mu.Lock()
	t.cancel = nil
	l.mu.Unlock()
}

// wasShed reports whether the limiter stopped the scan's call.
func (l *metadataScanLimiter) wasShed(t *scanToken) bool {
	if l == nil || t == nil {
		return false
	}
	return t.shed.Load()
}

// shedError is the 429 a request whose scan was stopped by shedLocked gets.
func (l *metadataScanLimiter) shedError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return &heavyQueryQueueFullError{
		limitFlag:     metadataScanLimitFlag,
		work:          metadataScanLimitWork,
		maxConcurrent: l.ceiling,
		queueWait:     l.queueWait,
		adaptive:      l.adaptiveNoteLocked(l.sig) + ", the scan was stopped to keep VictoriaLogs' memory inside the headroom",
	}
}

// metadataScanSilenceWindow is how long a VictoriaLogs that answered
// /metrics before and stopped answering keeps the limiter closed. A busy
// VictoriaLogs answers again within it; one that keeps failing (an auth
// change, a rule dropping /metrics, renamed series) leaves the latency and
// failure feedback, as for a backend that never exported the series.
const metadataScanSilenceWindow = 10 * time.Second

// silentLocked reports a VictoriaLogs that exported its select series before
// and has failed to answer /metrics for less than the silence window.
func (l *metadataScanLimiter) silentLocked(s backendSignals) bool {
	return s.failed && l.backendSeen && !l.failingSince.IsZero() && s.at.Sub(l.failingSince) < l.silenceWindow
}

// useReading is one reading of VictoriaLogs' memory in use.
type useReading struct {
	at    time.Time
	inUse float64
}

// metadataScanEscapeReadAge is how old a reading may be for an escape.
const metadataScanEscapeReadAge = 10 * time.Millisecond

// metadataScanSteadyWindow is how long VictoriaLogs' memory in use must have
// held steady before a scan without a cost of its own goes ahead beside other
// clients' work: a scan that started a moment ago builds its memory up over
// the first part of its run, and until then only this growth shows it.
const metadataScanSteadyWindow = 5 * time.Second

// steadyLocked reports whether the memory in use grew by at most limit over
// the last metadataScanSteadyWindow of readings, and whether the readings
// cover that window.
func (l *metadataScanLimiter) steadyLocked(s backendSignals, limit float64) bool {
	if len(l.recentUse) == 0 || s.at.Sub(l.recentUse[0].at) < metadataScanSteadyWindow-metadataScanRecheck {
		return false
	}
	low := s.inUse
	for _, r := range l.recentUse {
		if r.inUse < low {
			low = r.inUse
		}
	}
	return s.inUse-low <= limit
}

// othersReading is one reading of the selects VictoriaLogs ran for others.
type othersReading struct {
	at     time.Time
	others float64
}

// othersLocked is how many selects VictoriaLogs runs for anyone but this
// replica: other replicas, other clients.
//
// A long scan runs for seconds; other clients' dashboard queries, and the
// edge, minute-bucket and count calls of other replicas' listings, for
// milliseconds. A scan deciding for the first time counts every select
// running at that moment, so scans that arrive together see each other. A
// scan that has been waiting counts only what stayed running through the
// readings of its wait (at most the last metadataScanFleetWindow of them):
// selects that stay are long work, selects that come and go leave gaps, and
// a stream of short calls cannot keep it out for ever.
func (l *metadataScanLimiter) othersLocked(s backendSignals, waitingSince time.Time) float64 {
	if !s.concOK {
		return 0
	}
	floor := s.others
	for _, r := range l.recentOthers {
		if !r.at.Before(waitingSince) && s.at.Sub(r.at) <= metadataScanFleetWindow && r.others < floor {
			floor = r.others
		}
	}
	return floor
}

// metadataScanFleetWindow is how long a select must keep running to count as
// another client's long work (othersLocked).
const metadataScanFleetWindow = time.Second

// scanWaiter is the state of one synchronous scan across its admission
// attempts. A zero-value waiter never escapes (background work).
type scanWaiter struct {
	since     time.Time // first reading of its wait
	escapeAt  time.Time // when it may stop waiting for an unverified cost to fit (zero: never)
	minOthers float64   // fewest selects VictoriaLogs ran for others at any reading of its wait; -1 before the first
}

// observe records a reading taken while the scan waits.
func (w *scanWaiter) observe(s backendSignals) {
	if w == nil || !s.concOK {
		return
	}
	if w.minOthers < 0 || s.others < w.minOthers {
		w.minOthers = s.others
	}
}

// admitLocked reports whether a scan of class may start now.
func (l *metadataScanLimiter) admitLocked(class scanClass, s backendSignals, background bool, w *scanWaiter) (reserve float64, ok bool) {
	waitingSince := s.at
	if w != nil && !w.since.IsZero() {
		waitingSince = w.since
	}
	limit := int(l.limit)
	others := l.othersLocked(s, waitingSince)
	if background {
		// Background work never takes the last slot while a request could
		// use it: only below limit-1, or on an idle backend nobody waits for.
		idle := l.inFlight == 0 && l.waiters == 0 && others == 0
		if l.inFlight >= limit-1 && !idle {
			return 0, false
		}
	}
	if l.inFlight >= limit {
		return 0, false
	}
	if l.silentLocked(s) {
		// VictoriaLogs answered /metrics before and does not now: it is too
		// busy to serve a 40 KB page, so it gets no new scan.
		return 0, false
	}
	if s.concOK {
		if s.selectCapacity > 0 && s.selectCurrent >= s.selectCapacity {
			return 0, false
		}
		// Fleet share: what VictoriaLogs runs for others counts against
		// this replica's limit. A long scan is CPU-bound and VictoriaLogs
		// already spreads one over all its cores, so scans beyond the limit
		// only slow each other down; replicas that never talk to each other
		// thereby hold the backend near one replica's limit, not
		// replicas x limit.
		// Below the floor a replica still gets a scan while others run long
		// work, as long as that work leaves most of VictoriaLogs' select
		// slots free: otherwise a few long queries of other clients (charts,
		// alerting) would keep label listings out for ever.
		share := float64(limit)
		if l.inFlight < l.floor && s.selectCapacity > 0 {
			share = max(share, s.selectCapacity/4)
		}
		if float64(l.inFlight)+others >= share {
			return 0, false
		}
	}
	if l.headroom <= 0 || !s.memOK || s.available <= 0 {
		return 0, true
	}
	return l.memoryGateLocked(class, s, background, w, others)
}

// memoryGateLocked is admitLocked's memory gate: it reports the memory a scan
// of class reserves if it may start now.
func (l *metadataScanLimiter) memoryGateLocked(class scanClass, s backendSignals, background bool, w *scanWaiter, others float64) (reserve float64, ok bool) {
	cost, known, verified := l.costLocked(class, s)
	alone := l.inFlight == 0 && others == 0
	budget := s.available * (1 - l.headroom)
	pending := l.pendingLocked(s)
	if !known && l.unknownInFlight > 0 {
		// At most one listing nobody has measured per replica.
		return 0, false
	}
	// A listing nobody has measured may be many times the prior on a large
	// backend, and replicas that meet it together must not all start it, so
	// it runs alone on VictoriaLogs; once it has run, its cost is known.
	if known || others == 0 {
		// The selects VictoriaLogs runs for others are counted at this
		// class's cost: other replicas run the same listings, and a scan
		// they started a moment ago holds none of its memory yet. This
		// double-counts the memory of scans that have run for a while, which
		// errs on the side of the backend.
		if s.inUse+pending+others*cost+cost <= budget {
			return cost, true
		}
		if alone && !l.shedRecentlyLocked(class) && (s.inUse+cost <= l.brakeMark(s) || (!verified && s.inUse <= budget)) {
			// Alone, a scan may use the headroom up to the brake mark:
			// refusing the only scan on an idle backend refuses it for ever.
			// A cost that was not measured alone, or not in the last hour,
			// is only an upper bound (others' growth landed in it, or the
			// data changed); the scan then runs alone to measure it again
			// rather than being refused for good. A class the brake stopped
			// in the last hour is not started again only to be stopped.
			return cost, true
		}
	}
	if verified || background || w == nil || w.escapeAt.IsZero() || s.at.Before(w.escapeAt) || l.escapesInFlight > 0 || l.shedRecentlyLocked(class) {
		return 0, false
	}
	// A request whose listing has no cost of its own measured alone (the
	// cost is unknown, borrowed from another range or tenant, or measured
	// beside other work) and that waited through other clients' steady work
	// (on a busy VictoriaLogs some select always runs, so nothing is ever
	// measured alone) goes ahead as more than half of the memory left in the
	// budget, once VictoriaLogs' memory in use has held steady for
	// metadataScanSteadyWindow: the work that kept running through its wait
	// then already shows in it, while a scan that started a moment ago (a
	// large one builds up for many seconds) keeps it growing. Every select
	// that appeared during its wait (another replica's escape) counts at the
	// escape's size, so escapes that meet go one at a time, and waiters
	// escape at randomised times so replicas that met the listing together
	// see each other.
	room := budget - s.inUse - pending
	if room <= 0 || l.now().Sub(s.at) > metadataScanEscapeReadAge || !l.steadyLocked(s, room/8) {
		return 0, false
	}
	escape := room/2 + 1
	if stats := l.classes[class]; stats != nil && stats.measured {
		// Its own measurement, even beside other work, is a bound that
		// stands; a cost borrowed from another range or tenant is a guess.
		escape = max(escape, stats.costBytes)
	}
	newcomers := max(s.others-max(w.minOthers, 0), 0)
	if newcomers*escape+escape > room {
		return 0, false
	}
	l.admitEscaped = true
	return escape, true
}

// brakeMark is the memory in use above which shedLocked stops scans.
func (l *metadataScanLimiter) brakeMark(s backendSignals) float64 {
	return s.available * (1 - l.headroom*2/3)
}

// shedRecentlyLocked reports a class whose scan the brake stopped within the
// last metadataScanCostTrust: its cost is at least what it had grown to.
func (l *metadataScanLimiter) shedRecentlyLocked(class scanClass) bool {
	stats := l.classes[class]
	return stats != nil && !stats.shedAt.IsZero() && l.now().Sub(stats.shedAt) <= metadataScanCostTrust
}

// costLocked is the memory a scan of class is expected to hold, whether any
// estimate exists (known), and whether the estimate is the class's own,
// measured alone in the last hour (verified).
//
// A class this replica has measured costs what it measured. Otherwise the
// same listing of the same tenant over another range stands in: a longer
// range's cost as is, a shorter one's scaled up by the ratio of the ranges
// (listings grow at most in proportion to their range). Then another
// tenant's measurement of the same listing and range, doubled: tenants
// differ, but a new tenant must not wait for an idle backend before it can
// list anything. Without any of these the prior is a tenth of the memory
// available to VictoriaLogs and the class is unknown: it runs alone
// (admitLocked).
func (l *metadataScanLimiter) costLocked(class scanClass, s backendSignals) (cost float64, known, verified bool) {
	if stats := l.classes[class]; stats != nil && stats.measured {
		fresh := l.now().Sub(stats.measuredAt) <= metadataScanCostTrust
		return stats.costBytes, true, stats.solo && fresh
	}
	sameTenant, otherTenant := 0.0, 0.0
	for other, stats := range l.classes {
		if !stats.measured || other.path != class.path {
			continue
		}
		switch {
		case other.tenant == class.tenant && class.bucket != 0 && other.bucket != 0:
			scaled := stats.costBytes
			if class.bucket > other.bucket {
				scaled *= float64(class.bucket) / float64(other.bucket)
			}
			if sameTenant == 0 || scaled < sameTenant {
				sameTenant = scaled
			}
		case other.tenant != class.tenant && other.bucket == class.bucket:
			otherTenant = max(otherTenant, 2*stats.costBytes)
		}
	}
	switch {
	case sameTenant > 0:
		return sameTenant, true, false
	case otherTenant > 0:
		return otherTenant, true, false
	}
	return s.available * metadataScanUnknownCostFraction, false, false
}

// metadataScanUnknownEscape is how long a synchronous scan whose listing has
// no cost measured alone waits for it to fit before it goes ahead as more than
// half of the memory left in the budget (plus up to 8 x
// metadataScanAdmissionJitter, so replicas escape one by one).
const metadataScanUnknownEscape = 2 * time.Second

// metadataScanCostTrust is how long a cost measured alone is trusted as the
// class's own. Past it the cost is an upper bound again, so a class whose
// cost grew past the free memory (the data shrank since, or VictoriaLogs got
// more memory) is measured again when VictoriaLogs is idle instead of being
// refused until the replica restarts.
const metadataScanCostTrust = time.Hour

// metadataScanMaxClasses bounds the classes a limiter remembers: tenants
// come from request headers.
const metadataScanMaxClasses = 1024

// pendingLocked is the memory this replica's scans in flight have reserved
// and not yet shown in the resident set.
func (l *metadataScanLimiter) pendingLocked(s backendSignals) float64 {
	pending := 0.0
	for t := range l.tokens {
		// Resident-set growth since a scan started is shared by everything
		// that ran meanwhile; each scan is credited with its share only.
		grown := 0.0
		if t.memOK && s.memOK {
			grown = max(s.inUse-t.useAtStart, 0) / float64(len(l.tokens))
		}
		pending += max(t.reserved-grown, 0)
	}
	return pending
}

// acquire waits until a scan of the given path and params may start, until
// deadline passes, or until ctx ends. Background inventory work never waits.
func (l *metadataScanLimiter) acquire(ctx context.Context, path string, params url.Values, deadline time.Time, background bool) (*scanToken, error) {
	if l == nil {
		return nil, nil
	}
	class := classifyScan(path, params, getOrgID(ctx))
	l.mu.Lock()
	_, known, _ := l.costLocked(class, l.sig)
	l.mu.Unlock()
	if !known && l.sampler != nil && l.jitter != nil {
		// Replicas that meet a listing nobody has measured at the same
		// moment decide a random fraction of a second apart, so each sees
		// the others' selects running and only one starts it. Measured
		// listings are bounded by their cost and go at once.
		if d := l.jitter(); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			}
		}
	}
	waiting := false
	w := &scanWaiter{minOthers: -1}
	defer func() {
		if waiting {
			l.mu.Lock()
			l.waiters--
			l.mu.Unlock()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		age := l.decisionAge
		if !w.escapeAt.IsZero() && !l.now().Before(w.escapeAt) {
			// An escape decides on a reading of its own, so replicas that
			// escape a moment apart see each other.
			age = metadataScanEscapeReadAge
		}
		s := l.signals(ctx, age)
		l.mu.Lock()
		if w.since.IsZero() {
			w.since = s.at
			if !background {
				w.escapeAt = w.since.Add(metadataScanUnknownEscape)
				if l.jitter != nil {
					w.escapeAt = w.escapeAt.Add(8 * l.jitter())
				}
			}
		}
		w.observe(s)
		if t := l.tryAdmitLocked(class, s, background, w, scanWork(ctx)); t != nil {
			startPoll := !l.polling && l.sampler != nil
			if startPoll {
				l.polling = true
			}
			l.mu.Unlock()
			if startPoll {
				go l.pollWhileInFlight()
			}
			return t, nil
		}
		wait := l.changed
		full := l.queueFullErrorLocked(s)
		if background || !l.now().Before(deadline) {
			l.rejected++
			l.mu.Unlock()
			return nil, full
		}
		if !waiting {
			waiting = true
			l.waiters++
		}
		l.mu.Unlock()
		timer := time.NewTimer(time.Until(deadline))
		recheck := time.NewTimer(l.recheck)
		select {
		case <-wait:
		case <-recheck.C:
		case <-ctx.Done():
			timer.Stop()
			recheck.Stop()
			return nil, ctx.Err()
		case <-timer.C:
			// One last look: the loop admits the scan if a slot opened at
			// the deadline, and rejects it otherwise.
		}
		timer.Stop()
		recheck.Stop()
	}
}

// tryAdmitLocked admits a scan of class if admitLocked allows it at the
// reading s and records it as in flight; it returns nil otherwise. rows is the
// work the scan will read, when the caller knows it.
func (l *metadataScanLimiter) tryAdmitLocked(class scanClass, s backendSignals, background bool, w *scanWaiter, rows int64) *scanToken {
	l.admitEscaped = false
	reserve, ok := l.admitLocked(class, s, background, w)
	if !ok {
		return nil
	}
	waitingSince := s.at
	if w != nil && !w.since.IsZero() {
		waitingSince = w.since
	}
	others := l.othersLocked(s, waitingSince)
	l.inFlight++
	l.admitted++
	unknown := false
	if _, known, _ := l.costLocked(class, s); !known {
		l.unknownRuns++
		unknown = l.headroom > 0 && s.memOK
		if unknown {
			l.unknownInFlight++
		}
	}
	t := &scanToken{
		class: class, start: l.now(), rows: rows, useAtStart: s.inUse, peakUse: s.inUse, memOK: s.memOK,
		concurrent: l.inFlight, fleetLoad: l.inFlight + int(others), reserved: reserve, background: background,
		decreaseEpoch: l.decreaseEpoch, unknown: unknown, escaped: l.admitEscaped,
	}
	l.seq++
	t.seq = l.seq
	if t.escaped {
		l.escapesInFlight++
	}
	for other := range l.tokens {
		if other.concurrent < l.inFlight {
			other.concurrent = l.inFlight
		}
	}
	l.tokens[t] = struct{}{}
	return t
}

// pollWhileInFlight reads VictoriaLogs' /metrics while scans run, so that a
// scan's cost is its peak memory growth, not what is left at its end.
func (l *metadataScanLimiter) pollWhileInFlight() {
	ticker := time.NewTicker(l.poll)
	defer ticker.Stop()
	for range ticker.C {
		l.mu.Lock()
		if l.inFlight == 0 {
			l.polling = false
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), l.poll)
		l.signals(ctx, l.poll/2)
		cancel()
	}
}

func (l *metadataScanLimiter) queueFullErrorLocked(s backendSignals) error {
	return &heavyQueryQueueFullError{
		limitFlag:     metadataScanLimitFlag,
		work:          metadataScanLimitWork,
		maxConcurrent: l.ceiling,
		queueWait:     l.queueWait,
		adaptive:      l.adaptiveNoteLocked(s),
	}
}

// adaptiveNoteLocked describes the limiter's state for the 429 body: the
// adaptive limit, and the backend's load when it is what closed the door.
func (l *metadataScanLimiter) adaptiveNoteLocked(s backendSignals) string {
	note := fmt.Sprintf("adaptive limit %d, floor %s=%d", int(l.limit), metadataScanMinFloorFlag, l.floor)
	if s.concOK && s.selectCapacity > 0 {
		note += fmt.Sprintf(", VictoriaLogs running %.0f of %.0f selects", s.selectCurrent, s.selectCapacity)
	}
	if l.silentLocked(s) {
		note += ", VictoriaLogs /metrics not answering"
	}
	if l.headroom > 0 && s.memOK && s.available > 0 {
		note += fmt.Sprintf(", VictoriaLogs memory %.0f%% used with %.0f%% reserved by this replica's scans against %s=%.2f",
			100*s.inUse/s.available, 100*l.pendingLocked(s)/s.available, metadataScanHeadroomFlag, l.headroom)
	}
	return note
}

// markDistress records that the scan's backend call failed (transport error,
// timeout, 5xx), which halves the limit when the token is released.
func (t *scanToken) markDistress() {
	if t != nil {
		t.distress.Store(true)
	}
}

// markAborted records that the scan's call ended without an answer the
// limiter can learn from (its client went away, or VictoriaLogs refused the
// request): its duration and memory say nothing about the class.
func (t *scanToken) markAborted() {
	if t != nil {
		t.aborted.Store(true)
	}
}

// distressLocked halves the limit once per wave: the scans in flight when
// VictoriaLogs failed fail for the same reason.
func (l *metadataScanLimiter) distressLocked() {
	l.distressEvents++
	l.decreaseEpoch++
	l.setLimitLocked(l.limit * metadataScanDistressFactor)
}

// release ends an admitted scan and feeds its outcome back into the limit and
// the class statistics. It is idempotent.
func (l *metadataScanLimiter) release(t *scanToken) {
	if l == nil || t == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.released {
		return
	}
	t.released = true
	delete(l.tokens, t)
	duration := l.now().Sub(t.start)
	l.inFlight--
	if t.unknown {
		l.unknownInFlight--
	}
	defer func() {
		close(l.changed)
		l.changed = make(chan struct{})
	}()
	if t.escaped {
		l.escapesInFlight--
	}
	distress := t.distress.Load()
	if distress {
		if t.decreaseEpoch == l.decreaseEpoch {
			l.distressLocked()
		}
		if t.shed.Load() {
			l.learnShedLocked(t)
		}
		return
	}
	if t.aborted.Load() {
		return
	}
	stats := l.classes[t.class]
	if stats == nil {
		l.evictClassLocked()
		stats = &scanClassStats{}
		l.classes[t.class] = stats
	}
	stats.samples++
	stats.lastUsed = l.now()
	// Alone: no other scan of this replica and no select of anyone else on
	// VictoriaLogs at any reading while it ran.
	alone := t.fleetLoad <= 1 && t.concurrent == 1 && t.maxOthers == 0

	// Memory cost: the peak growth of the memory in use while the scan ran.
	// Beside this replica's other scans it is shared evenly among them (a
	// scan of a class never measured keeps it all), and others' work lands
	// in it too, so a cost measured beside anything is an upper bound; one
	// measured alone is the scan's own and may lower the estimate.
	if l.headroom > 0 && t.memOK && l.sig.memOK && l.sig.available > 0 {
		share := max(t.peakUse-t.useAtStart, 0)
		if !t.unknown {
			share /= float64(max(t.concurrent, 1))
		}
		share = max(share, l.sig.available*metadataScanMinCostFraction)
		raised := false
		switch {
		case !stats.measured, alone && !stats.solo:
			stats.costBytes = share
		case share > stats.costBytes:
			stats.costBytes, raised = share, true // grow at once: underestimating is what kills the backend
		case alone:
			stats.costBytes = 0.8*stats.costBytes + 0.2*share
		}
		stats.measured = true
		stats.solo = alone || (stats.solo && !raised)
		stats.measuredAt = l.now()
	}

	slow := !alone && l.slowScanLocked(stats, t, duration)
	stats.baseline = trackBaseline(stats.baseline, float64(duration), alone)
	if t.rows > 0 {
		stats.baselinePerRow = trackBaseline(stats.baselinePerRow, float64(duration)/float64(t.rows), alone)
	}
	switch {
	case slow:
		if t.decreaseEpoch == l.decreaseEpoch {
			// One decrease per wave of slow scans: the scans that ran
			// beside this one are slow for the same reason.
			l.slowLocked()
		}
	case max(t.fleetLoad, t.concurrent) >= int(l.limit):
		// Grow only on evidence: this scan used the whole limit and still
		// ran within the tolerance.
		l.setLimitLocked(l.limit + 1/l.limit)
	}
}

// learnShedLocked records what a scan the brake stopped had grown to: a lower
// bound of its class's cost, which keeps the class from being started only
// to be stopped again (shedRecentlyLocked).
func (l *metadataScanLimiter) learnShedLocked(t *scanToken) {
	stats := l.classes[t.class]
	if stats == nil {
		l.evictClassLocked()
		stats = &scanClassStats{}
		l.classes[t.class] = stats
	}
	grown := max(t.peakUse-t.useAtStart, 0)
	if grown > stats.costBytes || !stats.measured {
		stats.costBytes = max(stats.costBytes, grown)
		stats.solo = false
	}
	stats.measured = true
	stats.measuredAt = l.now()
	stats.shedAt = l.now()
	stats.lastUsed = l.now()
}

// evictClassLocked makes room for a new class by forgetting the one used
// least recently, once metadataScanMaxClasses are remembered.
func (l *metadataScanLimiter) evictClassLocked() {
	if len(l.classes) < metadataScanMaxClasses {
		return
	}
	var oldest scanClass
	var at time.Time
	first := true
	for class, stats := range l.classes {
		if first || stats.lastUsed.Before(at) {
			oldest, at, first = class, stats.lastUsed, false
		}
	}
	delete(l.classes, oldest)
}

// slowScanLocked compares a scan that ran beside others with its class's
// no-load baseline, per row when both sides know their rows.
func (l *metadataScanLimiter) slowScanLocked(stats *scanClassStats, t *scanToken, duration time.Duration) bool {
	if t.rows > 0 && stats.baselinePerRow > 0 {
		return float64(duration)/float64(t.rows) > stats.baselinePerRow*l.tolerance
	}
	return stats.baseline > 0 && float64(duration) > stats.baseline*l.tolerance
}

// slowLocked shrinks the limit by a fifth and starts a new decrease epoch.
func (l *metadataScanLimiter) slowLocked() {
	l.decreaseEpoch++
	l.setLimitLocked(l.limit * metadataScanDecrease)
}

// trackBaseline follows the no-load duration of a class the way Vegas
// follows the base round-trip time: a faster sample becomes the baseline at
// once, a slower one pulls it up only slowly, so the baseline follows data
// growth without absorbing the slowdown of a busy moment. A solo scan is the
// best estimate there is and pulls harder.
func trackBaseline(prev, sample float64, solo bool) float64 {
	if prev == 0 || sample < prev {
		return sample
	}
	weight := metadataScanBaselineDrift
	if solo {
		weight = metadataScanBaselineWeight
	}
	return prev + weight*(sample-prev)
}

func (l *metadataScanLimiter) setLimitLocked(next float64) {
	next = max(next, float64(l.floor))
	if next > float64(l.ceiling) {
		next = float64(l.ceiling)
	}
	switch {
	case int(next) > int(l.limit):
		l.increases++
	case int(next) < int(l.limit):
		l.decreases++
	}
	l.limit = next
}

// metadataScanLimiterSnapshot is the limiter's state for logs and tests.
type metadataScanLimiterSnapshot struct {
	Limit, Floor, Ceiling, InFlight     int
	ReservedBytes                       float64
	Increases, Decreases, DistressCount int
	UnknownRuns, Admitted, Rejected     int
	Shed                                int
	Signals                             backendSignals
	Classes                             map[string]scanClassStats
}

func (l *metadataScanLimiter) snapshot() metadataScanLimiterSnapshot {
	if l == nil {
		return metadataScanLimiterSnapshot{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	classes := make(map[string]scanClassStats, len(l.classes))
	for class, stats := range l.classes {
		classes[class.String()] = *stats
	}
	return metadataScanLimiterSnapshot{
		Limit: int(l.limit), Floor: l.floor, Ceiling: l.ceiling, InFlight: l.inFlight, ReservedBytes: l.pendingLocked(l.sig),
		Increases: l.increases, Decreases: l.decreases, DistressCount: l.distressEvents,
		UnknownRuns: l.unknownRuns, Admitted: l.admitted, Rejected: l.rejected, Shed: l.shed, Signals: l.sig, Classes: classes,
	}
}

// backendHealthy reports whether VictoriaLogs has room for background
// inventory work right now: its memory inside the headroom and its select
// slots not all taken. Without readable signals it answers true.
func (l *metadataScanLimiter) backendHealthy(ctx context.Context) bool {
	if l == nil || l.sampler == nil {
		return true
	}
	s := l.signals(ctx, l.decisionAge)
	l.mu.Lock()
	silent := l.silentLocked(s)
	l.mu.Unlock()
	if silent {
		return false
	}
	if s.concOK && s.selectCapacity > 0 && s.selectCurrent >= s.selectCapacity {
		return false
	}
	if l.headroom > 0 && s.memOK && s.available > 0 && s.inUse > s.available*(1-l.headroom) {
		return false
	}
	return true
}

// sampleBackendSignals reads VictoriaLogs' /metrics. The response is about
// 40 KB and answers in a millisecond; it is read at most every 100 ms while
// scans are being admitted and every 500 ms while they run.
func (p *Proxy) sampleBackendSignals(ctx context.Context) backendSignals {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	u := *p.backend
	u.Path = "/metrics"
	u.RawQuery = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return backendSignals{failed: true}
	}
	p.applyBackendHeaders(req)
	resp, err := p.doBackendRequest(req, p.client)
	if err != nil {
		return backendSignals{failed: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return backendSignals{failed: true}
	}
	// The request asks for zstd or gzip unless VictoriaLogs is on loopback.
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		return backendSignals{failed: true}
	}
	return parseBackendSignals(io.LimitReader(resp.Body, 8<<20))
}

// parseBackendSignals extracts the limiter's signals from a Prometheus text
// exposition. The memory figures are trusted only next to VictoriaLogs'
// select-concurrency series: a vmauth or another proxy in front exports its
// own process_resident_memory_bytes, which says nothing about the backend.
//
// The memory in use is the anonymous resident set (the whole resident set
// when the anonymous one is not exported) minus the Go heap spans that are
// free but not yet returned to the OS (heap_idle - heap_released): the next
// scans reuse those before the resident set grows, so counting them would
// make a backend that just finished a burst of scans look full, and would
// hide the memory of the scans that reuse them.
func parseBackendSignals(r io.Reader) backendSignals {
	var (
		s                    backendSignals
		anon, rss            float64
		heapIdle, heapFree   float64
		haveCap, haveCurrent bool
	)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, value, found := strings.Cut(line, " ")
		if !found || strings.ContainsRune(name, '{') {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			continue
		}
		switch name {
		case "process_resident_memory_anon_bytes":
			anon = v
		case "process_resident_memory_bytes":
			rss = v
		case "go_memstats_heap_idle_bytes":
			heapIdle = v
		case "go_memstats_heap_released_bytes":
			heapFree = v
		case "vm_available_memory_bytes":
			s.available = v
		case "vl_concurrent_select_current":
			s.selectCurrent, haveCurrent = v, true
		case "vl_concurrent_select_capacity":
			s.selectCapacity, haveCap = v, true
		case "vl_concurrent_select_limit_reached_total":
			s.limitReached = v
		case "vl_concurrent_select_limit_timeout_total":
			s.limitTimeout = v
		}
	}
	s.concOK = haveCap && haveCurrent
	// Anonymous memory is what the scans allocate and what the kernel cannot
	// reclaim; file-backed pages of the mapped parts come and go with reads.
	resident := anon
	if resident <= 0 {
		resident = rss
	}
	s.inUse = resident
	if spare := heapIdle - heapFree; spare > 0 && spare < resident {
		s.inUse = resident - spare
	}
	s.memOK = s.concOK && s.inUse > 0 && s.available > 0
	return s
}

// isClientCancel reports an error caused by the caller going away, which is
// not backend distress.
func isClientCancel(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled
}

func resolveMetadataScanFloor(configured int) int {
	if configured <= 0 {
		return DefaultBackendMinConcurrentMetadataScans
	}
	return configured
}

// resolveMetadataScanHeadroom keeps 0 as "gate off" and maps a negative
// (unset) value to the default.
func resolveMetadataScanHeadroom(configured float64) float64 {
	if configured < 0 {
		return DefaultBackendMetadataScanMemoryHeadroom
	}
	return configured
}

func resolveMetadataScanTolerance(configured float64) float64 {
	if configured <= 0 {
		return DefaultBackendMetadataScanLatencyTolerance
	}
	return configured
}
