package proxy

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const gib = float64(1 << 30)

func scanParams(rng time.Duration) url.Values {
	now := time.Unix(1_790_000_000, 0)
	v := url.Values{"query": {"*"}}
	if rng > 0 {
		v.Set("start", strconv.FormatInt(now.Add(-rng).UnixNano(), 10))
		v.Set("end", strconv.FormatInt(now.UnixNano(), 10))
	}
	return v
}

const sfnPath = "/select/logsql/stream_field_names"

// conformance: backend-admission-and-heavy-query-queueing, limits/metadata-scan-adaptive-limit
func TestClassifyScan_BucketsByRangeAndTenant(t *testing.T) {
	for _, tc := range []struct {
		rng  time.Duration
		want time.Duration
	}{
		{time.Hour, time.Hour}, {2 * time.Hour, 6 * time.Hour}, {6 * time.Hour, 6 * time.Hour}, {7 * time.Hour, 24 * time.Hour},
		{3 * 24 * time.Hour, 7 * 24 * time.Hour}, {20 * 24 * time.Hour, 30 * 24 * time.Hour}, {60 * 24 * time.Hour, 0}, {0, 0},
	} {
		if got := classifyScan(sfnPath, scanParams(tc.rng), "t1"); got.bucket != tc.want || got.path != sfnPath || got.tenant != "t1" {
			t.Fatalf("classifyScan(%s) = %v, want bucket %s", tc.rng, got, tc.want)
		}
	}
	if classifyScan(sfnPath, scanParams(time.Hour), "a") == classifyScan(sfnPath, scanParams(time.Hour), "b") {
		t.Fatal("tenants must not share a cost class")
	}
}

// The limit grows additively while scans stay fast, shrinks once per wave of
// slow concurrent scans, halves on distress, and never leaves [floor, ceiling].
//
// conformance: backend-admission-and-heavy-query-queueing, limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_LatencyAndDistressFeedback(t *testing.T) {
	l := newMetadataScanLimiter(8, 1, 0, 0, 1.5, nil)
	clock := time.Unix(1_790_000_000, 0)
	l.now = func() time.Time { return clock }
	params := scanParams(7 * 24 * time.Hour)
	ctx := context.Background()

	run := func(concurrent int, each time.Duration, distress bool) {
		t.Helper()
		tokens := make([]*scanToken, 0, concurrent)
		for i := 0; i < concurrent; i++ {
			tok, err := l.acquire(ctx, sfnPath, params, clock, false)
			if err != nil {
				t.Fatalf("acquire %d of %d at limit %d: %v", i+1, concurrent, l.snapshot().Limit, err)
			}
			tokens = append(tokens, tok)
		}
		clock = clock.Add(each)
		for _, tok := range tokens {
			if distress {
				tok.markDistress()
			}
			l.release(tok)
		}
	}

	if got := l.snapshot().Limit; got != metadataScanInitialLimit {
		t.Fatalf("initial limit = %d, want %d", got, metadataScanInitialLimit)
	}
	run(1, 10*time.Second, false) // baseline 10s
	for i := 0; i < 60 && l.snapshot().Limit < 8; i++ {
		run(l.snapshot().Limit, 12*time.Second, false) // within 1.5x: grows
	}
	if got := l.snapshot().Limit; got != 8 {
		t.Fatalf("limit after fast concurrent scans = %d, want the ceiling 8", got)
	}
	run(8, 30*time.Second, false) // 3x baseline beside others: one decrease for the wave
	if got := l.snapshot(); got.Limit != 6 || got.Decreases != 1 {
		t.Fatalf("after one slow wave: limit %d, decreases %d; want 6 (8 x 0.8) and one decrease", got.Limit, got.Decreases)
	}
	before := l.snapshot().Limit
	run(1, 10*time.Second, true) // distress halves
	if got := l.snapshot(); got.Limit > before/2+1 || got.DistressCount != 1 {
		t.Fatalf("limit after distress = %d (from %d, distress %d), want about half", got.Limit, before, got.DistressCount)
	}
	for i := 0; i < 10; i++ {
		run(1, 10*time.Second, true)
	}
	if got := l.snapshot().Limit; got != 1 {
		t.Fatalf("limit after repeated distress = %d, want the floor 1", got)
	}
	// At the floor one scan proceeds; a second one is refused (no wait), and
	// the refusal names the flags and the adaptive state.
	tok, err := l.acquire(ctx, sfnPath, params, clock, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.acquire(ctx, sfnPath, params, clock, false); !isHeavyQueryQueueFull(err) {
		t.Fatalf("second scan at the floor: err = %v, want queue-full", err)
	} else if msg := err.Error(); !strings.Contains(msg, metadataScanLimitFlag+"=8") || !strings.Contains(msg, "adaptive limit 1") || !strings.Contains(msg, metadataScanMinFloorFlag+"=1") || !strings.Contains(msg, "too many outstanding requests") {
		t.Fatalf("rejection does not describe the adaptive state and flags: %s", msg)
	}
	l.release(tok)
}

// Per-row baselines keep a scan of a busy day from looking slow next to a
// scan of a quiet one.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_CostWeightedByRows(t *testing.T) {
	l := newMetadataScanLimiter(8, 1, 0, 0, 1.5, nil)
	clock := time.Unix(1_790_000_000, 0)
	l.now = func() time.Time { return clock }
	params := scanParams(24 * time.Hour)
	quiet := withScanWork(context.Background(), 1_000_000)
	busy := withScanWork(context.Background(), 5_000_000)
	tok, _ := l.acquire(quiet, sfnPath, params, clock, false)
	clock = clock.Add(2 * time.Second)
	l.release(tok)
	a, _ := l.acquire(busy, sfnPath, params, clock, false)
	b, _ := l.acquire(busy, sfnPath, params, clock, false)
	clock = clock.Add(11 * time.Second) // 5x the rows, 5.5x the time: within tolerance per row
	l.release(a)
	l.release(b)
	if got := l.snapshot(); got.Decreases != 0 {
		t.Fatalf("a busy day's scan counted as slow: %+v", got)
	}
}

// While VictoriaLogs runs as many selects as it allows, nothing is admitted,
// and a rise of its queue-timeout counter halves the limit.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_BackendSaturationSignals(t *testing.T) {
	sig := backendSignals{inUse: gib, available: 8 * gib, memOK: true, selectCurrent: 16, selectCapacity: 16, concOK: true}
	var mu sync.Mutex
	l := newMetadataScanLimiter(8, 1, 0, 0.3, 1.5, func(context.Context) backendSignals {
		mu.Lock()
		defer mu.Unlock()
		return sig
	})
	l.jitter, l.decisionAge = nil, 0
	measure(l, 24*time.Hour, 0.1*gib)
	if _, err := l.acquire(context.Background(), sfnPath, scanParams(24*time.Hour), time.Now(), false); !isHeavyQueryQueueFull(err) {
		t.Fatalf("admitted a scan while VictoriaLogs runs all 16 selects: %v", err)
	} else if !strings.Contains(err.Error(), "VictoriaLogs running 16 of 16 selects") {
		t.Fatalf("rejection does not name the saturation: %v", err)
	}
	mu.Lock()
	sig.selectCurrent = 2
	mu.Unlock()
	l.mu.Lock()
	l.limit = 8
	l.mu.Unlock()
	tok, err := l.acquire(context.Background(), sfnPath, scanParams(24*time.Hour), time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	sig.limitTimeout = 3
	mu.Unlock()
	l.signals(context.Background(), 0)
	if got := l.snapshot().Limit; got != 4 {
		t.Fatalf("limit after a VictoriaLogs queue timeout = %d, want 4", got)
	}
	l.release(tok)
}

// conformance: limits/metadata-scan-adaptive-limit
func TestParseBackendSignals(t *testing.T) {
	body := "# HELP x\nvl_concurrent_select_current 3\nvl_concurrent_select_capacity 16\nvl_concurrent_select_limit_reached_total 484\n" +
		"vl_concurrent_select_limit_timeout_total 2\nprocess_resident_memory_bytes 3679182848\nprocess_resident_memory_anon_bytes 2103779328\n" +
		"vm_available_memory_bytes 8589934592\nvl_http_requests_total{path=\"/select/logsql/query\"} 7\n"
	s := parseBackendSignals(strings.NewReader(body))
	if !s.memOK || !s.concOK || s.inUse != 2103779328 || s.available != 8589934592 || s.selectCurrent != 3 || s.selectCapacity != 16 || s.limitReached != 484 || s.limitTimeout != 2 {
		t.Fatalf("parsed %+v", s)
	}
	// A vmauth in front exports its own process memory: not the backend's.
	vmauth := parseBackendSignals(strings.NewReader("process_resident_memory_bytes 50000000\nvm_available_memory_bytes 8589934592\n"))
	if vmauth.memOK || vmauth.concOK {
		t.Fatalf("memory of a process without VictoriaLogs select series must not be trusted: %+v", vmauth)
	}
	old := parseBackendSignals(strings.NewReader("vl_concurrent_select_current 1\nvl_concurrent_select_capacity 8\nprocess_resident_memory_bytes 1000\nvm_available_memory_bytes 8000\n"))
	if !old.memOK || old.inUse != 1000 {
		t.Fatalf("without anonymous memory the resident set is used: %+v", old)
	}
	// Freed heap spans not yet returned to the OS are resident but reused by
	// the next scans: they are not memory in use (numbers from the e2e
	// VictoriaLogs a minute after a burst of scans).
	heap := parseBackendSignals(strings.NewReader(body + "go_memstats_heap_idle_bytes 2000000000\ngo_memstats_heap_released_bytes 500000000\n"))
	if heap.inUse != 2103779328-1500000000 {
		t.Fatalf("in use = %.0f, want the anonymous resident set minus the idle unreleased heap", heap.inUse)
	}
}

// measure records a measured cost for the class of a 24h-style scan of the
// given range, as a finished scan would.
func measure(l *metadataScanLimiter, rng time.Duration, cost float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.classes[classifyScan(sfnPath, scanParams(rng), "")] = &scanClassStats{measured: true, solo: true, measuredAt: time.Now(), costBytes: cost, samples: 1}
}

// The memory gate: a class never measured runs alone (no scan of unknown
// cost beside anyone's long work, at most one per replica); the selects of
// others count at the class's cost; a scan alone may use the headroom but
// not the memory VictoriaLogs has; and another tenant's measurement stands
// in, doubled, for a tenant's first scan of the same listing.
//
// conformance: limits/metadata-scan-adaptive-limit, limits/concurrent-full-retention-scans-exhaust-backend
func TestMetadataScanLimiter_MemoryGate(t *testing.T) {
	sig := backendSignals{inUse: 2 * gib, available: 8 * gib, memOK: true, selectCurrent: 1, selectCapacity: 16, concOK: true}
	l := newMetadataScanLimiter(8, 1, 0, 0.3, 1.5, nil)
	l.limit = 8
	class := classifyScan(sfnPath, scanParams(7*24*time.Hour), "a")
	admit := func(s backendSignals) bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		s.at, s.others = time.Now(), s.selectCurrent
		_, ok := l.admitLocked(class, s, false, nil)
		return ok
	}
	if admit(sig) {
		t.Fatal("a scan of unknown cost started beside another client's select")
	}
	sig.selectCurrent = 0
	if !admit(sig) {
		t.Fatal("a scan of unknown cost refused on an idle backend")
	}
	l.unknownInFlight = 1
	if admit(sig) {
		t.Fatal("a second scan of unknown cost started on the same replica")
	}
	l.unknownInFlight = 0

	// Measured at 1.5 GiB: with two others' selects counted at that cost,
	// 2 + 3 + 1.5 = 6.5 GiB passes the 5.6 GiB budget.
	l.classes[class] = &scanClassStats{measured: true, solo: true, measuredAt: time.Now(), costBytes: 1.5 * gib}
	sig.selectCurrent = 2
	if admit(sig) {
		t.Fatal("admitted although others' selects at this cost exceed the budget")
	}
	sig.selectCurrent = 1
	if !admit(sig) {
		t.Fatal("refused although 2 + 1.5 + 1.5 GiB is inside the budget")
	}
	// Alone, a scan may use the headroom up to the brake mark (6.4 GiB).
	sig.selectCurrent, sig.inUse = 0, 4.5*gib
	if !admit(sig) {
		t.Fatal("the only scan on an idle backend was refused for the headroom")
	}
	sig.inUse = 7 * gib
	if admit(sig) {
		t.Fatal("a scan that cannot fit in VictoriaLogs' memory was admitted")
	}
	// A cost measured beside other scans is only an upper bound: alone, the
	// scan runs to measure it again while the backend is inside the budget,
	// instead of being refused until the replica restarts. The same holds
	// for a cost measured alone more than an hour ago.
	l.classes[class] = &scanClassStats{measured: true, solo: false, measuredAt: time.Now(), costBytes: 3.5 * gib}
	sig.inUse = 5 * gib
	if !admit(sig) {
		t.Fatal("an upper-bound cost locked the class out of an idle backend")
	}
	l.classes[class] = &scanClassStats{measured: true, solo: true, measuredAt: time.Now().Add(-2 * time.Hour), costBytes: 3.5 * gib}
	if !admit(sig) {
		t.Fatal("a cost measured alone two hours ago locked the class out")
	}
	l.classes[class].measuredAt = time.Now()
	if admit(sig) {
		t.Fatal("a fresh cost measured alone that does not fit was admitted")
	}
	// A class the brake stopped in the last hour is not started again only to
	// be stopped: alone, it must fit below the brake mark.
	l.classes[class] = &scanClassStats{measured: true, measuredAt: time.Now(), shedAt: time.Now(), costBytes: 3 * gib}
	if admit(sig) {
		t.Fatal("a class the brake just stopped was started again alone")
	}
	sig.inUse = 2 * gib
	if !admit(sig) {
		t.Fatal("a stopped class that now fits below the brake mark was refused")
	}
	l.classes[class] = &scanClassStats{measured: true, solo: true, measuredAt: time.Now(), costBytes: 1.5 * gib}

	// Another tenant's first scan of the same listing: twice the measured
	// cost; the same tenant's listing over a longer range: scaled by the
	// ranges; neither is the class's own measurement.
	other := classifyScan(sfnPath, scanParams(7*24*time.Hour), "b")
	if cost, known, verified := l.costLocked(other, sig); !known || verified || cost != 3*gib {
		t.Fatalf("new tenant's prior = %.1f GiB (known %v, verified %v), want 3 GiB", cost/gib, known, verified)
	}
	day := classifyScan(sfnPath, scanParams(24*time.Hour), "c")
	l.classes[day] = &scanClassStats{measured: true, solo: true, measuredAt: time.Now(), costBytes: 0.1 * gib}
	week := classifyScan(sfnPath, scanParams(7*24*time.Hour), "c")
	if cost, known, verified := l.costLocked(week, sig); !known || verified || cost < 0.69*gib || cost > 0.71*gib {
		t.Fatalf("7-day prior from a measured day = %.2f GiB (known %v, verified %v), want 0.7 GiB", cost/gib, known, verified)
	}
}

// A scan whose client went away, or that VictoriaLogs refused, teaches the
// limiter nothing: its class stays unmeasured and its baseline untouched.
// Failures while scans are in flight cut the limit once per wave.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_AbortedScansAndDistressWaves(t *testing.T) {
	l := newMetadataScanLimiter(8, 1, 0, 0.3, 1.5, nil)
	clock := time.Unix(1_790_000_000, 0)
	l.now = func() time.Time { return clock }
	l.limit = 8
	class := classifyScan(sfnPath, scanParams(7*24*time.Hour), "")
	sig := backendSignals{inUse: 2 * gib, available: 8 * gib, memOK: true, selectCapacity: 16, concOK: true, at: clock}
	admit := func() *scanToken {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.sig = sig
		return l.tryAdmitLocked(class, sig, false, nil, 0)
	}
	tok := admit()
	if tok == nil {
		t.Fatal("first scan refused on an idle backend")
	}
	clock = clock.Add(200 * time.Millisecond)
	tok.markAborted()
	l.release(tok)
	if st := l.classes[class]; st != nil && (st.measured || st.baseline != 0) {
		t.Fatalf("an aborted scan was learned from: %+v", *st)
	}

	tokens := []*scanToken{admit(), admit(), admit()}
	if tokens[1] != nil {
		// Unmeasured: only one runs.
		t.Fatal("a second scan of an unmeasured listing was admitted")
	}
	clock = clock.Add(9 * time.Second)
	l.release(tokens[0])
	l.classes[class].costBytes = 0.1 * gib // cheap enough for several at once
	tokens = []*scanToken{admit(), admit(), admit()}
	for _, tok := range tokens {
		if tok == nil {
			t.Fatal("measured cheap scans refused")
		}
		tok.markDistress()
	}
	for _, tok := range tokens {
		l.release(tok)
	}
	if got := l.snapshot(); got.Limit != 4 || got.DistressCount != 1 {
		t.Fatalf("three scans failing together: limit %d with %d distress cuts, want 4 and one cut", got.Limit, got.DistressCount)
	}
}

// Selects VictoriaLogs runs for others count against this replica's limit;
// its own calls (the short edge and count calls of the same listing) do not.
// Background work never takes the last slot while a request could use it,
// even at a limit of one, and a backend that stops answering /metrics after
// answering it gets no new scan.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_FleetShareBackgroundAndSilence(t *testing.T) {
	var mu sync.Mutex
	sig := backendSignals{inUse: gib, available: 8 * gib, memOK: true, selectCurrent: 4, selectCapacity: 16, concOK: true}
	mine := 0
	l := newMetadataScanLimiter(8, 1, 0, 0.3, 1.5, func(context.Context) backendSignals {
		mu.Lock()
		defer mu.Unlock()
		return sig
	})
	l.jitter, l.decisionAge = nil, 0
	measure(l, 24*time.Hour, 0.1*gib)
	l.localCalls = func() int { mu.Lock(); defer mu.Unlock(); return mine }
	params := scanParams(24 * time.Hour)
	ctx := context.Background()

	// Limit 2, four long selects of others (a quarter of the slots): closed.
	if _, err := l.acquire(ctx, sfnPath, params, time.Now(), false); !isHeavyQueryQueueFull(err) {
		t.Fatalf("admitted while others run 4 selects against a limit of 2: %v", err)
	}
	// The same three selects are this replica's own short calls: open.
	mu.Lock()
	mine = 3
	mu.Unlock()
	tok, err := l.acquire(ctx, sfnPath, params, time.Now(), false)
	if err != nil {
		t.Fatalf("own calls closed the limiter: %v", err)
	}
	l.release(tok)

	// Limit 1 (floor): background waits while a request could use the slot,
	// and runs on an idle backend nobody waits for.
	l.mu.Lock()
	l.limit = 1
	l.mu.Unlock()
	mu.Lock()
	sig.selectCurrent, mine = 0, 0
	mu.Unlock()
	held, err := l.acquire(ctx, sfnPath, params, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.acquire(ctx, sfnPath, params, time.Now(), true); !isHeavyQueryQueueFull(err) {
		t.Fatalf("background took a slot at limit 1 while a request held it: %v", err)
	}
	l.release(held)
	if tok, err := l.acquire(ctx, sfnPath, params, time.Now(), true); err != nil {
		t.Fatalf("background refused on an idle backend: %v", err)
	} else {
		l.release(tok)
	}

	// VictoriaLogs stops answering /metrics: no new scan.
	mu.Lock()
	sig = backendSignals{failed: true}
	mu.Unlock()
	l.signals(ctx, 0)
	if _, err := l.acquire(ctx, sfnPath, params, time.Now(), false); !isHeavyQueryQueueFull(err) || !strings.Contains(err.Error(), "/metrics not answering") {
		t.Fatalf("admitted while VictoriaLogs does not answer /metrics: %v", err)
	}
	// A /metrics that keeps failing past the silence window (an auth change,
	// a rule dropping it) no longer closes the limiter: latency and failure
	// feedback remain.
	l.mu.Lock()
	l.failingSince = time.Now().Add(-2 * l.silenceWindow)
	l.mu.Unlock()
	tok, err = l.acquire(ctx, sfnPath, params, time.Now(), false)
	if err != nil {
		t.Fatalf("a lasting /metrics failure kept the limiter closed: %v", err)
	}
	l.release(tok)
}

// Below the floor a replica still gets a scan while other clients' long work
// leaves most of VictoriaLogs' select slots free; above a quarter of them it
// waits.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_FloorIsNotStarvedByOtherClients(t *testing.T) {
	var mu sync.Mutex
	sig := backendSignals{inUse: gib, available: 8 * gib, memOK: true, selectCurrent: 3, selectCapacity: 16, concOK: true}
	l := newMetadataScanLimiter(8, 1, 0, 0.3, 1.5, func(context.Context) backendSignals {
		mu.Lock()
		defer mu.Unlock()
		return sig
	})
	l.jitter, l.decisionAge = nil, 0
	measure(l, 24*time.Hour, 0.1*gib)
	l.mu.Lock()
	l.limit = 1
	l.mu.Unlock()
	params := scanParams(24 * time.Hour)
	tok, err := l.acquire(context.Background(), sfnPath, params, time.Now(), false)
	if err != nil {
		t.Fatalf("three long selects of other clients kept a replica with nothing in flight out: %v", err)
	}
	if _, err := l.acquire(context.Background(), sfnPath, params, time.Now(), false); !isHeavyQueryQueueFull(err) {
		t.Fatalf("a second scan above the floor was admitted: %v", err)
	}
	l.release(tok)
	mu.Lock()
	sig.selectCurrent = 4
	mu.Unlock()
	if _, err := l.acquire(context.Background(), sfnPath, params, time.Now(), false); !isHeavyQueryQueueFull(err) {
		t.Fatalf("admitted while others' long work holds a quarter of the select slots: %v", err)
	}
}

// On a VictoriaLogs that always runs another client's select, a request for a
// listing nobody has measured does not wait for it to be idle for ever: after
// its escape time, on a fresh reading, with the memory in use steady for five
// seconds and no select started since it began waiting, it goes ahead
// reserving more than half the room left in the budget, so escapes that meet
// go one at a time. Background work never escapes.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanLimiter_UnmeasuredListingEscapesSteadyLoad(t *testing.T) {
	l := newMetadataScanLimiter(8, 1, 0, 0.3, 1.5, nil)
	clock := time.Unix(1_790_000_000, 0)
	l.now = func() time.Time { return clock }
	l.limit = 8
	class := classifyScan(sfnPath, scanParams(7*24*time.Hour), "")
	start := clock
	w := &scanWaiter{since: start, escapeAt: start.Add(metadataScanUnknownEscape), minOthers: -1}
	read := func(inUse, running float64) backendSignals {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.applySignalsLocked(backendSignals{inUse: inUse, available: 8 * gib, memOK: true, selectCurrent: running, selectCapacity: 16, concOK: true})
		w.observe(l.sig)
		return l.sig
	}
	admit := func(s backendSignals, background bool) (float64, bool) {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.admitLocked(class, s, background, w)
	}
	var s backendSignals
	for clock.Sub(start) < time.Second {
		s = read(2*gib, 1)
		clock = clock.Add(250 * time.Millisecond)
	}
	if _, ok := admit(s, false); ok {
		t.Fatal("an unmeasured listing started beside another client's select before its escape time")
	}
	for clock.Sub(start) <= metadataScanSteadyWindow+time.Second {
		s = read(2*gib, 1)
		clock = clock.Add(250 * time.Millisecond)
	}
	clock = clock.Add(-250 * time.Millisecond) // the reading is fresh
	if _, ok := admit(s, true); ok {
		t.Fatal("background work escaped")
	}
	reserve, ok := admit(s, false)
	if !ok || reserve < (5.6*gib-2*gib)/2 {
		t.Fatalf("escape after a steady wait: ok %v reserve %.2f GiB, want more than half of 3.6 GiB", ok, reserve/gib)
	}
	// Another replica's escape started since: it counts at the same size.
	clock = clock.Add(250 * time.Millisecond)
	if _, ok := admit(read(2*gib, 2), false); ok {
		t.Fatal("two escapes met and both went ahead")
	}
	// Memory still building up (a large scan that started a moment ago).
	w = &scanWaiter{since: clock, escapeAt: clock, minOthers: -1}
	for i := 0; i < 24; i++ {
		s = read(2*gib+float64(i)*0.1*gib, 1)
		clock = clock.Add(250 * time.Millisecond)
	}
	clock = clock.Add(-250 * time.Millisecond)
	if _, ok := admit(s, false); ok {
		t.Fatal("escaped while the memory in use kept growing")
	}
}
