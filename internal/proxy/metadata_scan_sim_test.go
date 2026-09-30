package proxy

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

// A deterministic, single-threaded simulation of a fleet of proxy replicas
// admitting long-range metadata scans into one VictoriaLogs. Virtual time
// advances in fixed steps; nothing sleeps, no goroutine runs, and every random
// choice comes from a seeded generator, so a run is reproducible to the step.
//
// The VictoriaLogs model follows what the e2e stack showed (September 2026,
// v1.52.0, 8 GiB):
//   - One scan uses part of the CPU (perQuery of the cores); scans beside it
//     share what is left and lose some to contention, so a 7-day
//     stream_field_names that takes 9 s alone takes 12 s beside one other and
//     19 s beside two (measured 16.4 s for three at once).
//   - A scan's memory builds up over the first third of its progress and is
//     held until it ends; slower scans hold it longer, which is how seven at
//     once took the container from 1.7 to 6.4 GiB.
//   - Freed heap stays resident for a while (Go returns it to the OS lazily)
//     and is reused by the next scans, so the resident set a replica reads can
//     sit well above what the running scans hold.
//   - Above -search.maxConcurrentRequests calls queue inside VictoriaLogs and
//     time out after -search.maxQueueDuration with a 503.
//   - The process is OOM-killed when the memory the running scans hold, plus
//     its caches, exceeds the memory it has; every call in it fails, and it is
//     back after five seconds.
//
// The replicas share nothing but what VictoriaLogs' /metrics shows them.

const simStep = 5 * time.Millisecond

type simVLConfig struct {
	available, idle float64
	capacity        int
	perQuery        float64 // share of the cores one scan can use alone
	contention      float64 // throughput lost per extra running call
	maxQueue        time.Duration
	timeout         time.Duration
	retainedHalf    time.Duration // half-life of freed heap that stays resident
	// noHeapStats hides the Go heap series, so a replica sees the resident
	// set with the freed heap still in it.
	noHeapStats bool
}

func e2eVL() simVLConfig {
	return simVLConfig{available: 8 * gib, idle: 2.3 * gib, capacity: 16, perQuery: 0.6, contention: 0.1,
		maxQueue: 60 * time.Second, timeout: 60 * time.Second, retainedHalf: 60 * time.Second}
}

type simCall struct {
	replica     int
	req         *simReq // nil for other clients' short selects
	work, total float64 // CPU-seconds of the whole VictoriaLogs left / in all
	cost        float64
	rateCap     float64   // most of the node it can use (an I/O-bound query); 0 = no cap
	arrive      time.Time // reaches VictoriaLogs
	queuedAt    time.Time
	started     bool
	failed      bool
	done        bool
	shed        bool // stopped by its replica's limiter (shedLocked)
}

func (c *simCall) mem() float64 {
	if !c.started || c.total <= 0 {
		return 0
	}
	progress := 1 - c.work/c.total
	return c.cost * math.Min(1, 0.05+3*progress)
}

type simVL struct {
	cfg                        simVLConfig
	running, queued, transit   []*simCall
	cancelled                  []*simCall // stopped by their proxy since the last step
	dying                      []dyingMem
	retained                   float64
	peak, peakScans            float64
	ooms                       int
	downUntil                  time.Time
	limitReached, limitTimeout float64
}

// dyingMem is the memory of a cancelled call, which VictoriaLogs holds until
// it notices the cancellation and its garbage is collected.
type dyingMem struct {
	until time.Time
	mem   float64
}

func (v *simVL) scanMem() float64 {
	sum := 0.0
	for _, c := range v.running {
		sum += c.mem()
	}
	for _, d := range v.dying {
		sum += d.mem
	}
	return sum
}

// cancel stops a call the way a proxy cancelling its request does: it leaves
// VictoriaLogs' queues at once and its memory a second later.
func (v *simVL) cancel(c *simCall, now time.Time) {
	remove := func(list []*simCall) []*simCall {
		out := list[:0]
		for _, x := range list {
			if x != c {
				out = append(out, x)
			}
		}
		return out
	}
	if c.done {
		return
	}
	v.dying = append(v.dying, dyingMem{until: now.Add(time.Second), mem: c.mem()})
	v.running, v.queued, v.transit = remove(v.running), remove(v.queued), remove(v.transit)
	c.failed, c.done, c.shed = true, true, true
	v.cancelled = append(v.cancelled, c)
}

func (v *simVL) rss() float64 {
	return v.cfg.idle + math.Max(v.scanMem(), v.retained)
}

func (v *simVL) signals(now time.Time) backendSignals {
	if now.Before(v.downUntil) {
		return backendSignals{failed: true}
	}
	inUse := v.cfg.idle + v.scanMem()
	if v.cfg.noHeapStats {
		inUse = v.rss()
	}
	return backendSignals{inUse: inUse, available: v.cfg.available, memOK: true,
		selectCurrent: float64(len(v.running)), selectCapacity: float64(v.cfg.capacity),
		limitReached: v.limitReached, limitTimeout: v.limitTimeout, concOK: true}
}

// step advances VictoriaLogs by one step and returns the calls that ended.
func (v *simVL) step(now time.Time) (ended []*simCall) {
	ended, v.cancelled = append(ended, v.cancelled...), nil
	alive := v.dying[:0]
	for _, d := range v.dying {
		if now.Before(d.until) {
			alive = append(alive, d)
		}
	}
	v.dying = alive
	// Calls reach VictoriaLogs.
	keep := v.transit[:0]
	for _, c := range v.transit {
		switch {
		case now.Before(c.arrive):
			keep = append(keep, c)
		case now.Before(v.downUntil):
			c.failed, c.done = true, true
			ended = append(ended, c)
		default:
			c.queuedAt = now
			if len(v.running) >= v.cfg.capacity {
				v.limitReached++
			}
			v.queued = append(v.queued, c)
		}
	}
	v.transit = keep
	for len(v.queued) > 0 && len(v.running) < v.cfg.capacity {
		c := v.queued[0]
		v.queued = v.queued[1:]
		c.started = true
		v.running = append(v.running, c)
	}
	waiting := v.queued[:0]
	for _, c := range v.queued {
		if now.Sub(c.queuedAt) > v.cfg.maxQueue {
			v.limitTimeout++
			c.failed, c.done = true, true
			ended = append(ended, c)
			continue
		}
		waiting = append(waiting, c)
	}
	v.queued = waiting

	// Running calls progress.
	if k := float64(len(v.running)); k > 0 {
		total := math.Min(1, k*v.cfg.perQuery) / (1 + v.cfg.contention*(k-1))
		each := total / k * simStep.Seconds()
		still := v.running[:0]
		for _, c := range v.running {
			if c.rateCap > 0 && c.rateCap*simStep.Seconds() < each {
				c.work -= c.rateCap * simStep.Seconds()
			} else {
				c.work -= each
			}
			if c.work <= 0 || (c.req != nil && now.Sub(c.queuedAt) > v.cfg.timeout) {
				c.failed = c.work > 0
				c.done = true
				v.retained = math.Max(v.retained, v.scanMem())
				ended = append(ended, c)
				continue
			}
			still = append(still, c)
		}
		v.running = still
	}
	v.retained *= math.Pow(0.5, simStep.Seconds()/v.cfg.retainedHalf.Seconds())
	v.retained = math.Max(v.retained, v.scanMem())

	held := v.cfg.idle + v.scanMem()
	v.peak = math.Max(v.peak, v.rss())
	v.peakScans = math.Max(v.peakScans, float64(len(v.running)))
	if held > v.cfg.available {
		v.ooms++
		for _, c := range append(v.running, v.queued...) {
			c.failed, c.done = true, true
			ended = append(ended, c)
		}
		v.running, v.queued, v.retained = nil, nil, 0
		v.downUntil = now.Add(5 * time.Second)
	}
	return ended
}

// simReq is one scan a replica wants to run.
type simReq struct {
	replica      int
	at           time.Time // arrival at the proxy
	rng          time.Duration
	work, cost   float64
	background   bool
	queueWait    time.Duration
	retryEvery   time.Duration // background: retried on the jittered keep-warm schedule until done
	decideAt     time.Time
	waitingSince time.Time
	waiting      bool
	token        *scanToken
	outcome      string // served, refused, failed
	finished     time.Time
	attempts     int
	repeat       int // a closed-loop client: sends the same request again this many times, each when the last ended
	respawned    bool
	jittered     bool
	waiter       scanWaiter
}

type simReplica struct {
	l       *metadataScanLimiter
	static  bool // reads no /metrics
	sig     backendSignals
	readAt  time.Time
	pollAt  time.Time
	calls   int // this replica's calls in transit, queued or running
	changed bool
}

type simFleet struct {
	t        *testing.T
	now      time.Time
	vl       *simVL
	replicas []*simReplica
	reqs     []*simReq
	rng      *rand.Rand
	netDelay time.Duration
	noise    func(now time.Time, rng *rand.Rand) *simCall // other clients' short selects
	decides  int
	tick     func() // called once per step, after the decisions
}

func newSimFleet(t *testing.T, cfg simVLConfig, seed uint64) *simFleet {
	return &simFleet{t: t, now: time.Unix(1_790_000_000, 0), vl: &simVL{cfg: cfg}, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), netDelay: 3 * time.Millisecond}
}

// addReplica adds a replica whose limiter is built by mk. The limiter's clock
// is the simulation's.
func (f *simFleet) addReplica(l *metadataScanLimiter, static bool) {
	r := &simReplica{l: l, static: static}
	if l != nil {
		l.now = func() time.Time { return f.now }
		l.localCalls = func() int { return r.calls }
	}
	f.replicas = append(f.replicas, r)
}

func (f *simFleet) adaptiveReplicas(n int, ceiling int) {
	for i := 0; i < n; i++ {
		f.addReplica(newMetadataScanLimiter(ceiling, DefaultBackendMinConcurrentMetadataScans, DefaultBackendHeavyQueryQueueWait,
			DefaultBackendMetadataScanMemoryHeadroom, DefaultBackendMetadataScanLatencyTolerance, nil), false)
	}
}

func (f *simFleet) add(r *simReq) {
	r.decideAt = r.at
	f.reqs = append(f.reqs, r)
}

func (f *simFleet) read(r *simReplica, maxAge time.Duration) backendSignals {
	if r.static {
		return backendSignals{}
	}
	if r.readAt.IsZero() || f.now.Sub(r.readAt) > maxAge {
		r.l.mu.Lock()
		r.l.applySignalsLocked(f.vl.signals(f.now))
		r.sig = r.l.sig
		victim := r.l.shedLocked(r.sig)
		r.l.mu.Unlock()
		r.readAt = f.now
		if victim != nil {
			victim()
		}
	}
	return r.sig
}

// run advances the simulation until every request has an outcome or until
// the deadline, and returns the time it took.
func (f *simFleet) run(limit time.Duration) time.Duration {
	start := f.now
	calls := map[*simCall]bool{}
	for f.now.Sub(start) < limit {
		pending := false
		for _, r := range f.reqs {
			if r.outcome == "" {
				pending = true
				break
			}
		}
		if !pending {
			break
		}
		if f.noise != nil {
			if c := f.noise(f.now, f.rng); c != nil {
				c.arrive = f.now
				c.replica = -1
				f.vl.transit = append(f.vl.transit, c)
			}
		}
		for _, c := range f.vl.step(f.now) {
			if c.req == nil {
				continue
			}
			delete(calls, c)
			rep := f.replicas[c.replica]
			rep.calls--
			rep.changed = true
			req := c.req
			if rep.l != nil {
				if c.failed {
					req.token.markDistress()
				}
				rep.l.release(req.token)
			}
			switch {
			case c.shed:
				req.outcome = "refused" // the 429 of a scan the limiter stopped
			case c.failed:
				req.outcome = "failed"
			default:
				req.outcome = "served"
			}
			req.finished = f.now
		}
		// Replicas read /metrics while their scans run, for the peak memory.
		for _, rep := range f.replicas {
			if rep.l != nil && !rep.static && rep.l.inFlight > 0 && !f.now.Before(rep.pollAt) {
				f.read(rep, metadataScanPoll/2)
				rep.pollAt = f.now.Add(metadataScanPoll)
			}
		}
		for _, req := range f.reqs {
			rep := f.replicas[req.replica]
			if req.outcome != "" || req.token != nil || f.now.Before(req.at) {
				continue
			}
			if f.now.Before(req.decideAt) && (!req.waiting || !rep.changed) {
				continue
			}
			if !req.jittered && rep.l != nil && !rep.static {
				// The admission jitter of acquire, for a listing not yet measured.
				req.jittered = true
				rep.l.mu.Lock()
				_, known, _ := rep.l.costLocked(classifyScan(sfnPath, scanParams(req.rng), "0"), rep.l.sig)
				rep.l.mu.Unlock()
				if !known {
					req.decideAt = f.now.Add(time.Duration(f.rng.Int64N(int64(metadataScanAdmissionJitter))))
					continue
				}
			}
			f.decides++
			var tok *scanToken
			if rep.l == nil {
				tok = &scanToken{}
			} else {
				age := metadataScanDecisionAge
				if !req.waiter.escapeAt.IsZero() && !f.now.Before(req.waiter.escapeAt) {
					age = 0
				}
				s := f.read(rep, age)
				rep.l.mu.Lock()
				if req.waitingSince.IsZero() {
					req.waitingSince = f.now
				}
				if req.waiter.since.IsZero() {
					req.waiter = scanWaiter{since: f.now, minOthers: -1}
					if !req.background {
						req.waiter.escapeAt = f.now.Add(metadataScanUnknownEscape + time.Duration(f.rng.Int64N(int64(8*metadataScanAdmissionJitter))))
					}
				}
				req.waiter.observe(s)
				tok = rep.l.tryAdmitLocked(classifyScan(sfnPath, scanParams(req.rng), "0"), s, req.background, &req.waiter, 0)
				if tok == nil && (req.background || !f.now.Before(req.at.Add(req.queueWait))) {
					rep.l.rejected++
				}
				if tok != nil && req.waiting {
					rep.l.waiters--
					req.waiting = false
				}
				rep.l.mu.Unlock()
			}
			if tok != nil {
				req.token = tok
				req.attempts++
				c := &simCall{replica: req.replica, req: req, work: req.work, total: req.work, cost: req.cost, arrive: f.now.Add(f.netDelay)}
				if rep.l != nil && !rep.static {
					rep.l.attachCancel(tok, func() { f.vl.cancel(c, f.now) })
				}
				f.vl.transit = append(f.vl.transit, c)
				calls[c] = true
				rep.calls++
				continue
			}
			req.attempts++
			switch {
			case req.background && req.retryEvery > 0:
				// Skipped: the jittered keep-warm loop tries again.
				d := req.retryEvery - req.retryEvery/4 + time.Duration(f.rng.Int64N(int64(req.retryEvery/2)))
				req.decideAt = f.now.Add(d)
			case req.background || !f.now.Before(req.at.Add(req.queueWait)):
				if req.waiting {
					rep.l.mu.Lock()
					rep.l.waiters--
					rep.l.mu.Unlock()
					req.waiting = false
				}
				req.outcome, req.finished = "refused", f.now
			default:
				if !req.waiting {
					req.waiting = true
					rep.l.mu.Lock()
					rep.l.waiters++
					rep.l.mu.Unlock()
				}
				req.decideAt = f.now.Add(metadataScanRecheck)
			}
		}
		if f.tick != nil {
			f.tick()
		}
		for _, req := range f.reqs {
			if req.outcome != "" && req.repeat > 0 && !req.respawned {
				req.respawned = true
				next := *req
				next.at, next.repeat, next.respawned = f.now, req.repeat-1, false
				next.token, next.outcome, next.waiting, next.waitingSince, next.attempts, next.waiter, next.jittered = nil, "", false, time.Time{}, 0, scanWaiter{}, false
				f.add(&next)
			}
		}
		for _, rep := range f.replicas {
			rep.changed = false
		}
		f.now = f.now.Add(simStep)
	}
	return f.now.Sub(start)
}

type simOutcome struct {
	served, refused, failed, unfinished int
	shed                                int
	syncLatencies                       []time.Duration
}

func (f *simFleet) outcome() simOutcome {
	var o simOutcome
	for _, r := range f.reqs {
		switch r.outcome {
		case "served":
			o.served++
			if !r.background {
				o.syncLatencies = append(o.syncLatencies, r.finished.Sub(r.at))
			}
		case "refused":
			o.refused++
			if r.token != nil {
				o.shed++
			}
		case "failed":
			o.failed++
		default:
			o.unfinished++
		}
	}
	sort.Slice(o.syncLatencies, func(i, j int) bool { return o.syncLatencies[i] < o.syncLatencies[j] })
	return o
}

func (o simOutcome) p(q float64) time.Duration {
	if len(o.syncLatencies) == 0 {
		return 0
	}
	return o.syncLatencies[int(q*float64(len(o.syncLatencies)-1))]
}

// preset is one label window a replica warms or a user asks for: its range,
// the VictoriaLogs work (CPU-seconds of the whole node) and the memory its
// stream_field_names scan holds on the e2e data.
type simPreset struct {
	rng        time.Duration
	work, cost float64
}

// e2ePresets are measured on the e2e stack: 6h 1.2 s, 24h 3.3 s, 7d 8.8 s
// alone (perQuery 0.6 of the node), the 7d scan holding 0.8 GiB.
var e2ePresets = []simPreset{{6 * time.Hour, 0.72, 0.3 * gib}, {24 * time.Hour, 2.0, 0.5 * gib}, {7 * 24 * time.Hour, 5.3, 0.8 * gib}}

func scaled(presets []simPreset, work, cost float64) []simPreset {
	out := make([]simPreset, len(presets))
	for i, p := range presets {
		out[i] = simPreset{p.rng, p.work * work, p.cost * cost}
	}
	return out
}

// startTogether queues, for every replica, a background warm-up of every
// preset at t=0 (retried on a jittered keep-warm interval while skipped), and
// for the first users replicas a synchronous request for the longest preset.
func (f *simFleet) startTogether(presets []simPreset, users int, keepWarm time.Duration) {
	for i := range f.replicas {
		for _, p := range presets {
			f.add(&simReq{replica: i, at: f.now, rng: p.rng, work: p.work, cost: p.cost, background: true, retryEvery: keepWarm})
		}
	}
	longest := presets[len(presets)-1]
	for i := 0; i < users && i < len(f.replicas); i++ {
		f.add(&simReq{replica: i, at: f.now, rng: longest.rng, work: longest.work, cost: longest.cost, queueWait: DefaultBackendHeavyQueryQueueWait})
	}
}

func (f *simFleet) logf(name string, o simOutcome) {
	f.t.Helper()
	limits := make([]int, 0, len(f.replicas))
	for _, r := range f.replicas {
		if r.l != nil {
			limits = append(limits, int(r.l.limit))
		}
	}
	f.t.Logf("%s: peak %.2f GiB of %.1f (budget %.2f), %d OOM kills, most scans at once %.0f; served %d, refused %d (%d stopped), failed %d, unfinished %d; users p50 %s max %s; limits %v",
		name, f.vl.peak/gib, f.vl.cfg.available/gib, f.vl.cfg.available*(1-DefaultBackendMetadataScanMemoryHeadroom)/gib, f.vl.ooms, f.vl.peakScans,
		o.served, o.refused, o.shed, o.failed, o.unfinished, o.p(0.5).Round(10*time.Millisecond), o.p(1).Round(10*time.Millisecond), limits)
}
