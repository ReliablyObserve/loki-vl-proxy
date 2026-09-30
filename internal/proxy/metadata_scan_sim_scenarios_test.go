package proxy

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// Eleven replicas start together against the e2e VictoriaLogs; each warms the
// 6h, 24h and 7d presets in the background, and seven users open a 7-day label
// browser on seven of them at the same moment. Without admission, and with a
// static two scans per replica, VictoriaLogs is OOM-killed, as it was at
// 07:17:08Z on 2026-09-24. Through the adaptive limiter it stays up, the fleet
// is warm after a few keep-warm ticks, and every user is served or refused
// with the 429, never failed.
//
// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend, limits/metadata-scan-adaptive-limit
func TestMetadataScanSim_ReplicasStartingTogether(t *testing.T) {
	skipSimulationUnderRace(t)
	const replicas, users = 11, 7
	keepWarm := 225 * time.Second
	t.Run("no admission", func(t *testing.T) {
		f := newSimFleet(t, e2eVL(), 1)
		f.vl.cfg.capacity = 64
		for i := 0; i < replicas; i++ {
			f.addReplica(nil, false)
		}
		f.startTogether(e2ePresets, users, 0)
		f.run(10 * time.Minute)
		f.logf("no admission", f.outcome())
		if f.vl.ooms == 0 {
			t.Fatal("the unbounded fleet should kill the simulated VictoriaLogs; the model is too lenient")
		}
	})
	t.Run("static two per replica", func(t *testing.T) {
		f := newSimFleet(t, e2eVL(), 1)
		f.vl.cfg.capacity = 64
		for i := 0; i < replicas; i++ {
			f.addReplica(newMetadataScanLimiter(2, 2, 0, 0, 1.5, nil), true)
		}
		f.startTogether(e2ePresets, users, keepWarm)
		f.run(10 * time.Minute)
		f.logf("static two per replica", f.outcome())
		if f.vl.ooms == 0 {
			t.Fatal("22 scans at once should kill the simulated VictoriaLogs")
		}
	})
	t.Run("adaptive", func(t *testing.T) {
		for seed := uint64(1); seed <= 10; seed++ {
			cfg := e2eVL()
			// Half the runs read a VictoriaLogs whose /metrics lacks the Go
			// heap series: the freed heap then stays in the memory in use.
			cfg.noHeapStats = seed%2 == 0
			f := newSimFleet(t, cfg, seed)
			f.adaptiveReplicas(replicas, DefaultBackendMaxConcurrentMetadataScans)
			f.startTogether(e2ePresets, users, keepWarm)
			f.run(30 * time.Minute)
			o := f.outcome()
			f.logf(fmt.Sprintf("adaptive seed %d (heap series %v)", seed, !cfg.noHeapStats), o)
			if f.vl.ooms > 0 || f.vl.peak > f.vl.cfg.available*(1-DefaultBackendMetadataScanMemoryHeadroom/2) {
				t.Fatalf("seed %d: VictoriaLogs peaked at %.2f GiB with %d OOM kills", seed, f.vl.peak/gib, f.vl.ooms)
			}
			if o.failed > 0 || o.unfinished > 0 {
				t.Fatalf("seed %d: %d scans failed, %d never finished", seed, o.failed, o.unfinished)
			}
			if o.served < replicas*len(e2ePresets) {
				t.Fatalf("seed %d: only %d scans served; the fleet never got warm", seed, o.served)
			}
		}
	})
}

// The same fleet over data 10x and 100x larger. On the same 8 GiB node a
// 7-day scan holds 3 GiB (two at once kill it); on nodes sized for the data
// the scans are as many times heavier. The limiter learns every size from
// the backend and holds it under its memory in each.
//
// conformance: limits/concurrent-full-retention-scans-exhaust-backend, limits/metadata-scan-adaptive-limit
func TestMetadataScanSim_DataVolumes(t *testing.T) {
	skipSimulationUnderRace(t)
	for _, tc := range []struct {
		name            string
		available, idle float64
		work, cost      float64
	}{
		{"e2e data on 8 GiB", 8 * gib, 2.3 * gib, 1, 1},
		{"10x data on the same 8 GiB node", 8 * gib, 1.5 * gib, 10, 3.75},
		{"10x data on a 64 GiB node", 64 * gib, 16 * gib, 10, 10},
		{"100x data on a 256 GiB node", 256 * gib, 60 * gib, 30, 75},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 3; seed++ {
				cfg := e2eVL()
				cfg.available, cfg.idle = tc.available, tc.idle
				cfg.timeout, cfg.maxQueue = 10*time.Minute, 10*time.Minute
				f := newSimFleet(t, cfg, seed)
				f.adaptiveReplicas(11, DefaultBackendMaxConcurrentMetadataScans)
				presets := scaled(e2ePresets, tc.work, tc.cost)
				f.startTogether(presets, 7, 225*time.Second)
				f.run(2 * time.Hour)
				o := f.outcome()
				f.logf(fmt.Sprintf("%s seed %d", tc.name, seed), o)
				if f.vl.ooms > 0 || f.vl.peak > cfg.available {
					t.Fatalf("seed %d: VictoriaLogs peaked at %.2f of %.1f GiB with %d OOM kills", seed, f.vl.peak/gib, cfg.available/gib, f.vl.ooms)
				}
				if o.failed > 0 || o.unfinished > 0 || o.served < 33 {
					t.Fatalf("seed %d: served %d, failed %d, unfinished %d", seed, o.served, o.failed, o.unfinished)
				}
			}
		})
	}
}

// A small, lightly loaded backend with cheap scans: the limit grows to the
// ceiling and scans run side by side, so the limiter costs nothing there.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanSim_SmallBackendIsNotStarved(t *testing.T) {
	skipSimulationUnderRace(t)
	cfg := simVLConfig{available: 2 * gib, idle: 0.2 * gib, capacity: 16, perQuery: 0.12, contention: 0.02,
		maxQueue: time.Minute, timeout: time.Minute, retainedHalf: time.Minute}
	f := newSimFleet(t, cfg, 7)
	f.adaptiveReplicas(1, DefaultBackendMaxConcurrentMetadataScans)
	// 400 cheap 24h scans (0.05 s alone, 8 MiB), 40 arriving every 100 ms.
	for i := 0; i < 400; i++ {
		f.add(&simReq{replica: 0, at: f.now.Add(time.Duration(i/40) * 100 * time.Millisecond), rng: 24 * time.Hour,
			work: 0.006, cost: 8 << 20, queueWait: DefaultBackendHeavyQueryQueueWait})
	}
	took := f.run(10 * time.Minute)
	o := f.outcome()
	f.logf("small backend", o)
	serial := 400 * 50 * time.Millisecond
	if o.served != 400 {
		t.Fatalf("served %d of 400", o.served)
	}
	if lim := int(f.replicas[0].l.limit); lim < DefaultBackendMaxConcurrentMetadataScans-1 {
		t.Fatalf("limit %d on a backend where eight scans run nearly as fast as one", lim)
	}
	if took > serial/4 {
		t.Fatalf("400 cheap scans took %s (serial %s): the limiter held a small backend back", took, serial)
	}
}

// Six users keep one replica busy with 24h label scans, each asking again as
// soon as the last answer arrives. The limit settles where scans still run
// within the latency tolerance (two at once take 1.3x as long as one, three
// 2.2x on this model): it neither collapses to the floor nor climbs to the
// ceiling, and nobody is refused or failed.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanSim_LimitConverges(t *testing.T) {
	skipSimulationUnderRace(t)
	cfg := e2eVL()
	cfg.available = 64 * gib // memory is not the constraint here
	f := newSimFleet(t, cfg, 3)
	f.adaptiveReplicas(1, DefaultBackendMaxConcurrentMetadataScans)
	for i := 0; i < 6; i++ {
		f.add(&simReq{replica: 0, at: f.now, rng: 24 * time.Hour, work: 2.0, cost: 0.5 * gib, queueWait: 5 * time.Minute, repeat: 99})
	}
	start := f.now
	histogram := map[int]time.Duration{}
	l := f.replicas[0].l
	f.tick = func() {
		if f.now.Sub(start) > 5*time.Minute {
			histogram[int(l.limit)] += simStep
		}
	}
	f.run(2 * time.Hour)
	o := f.outcome()
	f.logf("six closed-loop users", o)
	var total, inBand time.Duration
	for limit, d := range histogram {
		total += d
		if limit >= 2 && limit <= 3 {
			inBand += d
		}
	}
	t.Logf("time at each limit after the first 5 minutes: %v (%d increases, %d decreases)", histogram, l.increases, l.decreases)
	if total == 0 || float64(inBand) < 0.9*float64(total) {
		t.Fatalf("limit outside 2-3 for %s of %s", total-inBand, total)
	}
	if o.failed > 0 || o.refused > 0 || o.served != 600 {
		t.Fatalf("served %d failed %d refused %d", o.served, o.failed, o.refused)
	}
}

// Other clients' short selects (dashboards, alerting) keep a few select slots
// busy at all times: label scans still get through.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanSim_ShortSelectsOfOthersDoNotStarveScans(t *testing.T) {
	skipSimulationUnderRace(t)
	f := newSimFleet(t, e2eVL(), 11)
	f.adaptiveReplicas(3, DefaultBackendMaxConcurrentMetadataScans)
	// Short selects of other clients all the time: 40 per second, 20 ms each
	// alone, a quarter of the node.
	f.noise = func(now time.Time, rng *rand.Rand) *simCall {
		if rng.Float64() < 40*simStep.Seconds() {
			return &simCall{work: 0.006, total: 0.006, cost: 16 << 20}
		}
		return nil
	}
	for i := 0; i < 30; i++ {
		f.add(&simReq{replica: i % 3, at: f.now.Add(time.Duration(i) * 6 * time.Second), rng: 24 * time.Hour,
			work: 2.0, cost: 0.5 * gib, queueWait: DefaultBackendHeavyQueryQueueWait})
	}
	f.run(20 * time.Minute)
	o := f.outcome()
	f.logf("with dashboard traffic", o)
	if o.served != 30 {
		t.Fatalf("served %d of 30 label scans beside short selects of other clients", o.served)
	}
}

// steadyOthers is another client's long work that never stops: an I/O-bound
// query of about 2.5 s using a tenth of the node, every 1.5 s (alerting rules,
// a wall of dashboards), so VictoriaLogs always runs a select for someone else.
func steadyOthers(cost float64) func(time.Time, *rand.Rand) *simCall {
	var next time.Time
	return func(now time.Time, _ *rand.Rand) *simCall {
		if now.Before(next) {
			return nil
		}
		next = now.Add(1500 * time.Millisecond)
		return &simCall{work: 0.25, total: 0.25, cost: cost, rateCap: 0.1}
	}
}

// On a VictoriaLogs that always runs another client's long work, a fresh
// fleet's first scans of every listing cannot wait for it to be idle: after
// waiting, at randomised times, they go ahead as more than half of the room
// left in the budget, one at a time. On e2e-sized data 14-16 of 16 requests
// are served; on heavier data the brake stops escapes that would pass its
// mark and those requests get the 429. The backend never exceeds its
// memory, from the e2e data to 100x of it.
//
// conformance: limits/metadata-scan-adaptive-limit, limits/concurrent-full-retention-scans-exhaust-backend
func TestMetadataScanSim_BusyBackendStillServesUnmeasuredListings(t *testing.T) {
	skipSimulationUnderRace(t)
	for _, tc := range []struct {
		name            string
		available, idle float64
		work, cost      float64
		minServed       int
	}{
		{"e2e data on 8 GiB", 8 * gib, 2.3 * gib, 1, 1, 14},
		// One 7-day scan holds 47% of the memory: the brake stops the escapes
		// that meet, and their requests get the 429.
		{"10x data on the same 8 GiB node", 8 * gib, 1.5 * gib, 3, 3.75, 8},
		{"100x data on a 256 GiB node", 256 * gib, 60 * gib, 10, 75, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 3; seed++ {
				cfg := e2eVL()
				cfg.available, cfg.idle = tc.available, tc.idle
				cfg.timeout, cfg.maxQueue = 10*time.Minute, 10*time.Minute
				f := newSimFleet(t, cfg, seed)
				f.noise = steadyOthers(0.02 * tc.available)
				f.adaptiveReplicas(4, DefaultBackendMaxConcurrentMetadataScans)
				presets := scaled(e2ePresets, tc.work, tc.cost)
				// Four users per replica, one after another, each over one of
				// the presets, spaced and waiting in proportion to the scans'
				// duration so the backend has the capacity for all of them.
				pace := time.Duration(tc.work * float64(20*time.Second))
				for i := 0; i < 16; i++ {
					p := presets[i%len(presets)]
					f.add(&simReq{replica: i % 4, at: f.now.Add(time.Duration(i/4) * pace), rng: p.rng, work: p.work, cost: p.cost, queueWait: 3 * pace})
				}
				f.run(3 * time.Hour)
				o := f.outcome()
				f.logf(fmt.Sprintf("%s beside steady work, seed %d", tc.name, seed), o)
				if f.vl.ooms > 0 || f.vl.peak > cfg.available {
					t.Fatalf("seed %d: VictoriaLogs peaked at %.2f of %.1f GiB with %d OOM kills", seed, f.vl.peak/gib, cfg.available/gib, f.vl.ooms)
				}
				if o.served < tc.minServed || o.failed > 0 {
					t.Fatalf("seed %d: served %d of 16 (refused %d, failed %d): a busy backend starved unmeasured listings", seed, o.served, o.refused, o.failed)
				}
			}
		})
	}
}

// Behind a vmauth, /metrics answers without VictoriaLogs' select series and
// memory: the latency feedback alone holds a replica near what the backend
// serves within the tolerance.
//
// conformance: limits/metadata-scan-adaptive-limit
func TestMetadataScanSim_LatencyFeedbackWithoutSignals(t *testing.T) {
	skipSimulationUnderRace(t)
	cfg := e2eVL()
	cfg.available = 64 * gib
	f := newSimFleet(t, cfg, 5)
	f.addReplica(newMetadataScanLimiter(DefaultBackendMaxConcurrentMetadataScans, DefaultBackendMinConcurrentMetadataScans, DefaultBackendHeavyQueryQueueWait,
		DefaultBackendMetadataScanMemoryHeadroom, DefaultBackendMetadataScanLatencyTolerance, nil), true)
	for i := 0; i < 8; i++ {
		f.add(&simReq{replica: 0, at: f.now, rng: 24 * time.Hour, work: 2.0, cost: 0.5 * gib, queueWait: 5 * time.Minute, repeat: 49})
	}
	f.run(2 * time.Hour)
	o := f.outcome()
	l := f.replicas[0].l
	f.logf("eight closed-loop users, no signals", o)
	if f.vl.peakScans > 4 || l.decreases == 0 || int(l.limit) > 3 {
		t.Fatalf("latency feedback did not bound concurrency: most at once %.0f, limit %d, %d decreases", f.vl.peakScans, int(l.limit), l.decreases)
	}
	if o.served != 400 || o.failed > 0 {
		t.Fatalf("served %d failed %d refused %d", o.served, o.failed, o.refused)
	}
}

// skipSimulationUnderRace skips the fleet simulations in race-detector runs.
// They are single-threaded and start no goroutines, so the detector has
// nothing to check, and they cost about a minute of the race job's budget.
func skipSimulationUnderRace(t *testing.T) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("single-threaded simulation; nothing for the race detector to check")
	}
}
