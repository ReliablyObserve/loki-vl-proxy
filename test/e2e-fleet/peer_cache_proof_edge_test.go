//go:build e2e

package e2e_fleet

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// Edge cases of the peer cache proof, run after the five phases on the
// rebuilt fleet. Each one asserts correctness and only the bounds that are
// part of the contract (the owner-down bound, the peer protocol size bounds);
// the rest is recorded in the report.

// loadResult is one request sent while the fleet changes underneath.
type loadResult struct {
	rep     replica
	req     proofRequest
	latency time.Duration
	err     string
	correct bool
}

func recordLoad(r loadResult, repeat int) {
	rec := requestRecordFrom(r.rep, r.req, repeat, r.latency, fleetCounters{})
	rec.Error = r.err
	if r.err == "" && !r.correct {
		rec.Error = "answer differs from the first answer"
	}
	proofRec.addRequest(rec)
}

// readCheck sends req to rep without failing the test (for goroutines).
func readCheck(rep replica, req proofRequest, want string) loadResult {
	started := time.Now()
	body, err := fleetGetErr(rep.addr, req.path)
	r := loadResult{rep: rep, req: req, latency: time.Since(started)}
	if err != nil {
		r.err = err.Error()
		return r
	}
	got, err := canonicalJSONErr(req.String(), body)
	if err != nil {
		r.err = err.Error()
		return r
	}
	r.correct = got == want
	return r
}

func latencyMsOf(results []loadResult, name string) latencyStats {
	var values []float64
	for _, r := range results {
		if r.rep.name == name && r.err == "" {
			values = append(values, ms(r.latency))
		}
	}
	return latencyOf(values)
}

func runEdgeCases(t *testing.T, fx fleetFixture, base proofSet, first answers) {
	a, b, c := fleetReplica(replicaA), fleetReplica(replicaB), fleetReplica(replicaC)

	// The peer protocol's size bounds: a write-through body above 8 MiB is
	// refused (maxPeerSetBodyBytes, internal/cache/peer.go), one below is
	// stored and read back whole.
	t.Run("edge_large_entry_peer_bounds", func(t *testing.T) {
		phase := startPhase("edge_large_entry_peer_bounds", fleetReplicas())
		defer phase.stop()
		key := "fleet-edge-large-" + fx.probe
		tooLarge := bytes.Repeat([]byte("x"), 9<<20)
		started := time.Now()
		status, err := peerSet(b.addr, key+"-9mib", tooLarge)
		if err != nil {
			t.Fatal(err)
		}
		rejectMs := ms(time.Since(started))
		if status != http.StatusBadRequest {
			t.Errorf("a 9 MiB peer write was answered %d, want 400 (bodies above 8 MiB are refused)", status)
		}
		if st, _, err := peerGet(b.addr, key+"-9mib"); err != nil || st != http.StatusNotFound {
			t.Errorf("the refused 9 MiB entry is readable: status %d err %v", st, err)
		}
		fits := bytes.Repeat([]byte("y"), 7<<20)
		started = time.Now()
		status, err = peerSet(b.addr, key, fits)
		if err != nil {
			t.Fatal(err)
		}
		storeMs := ms(time.Since(started))
		if status != http.StatusNoContent {
			t.Fatalf("a 7 MiB peer write was answered %d, want 204", status)
		}
		started = time.Now()
		st, size, err := peerGet(b.addr, key)
		readMs := ms(time.Since(started))
		if err != nil || st != http.StatusOK || size != len(fits) {
			t.Errorf("the 7 MiB entry read back as status %d, %d bytes, err %v", st, size, err)
		}
		proofRec.addEdge("large_entry_peer_bounds", map[string]any{
			"replica": b.name, "reject_9mib_status": http.StatusBadRequest, "reject_9mib_ms": rejectMs,
			"store_7mib_ms": storeMs, "read_7mib_ms": readMs, "read_7mib_bytes": size,
		})
	})

	// A proxy whose disk tier is a 32 KiB tmpfs: disk writes fail, answers
	// must not.
	t.Run("edge_disk_full", func(t *testing.T) {
		full := replica{name: "proxy-diskfull", addr: fleetDiskFullURL}
		phase := startPhase("edge_disk_full", append(fleetReplicas(), full))
		defer phase.stop()
		started := time.Now()
		fleetCompose(t, "--profile", "edge", "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120", full.name)
		defer fleetCompose(t, "--profile", "edge", "rm", "--stop", "--force", "--volumes", full.name)
		waitProxyReady(t, full.addr)
		readyMs := ms(time.Since(started))
		// A set no replica holds yet, so the proxy computes it and writes
		// its disk tier (a peer hit is kept in memory only); A's answers are
		// the reference.
		set := freshProofSet(fx, "diskfull")
		onFull := answers{}
		warm(t, full, set.all(), onFull)
		for _, req := range set.all() {
			askSame(t, a, req, onFull[req.path])
		}
		// Disk writes are flushed every second; look for the failure.
		observed := false
		deadline := time.Now().Add(10 * time.Second)
		for !observed && time.Now().Before(deadline) {
			logs := fleetComposeOutput(t, "--profile", "edge", "logs", "--no-color", full.name)
			observed = strings.Contains(logs, "disk cache flush failed") || strings.Contains(logs, "no space left")
			if !observed {
				time.Sleep(500 * time.Millisecond)
			}
		}
		readAgain(t, []replica{full}, set.all(), onFull)
		waitProxyReady(t, full.addr)
		proofRec.addEdge("disk_full", map[string]any{"replica": full.name, "ready_ms": readyMs, "write_failure_logged": observed, "tmpfs_bytes": 32 << 10})
		if !observed {
			t.Logf("no disk write failure was logged on %s: the 32 KiB tier did not fill with this fixture", full.name)
		}
	})

	// A proxy whose disk cache path cannot be created (read-only root, no
	// volume) fails fast, saying why, or serves correct answers.
	t.Run("edge_unwritable_disk", func(t *testing.T) {
		ro := replica{name: "proxy-readonly", addr: fleetReadOnlyURL}
		phase := startPhase("edge_unwritable_disk", fleetReplicas())
		defer phase.stop()
		container := envOr("FLEET_NAME_PREFIX", "fleet") + "-" + ro.name
		started := time.Now()
		fleetCompose(t, "--profile", "edge", "up", "-d", "--no-deps", "--no-build", ro.name)
		defer fleetCompose(t, "--profile", "edge", "rm", "--stop", "--force", "--volumes", ro.name)
		status, code := "", 0
		deadline := time.Now().Add(30 * time.Second)
		for {
			status, code = containerState(t, container)
			if status == "exited" || readyErr(ro.addr) == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		outcomeMs := ms(time.Since(started))
		logs := fleetComposeOutput(t, "--profile", "edge", "logs", "--no-color", ro.name)
		fields := map[string]any{"replica": ro.name, "status": status, "exit_code": code, "outcome_ms": outcomeMs}
		switch status {
		case "exited":
			if code == 0 {
				t.Errorf("%s exited 0 with an unwritable disk cache path", ro.name)
			}
			if !strings.Contains(logs, "disk cache") {
				t.Errorf("%s exited without naming the disk cache:\n%s", ro.name, logs)
			}
		default:
			if err := readyErr(ro.addr); err != nil {
				t.Fatalf("%s neither exited nor became ready within 30s: %v\n%s", ro.name, err, logs)
			}
			for _, req := range base.metadata[:2] {
				askSame(t, ro, req, first[req.path])
			}
		}
		proofRec.addEdge("unwritable_disk", fields)
	})

	// A slow peer: A is paused, so connections to it hang until the peer
	// timeout. B and C must answer A's entries unchanged, each request
	// slower by at most the peer timeout.
	t.Run("edge_owner_paused_timeout_bound", func(t *testing.T) {
		phase := startPhase("edge_owner_paused_timeout_bound", fleetReplicas())
		defer phase.stop()
		set := freshProofSet(fx, "paused")
		onA := answers{}
		_, cold := warm(t, a, set.all(), onA)
		fleetCompose(t, "pause", a.name)
		markDown(a.name, true)
		defer func() {
			fleetCompose(t, "unpause", a.name)
			waitProxyReady(t, a.addr)
			markDown(a.name, false)
		}()
		// After the rebuild every inventory unit has its replicas again.
		total := requireOwnerDownBound(t, set.all(), onA, cold, true)
		if total.peerErrors < 1 {
			t.Errorf("no lookup on B or C waited for the paused A (%s)", total)
		}
		proofRec.addEdge("owner_paused_timeout_bound", map[string]any{"peer_errors": total.peerErrors, "vl_calls": total.vlSelect})
	})

	// The owner is killed while B is answering requests that need it.
	t.Run("edge_owner_killed_mid_request", func(t *testing.T) {
		phase := startPhase("edge_owner_killed_mid_request", fleetReplicas())
		defer phase.stop()
		set := freshProofSet(fx, "killed")
		onA := answers{}
		_, cold := warm(t, a, set.all(), onA)
		firstSent := make(chan struct{})
		var results []loadResult
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i, req := range set.all() {
				if i == 0 {
					close(firstSent)
				}
				results = append(results, readCheck(b, req, onA[req.path]))
			}
		}()
		<-firstSent
		fleetCompose(t, "kill", a.name)
		<-done
		fleetCompose(t, "start", a.name)
		waitProxyReady(t, a.addr)
		worst := time.Duration(0)
		for _, r := range results {
			recordLoad(r, 0)
			if r.err != "" || !r.correct {
				t.Errorf("%s on B while A was killed: err %q, correct %v", r.req, r.err, r.correct)
				continue
			}
			added := r.latency - cold[r.req.path]
			if added > worst {
				worst = added
			}
			if bound := cold[r.req.path] + peerTimeout + ownerDownSlack; r.latency > bound {
				t.Errorf("contract: %s on B took %s while A was killed, more than A's cold read (%s) plus the peer timeout (%s) and %s slack", r.req, r.latency, cold[r.req.path], peerTimeout, ownerDownSlack)
			}
		}
		proofRec.addEdge("owner_killed_mid_request", map[string]any{"requests": len(results), "worst_added_ms": ms(worst)})
	})

	// Clients keep reading every layer from all three replicas while B is
	// recreated without its disk: A and C never fail or change an answer, B
	// answers correctly whenever it answers.
	t.Run("edge_restart_under_load", func(t *testing.T) {
		phase := startPhase("edge_restart_under_load", fleetReplicas())
		defer phase.stop()
		const workers = 6
		reqs := base.all()
		reps := fleetReplicas()
		stop := make(chan struct{})
		var mu sync.Mutex
		var results []loadResult
		perWorker := make([]int, workers)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := w; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					req := reqs[i%len(reqs)]
					r := readCheck(reps[i%len(reps)], req, first[req.path])
					mu.Lock()
					results = append(results, r)
					perWorker[w]++
					mu.Unlock()
					if r.err != "" {
						// B is down: back off instead of spinning on refused
						// connections, which would slow the restart timed here.
						select {
						case <-stop:
							return
						case <-time.After(100 * time.Millisecond):
						}
					}
				}
			}(w)
		}
		started := time.Now()
		recreateWithoutDisk(t, replicaB)
		readyMs := ms(time.Since(started))
		// Keep the load on until every worker sent 2*len(reqs) more requests
		// with B back (bounded).
		mu.Lock()
		target := make([]int, workers)
		for w := range perWorker {
			target[w] = perWorker[w] + 2*len(reqs)
		}
		mu.Unlock()
		deadline := time.Now().Add(2 * time.Minute)
		for {
			mu.Lock()
			reached := true
			for w := range perWorker {
				reached = reached && perWorker[w] >= target[w]
			}
			mu.Unlock()
			if reached || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		close(stop)
		wg.Wait()
		// Folded into the edge record: the load is many repeats of cached
		// answers, not requests the summary should weigh.
		bErrors := 0
		for _, r := range results {
			switch {
			case r.err == "" && !r.correct:
				t.Errorf("%s on %s under load answered differently from the first answer", r.req, r.rep.name)
			case r.err != "" && r.rep.name != b.name:
				t.Errorf("%s on %s failed while B restarted: %s", r.req, r.rep.name, r.err)
			case r.err != "":
				bErrors++
			}
		}
		proofRec.addEdge("restart_under_load", map[string]any{
			"workers": workers, "requests": len(results), "b_errors_while_down": bErrors, "b_ready_ms": readyMs,
			"latency_a": latencyMsOf(results, a.name), "latency_b": latencyMsOf(results, b.name), "latency_c": latencyMsOf(results, c.name),
			"errors_a": errorsOf(results, a.name), "errors_c": errorsOf(results, c.name),
		})
	})

	// Every replica loses its caches at once and all three are asked every
	// request at the same time.
	t.Run("edge_cold_fleet_all_missing", func(t *testing.T) {
		phase := startPhase("edge_cold_fleet_all_missing", fleetReplicas())
		defer phase.stop()
		started := time.Now()
		recreateWithoutDisk(t, replicaA, replicaB, replicaC)
		readyMs := ms(time.Since(started))
		reqs := base.all()
		var mu sync.Mutex
		var results []loadResult
		var wg sync.WaitGroup
		burst := time.Now()
		for _, rep := range fleetReplicas() {
			wg.Add(1)
			go func(rep replica) {
				defer wg.Done()
				for _, req := range reqs {
					r := readCheck(rep, req, first[req.path])
					mu.Lock()
					results = append(results, r)
					mu.Unlock()
				}
			}(rep)
		}
		wg.Wait()
		burstMs := ms(time.Since(burst))
		var vl fleetCounters
		for _, rep := range fleetReplicas() {
			vl = vl.plus(waitSettled(t, rep.addr))
		}
		for _, r := range results {
			recordLoad(r, 0)
			if r.err != "" || !r.correct {
				t.Errorf("%s on %s on a cold fleet: err %q, correct %v", r.req, r.rep.name, r.err, r.correct)
			}
		}
		proofRec.addEdge("cold_fleet_all_missing", map[string]any{
			"ready_ms": readyMs, "burst_ms": burstMs, "requests": len(results),
			"fleet_vl_calls": vl.vlSelect, "fleet_vl_upstream_ms": vl.vlSeconds * 1000, "fleet_peer_hits": vl.peerHits,
			"note": fmt.Sprintf("one cold replica alone made the calls recorded for %s in phase all_lost_rebuild_equals_loki", a.name),
		})
	})
}

func errorsOf(results []loadResult, name string) int {
	n := 0
	for _, r := range results {
		if r.rep.name == name && r.err != "" {
			n++
		}
	}
	return n
}

// readyErr reports whether a proxy answers /ready with 200.
func readyErr(addr string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(addr + "/ready")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ready: %d", resp.StatusCode)
	}
	return nil
}
