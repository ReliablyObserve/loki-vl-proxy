//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Near-now phase of the peer cache proof: label names, label values and
// /series over windows that end now (1h, 6h, 24h), on a second fixture whose
// rows span the last 23 hours and keep arriving during the phase.
//
// On builds with the label stream inventory (its metadata_stream_inventory
// counters move) these answers come from the tenant's inventory: its owner
// on the peer ring holds it, the other replicas forward to the owner, and its
// sealed hour units are kept on the units' peer replicas. The phase then
// requires that B and C are answered by the inventory with no unit read from
// VictoriaLogs, that the owner rebuilt without its disk and the replicas
// reading for a stopped owner load the units from peers (0 unit reads from
// VictoriaLogs), and that a row written hours in the past is listed on every
// replica within one late-data sweep. On every build it requires equal
// answers on all replicas and the near-now edge contract: a near-now request
// waits at most -metadata-edge-refresh-budget for its edge (plus slack) and
// a stream written now is listed within -metadata-max-staleness.

// nearNow bounds, from the proxies' defaults (overridable for a stack that
// runs other values).
var (
	nearMaxStaleness = envDuration("FLEET_METADATA_MAX_STALENESS", 10*time.Second)
	nearEdgeBudget   = envDuration("FLEET_METADATA_EDGE_REFRESH_BUDGET", 100*time.Millisecond)
	nearLateSweep    = envDuration("FLEET_METADATA_LATE_DATA_SWEEP_INTERVAL", 15*time.Second)
)

// nearRequestSlack is what a near-now request may take beyond the edge
// budget: the forward to the scope owner, the rest of the request, a busy
// runner.
const nearRequestSlack = time.Second

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(envOr(key, "")); err == nil {
		return d
	}
	return def
}

type nearFixture struct {
	id      string
	stop    chan struct{}
	done    chan struct{}
	mu      sync.Mutex
	written int
	errs    []string
}

func nearSelector(id string) string { return fmt.Sprintf(`{fleet_near=%q}`, id) }

// nearRow writes one row of stream {fleet_near=id, near_app=app, extra...} at ts.
func nearRow(id, app string, ts time.Time, extra map[string]string) error {
	row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": "near " + app + " " + ts.Format(time.RFC3339Nano), "fleet_near": id, "near_app": app}
	fields := []string{"fleet_near", "near_app"}
	for k, v := range extra {
		row[k] = v
		fields = append(fields, k)
	}
	encoded, _ := json.Marshal(row)
	if err := nearPost(fleetVLURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(fields, ",")), string(encoded)+"\n"); err != nil {
		return err
	}
	return nearPost(fleetVLURL+"/internal/force_flush", "")
}

func nearPost(target, body string) error {
	resp, err := fleetHTTP.Post(target, "application/stream+json", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("POST %s: %d %s", target, resp.StatusCode, b)
	}
	return nil
}

// startNearFixture writes streams a and b, one row every 10 minutes over the
// last 23 hours, and starts the live stream: one row a second until stop.
func startNearFixture(t *testing.T) *nearFixture {
	t.Helper()
	fx := &nearFixture{id: fmt.Sprintf("fn%d", time.Now().UnixNano()), stop: make(chan struct{}), done: make(chan struct{})}
	now := time.Now().UTC()
	var rows strings.Builder
	n := 0
	for _, app := range []string{"a", "b"} {
		for at := now.Add(-23 * time.Hour); at.Before(now.Add(-2 * time.Minute)); at = at.Add(10 * time.Minute) {
			row := map[string]string{"_time": at.Format(time.RFC3339Nano), "_msg": "near " + app, "fleet_near": fx.id, "near_app": app}
			encoded, _ := json.Marshal(row)
			rows.Write(encoded)
			rows.WriteByte('\n')
			n++
		}
	}
	if err := nearPost(fleetVLURL+"/insert/jsonline?_stream_fields=fleet_near,near_app", rows.String()); err != nil {
		t.Fatal(err)
	}
	if err := nearRow(fx.id, "live", time.Now(), nil); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(fx.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-fx.stop:
				return
			case <-ticker.C:
			}
			err := nearRow(fx.id, "live", time.Now(), nil)
			fx.mu.Lock()
			if err != nil {
				fx.errs = append(fx.errs, err.Error())
			} else {
				fx.written++
			}
			fx.mu.Unlock()
		}
	}()
	t.Cleanup(fx.close)
	// VictoriaLogs must hold the backfill before anything reads it.
	deadline := time.Now().Add(time.Minute)
	for {
		params := url.Values{
			"query": {fmt.Sprintf(`fleet_near:=%q near_app:in(a,b) | stats count() n`, fx.id)},
			"start": {strconv.FormatInt(now.Add(-24*time.Hour).UnixNano(), 10)},
			"end":   {strconv.FormatInt(time.Now().UnixNano(), 10)},
		}
		body, err := fleetGetErr(fleetVLURL, "/select/logsql/query?"+params.Encode())
		var row struct {
			N string `json:"n"`
		}
		if err == nil && json.Unmarshal([]byte(strings.TrimSpace(string(body))), &row) == nil && row.N == strconv.Itoa(n) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("near-now fixture not visible in VictoriaLogs after 1m (%d rows pushed)", n)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fx
}

func (fx *nearFixture) close() {
	select {
	case <-fx.stop:
	default:
		close(fx.stop)
	}
	<-fx.done
}

// nearRequests are the metadata requests over windows that end now.
func nearRequests(id string) []proofRequest {
	now := time.Now()
	var out []proofRequest
	for _, w := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour} {
		v := fleetRange(now.Add(-w), now)
		v.Set("query", nearSelector(id))
		q := v.Encode()
		s := fleetRange(now.Add(-w), now)
		s.Set("match[]", nearSelector(id))
		name := strings.TrimSuffix(w.String(), "0m0s")
		out = append(out,
			proofRequest{layer: "near_now", name: name + "/labels", path: "/loki/api/v1/labels?" + q},
			proofRequest{layer: "near_now", name: name + "/near_app_values", path: "/loki/api/v1/label/near_app/values?" + q},
			proofRequest{layer: "near_now", name: name + "/series", path: "/loki/api/v1/series?" + s.Encode()},
		)
	}
	return out
}

// nearAnswer is a comparable form of a label or series answer: sorted
// names or values, or sorted series label sets.
func nearAnswer(label string, body []byte) (string, error) {
	var resp struct {
		Status string            `json:"status"`
		Data   []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" {
		return "", fmt.Errorf("%s: not a successful answer: %v: %.300s", label, err, body)
	}
	items := make([]string, 0, len(resp.Data))
	for _, raw := range resp.Data {
		var series map[string]string
		if json.Unmarshal(raw, &series) == nil {
			keys := make([]string, 0, len(series))
			for k, v := range series {
				keys = append(keys, k+"="+v)
			}
			sort.Strings(keys)
			items = append(items, "{"+strings.Join(keys, ",")+"}")
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("%s: unexpected item %s", label, raw)
		}
		items = append(items, s)
	}
	sort.Strings(items)
	return strings.Join(items, " "), nil
}

// nearAsk sends one near-now request (fresh window ending now) and returns
// its answer, the replica's counter deltas, the fleet's unit reads and the
// latency.
func nearAsk(t *testing.T, rep replica, req proofRequest) (string, fleetCounters, time.Duration) {
	t.Helper()
	counted := countedReplicas(rep)
	before := scrapeAll(t, counted)
	started := time.Now()
	body := fleetGet(t, rep.addr, req.path)
	latency := time.Since(started)
	after := settleFleet(t, counted)
	d := after[rep.name].minus(before[rep.name])
	fleet := fleetCounters{}
	for _, peer := range counted {
		fleet = fleet.plus(after[peer.name].minus(before[peer.name]))
	}
	d.fleetVLSelect = fleet.vlSelect - fleet.vlBackground
	d.fleetVLStats = fleet.vlStats - fleet.vlStatsBackground
	d.fleetVLSeconds = fleet.vlSeconds
	// The fleet's inventory: unit sources and the owner's served_peer.
	d.invVL, d.invPeer, d.invDisk, d.invMemory, d.invServedPeer = fleet.invVL, fleet.invPeer, fleet.invDisk, fleet.invMemory, fleet.invServedPeer
	proofRec.addRequest(requestRecordFrom(rep, req, 0, latency, d))
	answer, err := nearAnswer(req.String(), body)
	if err != nil {
		t.Fatal(err)
	}
	return answer, d, latency
}

// inventoryAnswered: the asking replica answered from its own inventory or
// through the scope's owner.
func inventoryAnswered(d fleetCounters) bool { return d.invHit > 0 || d.invForwarded > 0 }

// nearWarm asks every request on rep until the inventory answers it (a
// scope's first request is answered from VictoriaLogs while the inventory
// reads in the background), and returns the answers.
func nearWarm(t *testing.T, rep replica, reqs func() []proofRequest, inventory bool) answers {
	t.Helper()
	out := answers{}
	deadline := time.Now().Add(90 * time.Second)
	for i := range reqs() {
		for {
			req := reqs()[i]
			got, d, _ := nearAsk(t, rep, req)
			out[req.name] = got
			if !inventory || inventoryAnswered(d) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the stream inventory never answered %s on %s within 90s (%s)", req, rep.name, d)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	return out
}

// inventoryPresent probes whether the build has the label stream inventory:
// its counters move when a near-now listing is asked repeatedly.
func inventoryPresent(t *testing.T, rep replica, id string) bool {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, d, _ := nearAsk(t, rep, nearRequests(id)[0])
		if inventoryAnswered(d) || d.invServedPeer > 0 {
			return true
		}
		m := scrapeMetrics(t, rep.addr)
		if m.sum("loki_vl_proxy_internal_operation_total", map[string]string{"operation": "metadata_stream_inventory"}) > 0 {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// requireNearSame reads every near-now request on reps and requires the
// warm answer; with the inventory, every read must be answered by it and
// read no unit from VictoriaLogs anywhere in the fleet.
func requireNearSame(t *testing.T, reps []replica, id string, want answers, inventory bool) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, rep := range reps {
		for _, req := range nearRequests(id) {
			got, d, latency := nearAsk(t, rep, req)
			total = total.plus(d)
			if got != want[req.name] {
				t.Errorf("%s on %s: %s, want %s", req, rep.name, got, want[req.name])
			}
			if bound := nearEdgeBudget + nearRequestSlack; latency > bound {
				t.Errorf("contract: near-now %s on %s took %s, more than -metadata-edge-refresh-budget (%s) plus %s", req, rep.name, latency, nearEdgeBudget, nearRequestSlack)
			}
			if inventory {
				if !inventoryAnswered(d) {
					t.Errorf("%s on %s was not answered by the stream inventory (%s)", req, rep.name, d)
				}
				if d.invVL != 0 {
					t.Errorf("%s on %s read %.0f inventory units from VictoriaLogs in the fleet after warm-up (%s)", req, rep.name, d.invVL, d)
				}
			}
		}
	}
	return total
}

// scopeOwner finds the replica holding the tenant's inventory: the one the
// others forward to (its served_peer moves), or the one that never forwards.
func scopeOwner(t *testing.T, id string) int {
	t.Helper()
	req := nearRequests(id)[0]
	served := map[int]float64{}
	forwards := map[int]float64{}
	for idx := range fleetServices {
		before := scrapeAll(t, fleetReplicas())
		fleetGet(t, proxyAddrs[idx], req.path)
		after := settleFleet(t, fleetReplicas())
		for j, rep := range fleetReplicas() {
			served[j] += after[rep.name].invServedPeer - before[rep.name].invServedPeer
		}
		forwards[idx] += after[fleetServices[idx]].invForwarded - before[fleetServices[idx]].invForwarded
	}
	best, bestServed := -1, 0.0
	for j, n := range served {
		if n > bestServed {
			best, bestServed = j, n
		}
	}
	if best >= 0 {
		return best
	}
	for idx := range fleetServices {
		if forwards[idx] == 0 {
			return idx
		}
	}
	t.Fatalf("no replica holds the near-now inventory scope (served_peer %v, forwarded %v)", served, forwards)
	return -1
}

// waitListed polls label names over the last 6h on rep until want is listed.
func waitListed(t *testing.T, rep replica, id, label string, deadline time.Time) (time.Duration, bool) {
	t.Helper()
	started := time.Now()
	for {
		now := time.Now()
		v := fleetRange(now.Add(-6*time.Hour), now)
		v.Set("query", nearSelector(id))
		body, err := fleetGetErr(rep.addr, "/loki/api/v1/labels?"+v.Encode())
		if err == nil {
			if got, err := nearAnswer("labels", body); err == nil && slices.Contains(strings.Fields(got), label) {
				return time.Since(started), true
			}
		}
		if time.Now().After(deadline) {
			return time.Since(started), false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitValue polls near_app values over the last hour on rep until value is
// listed.
func waitValue(t *testing.T, rep replica, id, value string, deadline time.Time) (time.Duration, bool) {
	t.Helper()
	started := time.Now()
	for {
		now := time.Now()
		v := fleetRange(now.Add(-time.Hour), now)
		v.Set("query", nearSelector(id))
		body, err := fleetGetErr(rep.addr, "/loki/api/v1/label/near_app/values?"+v.Encode())
		if err == nil {
			if got, err := nearAnswer("values", body); err == nil && slices.Contains(strings.Fields(got), value) {
				return time.Since(started), true
			}
		}
		if time.Now().After(deadline) {
			return time.Since(started), false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func runNearNowPhase(t *testing.T) {
	proofRun(t, "near_now_label_inventory", func(t *testing.T) {
		phase := startPhase("near_now_label_inventory", fleetReplicas())
		defer phase.stop()
		fx := startNearFixture(t)
		a, b, c := fleetReplica(replicaA), fleetReplica(replicaB), fleetReplica(replicaC)
		reqs := func() []proofRequest { return nearRequests(fx.id) }
		inventory := inventoryPresent(t, a, fx.id)
		t.Logf("label stream inventory present: %v", inventory)
		proofRec.addEdge("near_now_inventory_present", map[string]any{"present": inventory})

		var warmed answers
		proofRun(t, "cross_peer_read", func(t *testing.T) {
			warmed = nearWarm(t, a, reqs, inventory)
			total := requireNearSame(t, []replica{b, c}, fx.id, warmed, inventory)
			if inventory && total.invServedPeer < 1 && total.invHit < 1 {
				t.Errorf("B and C were answered neither through the scope owner nor from their own inventory (%s)", total)
			}
			proofRec.addEdge("near_now_cross_peer_read", map[string]any{"forwarded": total.invForwarded, "served_peer": total.invServedPeer, "hits": total.invHit, "units_vl": total.invVL, "fleet_vl_calls": total.fleetVLSelect})
		})
		if warmed == nil {
			t.FailNow()
		}

		proofRun(t, "edge_freshness", func(t *testing.T) {
			// A stream written now is listed on B and C within
			// -metadata-max-staleness.
			written := time.Now()
			if err := nearRow(fx.id, "fresh", written, nil); err != nil {
				t.Fatal(err)
			}
			deadline := written.Add(nearMaxStaleness + 2*time.Second)
			for _, rep := range []replica{b, c} {
				took, ok := waitValue(t, rep, fx.id, "fresh", deadline)
				proofRec.addEdge("near_now_fresh_stream_listed", map[string]any{"replica": rep.name, "after_ms": ms(time.Since(written)), "polled_ms": ms(took), "listed": ok})
				if !ok {
					t.Errorf("contract: a stream written now was not listed on %s within -metadata-max-staleness (%s) plus 2s", rep.name, nearMaxStaleness)
				}
			}
			// The warm answers now include the new value.
			warmed = nearWarm(t, a, reqs, inventory)
		})

		if !inventory {
			t.Log("no label stream inventory on this build: the near-now phase checked equal answers and the edge contract only")
			return
		}
		owner := scopeOwner(t, fx.id)
		ownerRep := fleetReplica(owner)
		var others []replica
		for i := range fleetServices {
			if i != owner {
				others = append(others, fleetReplica(i))
			}
		}
		t.Logf("near-now inventory scope owner: %s", ownerRep.name)
		proofRec.addEdge("near_now_scope_owner", map[string]any{"replica": ownerRep.name})

		proofRun(t, "owner_disk_lost_units_from_peers", func(t *testing.T) {
			// The owner loses memory and disk; it rebuilds the scope from the
			// hour units its peers hold.
			before := scrapeAll(t, others)
			recreateWithoutDisk(t, owner)
			rebuilt := nearWarm(t, ownerRep, reqs, true)
			after := scrapeAll(t, append(others, ownerRep))
			var units fleetCounters
			units = units.plus(after[ownerRep.name])
			for _, rep := range others {
				units = units.plus(after[rep.name].minus(before[rep.name]))
			}
			for _, req := range reqs() {
				if rebuilt[req.name] != warmed[req.name] {
					t.Errorf("%s on %s after losing its disk: %s, want %s", req, ownerRep.name, rebuilt[req.name], warmed[req.name])
				}
			}
			if units.invVL != 0 {
				t.Errorf("%s rebuilt its inventory without its disk and %.0f units were read from VictoriaLogs; every sealed unit has replicas on its peers (%s)", ownerRep.name, units.invVL, units)
			}
			if after[ownerRep.name].invPeer < 1 {
				t.Errorf("%s rebuilt its inventory without its disk and loaded no unit from a peer (%s)", ownerRep.name, after[ownerRep.name])
			}
			proofRec.addEdge("near_now_owner_disk_lost", map[string]any{"replica": ownerRep.name, "units_peer": after[ownerRep.name].invPeer, "units_vl": units.invVL, "units_disk": after[ownerRep.name].invDisk})
		})

		proofRun(t, "owner_down_units_from_backup", func(t *testing.T) {
			// With the owner stopped the others read for themselves and load
			// the units from their replicas.
			fleetCompose(t, "stop", ownerRep.name)
			markDown(ownerRep.name, true)
			defer func() {
				fleetCompose(t, "start", ownerRep.name)
				waitProxyReady(t, ownerRep.addr)
				markDown(ownerRep.name, false)
			}()
			before := scrapeAll(t, others)
			got := map[string]answers{}
			for _, rep := range others {
				got[rep.name] = nearWarm(t, rep, reqs, true)
			}
			after := scrapeAll(t, others)
			var units fleetCounters
			for _, rep := range others {
				units = units.plus(after[rep.name].minus(before[rep.name]))
				for _, req := range reqs() {
					if got[rep.name][req.name] != warmed[req.name] {
						t.Errorf("%s on %s with the owner down: %s, want %s", req, rep.name, got[rep.name][req.name], warmed[req.name])
					}
				}
			}
			if units.invVL != 0 {
				t.Errorf("with the scope owner down %.0f inventory units were read from VictoriaLogs; every sealed unit has a backup replica (%s)", units.invVL, units)
			}
			if units.invForwardFallback < 1 {
				t.Errorf("no replica fell back from the stopped owner (%s)", units)
			}
			proofRec.addEdge("near_now_owner_down", map[string]any{"owner": ownerRep.name, "forward_fallback": units.invForwardFallback, "units_peer": units.invPeer, "units_vl": units.invVL})
		})

		proofRun(t, "late_row_listed_fleet_wide", func(t *testing.T) {
			// A row written three hours in the past, in an hour every replica
			// already read, is listed everywhere within one late-data sweep
			// (plus the sweep's tick, the near-now share window and slack).
			written := time.Now()
			if err := nearRow(fx.id, "late", written.Add(-3*time.Hour), map[string]string{"near_late": "yes"}); err != nil {
				t.Fatal(err)
			}
			deadline := written.Add(nearLateSweep + 15*time.Second)
			for _, rep := range fleetReplicas() {
				took, ok := waitListed(t, rep, fx.id, "near_late", deadline)
				proofRec.addEdge("near_now_late_row_listed", map[string]any{"replica": rep.name, "after_ms": ms(time.Since(written)), "polled_ms": ms(took), "listed": ok})
				if !ok {
					t.Errorf("a row written 3h in the past was not listed on %s within one late-data sweep (%s) plus 15s", rep.name, nearLateSweep)
				}
			}
		})

		fx.mu.Lock()
		proofRec.addEdge("near_now_live_writer", map[string]any{"rows": fx.written, "errors": len(fx.errs)})
		errs := append([]string(nil), fx.errs...)
		fx.mu.Unlock()
		if len(errs) > 0 {
			t.Errorf("the live writer failed %d times, first: %s", len(errs), errs[0])
		}
	})
}
