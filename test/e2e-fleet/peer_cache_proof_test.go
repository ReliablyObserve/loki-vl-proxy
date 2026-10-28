//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Peer cache proof: every cache layer that reaches the peer tier is warmed
// on one replica and read on the others, then survives a replica losing its
// memory (disk kept), a replica losing its disk, its owner going down and the
// whole fleet losing every cache, with answers equal to the first ones and,
// after the rebuild, to Loki's. Edge cases follow (peer_cache_proof_edge_test.go).
// Every request is timed and counted, every phase sampled for CPU, memory and
// cache sizes (peer_cache_proof_report_test.go).
//
// The ring owner of a key is not chosen by the test. The assertions hold
// whichever replica owns each key:
//
//   - Write-through layers (stats extent buckets, patterns) push every entry
//     to its owner, so after A computed it, B and C answer with no
//     VictoriaLogs call for that layer, and at least one of them reads it
//     from a peer (a key owned by A is read by both, one owned by B is read
//     by C, one owned by C by B).
//   - Read-through layers (metadata answers, query_range windows) are not
//     pushed: a peer finds an entry only on its owner, which holds it only
//     when it computed it. Read on A, then B, then C, at least one of B and
//     C reads every key not owned by C from a peer; with 12 metadata probes
//     and 16 windows the chance that C owns them all is below 1e-5.
//
// The compatibility edge cache in front of every endpoint is per replica:
// a replica that already answered a request answers it again from there, so
// every assertion about tiers reads a request on a replica for the first
// time (a restarted or recreated replica, a fresh request, or the same
// buckets through a shifted window).
//
// Layers that never reach the peer tier (that edge cache, the query_range
// response cache behind it, the translation and stream field name caches,
// the detected-level probes) are per-replica memory by design; the registry
// rows wired here say which layer uses which tier.

const (
	replicaA = 0
	replicaB = 1
	replicaC = 2
	// repeatReads is how many times each request is read again on a replica
	// that has answered it, for the repeat latency.
	repeatReads = 2
)

// peerTimeout is the proxies' -peer-timeout (default 2s): an unreachable
// owner may add at most this, plus scheduling slack, to a request.
var peerTimeout = func() time.Duration {
	if d, err := time.ParseDuration(envOr("FLEET_PEER_TIMEOUT", "2s")); err == nil {
		return d
	}
	return 2 * time.Second
}()

const ownerDownSlack = time.Second

type proofRequest struct {
	layer string // stats_extent, query_range_window, metadata, patterns
	name  string
	path  string
}

func (r proofRequest) String() string { return r.layer + "/" + r.name }

func fleetRange(start, end time.Time) url.Values {
	return url.Values{
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
}

// metricRequest is a tumbling count (range == step) over [from, fx.end):
// VictoriaLogs answers it from stats_query_range, whose buckets the extent
// cache keeps (about four entries over 8h at a 60s step).
func metricRequest(fx fleetFixture, by string, from time.Time, name string) proofRequest {
	v := fleetRange(from, fx.end)
	v.Set("query", fmt.Sprintf(`sum by (%s) (count_over_time({fleet_probe=%q}[1m]))`, by, fx.probe))
	v.Set("step", "60")
	return proofRequest{layer: "stats_extent", name: name, path: "/loki/api/v1/query_range?" + v.Encode()}
}

// logRequest is a log query over the 8h fixture: eight 1h windows.
func logRequest(fx fleetFixture, selector, name string) proofRequest {
	v := fleetRange(fx.start, fx.end)
	v.Set("query", selector)
	v.Set("limit", "5000")
	v.Set("direction", "backward")
	return proofRequest{layer: "query_range_window", name: name, path: "/loki/api/v1/query_range?" + v.Encode()}
}

// metadataRequests are six metadata answers for one selector.
func metadataRequests(fx fleetFixture, name, selector string) []proofRequest {
	v := fleetRange(fx.start, fx.end)
	v.Set("query", selector)
	q := v.Encode()
	return []proofRequest{
		{layer: "metadata", name: name + "/labels", path: "/loki/api/v1/labels?" + q},
		{layer: "metadata", name: name + "/app_values", path: "/loki/api/v1/label/app/values?" + q},
		{layer: "metadata", name: name + "/env_values", path: "/loki/api/v1/label/env/values?" + q},
		{layer: "metadata", name: name + "/detected_labels", path: "/loki/api/v1/detected_labels?" + q},
		{layer: "metadata", name: name + "/detected_fields", path: "/loki/api/v1/detected_fields?" + q},
		{layer: "metadata", name: name + "/method_values", path: "/loki/api/v1/detected_field/method/values?" + q},
	}
}

func patternsRequest(fx fleetFixture, name, selector string) proofRequest {
	v := fleetRange(fx.start, fx.end)
	v.Set("query", selector)
	v.Set("step", "60s")
	return proofRequest{layer: "patterns", name: name, path: "/loki/api/v1/patterns?" + v.Encode()}
}

// proofSet is one request per layer shape; each set uses its own selectors,
// so none of its keys is cached anywhere before it is first read.
type proofSet struct {
	metric   []proofRequest
	windows  []proofRequest
	metadata []proofRequest
	patterns []proofRequest
}

func (s proofSet) all() []proofRequest {
	out := append([]proofRequest{}, s.metric...)
	out = append(out, s.windows...)
	out = append(out, s.metadata...)
	return append(out, s.patterns...)
}

// selectorSet builds a proof set from a metric grouping and two selectors.
func selectorSet(fx fleetFixture, tag, by string, names [2]string, selectors [2]string) proofSet {
	return proofSet{
		metric:   []proofRequest{metricRequest(fx, by, fx.start, tag+"_count_by_"+by)},
		windows:  []proofRequest{logRequest(fx, selectors[0], tag+"_"+names[0]), logRequest(fx, selectors[1], tag+"_"+names[1])},
		metadata: append(metadataRequests(fx, tag+"_"+names[0], selectors[0]), metadataRequests(fx, tag+"_"+names[1], selectors[1])...),
		patterns: []proofRequest{patternsRequest(fx, tag+"_"+names[0], selectors[0]), patternsRequest(fx, tag+"_"+names[1], selectors[1])},
	}
}

func baseProofSet(fx fleetFixture) proofSet {
	return selectorSet(fx, "base", "app", [2]string{"all", "web"}, [2]string{
		fmt.Sprintf(`{fleet_probe=%q}`, fx.probe),
		fmt.Sprintf(`{fleet_probe=%q, app="web"}`, fx.probe),
	})
}

func freshProofSet(fx fleetFixture, tag string) proofSet {
	switch tag {
	case "paused":
		return selectorSet(fx, tag, "env, app", [2]string{"web_prod", "api_staging"}, [2]string{
			fmt.Sprintf(`{fleet_probe=%q, app="web", env="prod"}`, fx.probe),
			fmt.Sprintf(`{fleet_probe=%q, app="api", env="staging"}`, fx.probe),
		})
	case "killed":
		return selectorSet(fx, tag, "app, env", [2]string{"web_staging", "api_prod"}, [2]string{
			fmt.Sprintf(`{fleet_probe=%q, app="web", env="staging"}`, fx.probe),
			fmt.Sprintf(`{fleet_probe=%q, app="api", env="prod"}`, fx.probe),
		})
	case "diskfull":
		return selectorSet(fx, tag, "fleet_probe", [2]string{"staging", "any_app"}, [2]string{
			fmt.Sprintf(`{fleet_probe=%q, env="staging"}`, fx.probe),
			fmt.Sprintf(`{fleet_probe=%q, app=~"web|api"}`, fx.probe),
		})
	default: // "down"
		return selectorSet(fx, tag, "env", [2]string{"prod", "api"}, [2]string{
			fmt.Sprintf(`{fleet_probe=%q, env="prod"}`, fx.probe),
			fmt.Sprintf(`{fleet_probe=%q, app="api"}`, fx.probe),
		})
	}
}

// answers holds the first answer of every request, canonical JSON.
type answers map[string]string

// ask sends req to a replica and returns the answer, the replica's counter
// deltas for that one request (after its background work settled) and the
// request's latency. Every call is recorded for the report.
func ask(t *testing.T, rep replica, req proofRequest) (string, fleetCounters, time.Duration) {
	t.Helper()
	counted := countedReplicas(rep)
	before := scrapeAll(t, counted)
	started := time.Now()
	body := fleetGet(t, rep.addr, req.path)
	latency := time.Since(started)
	after := settleFleet(t, counted)
	d := after[rep.name].minus(before[rep.name])
	d.fleetVLRoutes = map[string]float64{}
	d.slnRefreshes = 0
	// Calls made by background work (warm-ups, inventory refreshes, sweeps)
	// are not the request's: they are left out where the build counts them.
	for _, peer := range counted {
		f := after[peer.name].minus(before[peer.name])
		d.fleetVLSelect += f.vlSelect - f.vlBackground
		d.fleetVLStats += f.vlStats - f.vlStatsBackground
		d.fleetVLSeconds += f.vlSeconds
		d.slnRefreshes += f.slnRefreshes
		for route, n := range f.vlRoutes {
			d.fleetVLRoutes[route] += n
		}
	}
	proofRec.addRequest(requestRecordFrom(rep, req, 0, latency, d))
	return canonicalJSON(t, fmt.Sprintf("%s on %s", req, rep.name), body), d, latency
}

// askSame sends req to a replica and fails unless the answer equals want.
func askSame(t *testing.T, rep replica, req proofRequest, want string) (fleetCounters, time.Duration) {
	t.Helper()
	got, d, latency := ask(t, rep, req)
	if got != want {
		t.Fatalf("%s on %s answered differently from the first answer:\n got: %.600s\nwant: %.600s", req, rep.name, got, want)
	}
	return d, latency
}

// readAgain reads every request repeatReads more times on each replica, for
// the repeat latency; the answer must not change.
func readAgain(t *testing.T, reps []replica, reqs []proofRequest, want answers) {
	t.Helper()
	for _, rep := range reps {
		for _, req := range reqs {
			for i := 1; i <= repeatReads; i++ {
				started := time.Now()
				body := fleetGet(t, rep.addr, req.path)
				latency := time.Since(started)
				if got := canonicalJSON(t, req.String(), body); got != want[req.path] {
					t.Fatalf("%s on %s changed on repeat %d:\n got: %.600s\nwant: %.600s", req, rep.name, i, got, want[req.path])
				}
				proofRec.addRequest(requestRecordFrom(rep, req, i, latency, fleetCounters{}))
			}
		}
	}
}

// warm computes every request of reqs on rep and records the answers and
// their (cold) latencies.
func warm(t *testing.T, rep replica, reqs []proofRequest, into answers) (fleetCounters, map[string]time.Duration) {
	t.Helper()
	var total fleetCounters
	cold := map[string]time.Duration{}
	for _, req := range reqs {
		got, d, latency := ask(t, rep, req)
		into[req.path] = got
		cold[req.path] = latency
		total = total.plus(d)
	}
	return total, cold
}

// requireReadsWithoutVL reads every write-through request on B and C and
// requires that no lookup missed every tier and no VictoriaLogs call for the
// layer was made anywhere in the fleet: a hit in the reader's own memory or
// disk (it holds a pushed replica copy) counts like a peer hit. With one
// copy per key (factor 1) at least one of B and C must read from a peer (a
// key owned by A is read by both, by B by C, by C by B). With a backup
// replica a key whose replicas are B and C is a local hit on both, so there
// a peer read is not required.
// route selects the counter: fleetVLStats for the extent cache, fleetVLSelect for all.
func requireReadsWithoutVL(t *testing.T, reqs []proofRequest, want answers, route func(fleetCounters) float64, routeName string) {
	t.Helper()
	peerReads := 0.0
	for _, req := range reqs {
		for _, rep := range []replica{fleetReplica(replicaB), fleetReplica(replicaC)} {
			d, _ := askSame(t, rep, req, want[req.path])
			if route(d) != 0 {
				t.Errorf("%s on %s caused %.0f VictoriaLogs %s calls in the fleet; every entry was written on another replica and pushed to its replicas (%s)", req, rep.name, route(d), routeName, d)
			}
			if d.missedAll != 0 {
				t.Errorf("%s on %s: %.0f lookups missed every tier although every entry was pushed to its replicas (%s)", req, rep.name, d.missedAll, d)
			}
			peerReads += d.peerHits
		}
	}
	if peerReads < 1 && replicationFactor < 2 {
		t.Errorf("%s: no replica read an entry from a peer; one written elsewhere must come through the peer tier", reqs[0].layer)
	}
}

func fleetStats(c fleetCounters) float64  { return c.fleetVLStats }
func fleetSelect(c fleetCounters) float64 { return c.fleetVLSelect }

// replicationFactor is read from the proxies when the proof starts.
var replicationFactor = 1

// requireMetadataReadThrough reads each metadata request on the replicas in
// order. A request none of whose shared-read lookups missed every tier was
// answered from memory, disk or a peer and must cost no VictoriaLogs call
// anywhere in the fleet. It returns how many requests were answered at the
// top level from a peer and from the replica's disk (inventory unit lookups,
// which use the same tiers, are subtracted).
func requireMetadataReadThrough(t *testing.T, reqs []proofRequest, want answers, replicas ...int) (fleetCounters, int, int) {
	t.Helper()
	var total fleetCounters
	fromPeer, fromDisk := 0, 0
	for _, req := range reqs {
		for _, idx := range replicas {
			rep := fleetReplica(idx)
			d, _ := askSame(t, rep, req, want[req.path])
			if d.missedAll == 0 {
				if d.fleetVLSelect != 0 {
					t.Errorf("%s on %s missed no cache tier yet caused %.0f VictoriaLogs calls in the fleet (%s)", req, rep.name, d.fleetVLSelect, d)
				}
				if d.topLevelL3Hits() > 0 {
					fromPeer++
				}
				if d.topLevelL2Hits() > 0 {
					fromDisk++
				}
			}
			total = total.plus(d)
		}
	}
	return total, fromPeer, fromDisk
}

// requireWindowReadThrough reads each log request on the replicas in order;
// every window hit on a replica whose memory is cold came from a peer.
func requireWindowReadThrough(t *testing.T, reqs []proofRequest, want answers, replicas ...int) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		for _, idx := range replicas {
			rep := fleetReplica(idx)
			d, _ := askSame(t, rep, req, want[req.path])
			if d.windowHits > d.peerHits {
				t.Errorf("%s on %s: %.0f window hits but only %.0f peer reads; windows are kept in memory only, so every hit on a cold replica must come from a peer", req, rep.name, d.windowHits, d.peerHits)
			}
			total = total.plus(d)
		}
	}
	return total
}

// requireOwnerDownBound reads every request on B and C while A is down and
// requires A's answer, within A's cold latency plus the peer timeout. With a
// backup replica the write-through layers (extent buckets, patterns) and the
// inventory units A wrote are on another replica as well, so they cost no
// VictoriaLogs call.
//
// checkInventory is false right after a replica lost its disk: inventory
// units are scoped to the tenant, not to the request, and that replica
// refills a unit only when it looks it up, so a unit whose other replica is
// the stopped owner may have no live copy left and is rightly read again.
func requireOwnerDownBound(t *testing.T, reqs []proofRequest, onA answers, coldOnA map[string]time.Duration, checkInventory bool) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		for _, idx := range []int{replicaB, replicaC} {
			rep := fleetReplica(idx)
			d, latency := askSame(t, rep, req, onA[req.path])
			total = total.plus(d)
			if replicationFactor >= 2 {
				requireBackupServes(t, rep, req, d, checkInventory)
			}
			if bound := coldOnA[req.path] + peerTimeout + ownerDownSlack; latency > bound {
				t.Errorf("contract: %s on %s took %s with its owner down, more than the cold read on A (%s) plus the peer timeout (%s) and %s slack", req, rep.name, latency, coldOnA[req.path], peerTimeout, ownerDownSlack)
			}
		}
	}
	return total
}

// requireBackupServes: with a backup replica of every key, a write-through
// layer answers without VictoriaLogs after its owner went away, and
// inventory units come from memory, disk or a peer.
func requireBackupServes(t *testing.T, rep replica, req proofRequest, d fleetCounters, checkInventory bool) {
	t.Helper()
	switch req.layer {
	case "stats_extent":
		if d.fleetVLStats != 0 {
			t.Errorf("%s on %s: %.0f stats_query_range calls although every bucket has a backup replica (%s)", req, rep.name, d.fleetVLStats, d)
		}
	case "patterns":
		if d.fleetVLSelect != 0 {
			t.Errorf("%s on %s: %.0f VictoriaLogs calls although the patterns have a backup replica (%s)", req, rep.name, d.fleetVLSelect, d)
		}
	}
	if checkInventory && d.invVL != 0 {
		t.Errorf("%s on %s read %.0f inventory units from VictoriaLogs although every sealed unit has a backup replica (%s)", req, rep.name, d.invVL, d)
	}
}

func patternsCount(t *testing.T, label, answer string) int {
	t.Helper()
	var resp struct {
		Data []any `json:"data"`
	}
	if err := json.Unmarshal([]byte(answer), &resp); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	return len(resp.Data)
}

const (
	patternsSnapshotKey    = "__patterns_snapshot:v1"
	labelValuesSnapshotKey = "__label_values_index_snapshot:v1"
)

// conformance: peer-cache-stats-extent, peer-cache-metadata, peer-cache-query-range-windows, peer-cache-patterns, peer-cache-label-values-index, peer-cache-fleet-recovery, peer-cache-replication
func TestFleetPeerCacheProof(t *testing.T) {
	for _, addr := range proxyAddrs {
		waitProxyReady(t, addr)
	}
	proofRec.report = proofReport{
		Started: time.Now(),
		Images: map[string]string{
			"proxy": envOr("PROXY_IMAGE", "loki-vl-proxy:fleet-local"), "victorialogs": envOr("VICTORIALOGS_IMAGE", "victoriametrics/victoria-logs:v1.52.0"), "loki": envOr("LOKI_IMAGE", "grafana/loki:3.7.9"),
		},
		Bounds: map[string]float64{"peer_timeout_ms": ms(peerTimeout), "owner_down_slack_ms": ms(ownerDownSlack), "rss_refill_slack_bytes": rssRefillSlack},
	}
	t.Cleanup(func() { writeProofReport(t) })

	replicationFactor = fleetReplicationFactor(t)
	proofRec.report.ReplicationFactor = replicationFactor
	t.Logf("peer-cache replication factor reported by the proxies: %d", replicationFactor)
	fx := ingestFleetFixture(t)
	proofRec.report.Fixture = map[string]any{"probe": fx.probe, "lines": fx.lines, "start": fx.start, "end": fx.end}
	base := baseProofSet(fx)
	first := answers{}
	a, b, c := fleetReplica(replicaA), fleetReplica(replicaB), fleetReplica(replicaC)
	t.Logf("fixture %s: %d lines over [%s, %s)", fx.probe, fx.lines, fx.start.Format(time.RFC3339), fx.end.Format(time.RFC3339))

	// Phase 1: an entry computed on A is read by B and C.
	ok := proofRun(t, "cross_peer_read", func(t *testing.T) {
		phase := startPhase("cross_peer_read", fleetReplicas())
		defer phase.stop()
		proofRun(t, "stats_extent", func(t *testing.T) {
			d, _ := warm(t, a, base.metric, first)
			if d.fleetVLStats == 0 {
				t.Fatalf("A answered %s without a stats_query_range call: the extent cache path was not taken (%s)", base.metric[0], d)
			}
			requireReadsWithoutVL(t, base.metric, first, fleetStats, "stats_query_range")
			readAgain(t, fleetReplicas(), base.metric, first)
		})
		proofRun(t, "patterns", func(t *testing.T) {
			warm(t, a, base.patterns, first)
			for _, req := range base.patterns {
				if patternsCount(t, req.String(), first[req.path]) == 0 {
					t.Fatalf("%s: A mined no pattern; an empty answer is never cached, so nothing would be proven", req)
				}
			}
			requireReadsWithoutVL(t, base.patterns, first, fleetSelect, "select")
			readAgain(t, fleetReplicas(), base.patterns, first)
		})
		proofRun(t, "query_range_window", func(t *testing.T) {
			d, _ := warm(t, a, base.windows, first)
			if d.windowMisses == 0 {
				t.Fatalf("A answered the log queries without window fetches: windowing was not used (%s)", d)
			}
			total := requireWindowReadThrough(t, base.windows, first, replicaB, replicaC)
			if total.windowHits < 1 {
				t.Errorf("no window computed on A was read by B or C through the peer tier (%s)", total)
			}
			readAgain(t, fleetReplicas(), base.windows, first)
		})
		proofRun(t, "metadata", func(t *testing.T) {
			warm(t, a, base.metadata, first)
			total, fromPeer, _ := requireMetadataReadThrough(t, base.metadata, first, replicaB, replicaC)
			if fromPeer < 1 {
				t.Errorf("no metadata answer computed elsewhere was read by B or C from its owner (%s)", total)
			}
			readAgain(t, fleetReplicas(), base.metadata, first)
		})
		proofRun(t, "snapshots_published", func(t *testing.T) {
			// The patterns and label-values index snapshots a replica that
			// lost its disk warms from: some peer other than B must hold each.
			waitPeerHasKey(t, patternsSnapshotKey, 20*time.Second, a.addr, c.addr)
			waitPeerHasKey(t, labelValuesSnapshotKey, 20*time.Second, a.addr, c.addr)
		})
	})
	if !ok {
		t.FailNow()
	}

	// Phase 2: B restarts with its disk; its memory is empty.
	proofRun(t, "memory_lost_disk_kept", func(t *testing.T) {
		phase := startPhase("memory_lost_disk_kept", fleetReplicas())
		defer phase.stop()
		started := time.Now()
		restartKeepDisk(t, replicaB)
		proofRec.addEdge("restart_keep_disk", map[string]any{"replica": b.name, "ready_after_ms": ms(time.Since(started))})
		proofRun(t, "stats_extent", func(t *testing.T) {
			// Buckets B owns were pushed to it and written to its disk; the
			// others are on their owners.
			for _, req := range base.metric {
				d, _ := askSame(t, b, req, first[req.path])
				if d.fleetVLStats != 0 || d.missedAll != 0 {
					t.Errorf("%s on B after a restart caused %.0f stats_query_range calls and %.0f lookups that missed every tier; its buckets are on its disk or its peers (%s)", req, d.fleetVLStats, d.missedAll, d)
				}
			}
		})
		proofRun(t, "patterns", func(t *testing.T) {
			for _, req := range base.patterns {
				if d, _ := askSame(t, b, req, first[req.path]); d.fleetVLSelect != 0 {
					t.Errorf("%s on B after a restart caused %.0f VictoriaLogs calls (%s)", req, d.fleetVLSelect, d)
				}
			}
		})
		proofRun(t, "metadata", func(t *testing.T) {
			total, _, fromDisk := requireMetadataReadThrough(t, base.metadata, first, replicaB)
			if fromDisk < 1 {
				t.Errorf("B served no metadata answer from its own disk after a restart (%s)", total)
			}
		})
		proofRun(t, "query_range_window_memory_only", func(t *testing.T) {
			// Windows are kept in memory only (SetLocalOnlyWithTTL): after a
			// restart every window hit on B is a peer read, none a disk read.
			total := requireWindowReadThrough(t, base.windows, first, replicaB)
			if total.tierL2Hits != 0 {
				t.Errorf("B served %.0f lookups of log queries from its disk after a restart; windows are kept in memory only (%s)", total.tierL2Hits, total)
			}
		})
		readAgain(t, []replica{b}, base.all(), first)
	})

	// Phase 3: B loses its disk too and refills from its peers.
	proofRun(t, "disk_lost_refill_from_peers", func(t *testing.T) {
		phase := startPhase("disk_lost_refill_from_peers", fleetReplicas())
		stopped := false
		defer func() {
			if !stopped {
				phase.stop()
			}
		}()
		started := time.Now()
		recreateWithoutDisk(t, replicaB)
		proofRec.addEdge("recreate_without_disk", map[string]any{"replicas": []string{b.name}, "ready_after_ms": ms(time.Since(started))})
		proofRun(t, "snapshots_warmed_from_peers", func(t *testing.T) {
			deadline := time.Now().Add(30 * time.Second)
			for readCounters(t, b.addr).patternsPeer < 1 {
				if time.Now().After(deadline) {
					t.Fatal("B started without a disk and restored no pattern from its peers' snapshot within 30s")
				}
				time.Sleep(250 * time.Millisecond)
			}
			// B persists the label-values index it warmed from its peers and
			// publishes it to the fleet again.
			waitPeerHasKey(t, labelValuesSnapshotKey, 30*time.Second, b.addr)
			proofRec.addEdge("snapshot_warm_from_peers", map[string]any{"replica": b.name, "warm_after_ready_ms": ms(time.Since(started))})
		})
		proofRun(t, "patterns", func(t *testing.T) {
			for _, req := range base.patterns {
				if d, _ := askSame(t, b, req, first[req.path]); d.fleetVLSelect != 0 {
					t.Errorf("%s on B without a disk caused %.0f VictoriaLogs calls; the patterns came back from its peers (%s)", req, d.fleetVLSelect, d)
				}
			}
		})
		proofRun(t, "stats_extent", func(t *testing.T) {
			// Buckets owned by A or C come from them. The ones B owned are
			// lost with its disk: with one copy per key (an owner never asks
			// its peers for its own keys) they are read again from
			// VictoriaLogs once; with a backup replica they come from the
			// backup with no VictoriaLogs call. The same buckets through a
			// shifted window (a new request everywhere, so no replica answers
			// it from its edge cache) then cost no VictoriaLogs call on any
			// replica.
			for _, req := range base.metric {
				d, _ := askSame(t, b, req, first[req.path])
				if replicationFactor >= 2 && d.fleetVLStats != 0 {
					t.Errorf("%s on B without a disk caused %.0f stats_query_range calls although every bucket it owned has a backup replica (%s)", req, d.fleetVLStats, d)
				}
			}
			shifted := metricRequest(fx, "app", fx.start.Add(time.Hour), "base_count_by_app_shifted")
			onA, d, _ := ask(t, a, shifted)
			if d.fleetVLStats != 0 {
				t.Errorf("%s on A caused %.0f stats_query_range calls after B refilled (%s)", shifted, d.fleetVLStats, d)
			}
			for _, rep := range []replica{b, c} {
				if d, _ := askSame(t, rep, shifted, onA); d.fleetVLStats != 0 {
					t.Errorf("%s on %s caused %.0f stats_query_range calls after B refilled (%s)", shifted, rep.name, d.fleetVLStats, d)
				}
			}
		})
		proofRun(t, "metadata", func(t *testing.T) {
			total, fromPeer, _ := requireMetadataReadThrough(t, base.metadata, first, replicaB)
			if fromPeer < 1 {
				t.Errorf("B without a disk read no metadata answer from its peers (%s)", total)
			}
			// Builds with the label stream inventory replicate its sealed
			// units: B gets every unit it lost from a peer.
			if replicationFactor >= 2 && total.invVL != 0 {
				t.Errorf("B without a disk read %.0f inventory units from VictoriaLogs although every sealed unit has a backup replica (%s)", total.invVL, total)
			}
			proofRec.addEdge("disk_lost_inventory_units", map[string]any{"replica": b.name, "memory": total.invMemory, "disk": total.invDisk, "peer": total.invPeer, "vl": total.invVL})
		})
		proofRun(t, "query_range_window", func(t *testing.T) {
			total := requireWindowReadThrough(t, base.windows, first, replicaB)
			if total.windowHits < 1 {
				t.Errorf("B without a disk read no window from its peers (%s)", total)
			}
		})
		readAgain(t, []replica{b}, base.all(), first)
		res := phase.stop()
		stopped = true
		requireRefillRSSBound(t, res)
	})

	// Phase 4: A goes down holding the only copy of the entries it owns; B
	// and C answer from VictoriaLogs, unchanged, within the peer timeout.
	proofRun(t, "owner_down_served_by_others", func(t *testing.T) {
		phase := startPhase("owner_down_served_by_others", fleetReplicas())
		defer phase.stop()
		fresh := freshProofSet(fx, "down")
		onA := answers{}
		_, cold := warm(t, a, fresh.all(), onA)
		fleetCompose(t, "stop", a.name)
		markDown(a.name, true)
		defer func() {
			fleetCompose(t, "start", a.name)
			waitProxyReady(t, a.addr)
			markDown(a.name, false)
		}()
		total := requireOwnerDownBound(t, fresh.all(), onA, cold, false)
		proofRec.addEdge("owner_down_inventory_units", map[string]any{"memory": total.invMemory, "disk": total.invDisk, "peer": total.invPeer, "vl": total.invVL, "backup_hits": total.backupHits})
		if total.peerErrors < 1 {
			t.Errorf("no lookup on B or C reached for the stopped A; the requests did not exercise its keys (%s)", total)
		}
	})

	// Phase 5: every replica loses memory and disk; the fleet rebuilds from
	// VictoriaLogs and answers like Loki.
	rebuilt := answers{}
	proofRun(t, "all_lost_rebuild_equals_loki", func(t *testing.T) {
		phase := startPhase("all_lost_rebuild_equals_loki", fleetReplicas())
		defer phase.stop()
		started := time.Now()
		recreateWithoutDisk(t, replicaA, replicaB, replicaC)
		proofRec.addEdge("recreate_without_disk", map[string]any{"replicas": fleetServices, "ready_after_ms": ms(time.Since(started))})
		warm(t, a, base.all(), rebuilt)
		for _, req := range base.all() {
			if rebuilt[req.path] != first[req.path] {
				t.Errorf("%s after the rebuild differs from the first answer:\n got: %.600s\nwant: %.600s", req, rebuilt[req.path], first[req.path])
			}
		}
		requireReadsWithoutVL(t, base.metric, rebuilt, fleetStats, "stats_query_range")
		requireReadsWithoutVL(t, base.patterns, rebuilt, fleetSelect, "select")
		compareWithLoki(t, fx, base, rebuilt)
	})

	runEdgeCases(t, fx, base, first)
}

// rssRefillSlack is what Go's heap may grow beyond the refilled cache bytes.
const rssRefillSlack = 64 << 20

// requireRefillRSSBound: a replica that lost its disk and refilled must not
// hold more resident memory than before the loss plus what it refilled.
func requireRefillRSSBound(t *testing.T, refill phaseResources) {
	t.Helper()
	before, ok := proofRec.phaseResources("cross_peer_read")
	if !ok {
		return
	}
	pre, post := before.Replicas[fleetServices[replicaB]], refill.Replicas[fleetServices[replicaB]]
	if pre == nil || post == nil || pre.Samples == 0 || post.Samples == 0 {
		t.Logf("RSS bound not checked: no resource samples of B")
		return
	}
	grown := post.MemCacheBytes - pre.MemCacheBytes
	if grown < 0 {
		grown = 0
	}
	if bound := pre.RSSMedianBytes + grown + rssRefillSlack; post.RSSMedianBytes > bound {
		t.Errorf("contract: B's median RSS after refilling (%s) exceeds its median before the disk loss (%s) plus the refilled cache bytes (%s) and %s slack",
			describeBytes(post.RSSMedianBytes), describeBytes(pre.RSSMedianBytes), describeBytes(grown), describeBytes(rssRefillSlack))
	}
}

// compareWithLoki checks the rebuilt answers against Loki for the shapes both
// answer the same way: the metric, the log lines and the label values.
// Patterns (Loki mines them in its pattern ingester) and detected fields are
// compared with the first answers only.
func compareWithLoki(t *testing.T, fx fleetFixture, base proofSet, rebuilt answers) {
	t.Helper()
	waitLokiFixture(t, fx)
	for _, req := range base.metric {
		loki := matrixPoints(t, "Loki "+req.String(), fleetGet(t, fleetLokiURL, req.path), "app")
		proxy := matrixPoints(t, "proxy "+req.String(), []byte(rebuilt[req.path]), "app")
		points := 0
		for _, s := range loki {
			points += len(s)
		}
		if points == 0 {
			t.Fatalf("%s: Loki answered no sample; the comparison would be empty against empty", req)
		}
		if !reflect.DeepEqual(loki, proxy) {
			t.Errorf("%s: rebuilt answer differs from Loki (%d Loki points)\nLoki:  %v\nproxy: %v", req, points, loki, proxy)
		}
	}
	for _, req := range base.windows {
		loki := streamLines(t, "Loki "+req.String(), fleetGet(t, fleetLokiURL, req.path))
		proxy := streamLines(t, "proxy "+req.String(), []byte(rebuilt[req.path]))
		if len(loki) == 0 {
			t.Fatalf("%s: Loki answered no line", req)
		}
		if !reflect.DeepEqual(loki, proxy) {
			t.Errorf("%s: rebuilt lines differ from Loki: Loki %d lines, proxy %d", req, len(loki), len(proxy))
		}
	}
	compared := 0
	for _, req := range base.metadata {
		switch req.name {
		case "base_all/app_values", "base_all/env_values", "base_web/app_values", "base_web/env_values":
			loki := labelValues(t, "Loki "+req.String(), fleetGet(t, fleetLokiURL, req.path))
			proxy := labelValues(t, "proxy "+req.String(), []byte(rebuilt[req.path]))
			if len(loki) == 0 || !reflect.DeepEqual(loki, proxy) {
				t.Errorf("%s: rebuilt values %v, Loki %v", req, proxy, loki)
			}
			compared++
		}
	}
	if compared != 4 {
		t.Errorf("compared %d label-value answers with Loki, want 4", compared)
	}
}
