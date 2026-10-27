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
// after the rebuild, to Loki's.
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
// Layers that never reach the peer tier (the compatibility edge cache, the
// query_range response cache behind it, the translation and stream field
// name caches, the detected-level probes) are per-replica memory by design;
// the registry rows wired here say which layer uses which tier.

const (
	replicaA = 0
	replicaB = 1
	replicaC = 2
)

type proofRequest struct {
	layer string // stats_extent, query_range_window, metadata, patterns
	name  string
	path  string
}

func (r proofRequest) String() string { return r.layer + "/" + r.name }

func fleetRange(fx fleetFixture) url.Values {
	return url.Values{
		"start": {strconv.FormatInt(fx.start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(fx.end.UnixNano(), 10)},
	}
}

// metricRequest is a tumbling count (range == step) over the 8h fixture:
// VictoriaLogs answers it from stats_query_range, whose buckets the extent
// cache keeps (about four entries over 8h at a 60s step).
func metricRequest(fx fleetFixture, by string) proofRequest {
	v := fleetRange(fx)
	v.Set("query", fmt.Sprintf(`sum by (%s) (count_over_time({fleet_probe=%q}[1m]))`, by, fx.probe))
	v.Set("step", "60")
	return proofRequest{layer: "stats_extent", name: "count_by_" + by, path: "/loki/api/v1/query_range?" + v.Encode()}
}

// logRequest is a log query over the 8h fixture: eight 1h windows.
func logRequest(fx fleetFixture, label, value string) proofRequest {
	v := fleetRange(fx)
	v.Set("query", fmt.Sprintf(`{fleet_probe=%q, %s=%q}`, fx.probe, label, value))
	v.Set("limit", "5000")
	v.Set("direction", "backward")
	return proofRequest{layer: "query_range_window", name: label + "_" + value, path: "/loki/api/v1/query_range?" + v.Encode()}
}

// metadataRequests are six metadata answers for one selector.
func metadataRequests(fx fleetFixture, name, selector string) []proofRequest {
	v := fleetRange(fx)
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
	v := fleetRange(fx)
	v.Set("query", selector)
	v.Set("step", "60s")
	return proofRequest{layer: "patterns", name: name, path: "/loki/api/v1/patterns?" + v.Encode()}
}

// proofSet is one request per layer shape; fresh sets use other selectors so
// none of their keys is cached anywhere yet.
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

func baseProofSet(fx fleetFixture) proofSet {
	all := fmt.Sprintf(`{fleet_probe=%q}`, fx.probe)
	web := fmt.Sprintf(`{fleet_probe=%q, app="web"}`, fx.probe)
	return proofSet{
		metric:   []proofRequest{metricRequest(fx, "app")},
		windows:  []proofRequest{logRequest(fx, "app", "web"), logRequest(fx, "app", "api")},
		metadata: append(metadataRequests(fx, "all", all), metadataRequests(fx, "web", web)...),
		patterns: []proofRequest{patternsRequest(fx, "all", all), patternsRequest(fx, "web", web)},
	}
}

func freshProofSet(fx fleetFixture) proofSet {
	prod := fmt.Sprintf(`{fleet_probe=%q, env="prod"}`, fx.probe)
	api := fmt.Sprintf(`{fleet_probe=%q, app="api"}`, fx.probe)
	return proofSet{
		metric:   []proofRequest{metricRequest(fx, "env")},
		windows:  []proofRequest{logRequest(fx, "env", "prod"), logRequest(fx, "env", "staging")},
		metadata: append(metadataRequests(fx, "prod", prod), metadataRequests(fx, "api", api)...),
		patterns: []proofRequest{patternsRequest(fx, "prod", prod), patternsRequest(fx, "api", api)},
	}
}

// answers holds the first answer of every request, canonical JSON.
type answers map[string]string

// ask sends req to a replica and returns the answer and the replica's counter
// deltas for that one request.
func ask(t *testing.T, idx int, req proofRequest) (string, fleetCounters) {
	t.Helper()
	before := readCounters(t, proxyAddrs[idx])
	body := fleetGet(t, proxyAddrs[idx], req.path)
	after := waitSettled(t, proxyAddrs[idx])
	return canonicalJSON(t, fmt.Sprintf("%s on %s", req, fleetServices[idx]), body), after.minus(before)
}

// askSame sends req to a replica and fails unless the answer equals want.
func askSame(t *testing.T, idx int, req proofRequest, want string) fleetCounters {
	t.Helper()
	got, d := ask(t, idx, req)
	if got != want {
		t.Fatalf("%s on %s answered differently from the first answer:\n got: %.600s\nwant: %.600s", req, fleetServices[idx], got, want)
	}
	return d
}

// warm computes every request of reqs on idx and records the answers.
func warm(t *testing.T, idx int, reqs []proofRequest, into answers) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		got, d := ask(t, idx, req)
		into[req.path] = got
		total = total.plus(d)
	}
	return total
}

// requireReadsWithoutVL reads every write-through request on B and C and
// requires no VictoriaLogs call for the layer and at least one peer read.
// route selects the counter: vlStats for the extent cache, vlSelect for all.
func requireReadsWithoutVL(t *testing.T, reqs []proofRequest, want answers, route func(fleetCounters) float64, routeName string) {
	t.Helper()
	peerReads := 0.0
	for _, req := range reqs {
		for _, idx := range []int{replicaB, replicaC} {
			d := askSame(t, idx, req, want[req.path])
			if route(d) != 0 {
				t.Errorf("%s on %s made %.0f VictoriaLogs %s calls; every entry was written on another replica and pushed to its owner (%s)", req, fleetServices[idx], route(d), routeName, d)
			}
			peerReads += d.peerHits
		}
	}
	if peerReads < 1 {
		t.Errorf("%s: no replica read an entry from a peer; one written elsewhere must come through the peer tier", reqs[0].layer)
	}
}

// requireMetadataReadThrough reads each metadata request on B then C. A
// shared-tier hit (disk or peer) must cost no VictoriaLogs call; at least
// one request must come from a peer.
func requireMetadataReadThrough(t *testing.T, reqs []proofRequest, want answers, replicas ...int) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		for _, idx := range replicas {
			d := askSame(t, idx, req, want[req.path])
			if (d.tierL3Hits > 0 || d.tierL2Hits > 0) && d.vlSelect != 0 {
				t.Errorf("%s on %s was a cache hit (disk %.0f, peer %.0f) yet made %.0f VictoriaLogs calls", req, fleetServices[idx], d.tierL2Hits, d.tierL3Hits, d.vlSelect)
			}
			t.Logf("%s on %s: %s", req, fleetServices[idx], d)
			total = total.plus(d)
		}
	}
	return total
}

// requireWindowReadThrough reads each log request on the replicas in order;
// every window hit on a replica whose memory is cold came from a peer.
func requireWindowReadThrough(t *testing.T, reqs []proofRequest, want answers, replicas ...int) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		for _, idx := range replicas {
			d := askSame(t, idx, req, want[req.path])
			if d.windowHits > d.peerHits {
				t.Errorf("%s on %s: %.0f window hits but only %.0f peer reads; windows are kept in memory only, so every hit on a cold replica must come from a peer", req, fleetServices[idx], d.windowHits, d.peerHits)
			}
			t.Logf("%s on %s: %s", req, fleetServices[idx], d)
			total = total.plus(d)
		}
	}
	return total
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

// conformance: peer-cache-stats-extent, peer-cache-metadata, peer-cache-query-range-windows, peer-cache-patterns, peer-cache-label-values-index, peer-cache-fleet-recovery
func TestFleetPeerCacheProof(t *testing.T) {
	for _, addr := range proxyAddrs {
		waitProxyReady(t, addr)
	}
	fx := ingestFleetFixture(t)
	base := baseProofSet(fx)
	first := answers{}
	t.Logf("fixture %s: %d lines over [%s, %s)", fx.probe, fx.lines, fx.start.Format(time.RFC3339), fx.end.Format(time.RFC3339))

	// Phase 1: an entry computed on A is read by B and C.
	ok := t.Run("cross_peer_read", func(t *testing.T) {
		t.Run("stats_extent", func(t *testing.T) {
			d := warm(t, replicaA, base.metric, first)
			if d.vlStats == 0 {
				t.Fatalf("A answered %s without a stats_query_range call: the extent cache path was not taken (%s)", base.metric[0], d)
			}
			requireReadsWithoutVL(t, base.metric, first, func(c fleetCounters) float64 { return c.vlStats }, "stats_query_range")
		})
		t.Run("patterns", func(t *testing.T) {
			warm(t, replicaA, base.patterns, first)
			for _, req := range base.patterns {
				if patternsCount(t, req.String(), first[req.path]) == 0 {
					t.Fatalf("%s: A mined no pattern; an empty answer is never cached, so nothing would be proven", req)
				}
			}
			requireReadsWithoutVL(t, base.patterns, first, func(c fleetCounters) float64 { return c.vlSelect }, "select")
		})
		t.Run("query_range_window", func(t *testing.T) {
			d := warm(t, replicaA, base.windows, first)
			if d.windowMisses == 0 {
				t.Fatalf("A answered the log queries without window fetches: windowing was not used (%s)", d)
			}
			total := requireWindowReadThrough(t, base.windows, first, replicaB, replicaC)
			if total.windowHits < 1 {
				t.Errorf("no window computed on A was read by B or C through the peer tier (%s)", total)
			}
		})
		t.Run("metadata", func(t *testing.T) {
			warm(t, replicaA, base.metadata, first)
			total := requireMetadataReadThrough(t, base.metadata, first, replicaB, replicaC)
			if total.tierL3Hits < 1 {
				t.Errorf("no metadata answer computed elsewhere was read by B or C from its owner (%s)", total)
			}
		})
		t.Run("snapshots_published", func(t *testing.T) {
			// The patterns and label-values index snapshots a replica that
			// lost its disk warms from: some peer other than B must hold each.
			waitPeerHasKey(t, "__patterns_snapshot:v1", 20*time.Second, proxyAddrs[replicaA], proxyAddrs[replicaC])
			waitPeerHasKey(t, "__label_values_index_snapshot:v1", 20*time.Second, proxyAddrs[replicaA], proxyAddrs[replicaC])
		})
	})
	if !ok {
		t.FailNow()
	}

	// Phase 2: B restarts with its disk; its memory is empty.
	t.Run("memory_lost_disk_kept", func(t *testing.T) {
		restartKeepDisk(t, replicaB)
		t.Run("stats_extent", func(t *testing.T) {
			// Buckets B owns were pushed to it and written to its disk; the
			// others are on their owners.
			for _, req := range base.metric {
				d := askSame(t, replicaB, req, first[req.path])
				if d.vlStats != 0 {
					t.Errorf("%s on B after a restart made %.0f stats_query_range calls; its buckets are on its disk or its peers (%s)", req, d.vlStats, d)
				}
			}
		})
		t.Run("patterns", func(t *testing.T) {
			for _, req := range base.patterns {
				if d := askSame(t, replicaB, req, first[req.path]); d.vlSelect != 0 {
					t.Errorf("%s on B after a restart made %.0f VictoriaLogs calls (%s)", req, d.vlSelect, d)
				}
			}
		})
		t.Run("metadata", func(t *testing.T) {
			total := requireMetadataReadThrough(t, base.metadata, first, replicaB)
			if total.tierL2Hits < 1 {
				t.Errorf("B served no metadata answer from its own disk after a restart (%s)", total)
			}
		})
		t.Run("query_range_window_memory_only", func(t *testing.T) {
			// Windows are kept in memory only (SetLocalOnlyWithTTL): after a
			// restart every window hit on B is a peer read, none a disk read.
			requireWindowReadThrough(t, base.windows, first, replicaB)
		})
	})

	// Phase 3: B loses its disk too and refills from its peers.
	t.Run("disk_lost_refill_from_peers", func(t *testing.T) {
		recreateWithoutDisk(t, replicaB)
		t.Run("snapshots_warmed_from_peers", func(t *testing.T) {
			deadline := time.Now().Add(30 * time.Second)
			for readCounters(t, proxyAddrs[replicaB]).patternsPeer < 1 {
				if time.Now().After(deadline) {
					t.Fatal("B started without a disk and restored no pattern from its peers' snapshot within 30s")
				}
				time.Sleep(250 * time.Millisecond)
			}
			// B persists the label-values index it warmed from its peers and
			// publishes it to the fleet again.
			waitPeerHasKey(t, "__label_values_index_snapshot:v1", 30*time.Second, proxyAddrs[replicaB])
		})
		t.Run("patterns", func(t *testing.T) {
			for _, req := range base.patterns {
				if d := askSame(t, replicaB, req, first[req.path]); d.vlSelect != 0 {
					t.Errorf("%s on B without a disk made %.0f VictoriaLogs calls; the patterns came back from its peers (%s)", req, d.vlSelect, d)
				}
			}
		})
		t.Run("stats_extent", func(t *testing.T) {
			// Buckets owned by A or C come from them; the ones B owned are
			// lost with its disk (an owner never asks its peers for its own
			// keys) and are read again from VictoriaLogs once, after which the
			// fleet serves them without VictoriaLogs.
			for _, req := range base.metric {
				d := askSame(t, replicaB, req, first[req.path])
				t.Logf("%s on B without a disk: %s", req, d)
				for _, idx := range []int{replicaB, replicaC} {
					if d := askSame(t, idx, req, first[req.path]); d.vlStats != 0 {
						t.Errorf("%s on %s after B refilled made %.0f stats_query_range calls (%s)", req, fleetServices[idx], d.vlStats, d)
					}
				}
			}
		})
		t.Run("metadata", func(t *testing.T) {
			total := requireMetadataReadThrough(t, base.metadata, first, replicaB)
			if total.tierL3Hits < 1 {
				t.Errorf("B without a disk read no metadata answer from its peers (%s)", total)
			}
		})
		t.Run("query_range_window", func(t *testing.T) {
			total := requireWindowReadThrough(t, base.windows, first, replicaB)
			if total.windowHits < 1 {
				t.Errorf("B without a disk read no window from its peers (%s)", total)
			}
		})
	})

	// Phase 4: A goes down holding the only copy of the entries it owns; B
	// and C answer from VictoriaLogs, unchanged.
	t.Run("owner_down_served_by_others", func(t *testing.T) {
		fresh := freshProofSet(fx)
		onA := answers{}
		warm(t, replicaA, fresh.all(), onA)
		fleetCompose(t, "stop", fleetServices[replicaA])
		defer func() {
			fleetCompose(t, "start", fleetServices[replicaA])
			waitProxyReady(t, proxyAddrs[replicaA])
		}()
		var total fleetCounters
		for _, req := range fresh.all() {
			for _, idx := range []int{replicaB, replicaC} {
				total = total.plus(askSame(t, idx, req, onA[req.path]))
			}
		}
		if total.peerErrors < 1 {
			t.Errorf("no lookup on B or C reached for the stopped A; the requests did not exercise its keys (%s)", total)
		}
		t.Logf("owner down, B+C: %s", total)
	})

	// Phase 5: every replica loses memory and disk; the fleet rebuilds from
	// VictoriaLogs and answers like Loki.
	t.Run("all_lost_rebuild_equals_loki", func(t *testing.T) {
		recreateWithoutDisk(t, replicaA, replicaB, replicaC)
		rebuilt := answers{}
		warm(t, replicaA, base.all(), rebuilt)
		for _, req := range base.all() {
			if rebuilt[req.path] != first[req.path] {
				t.Errorf("%s after the rebuild differs from the first answer:\n got: %.600s\nwant: %.600s", req, rebuilt[req.path], first[req.path])
			}
		}
		requireReadsWithoutVL(t, base.metric, rebuilt, func(c fleetCounters) float64 { return c.vlStats }, "stats_query_range")
		requireReadsWithoutVL(t, base.patterns, rebuilt, func(c fleetCounters) float64 { return c.vlSelect }, "select")
		compareWithLoki(t, fx, base, rebuilt)
	})
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
	for _, req := range base.metadata {
		switch {
		case req.name == "all/app_values" || req.name == "all/env_values" || req.name == "web/app_values" || req.name == "web/env_values":
			loki := labelValues(t, "Loki "+req.String(), fleetGet(t, fleetLokiURL, req.path))
			proxy := labelValues(t, "proxy "+req.String(), []byte(rebuilt[req.path]))
			if len(loki) == 0 || !reflect.DeepEqual(loki, proxy) {
				t.Errorf("%s: rebuilt values %v, Loki %v", req, proxy, loki)
			}
		}
	}
}
