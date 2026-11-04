//go:build e2e

package e2e_fleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
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
//   - Write-through layers (stats extent buckets, patterns, log query-range
//     windows) push every admitted entry
//     to its owner, so after A computed it, B and C answer with no
//     VictoriaLogs call for that layer, and at least one of them reads it
//     from a peer (a key owned by A is read by both, one owned by B is read
//     by C, one owned by C by B).
//   - Metadata answers also use the shared tiered path. Read on A, then B,
//     then C, so a cold frontend can use an owner's retained answer.
//
// Completed query_range answers of up to 128 KiB are also kept whole as
// exact replies in the shared tiers (memory, disk, peer owner), in front of
// the stats extent buckets and the settled steps. A replica that never
// answered a request reads another replica's reply for it before any of
// those layers is consulted, so each layer under the replies is proven with
// a moved window: the base request over a later start, a key no replica has
// answered, which only the buckets or steps already kept can serve.
//
// The compatibility edge cache in front of every endpoint is per replica:
// a replica that already answered a request answers it again from there, so
// every assertion about tiers reads a request on a replica for the first
// time (a restarted or recreated replica, a fresh request, or the same
// buckets through a shifted window).
//
// Layers that never reach the peer tier (the exact-response edge cache, the
// query_range response cache behind it, translation and stream field caches,
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
	layer string // stats_extent, query_range_window, metadata, patterns, volume_segments
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

// stepsRequests are two window-evaluated metrics over [from, fx.end) at a
// 60s step with a 3-7 minute range, so every evaluation time has a window of
// several buckets: a count folded without a label (VictoriaLogs folds the
// streams bucket by bucket) and a sum of the status field read from the
// lines' JSON (sums are the same whatever level split Loki and the proxy give
// a stream). Every step is older than -query-range-freshness, so every step
// settles and is kept (quality/query-range-settled-steps). The range differs
// per tag, so no tag shares a key with another.
func stepsRequests(fx fleetFixture, from time.Time, tag string, minutes int) []proofRequest {
	build := func(name, query string) proofRequest {
		v := fleetRange(from, fx.end)
		v.Set("query", query)
		v.Set("step", "60")
		return proofRequest{layer: "query_steps", name: tag + "_" + name, path: "/loki/api/v1/query_range?" + v.Encode()}
	}
	return []proofRequest{
		build("count_without_env", fmt.Sprintf(`sum without (env) (count_over_time({fleet_probe=%q}[%dm]))`, fx.probe, minutes)),
		build("unwrap_sum_by_app", fmt.Sprintf(`sum by (app) (sum_over_time({fleet_probe=%q} | json status | unwrap status [%dm]))`, fx.probe, minutes)),
	}
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

// volumeRequest is Logs Drilldown's landing volume (an instant /index/volume
// by service_name, which the proxy derives from app) over [from, fx.end):
// the proxy sums it from the hour segments it keeps in the read cache
// (quality/index-volume-settled-segments).
func volumeRequest(fx fleetFixture, selector string, from time.Time, name string) proofRequest {
	v := fleetRange(from, fx.end)
	v.Set("query", selector)
	v.Set("targetLabels", "service_name")
	v.Set("limit", "5000")
	return proofRequest{layer: "volume_segments", name: name, path: "/loki/api/v1/index/volume?" + v.Encode()}
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
	// steps are window-evaluated metrics (a range longer than the step):
	// the steps they settle are kept per evaluation time (query_steps_cache).
	steps []proofRequest
	// volume are instant volumes summed from kept segments (volume_segments).
	volume []proofRequest
}

func (s proofSet) all() []proofRequest {
	out := append([]proofRequest{}, s.metric...)
	out = append(out, s.steps...)
	out = append(out, s.windows...)
	out = append(out, s.metadata...)
	out = append(out, s.volume...)
	return append(out, s.patterns...)
}

// selectorSet builds a proof set from a metric grouping and two selectors.
func selectorSet(fx fleetFixture, tag, by string, names [2]string, selectors [2]string) proofSet {
	return proofSet{
		metric:   []proofRequest{metricRequest(fx, by, fx.start, tag+"_count_by_"+by)},
		windows:  []proofRequest{logRequest(fx, selectors[0], tag+"_"+names[0]), logRequest(fx, selectors[1], tag+"_"+names[1])},
		metadata: append(metadataRequests(fx, tag+"_"+names[0], selectors[0]), metadataRequests(fx, tag+"_"+names[1], selectors[1])...),
		patterns: []proofRequest{patternsRequest(fx, tag+"_"+names[0], selectors[0]), patternsRequest(fx, tag+"_"+names[1], selectors[1])},
		steps:    stepsRequests(fx, fx.start, tag, stepsMinutes[tag]),
		volume:   []proofRequest{volumeRequest(fx, selectors[0], fx.start, tag+"_"+names[0]), volumeRequest(fx, selectors[1], fx.start, tag+"_"+names[1])},
	}
}

// stepsMinutes is the range of each set's window-evaluated metrics.
var stepsMinutes = map[string]int{"base": 5, "paused": 4, "killed": 6, "diskfull": 3, "down": 7}

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

// requireRepliesRead reads requests another replica answered on replicas that
// never answered them: the exact reply cache answers each one whole (a pushed
// copy, the replica's disk or a peer) before the extent buckets or the
// settled steps are consulted. It requires one reply hit, no
// stats_query_range call in the fleet and no settled-steps lookup.
func requireRepliesRead(t *testing.T, reqs []proofRequest, want answers, where string, replicas ...int) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		for _, idx := range replicas {
			rep := fleetReplica(idx)
			d, _ := askSame(t, rep, req, want[req.path])
			if d.replyHits < 1 {
				t.Errorf("%s on %s %s: no exact reply answered it although another replica kept its answer (%s)", req, rep.name, where, d)
			}
			if d.fleetVLStats != 0 {
				t.Errorf("%s on %s %s caused %.0f stats_query_range calls in the fleet; the reply was kept (%s)", req, rep.name, where, d.fleetVLStats, d)
			}
			if steps := d.stepsHit + d.stepsPartial + d.stepsMiss; steps != 0 {
				t.Errorf("%s on %s %s: %.0f settled-steps lookups behind a kept reply (%s)", req, rep.name, where, steps, d)
			}
			total = total.plus(d)
		}
	}
	return total
}

// movedMetric and movedSteps are the base metric and window-evaluated
// requests over [fx.start+by, fx.end): the same extent buckets and settled
// steps as the base requests under a key no replica has answered, so no exact
// reply is kept for them anywhere. Every caller uses its own offset.
func movedMetric(fx fleetFixture, by time.Duration) []proofRequest {
	return []proofRequest{metricRequest(fx, "app", fx.start.Add(by), "base_count_by_app_moved_"+by.String())}
}

// movedVolume is the base volume over [fx.start+by, fx.end): by is whole
// hours, so every segment of it is one a whole-window answer kept, under a
// key no replica has answered.
func movedVolume(fx fleetFixture, by time.Duration) proofRequest {
	return volumeRequest(fx, fmt.Sprintf(`{fleet_probe=%q}`, fx.probe), fx.start.Add(by), "base_all_moved_"+by.String())
}

// volumeRead counts a replica's segment reads of one request that went to
// VictoriaLogs: some or every kept segment missing, or the whole window.
func volumeRead(d fleetCounters) float64 { return d.volPartial + d.volScanned + d.volWhole }

// requireVolumeLikeVL requires that a volume answer holds, per service, the
// line bytes VictoriaLogs itself sums over the request's window
// (sum_len(_msg) by app; the fixture's service_name is its app).
func requireVolumeLikeVL(t *testing.T, label string, req proofRequest, answer string) {
	t.Helper()
	params, err := url.ParseQuery(req.path[strings.Index(req.path, "?")+1:])
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	vl := url.Values{
		"query": {params.Get("query") + " | stats by (app) sum_len(_msg) as b"},
		"start": {params.Get("start")},
		"end":   {params.Get("end")},
	}
	var truth, got struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(fleetGet(t, fleetVLURL, "/select/logsql/stats_query?"+vl.Encode()), &truth); err != nil {
		t.Fatalf("%s: VictoriaLogs stats: %v", label, err)
	}
	if err := json.Unmarshal([]byte(answer), &got); err != nil {
		t.Fatalf("%s: %v: %.400s", label, err, answer)
	}
	want, have := map[string]string{}, map[string]string{}
	for _, r := range truth.Data.Result {
		want[r.Metric["app"]] = fmt.Sprint(r.Value[1])
	}
	for _, r := range got.Data.Result {
		have[r.Metric["service_name"]] = fmt.Sprint(r.Value[1])
	}
	if len(want) == 0 {
		t.Fatalf("%s: VictoriaLogs holds no line in the window; nothing would be compared", label)
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("%s: volume per service %v, VictoriaLogs' line bytes per app %v", label, have, want)
	}
}

// requireMovedVolume reads a moved volume no replica has answered: its
// answer must equal VictoriaLogs' own sums, it is never a reply hit, and
// unless mayEvaluate every segment comes from a cache tier (the request's
// volume_segments outcome is cached: memory, disk or a peer held each one).
func requireMovedVolume(t *testing.T, rep replica, req proofRequest, where string, mayEvaluate bool) fleetCounters {
	t.Helper()
	body, d, _ := ask(t, rep, req)
	label := fmt.Sprintf("%s on %s %s", req, rep.name, where)
	requireVolumeLikeVL(t, label, req, body)
	if d.replyHits != 0 {
		t.Errorf("%s: %.0f reply hits for a window no replica answered (%s)", label, d.replyHits, d)
	}
	if !mayEvaluate && (d.volCached != 1 || volumeRead(d) != 0) {
		t.Errorf("%s: volume_segments outcomes cached %.0f, partial %.0f, scanned %.0f, whole window %.0f; every segment of the window was kept (%s)", label, d.volCached, d.volPartial, d.volScanned, d.volWhole, d)
	}
	return d
}

func movedSteps(fx fleetFixture, by time.Duration) []proofRequest {
	return stepsRequests(fx, fx.start.Add(by), "base_moved_"+by.String(), stepsMinutes["base"])
}

// requireMovedRead reads moved requests (movedMetric or movedSteps, in the
// order of whole) on rep. No exact reply exists for them, so the layer under
// the replies answers: no reply hit, and the points of the whole window's
// first answer from the moved start on (every point is evaluated over its own
// range, whatever the request's start). Unless mayEvaluate, the layer must
// hold everything: no stats_query_range call in the fleet, every settled step
// found for a window-evaluated metric and, for a bucketed one, no lookup
// that missed every tier but the one of its own reply. It returns the sum of
// the deltas.
func requireMovedRead(t *testing.T, rep replica, reqs, whole []proofRequest, want answers, from time.Time, where string, mayEvaluate bool) fleetCounters {
	t.Helper()
	var total fleetCounters
	for i, req := range reqs {
		body, d, _ := ask(t, rep, req)
		total = total.plus(d)
		requireWindowSlice(t, fmt.Sprintf("%s on %s %s", req, rep.name, where), body, want[whole[i].path], from)
		if d.replyHits != 0 {
			t.Errorf("%s on %s %s: %.0f reply hits for a window no replica answered (%s)", req, rep.name, where, d.replyHits, d)
		}
		if mayEvaluate {
			continue
		}
		if d.fleetVLStats != 0 {
			t.Errorf("%s on %s %s caused %.0f stats_query_range calls in the fleet; every %s entry was kept (%s)", req, rep.name, where, d.fleetVLStats, req.layer, d)
		}
		switch req.layer {
		case "query_steps":
			if d.stepsHit < 1 {
				t.Errorf("%s on %s %s: the settled steps did not answer the whole window (%s)", req, rep.name, where, d)
			}
		case "stats_extent":
			// The one lookup of the window's own exact reply misses every
			// tier (no replica answered it); every bucket lookup must hit.
			if d.missedAll > 1 {
				t.Errorf("%s on %s %s: %.0f lookups missed every tier although every bucket was kept (%s)", req, rep.name, where, d.missedAll, d)
			}
		}
	}
	return total
}

// requireWindowSlice requires that a matrix answer over [from, end) holds
// exactly the points the whole window's answer holds at or after from.
func requireWindowSlice(t *testing.T, label, got, whole string, from time.Time) {
	t.Helper()
	want := map[string]map[string]string{}
	for series, points := range matrixByLabels(t, label+" (whole window)", []byte(whole)) {
		for ts, v := range points {
			if sec, err := strconv.ParseFloat(ts, 64); err == nil && sec >= float64(from.Unix()) {
				if want[series] == nil {
					want[series] = map[string]string{}
				}
				want[series][ts] = v
			}
		}
	}
	have := matrixByLabels(t, label, []byte(got))
	if len(want) == 0 {
		t.Fatalf("%s: the whole window holds no point from %s on; nothing would be compared", label, from.Format(time.RFC3339))
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("%s: the moved window differs from the whole window's points from %s on\n got: %.600v\nwant: %.600v", label, from.Format(time.RFC3339), have, want)
	}
}

// matrixByLabels returns a matrix answer's points, timestamp to value, per
// series keyed by its whole label set.
func matrixByLabels(t *testing.T, label string, body []byte) map[string]map[string]string {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "matrix" {
		t.Fatalf("%s: not a successful matrix: %v: %.400s", label, err, body)
	}
	out := map[string]map[string]string{}
	for _, series := range resp.Data.Result {
		key, _ := json.Marshal(series.Metric)
		points := map[string]string{}
		for _, pair := range series.Values {
			if len(pair) != 2 {
				t.Fatalf("%s: malformed sample %v", label, pair)
			}
			points[fmt.Sprint(pair[0])] = fmt.Sprint(pair[1])
		}
		out[string(key)] = points
	}
	return out
}

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

// requireWindowReadThrough reads each log request on replicas that never
// computed its windows. Windows are written through to their owner, so a
// reused window is a copy pushed to this replica (a memory hit), a disk read
// or a peer read; a window hit with none of these was computed here.
func requireWindowReadThrough(t *testing.T, reqs []proofRequest, want answers, replicas ...int) fleetCounters {
	t.Helper()
	var total fleetCounters
	for _, req := range reqs {
		for _, idx := range replicas {
			rep := fleetReplica(idx)
			d, _ := askSame(t, rep, req, want[req.path])
			if d.windowHits > d.tierL1Hits+d.tierL2Hits+d.tierL3Hits {
				t.Errorf("%s on %s: %.0f window hits but only %.0f pushed-copy, %.0f L2 and %.0f L3 reads on a replica that never computed them (%s)", req, rep.name, d.windowHits, d.tierL1Hits, d.tierL2Hits, d.tierL3Hits, d)
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
	case "query_steps":
		if d.fleetVLStats != 0 {
			t.Errorf("%s on %s: %.0f stats_query_range calls although every settled step has a backup replica (%s)", req, rep.name, d.fleetVLStats, d)
		}
	case "patterns":
		if d.fleetVLSelect != 0 {
			t.Errorf("%s on %s: %.0f VictoriaLogs calls although the patterns have a backup replica (%s)", req, rep.name, d.fleetVLSelect, d)
		}
	case "volume_segments":
		if volumeRead(d) != 0 {
			t.Errorf("%s on %s read volume segments from VictoriaLogs although every segment has a backup replica (%s)", req, rep.name, d)
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

// conformance: quality/index-volume-settled-segments, quality/query-range-settled-steps, peer-cache-stats-extent, peer-cache-query-replies, peer-cache-metadata, peer-cache-query-range-windows, peer-cache-patterns, peer-cache-label-values-index, peer-cache-fleet-recovery, peer-cache-replication
func TestFleetPeerCacheProof(t *testing.T) {
	for _, addr := range proxyAddrs {
		waitProxyReady(t, addr)
	}
	proofRec.report = proofReport{
		Started: time.Now(),
		Images: map[string]string{
			"proxy": envOr("PROXY_IMAGE", "loki-vl-proxy:fleet-local"), "victorialogs": envOr("VICTORIALOGS_IMAGE", "victoriametrics/victoria-logs:v1.53.0"), "loki": envOr("LOKI_IMAGE", "grafana/loki:3.7.9"),
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
			// B and C read A's exact reply; then each reads its own moved
			// window, which only the buckets A kept can answer.
			requireReadsWithoutVL(t, base.metric, first, fleetStats, "stats_query_range")
			peerReads := 0.0
			for idx, by := range map[int]time.Duration{replicaB: 10 * time.Minute, replicaC: 20 * time.Minute} {
				d := requireMovedRead(t, fleetReplica(idx), movedMetric(fx, by), base.metric, first, fx.start.Add(by), "after A kept the buckets", false)
				peerReads += d.peerHits
			}
			if peerReads < 1 && replicationFactor < 2 {
				t.Errorf("no replica read a bucket kept on A from a peer through a moved window")
			}
			readAgain(t, fleetReplicas(), base.metric, first)
		})
		proofRun(t, "query_steps", func(t *testing.T) {
			d, _ := warm(t, a, base.steps, first)
			if d.fleetVLStats == 0 || d.stepsStored < float64(len(base.steps)) {
				t.Fatalf("A answered %s with %.0f stats_query_range calls and kept %.0f of %d answers' steps: the settled steps path was not taken (%s)", base.steps[0], d.fleetVLStats, d.stepsStored, len(base.steps), d)
			}
			for _, req := range base.steps {
				if points := matrixSumBy(t, req.String(), []byte(first[req.path]), "app"); len(points) == 0 {
					t.Fatalf("%s: A answered no series; an empty answer is never kept, so nothing would be proven", req)
				}
			}
			// B and C read A's exact replies. Then each reads its own moved
			// window: it finds every step on the replica that owns it (a
			// pushed copy) and evaluates nothing.
			requireRepliesRead(t, base.steps, first, "after A kept the replies", replicaB, replicaC)
			var total fleetCounters
			for idx, by := range map[int]time.Duration{replicaB: 10 * time.Minute, replicaC: 20 * time.Minute} {
				d := requireMovedRead(t, fleetReplica(idx), movedSteps(fx, by), base.steps, first, fx.start.Add(by), "after A kept the steps", false)
				total = total.plus(d)
			}
			if total.peerHits < 1 && replicationFactor < 2 {
				t.Errorf("no replica read a settled step from a peer (%s)", total)
			}
			readAgain(t, fleetReplicas(), base.steps, first)
		})
		proofRun(t, "volume_segments", func(t *testing.T) {
			d, _ := warm(t, a, base.volume, first)
			if d.volScanned < 1 {
				t.Fatalf("A answered %s without reading volume segments (scanned %.0f, whole window %.0f): the segment path was not taken (%s)", base.volume[0], d.volScanned, d.volWhole, d)
			}
			for _, req := range base.volume {
				requireVolumeLikeVL(t, req.String()+" on A", req, first[req.path])
			}
			// Each replica first reads its own moved window, which only the
			// segments A kept can answer: it has read nothing of this selector
			// yet, so every segment it does not own itself comes from a peer.
			// (Read after the replica summed A's segments for the base window,
			// the segments are already in its memory and no peer read is left
			// to see: 4 of 10 runs on the CI stack.)
			peerReads := 0.0
			for _, move := range []struct {
				idx int
				by  time.Duration
			}{{replicaB, time.Hour}, {replicaC, 2 * time.Hour}} {
				d := requireMovedVolume(t, fleetReplica(move.idx), movedVolume(fx, move.by), "after A kept the segments", false)
				peerReads += d.peerHits
			}
			if peerReads < 1 && replicationFactor < 2 {
				t.Errorf("no replica read a volume segment kept on A from a peer through a moved window")
			}
			// Then B and C read A's replies, or sum A's segments: nothing is
			// read from VictoriaLogs.
			for _, req := range base.volume {
				for _, rep := range []replica{b, c} {
					if d, _ := askSame(t, rep, req, first[req.path]); volumeRead(d) != 0 {
						t.Errorf("%s on %s read volume segments from VictoriaLogs; A kept its reply and segments (%s)", req, rep.name, d)
					}
				}
			}
			readAgain(t, fleetReplicas(), base.volume, first)
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
			requireMovedRead(t, b, movedMetric(fx, 30*time.Minute), base.metric, first, fx.start.Add(30*time.Minute), "after a restart", false)
		})
		proofRun(t, "query_steps", func(t *testing.T) {
			// B answered these before the restart: its replies, and the steps
			// a moved window reads, are on its disk or its peers.
			requireRepliesRead(t, base.steps, first, "after a restart", replicaB)
			requireMovedRead(t, b, movedSteps(fx, 30*time.Minute), base.steps, first, fx.start.Add(30*time.Minute), "after a restart", false)
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
		proofRun(t, "volume_segments", func(t *testing.T) {
			// The segments B owned are on its disk, the others on their owners.
			requireMovedVolume(t, b, movedVolume(fx, 3*time.Hour), "after a restart", false)
		})
		proofRun(t, "query_range_window_disk_or_peer", func(t *testing.T) {
			// A restart clears L1; settled windows on B's disk or its peers
			// should answer the same log request without a VL scan.
			total := requireWindowReadThrough(t, base.windows, first, replicaB)
			if total.windowHits == 0 || total.tierL2Hits+total.tierL3Hits == 0 {
				t.Errorf("B reused no settled log window from disk or peers after restart (%s)", total)
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
			// The base request's exact reply comes from its owner (or, with
			// one copy per key, is computed again when B owned it). Buckets
			// owned by A or C come from them. The ones B owned are lost with
			// its disk: with one copy per key (an owner never asks its peers
			// for its own keys) B's first moved window reads them again from
			// VictoriaLogs once; with a backup replica they come from the
			// backup with no VictoriaLogs call. Moved windows of their own
			// (no replica answered them, so no reply is kept) then cost no
			// VictoriaLogs call on A or C.
			for _, req := range base.metric {
				d, _ := askSame(t, b, req, first[req.path])
				if replicationFactor >= 2 && d.fleetVLStats != 0 {
					t.Errorf("%s on B without a disk caused %.0f stats_query_range calls although every bucket it owned has a backup replica (%s)", req, d.fleetVLStats, d)
				}
			}
			requireMovedRead(t, b, movedMetric(fx, time.Hour), base.metric, first, fx.start.Add(time.Hour), "without a disk", replicationFactor < 2)
			for idx, by := range map[int]time.Duration{replicaA: 70 * time.Minute, replicaC: 80 * time.Minute} {
				requireMovedRead(t, fleetReplica(idx), movedMetric(fx, by), base.metric, first, fx.start.Add(by), "after B refilled", false)
			}
		})
		proofRun(t, "query_steps", func(t *testing.T) {
			// The base requests' exact replies come from their owners (or,
			// with one copy per key, are computed again when B owned them).
			// Steps owned by A or C come from them. The ones B owned are lost
			// with its disk: with one copy per key B's first moved window
			// evaluates them again from VictoriaLogs once; with a backup
			// replica they come from the backup with no VictoriaLogs call.
			// Moved windows of their own then need no VictoriaLogs call on A
			// or C: every step is held again.
			for _, req := range base.steps {
				d, _ := askSame(t, b, req, first[req.path])
				if replicationFactor >= 2 && d.fleetVLStats != 0 {
					t.Errorf("%s on B without a disk caused %.0f stats_query_range calls although every step it owned has a backup replica (%s)", req, d.fleetVLStats, d)
				}
			}
			requireMovedRead(t, b, movedSteps(fx, time.Hour), base.steps, first, fx.start.Add(time.Hour), "without a disk", replicationFactor < 2)
			for idx, by := range map[int]time.Duration{replicaA: 70 * time.Minute, replicaC: 80 * time.Minute} {
				requireMovedRead(t, fleetReplica(idx), movedSteps(fx, by), base.steps, first, fx.start.Add(by), "after B refilled", false)
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
		proofRun(t, "volume_segments", func(t *testing.T) {
			// Segments owned by A or C come from them. The ones B owned are
			// lost with its disk: with one copy per key B's moved window reads
			// them again from VictoriaLogs once; with a backup replica they
			// come from the backup. Moved windows inside it then cost no
			// VictoriaLogs call on A or C.
			requireMovedVolume(t, b, movedVolume(fx, 4*time.Hour), "without a disk", replicationFactor < 2)
			for idx, by := range map[int]time.Duration{replicaA: 5 * time.Hour, replicaC: 6 * time.Hour} {
				requireMovedVolume(t, fleetReplica(idx), movedVolume(fx, by), "after B refilled", false)
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
		requireRepliesRead(t, base.steps, rebuilt, "after the rebuild", replicaB, replicaC)
		for idx, by := range map[int]time.Duration{replicaB: 90 * time.Minute, replicaC: 100 * time.Minute} {
			requireMovedRead(t, fleetReplica(idx), movedMetric(fx, by), base.metric, rebuilt, fx.start.Add(by), "after the rebuild", false)
			requireMovedRead(t, fleetReplica(idx), movedSteps(fx, by), base.steps, rebuilt, fx.start.Add(by), "after the rebuild", false)
		}
		for idx, by := range map[int]time.Duration{replicaB: time.Hour, replicaC: 2 * time.Hour} {
			requireMovedVolume(t, fleetReplica(idx), movedVolume(fx, by), "after the rebuild", false)
		}
		compareWithLoki(t, fx, base, rebuilt)
	})

	runNearNowPhase(t)

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
	for _, req := range base.steps {
		loki := matrixSumBy(t, "Loki "+req.String(), fleetGet(t, fleetLokiURL, req.path), "app")
		proxy := matrixSumBy(t, "proxy "+req.String(), []byte(rebuilt[req.path]), "app")
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
	// Volume values are line bytes on the proxy and chunk-size estimates on
	// Loki (profiles/volume-bytes-accounting): the series must match.
	for _, req := range base.volume {
		names := func(label string, body []byte) []string {
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			var out []string
			for _, r := range resp.Data.Result {
				out = append(out, fmt.Sprint(r.Metric))
			}
			sort.Strings(out)
			return out
		}
		loki, proxy := names("Loki "+req.String(), fleetGet(t, fleetLokiURL, req.path)), names("proxy "+req.String(), []byte(rebuilt[req.path]))
		if len(loki) == 0 || !reflect.DeepEqual(loki, proxy) {
			t.Errorf("%s: rebuilt volume series %v, Loki %v", req, proxy, loki)
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
