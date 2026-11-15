//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestFleet_PushesBufferedForAPausedReplicaReachItWhenItAnswers: write-through
// pushes to a replica whose breaker is open are kept and replayed once it
// answers, so the writer can then lose its disk without its keys being read
// from VictoriaLogs again (peer-cache-push-replay).
//
//   - replica b is paused: a's pushes to it time out, b is suspended on a and
//     every later push to b waits in a's push buffer;
//   - replica a answers the "replay" set while b is paused, and a "control"
//     set while every replica is up;
//   - b answers again: a replays every buffered push (none dropped) and its
//     buffer empties;
//   - a loses its memory and disk, and answers both sets again: every answer
//     equals the first one, and the replay set costs the fleet no more
//     VictoriaLogs calls than the control set, whose pushes were never
//     delayed. Without the replay, keys whose replicas are a and b exist only
//     on the lost disk and are read again.
//
// conformance: peer-cache-push-replay, peer-cache-fleet-recovery
func TestFleet_PushesBufferedForAPausedReplicaReachItWhenItAnswers(t *testing.T) {
	requireFleetUp(t)
	if rf := fleetReplicationFactor(t); rf < 2 {
		t.Skipf("replication factor %d: one replica per key, a writer's lost disk is not covered by its peers", rf)
	}
	a, b, c := fleetReplica(0), fleetReplica(1), fleetReplica(2)

	// Fixture: 12 services, one line a minute each, inside VictoriaLogs'
	// retention and older than -query-range-freshness.
	app := fmt.Sprintf("push-replay-%d", time.Now().UnixNano())
	from := time.Now().Add(-9 * time.Hour).Truncate(time.Hour)
	to := from.Add(3 * time.Hour)
	const services = 12
	var rows strings.Builder
	for svc := 0; svc < services; svc++ {
		for m := 0; m < 180; m++ {
			row, _ := json.Marshal(map[string]string{
				"_time": from.Add(time.Duration(m)*time.Minute + time.Duration(svc+1)*time.Second).Format(time.RFC3339Nano),
				"_msg":  "tick",
				"app":   app,
				"svc":   fmt.Sprintf("s%02d", svc),
				"level": []string{"info", "warn", "error"}[m%3],
			})
			rows.Write(row)
			rows.WriteByte('\n')
		}
	}
	resp, err := http.Post(fleetVLURL+"/insert/jsonline?_stream_fields=app,svc", "application/stream+json", strings.NewReader(rows.String()))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("ingest: %d", resp.StatusCode)
	}
	if resp, err := http.Post(fleetVLURL+"/internal/force_flush", "text/plain", nil); err == nil {
		resp.Body.Close()
	}
	request := func(set string, svc int, filter string) proofRequest {
		params := url.Values{
			"query": {fmt.Sprintf(`sum by (level) (count_over_time({app=%q, svc="s%02d"}%s [1m]))`, app, svc, filter)},
			"start": {strconv.FormatInt(from.UnixNano(), 10)},
			"end":   {strconv.FormatInt(to.UnixNano(), 10)},
			"step":  {"60"},
		}
		return proofRequest{layer: "push_replay", name: fmt.Sprintf("%s_s%02d", set, svc), path: "/loki/api/v1/query_range?" + params.Encode()}
	}
	var replaySet, controlSet []proofRequest
	for svc := 0; svc < services; svc++ {
		if svc%2 == 0 {
			replaySet = append(replaySet, request("replay", svc, ""))
		} else {
			controlSet = append(controlSet, request("control", svc, ""))
		}
	}
	// Wait until VictoriaLogs answers every line, on c, under queries no
	// replica keeps (a filter every line passes, spelled once).
	deadline := time.Now().Add(60 * time.Second)
	for svc := 0; svc < services; svc++ {
		for try := 0; ; try++ {
			body := fleetGet(t, c.addr, request("probe", svc, fmt.Sprintf(` != "absent-%d-%d"`, svc, try)).path)
			if strings.Count(string(body), `"level"`) == 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("VictoriaLogs does not hold the lines of %s s%02d: %.300s", app, svc, body)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	type bufferCounters struct{ buffered, coalesced, replayed, entries, full, expired, rejected, closed float64 }
	readBuffer := func() bufferCounters {
		m := scrapeMetrics(t, a.addr)
		dropped := func(reason string) float64 {
			return m.sum("loki_vl_proxy_peer_cache_push_buffer_dropped_total", map[string]string{"reason": reason})
		}
		return bufferCounters{
			buffered:  m.sum("loki_vl_proxy_peer_cache_push_buffered_total", nil),
			coalesced: m.sum("loki_vl_proxy_peer_cache_push_buffer_coalesced_total", nil),
			replayed:  m.sum("loki_vl_proxy_peer_cache_push_replayed_total", nil),
			entries:   m.sum("loki_vl_proxy_peer_cache_push_buffer_entries", nil),
			full:      dropped("full"), expired: dropped("expired"), rejected: dropped("rejected"), closed: dropped("closed"),
		}
	}

	settleFleet(t, fleetReplicas())
	first := map[string]string{}
	for _, req := range controlSet {
		first[req.name] = canonicalJSON(t, req.String(), fleetGet(t, a.addr, req.path))
	}
	settleFleet(t, fleetReplicas())
	before := readBuffer()

	// b stops answering: a's pushes to it are buffered.
	fleetCompose(t, "pause", b.name)
	markDown(b.name, true)
	paused := true
	unpause := func() {
		if paused {
			fleetCompose(t, "unpause", b.name)
			markDown(b.name, false)
			paused = false
		}
	}
	t.Cleanup(unpause)
	for _, req := range replaySet {
		first[req.name] = canonicalJSON(t, req.String(), fleetGet(t, a.addr, req.path))
	}
	// Pushes leave after the answer; the first to b waits -peer-timeout.
	var during bufferCounters
	for wait, last := time.Now().Add(30*time.Second), -1.0; ; time.Sleep(500 * time.Millisecond) {
		during = readBuffer()
		if during.buffered > before.buffered && during.buffered == last {
			break
		}
		if time.Now().After(wait) {
			t.Fatalf("a buffered %v pushes while b was paused (before %v): its pushes to b were not kept", during.buffered-before.buffered, before.buffered)
		}
		last = during.buffered
	}
	t.Logf("while b was paused a buffered %v pushes (%v coalesced), %v waiting", during.buffered-before.buffered, during.coalesced-before.coalesced, during.entries)

	// b answers again: a replays the buffer, slowly first, until it is empty.
	unpause()
	waitProxyReady(t, b.addr)
	var after bufferCounters
	for wait := time.Now().Add(60 * time.Second); ; time.Sleep(250 * time.Millisecond) {
		after = readBuffer()
		if after.entries == 0 && after.replayed > before.replayed {
			break
		}
		if time.Now().After(wait) {
			t.Fatalf("a did not replay its buffer within 60s of b answering: %+v (before %+v)", after, before)
		}
	}
	if lost := (after.full - before.full) + (after.expired - before.expired) + (after.rejected - before.rejected); lost != 0 {
		t.Fatalf("a dropped %v buffered pushes (full %v, expired %v, rejected %v)", lost, after.full-before.full, after.expired-before.expired, after.rejected-before.rejected)
	}
	t.Logf("after b answered a replayed %v pushes", after.replayed-before.replayed)

	// a loses its memory and disk: both sets are answered from the replicas.
	settleFleet(t, fleetReplicas())
	recreateWithoutDisk(t, 0)
	settleFleet(t, fleetReplicas())
	calls := map[string]float64{}
	for _, set := range [][]proofRequest{controlSet, replaySet} {
		for _, req := range set {
			got, d, _ := ask(t, a, req)
			if got != first[req.name] {
				t.Fatalf("%s after a lost its disk differs from its first answer:\n got: %.400s\nwant: %.400s", req, got, first[req.name])
			}
			set := strings.SplitN(req.name, "_", 2)[0]
			calls[set] += d.fleetVLSelect
			if d.fleetVLSelect != 0 {
				t.Logf("%s: %v VictoriaLogs calls, %v peer hits", req, d.fleetVLSelect, d.peerHits)
			}
		}
	}
	if calls["replay"] > calls["control"] {
		t.Fatalf("after a lost its disk the set written while b was paused cost %v VictoriaLogs calls, the control set %v: the buffered pushes did not reach b", calls["replay"], calls["control"])
	}
	t.Logf("a refilled from its peers: replay set %v VictoriaLogs calls, control set %v", calls["replay"], calls["control"])
}
