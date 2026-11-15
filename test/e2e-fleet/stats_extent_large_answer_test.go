//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestFleet_StatsExtentKeepsAnswersLargerThanOneEntry: a metric answer larger
// than one cache entry (a tenth of -cache-max-bytes, 25.6 MiB at the fleet's
// default size) is kept split by split (quality/query-range-extent-cache),
// and its splits reach the other replicas through the peer tier:
//   - replica a reads the answer from VictoriaLogs and stores its splits;
//   - replica b answers the window moved back by a step from them (its
//     stats_extent_cache outcome is partial, with one VictoriaLogs call for
//     the one bucket it lacks);
//   - replica a, after losing its memory and its disk, answers the window
//     moved back again from its peers (a split it alone owned is read again);
//   - each answer is the one replica c computes from VictoriaLogs alone (the
//     same answer under a query no replica holds: a filter every line passes,
//     spelled differently on every call).
//
// The fixture is 450 lines over six hours, one per series, each with a long
// field value, so the answer is over one entry while each of its splits (three
// or four, by where the hour falls on the 128-bucket split grid) is under it,
// within the default series limit (500).
//
// conformance: quality/query-range-extent-cache-large-answer, quality/query-range-extent-cache
func TestFleet_StatsExtentKeepsAnswersLargerThanOneEntry(t *testing.T) {
	app := fmt.Sprintf("extent-large-%d", time.Now().UnixNano())
	// Inside VictoriaLogs' retention (-retentionPeriod=3d) and older than
	// -query-range-freshness, on the 60s step grid.
	s0 := time.Now().Add(-9 * time.Hour).Truncate(time.Hour)
	const series = 450
	value := strings.Repeat("pod-segment-", 6000) // 72 KB
	var rows strings.Builder
	for i := 0; i < series; i++ {
		row, _ := json.Marshal(map[string]string{
			"_time": s0.Add(5*time.Second + time.Duration(i)*48*time.Second).Format(time.RFC3339Nano),
			"_msg":  "tick",
			"app":   app,
			"pod":   fmt.Sprintf("%s%04d", value, i),
		})
		rows.Write(row)
		rows.WriteByte('\n')
	}
	resp, err := http.Post(fleetVLURL+"/insert/jsonline?_stream_fields=app", "application/stream+json", strings.NewReader(rows.String()))
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

	query := `sum by (pod) (count_over_time({app="` + app + `"}[1m]))`
	// reference is the same answer under a query no replica holds: a line
	// filter every line passes, spelled differently on every call, so it is
	// read from VictoriaLogs whatever a replica keeps.
	refs := 0
	reference := func() string {
		refs++
		return fmt.Sprintf(`sum by (pod) (count_over_time({app="%s"} != "absent-%d" [1m]))`, app, refs)
	}
	get := func(addr, q string, from, to time.Time) (map[string]any, int) {
		t.Helper()
		params := url.Values{"query": {q}, "start": {strconv.FormatInt(from.UnixNano(), 10)}, "end": {strconv.FormatInt(to.UnixNano(), 10)}, "step": {"60"}}
		resp, err := http.Get(addr + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &out) != nil || out["status"] != "success" {
			t.Fatalf("%s: %d %.500s", addr, resp.StatusCode, body)
		}
		result, _ := out["data"].(map[string]any)["result"].([]any)
		return out, len(result)
	}
	extent := func(addr, outcome string) float64 {
		return metricValueOrZero(t, addr, `loki_vl_proxy_internal_operation_total{operation="stats_extent_cache",outcome="`+outcome+`"}`)
	}
	statsCalls := func(addr string) float64 {
		return readCounters(t, addr).vlStats
	}
	// Wait until VictoriaLogs answers every line.
	from, to := s0, s0.Add(6*time.Hour)
	deadline := time.Now().Add(60 * time.Second)
	for {
		// The reference query is read from VictoriaLogs on replica c only.
		if _, n := get(proxyAddrs[2], reference(), from, to); n == series {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs does not hold the %d lines of %s", series, app)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Nothing an earlier test started may still run: a push queue it left
	// full, or a peer it left suspended, drops replica a's pushes below.
	requireFleetUp(t)
	settleFleet(t, fleetReplicas())
	storedBefore := extent(proxyAddrs[0], "stored")
	pushesBefore := readCounters(t, proxyAddrs[0])
	throttledBefore := metricValueOrZero(t, proxyAddrs[0], "loki_vl_proxy_peer_cache_push_throttled_total")
	first, n := get(proxyAddrs[0], query, from, to)
	if n != series {
		t.Fatalf("replica a answered %d series, want %d", n, series)
	}
	deadline = time.Now().Add(30 * time.Second)
	for extent(proxyAddrs[0], "stored") <= storedBefore {
		if time.Now().After(deadline) {
			t.Fatalf("replica a kept no split of an answer larger than one entry")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Every split has a replica other than a when keys have two replicas, so
	// at least one push per split (three or more) must leave a before b and
	// the restarted a can read the splits from a peer. A push that cannot
	// leave at once (a peer's breaker open, the queue full) waits in the push
	// buffer and is replayed; the pushes counter counts only delivered ones.
	if fleetReplicationFactor(t) >= 2 {
		deadline = time.Now().Add(10 * time.Second)
		for {
			now := readCounters(t, proxyAddrs[0])
			if now.wtPushes >= pushesBefore.wtPushes+3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replica a pushed %v of its splits to its peers within 10s, want at least 3 (write-through errors +%v, pushes throttled +%v, peer errors +%v)",
					now.wtPushes-pushesBefore.wtPushes, now.wtErrors-pushesBefore.wtErrors,
					metricValueOrZero(t, proxyAddrs[0], "loki_vl_proxy_peer_cache_push_throttled_total")-throttledBefore, now.peerErrors-pushesBefore.peerErrors)
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		// One copy per key: a split a owns is pushed nowhere.
		for wait := time.Now().Add(10 * time.Second); time.Now().Before(wait); time.Sleep(100 * time.Millisecond) {
			if readCounters(t, proxyAddrs[0]).wtPushes >= pushesBefore.wtPushes+3 {
				break
			}
		}
	}
	settleFleet(t, fleetReplicas())
	want, _ := get(proxyAddrs[2], reference(), from, to)
	if !reflect.DeepEqual(answerData(first), answerData(want)) {
		t.Fatalf("replica a's answer differs from VictoriaLogs'")
	}

	// movedFromPeers asks replica idx for the window moved back by `back`
	// and requires it answered from stored splits, with at most maxCalls
	// VictoriaLogs calls (the missing bucket, and a split whose only copy was
	// on a replica that lost it). mayReadWhole accepts a whole read: with one
	// copy per key a replica that lost its disk lost every split it owned,
	// and an owner never asks its peers for its own keys, so when it owned
	// every split of the window (the ring puts each split on it with odds of
	// about one in three) nothing is left to read from its peers.
	movedFromPeers := func(idx int, back time.Duration, maxCalls float64, mayReadWhole bool) {
		t.Helper()
		addr := proxyAddrs[idx]
		outcomes := []string{"hit", "partial", "miss", "unreadable", "error"}
		before := map[string]float64{}
		for _, o := range outcomes {
			before[o] = extent(addr, o)
		}
		peerBefore := readCounters(t, addr)
		partial, calls := before["partial"]+before["hit"], statsCalls(addr)
		got, _ := get(addr, query, from.Add(-back), to.Add(-back))
		if after := extent(addr, "partial") + extent(addr, "hit"); after <= partial && mayReadWhole {
			t.Logf("replica %d read the moved window whole: with one copy per key it owned every split it lost", idx)
		} else if after <= partial {
			deltas := map[string]float64{}
			for _, o := range outcomes {
				deltas[o] = extent(addr, o) - before[o]
			}
			peerAfter := readCounters(t, addr)
			t.Fatalf("replica %d read the moved window whole: the splits did not reach it (partial+hit %v -> %v; stats_extent_cache outcomes %v; l3 hits +%v, peer misses +%v, peer errors +%v)",
				idx, partial, after, deltas, peerAfter.tierL3Hits-peerBefore.tierL3Hits, peerAfter.peerMisses-peerBefore.peerMisses, peerAfter.peerErrors-peerBefore.peerErrors)
		}
		if made := statsCalls(addr) - calls; made > maxCalls {
			t.Fatalf("replica %d made %v stats_query_range calls, want at most %v", idx, made, maxCalls)
		}
		ref, _ := get(proxyAddrs[2], reference(), from.Add(-back), to.Add(-back))
		if !reflect.DeepEqual(answerData(got), answerData(ref)) {
			t.Fatalf("replica %d's answer from stored splits differs from VictoriaLogs'", idx)
		}
	}
	movedFromPeers(1, time.Minute, 1, false)
	recreateWithoutDisk(t, 0) // memory and disk lost: the splits are on the peers
	// At most three missing ranges are read apart.
	movedFromPeers(0, 2*time.Minute, 3, fleetReplicationFactor(t) < 2)
}
