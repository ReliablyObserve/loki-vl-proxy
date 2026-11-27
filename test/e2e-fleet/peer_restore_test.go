//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestFleet_PeerRestoreRefillsAWipedDisk: the ring of two replicas of the
// "handover" profile (-cache-startup-warm-bytes on, -peer-replication-factor
// at its default 2, so both replicas hold every key; the CI run at RF 1 only
// checks that the restore ends at once, as no other copy exists). Replica a computes an
// answer from VictoriaLogs and pushes it to replica b. Replica b is then
// removed with its volume and started on a new, empty one:
//   - before /ready answers 200 it pulled its keys from replica a in one bulk
//     stream (source="peer", outcome done, planned = processed = restored +
//     skipped, an empty index rebuild), and replica a counts the stream;
//   - the repeat on replica b is answered from its memory tier with no
//     VictoriaLogs call (X-Loki-VL-Backend-Calls: 0), no disk read and no peer
//     read, equal to the first answer.
//
// conformance: cache-restart-handover
func TestFleet_PeerRestoreRefillsAWipedDisk(t *testing.T) {
	fleetCompose(t, append([]string{"--profile", "handover", "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120"}, handoverServices...)...)
	t.Cleanup(func() {
		fleetCompose(t, append([]string{"--profile", "handover", "rm", "--stop", "--force", "--volumes"}, handoverServices...)...)
	})
	for _, addr := range handoverAddrs {
		waitProxyReady(t, addr)
	}
	a, b := handoverAddrs[0], handoverAddrs[1]

	app := fmt.Sprintf("peer-restore-%d", time.Now().UnixNano())
	end := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	start := end.Add(-time.Hour)
	var rows strings.Builder
	for i := 0; i < 120; i++ {
		row, _ := json.Marshal(map[string]string{
			"_time": start.Add(5*time.Second + time.Duration(i)*29*time.Second).Format(time.RFC3339Nano),
			"_msg":  "tick",
			"app":   app,
			"pod":   fmt.Sprintf("pod-%02d", i%7),
		})
		rows.Write(row)
		rows.WriteByte('\n')
	}
	resp, err := http.Post(fleetVLURL+"/insert/jsonline?_stream_fields=app,pod", "application/stream+json", strings.NewReader(rows.String()))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp, err := http.Post(fleetVLURL+"/internal/force_flush", "text/plain", nil); err == nil {
		resp.Body.Close()
	}

	type answer struct {
		data    any
		series  int
		vlCalls string
	}
	get := func(addr, q string) answer {
		t.Helper()
		params := url.Values{"query": {q}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
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
		return answer{data: answerData(out), series: len(result), vlCalls: resp.Header.Get("X-Loki-VL-Backend-Calls")}
	}
	q := `sum by (pod) (count_over_time({app="` + app + `"}[5m]))`
	deadline := time.Now().Add(60 * time.Second)
	var first answer
	for first = get(a, q); first.series != 7; first = get(a, q) {
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs does not hold the lines of %s (%d series)", app, first.series)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// Replica b holds the pushed copy before it loses its disk.
	waitFor(t, "replica b's copy", 30*time.Second, func() bool { return get(b, q).vlCalls == "0" })
	aBefore := scrapeMetrics(t, a)

	// Replica b loses its disk: removed with its volume, started on a new one.
	restarted := time.Now()
	fleetCompose(t, "--profile", "handover", "rm", "--stop", "--force", "--volumes", handoverServices[1])
	fleetCompose(t, "--profile", "handover", "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120", handoverServices[1])
	waitProxyReady(t, b)
	took := time.Since(restarted)

	m := scrapeMetrics(t, b)
	peer := map[string]string{"source": "peer"}
	if strings.Contains(os.Getenv("FLEET_PROXY_EXTRA_ARG"), "-peer-replication-factor=1") {
		// One copy per key: the keys replica b owned were on its disk only (a
		// non-owner keeps a shadow, which is never sent), so there is nothing
		// to restore. The restore ends at once and b answers correctly.
		if m.sum("loki_vl_proxy_cache_restart_warm_outcome", map[string]string{"source": "peer", "outcome": "running"}) != 0 ||
			m.sum("loki_vl_proxy_cache_restart_warm_outcome", map[string]string{"source": "peer", "outcome": "deadline"}) != 0 {
			t.Fatal("at RF 1 the peer restore did not end at once")
		}
		if again := get(b, q); !reflect.DeepEqual(again.data, first.data) {
			t.Fatal("replica b answered differently")
		}
		t.Logf("RF 1: ready after %s; the restore had nothing to pull (the wiped copies had no other replica)", took.Round(time.Millisecond))
		return
	}
	if got := m.sum("loki_vl_proxy_cache_restart_warm_outcome", map[string]string{"source": "peer", "outcome": "done"}); got != 1 {
		t.Fatalf("peer restore outcome done = %v\n%s", got, replicaDiagnostics(b))
	}
	if got := m.sum("loki_vl_proxy_cache_restart_index_rebuild_entries", map[string]string{"source": "disk"}); got != 0 {
		t.Fatalf("the new disk held %v records at start", got)
	}
	planned := m.sum("loki_vl_proxy_cache_restart_warm_planned_entries", peer)
	processed := m.sum("loki_vl_proxy_cache_restart_warm_processed_entries", peer)
	skipped := m.sum("loki_vl_proxy_cache_restart_warm_skipped_entries", peer)
	if planned < 1 || processed != planned || skipped >= planned {
		t.Fatalf("peer restore planned %v, processed %v, skipped %v", planned, processed, skipped)
	}
	aAfter := scrapeMetrics(t, a)
	if served := aAfter.sum("loki_vl_proxy_peer_cache_bulk_serve_streams_total", map[string]string{"outcome": "done"}) - aBefore.sum("loki_vl_proxy_peer_cache_bulk_serve_streams_total", map[string]string{"outcome": "done"}); served != 1 {
		t.Fatalf("replica a served %v bulk streams, want 1", served)
	}
	t.Logf("wiped disk: ready after %s, %v entries planned, %v skipped, %v records and %v bytes sent by replica a, %.0f B/s restored",
		took.Round(time.Millisecond), planned, skipped,
		aAfter.sum("loki_vl_proxy_peer_cache_bulk_serve_records_total", nil)-aBefore.sum("loki_vl_proxy_peer_cache_bulk_serve_records_total", nil),
		aAfter.sum("loki_vl_proxy_peer_cache_bulk_serve_bytes_total", nil)-aBefore.sum("loki_vl_proxy_peer_cache_bulk_serve_bytes_total", nil),
		m.sum("loki_vl_proxy_cache_restart_warm_throughput_bytes_per_second", peer))

	before := readCounters(t, b)
	again := get(b, q)
	after := readCounters(t, b)
	if again.vlCalls != "0" || after.vlSelect != before.vlSelect {
		t.Fatalf("replica b made %s VictoriaLogs calls after the peer restore, want none", again.vlCalls)
	}
	if after.peerHits != before.peerHits || after.tierL2Hits != before.tierL2Hits || after.tierL1Hits <= before.tierL1Hits {
		t.Fatalf("replica b answered from peers %v, disk %v, memory %v times: want memory only", after.peerHits-before.peerHits, after.tierL2Hits-before.tierL2Hits, after.tierL1Hits-before.tierL1Hits)
	}
	if !reflect.DeepEqual(again.data, first.data) {
		t.Fatal("replica b answered differently after the peer restore")
	}
}

func waitFor(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(within); !cond(); time.Sleep(250 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
