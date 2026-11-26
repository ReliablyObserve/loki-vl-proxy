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

var (
	handoverServices = []string{"proxy-handover-a", "proxy-handover-b"}
	handoverAddrs    = []string{
		envOr("FLEET_PROXY_HANDOVER_A_URL", "http://localhost:3105"),
		envOr("FLEET_PROXY_HANDOVER_B_URL", "http://localhost:3106"),
	}
)

// TestFleet_RestartHandoverKeepsTheResponseCacheWarm: a ring of two replicas
// with the restart hand-over on (-cache-shutdown-flush-timeout,
// -cache-startup-warm-bytes; compose profile "handover"). Replica a computes
// two answers from VictoriaLogs: one that its disk tier also holds (a single
// series) and one larger than an edge cache entry, which stays in its memory
// only. After `docker compose restart` (SIGTERM, then a new process on the same
// disk):
//   - replica a reports the hand-over its shutdown left (complete, ranked
//     records and the memory-only answer) and what its warm-up restored;
//   - both repeats are answered from its memory tier with no VictoriaLogs call
//     (X-Loki-VL-Backend-Calls: 0) and no disk read, equal to the first answers;
//   - replica b answers the same data (from VictoriaLogs, or by a peer read of
//     replica a's copy, which it keeps memory-only).
//
// conformance: cache-restart-handover
func TestFleet_RestartHandoverKeepsTheResponseCacheWarm(t *testing.T) {
	fleetCompose(t, append([]string{"--profile", "handover", "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120"}, handoverServices...)...)
	t.Cleanup(func() {
		fleetCompose(t, append([]string{"--profile", "handover", "rm", "--stop", "--force", "--volumes"}, handoverServices...)...)
	})
	for _, addr := range handoverAddrs {
		waitProxyReady(t, addr)
	}

	app := fmt.Sprintf("restart-handover-%d", time.Now().UnixNano())
	end := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	start := end.Add(-time.Hour)
	const series = 300
	value := strings.Repeat("pod-segment-", 1000) // 12 KB per series label
	var rows strings.Builder
	for i := 0; i < series; i++ {
		row, _ := json.Marshal(map[string]string{
			"_time": start.Add(5*time.Second + time.Duration(i)*11*time.Second).Format(time.RFC3339Nano),
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

	small := `sum(count_over_time({app="` + app + `"}[5m]))`
	oversized := `sum by (pod) (count_over_time({app="` + app + `"}[5m]))`
	refs := 0
	reference := func() string {
		refs++
		return fmt.Sprintf(`sum by (pod) (count_over_time({app="%s"} != "absent-%d" [5m]))`, app, refs)
	}
	type answer struct {
		data    any
		series  int
		bytes   int
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
		return answer{data: answerData(out), series: len(result), bytes: len(body), vlCalls: resp.Header.Get("X-Loki-VL-Backend-Calls")}
	}
	// Wait until VictoriaLogs answers every line (read on replica b only).
	deadline := time.Now().Add(60 * time.Second)
	for get(handoverAddrs[1], reference()).series != series {
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs does not hold the %d lines of %s", series, app)
		}
		time.Sleep(500 * time.Millisecond)
	}

	a, b := handoverAddrs[0], handoverAddrs[1]
	firstSmall, firstBig := get(a, small), get(a, oversized)
	if firstSmall.series != 1 || firstBig.series != series || firstBig.bytes <= 2_684_354 {
		t.Fatalf("replica a: %d and %d series (%d bytes); want 1, and %d over the edge cache entry", firstSmall.series, firstBig.series, firstBig.bytes, series)
	}
	if firstSmall.vlCalls == "0" || firstBig.vlCalls == "0" {
		t.Fatal("replica a answered a cold request without VictoriaLogs")
	}
	for _, q := range []string{small, oversized} {
		if again := get(a, q); again.vlCalls != "0" {
			t.Fatalf("replica a's repeat before the restart made %s VictoriaLogs calls", again.vlCalls)
		}
	}

	// SIGTERM: the HTTP server drains, the hand-over is flushed, the disk tier
	// writes its manifest last; then a new process starts on the same disk.
	flushStarted := time.Now()
	fleetCompose(t, "--profile", "handover", "restart", "-t", "40", handoverServices[0])
	waitProxyReady(t, a)
	restartTook := time.Since(flushStarted)

	m := scrapeMetrics(t, a)
	if got := m.sum("loki_vl_proxy_cache_restart_previous_flush_complete", nil); got != 1 {
		t.Fatalf("previous flush complete = %v, want 1", got)
	}
	if got := m.sum("loki_vl_proxy_cache_restart_previous_flush_records", map[string]string{"kind": "memory"}); got < 1 {
		t.Fatalf("previous flush kept %v memory-only answers, want the oversized one", got)
	}
	if got := m.sum("loki_vl_proxy_cache_restart_previous_flush_records", map[string]string{"kind": "ranked"}); got < 1 {
		t.Fatalf("previous flush ranked %v disk records", got)
	}
	if got := m.sum("loki_vl_proxy_cache_restart_warm_done", nil); got != 1 {
		t.Fatalf("warm done = %v", got)
	}
	warmDisk := m.sum("loki_vl_proxy_cache_restart_warm_entries", map[string]string{"kind": "disk"})
	warmMemory := m.sum("loki_vl_proxy_cache_restart_warm_entries", map[string]string{"kind": "memory"})
	if warmDisk < 1 || warmMemory < 1 {
		t.Fatalf("warm-up restored %v disk and %v memory entries, want both kinds", warmDisk, warmMemory)
	}
	// The warm-up's plan and progress: everything planned was processed, the
	// restore ended "done", and the index rebuild was measured.
	disk := map[string]string{"source": "disk"}
	planned := m.sum("loki_vl_proxy_cache_restart_warm_planned_entries", disk)
	processed := m.sum("loki_vl_proxy_cache_restart_warm_processed_entries", disk)
	skipped := m.sum("loki_vl_proxy_cache_restart_warm_skipped_entries", disk)
	if planned < warmDisk+warmMemory || processed != planned || planned != warmDisk+warmMemory+skipped {
		t.Fatalf("warm-up planned %v, processed %v, restored %v+%v, skipped %v", planned, processed, warmDisk, warmMemory, skipped)
	}
	if got := m.sum("loki_vl_proxy_cache_restart_warm_outcome", map[string]string{"source": "disk", "outcome": "done"}); got != 1 {
		t.Fatalf("warm-up outcome done = %v, want 1", got)
	}
	if got := m.sum("loki_vl_proxy_cache_restart_index_rebuild_entries", disk); got < warmDisk {
		t.Fatalf("index rebuild held %v records, the warm-up restored %v from disk", got, warmDisk)
	}
	t.Logf("restart %s, previous flush %.3fs, warm-up %.3fs: %v disk + %v memory entries, %v bytes",
		restartTook.Round(time.Millisecond),
		m.sum("loki_vl_proxy_cache_restart_previous_flush_duration_seconds", nil),
		m.sum("loki_vl_proxy_cache_restart_warm_duration_seconds", nil),
		warmDisk, warmMemory, m.sum("loki_vl_proxy_cache_restart_warm_bytes", nil))

	before := readCounters(t, a)
	afterSmall, afterBig := get(a, small), get(a, oversized)
	after := readCounters(t, a)
	if afterSmall.vlCalls != "0" || afterBig.vlCalls != "0" {
		t.Fatalf("after the restart replica a made %s and %s VictoriaLogs calls, want none", afterSmall.vlCalls, afterBig.vlCalls)
	}
	if made := after.vlSelect - before.vlSelect; made != 0 {
		t.Fatalf("after the restart replica a made %v VictoriaLogs select calls, want none", made)
	}
	if after.tierL2Hits != before.tierL2Hits {
		t.Fatalf("after the restart replica a read its disk %v times: the warm-up should have served from memory", after.tierL2Hits-before.tierL2Hits)
	}
	if after.tierL1Hits <= before.tierL1Hits {
		t.Fatal("after the restart replica a's repeats were not memory hits")
	}
	if !reflect.DeepEqual(afterSmall.data, firstSmall.data) || !reflect.DeepEqual(afterBig.data, firstBig.data) {
		t.Fatal("after the restart replica a answered differently")
	}

	// Replica b answers the same data, from VictoriaLogs or by reading replica
	// a's copy (a peer read, as before the restart; the reader keeps such a
	// copy memory-only, which the unit tests pin).
	bBefore := readCounters(t, b)
	other := get(b, oversized)
	bAfter := readCounters(t, b)
	if other.vlCalls == "0" && bAfter.peerHits <= bBefore.peerHits {
		t.Fatal("replica b answered the oversized query with neither VictoriaLogs nor a peer read")
	}
	if !reflect.DeepEqual(other.data, firstBig.data) {
		t.Fatal("replica b's answer differs")
	}
}
