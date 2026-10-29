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

// TestFleet_QueryRangeSettledStepsReachPeers: the settled steps one replica
// keeps for a window evaluator answer (quality/query-range-settled-steps)
// reach the others through the peer tier: a window moved by a step on a
// second replica is answered from them (its query_steps_cache outcome is
// partial, not miss), and its answer is the one a replica computes from
// VictoriaLogs alone (the same query under another limit argument, a key no
// replica holds).
//
// conformance: quality/query-range-settled-steps
func TestFleet_QueryRangeSettledStepsReachPeers(t *testing.T) {
	app := fmt.Sprintf("settled-steps-%d", time.Now().UnixNano())
	// Inside VictoriaLogs' retention (-retentionPeriod=3d) and 	// -query-range-freshness, on the 30s step grid.
	s0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	var rows strings.Builder
	for i := 0; i < 360; i++ { // a line every 10s for an hour, a new pod every two minutes
		row, _ := json.Marshal(map[string]string{
			"_time": s0.Add(3*time.Second + time.Duration(i)*10*time.Second).Format(time.RFC3339Nano),
			"_msg":  "tick",
			"app":   app,
			"pod":   fmt.Sprintf("pod-%02d", (i/12)%30),
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
	if resp.StatusCode >= 300 {
		t.Fatalf("ingest: %d", resp.StatusCode)
	}
	if resp, err := http.Get(fleetVLURL + "/internal/force_flush"); err == nil {
		resp.Body.Close()
	}

	query := `sum without (pod) (count_over_time({app="` + app + `"}[2m]))`
	get := func(addr string, from, to time.Time, limit string) map[string]any {
		t.Helper()
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(from.UnixNano(), 10)}, "end": {strconv.FormatInt(to.UnixNano(), 10)}, "step": {"30"}, "limit": {limit}}
		resp, err := http.Get(addr + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &out) != nil || out["status"] != "success" {
			t.Fatalf("%s: %d %s", addr, resp.StatusCode, body)
		}
		return out
	}
	steps := func(addr, outcome string) float64 {
		return metricValueOrZero(t, addr, `loki_vl_proxy_internal_operation_total{operation="query_steps_cache",outcome="`+outcome+`"}`)
	}
	// Wait until VictoriaLogs answers the lines.
	deadline := time.Now().Add(60 * time.Second)
	for attempt := 1000; ; attempt++ { // a limit per attempt: an empty answer is not served again
		data, _ := get(proxyAddrs[2], s0, s0.Add(time.Hour), strconv.Itoa(attempt))["data"].(map[string]any)
		if result, _ := data["result"].([]any); len(result) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs holds no line of %s", app)
		}
		time.Sleep(500 * time.Millisecond)
	}

	from, to := s0.Add(10*time.Minute), s0.Add(40*time.Minute)
	pushes := metricValueOrZero(t, proxyAddrs[0], "loki_vl_proxy_peer_cache_write_through_pushes_total")
	get(proxyAddrs[0], from, to, "100")
	// The steps are stored off the request path, then pushed to their owner
	// (none when replica a owns them).
	deadline = time.Now().Add(30 * time.Second)
	for steps(proxyAddrs[0], "stored") < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("replica a kept no settled steps")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for wait := time.Now().Add(5 * time.Second); time.Now().Before(wait); time.Sleep(100 * time.Millisecond) {
		if metricValueOrZero(t, proxyAddrs[0], "loki_vl_proxy_peer_cache_write_through_pushes_total") > pushes {
			break
		}
	}

	before := steps(proxyAddrs[1], "partial")
	moved := get(proxyAddrs[1], from.Add(30*time.Second), to.Add(30*time.Second), "100")
	if after := steps(proxyAddrs[1], "partial"); after <= before {
		t.Fatalf("replica b evaluated the moved window whole (partial %v -> %v): the settled steps did not reach it", before, after)
	}
	fresh := get(proxyAddrs[2], from.Add(30*time.Second), to.Add(30*time.Second), "99")
	if !reflect.DeepEqual(moved["data"], fresh["data"]) {
		t.Fatalf("the answer from peer steps differs from VictoriaLogs'\n peer: %v\n VictoriaLogs: %v", moved["data"], fresh["data"])
	}
}

func metricValueOrZero(t *testing.T, proxyAddr, metricPrefix string) float64 {
	t.Helper()
	resp, err := http.Get(proxyAddr + "/metrics")
	if err != nil {
		t.Fatalf("metrics from %s failed: %v", proxyAddr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, metricPrefix+" "); ok {
			v, _ := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			return v
		}
	}
	return 0
}
