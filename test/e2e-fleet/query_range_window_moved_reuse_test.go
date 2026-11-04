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

// TestFleet_QueryRangeWindowsServeMovedRangesOnPeers: the log query windows
// one replica reads (semantics/query-range-window-full-limit-reads) answer a
// moved range on the other replicas. Replica a answers a 24h backward query
// with a line filter (a line a second, one in six matching: every window is
// expensive by its /hits estimate, so the windows are read one at a time);
// the newest, partial hour gives 400 of the 900 lines and the hour before it
// the rest. Replicas b and c then answer the same query moved one and two
// minutes back. A kept window is read with the request's whole limit, so a
// moved request reads only its moved newest window from VictoriaLogs (one
// window cache miss on b and c) and the hour before it from the window a
// pushed to its owner, and every answer is the newest 900 matching lines of
// its range.
//
// conformance: semantics/query-range-window-full-limit-reads
func TestFleet_QueryRangeWindowsServeMovedRangesOnPeers(t *testing.T) {
	app := fmt.Sprintf("window-moved-peers-%d", time.Now().UnixNano())
	// Inside VictoriaLogs' retention and older than -query-range-freshness.
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	var rows strings.Builder
	var stamps []time.Time
	i := 0
	for ts := anchor.Add(-2*time.Hour + 300*time.Millisecond); ts.Before(anchor.Add(50 * time.Minute)); ts = ts.Add(time.Second) {
		word := "skip"
		if i%6 == 0 {
			word = "keep-me"
			stamps = append(stamps, ts)
		}
		row, _ := json.Marshal(map[string]string{
			"_time": ts.Format(time.RFC3339Nano),
			"_msg":  fmt.Sprintf("moved-peers-%d %s", ts.UnixMilli(), word),
			"app":   app,
		})
		rows.Write(row)
		rows.WriteByte('\n')
		i++
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
	if resp, err := http.Get(fleetVLURL + "/internal/force_flush"); err == nil {
		resp.Body.Close()
	}

	// want is the newest 900 matching lines at or before end, newest first.
	want := func(end time.Time) []string {
		var out []string
		for i := len(stamps) - 1; i >= 0 && len(out) < 900; i-- {
			if !stamps[i].After(end) && stamps[i].After(end.Add(-24*time.Hour)) {
				out = append(out, fmt.Sprintf("moved-peers-%d keep-me", stamps[i].UnixMilli()))
			}
		}
		return out
	}
	get := func(addr string, end time.Time, limit int) []string {
		t.Helper()
		params := url.Values{
			"query":     {`{app="` + app + `"} |= "keep-me"`},
			"start":     {strconv.FormatInt(end.Add(-24*time.Hour).UnixNano(), 10)},
			"end":       {strconv.FormatInt(end.UnixNano(), 10)},
			"limit":     {strconv.Itoa(limit)},
			"direction": {"backward"},
		}
		resp, err := http.Get(addr + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var out struct {
			Status string `json:"status"`
			Data   struct {
				Result []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &out) != nil || out.Status != "success" {
			t.Fatalf("%s: %d %s", addr, resp.StatusCode, body)
		}
		type line struct {
			ts  int64
			msg string
		}
		var lines []line
		for _, stream := range out.Data.Result {
			for _, value := range stream.Values {
				ts, _ := strconv.ParseInt(value[0], 10, 64)
				lines = append(lines, line{ts, value[1]})
			}
		}
		for i := 1; i < len(lines); i++ { // newest first across streams
			for j := i; j > 0 && lines[j].ts > lines[j-1].ts; j-- {
				lines[j], lines[j-1] = lines[j-1], lines[j]
			}
		}
		msgs := make([]string, 0, len(lines))
		for _, l := range lines {
			msgs = append(msgs, l.msg)
		}
		return msgs
	}
	misses := func(addr string) float64 { return metricValueOrZero(t, addr, "loki_vl_proxy_window_cache_miss_total") }
	hits := func(addr string) float64 { return metricValueOrZero(t, addr, "loki_vl_proxy_window_cache_hit_total") }

	end := anchor.Add(40 * time.Minute)
	// Wait until VictoriaLogs answers every matching line; a different limit
	// per attempt so an incomplete answer is never served from a cache again.
	deadline := time.Now().Add(60 * time.Second)
	inRange := 0
	for _, ts := range stamps {
		if !ts.After(end) {
			inRange++
		}
	}
	for attempt := 5000; ; attempt++ {
		if got := get(proxyAddrs[2], end, attempt); len(got) == inRange {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs holds no complete fixture for %s", app)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if got := get(proxyAddrs[0], end, 900); !reflect.DeepEqual(got, want(end)) {
		t.Fatalf("replica a: %d lines, want the newest %d", len(got), len(want(end)))
	}
	// The windows are pushed to their owners after the answer: wait until
	// replica a's pushes stop changing for half a second.
	last, stable := metricValueOrZero(t, proxyAddrs[0], "loki_vl_proxy_peer_cache_write_through_pushes_total"), time.Now()
	for wait := time.Now().Add(10 * time.Second); time.Now().Before(wait) && time.Since(stable) < 500*time.Millisecond; time.Sleep(100 * time.Millisecond) {
		if now := metricValueOrZero(t, proxyAddrs[0], "loki_vl_proxy_peer_cache_write_through_pushes_total"); now != last {
			last, stable = now, time.Now()
		}
	}

	for i, replica := range []int{1, 2} {
		addr := proxyAddrs[replica]
		moved := end.Add(-time.Duration(i+1) * time.Minute)
		missesBefore, hitsBefore := misses(addr), hits(addr)
		got := get(addr, moved, 900)
		if !reflect.DeepEqual(got, want(moved)) {
			t.Fatalf("replica %d moved %dm: %d lines, want the newest %d", replica, i+1, len(got), len(want(moved)))
		}
		if d := misses(addr) - missesBefore; d != 1 {
			t.Errorf("replica %d moved %dm: %.0f window cache misses, want 1 (the moved newest window); the hour before it comes from replica a's window", replica, i+1, d)
		}
		if d := hits(addr) - hitsBefore; d < 1 {
			t.Errorf("replica %d moved %dm: %.0f window cache hits, want the hour before the newest one through the peer tier", replica, i+1, d)
		}
	}
}
