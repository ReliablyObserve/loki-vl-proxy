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

// TestFleet_OversizedReplyStaysOnItsReplica: an answer larger than one entry of
// the compatibility-edge cache (a tenth of its budget: 2.56 MiB at the fleet's
// default -cache-max-bytes and -compat-cache-max-percent) is kept in the primary
// memory cache of the replica that computed it, and only there
// (quality/edge-cache-oversized-reply, a documented limit):
//   - replica a computes the answer from VictoriaLogs;
//   - replica a answers the repeat from its cache, asking VictoriaLogs nothing;
//   - replica b, which never computed it, gets no copy from a: it asks
//     VictoriaLogs and answers the same data.
//
// The window ends five minutes before now: inside -query-range-freshness, so
// the exact range cache keeps no copy of it, and outside
// -recent-tail-refresh-window, so a repeat is not refreshed. The fixture is 300
// lines over the hour, one per series, each with a 12 KB field value: an
// answer of about 3.6 MB, over the edge cache entry and under the primary
// cache entry (25.6 MiB), within the default series limit (500).
//
// conformance: quality/edge-cache-oversized-reply
func TestFleet_OversizedReplyStaysOnItsReplica(t *testing.T) {
	app := fmt.Sprintf("oversized-reply-%d", time.Now().UnixNano())
	end := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	start := end.Add(-time.Hour)
	const series = 300
	value := strings.Repeat("pod-segment-", 1000) // 12 KB
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

	query := `sum by (pod) (count_over_time({app="` + app + `"}[5m]))`
	refs := 0
	// reference is the same answer under a query no replica holds (a line
	// filter every line passes, spelled differently on every call).
	reference := func() string {
		refs++
		return fmt.Sprintf(`sum by (pod) (count_over_time({app="%s"} != "absent-%d" [5m]))`, app, refs)
	}
	get := func(addr, q string) (map[string]any, int, int) {
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
		return out, len(result), len(body)
	}
	// Wait until VictoriaLogs answers every line (read on replica c only).
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, n, _ := get(proxyAddrs[2], reference()); n == series {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs does not hold the %d lines of %s", series, app)
		}
		time.Sleep(500 * time.Millisecond)
	}
	want, _, _ := get(proxyAddrs[2], reference())

	a, b := proxyAddrs[0], proxyAddrs[1]
	before := readCounters(t, a)
	first, n, size := get(a, query)
	if n != series || size <= 2_684_354 || size >= 26_843_545 {
		t.Fatalf("replica a: %d series in %d bytes; want %d series over the edge entry and under the primary entry", n, size, series)
	}
	if !reflect.DeepEqual(answerData(first), answerData(want)) {
		t.Fatal("replica a's answer differs from VictoriaLogs'")
	}
	cold := readCounters(t, a)
	if cold.vlSelect <= before.vlSelect {
		t.Fatal("replica a answered the cold request without VictoriaLogs")
	}

	// The repeat on a: a hit, no VictoriaLogs call.
	repeat, _, _ := get(a, query)
	after := readCounters(t, a)
	if made := after.vlSelect - cold.vlSelect; made != 0 {
		t.Fatalf("replica a's repeat made %v VictoriaLogs calls, want none", made)
	}
	if after.replyHits <= cold.replyHits {
		t.Fatal("replica a's repeat was not answered from its cache")
	}
	if !reflect.DeepEqual(answerData(repeat), answerData(first)) {
		t.Fatal("replica a's repeat answered differently")
	}

	// Replica b holds no copy: it asks VictoriaLogs and gets the same data.
	bBefore := readCounters(t, b)
	other, _, _ := get(b, query)
	bAfter := readCounters(t, b)
	if bAfter.vlSelect <= bBefore.vlSelect {
		t.Fatal("replica b answered without VictoriaLogs: the oversized copy left replica a")
	}
	if !reflect.DeepEqual(answerData(other), answerData(want)) {
		t.Fatal("replica b's answer differs from VictoriaLogs'")
	}
}
