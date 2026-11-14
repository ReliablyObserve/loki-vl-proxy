//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Loki's /loki/api/v1/tail takes start (default one hour ago) and limit
// (default 100) and sends the entries since start, up to limit, before the
// live ones (HTTP API reference). This test pushes more entries than the
// default limit to Loki and VictoriaLogs, proves both stores hold them, then
// opens tails with the same parameters on Loki and on every proxy tail
// variant (main, synthetic tail, native tail) and compares the entries each
// sends: the history (timestamps, lines, order) and a live line after it.

const tailHistoryRows = 130

// tailHistoryTargets are the tails compared with Loki's.
var tailHistoryTargets = []struct{ name, base string }{
	{"proxy", proxyURL},
	{"tail-synthetic", tailProxyURL},
	{"tail-native", tailNativeURL},
}

// tailHistoryEntry is one entry of a tail frame: its timestamp, line, stream
// labels (sorted) and, with categorize-labels, its metadata element.
type tailHistoryEntry struct{ ts, line, labels, metadata string }

// dialTailHistory opens a tail; it returns the handshake status when the
// upgrade is refused.
func dialTailHistory(t *testing.T, base string, params url.Values, categorize bool) (*websocket.Conn, int) {
	t.Helper()
	headers := http.Header{"X-Scope-OrgID": {"0"}}
	if categorize {
		headers.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 10 * time.Second}).Dial("ws"+strings.TrimPrefix(base, "http")+"/loki/api/v1/tail?"+params.Encode(), headers)
	if err != nil {
		if resp != nil {
			return nil, resp.StatusCode
		}
		t.Fatalf("tail dial %s: %v", base, err)
	}
	return conn, http.StatusSwitchingProtocols
}

// readTailEntries reads frames until want entries arrived and then quiet
// passed without one, or timeout.
func readTailEntries(conn *websocket.Conn, want int, quiet, timeout time.Duration) []tailHistoryEntry {
	var got []tailHistoryEntry
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		wait := quiet
		if len(got) < want {
			wait = time.Until(deadline)
		}
		_ = conn.SetReadDeadline(time.Now().Add(wait))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break // quiet passed, or the tail ended
		}
		var frame struct {
			Streams []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"streams"`
		}
		if json.Unmarshal(msg, &frame) != nil {
			continue
		}
		for _, s := range frame.Streams {
			names := make([]string, 0, len(s.Stream))
			for k := range s.Stream {
				names = append(names, k)
			}
			sort.Strings(names)
			var labels strings.Builder
			for _, k := range names {
				fmt.Fprintf(&labels, "%s=%q,", k, s.Stream[k])
			}
			for _, v := range s.Values {
				if len(v) < 2 {
					continue
				}
				var ts, line, metadata string
				_ = json.Unmarshal(v[0], &ts)
				_ = json.Unmarshal(v[1], &line)
				if len(v) > 2 {
					metadata = tailHistoryCanonical(v[2])
				}
				got = append(got, tailHistoryEntry{ts, line, labels.String(), metadata})
			}
		}
	}
	return got
}

// tailHistoryCanonical is a JSON value with its object keys sorted.
func tailHistoryCanonical(raw json.RawMessage) string {
	var v interface{}
	if json.Unmarshal(raw, &v) != nil {
		return string(raw)
	}
	out, _ := json.Marshal(v) // encoding/json writes map keys sorted
	return string(out)
}

// tailHistoryLogCount is the entries a query_range answer holds.
func tailHistoryLogCount(t *testing.T, base, query string, start, end time.Time) int {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	params.Set("limit", "1000")
	resp, err := http.Get(base + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		t.Fatalf("query_range %s: %v", base, err)
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
		Warnings []string `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK || body.Status != "success" || len(body.Warnings) > 0 {
		t.Fatalf("query_range %s: status %d %q warnings %v err %v", base, resp.StatusCode, body.Status, body.Warnings, err)
	}
	n := 0
	for _, s := range body.Data.Result {
		n += len(s.Values)
	}
	return n
}

// conformance: semantics/tail-history-start-limit, loki_api_v1_tail
func TestTailHistory_StartLimitLikeLoki(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-history-%d", now.UnixNano())
	query := fmt.Sprintf(`{app=%q}`, app)
	base := now.Add(-10 * time.Minute)
	lines := make([]logLine, tailHistoryRows)
	for i := range lines {
		lines[i] = logLine{Msg: fmt.Sprintf("tail history %03d %s", i, app), Level: "info"}
	}
	// One second apart, from ten minutes ago: identical bytes, timestamps
	// and stream labels on both stores.
	pushCustomToLoki(t, base, map[string]string{"app": app, "env": "test"}, lines)
	pushCustomToVL(t, base, map[string]string{"app": app, "env": "test"}, lines, []string{"app", "env"})

	// Both stores hold every entry before any tail is compared.
	deadline := time.Now().Add(60 * time.Second)
	for {
		end := time.Now()
		loki := tailHistoryLogCount(t, lokiURL, query, base.Add(-time.Minute), end)
		proxy := tailHistoryLogCount(t, proxyURL, query, base.Add(-time.Minute), end)
		if loki == tailHistoryRows && proxy == tailHistoryRows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stores not ready: Loki %d entries, proxy %d, want %d each", loki, proxy, tailHistoryRows)
		}
		time.Sleep(time.Second)
	}

	cases := []struct {
		name   string
		params url.Values
		want   int // the history Loki is expected to send
	}{
		{"no start and no limit", url.Values{}, 100},
		{"start an hour ago, limit 100 (Grafana Live)", url.Values{"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "limit": {"100"}}, 100},
		{"limit 5", url.Values{"limit": {"5"}}, 5},
		{"limit above the entries", url.Values{"limit": {"500"}}, tailHistoryRows},
		{"start inside the entries", url.Values{"start": {strconv.FormatInt(base.Add(100*time.Second).UnixNano(), 10)}}, tailHistoryRows - 100},
		{"start in unix seconds", url.Values{"start": {strconv.FormatInt(base.Add(100*time.Second).Unix(), 10)}}, tailHistoryRows - 100},
		{"start older than the retention", url.Values{"start": {"1"}, "limit": {"500"}}, tailHistoryRows},
		{"start in the future", url.Values{"start": {strconv.FormatInt(now.Add(time.Hour).UnixNano(), 10)}}, 0},
	}
	for _, categorize := range []bool{false, true} {
		for _, tc := range cases {
			name := tc.name
			if categorize {
				name += "/categorize-labels"
			}
			t.Run(name, func(t *testing.T) {
				params := url.Values{"query": {query}}
				for k, v := range tc.params {
					params[k] = v
				}
				lokiConn, lokiStatus := dialTailHistory(t, lokiURL, params, categorize)
				var lokiEntries []tailHistoryEntry
				if lokiConn != nil {
					defer lokiConn.Close()
					lokiEntries = readTailEntries(lokiConn, tc.want, 2*time.Second, 20*time.Second)
					if len(lokiEntries) != tc.want {
						t.Logf("Loki sent %d history entries, the case expects %d (recorded as Loki's answer)", len(lokiEntries), tc.want)
					}
				}
				for _, target := range tailHistoryTargets {
					conn, status := dialTailHistory(t, target.base, params, categorize)
					if status != lokiStatus {
						t.Errorf("%s: handshake %d, Loki %d", target.name, status, lokiStatus)
						continue
					}
					if conn == nil {
						continue
					}
					got := readTailEntries(conn, len(lokiEntries), 2*time.Second, 20*time.Second)
					_ = conn.Close()
					if fmt.Sprint(got) != fmt.Sprint(lokiEntries) {
						t.Errorf("%s: %d history entries %s\nLoki: %d %s", target.name, len(got), tailHistorySummary(got), len(lokiEntries), tailHistorySummary(lokiEntries))
					}
				}
			})
		}
	}

	// After the history the live entries follow, once each, on every tail.
	t.Run("live entry after the history", func(t *testing.T) {
		params := url.Values{"query": {query}, "limit": {"5"}}
		conns := map[string]*websocket.Conn{}
		lokiConn, _ := dialTailHistory(t, lokiURL, params, false)
		if lokiConn == nil {
			t.Fatal("Loki refused the tail")
		}
		defer lokiConn.Close()
		conns["loki"] = lokiConn
		for _, target := range tailHistoryTargets {
			conn, _ := dialTailHistory(t, target.base, params, false)
			if conn == nil {
				t.Fatalf("%s refused the tail", target.name)
			}
			defer conn.Close()
			conns[target.name] = conn
		}
		history := map[string][]tailHistoryEntry{}
		for name, conn := range conns {
			history[name] = readTailEntries(conn, 5, 2*time.Second, 20*time.Second)
		}
		live := logLine{Msg: "tail history live " + app, Level: "info"}
		at := time.Now()
		pushCustomToLoki(t, at, map[string]string{"app": app, "env": "test"}, []logLine{live})
		pushCustomToVL(t, at, map[string]string{"app": app, "env": "test"}, []logLine{live}, []string{"app", "env"})
		for name, conn := range conns {
			got := readTailEntries(conn, 1, 3*time.Second, 20*time.Second)
			seen := 0
			for _, e := range got {
				if e.line == live.Msg {
					seen++
				}
			}
			if seen != 1 || len(history[name]) != len(history["loki"]) {
				t.Errorf("%s: live line seen %d times (want 1), history %d entries (Loki %d), after: %s", name, seen, len(history[name]), len(history["loki"]), tailHistorySummary(got))
			}
		}
	})
}

// tailHistorySummary is the first and last entries of a list.
func tailHistorySummary(entries []tailHistoryEntry) string {
	if len(entries) <= 4 {
		return fmt.Sprint(entries)
	}
	return fmt.Sprint(entries[:2], " ... ", entries[len(entries)-2:])
}
