//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRangeMetricCompatibilityTumblingAndShortWindows compares Loki and the
// proxy for log range metrics whose range equals the step (tumbling) or is
// shorter than it, and for grouping labels that some or all series lack. The
// fixture writes a line exactly on every window edge, so a bucket labelled at
// its start instead of Loki's (T-range, T] evaluation window shows up as a
// shifted or missing first sample.
func TestRangeMetricCompatibilityTumblingAndShortWindows(t *testing.T) {
	app := fmt.Sprintf("e2e-tumbling-short-%d", time.Now().UnixNano())
	selector := `{app="` + app + `"}`
	base := time.Now().UTC().Truncate(5 * time.Minute).Add(-25 * time.Minute)
	const span = 20 * time.Minute

	type stream struct {
		pod   string // empty: the stream has no pod label
		every time.Duration
		line  func(int) string
	}
	streams := []stream{
		{"a", 10 * time.Second, func(i int) string { return fmt.Sprintf("level=info msg=%q pod=a", "tick "+strings.Repeat("a", i%5)) }},
		{"b", 20 * time.Second, func(i int) string { return fmt.Sprintf("level=error msg=%q pod=b", "tock "+strings.Repeat("b", i%3)) }},
		{"", 30 * time.Second, func(i int) string { return fmt.Sprintf("msg=%q", "no pod "+strconv.Itoa(i)) }},
	}

	// Identical lines and timestamps on both backends. level lives only in the
	// line; detected_level=unknown is stored explicitly on both (Loki structured
	// metadata, a VictoriaLogs stream field) so raw stream series carry the
	// same labels.
	var vlRows strings.Builder
	lokiStreams := make([]any, 0, len(streams))
	total := 0
	for _, s := range streams {
		labels := map[string]string{"app": app}
		if s.pod != "" {
			labels["pod"] = s.pod
		}
		var values [][]any
		for i := 0; time.Duration(i)*s.every < span; i++ {
			ts := base.Add(time.Duration(i) * s.every)
			msg := s.line(i)
			values = append(values, []any{strconv.FormatInt(ts.UnixNano(), 10), msg, map[string]string{"detected_level": "unknown"}})
			fields := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": msg, "detected_level": "unknown"}
			for k, v := range labels {
				fields[k] = v
			}
			row, _ := json.Marshal(fields)
			vlRows.Write(row)
			vlRows.WriteByte('\n')
			total++
		}
		lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,pod,detected_level", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	// Both backends hold every line before any comparison.
	waitForFixtureOnBothBackends(t, fmt.Sprintf("app:=%q", app), selector, base.Add(-time.Minute), base.Add(span), total)
	waitForTumblingShortLokiMetrics(t, selector, base)

	start, end := base, base.Add(15*time.Minute)
	const pattern = `| pattern "level=<level> <_>"`
	cases := []struct {
		query  string
		step   time.Duration
		series int
	}{
		{`count_over_time(` + selector + `[5m])`, 5 * time.Minute, 3},
		{`sum by (pod) (count_over_time(` + selector + `[5m]))`, 5 * time.Minute, 3},
		{`sum(count_over_time(` + selector + `[5m]))`, 5 * time.Minute, 1},
		{`rate(` + selector + `[5m])`, 5 * time.Minute, 3},
		{`bytes_over_time(` + selector + `[5m])`, 5 * time.Minute, 3},
		{`rate(` + selector + `[1m])`, 5 * time.Minute, 3},
		{`count_over_time(` + selector + `[1m])`, 5 * time.Minute, 3},
		{`sum by (level) (count_over_time(` + selector + ` ` + pattern + ` [1m]))`, 5 * time.Minute, 3},
		{`rate(` + selector + `[5m])`, time.Minute, 3},
		{`sum by (level) (count_over_time(` + selector + `[5m]))`, 5 * time.Minute, 1},
		{`sum by (app, pod) (count_over_time(` + selector + `[5m]))`, 5 * time.Minute, 3},
	}
	for _, tc := range cases {
		t.Run(tc.query+"@"+tc.step.String(), func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, tc.step, nil)
			if len(loki) != tc.series {
				t.Fatalf("Loki returned %d series, want %d: %v", len(loki), tc.series, loki)
			}
			// Every stream writes a line on start, so every step has a sample.
			wantPoints := int(end.Sub(start)/tc.step) + 1
			for key, points := range loki {
				if len(points) != wantPoints {
					t.Fatalf("Loki series {%s} has %d samples, want %d step-aligned samples: %v", key, len(points), wantPoints, points)
				}
				for ts := range points {
					if sec, _ := strconv.ParseInt(ts, 10, 64); sec%int64(tc.step/time.Second) != 0 || sec < start.Unix() || sec > end.Unix() {
						t.Fatalf("Loki series {%s} sample at %s is outside the step grid [%d, %d]", key, ts, start.Unix(), end.Unix())
					}
				}
			}
			proxy := slidingRangeSeries(t, proxyURL, tc.query, start, end, tc.step, nil)
			assertSlidingParity(t, tc.query, loki, proxy)
		})
	}

	// Instant windows are (time-range, time]: at start a stream counts only its
	// line on the edge, one range later the edge line of start drops out.
	for _, at := range []time.Time{start, start.Add(5 * time.Minute)} {
		query := `sum by (pod) (count_over_time(` + selector + `[5m]))`
		t.Run(fmt.Sprintf("instant/%s@%d", query, at.Unix()), func(t *testing.T) {
			loki := tumblingShortInstant(t, lokiURL, query, at)
			if len(loki) != 3 {
				t.Fatalf("Loki returned %d series, want 3: %v", len(loki), loki)
			}
			proxy := tumblingShortInstant(t, proxyURL, query, at)
			if fmt.Sprint(proxy) != fmt.Sprint(loki) {
				t.Fatalf("instant %s at %d: proxy %v, Loki %v", query, at.Unix(), proxy, loki)
			}
		})
	}
}

// tumblingShortInstant runs an instant query and returns series labels → value,
// failing unless the response is a successful vector without warnings.
func tumblingShortInstant(t *testing.T, baseURL, query string, at time.Time) map[string]string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", baseURL, query, status, body)
	}
	data := extractMap(resp, "data")
	if data["resultType"] != "vector" {
		t.Fatalf("%s %s: expected vector, got %s", baseURL, query, body)
	}
	out := map[string]string{}
	for _, item := range extractArray(data, "result") {
		sample, _ := item.(map[string]interface{})
		metric, _ := json.Marshal(sample["metric"])
		value, _ := sample["value"].([]interface{})
		if len(value) != 2 {
			t.Fatalf("%s %s: malformed sample %v", baseURL, query, item)
		}
		out[string(metric)] = fmt.Sprint(value[1])
	}
	return out
}

// waitForTumblingShortLokiMetrics waits out Loki's fresh-stream metric blank
// window: range metrics stay empty until every stream is visible. The probe
// uses its own window and step so no partial answer lands in Loki's results
// cache for the compared queries.
func waitForTumblingShortLokiMetrics(t *testing.T, selector string, base time.Time) {
	t.Helper()
	params := url.Values{
		"query": {`sum by (pod) (count_over_time(` + selector + `[10m]))`},
		"start": {strconv.FormatInt(base.Add(10*time.Minute).Unix(), 10)},
		"end":   {strconv.FormatInt(base.Add(20*time.Minute).Unix(), 10)},
		"step":  {"600"},
	}
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		status, body, resp := doJSONGET(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		last = body
		if status == http.StatusOK && resp["warnings"] == nil {
			result := extractArray(extractMap(resp, "data"), "result")
			complete := len(result) == 3
			for _, item := range result {
				series, _ := item.(map[string]interface{})
				values, _ := series["values"].([]interface{})
				complete = complete && len(values) == 2
			}
			if complete {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("Loki range metrics incomplete after 3m: %s", last)
}
