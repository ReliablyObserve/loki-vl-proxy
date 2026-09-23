//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: series-limits-and-partial-results, window-bounds-and-step-alignment,
// conformance: limits/series-limit-error, limits/drilldown-partial-with-warning, semantics/offset-sample-timestamps
// conformance: loki_api_v1_query_range, loki_api_v1_query
// TestRangeMetricCompatibilityOffsetAndSeriesLimit compares Loki and the proxy
// for `offset` (samples stay stamped at the evaluation time, not the shifted
// one) and for a grouping with many series. Over its series limit Loki fails
// the query with its max_query_series error and gives Logs Drilldown a partial
// result with a warning; the proxy does the same with -max-stats-query-series.
// The stack raises both limits well above this fixture, so the limit subtests
// run only where the proxy is configured below the fixture's cardinality (the
// default 500) and otherwise assert plain parity with Loki.
func TestRangeMetricCompatibilityOffsetAndSeriesLimit(t *testing.T) {
	app := fmt.Sprintf("e2e-align-limit-%d", time.Now().UnixNano())
	base := time.Now().UTC().Truncate(10 * time.Minute).Add(-80 * time.Minute)
	const span = time.Hour
	const widePods = 520

	type line struct {
		ts     time.Time
		labels map[string]string
		msg    string
	}
	var lines []line
	for _, s := range []struct {
		pod   string
		every time.Duration
	}{{"a", 10 * time.Second}, {"b", 25 * time.Second}} {
		for i := 0; time.Duration(i)*s.every < span; i++ {
			lines = append(lines, line{base.Add(time.Duration(i) * s.every), map[string]string{"app": app, "kind": "tick", "pod": s.pod}, "tick " + strconv.Itoa(i)})
		}
	}
	// One line per pod, 30s before a 5m mark, so every [1m] window at step 300
	// holds a sixth of the pods.
	for i := 0; i < widePods; i++ {
		pod := fmt.Sprintf("w%03d", i)
		lines = append(lines, line{base.Add(time.Duration(i%6+1)*5*time.Minute - 30*time.Second), map[string]string{"app": app, "kind": "wide", "pod": pod}, "wide " + pod})
	}

	var vlRows strings.Builder
	lokiStreams := map[string]map[string]any{}
	for _, l := range lines {
		fields := map[string]string{"_time": l.ts.Format(time.RFC3339Nano), "_msg": l.msg}
		for k, v := range l.labels {
			fields[k] = v
		}
		row, _ := json.Marshal(fields)
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		key := l.labels["kind"] + "/" + l.labels["pod"]
		if lokiStreams[key] == nil {
			lokiStreams[key] = map[string]any{"stream": l.labels, "values": [][]string{}}
		}
		lokiStreams[key]["values"] = append(lokiStreams[key]["values"].([][]string), []string{strconv.FormatInt(l.ts.UnixNano(), 10), l.msg})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,kind,pod", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	streams := make([]any, 0, len(lokiStreams))
	for _, s := range lokiStreams {
		streams = append(streams, s)
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	tick := `{app="` + app + `", kind="tick"}`
	wide := `{app="` + app + `", kind="wide"}`
	waitForFixtureOnBothBackends(t, fmt.Sprintf("app:=%q", app), `{app="`+app+`"}`, base.Add(-time.Minute), base.Add(span), len(lines))
	waitForAlignLimitLokiMetrics(t, wide, base, widePods)

	t.Run("offset", func(t *testing.T) {
		query := `sum by (pod) (count_over_time(` + tick + `[5m] offset 5m))`
		start, end := base.Add(15*time.Minute), base.Add(45*time.Minute)
		loki := slidingRangeSeries(t, lokiURL, query, start, end, 5*time.Minute, nil)
		assertSlidingParity(t, query, loki, slidingRangeSeries(t, proxyURL, query, start, end, 5*time.Minute, nil))
		at := base.Add(32 * time.Minute)
		lokiInstant := tumblingShortInstant(t, lokiURL, query, at)
		if len(lokiInstant) != 2 {
			t.Fatalf("Loki returned %d instant series, want 2: %v", len(lokiInstant), lokiInstant)
		}
		if proxyInstant := tumblingShortInstant(t, proxyURL, query, at); fmt.Sprint(proxyInstant) != fmt.Sprint(lokiInstant) {
			t.Fatalf("instant %s: proxy %v, Loki %v", query, proxyInstant, lokiInstant)
		}
	})

	// The series-limit subtests run against the proxy that keeps the built-in
	// default (500), the vmauth-fronted one: every other proxy in the stack
	// matches its Loki, whose limit is far above this fixture.
	limitProxyURL := envOr("PROXY_SERIES_LIMIT_URL", proxyVmauthURL)
	start, end := base, base.Add(30*time.Minute)
	limit := 0 // the proxy's -max-stats-query-series, read from its own answer
	for _, window := range []string{"5m", "1m"} {
		query := `sum by (pod) (count_over_time(` + wide + `[` + window + `]))`
		t.Run("series limit/"+query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, start, end, 5*time.Minute, nil)
			if len(loki) != widePods {
				t.Fatalf("Loki returned %d series, want %d", len(loki), widePods)
			}
			status, body, _ := alignLimitRangeQuery(t, limitProxyURL, query, start, end, nil)
			if status == http.StatusOK {
				// Configured above this fixture (the compose stack matches its
				// Loki): the answer must simply be Loki's.
				assertSlidingParity(t, query, loki, slidingRangeSeries(t, limitProxyURL, query, start, end, 5*time.Minute, nil))
				return
			}
			n := seriesLimitFromError(t, body)
			if n >= widePods {
				t.Fatalf("limit %d is not below the fixture's %d series, yet the query failed: %s", n, widePods, body)
			}
			limit = n
			// Loki's message, word for word: Grafana renders it, so no
			// proxy-internal text may appear in it.
			if !strings.Contains(body, "consider reducing query cardinality by adding more specific stream selectors") || strings.Contains(body, "-max-stats-query-series") {
				t.Fatalf("expected Loki's series limit message, got %s", body)
			}
		})
	}

	// Drilldown gets the partial result: at most the limit, every kept series
	// exactly as Loki computes it, and Loki's warning.
	t.Run("series limit/drilldown partial result", func(t *testing.T) {
		if limit == 0 {
			t.Skip("proxy series limit is above this fixture's cardinality")
		}
		query := `sum by (pod) (count_over_time(` + wide + `[1m]))`
		loki := slidingRangeSeries(t, lokiURL, query, start, end, 5*time.Minute, nil)
		status, body, resp := alignLimitRangeQuery(t, limitProxyURL, query, start, end, map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"})
		if status != http.StatusOK || resp["status"] != "success" {
			t.Fatalf("Drilldown: expected 200, got %d %s", status, body)
		}
		warnings, _ := resp["warnings"].([]interface{})
		wantWarning := fmt.Sprintf("maximum number of series (%d) reached for a single query; returning partial results", limit)
		if len(warnings) != 1 || warnings[0] != wantWarning {
			t.Fatalf("Drilldown: expected Loki's partial result warning, got %v", resp["warnings"])
		}
		proxy := alignLimitMatrix(t, resp)
		if len(proxy) != limit {
			t.Fatalf("Drilldown: expected %d series, got %d", limit, len(proxy))
		}
		for key, got := range proxy {
			want, ok := loki[key]
			if !ok || fmt.Sprint(sortedPoints(got)) != fmt.Sprint(sortedPoints(want)) {
				t.Errorf("Drilldown {%s}: proxy %v, Loki %v", key, sortedPoints(got), sortedPoints(want))
			}
		}
	})
}

// seriesLimitFromError reads N out of Loki's "maximum number of series (N)
// reached for a single query" error, failing the test on any other body.
func seriesLimitFromError(t *testing.T, body string) int {
	t.Helper()
	m := regexp.MustCompile(`maximum number of series \((\d+)\) reached for a single query`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("expected Loki's series limit error, got %s", body)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatalf("series limit %q: %v", m[1], err)
	}
	return n
}

func alignLimitRangeQuery(t *testing.T, baseURL, query string, start, end time.Time, headers map[string]string) (int, string, map[string]interface{}) {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"300"},
	}
	all := map[string]string{"X-Scope-OrgID": "0"}
	for k, v := range headers {
		all[k] = v
	}
	return doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), all)
}

func alignLimitMatrix(t *testing.T, resp map[string]interface{}) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		keys := make([]string, 0, len(metric))
		for k, v := range metric {
			keys = append(keys, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(keys)
		points := map[string]string{}
		values, _ := series["values"].([]interface{})
		for _, raw := range values {
			pair, _ := raw.([]interface{})
			if len(pair) == 2 {
				ts, _ := pair[0].(float64)
				points[strconv.FormatFloat(ts, 'f', -1, 64)] = fmt.Sprint(pair[1])
			}
		}
		out[strings.Join(keys, ",")] = points
	}
	return out
}

func sortedPoints(points map[string]string) []string {
	out := make([]string, 0, len(points))
	for ts, v := range points {
		out = append(out, ts+"="+v)
	}
	sort.Strings(out)
	return out
}

// waitForAlignLimitLokiMetrics waits out Loki's fresh-stream metric blank window
// with a query shape no subtest compares, so no partial answer lands in Loki's
// results cache.
func waitForAlignLimitLokiMetrics(t *testing.T, selector string, base time.Time, series int) {
	t.Helper()
	params := url.Values{
		"query": {`count(count_over_time(` + selector + `[40m]))`},
		"start": {strconv.FormatInt(base.Add(40*time.Minute).Unix(), 10)},
		"end":   {strconv.FormatInt(base.Add(40*time.Minute).Unix(), 10)},
		"step":  {"600"}, // base+40m is on this grid, so Loki's alignment keeps the time
	}
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		status, body, resp := doJSONGET(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		last = body
		if status == http.StatusOK && strings.Contains(body, `"`+strconv.Itoa(series)+`"`) && resp["warnings"] == nil {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("Loki range metrics incomplete after 3m: %s", last)
}
