//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// jsonErrorFilterSplitFixture holds the lines a `| json | __error__=""`
// metric must count like Loki's parser: valid JSON of two statuses, some with
// a level stream label, nested objects and arrays, lines VictoriaLogs' parser
// accepts and Loki's rejects (NaN, a leading dot, a plus sign, an invalid key
// escape), JSON with trailing text (Loki accepts it), truncated JSON, an empty
// object, a line with leading whitespace, plain and logfmt text.
func jsonErrorFilterSplitFixture(s0 time.Time) []jsonVolumeStreamLine {
	shapes := []struct{ level, msg string }{
		{"info", `{"status":"200","method":"GET","n":%d}`},
		{"", `{"status":"500","method":"POST","n":%d}`},
		{"", `{"status":"200","nested":{"a":{"b":%d}},"arr":[1,2]}`},
		{"warn", `{"status":"500","level":"error","n":%d}`},
		{"", `{"status":"200","ratio":NaN,"n":%d}`},
		{"", `{"status":"500","v":.5,"n":%d}`},
		{"", `{"status":"200","v":+1,"n":%d}`},
		{"", `{"st\qatus":"500","n":%d}`},
		{"", `{"status":"500","n":%d} trailing`},
		{"", `{"status":"200","msg":"truncated %d`},
		{"", `{}`},
		{"", `  {"status":"500","n":%d}`},
		{"", `plain text %d`},
		{"info", `status=200 n=%d`},
	}
	var lines []jsonVolumeStreamLine
	for i := 0; i < 120; i++ {
		shape := shapes[i%len(shapes)]
		msg := shape.msg
		if strings.Contains(msg, "%d") {
			msg = fmt.Sprintf(msg, i)
		}
		lines = append(lines, jsonVolumeStreamLine{ts: s0.Add(time.Duration(i) * 5 * time.Second), level: shape.level, msg: msg})
	}
	return lines
}

// A `| json | __error__=""` metric is answered from one VictoriaLogs stats
// scan over the lines VictoriaLogs parses (a second scan once a probe finds a
// line Loki's parser rejects), plus a raw read of only the buckets holding the
// other brace-opened lines, and matches Loki: count, rate and bytes, without
// grouping, by a stream label and by a parsed label. On v2.14.2 these shapes
// read every selected line as a raw row (and failed at 24 h on the row and
// byte limits).
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestRangeMetricCompatibilityJSONErrorFilterSplit(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("json-error-split-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: jsonErrorFilterSplitFixture(s0)})

	route, recorder := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "in-process": route}
	start, end := s0.Add(-2*time.Minute), s0.Add(11*time.Minute)
	selector := `{app="` + app + `"}`
	for _, tc := range []struct {
		name, query string
		minimum     int // Loki series expected for the fixture
	}{
		{"ungrouped count", `sum(count_over_time(` + selector + ` | json | __error__="" [1m]))`, 1},
		{"count by stream label", `sum by (level) (count_over_time(` + selector + ` | json | __error__="" [1m]))`, 2},
		{"count by parsed label", `sum by (status) (count_over_time(` + selector + ` | json | __error__="" [1m]))`, 2},
		{"rate by parsed label", `sum by (status) (rate(` + selector + ` | json | __error__="" [2m]))`, 2},
		{"bytes by stream label", `sum by (level) (bytes_over_time(` + selector + ` | json | __error__="" [1m]))`, 2},
		{"ungrouped bytes rate", `sum(bytes_rate(` + selector + ` | json | __error__="" [1m]))`, 1},
		{"count by service_name", `sum by (service_name) (count_over_time(` + selector + ` | json | __error__="" [1m]))`, 1},
		{"count by three labels", `sum by (level, service_name, status) (count_over_time(` + selector + ` | json | __error__="" [1m]))`, 2},
		{"bytes by two labels with a filter", `sum by (level, status) (bytes_over_time(` + selector + ` | json | __error__="" | status="500" [1m]))`, 1},
		{"count with a label filter", `sum by (level) (count_over_time(` + selector + ` | json | __error__="" | status="500" [1m]))`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, nil)
			if len(loki) < tc.minimum {
				t.Fatalf("Loki sanity: expected at least %d series, got %d: %v", tc.minimum, len(loki), loki)
			}
			for name, base := range targets {
				recorder.reset()
				before := parserMetricEvaluations(t, route)
				assertSlidingParity(t, tc.query+" ["+name+"]", loki, slidingRangeSeries(t, base, tc.query, start, end, time.Minute, nil))
				if name != "in-process" {
					continue
				}
				after := parserMetricEvaluations(t, route)
				stats, raw := 0, 0
				for _, call := range recorder.reset() {
					switch {
					case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
						stats++
					case strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1"):
						raw++
						if !strings.Contains(call, "__vlp:* or ") {
							t.Fatalf("raw read %q is not limited to the lines VictoriaLogs cannot vouch for", call)
						}
					}
				}
				// The fixture holds a NaN line: the hazard probe finds it and the
				// stats scan runs again with such lines read as raw rows.
				if stats != 2 || raw == 0 || after["vl_stats_buckets/pushdown"]-before["vl_stats_buckets/pushdown"] != 1 {
					t.Fatalf("expected two stats scans and a bounded raw read: stats=%d raw=%d", stats, raw)
				}
			}
		})
	}
}

// The series limit counts the series Loki's answer holds, never the groups
// VictoriaLogs makes: it emits one for every row reaching its stats pipe,
// including the lines a label filter drops and the lines its parser rejects
// while Loki's accepts. A filter that keeps 2 of 20 paths and exactly as many
// paths as the limit both answer as Loki does, while 20 paths under a limit of
// 5 are Loki's series limit error (the split hands a read of more than twice
// the limit in groups to the row evaluator, which ends with that error).
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestRangeMetricCompatibilityJSONErrorFilterSplitSeriesLimit(t *testing.T) {
	const limit = 5
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	filtered := fmt.Sprintf("json-error-split-filtered-%d", now.UnixNano())
	exact := fmt.Sprintf("json-error-split-exact-%d", now.UnixNano())
	var filteredLines, exactLines []jsonVolumeStreamLine
	for i := 0; i < 20; i++ {
		status := "200"
		if i < 2 {
			status = "500"
		}
		filteredLines = append(filteredLines, jsonVolumeStreamLine{ts: s0.Add(time.Duration(i) * 5 * time.Second), msg: fmt.Sprintf(`{"path":"/p%d","status":"%s"}`, i, status)})
	}
	for i := 0; i < limit; i++ {
		exactLines = append(exactLines, jsonVolumeStreamLine{ts: s0.Add(time.Duration(i) * 5 * time.Second), msg: fmt.Sprintf(`{"path":"/p%d"}`, i)})
	}
	// Trailing text: Loki's parser accepts the line, VictoriaLogs' does not.
	exactLines = append(exactLines, jsonVolumeStreamLine{ts: s0.Add(30 * time.Second), msg: `{"path":"/p0"} trailing`})
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{filtered: filteredLines, exact: exactLines})

	route, _ := newJSONVolumeRouteProxyWithLimits(t, 0, limit)
	start, end := s0.Add(-2*time.Minute), s0.Add(4*time.Minute)
	for _, tc := range []struct{ name, query string }{
		{"filter keeps few of many groups", `sum by (path) (count_over_time({app="` + filtered + `"} | json | __error__="" | status="500" [1m]))`},
		{"filtered bytes", `sum by (path) (bytes_over_time({app="` + filtered + `"} | json | __error__="" | status="500" [1m]))`},
		{"unparseable line beside exactly limit paths", `sum by (path) (count_over_time({app="` + exact + `"} | json | __error__="" [1m]))`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, nil)
			if len(loki) == 0 || len(loki) > limit {
				t.Fatalf("Loki sanity: expected 1 to %d series, got %d: %v", limit, len(loki), loki)
			}
			assertSlidingParity(t, tc.query+" [limit "+strconv.Itoa(limit)+"]", loki, slidingRangeSeries(t, route, tc.query, start, end, time.Minute, nil))
		})
	}

	t.Run("20 paths over the limit are Loki's series limit error", func(t *testing.T) {
		query := `sum by (path) (count_over_time({app="` + filtered + `"} | json | __error__="" | status=~"200|500" [1m]))`
		// Loki's own limit is higher than the one set on the proxy: it holds all 20.
		if loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil); len(loki) != 20 {
			t.Fatalf("Loki sanity: expected 20 series, got %d: %v", len(loki), loki)
		}
		// A step of the window: every evaluation point sees lines, as Loki's
		// series limit is the count of series the answer holds.
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		status, body, _ := doJSONGET(t, route+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if status != http.StatusBadRequest || !strings.Contains(body, "maximum number of series") {
			t.Fatalf("20 series with limit %d: status %d body %s", limit, status, body)
		}
	})
}

// detected_level is the level Loki derives at ingest, normalized, from a stream
// label or from the JSON body. The split groups it from the stored level or the
// body's level key and must answer like Loki, VictoriaLogs groups (one per
// spelling) notwithstanding: the spellings of one level are one series, and the
// series limit counts series. The body variant also holds lines only Loki's
// parser accepts (nested deeper than VictoriaLogs reads), which the split reads
// as raw rows.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestRangeMetricCompatibilityJSONErrorFilterSplitDetectedLevel(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	body := fmt.Sprintf("json-error-split-lvl-body-%d", now.UnixNano())
	stream := fmt.Sprintf("json-error-split-lvl-stream-%d", now.UnixNano())
	spellings := []string{"info", "INFO", "Info", "inf", "information", "warn", "WARN", "warning", "error", "ERR", "debug"}
	var bodyLines, streamLines, rawLines []jsonVolumeStreamLine
	for i := 0; i < 66; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		level := spellings[i%len(spellings)]
		bodyLines = append(bodyLines, jsonVolumeStreamLine{ts: ts, msg: fmt.Sprintf(`{"level":%q,"n":%d}`, level, i)})
		streamLines = append(streamLines, jsonVolumeStreamLine{ts: ts, level: level, msg: fmt.Sprintf(`{"n":%d}`, i)})
		// Nested deeper than VictoriaLogs' parser reads (300 levels): Loki's
		// parser accepts the line, so the split reads it as a raw row.
		rawLines = append(rawLines, jsonVolumeStreamLine{ts: ts, msg: fmt.Sprintf(`{"level":%q,"deep":%s1%s,"n":%d}`, level, strings.Repeat(`{"a":`, 320), strings.Repeat(`}`, 320), i)})
	}
	// A line without any level, the last of the fixture.
	bodyLines = append(bodyLines, jsonVolumeStreamLine{ts: s0.Add(331 * time.Second), msg: `{"n":-1}`})
	streamLines = append(streamLines, jsonVolumeStreamLine{ts: s0.Add(331 * time.Second), msg: `{"n":-1}`})
	raw := fmt.Sprintf("json-error-split-lvl-raw-%d", now.UnixNano())
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{body: bodyLines, stream: streamLines, raw: rawLines})

	route, _ := newJSONVolumeRouteProxyWithLimits(t, 0, 50)
	// 11 spellings and the line without a level are 12 groups past twice the
	// limit of 5, but 5 series: the row evaluator answers that read.
	fallback, fallbackRecorder := newJSONVolumeRouteProxyWithLimits(t, 0, 5)
	targets := map[string]string{"proxy": proxyURL, "in-process": route, "in-process limit 5": fallback}
	start, end := s0.Add(-2*time.Minute), s0.Add(9*time.Minute)
	for _, tc := range []struct{ name, app string }{
		{"level in the body", body},
		{"level as a stream label with mixed spellings", stream},
		{"level in the body of lines nested deeper than VictoriaLogs reads", raw},
	} {
		for _, function := range []string{"count_over_time", "bytes_over_time"} {
			query := `sum by (detected_level) (` + function + `({app="` + tc.app + `"} | json | __error__="" [1m]))`
			t.Run(tc.name+"/"+function, func(t *testing.T) {
				loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
				t.Logf("Loki: %d series", len(loki))
				if len(loki) < 2 {
					t.Fatalf("Loki sanity: expected several detected_level series, got %d: %v", len(loki), loki)
				}
				for name, base := range targets {
					fallbackRecorder.reset()
					assertSlidingParity(t, query+" ["+name+"]", loki, slidingRangeSeries(t, base, query, start, end, time.Minute, nil))
					if name != "in-process limit 5" {
						continue
					}
					rowReads := 0 // a read of the selected rows, not the split's raw candidates
					for _, call := range fallbackRecorder.reset() {
						if strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1") && !strings.Contains(call, "__vlp:*") {
							rowReads++
						}
					}
					// The raw app reads its lines as raw rows only: one group.
					if rowReads == 0 && tc.app != raw {
						t.Errorf("limit 5: the row evaluator did not answer the read of past twice the limit in groups")
					}
				}
			})
		}
	}
}
