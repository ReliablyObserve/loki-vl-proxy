//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func rangeParams(query string, start, end time.Time, step time.Duration) url.Values {
	return url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	}
}

// parserMetricEvaluations reads loki_vl_proxy_parser_metric_evaluations_total
// from a proxy's /metrics, keyed "evaluator/reason".
func parserMetricEvaluations(t *testing.T, baseURL string) map[string]int {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, baseURL+"/metrics", "", nil)
	if status != http.StatusOK {
		t.Fatalf("/metrics: %d %s", status, body)
	}
	out := map[string]int{}
	for _, m := range regexp.MustCompile(`(?m)^loki_vl_proxy_parser_metric_evaluations_total\{evaluator="([^"]+)",reason="([^"]+)"\} (\d+)$`).FindAllStringSubmatch(string(body), -1) {
		out[m[1]+"/"+m[2]], _ = strconv.Atoi(m[3])
	}
	return out
}

// jsonFilterFixture is a JSON stream in the shape of the OTel collector logs
// of the e2e generator: pipeline and level are keys of the body,
// service_version is a body key on some lines and structured metadata
// (stored by VictoriaLogs as service.version) on others, one line in eight
// is unparseable and one is plain text.
func jsonFilterFixture(s0 time.Time) []jsonVolumeStreamLine {
	var lines []jsonVolumeStreamLine
	for i := 0; i < 80; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		switch i % 8 {
		case 0:
			lines = append(lines, jsonVolumeStreamLine{ts: ts, level: "info", msg: fmt.Sprintf(`{"level":"info","service_version":"0.96.0","pipeline":"logs/loki","n":%d}`, i)})
		case 1: // the body's level collides with the stream label: level_extracted
			lines = append(lines, jsonVolumeStreamLine{ts: ts, level: "warn", msg: `{"level":"error","service_version":"0.96.0","pipeline":"logs/loki"}`})
		case 2:
			lines = append(lines, jsonVolumeStreamLine{ts: ts, msg: `{"level":"error","service_version":"0.95.0","pipeline":"logs/loki"}`})
		case 3: // no service_version key
			lines = append(lines, jsonVolumeStreamLine{ts: ts, msg: `{"level":"debug","pipeline":"traces/otlp"}`})
		case 4: // unparseable, no key before the error
			lines = append(lines, jsonVolumeStreamLine{ts: ts, msg: fmt.Sprintf(`{"msg": "truncated %d`, i)})
		case 5:
			lines = append(lines, jsonVolumeStreamLine{ts: ts, msg: fmt.Sprintf("plain text line %d", i)})
		case 6: // structured metadata wins over the body's key (service_version_extracted)
			lines = append(lines, jsonVolumeStreamLine{ts: ts, level: "info", msg: `{"pipeline":"logs/loki","service_version":"9.9.9"}`,
				meta: map[string]string{"service_version": "0.96.0"}, vlMeta: map[string]string{"service.version": "0.96.0"}})
		default:
			lines = append(lines, jsonVolumeStreamLine{ts: ts, msg: `{"level":"warn","pipeline":"logs/loki"}`,
				meta: map[string]string{"service_version": "0.95.0"}, vlMeta: map[string]string{"service.version": "0.95.0"}})
		}
	}
	return lines
}

// Grafana Explore's logs volume for a `| json` query filtered on
// service_version and pipeline, a user's grouped and ungrouped sums over `| json`
// with a label filter and no `drop __error__`, and Logs Drilldown field
// breakdowns on underscore-named labels must match Loki and be answered from
// VictoriaLogs stats buckets, never from raw rows; the spelling probe keeps
// the exact raw evaluator for a stream in which a nested object yields the
// filtered label in Loki. On v1.91.0 every one of these shapes fell to the
// raw-row evaluator (`manual range metric row limit exceeded` at 3 h).
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-underscore-label, semantics/json-filter-pushdown-without-error-drop, semantics/json-filter-pushdown-ungrouped-sum, semantics/json-filter-pushdown-translated-label, semantics/json-label-spelling-probe
func TestRangeMetricCompatibilityJSONFilterPushdown(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("json-filter-%d", now.UnixNano())
	nested := fmt.Sprintf("json-filter-nested-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	fixture := map[string][]jsonVolumeStreamLine{app: jsonFilterFixture(s0)}
	for i := 0; i < 60; i++ {
		ts := s0.Add(time.Duration(i) * 10 * time.Second)
		fixture[nested] = append(fixture[nested],
			jsonVolumeStreamLine{ts: ts, level: "info", msg: `{"service_version":"0.96.0","pipeline":"logs/loki"}`},
			// Loki flattens the nested object to service_version; unpack_json spells it service.version.
			jsonVolumeStreamLine{ts: ts.Add(5 * time.Second), msg: `{"service":{"version":"0.96.0"},"pipeline":"logs/loki","level":"warn"}`},
		)
	}
	ingestJSONVolumeFixture(t, fixture)

	route, recorder := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "patterns-autodetect": patternsAutodetectProxyURL, "in-process": route}
	start, end := s0.Add(-2*time.Minute), s0.Add(9*time.Minute)
	for _, tc := range []struct {
		name, query string
		step        time.Duration
		pushed      bool
		guards      int
		minimum     int // Loki series expected for the fixture
	}{
		// Grafana Explore's logs volume for the filtered query.
		{"explore volume with filters", `sum by (level, detected_level) (count_over_time({app="` + app + `"} | json | service_version=` + "`0.96.0`" + ` | pipeline=` + "`logs/loki`" + ` | drop __error__[1m]))`, time.Minute, true, 1, 2},
		{"explore volume with filters, 2m window", `sum by (level, detected_level) (count_over_time({app="` + app + `"} | json | pipeline="logs/loki" | drop __error__[2m]))`, 30 * time.Second, true, 1, 3},
		// A user's metric queries without `drop __error__`.
		{"grouped sum without error drop", `sum by (level) (count_over_time({app="` + app + `"} | json | pipeline="logs/loki" [1m]))`, time.Minute, true, 2, 3},
		{"ungrouped sum without error drop", `sum(count_over_time({app="` + app + `"} | json | pipeline="logs/loki" [1m]))`, time.Minute, true, 2, 1},
		{"ungrouped rate with regexp filter", `sum(rate({app="` + app + `"} | json | pipeline=~"logs/.*" [1m]))`, time.Minute, true, 2, 1},
		// Logs Drilldown field breakdowns.
		// 0.96.0 and 0.95.0 only: the body's 9.9.9 is service_version_extracted.
		{"field breakdown on structured metadata", `sum by (service_version) (count_over_time({app="` + app + `"} | json | drop __error__ | service_version!="" [1m]))`, time.Minute, true, 1, 2},
		{"field breakdown without error drop", `sum by (pipeline) (count_over_time({app="` + app + `"} | json | pipeline!="" [1m]))`, time.Minute, true, 2, 2},
		{"bytes volume with regexp filter", `sum by (level, detected_level) (bytes_over_time({app="` + app + `"} | json | service_version=~"0\\.9[0-9]\\.0" | drop __error__[1m]))`, time.Minute, true, 1, 3},
		// The spelling probe: a nested object yields service_version in Loki.
		{"nested key keeps the raw evaluator", `sum by (level, detected_level) (count_over_time({app="` + nested + `"} | json | service_version=` + "`0.96.0`" + ` | pipeline=` + "`logs/loki`" + ` | drop __error__[1m]))`, time.Minute, false, 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, tc.step, nil)
			if len(loki) < tc.minimum {
				t.Fatalf("Loki sanity: expected at least %d series, got %d: %v", tc.minimum, len(loki), loki)
			}
			for name, base := range targets {
				recorder.reset()
				before := parserMetricEvaluations(t, route)
				assertSlidingParity(t, tc.query+" ["+name+"]", loki, slidingRangeSeries(t, base, tc.query, start, end, tc.step, nil))
				if name != "in-process" {
					continue
				}
				after := parserMetricEvaluations(t, route)
				stats, guards, raw := 0, 0, 0
				for _, call := range recorder.reset() {
					switch {
					case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
						stats++
					case strings.HasPrefix(call, "/select/logsql/query ") && strings.HasSuffix(call, " | limit 1"):
						guards++
					case strings.HasPrefix(call, "/select/logsql/query "):
						raw++
					}
				}
				rawDelta := after["raw_rows/probe"] - before["raw_rows/probe"]
				statsDelta := after["vl_stats_buckets/pushdown"] - before["vl_stats_buckets/pushdown"]
				if tc.pushed && (stats != 1 || guards != tc.guards || raw != 0 || rawDelta != 0 || statsDelta != 1) {
					t.Fatalf("expected %d probes and one stats_query_range request without raw rows: stats=%d guards=%d raw=%d counter raw_rows+%d vl_stats_buckets+%d", tc.guards, stats, guards, raw, rawDelta, statsDelta)
				}
				// The bucket query runs beside the probe and is cancelled when the
				// probe finds a line the parsers read differently; the answer must
				// come from the raw evaluator.
				if !tc.pushed && (guards != tc.guards || raw != 1 || rawDelta != 1 || statsDelta != 0) {
					t.Fatalf("expected the probe to keep the raw evaluator: stats=%d guards=%d raw=%d counter raw_rows+%d vl_stats_buckets+%d", stats, guards, raw, rawDelta, statsDelta)
				}
			}
		})
	}

	// A line holding the filtered key before its syntax error passes the
	// filter with a parser error; Loki fails such a query, and so must the
	// proxy, from the raw evaluator the partial-parse probe keeps.
	t.Run("key before syntax error is a pipeline error", func(t *testing.T) {
		partial := fmt.Sprintf("json-filter-partial-%d", now.UnixNano())
		var lines []jsonVolumeStreamLine
		for i := 0; i < 30; i++ {
			ts := s0.Add(time.Duration(i) * 10 * time.Second)
			lines = append(lines,
				jsonVolumeStreamLine{ts: ts, level: "info", msg: `{"pipeline":"logs/loki","n":1}`},
				jsonVolumeStreamLine{ts: ts.Add(5 * time.Second), msg: fmt.Sprintf(`{"pipeline":"logs/loki","msg": truncated %d`, i)},
			)
		}
		ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{partial: lines})
		query := `sum by (level) (count_over_time({app="` + partial + `"} | json | pipeline="logs/loki" [1m]))`
		lokiStatus, lokiBody, _ := queryRangeGET(t, lokiURL, rangeParams(query, start, end, time.Minute), map[string]string{"X-Scope-OrgID": "0"})
		if lokiStatus != http.StatusBadRequest || !strings.Contains(lokiBody, "pipeline error") {
			t.Fatalf("Loki sanity: expected 400 pipeline error, got %d %s", lokiStatus, lokiBody)
		}
		for name, base := range targets {
			status, body, _ := queryRangeGET(t, base, rangeParams(query, start, end, time.Minute), map[string]string{"X-Scope-OrgID": "0"})
			if status != http.StatusBadRequest || !strings.Contains(body, "pipeline error") {
				t.Fatalf("%s: expected Loki's 400 pipeline error, got %d %s", name, status, body)
			}
		}
	})

	// A line whose filtered label is structured metadata while its body is
	// not JSON passes the filter with a parser error in Loki (400); the
	// stored-field probe finds it on real VictoriaLogs and keeps the raw
	// evaluator, which answers as Loki does.
	t.Run("stored filter label on an unparseable line is a pipeline error", func(t *testing.T) {
		storedApp := fmt.Sprintf("json-filter-stored-%d", now.UnixNano())
		var lines []jsonVolumeStreamLine
		for i := 0; i < 30; i++ {
			ts := s0.Add(time.Duration(i) * 10 * time.Second)
			lines = append(lines,
				jsonVolumeStreamLine{ts: ts, level: "info", msg: `{"pipeline":"logs/loki","n":1}`},
				jsonVolumeStreamLine{ts: ts.Add(5 * time.Second), msg: fmt.Sprintf("plain text %d", i), meta: map[string]string{"pipeline": "logs/loki"}, vlMeta: map[string]string{"pipeline": "logs/loki"}},
			)
		}
		ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{storedApp: lines})
		query := `sum by (level) (count_over_time({app="` + storedApp + `"} | json | pipeline="logs/loki" [1m]))`
		lokiStatus, lokiBody, _ := queryRangeGET(t, lokiURL, rangeParams(query, start, end, time.Minute), map[string]string{"X-Scope-OrgID": "0"})
		if lokiStatus != http.StatusBadRequest || !strings.Contains(lokiBody, "pipeline error") {
			t.Fatalf("Loki sanity: expected 400 pipeline error, got %d %s", lokiStatus, lokiBody)
		}
		for name, base := range targets {
			recorder.reset()
			before := parserMetricEvaluations(t, route)
			status, body, _ := queryRangeGET(t, base, rangeParams(query, start, end, time.Minute), map[string]string{"X-Scope-OrgID": "0"})
			if status != http.StatusBadRequest || !strings.Contains(body, "pipeline error") {
				t.Fatalf("%s: expected Loki's 400 pipeline error, got %d %s", name, status, body)
			}
			if name != "in-process" {
				continue
			}
			after := parserMetricEvaluations(t, route)
			guards, raw := 0, 0
			for _, call := range recorder.reset() {
				switch {
				case strings.HasPrefix(call, "/select/logsql/query ") && strings.HasSuffix(call, " | limit 1"):
					guards++
				case strings.HasPrefix(call, "/select/logsql/query "):
					raw++
				}
			}
			if guards != 2 || raw != 1 || after["raw_rows/probe"]-before["raw_rows/probe"] != 1 {
				t.Fatalf("expected the stored-field probe to keep the raw evaluator: guards=%d raw=%d counter raw_rows/probe+%d", guards, raw, after["raw_rows/probe"]-before["raw_rows/probe"])
			}
		}
	})
}
