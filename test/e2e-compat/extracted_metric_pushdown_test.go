//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// extractedMetricFixture is a JSON stream whose body level collides with the
// stream label level on some lines (Loki exposes the parsed value as
// level_extracted), has no stream label to collide with on others, carries the
// literal key level_extracted on one, and is unparseable on another.
func extractedMetricFixture(s0 time.Time, lines int, every time.Duration) []jsonVolumeStreamLine {
	var out []jsonVolumeStreamLine
	for i := 0; i < lines; i++ {
		ts := s0.Add(time.Duration(i) * every)
		switch i % 8 {
		case 0:
			out = append(out, jsonVolumeStreamLine{ts: ts, level: "warn", msg: fmt.Sprintf(`{"level":"error","pipeline":"logs/loki","n":%d}`, i)})
		case 1:
			out = append(out, jsonVolumeStreamLine{ts: ts, level: "info", msg: `{"level":"info","pipeline":"logs/loki"}`})
		case 2:
			out = append(out, jsonVolumeStreamLine{ts: ts, msg: `{"level":"debug","pipeline":"traces/otlp"}`})
		case 3:
			out = append(out, jsonVolumeStreamLine{ts: ts, msg: `{"level_extracted":"literal","pipeline":"logs/loki"}`})
		case 4:
			out = append(out, jsonVolumeStreamLine{ts: ts, level: "error", msg: `{"pipeline":"logs/loki"}`})
		case 5:
			out = append(out, jsonVolumeStreamLine{ts: ts, level: "warn", msg: fmt.Sprintf(`{"msg": "truncated %d`, i)})
		case 6:
			out = append(out, jsonVolumeStreamLine{ts: ts, level: "info", msg: fmt.Sprintf("plain text line %d", i)})
		default:
			out = append(out, jsonVolumeStreamLine{ts: ts, level: "warn", msg: `{"level":"fatal","pipeline":"logs/loki"}`})
		}
	}
	return out
}

// A metric filtered or grouped on a name_extracted label after `| json` is
// answered from VictoriaLogs stats buckets, never from the raw-row evaluator,
// and equals Loki's at 1 h and over 6 h30m (a range the raw evaluator cannot
// answer under a row limit of 100: v2.3.3 answered 502 `manual range metric
// row limit exceeded` for every one of these shapes on real data volumes).
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown, semantics/extracted-suffix-collision
func TestCompat_ExtractedLabelMetricPushdown(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("extracted-metric-%d", now.UnixNano())
	// 20 h back, as the other long-range fixtures: newer hours hold the
	// label fixtures, whose label inventory buckets must not hold this stream
	// (quality/labels-backfill-into-cached-bucket).
	s0 := now.Add(-20 * time.Hour).Truncate(time.Hour)
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: extractedMetricFixture(s0, 1200, 20*time.Second)})

	// Below VictoriaLogs v1.45 the pushdown is gated off (versions/stats-bucket-label-v1.45):
	// the raw evaluator answers within the default row limit and the routing
	// assertions do not apply, only the parity with Loki.
	pushdown := vlSupportsStatsRangeOffset(t)
	rowLimit := 100
	if !pushdown {
		rowLimit = 0
	}
	route, recorder := newJSONVolumeRouteProxyWithRowLimit(t, rowLimit)
	targets := map[string]string{"proxy": proxyURL, "patterns-autodetect": patternsAutodetectProxyURL, "in-process": route}
	selector := `{app="` + app + `"}`
	for _, window := range []struct {
		name       string
		start, end time.Time
	}{
		{"1h", s0.Add(time.Hour), s0.Add(2 * time.Hour)},
		{"6h+", s0.Add(-time.Minute), s0.Add(6*time.Hour + 30*time.Minute)},
	} {
		for _, tc := range []struct{ name, query string }{
			{"filter, grouped by the stream label", `sum by (level) (count_over_time(` + selector + ` | json | level_extracted!="" [1m]))`},
			{"grouped by the renamed label", `sum by (level_extracted) (count_over_time(` + selector + ` | json | drop __error__ [1m]))`},
			{"both labels", `sum by (level, level_extracted) (count_over_time(` + selector + ` | json | drop __error__ [1m]))`},
			{"ungrouped rate, equality", `sum(rate(` + selector + ` | json | level_extracted="error" [1m]))`},
			{"bytes with a regexp", `sum by (level) (bytes_over_time(` + selector + ` | json | level_extracted=~"err.*|literal" [1m]))`},
			{"negated filter", `sum by (level) (bytes_rate(` + selector + ` | json | drop __error__ | level_extracted!="error" [1m]))`},
		} {
			t.Run(window.name+"/"+tc.name, func(t *testing.T) {
				loki := slidingRangeSeries(t, lokiURL, tc.query, window.start, window.end, time.Minute, nil)
				for name, base := range targets {
					recorder.reset()
					before := parserMetricEvaluations(t, route)
					got := slidingRangeSeries(t, base, tc.query, window.start, window.end, time.Minute, nil)
					assertSlidingParity(t, tc.query+" ["+name+"]", loki, got)
					if name != "in-process" || !pushdown {
						continue
					}
					after := parserMetricEvaluations(t, route)
					raw := 0
					for _, call := range recorder.reset() {
						if strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1") {
							raw++
						}
					}
					if raw != 0 || after["vl_stats_buckets/pushdown"]-before["vl_stats_buckets/pushdown"] != 1 {
						t.Fatalf("expected one stats pushdown and no raw rows: raw requests=%d counters before=%v after=%v", raw, before, after)
					}
				}
			})
		}
	}

	// No stream label named pipeline: there is no pipeline_extracted, Loki
	// answers no series and so must the proxy, from stats, with a 200.
	t.Run("a label without a collision answers no series", func(t *testing.T) {
		query := `sum by (level) (count_over_time(` + selector + ` | json | pipeline_extracted!="" [1m]))`
		start, end := s0.Add(time.Hour), s0.Add(2*time.Hour)
		for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL, "in-process": route} {
			status, body, _ := queryRangeGET(t, base, rangeParams(query, start, end, time.Minute), map[string]string{"X-Scope-OrgID": "0"})
			if status != http.StatusOK || strings.Contains(body, `"values"`) {
				t.Fatalf("%s: expected 200 with no series, got %d %s", name, status, body)
			}
		}
	})
}

// vlSupportsStatsRangeOffset reports whether the VictoriaLogs under test is
// v1.45 or newer (the stats_query_range offset arg), from its metrics.
func vlSupportsStatsRangeOffset(t *testing.T) bool {
	t.Helper()
	m := regexp.MustCompile(`short_version="v(\d+)\.(\d+)\.`).FindStringSubmatch(scrapeProxyMetrics(t, vlURL))
	if m == nil {
		return true
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	return major > 1 || minor >= 45
}
