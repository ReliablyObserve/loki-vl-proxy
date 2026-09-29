//go:build e2e

package e2e_compat

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A Logs Drilldown field breakdown asks VictoriaLogs to parse only the lines
// that can hold the field (a stored value, the field's word in the line, or a
// JSON \u escape that may spell the key) and to keep a stored value over the
// line's key. The answer must stay Loki's, for the `| json` breakdown and the
// `| json field="[\"field\"]"` form alike: structured metadata wins over a
// body key of the same name (Loki renames the parsed one to _extracted), a key
// spelled with an escape still counts, and lines without the field, with an
// empty value, a key that only contains the name, or no JSON do not.
//
// conformance: limits/drilldown-breakdown-exact, parser-json, loki_api_v1_query_range
func TestRangeMetricCompatibilityDrilldownFieldBreakdown(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("drilldown-field-breakdown-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	var lines []jsonVolumeStreamLine
	for i := 0; i < 60; i++ { // one tick every 10s for 10 minutes
		tick := s0.Add(time.Duration(i) * 10 * time.Second)
		at := func(k int) time.Time { return tick.Add(time.Duration(k) * time.Second) }
		meta := func(pipeline, user string) map[string]string {
			return map[string]string{"pipeline": pipeline, "user_id": user}
		}
		lines = append(lines,
			jsonVolumeStreamLine{ts: at(0), msg: fmt.Sprintf(`{"pipeline":"p-body","user_id":"u-body","n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(1), msg: fmt.Sprintf(`{"pipeline":"p-escaped","user_id":"u-escaped","n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(2), msg: fmt.Sprintf(`{"msg":"stored only","n":%d}`, i), meta: meta("p-meta", "u-meta"), vlMeta: meta("p-meta", "u-meta")},
			jsonVolumeStreamLine{ts: at(3), msg: fmt.Sprintf(`{"pipeline":"p-shadowed","user_id":"u-shadowed","n":%d}`, i), meta: meta("p-stored", "u-stored"), vlMeta: meta("p-stored", "u-stored")},
			jsonVolumeStreamLine{ts: at(4), msg: fmt.Sprintf(`{"pipeline" : "p-spaced", "user_id" : "u-spaced", "n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(5), msg: fmt.Sprintf(`{"pipeline":"","user_id":"","n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(6), msg: fmt.Sprintf(`{"pipelines":"p-other","my_user_id":"u-other","n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(7), msg: fmt.Sprintf(`pipeline=p-logfmt user_id=u-logfmt n=%d`, i)},
			jsonVolumeStreamLine{ts: at(8), msg: fmt.Sprintf(`{"pipeline":%d,"user_id":%d}`, i%3, i%2)},
		)
	}
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: lines})

	route, recorder := newJSONVolumeRouteProxy(t)
	drilldown := map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}
	start, end := s0.Add(-2*time.Minute), s0.Add(12*time.Minute)
	for _, tc := range []struct{ field, query string }{
		{"pipeline", `sum by (pipeline) (count_over_time({app="` + app + `"} | json | drop __error__, __error_details__ | pipeline!="" [1m]))`},
		{"user_id", `sum by (user_id) (count_over_time({app="` + app + `"} | json | drop __error__, __error_details__ | user_id!="" [1m]))`},
		{"user_id", `sum by (user_id) (count_over_time({app="` + app + `"} | json user_id="[\"user_id\"]" | drop __error__, __error_details__ | user_id!="" [1m]))`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, drilldown)
			// body, escaped, meta, stored and spaced, plus the three numeric values.
			prefix := strings.Split(tc.field, "_")[0][:1]
			for _, value := range []string{"body", "escaped", "meta", "stored", "spaced"} {
				if _, ok := loki[tc.field+"="+prefix+"-"+value]; !ok {
					t.Fatalf("Loki sanity: expected the %s-%s series, got %v", prefix, value, loki)
				}
			}
			if len(loki) != 5+map[string]int{"pipeline": 3, "user_id": 2}[tc.field] {
				t.Fatalf("Loki sanity: unexpected series %v", loki)
			}
			recorder.reset()
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, route, tc.query, start, end, time.Minute, drilldown))
			var stats []string
			for _, call := range recorder.reset() {
				if strings.HasPrefix(call, "/select/logsql/stats_query_range ") {
					stats = append(stats, call)
				}
			}
			parse := fmt.Sprintf(`| filter (%[1]s:* or _msg:"%[1]s" or _msg:~"\\\\u") | unpack_json if (-%[1]s:*) fields (%[1]s) | filter %[1]s:!""`, tc.field)
			if len(stats) != 1 || !strings.Contains(stats[0], parse) {
				t.Fatalf("expected one stats_query_range request parsing only the lines that can hold %s, got %v", tc.field, stats)
			}
		})
	}
}
