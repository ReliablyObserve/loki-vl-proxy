//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"
)

// Loki evaluates sum, avg, max and min over time on the window (t-range, t] of
// every step (pkg/logql/range_vector.go:148-189 batchRangeVectorIterator.load,
// range_vector.go:371-425), whatever the range and the step are; the proxy
// answered a range other than the step from one VictoriaLogs step bucket
// (semantics/unwrap-over-time-range-windows). The fixture streams hold a line
// every 10s with a distinct amount, so every window value depends on which
// lines it holds. Each `by (app, route)` group is one Loki series (the stream
// and the parsed route; msg is constant), so the values compare one to one.

var unwrapWindowShapes = []unwrapEndShape{
	{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range shorter than the step", "2m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range longer than the step", "10m", 3 * time.Minute, 12 * time.Minute, 27 * time.Minute},
	{"range that is not a multiple of the step", "90s", time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range shorter than the step, not a divisor", "45s", 2 * time.Minute, 4 * time.Minute, 28 * time.Minute},
}

// TestCompat_UnwrapOverTimeRangeWindows: every series, timestamp and value of
// sum, avg, max and min over time with unwrap is Loki's, over a logfmt and a
// JSON stream group, grouped by series, ungrouped (sum over time), and with an
// offset, for ranges equal to, longer and shorter than the step and not a
// multiple of it.
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_UnwrapOverTimeRangeWindows(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, g := range []struct{ name, selector, parser string }{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
	} {
		for _, shape := range unwrapWindowShapes {
			var queries []string
			for _, function := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
				queries = append(queries,
					fmt.Sprintf(`sum by (app, route) (%s(%s | %s | unwrap amount | __error__="" [%s]))`, function, g.selector, g.parser, shape.window),
					fmt.Sprintf(`sum by (app, route) (%s(%s | %s | unwrap amount | __error__="" [%s] offset 2m))`, function, g.selector, g.parser, shape.window))
			}
			queries = append(queries, fmt.Sprintf(`sum(sum_over_time(%s | %s | unwrap amount [%s]))`, g.selector, g.parser, shape.window))
			for _, query := range queries {
				t.Run(g.name+"/"+shape.name+"/"+query, func(t *testing.T) {
					start, end := fx.s0.Add(shape.start), fx.s0.Add(shape.finish)
					loki := slidingRangeSeries(t, lokiURL, query, start, end, shape.step, nil)
					proxy := slidingRangeSeries(t, proxyURL, query, start, end, shape.step, nil)
					if len(loki) == 0 {
						t.Fatalf("%s: Loki answered nothing; parity would be empty-vs-empty", query)
					}
					if len(proxy) != len(loki) {
						t.Errorf("%s: Loki has %d series, proxy %d", query, len(loki), len(proxy))
					}
					for key, want := range loki {
						got, ok := proxy[key]
						if !ok {
							t.Errorf("%s: series {%s} missing from the proxy", query, key)
							continue
						}
						if fmt.Sprint(sortedSampleTimes(got)) != fmt.Sprint(sortedSampleTimes(want)) {
							t.Errorf("%s {%s}: proxy has %d samples, Loki %d", query, key, len(got), len(want))
						}
						for ts, w := range want {
							wv, _ := strconv.ParseFloat(w, 64)
							gv, err := strconv.ParseFloat(got[ts], 64)
							if err != nil || math.Abs(gv-wv) > 1e-9*math.Max(1, math.Abs(wv)) {
								t.Errorf("%s {%s} at %s: proxy %q, Loki %q", query, key, ts, got[ts], w)
							}
						}
					}
				})
			}
		}
	}
}
