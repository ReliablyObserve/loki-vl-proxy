//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// Loki reads a step's sample of sum, avg, max and min over time from the window
// (t-range, t] only (pkg/logql/range_vector.go:102-189): with a range shorter
// than the step, the lines between two windows belong to no sample, and a step
// whose window holds no line has none. The fixture streams hold a line every
// 10s, off the minute marks, so every window of these shapes holds lines and
// the step bucket holds more lines than the window.
var unwrapShortRangeShapes = []unwrapSumShape{
	{"2m range at a 5m step", "2m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"1m range at a 5m step", "1m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"30s range at a 2m step", "30s", 2 * time.Minute, 4 * time.Minute, 26 * time.Minute},
}

// TestCompat_UnwrapOverTimeShortRange: `<outer>(<fn>(... | unwrap amount [r]))`
// with r shorter than the step answers Loki's series, timestamps and values for
// sum, avg, max and min over time, over a logfmt and a JSON stream group, with
// and without the error filter (the conversion-error check rides on the same
// query). Each outer aggregation reads the same answer from any split of a group
// into series (a sum of sums, a maximum of maxima, a minimum of minima, and avg
// grouped by the whole series identity), so only the window is compared here;
// the series identity of the other forms is registered apart
// (semantics/unwrap-sum-by-per-series-function-extraction-list).
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_UnwrapOverTimeShortRange(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, g := range []struct{ name, selector, parser string }{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
		{"logfmt without errors", fx.selLogfmt, `logfmt | __error__=""`},
		{"json without errors", fx.selJSON, `json | __error__=""`},
	} {
		for _, shape := range unwrapShortRangeShapes {
			for _, f := range []struct{ function, outer string }{
				{"sum_over_time", "sum by (route)"},
				{"max_over_time", "max by (route)"},
				{"min_over_time", "min by (app)"},
				{"avg_over_time", "sum by (app, route)"},
			} {
				query := unwrapSumQuery(f.outer, f.function, g.selector, g.parser, shape.window)
				t.Run(fmt.Sprintf("%s/%s/%s", g.name, shape.name, f.function), func(t *testing.T) {
					loki := shape.run(t, fx, lokiURL, query)
					proxy := shape.run(t, fx, proxyURL, query)
					compareUnwrapSumSeries(t, query, loki, proxy)
				})
			}
		}
	}
}
