//go:build e2e

package e2e_compat

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// Loki evaluates a range query at start, start+step, ... up to and including
// end (pkg/logql/range_vector.go step iteration), so `end` carries a sample
// whenever the window (end-range, end] holds a line. The fixture lines stop
// just before the 30 minute mark and every shape ends there, so the sample at
// `end` is the one a bucket labelled by its start leaves out: sum, avg, max and
// min over time with unwrap answered one point short (semantics/unwrap-range-last-point).
// Only the timestamps are compared: a route group holds three series here, so
// the values of avg, max and min follow the series identity; the window values
// are compared per series by TestCompat_UnwrapOverTimeRangeWindows.

type unwrapEndShape struct {
	name          string
	window        string
	step          time.Duration
	start, finish time.Duration
}

var unwrapEndShapes = []unwrapEndShape{
	{"sliding", "5m", time.Minute, 6 * time.Minute, 30 * time.Minute},
	{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 30 * time.Minute},
	{"range shorter than the step", "2m", 5 * time.Minute, 5 * time.Minute, 30 * time.Minute},
	{"range longer than the step", "10m", 3 * time.Minute, 12 * time.Minute, 30 * time.Minute},
	{"range that is not a multiple of the step", "90s", time.Minute, 5 * time.Minute, 30 * time.Minute},
}

func sortedSampleTimes(points map[string]string) []string {
	out := make([]string, 0, len(points))
	for ts := range points {
		out = append(out, ts)
	}
	sort.Strings(out)
	return out
}

// TestCompat_UnwrapRangeEndSample: `sum by (route) (<fn>(... | unwrap amount [r]))`
// has a sample at every step from start to end inclusive, as Loki does, for sum,
// avg, max and min over time, over a logfmt and a JSON stream group, for a range
// equal to, shorter than and longer than the step and not a multiple of it.
// conformance: semantics/unwrap-range-last-point, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_UnwrapRangeEndSample(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, g := range []struct{ name, selector, parser string }{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
	} {
		for _, shape := range unwrapEndShapes {
			for _, function := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
				query := fmt.Sprintf(`sum by (route) (%s(%s | %s | unwrap amount | __error__="" [%s]))`, function, g.selector, g.parser, shape.window)
				t.Run(g.name+"/"+shape.name+"/"+function, func(t *testing.T) {
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
						if _, ok := want[fmt.Sprint(end.Unix())]; !ok {
							t.Fatalf("%s {%s}: Loki sanity: no sample at end", query, key)
						}
						if fmt.Sprint(sortedSampleTimes(got)) != fmt.Sprint(sortedSampleTimes(want)) {
							t.Errorf("%s {%s}: proxy has %d samples, last %v; Loki %d, last %v", query, key,
								len(got), tail(sortedSampleTimes(got)), len(want), tail(sortedSampleTimes(want)))
						}
					}
				})
			}
		}
	}
}

func tail(v []string) string {
	if len(v) == 0 {
		return "none"
	}
	return v[len(v)-1]
}
