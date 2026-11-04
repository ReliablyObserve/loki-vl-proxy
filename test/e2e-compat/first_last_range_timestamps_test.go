//go:build e2e

package e2e_compat

import (
	"testing"
	"time"
)

// TestCompat_UnwrapFirstLastRangeTimestamps: on query_range with a range equal
// to the step (Grafana's $__auto) or shorter than it, an aggregation over
// first_over_time or last_over_time answers Loki's points: the same series,
// evaluation timestamps and values. Loki stamps the sample of the window
// (t-range, t] at t (pkg/logql/range_vector.go:102-113, 150-160, 189); the
// proxy reads VictoriaLogs step buckets, labelled by their start, and relabels
// them onto Loki's times. Before, every point came one step early and a line on
// a step edge landed in the next step.
// conformance: semantics/unwrap-first-last-range-timestamps, loki_api_v1_query_range
func TestCompat_UnwrapFirstLastRangeTimestamps(t *testing.T) {
	fx := ensureFirstLastFixtures(t)
	ufx := &unwrapSumFixtures{selLogfmt: fx.logfmt, selJSON: fx.json, s0: fx.s0}
	ufx.waitForLoki(t)
	shapes := []unwrapSumShape{
		{"range equal to a 10s step", "10s", 10 * time.Second, 5 * time.Minute, 20 * time.Minute},
		{"range equal to a 5s step", "5s", 5 * time.Second, 5*time.Minute + 2*time.Second, 15 * time.Minute},
		{"range equal to a 1m step", "1m", time.Minute, 5 * time.Minute, 40 * time.Minute},
		{"range shorter than the step", "20s", time.Minute, 5 * time.Minute, 40 * time.Minute},
	}
	for _, g := range []struct{ name, selector, parser string }{{"logfmt", fx.logfmt, "logfmt"}, {"json", fx.json, "json"}} {
		for _, function := range []string{"first_over_time", "last_over_time"} {
			for _, outer := range []string{"sum", "sum by (app)", "max by (app)", "count"} {
				for _, shape := range shapes {
					query := firstLastQuery(outer, function, g.selector, g.parser, shape.window)
					t.Run(g.name+"/"+function+"/"+outer+"/"+shape.name, func(t *testing.T) {
						loki := shape.run(t, ufx, lokiURL, query)
						proxy := shape.run(t, ufx, proxyURL, query)
						compareUnwrapSumSeries(t, query, loki, proxy)
					})
				}
			}
		}
	}
}
