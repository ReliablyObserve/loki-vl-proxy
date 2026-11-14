//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// TestCompat_UnwrapFirstLastRangeLongerThanStep: on query_range with a range longer than the step, first_over_time
// and last_over_time over a series a keep decides answer Loki's points: the same series, evaluation timestamps and
// values. Loki's sample at t is the first or last sample of each series in the window (t-range, t], and the windows
// of two evaluation times overlap; the proxy reads the earliest or latest row of each series in buckets of
// gcd(step, range) and takes each window's earliest or latest bucket that holds a line. Before, every point was the
// step before it, so first_over_time took the first line of the last step instead of the window's.
// conformance: semantics/unwrap-first-last-range-longer-than-step, loki_api_v1_query_range
func TestCompat_UnwrapFirstLastRangeLongerThanStep(t *testing.T) {
	fx := ensureFirstLastFixtures(t)
	ufx := &unwrapSumFixtures{selLogfmt: fx.logfmt, selJSON: fx.json, s0: fx.s0}
	ufx.waitForLoki(t)
	shapes := []unwrapSumShape{
		{"a 5m range at a 1m step", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
		{"a 10m range at a 3m step", "10m", 3 * time.Minute, 12 * time.Minute, 27 * time.Minute},
		{"a range that is not a multiple of the step", "90s", time.Minute, 5 * time.Minute, 25 * time.Minute},
	}
	for _, g := range []struct{ name, selector, parser string }{{"logfmt", fx.logfmt, "logfmt"}, {"json", fx.json, "json"}} {
		for _, function := range []string{"first_over_time", "last_over_time"} {
			for _, outer := range []string{"", "sum", "sum by (app)", "max by (app)", "count"} {
				for _, shape := range shapes {
					inner := fmt.Sprintf("%s(%s | %s | keep app, amount | unwrap amount [%s])", function, g.selector, g.parser, shape.window)
					query := inner
					if outer != "" {
						query = outer + "(" + inner + ")"
					}
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
