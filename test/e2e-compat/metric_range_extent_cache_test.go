//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// TestCompat_MetricRangeExtentCacheMatchesLoki: a metric range re-run with its
// window moved back (a user re-running a query) or forward (a dashboard
// refresh), then widened over buckets read before and buckets never read,
// answers Loki's series and values every time. The proxy answers the moved
// windows from the VictoriaLogs buckets its earlier requests read (the
// extent cache, on by default) and asks VictoriaLogs only for the rest; the
// fixture is hours old, past -query-range-freshness, so its buckets are kept.
// conformance: quality/query-range-extent-cache, loki_api_v1_query_range
func TestCompat_MetricRangeExtentCacheMatchesLoki(t *testing.T) {
	fx := ensureNonSumFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	shapes := []struct {
		query string
		step  time.Duration
	}{
		{fmt.Sprintf(`sum by (zone) (count_over_time(%s [1m]))`, selector), time.Minute},
		{fmt.Sprintf(`sum(rate(%s [5m]))`, selector), 30 * time.Second},
		{fmt.Sprintf(`sum by (pod) (bytes_over_time(%s [2m]))`, selector), time.Minute},
		{fmt.Sprintf(`max by (zone) (count_over_time(%s [5m]))`, selector), time.Minute},
		{fmt.Sprintf(`topk(2, sum by (pod) (rate(%s [1m])))`, selector), time.Minute},
		{fmt.Sprintf(`sum by (zone) (count_over_time(%s [90s]))`, selector), 20 * time.Second},
	}
	// Window ends as offsets from the fixture start: a first read, two moved
	// back, three moved forward, and one wide window over all of them.
	type window struct{ start, end time.Duration }
	windows := []window{
		{5 * time.Minute, 20 * time.Minute},
		{4 * time.Minute, 19 * time.Minute},
		{3 * time.Minute, 18 * time.Minute},
		{6 * time.Minute, 21 * time.Minute},
		{7 * time.Minute, 22 * time.Minute},
		{8 * time.Minute, 23 * time.Minute},
		{2 * time.Minute, 29 * time.Minute},
	}
	for _, shape := range shapes {
		t.Run(shape.query, func(t *testing.T) {
			for _, w := range windows {
				start, end := fx.s0.Add(w.start), fx.s0.Add(w.end)
				loki := nonSumRange(t, lokiURL, shape.query, start, end, shape.step)
				proxy := nonSumRange(t, proxyURL, shape.query, start, end, shape.step)
				compareNonSum(t, fmt.Sprintf("%s [%s, %s]", shape.query, w.start, w.end), loki, proxy)
			}
			// The first window again: answered from stored buckets, the same values.
			start, end := fx.s0.Add(windows[0].start), fx.s0.Add(windows[0].end)
			loki := nonSumRange(t, lokiURL, shape.query, start, end, shape.step)
			proxy := nonSumRange(t, proxyURL, shape.query, start, end, shape.step)
			compareNonSum(t, shape.query+" (repeated)", loki, proxy)
		})
	}
}
