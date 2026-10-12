//go:build e2e

package e2e_compat

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// quantile_over_time (Loki v3.7.7 pkg/logql/range_vector.go:449-487) sorts the
// samples of the window (t-range, t] and interpolates at rank phi*(n-1). The
// proxy reads VictoriaLogs' row counts per converted value and bucket and
// interpolates over the counts of each window
// (semantics/unwrap-quantile-response-limit). A range aggregation's own
// by (...) takes one quantile over the group's samples
// (pkg/logql/syntax/extractor.go); a vector aggregation over the quantile folds
// the quantile of every series per step (pkg/logql/evaluator.go
// newVectorAggEvaluator).

type unwrapQuantileFixtures struct {
	s0 time.Time
	// selPlain: three streams whose JSON lines hold only the unwrapped field, so
	// a series is the stream (Loki adds no parsed label but the unwrapped one).
	selPlain string
	// selRoute: three streams whose lines also hold a route, for a range
	// aggregation grouped by a parsed label.
	selRoute string
}

var (
	unwrapQuantileOnce sync.Once
	unwrapQuantileFx   *unwrapQuantileFixtures
)

// ensureUnwrapQuantileFixtures ingests six streams 22 hours back (clear of the
// other fixtures and of the generator's recent data), a line every 10s for 30
// minutes per stream, each stream on its own offset so no line sits on a window
// edge. Values repeat (integers) and vary (decimals), so windows hold ties and
// interpolate between distinct neighbours.
func ensureUnwrapQuantileFixtures(t *testing.T) *unwrapQuantileFixtures {
	t.Helper()
	unwrapQuantileOnce.Do(func() {
		now := time.Now()
		id := now.UnixNano()
		fx := &unwrapQuantileFixtures{
			s0:       now.Add(-22 * time.Hour).Truncate(time.Hour),
			selPlain: fmt.Sprintf(`{app=~"uwq-plain-%d-.+"}`, id),
			selRoute: fmt.Sprintf(`{app=~"uwq-route-%d-.+"}`, id),
		}
		var all []slidingLiveFixture
		for _, kind := range []string{"plain", "route"} {
			for k, name := range []string{"a", "b", "c"} {
				stream := slidingLiveFixture{app: fmt.Sprintf("uwq-%s-%d-%s", kind, id, name)}
				for i := 0; i < 180; i++ {
					value := fmt.Sprint((i*7 + k*3) % 13)
					if i%4 == 1 {
						value = fmt.Sprintf("%d.%03d", (i*11+k)%97, (i*37+k*101)%1000)
					}
					msg := fmt.Sprintf(`{"duration_ms":%s}`, value)
					if kind == "route" {
						msg = fmt.Sprintf(`{"route":"/r%d","duration_ms":%s}`, (i+k)%3, value)
					}
					ts := fx.s0.Add(time.Duration(3+2*k)*time.Second + time.Duration(i)*10*time.Second)
					stream.lines = append(stream.lines, slidingLiveLine{ts: ts, msg: msg})
				}
				all = append(all, stream)
			}
		}
		ingestSlidingFixtures(t, all...)
		unwrapQuantileFx = fx
	})
	if unwrapQuantileFx == nil {
		t.Fatal("the unwrap quantile fixtures were not ingested; see the first test that ran")
	}
	return unwrapQuantileFx
}

// waitForLoki polls until Loki's range metric path answers for the fixture
// window (a fresh stack answers range metrics empty for a few minutes).
func (fx *unwrapQuantileFixtures) waitForLoki(t *testing.T) {
	t.Helper()
	at := fx.s0.Add(20 * time.Minute)
	deadline := time.Now().Add(240 * time.Second)
	for _, selector := range []string{fx.selPlain, fx.selRoute} {
		query := fmt.Sprintf("sum(count_over_time(%s [5m]))", selector)
		for len(slidingRangeSeries(t, lokiURL, query, at.Add(-time.Minute), at, time.Minute, nil)) == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("Loki's range metric path answered nothing for %s after 240s", selector)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

// TestCompat_UnwrapQuantile: quantile_over_time over a JSON unwrap answers
// Loki's series and values (to 1e-9 relative: a platform may fuse the
// interpolation's multiply-add) for the phi bounds and inner points, grouped by
// the range aggregation (by a stream label, by a parsed label, by ()) and
// under sum, avg, max, min and count over per-stream quantiles, with and without the error
// filter, for windows equal to, shorter than, longer than and not a multiple of
// the step, and at an instant.
// conformance: semantics/unwrap-quantile-response-limit, metric-series-identity, window-bounds-and-step-alignment, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapQuantile(t *testing.T) {
	fx := ensureUnwrapQuantileFixtures(t)
	fx.waitForLoki(t)
	lokiFx := &unwrapSumFixtures{s0: fx.s0}
	forms := []struct{ name, selector, outer, grouping string }{
		{"by stream label", fx.selPlain, "", "by (app)"},
		{"by ()", fx.selPlain, "", "by ()"},
		{"by parsed label", fx.selRoute, "", "by (route)"},
		{"by () over parsed labels", fx.selRoute, "", "by ()"},
		{"sum", fx.selPlain, "sum", ""},
		{"sum by stream label", fx.selPlain, "sum by (app)", ""},
		{"avg", fx.selPlain, "avg", ""},
		{"max", fx.selPlain, "max", ""},
		{"min", fx.selPlain, "min", ""},
		{"count", fx.selPlain, "count", ""},
		{"sum over a grouped range", fx.selPlain, "sum", "by (app)"},
	}
	for _, form := range forms {
		for _, filter := range []string{"", ` | __error__=""`} {
			for _, phi := range []string{"0", "0.5", "0.95", "1"} {
				for _, shape := range unwrapSumShapes {
					inner := fmt.Sprintf(`quantile_over_time(%s, %s | json | unwrap duration_ms%s [%s]) %s`, phi, form.selector, filter, shape.window, form.grouping)
					query := strings.TrimSpace(inner)
					if form.outer != "" {
						query = fmt.Sprintf("%s (%s)", form.outer, query)
					}
					t.Run(form.name+filter+"/phi="+phi+"/"+shape.name, func(t *testing.T) {
						loki := shape.run(t, lokiFx, lokiURL, query)
						proxy := shape.run(t, lokiFx, proxyURL, query)
						compareUnwrapSumSeries(t, query, loki, proxy)
					})
				}
			}
		}
	}
}
