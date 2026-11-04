//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// A count, min, max or avg of a range function takes a value per Loki stream
// at every step, and a sum by over rate / count_over_time of one selector is the
// number of active streams over the range (pkg/logql/evaluator.go
// newBinOpStepEvaluator, VectorAggEvaluator.Next). The proxy folded the
// per-stream matrix after encoding it, so a wide range, a fine step or many
// streams passed -backend-max-buffered-response-bytes with a 500 where Loki
// answers 200 (semantics/series-level-aggregation-bounded-memory). The fixture
// is the non-sum one: four streams in two zones.

// seriesLevelFineSteps: a fine step over a sliding window (every stream has a
// sample at every step: the shape that buffered most), a step that does not
// divide the window, and the 120 s step of the 48h check at a window that
// divides it.
var seriesLevelFineSteps = []struct {
	name        string
	window      string
	step        time.Duration
	start, stop time.Duration
}{
	{"20s step, 5m window", "5m", 20 * time.Second, 6 * time.Minute, 26 * time.Minute},
	{"7s step, 5m window", "5m", 7 * time.Second, 6 * time.Minute, 16 * time.Minute},
	{"120s step, 5m window", "5m", 120 * time.Second, 6 * time.Minute, 26 * time.Minute},
}

// TestCompat_SeriesLevelAggregationFineStep: B2 of bench/ab (aggregation-over-binary),
// count / min / max / avg by, and a sum over a max by, at fine steps, answer 200
// with Loki's series and values.
// conformance: semantics/series-level-aggregation-bounded-memory, metric-series-identity, loki_api_v1_query_range
func TestCompat_SeriesLevelAggregationFineStep(t *testing.T) {
	fx := ensureNonSumFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	forms := []func(window string) string{
		func(w string) string {
			return fmt.Sprintf(`sum by (zone) (rate(%s [%s]) / count_over_time(%s [%s]))`, selector, w, selector, w)
		},
		func(w string) string { return fmt.Sprintf(`count by (zone) (count_over_time(%s [%s]))`, selector, w) },
		func(w string) string { return fmt.Sprintf(`avg by (zone) (rate(%s [%s]))`, selector, w) },
		func(w string) string {
			return fmt.Sprintf(`min by (zone, pod) (count_over_time(%s [%s]))`, selector, w)
		},
		func(w string) string { return fmt.Sprintf(`max (rate(%s [%s]))`, selector, w) },
		func(w string) string {
			return fmt.Sprintf(`sum by (zone) (max by (zone, pod) (count_over_time(%s [%s])))`, selector, w)
		},
	}
	for _, shape := range seriesLevelFineSteps {
		for _, form := range forms {
			query := form(shape.window)
			t.Run(shape.name+"/"+query, func(t *testing.T) {
				start, end := fx.s0.Add(shape.start), fx.s0.Add(shape.stop)
				loki := nonSumRange(t, lokiURL, query, start, end, shape.step)
				proxy := nonSumRange(t, proxyURL, query, start, end, shape.step)
				compareNonSum(t, query, loki, proxy)
			})
		}
	}
}
