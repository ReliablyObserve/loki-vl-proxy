//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// stdvar_over_time under a grouping, or without a parser, was answered with an
// error status (Loki answers 200): the translated
// query, the standard deviation raised to the power 2, was sent to
// VictoriaLogs with its binary marker as a query. Instant and range queries
// now get a 200 with the series Loki has. The values of the outer `sum by` form are not
// compared: Loki sums the variances of each series' own samples, the proxy takes
// the variance of the group's pooled samples (semantics/stdvar-grouped-pooled-variance,
// open). The by() form's values are compared apart.
//
// conformance: semantics/stdvar-grouped-binary-marker, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_StdvarOverTimeGroupedAnswers200(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, query := range []string{
		`sum by (app) (stdvar_over_time(` + fx.selJSON + ` | json | unwrap amount [5m]))`,
		`stdvar_over_time(` + fx.selJSON + ` | json | unwrap amount [5m]) by (app)`,
	} {
		t.Run("range/"+query, func(t *testing.T) {
			start, end := fx.s0.Add(5*time.Minute), fx.s0.Add(25*time.Minute)
			loki := slidingRangeSeries(t, lokiURL, query, start, end, 5*time.Minute, nil)
			proxy := slidingRangeSeries(t, proxyURL, query, start, end, 5*time.Minute, nil)
			if len(loki) == 0 || len(proxy) == 0 {
				t.Fatalf("%s: Loki has %d series, proxy %d; both must answer series", query, len(loki), len(proxy))
			}
		})
		t.Run("instant/"+query, func(t *testing.T) {
			at := fx.s0.Add(20 * time.Minute)
			loki := unwrapInstant(t, lokiURL, query, at)
			proxy := unwrapInstant(t, proxyURL, query, at)
			if len(loki) == 0 || len(proxy) == 0 {
				t.Fatalf("%s: Loki has %d series, proxy %d; both must answer series", query, len(loki), len(proxy))
			}
		})
	}
}

// stdvar_over_time(...) by (app) pools the samples of each group, as Loki does:
// its values are Loki's on a range query at step 5m and on an instant query.
//
// conformance: semantics/stdvar-grouped-binary-marker, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_StdvarOverTimeByMatchesLoki(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	query := `stdvar_over_time(` + fx.selJSON + ` | json | unwrap amount [5m]) by (app)`
	t.Run("range", func(t *testing.T) {
		start, end := fx.s0.Add(5*time.Minute), fx.s0.Add(25*time.Minute)
		loki := slidingRangeSeries(t, lokiURL, query, start, end, 5*time.Minute, nil)
		proxy := slidingRangeSeries(t, proxyURL, query, start, end, 5*time.Minute, nil)
		compareUnwrapSumSeries(t, query, loki, proxy)
	})
	t.Run("instant", func(t *testing.T) {
		at := fx.s0.Add(20 * time.Minute)
		loki := unwrapInstant(t, lokiURL, query, at)
		proxy := unwrapInstant(t, proxyURL, query, at)
		asSeries := func(in map[string]string) map[string]map[string]string {
			out := map[string]map[string]string{}
			for key, value := range in {
				out[key] = map[string]string{fmt.Sprint(at.Unix()): value}
			}
			return out
		}
		compareUnwrapSumSeries(t, query, asSeries(loki), asSeries(proxy))
	})
}

// stddev_over_time with unwrap over a range equal to the step is the window
// ending at each evaluation time, with Loki's values (it was one step late: the
// bucket VictoriaLogs labels T holds [T, T+step)). A range that is not the step
// is registered apart (semantics/stddev-stdvar-over-time-step-bucket).
//
// conformance: semantics/stddev-stdvar-over-time-step-bucket, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_StddevOverTimeTumblingMatchesLoki(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	query := `stddev_over_time(` + fx.selJSON + ` | json | unwrap amount [5m]) by (app)`
	start, end := fx.s0.Add(5*time.Minute), fx.s0.Add(25*time.Minute)
	loki := slidingRangeSeries(t, lokiURL, query, start, end, 5*time.Minute, nil)
	proxy := slidingRangeSeries(t, proxyURL, query, start, end, 5*time.Minute, nil)
	compareUnwrapSumSeries(t, query, loki, proxy)
}
