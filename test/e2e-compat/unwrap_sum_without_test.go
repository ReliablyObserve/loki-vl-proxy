//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

// Loki's `<op> without (...)` groups the series of its input by every label
// but the excluded ones and folds each group (pkg/logql/evaluator.go:451-466
// VectorAggEvaluator.Next, HashWithoutLabels), so the answer keeps a series per
// remaining label set. The proxy answered one series for a range function with
// unwrap, on query_range and at an instant (semantics/unwrap-sum-without-collapses).
//
// The fixture's lines carry a parsed `route` and `msg`, which are part of
// Loki's series and which the proxy does not expose
// (semantics/unwrap-per-series-identity-raw-rows), so both answers are
// projected onto the stream label `app` with the same operator before they are
// compared: exact for sum over rate and sum_over_time, and for max and min over
// max and min over time.

// projectOnApp folds the series of an answer onto their `app` label with op
// and fails on a series without one.
func projectOnApp(t *testing.T, query, side string, series map[string]map[string]string, op string) map[string]map[string]float64 {
	t.Helper()
	out := map[string]map[string]float64{}
	for key, points := range series {
		app := ""
		for _, pair := range strings.Split(key, ",") {
			if name, value, ok := strings.Cut(pair, "="); ok && name == "app" {
				app = value
			}
		}
		if app == "" {
			t.Fatalf("%s: %s series {%s} has no app label", query, side, key)
		}
		if out[app] == nil {
			out[app] = map[string]float64{}
		}
		for ts, raw := range points {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				t.Fatalf("%s: %s sample %q: %v", query, side, raw, err)
			}
			acc, seen := out[app][ts]
			switch {
			case !seen:
				out[app][ts] = v
			case op == "max":
				out[app][ts] = math.Max(acc, v)
			case op == "min":
				out[app][ts] = math.Min(acc, v)
			default:
				out[app][ts] = acc + v
			}
		}
	}
	return out
}

// TestCompat_UnwrapSumWithoutKeepsSeries: `sum without (...)`, and max and min,
// over a range function with unwrap keep a series per stream on query_range
// and at an instant, with Loki's values projected onto the stream (compared at
// an instant and for rate on query_range; the range values of the over-time
// functions follow semantics/unwrap-over-time-range-windows, so only the series
// are compared there), over a logfmt and a JSON stream group, excluding a parsed label and a label no
// series has.
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapSumWithoutKeepsSeries(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	type tc struct {
		op, function string
		values       bool // the values are exact under the projection
	}
	cases := []tc{
		{"sum", "rate", true},
		{"sum", "sum_over_time", true},
		{"max", "max_over_time", true},
		{"min", "min_over_time", true},
		// The registered shape: the per-series max depends on the parsed
		// labels, so only the series are compared.
		{"sum", "max_over_time", false},
	}
	for _, g := range []struct{ name, selector, parser string }{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
	} {
		for _, shape := range unwrapSumShapes {
			for _, c := range cases {
				for _, excluded := range []string{"route", "pod"} {
					query := unwrapSumQuery(c.op+" without ("+excluded+")", c.function, g.selector, g.parser, shape.window)
					t.Run(fmt.Sprintf("%s/%s/%s/%s", g.name, shape.name, query[:strings.Index(query, "{")], excluded), func(t *testing.T) {
						loki := shape.run(t, fx, lokiURL, query)
						proxy := shape.run(t, fx, proxyURL, query)
						if len(loki) < 3 {
							t.Fatalf("%s: Loki sanity: %d series, want one per stream and parsed label set at least", query, len(loki))
						}
						for key := range proxy {
							if strings.Contains(","+key, ","+excluded+"=") {
								t.Fatalf("%s: proxy series {%s} keeps the excluded label", query, key)
							}
						}
						want := projectOnApp(t, query, "Loki", loki, c.op)
						got := projectOnApp(t, query, "proxy", proxy, c.op)
						if len(got) != len(want) || len(proxy) != len(got) {
							t.Fatalf("%s: proxy has %d series over %d streams, Loki's answer covers %d streams", query, len(proxy), len(got), len(want))
						}
						// A range query of a function over time reads the
						// step-sized bucket (semantics/unwrap-over-time-range-windows).
						if !c.values || (shape.step != 0 && c.function != "rate") {
							return
						}
						for app, points := range want {
							for ts, w := range points {
								g, ok := got[app][ts]
								if !ok {
									t.Errorf("%s {app=%s} t=%s: proxy has no sample, Loki %g", query, app, ts, w)
									continue
								}
								if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
									t.Errorf("%s {app=%s} t=%s: proxy %g, Loki %g", query, app, ts, g, w)
								}
							}
							if len(got[app]) != len(points) {
								t.Errorf("%s {app=%s}: proxy has %d samples, Loki %d", query, app, len(got[app]), len(points))
							}
						}
					})
				}
			}
		}
	}
}
