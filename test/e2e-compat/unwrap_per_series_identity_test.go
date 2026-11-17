//go:build e2e

package e2e_compat

import (
	"strings"
	"testing"
)

// After `| json` or `| logfmt` with no keep, a Loki series is the stream plus
// every parsed label less the unwrapped one, and avg, max, min, stddev and
// stdvar over time are taken per series before an aggregation folds them. The
// unwrap sum fixtures hold three streams per format whose lines carry one of
// three `route` values and an `amount` that differs per line, so a stream holds
// three series and the pooled samples of the stream or the app give another
// value than the per-series ones.

// TestCompat_UnwrapPerSeriesIdentity: sum, sum by (app) and max by (app) over
// avg, max, min, stddev and stdvar over time of an unwrapped label after json
// and logfmt answer Loki's series and values, on a range query whose range is
// its step, on ranges longer than the step (sliding windows: a range of five
// steps, one of more than three steps, and one the step does not divide) and
// on an instant query; so does the upper-case spelling, and so do binary
// operations of such operands (two identities, an identity and a number,
// `on (app)` matching), each operand read as it is read alone.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/stdvar-grouped-pooled-variance, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapPerSeriesIdentity(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	var shapes []unwrapSumShape
	for _, shape := range unwrapSumShapes {
		switch shape.name {
		case "tumbling", "instant", "sliding", "range longer than the step", "range that is not a multiple of the step":
			shapes = append(shapes, shape)
		}
	}
	if len(shapes) != 5 {
		t.Fatalf("the tumbling, sliding and instant shapes are not all defined: %v", shapes)
	}
	type group struct {
		name, selector, parser string
		streams                int // the series of `by (app)`: one per stream
	}
	groups := []group{
		{"json", fx.selJSON, "json", len(fx.json)},
		{"logfmt", fx.selLogfmt, "logfmt", len(fx.logfmt)},
	}
	type tc struct {
		name, query string
		series      int // series Loki answers
	}
	var cases []tc
	for _, g := range groups {
		for _, function := range []string{"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time"} {
			for _, outer := range []struct {
				clause string
				series int
			}{{"sum", 1}, {"sum by (app)", g.streams}, {"max by (app)", g.streams}} {
				cases = append(cases, tc{name: g.name + "/" + outer.clause + "/" + function, query: unwrapSumQuery(outer.clause, function, g.selector, g.parser, "5m"), series: outer.series})
			}
		}
		upper := map[string]string{"json": "JSON", "logfmt": "LOGFMT"}[g.parser]
		cases = append(cases,
			tc{name: g.name + "/upper case", query: `SUM by (app) (AVG_OVER_TIME(` + g.selector + ` | ` + upper + ` | UNWRAP amount [5m]))`, series: g.streams},
			// A difference, not a ratio: the per-series maxima and minima of this fixture often
			// coincide with the stream's, which a quotient hides. Each operand is evaluated as a
			// request of its own, so it reads the series hash as it does alone.
			tc{name: g.name + "/binary operation", query: `sum by (app) (max_over_time(` + g.selector + ` | ` + g.parser + ` | unwrap amount [5m])) - sum by (app) (min_over_time(` + g.selector + ` | ` + g.parser + ` | unwrap amount [5m]))`, series: g.streams},
			tc{name: g.name + "/binary operation with a number", query: `2 * sum by (app) (avg_over_time(` + g.selector + ` | ` + g.parser + ` | unwrap amount [5m]))`, series: g.streams},
			tc{name: g.name + "/binary operation on (app)", query: `sum by (app) (stddev_over_time(` + g.selector + ` | ` + g.parser + ` | unwrap amount [5m])) / on (app) sum by (app) (avg_over_time(` + g.selector + ` | ` + g.parser + ` | unwrap amount [5m]))`, series: g.streams},
		)
	}
	for _, c := range cases {
		for _, shape := range shapes {
			// Each shape reads its own range: the cases are written over [5m].
			query := strings.ReplaceAll(c.query, "[5m]", "["+shape.window+"]")
			t.Run(shape.name+"/"+c.name, func(t *testing.T) {
				loki := shape.run(t, fx, lokiURL, query)
				if len(loki) != c.series {
					t.Fatalf("%s: Loki answers %d series, the fixture holds %d: parity would be empty-vs-empty or the fixture is not what the case reads", query, len(loki), c.series)
				}
				compareUnwrapSumSeries(t, query, loki, shape.run(t, fx, proxyURL, query))
			})
		}
	}
}
