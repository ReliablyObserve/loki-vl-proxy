//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"reflect"
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
		app := metricKeyLabels(key)["app"]
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

func TestCompat_UnwrapSumWithoutCanonicalLabels(t *testing.T) {
	app := "api,a=b"
	first := metricJSONKey(map[string]string{"app": app, "route": "/v1,a=b", "detected_level": "unknown"})
	second := metricJSONKey(map[string]string{"app": app, "route": "/v2"})
	series := map[string]map[string]string{first: {"1.5": "2"}, second: {"1.5": "3"}}
	for op, want := range map[string]float64{"sum": 5, "max": 3, "min": 2} {
		got := projectOnApp(t, "projection fixture", "reference", series, op)
		if len(got) != 1 || got[app]["1.5"] != want {
			t.Fatalf("%s projection lost label bytes or samples: %v", op, got)
		}
	}
	got := withoutUnknownDetectedLevel(series)
	want := map[string]map[string]string{
		metricJSONKey(map[string]string{"app": app, "route": "/v1,a=b"}): {"1.5": "2"},
		second: {"1.5": "3"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata projection changed another label or point: got %v want %v", got, want)
	}
	for _, level := range []string{"info", "unknown,app=other"} {
		key := metricJSONKey(map[string]string{"app": app, "detected_level": level})
		input := map[string]map[string]string{key: {"1.5": "7"}}
		if got := withoutUnknownDetectedLevel(input); !reflect.DeepEqual(got, input) {
			t.Fatalf("projected a level other than exactly unknown: %v", got)
		}
	}
	legacy := map[string]map[string]string{"app=plain,detected_level=unknown": {"1.5": "4"}}
	if got := withoutUnknownDetectedLevel(legacy)[metricJSONKey(map[string]string{"app": "plain"})]["1.5"]; got != "4" {
		t.Fatalf("legacy instant projection lost the sample: %q", got)
	}
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
							if _, kept := metricKeyLabels(key)[excluded]; kept {
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

// TestCompat_UnwrapSumWithoutExtractionList: a parser that names the keys it
// extracts makes a Loki series the stream plus those labels less the unwrapped
// one, so the unique `msg`/`route` values of the fixture's lines do not split
// it. `<op> without (route)` over `| logfmt amount` is then a value per stream
// and over `| logfmt amount, route` a value per stream and route folded across
// routes; both are compared with Loki's values exactly (no projection), at an
// instant for every function and on query_range for rate.
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapSumWithoutExtractionList(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	instant, sliding := unwrapSumShapes[len(unwrapSumShapes)-1], unwrapSumShapes[0]
	for _, parser := range []string{"logfmt amount", "logfmt amount, route"} {
		for _, function := range []string{"rate", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
			for _, op := range []string{"sum", "max", "min", "count", "avg"} {
				for _, shape := range []unwrapSumShape{instant, sliding} {
					if shape.step != 0 && function != "rate" {
						continue // semantics/unwrap-over-time-range-windows
					}
					query := fmt.Sprintf("%s without (route) (%s(%s | %s | unwrap amount [%s]))", op, function, fx.selLogfmt, parser, shape.window)
					t.Run(fmt.Sprintf("%s/%s/%s/%s", parser, function, op, shape.name), func(t *testing.T) {
						loki := shape.run(t, fx, lokiURL, query)
						proxy := shape.run(t, fx, proxyURL, query)
						if len(loki) < 3 {
							t.Fatalf("%s: Loki sanity: %d series, want one per stream at least", query, len(loki))
						}
						compareUnwrapSumSeries(t, query, withoutUnknownDetectedLevel(loki), withoutUnknownDetectedLevel(proxy))
					})
				}
			}
		}
	}
}

// withoutUnknownDetectedLevel drops the `detected_level=unknown` label from the
// series keys: Loki adds it to every series of a stream that has no level, and
// the proxy does on query_range but not at an instant
// (semantics/without-instant-detected-level-unknown), which these tests do not
// compare.
func withoutUnknownDetectedLevel(series map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(series))
	for key, points := range series {
		labels := metricKeyLabels(key)
		if labels["detected_level"] == "unknown" {
			delete(labels, "detected_level")
		}
		out[metricJSONKey(labels)] = points
	}
	return out
}

// TestCompat_UnwrapSumWithoutUnderAnotherAggregation: a `without` under a sum,
// topk, stddev or stdvar is Loki's answer too, with the proxy leaving the fold to
// the outer operation. A max, min, avg or count over it folds the inner `without`
// first and then the outer level, innermost first, as Loki does
// (semantics/without-nested-under-aggregation).
// conformance: semantics/unwrap-sum-without-collapses, semantics/without-nested-under-aggregation, metric-series-identity, loki_api_v1_query
func TestCompat_UnwrapSumWithoutUnderAnotherAggregation(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	instant := unwrapSumShapes[len(unwrapSumShapes)-1]
	inner := fmt.Sprintf("sum without (route) (rate(%s | logfmt amount, route | unwrap amount [5m]))", fx.selLogfmt)
	for _, outer := range []string{"stddev(%s)", "stdvar(%s)", "sum(%s)", "topk(2, %s)", "max(%s)", "min(%s)", "avg(%s)", "count(%s)"} {
		query := strings.Replace(outer, "%s", inner, 1)
		t.Run(strings.ReplaceAll(outer, "%s", "…"), func(t *testing.T) {
			loki := instant.run(t, fx, lokiURL, query)
			proxy := instant.run(t, fx, proxyURL, query)
			if len(loki) == 0 {
				t.Fatalf("%s: Loki answered nothing", query)
			}
			compareUnwrapSumSeries(t, query, withoutUnknownDetectedLevel(loki), withoutUnknownDetectedLevel(proxy))
		})
	}
}
