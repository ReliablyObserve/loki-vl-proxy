//go:build e2e

package e2e_compat

import (
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Loki evaluates a vector aggregation over a binary expression by joining the
// operands per series (pkg/logql/evaluator.go newBinOpStepEvaluator and
// vectorBinop: matching on every label or on()/ignoring(), group_left, a
// literal on either side, a comparison filtering or answering 0/1 with bool)
// and aggregating the joined series per step (VectorAggEvaluator.Next). The
// proxy kept only the first operand of the binary under the aggregation, or
// answered 400 on an instant query (semantics/aggregation-over-binary-dropped).

// TestCompat_AggregationOverBinary: sum, avg, min, max and count, grouped by a
// stream label, by a parsed label, without one and not at all, over binaries of
// two metrics, a metric and a literal, a comparison with and without bool,
// on()/ignoring() with group_left and nested binaries, over the unwrap sum
// fixture's logfmt streams, on every window shape and at an instant: the
// proxy's series, timestamps and values are Loki's.
// conformance: semantics/aggregation-over-binary-dropped, semantics/without-comparison-inside-ignored, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_AggregationOverBinary(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, shape := range unwrapSumShapes {
		sel := fx.selLogfmt
		amount := "sum by (app, route) (rate(" + sel + " | logfmt | unwrap amount [" + shape.window + "]))"
		lines := "sum by (app, route) (count_over_time(" + sel + " | logfmt [" + shape.window + "]))"
		perApp := "sum by (app) (count_over_time(" + sel + " [" + shape.window + "]))"
		for _, query := range []string{
			"sum by (app) (" + amount + " / " + lines + ")",
			"avg by (route) (" + amount + " / " + lines + ")",
			"min(" + amount + " / " + lines + ")",
			"max without (route) (" + amount + " / " + lines + ")",
			"count without (route) (" + amount + " / " + lines + ")",
			"sum by (app) (" + lines + " * 2)",
			"max(2 * " + lines + ")",
			// A rate of the amount is about 0.22 per series on every window
			// (a line per 30 s per route, amounts 0.5 to 13): the thresholds
			// keep some series and drop others.
			"count(" + amount + " > 0.2)",
			"sum by (route) (" + amount + " > 0.1)",
			"sum(" + amount + " > bool 0.2)",
			"count(0.2 < " + amount + ")",
			"sum by (app) (" + lines + " > " + amount + ")",
			"max by (app) (" + amount + " >= bool " + lines + ")",
			"sum by (app) (" + lines + " / on (app) group_left " + perApp + ")",
			"max by (route) (" + lines + " / ignoring (route) group_left " + perApp + ")",
			"sum by (app) ((" + lines + " + " + lines + ") / 2)",
			"max(sum by (app) (" + amount + " / " + lines + "))",
			// A comparison inside a without filters before the sum
			// (semantics/without-comparison-inside-ignored).
			"sum without (route) (rate(" + sel + " | logfmt | unwrap amount [" + shape.window + "]) > 0.2)",
		} {
			t.Run(shape.name+"/"+query, func(t *testing.T) {
				loki := shape.run(t, fx, lokiURL, query)
				proxy := shape.run(t, fx, proxyURL, query)
				compareUnwrapSumSeries(t, query, loki, proxy)
				if shape.step == 0 {
					assertInstantVector(t, query, fx)
				}
			})
		}
	}
}

// TestCompat_AggregationOverBinaryMixedParsers: the registry's discovered shape
// (cluster f2066991): a label_replace around an aggregation over a binary whose
// operands parse with different parsers, JSON on one side and logfmt on the
// other, so the whole query names both; each operand keeps its own and the
// transform applies to the folded answer. The cluster's own /query is sent as
// recorded (over the setup data it may be empty on both, so it pins the answer's
// type and equality); the fixture's variant pins the values. An instant query
// only: a sum_over_time range query is registered apart
// (semantics/unwrap-over-time-range-windows).
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query
func TestCompat_AggregationOverBinaryMixedParsers(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	t.Run("cluster f2066991", func(t *testing.T) {
		at := time.Now()
		// unwrapInstant asserts a success without warnings from each.
		loki := unwrapInstant(t, lokiURL, aggregationOverBinaryCluster, at)
		proxy := unwrapInstant(t, proxyURL, aggregationOverBinaryCluster, at)
		if len(loki) != len(proxy) {
			t.Fatalf("Loki %v, proxy %v", loki, proxy)
		}
		for key, wv := range loki {
			w, werr := strconv.ParseFloat(wv, 64)
			g, gerr := strconv.ParseFloat(proxy[key], 64)
			if _, ok := proxy[key]; !ok || werr != nil || gerr != nil || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("{%s}: Loki %s, proxy %q (Loki %v, proxy %v)", key, wv, proxy[key], loki, proxy)
			}
		}
		assertInstantVectorAt(t, aggregationOverBinaryCluster, at)
	})
	shape := unwrapSumShape{name: "instant", window: "5m", stop: unwrapSumShapes[len(unwrapSumShapes)-1].stop}
	binary := "sum by (route) (sum_over_time(" + fx.selJSON + " | json | unwrap amount [5m])) / sum by (route) (count_over_time(" + fx.selLogfmt + " | logfmt [5m]))"
	inner := "sum by (route) (" + binary + ")"
	// The bare binary too: the parser probe cut it after its json operand's
	// pipeline and answered the left operand alone with a 200.
	for _, query := range []string{inner, `label_replace(` + inner + `, "dst", "$1", "route", "/(.*)")`, binary} {
		loki := shape.run(t, fx, lokiURL, query)
		proxy := shape.run(t, fx, proxyURL, query)
		compareUnwrapSumSeries(t, query, loki, proxy)
		assertInstantVector(t, query, fx)
	}
}

// aggregationOverBinaryCluster is the first query of cluster f2066991
// (conformance/registry/generated/parity-discovery.json).
const aggregationOverBinaryCluster = `label_replace( sum by (job) ( sum_over_time( {service_name="api-gateway"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] ) / count_over_time({service_name="api-gateway"} | logfmt | label_format foo=bar[5m]) ), "foo", "$1", "service", "(.*):.*" )`

// TestCompat_AggregationOverBinaryUnderLabelTransforms: a label_replace, and
// one over another that reads its destination, around an aggregation over a
// binary is applied to the folded answer, on every window shape and at an
// instant. (Loki's grammar has no label_join: pkg/logql/syntax/lex.go knows
// only label_replace, and Loki answers a parse error for it.)
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_AggregationOverBinaryUnderLabelTransforms(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, shape := range unwrapSumShapes {
		amount := "sum by (app, route) (rate(" + fx.selLogfmt + " | logfmt | unwrap amount [" + shape.window + "]))"
		lines := "sum by (app, route) (rate(" + fx.selLogfmt + " | logfmt [" + shape.window + "]))"
		inner := "sum by (route) (" + amount + " / " + lines + ")"
		for _, query := range []string{
			`label_replace(` + inner + `, "dst", "$1", "route", "/(.*)")`,
			`label_replace(label_replace(` + inner + `, "dst", "$1", "route", "/(.*)"), "dst2", "$1-x", "dst", "(.*)")`,
		} {
			t.Run(shape.name+"/"+query, func(t *testing.T) {
				compareUnwrapSumSeries(t, query, shape.run(t, fx, lokiURL, query), shape.run(t, fx, proxyURL, query))
			})
		}
	}
}

func assertInstantVector(t *testing.T, query string, fx *unwrapSumFixtures) {
	t.Helper()
	assertInstantVectorAt(t, query, fx.s0.Add(unwrapSumShapes[len(unwrapSumShapes)-1].stop))
}

func assertInstantVectorAt(t *testing.T, query string, at time.Time) {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	status, body, resp := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || extractMap(resp, "data")["resultType"] != "vector" {
		t.Fatalf("%s: proxy answered %d %s, want a vector", query, status, strings.TrimSpace(string(body)))
	}
}
