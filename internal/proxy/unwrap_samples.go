package proxy

import (
	"math"
	"slices"
	"sort"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Unwrap sample validity (Loki v3.7.7 pkg/logql/log/metrics_extraction.go,
// streamLabelSampleExtractor.Process): a line whose unwrapped label is absent
// or empty makes no sample, and a value the conversion rejects is not a sample
// either (it carries __error__="SampleExtractionErr", which `| __error__=""`
// drops). VictoriaLogs' stats functions parse leniently instead (a unit such as
// "86282s" or "1KiB" becomes a number, max and min compare strings, a group
// without a value answers "" or NaN), so a stats pipe over an unwrapped label
// reads only the rows the translator selected and converted in Loki's syntax
// (translator.UnwrapGateFor, plain, duration() and bytes()). The raw evaluators
// (quantile, rate_counter, first, last, the bare-parser rows) convert with Go's
// own parsing (convertUnwrap).

// withoutUnwrapGate returns spec with the unwrap gate off its base query: the
// raw evaluators read the rows whole and convert the value themselves.
func withoutUnwrapGate(spec statsCompatSpec) statsCompatSpec {
	if base, _, ok := translator.SplitUnwrapGate(spec.BaseQuery); ok {
		spec.BaseQuery = base
	}
	return spec
}

// unwrapManualSpecs splits the spec of a manual range metric into the one the
// stats buckets read and the one the raw evaluator reads. The raw evaluator
// reads rows whole and converts the value itself, so it never sees the unwrap
// gate. The stats of an unwrap rate and of sum, avg, max and min over time keep
// it: their windows merge from buckets of a partial aggregate with the sample
// count as presence (unwrapBucketAgg). Every other metric reads the ungated spec
// on both routes.
func unwrapManualSpecs(spec statsCompatSpec, manualFunc string) (stats, raw statsCompatSpec, statsAggFunc string) {
	raw = withoutUnwrapGate(spec)
	if agg := unwrapBucketAgg(manualFunc); agg != "" && spec.BaseQuery != raw.BaseQuery {
		return spec, raw, agg
	}
	return raw, raw, ""
}

// isUnwrapBucketFunc reports the unwrap range functions whose windows merge
// from buckets of a partial aggregate (mergeUnwrapBuckets).
func isUnwrapBucketFunc(manualFunc string) bool {
	switch manualFunc {
	case "unwrap_rate", "sum", "avg", "max", "min":
		return true
	}
	return false
}

// avgBucketSum names the bucket sum of an average, whose sample counts the
// collector keeps as values (unwrapBucketKeepsCounts) to divide by.
const avgBucketSum = "sum(" + translator.UnwrapValueAlias + ") as __lvp_sum, count() as __sample_count"

// unwrapBucketAgg is the stats of one bucket of an unwrap range function whose
// windows merge from buckets: its partial aggregate (the sum for rate, sum and
// avg over time, the extreme for max and min) and the sample count, which
// proves presence and divides an average. "" for every other function.
func unwrapBucketAgg(manualFunc string) string {
	switch manualFunc {
	case "unwrap_rate", "sum":
		return "sum(" + translator.UnwrapValueAlias + ") as c, count() as __sample_count"
	case "avg":
		return avgBucketSum
	case "max", "min":
		return manualFunc + "(" + translator.UnwrapValueAlias + ") as c, count() as __sample_count"
	}
	return ""
}

// unwrapBucketKeepsCounts reports whether the buckets of statsAggFunc keep their
// sample counts as values: the sum of an average.
func unwrapBucketKeepsCounts(statsAggFunc string) bool {
	return statsAggFunc == avgBucketSum
}

// unwrapWindowBuckets reports whether the manual evaluator answers sum, avg, max
// or min over time with unwrap from anchored buckets of their partial aggregates
// (collectStatsFastPathHits): a gated query, bare or under one sum (the outer
// aggregation the evaluator applies), whose series the stats group: an
// ungrouped sum over time summed upstream, a by() grouping, or the streams of a
// query without parser stages. A parser query read per stream, or per stream
// and line (_msg), keeps the native step buckets: its series are not a bounded
// stats grouping. originalLogql is the request's query with Grafana's tokens
// resolved, query the request's own text, which the evaluator reads
// (isSumAllLogRange).
func unwrapWindowBuckets(spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc, originalLogql, query string) bool {
	switch manualFunc {
	case "sum", "avg", "max", "min":
	default:
		return false
	}
	if !origSpec.HasUnwrap || origSpec.Window <= 0 || !rangeUnderSumOrBare(originalLogql) {
		return false
	}
	if _, _, gated := translator.SplitUnwrapGate(spec.BaseQuery); !gated {
		return false
	}
	if isSumAllLogRange(query) {
		return true
	}
	if len(spec.GroupBy) == 0 && !spec.ByExplicit {
		return false
	}
	if slices.Contains(spec.GroupBy, "_msg") {
		return false // a series per distinct line: the buckets would hold every line
	}
	return !queryUsesParserStages(spec.BaseQuery) || !slices.Contains(spec.GroupBy, "_stream")
}

// rangeUnderSumOrBare reports whether the query is a range aggregation, bare or
// under one sum (by, without or none), inside topk or bottomk and label_replace.
func rangeUnderSumOrBare(query string) bool {
	expr, err := logqlpkg.Parse(stripOuterLabelReplace(strings.TrimSpace(query)))
	if err != nil {
		return false
	}
	if agg, ok := expr.(*logqlpkg.VectorAggregation); ok && (agg.Op == logqlpkg.VectorTopK || agg.Op == logqlpkg.VectorBottomK) {
		expr = agg.Inner
	}
	switch e := expr.(type) {
	case *logqlpkg.RangeAggregation:
		return true
	case *logqlpkg.VectorAggregation:
		_, ok := e.Inner.(*logqlpkg.RangeAggregation)
		return ok && e.Op == logqlpkg.VectorSum
	}
	return false
}

// mergeUnwrapBuckets merges the partial aggregates of the buckets of one window,
// in time order, into Loki's value over the window (pkg/logql/range_vector.go
// sumOverTime, avgOverTime, maxOverTime, minOverTime): the sum (also of a rate
// before its division), the sum over the window's sample count, or the extreme,
// where a NaN gives way like in Loki. A first or last over time would take its
// first or last bucket's value: the earliest or latest row of the window.
func mergeUnwrapBuckets(manualFunc string, buckets, counts []rangeMetricSample) float64 {
	value := buckets[0].value
	for _, b := range buckets[1:] {
		switch manualFunc {
		case "max":
			if b.value > value || math.IsNaN(value) {
				value = b.value
			}
		case "min":
			if b.value < value || math.IsNaN(value) {
				value = b.value
			}
		default:
			value += b.value
		}
	}
	if manualFunc == "avg" {
		var n float64
		for _, c := range counts {
			n += c.value
		}
		value /= n
	}
	return value
}

// windowSamples returns the ascending samples labelled in [from, to).
func windowSamples(samples []rangeMetricSample, from, to int64) []rangeMetricSample {
	lo := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= from })
	hi := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= to })
	return samples[lo:hi]
}

// queryWithoutUnwrapGate returns a translated query without the unwrap gate
// that ends the pipes before its first stats pipe.
func queryWithoutUnwrapGate(query string) string {
	idx := strings.Index(query, " | stats ")
	if idx < 0 {
		return query
	}
	if base, _, ok := translator.SplitUnwrapGate(query[:idx]); ok {
		return base + query[idx:]
	}
	return query
}
