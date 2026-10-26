package proxy

import (
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

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
// count as presence (unwrapBucketAgg, mergeUnwrapBuckets). Every other metric
// reads the ungated spec on both routes.
func unwrapManualSpecs(spec statsCompatSpec, manualFunc string) (stats, raw statsCompatSpec, statsAggFunc string) {
	raw = withoutUnwrapGate(spec)
	if agg := unwrapBucketAgg(manualFunc); agg != "" && spec.BaseQuery != raw.BaseQuery {
		return spec, raw, agg
	}
	return raw, raw, ""
}

// avgBucketSum is the bucket stats of avg over time: the sum and the sample
// count, which the collector keeps as values (Counts) to divide by.
const avgBucketSum = "sum(" + translator.UnwrapValueAlias + ") as __lvp_sum, count() as __sample_count"

// unwrapBucketAgg is the stats of one bucket of an unwrap range function whose
// windows merge from buckets: its partial aggregate (the sum for rate, sum and
// avg over time, the extreme for max and min over time) and the sample count,
// which proves presence and divides an average. "" for every other function.
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

// mergeUnwrapBuckets merges the partial aggregates of the buckets of one window
// into Loki's value over the window (pkg/logql/range_vector.go:371-420
// sumOverTime, avgOverTime, maxOverTime, minOverTime): the sum (of a rate too,
// before its division), the sum over the window's sample count, or the
// extreme, where a NaN gives way as in Loki. Every bucket holds a sample.
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

// unwrapWindowRoute reports whether the window evaluator answers the request
// from merged buckets (unwrapWindowBuckets). An outer aggregation folds the
// series the buckets hold afterwards (foldUngroupedSum, foldVectorAggregations,
// withWithoutFold), and Loki counts its series limit on the folded answer: the
// reads are bounded by the raw-row budget, as for a `without`.
func (p *Proxy) unwrapWindowRoute(r *http.Request, spec statsCompatSpec, origSpec originalRangeMetricSpec, hasOrigSpec bool, manualFunc, originalLogql string, step time.Duration) bool {
	if !hasOrigSpec || !unwrapWindowBuckets(spec, origSpec, manualFunc, originalLogql, r.FormValue("query"), step) {
		return false
	}
	p.recordFoldBudget(r.Context())
	return true
}

// unwrapWindowBuckets reports whether the window evaluator answers sum, avg,
// max or min over time with unwrap from anchored buckets of their partial
// aggregates (unwrapBucketAgg): a range longer than the step, whose windows
// overlap, so one step bucket per point holds only the newest step of Loki's
// window (t-range, t] (pkg/logql/range_vector.go:102-189). A range up to the
// step stays on the native step buckets, which hold exactly one window each
// (the window phase filter keeps a shorter range's lines). The query is gated
// and its series are a stats grouping the evaluator reads from buckets
// (collectStatsFastPathHits): an ungrouped sum of sum over time, which
// VictoriaLogs sums, labels it groups by, or the streams of a query without
// parser stages. The streams of a parser query, and its lines (_msg), are no
// bounded grouping and keep the native step buckets, as do the aggregations
// the evaluator does not apply after the window (unwrapWindowForm).
func unwrapWindowBuckets(spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc, originalLogql, query string, step time.Duration) bool {
	switch manualFunc {
	case "sum", "avg", "max", "min":
	default:
		return false
	}
	if !origSpec.HasUnwrap || step <= 0 || origSpec.Window <= step || !unwrapWindowForm(originalLogql) {
		return false
	}
	if _, _, gated := translator.SplitUnwrapGate(spec.BaseQuery); !gated {
		return false
	}
	if isSumAllLogRange(query) {
		return true
	}
	if len(spec.GroupBy) == 0 && !spec.ByExplicit && !hasOuterAggregationWithoutBy(originalLogql) {
		return false
	}
	parser := queryUsesParserStages(spec.BaseQuery)
	for _, g := range spec.GroupBy {
		if g == "_msg" || (g == "_stream" && parser) {
			return false
		}
	}
	return true
}

// unwrapWindowForm reports whether the aggregations around the range
// aggregation are ones the window evaluator applies after the window: none, one
// sum (by, none or a labelled without, folded by withWithoutFold), one count,
// min, max or avg (by, none or a labelled without), or a chain of them without
// a `without` that foldVectorAggregations folds (seriesAggregationChain), under
// an optional topk or bottomk by labels and label_replace. A nested `without`,
// an empty `without ()` and a ranking `without` are answered as before.
func unwrapWindowForm(query string) bool {
	expr, err := logqlpkg.Parse(stripOuterLabelReplace(strings.TrimSpace(query)))
	if err != nil {
		return false
	}
	if agg, ok := expr.(*logqlpkg.VectorAggregation); ok && (agg.Op == logqlpkg.VectorTopK || agg.Op == logqlpkg.VectorBottomK) {
		if agg.Grouping != nil && agg.Grouping.Without {
			return false
		}
		expr = agg.Inner
	}
	var chain []*logqlpkg.VectorAggregation
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		switch agg.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorCount, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorAvg:
		default:
			return false
		}
		if agg.Grouping != nil && agg.Grouping.Without && (len(agg.Grouping.Labels) == 0 || len(chain) > 0 || !isRangeAggregation(agg.Inner)) {
			return false
		}
		chain = append(chain, agg)
		expr = agg.Inner
	}
	if !isRangeAggregation(expr) {
		return false
	}
	if len(chain) <= 1 {
		return true
	}
	for _, agg := range chain {
		if agg.Op != logqlpkg.VectorSum {
			return true // a series-level chain (seriesAggregationChain)
		}
	}
	return false // sums over sums: the evaluator reads the innermost only
}

func isRangeAggregation(expr logqlpkg.Expr) bool {
	_, ok := expr.(*logqlpkg.RangeAggregation)
	return ok
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
