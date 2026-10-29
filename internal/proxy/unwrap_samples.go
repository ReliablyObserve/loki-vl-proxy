package proxy

import (
	"math"
	"net/http"
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
// avg over time, the extreme for max and min over time). VictoriaLogs answers a
// bucket only when it holds a row, so the aggregate's point is the presence of
// sum, max and min over time; an average also takes the sample count to divide
// by, and rate keeps it as presence. "" for every other function.
func unwrapBucketAgg(manualFunc string) string {
	switch manualFunc {
	case "unwrap_rate":
		return "sum(" + translator.UnwrapValueAlias + ") as c, count() as __sample_count"
	case "sum", "max", "min":
		return manualFunc + "(" + translator.UnwrapValueAlias + ") as c"
	case "avg":
		return avgBucketSum
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
// from merged buckets (unwrapWindowBuckets). When an outer aggregation folds
// the series the buckets hold afterwards (foldUngroupedSum,
// foldVectorAggregations), Loki counts its series limit on the folded answer:
// the reads are bounded by the raw-row budget, as for a `without`
// (withWithoutFold records it itself).
//
// grouped reports that the returned spec groups the buckets by the outer
// labels (unwrapGroupPushdown): the evaluator answers the outer aggregation
// itself and nothing folds afterwards.
func (p *Proxy) unwrapWindowRoute(r *http.Request, spec statsCompatSpec, origSpec originalRangeMetricSpec, hasOrigSpec bool, manualFunc, originalLogql string, step time.Duration) (routed statsCompatSpec, grouped, ok bool) {
	query := r.FormValue("query")
	if !hasOrigSpec {
		return spec, false, false
	}
	if pushed, ok := p.unwrapGroupPushdown(spec, manualFunc, query); ok && !unwrapGroupPushdownOff && unwrapWindowBuckets(pushed, origSpec, manualFunc, originalLogql, query, step) {
		return pushed, true, true
	}
	if !unwrapWindowBuckets(spec, origSpec, manualFunc, originalLogql, query, step) {
		return spec, false, false
	}
	if _, chain := seriesAggregationChain(query); chain {
		p.recordFoldBudget(r.Context())
	} else if _, ungrouped := ungroupedSumRange(query); ungrouped && !isSumAllLogRange(query) {
		p.recordFoldBudget(r.Context())
	}
	return spec, false, true
}

// unwrapGroupPushdownOff turns unwrapGroupPushdown off, so tests can compare
// its answers with the per-stream fold's.
var unwrapGroupPushdownOff bool

// unwrapGroupPushdown returns spec grouped by L for `max by (L)
// (max_over_time(...))` and `min by (L) (min_over_time(...))`, L possibly
// empty, under an optional label_replace. Loki
// takes the extreme per series over the window and then per group of L
// (pkg/logql/range_vector.go:405-420 maxOverTime, minOverTime;
// pkg/logql/evaluator.go VectorAggEvaluator.Next, a NaN giving way in both),
// which is the extreme of the group's samples in the window: VictoriaLogs takes
// it per bucket and group of L, the proxy over the window's buckets, and no
// series per stream (or per line, for a parser query) is read or folded. Loki
// counts max_query_series on the groups (pkg/logql/engine.go:530-590
// JoinSampleVector). The translated grouping must be L and the series
// identity (_stream, _msg, level); anything else is not pushed down.
func (p *Proxy) unwrapGroupPushdown(spec statsCompatSpec, manualFunc, query string) (statsCompatSpec, bool) {
	agg, ok := decomposableUnwrapAggregation(query, manualFunc)
	if !ok {
		return spec, false
	}
	var labels []string
	if agg.Grouping != nil {
		labels = agg.Grouping.Labels
	}
	for _, label := range labels {
		if label == "detected_level" || label == "level" {
			// The level a stream's series is labelled by (unknown when it has
			// none) comes from the per-stream read (buildMetricSeriesEntry).
			return spec, false
		}
	}
	vlLabels := make(map[string]bool, len(labels))
	for _, label := range labels {
		vlLabels[label] = true
		if p.labelTranslator != nil {
			vlLabels[p.labelTranslator.ToVL(label)] = true
		}
	}
	var kept []string
	for _, g := range spec.GroupBy {
		// The series identity the translation adds after the outer labels: the
		// stream, the line, and the stored or derived level a stream's series
		// is labelled by. The outer max/min discards these inner labels.
		if g == "_stream" || g == "_msg" || (g == "level" && !vlLabels[g]) ||
			(g == "detected_level" && !vlLabels[g]) {
			continue
		}
		kept = append(kept, g)
	}
	if len(kept) != len(labels) {
		return spec, false
	}
	// The Loki name of each kept field, in the translation's order, which the
	// collector pairs by position (collectRangeMetricHitsAux vlToLoki).
	names := make([]string, len(kept))
	for i, g := range kept {
		for _, label := range labels {
			if label == g || (p.labelTranslator != nil && p.labelTranslator.ToVL(label) == g) {
				names[i] = label
			}
		}
		if names[i] == "" {
			return spec, false
		}
	}
	spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit = kept, names, len(labels) == 0
	return spec, true
}

// decomposableUnwrapAggregation returns the outer max or min of `max by (L)
// (max_over_time(...))` or `min by (L) (min_over_time(...))` (by or none, not
// without), under an optional label_replace. A ranking around it is applied
// to the answer by its caller (handleRangeMetricPostAggregation).
func decomposableUnwrapAggregation(query, manualFunc string) (*logqlpkg.VectorAggregation, bool) {
	if manualFunc != "max" && manualFunc != "min" {
		return nil, false
	}
	expr, err := logqlpkg.Parse(stripOuterLabelReplace(strings.TrimSpace(query)))
	if err != nil {
		return nil, false
	}
	agg, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || string(agg.Op) != manualFunc || (agg.Grouping != nil && agg.Grouping.Without) {
		return nil, false
	}
	ra, ok := agg.Inner.(*logqlpkg.RangeAggregation)
	return agg, ok && ra.Grouping == nil
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
