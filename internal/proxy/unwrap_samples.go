package proxy

import (
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	fj "github.com/valyala/fastjson"
)

// Unwrapped range functions read from VictoriaLogs stats. A step's sample is a function of the window
// (t-range, t] (LogQL docs, "Range Vector aggregation"); for a range up to the step VictoriaLogs' own step buckets
// hold it, and a longer range is merged from buckets of gcd(step, range) that each keep a partial aggregate:
//
//   - sum and the rate of an unwrapped label: the sum of the bucket (the rate divides the window's sum by its
//     seconds);
//   - max and min: the extreme of the bucket;
//   - avg: the sum and the sample count of the bucket;
//   - stddev and stdvar: the mean, the sample count and the population standard deviation of the bucket, combined
//     through their squared deviations;
//   - first and last: the value of the earliest or the latest row of the bucket.
//
// The raw evaluators read rows whole and convert the value themselves, so they take the query without the gate.

// withoutUnwrapGate is spec with the unwrap gate taken off its base query: the query the raw evaluators read.
func withoutUnwrapGate(spec statsCompatSpec) statsCompatSpec {
	spec.BaseQuery = spec.ungated()
	spec.Gate, spec.UnwrapField = "", ""
	return spec
}

// unwrapManualSpecs splits spec for the evaluator of manualFunc: the stats spec and the stats function that reads
// a bucket's partial aggregate (unwrapBucketAgg) when the function merges from buckets and the query holds the
// gate, and the spec the raw evaluator reads. A function that does not merge from buckets reads raw rows only.
func unwrapManualSpecs(spec statsCompatSpec, manualFunc string) (stats, raw statsCompatSpec, statsAggFunc string) {
	raw = withoutUnwrapGate(spec)
	if !spec.gated() {
		return raw, raw, ""
	}
	if agg := unwrapBucketAgg(manualFunc); agg != "" {
		return spec, raw, agg
	}
	return raw, raw, ""
}

// The stats a bucket keeps for the functions that merge from buckets. The value is the gate's converted field.
const (
	// avgBucketSum is the sum and the sample count of a bucket: avg over time divides the window's sum by its count.
	avgBucketSum = "sum(" + translate.UnwrapValueAlias + ") as __lvp_sum, count() as __sample_count"
	// deviationBucketName names the population standard deviation of a bucket.
	deviationBucketName = "__lvp_sd"
	// deviationMeanName names the mean of a bucket.
	deviationMeanName = "__lvp_avg"
	// deviationBucketStats is the mean, the sample count and the population standard deviation of a bucket.
	deviationBucketStats = "avg(" + translate.UnwrapValueAlias + ") as " + deviationMeanName + ", count() as __sample_count, stddev(" + translate.UnwrapValueAlias + ") as " + deviationBucketName
	// rowBucketInner names the row a first or last bucket picks (VictoriaLogs' row_min and row_max answer the row
	// as a JSON object of the fields asked for).
	rowBucketInner = "__lvp_inner"
	// firstBucketStats and lastBucketStats pick the earliest and the latest row of a bucket.
	firstBucketStats = "row_min(_time, " + translate.UnwrapValueAlias + ") as " + rowBucketInner
	lastBucketStats  = "row_max(_time, " + translate.UnwrapValueAlias + ") as " + rowBucketInner
	// rowBucketUnpack reads the value out of the picked row, and rowBucketValue takes it again per group: the
	// groups of a bucket hold one row each.
	rowBucketUnpack = "| unpack_json from " + rowBucketInner + " fields (" + translate.UnwrapValueAlias + ") | math " + translate.UnwrapValueAlias + " as " + rowBucketInner
	rowBucketValue  = "max(" + rowBucketInner + ") as c"
)

// rowBucketPick is what follows statsAggFunc in a bucket query: for first and last, the pick of the row's value
// grouped like the stats (by is the stats pipe up to its function, " | stats by (...) "); nothing otherwise.
func rowBucketPick(statsAggFunc, by string) string {
	if statsAggFunc != firstBucketStats && statsAggFunc != lastBucketStats {
		return ""
	}
	return " " + rowBucketUnpack + by + rowBucketValue
}

// unwrapBucketAgg is the stats function of a bucket for the window evaluator's function manualFunc, "" for one
// that does not merge from buckets.
func unwrapBucketAgg(manualFunc string) string {
	switch manualFunc {
	case "sum", "max", "min":
		return manualFunc + "(" + translate.UnwrapValueAlias + ") as c"
	case "avg":
		return avgBucketSum
	case "stddev", "stdvar":
		return deviationBucketStats
	case "first":
		return firstBucketStats
	case "last":
		return lastBucketStats
	case "unwrap_rate":
		return "sum(" + translate.UnwrapValueAlias + ") as c, count() as __sample_count"
	}
	return ""
}

// mergeUnwrapBuckets is the value of one window of manualFunc from the partial aggregates of the buckets it holds
// (ascending; counts and devs where the function keeps them). The rate's division by the window is the caller's.
func mergeUnwrapBuckets(manualFunc string, buckets, counts, devs []rangeMetricSample) float64 {
	switch manualFunc {
	case "first":
		return buckets[0].value
	case "last":
		return buckets[len(buckets)-1].value
	case "max":
		v := buckets[0].value
		for _, b := range buckets[1:] {
			if b.value > v || math.IsNaN(v) {
				v = b.value
			}
		}
		return v
	case "min":
		v := buckets[0].value
		for _, b := range buckets[1:] {
			if b.value < v || math.IsNaN(v) {
				v = b.value
			}
		}
		return v
	case "avg":
		var sum, n float64
		for _, b := range buckets {
			sum += b.value
		}
		for _, c := range counts {
			n += c.value
		}
		return sum / n
	case "stdvar":
		return mergeDeviationBuckets(buckets, counts, devs)
	case "stddev":
		return math.Sqrt(mergeDeviationBuckets(buckets, counts, devs))
	}
	var sum float64
	for _, b := range buckets {
		sum += b.value
	}
	return sum
}

// mergeDeviationBuckets is the population variance of a window from the mean, the sample count and the population
// standard deviation of each of its buckets: the window's sum of squared deviations is the buckets' own plus each
// bucket's count times the squared distance of its mean from the window's mean (two passes over the buckets, so no
// sum of squares is subtracted from another). Partials that do not line up, no bucket, or a value that is not
// finite (an infinite or NaN sample makes the deviation NaN) are NaN.
func mergeDeviationBuckets(means, counts, devs []rangeMetricSample) float64 {
	if len(means) == 0 || len(means) != len(counts) || len(means) != len(devs) {
		return math.NaN()
	}
	// x*0 is 0 for a finite x and NaN for an infinity or NaN: one sum tells a non-finite mean or deviation.
	var n, sum, nonFinite float64
	for i := range means {
		c, m := counts[i].value, means[i].value
		nonFinite += m*0 + devs[i].value*0
		n += c
		sum += c * m
	}
	if nonFinite != 0 {
		return math.NaN()
	}
	mean := sum / n
	var m2 float64
	for i := range means {
		d := means[i].value - mean
		m2 += counts[i].value * (devs[i].value*devs[i].value + d*d)
	}
	return m2 / n
}

// finite reports a number that is neither infinite nor NaN.
func finite(v float64) bool { return !math.IsInf(v, 0) && !math.IsNaN(v) }

// deviationPartials gathers the three stats results of the deviation buckets (deviationBucketStats) per series
// and per VictoriaLogs group, and folds them into the series' samples, counts and deviations.
type deviationPartials struct {
	series map[string]*deviationSeries
	order  []string
}

// deviationSeries is one series' VictoriaLogs groups, in the order they came.
type deviationSeries struct {
	metric map[string]string
	groups map[string]*deviationGroup
	order  []*deviationGroup
}

// deviationGroup is one VictoriaLogs group of a series: its mean, count and deviation per bucket.
type deviationGroup struct {
	means, counts, devs []rangeMetricSample
}

// deviationPoint is a bucket's partial: the sample count, the mean and the sum of squared deviations.
type deviationPoint struct {
	ts          int64
	n, mean, m2 float64
}

// newDeviationPartials returns the gatherer for the stats statsAggFunc, nil for stats that are not the deviation
// buckets.
func newDeviationPartials(statsAggFunc string) *deviationPartials {
	if statsAggFunc != deviationBucketStats {
		return nil
	}
	return &deviationPartials{}
}

// deviationResults is the number of stats results per series the deviation buckets add to the mean and the count.
func deviationResults(statsAggFunc string) int {
	if statsAggFunc == deviationBucketStats {
		return 1
	}
	return 0
}

// add reads one stats result (name, its points) of the group group of the series seriesKey.
func (d *deviationPartials) add(seriesKey, group string, metric map[string]string, name string, values []byte, start time.Time, step time.Duration) {
	if d.series == nil {
		d.series = map[string]*deviationSeries{}
	}
	s, ok := d.series[seriesKey]
	if !ok {
		s = &deviationSeries{metric: metric, groups: map[string]*deviationGroup{}}
		d.series[seriesKey] = s
		d.order = append(d.order, seriesKey)
	}
	g, ok := s.groups[group]
	if !ok {
		g = &deviationGroup{}
		s.groups[group] = g
		s.order = append(s.order, g)
	}
	switch name {
	case deviationMeanName:
		g.means = appendBucketSamples(g.means, values, start, step)
	case "__sample_count":
		g.counts = appendBucketSamples(g.counts, values, start, step)
	case deviationBucketName:
		g.devs = appendBucketSamples(g.devs, values, start, step)
	}
}

// fold writes every series' buckets into series: per bucket the groups of a series combine into one mean, count
// and deviation. A nil gatherer folds nothing.
func (d *deviationPartials) fold(series map[string]manualSeriesSamples) {
	if d == nil {
		return
	}
	var points []deviationPoint
	for _, key := range d.order {
		s := d.series[key]
		points = points[:0]
		for _, g := range s.order {
			points = g.appendPoints(points)
		}
		if len(s.order) > 1 {
			sort.SliceStable(points, func(i, j int) bool { return points[i].ts < points[j].ts })
		}
		buckets := 0
		for i := range points {
			if i == 0 || points[i].ts != points[i-1].ts {
				buckets++
			}
		}
		entry := series[key]
		if entry.Metric == nil {
			entry.Metric = s.metric
		}
		entry.Samples = slices.Grow(entry.Samples, buckets)
		entry.Counts = slices.Grow(entry.Counts, buckets)
		entry.Deviations = slices.Grow(entry.Deviations, buckets)
		if entry.PresentBuckets == nil {
			entry.PresentBuckets = make([]int64, 0, buckets)
		}
		for i := 0; i < len(points); {
			acc := points[i]
			j := i + 1
			for ; j < len(points) && points[j].ts == acc.ts; j++ {
				acc = combineDeviation(acc, points[j])
			}
			i = j
			dev := math.NaN()
			if acc.n > 0 {
				dev = math.Sqrt(acc.m2 / acc.n)
			}
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: acc.ts, value: acc.mean})
			entry.Counts = append(entry.Counts, rangeMetricSample{ts: acc.ts, value: acc.n})
			entry.Deviations = append(entry.Deviations, rangeMetricSample{ts: acc.ts, value: dev})
			entry.PresentBuckets = append(entry.PresentBuckets, acc.ts)
		}
		series[key] = entry
	}
}

// appendPoints appends the group's buckets that hold samples (a count above zero): the mean and the deviation of
// the same bucket, NaN where VictoriaLogs answered none.
func (g *deviationGroup) appendPoints(dst []deviationPoint) []deviationPoint {
	at := func(samples []rangeMetricSample, ts int64) float64 {
		i := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= ts })
		if i < len(samples) && samples[i].ts == ts {
			return samples[i].value
		}
		return math.NaN()
	}
	dst = slices.Grow(dst, len(g.counts))
	for _, c := range g.counts {
		if math.IsNaN(c.value) || c.value <= 0 {
			continue
		}
		mean, dev := at(g.means, c.ts), at(g.devs, c.ts)
		dst = append(dst, deviationPoint{ts: c.ts, n: c.value, mean: mean, m2: dev * dev * c.value})
	}
	return dst
}

// combineDeviation is the partial of the samples of a and b together.
func combineDeviation(a, b deviationPoint) deviationPoint {
	if a.n == 0 {
		return deviationPoint{ts: b.ts, n: b.n, mean: b.mean, m2: b.m2}
	}
	if b.n == 0 {
		return a
	}
	n := a.n + b.n
	delta := b.mean - a.mean
	return deviationPoint{ts: a.ts, n: n, mean: a.mean + delta*b.n/n, m2: a.m2 + b.m2 + delta*delta*a.n*b.n/n}
}

// statsGroupID names the VictoriaLogs group of a stats result: every field of its metric but the result's name,
// sorted, so the results of one group pair up whatever order VictoriaLogs writes them in.
func statsGroupID(metric *fj.Object) string {
	if metric == nil {
		return ""
	}
	var parts []string
	metric.Visit(func(key []byte, v *fj.Value) {
		if string(key) == "__name__" {
			return
		}
		parts = append(parts, string(key)+"="+string(v.GetStringBytes()))
	})
	sort.Strings(parts)
	return strings.Join(parts, "\xff")
}

// unwrapWindowRoute decides how the window evaluator reads spec: windows merged from buckets (ok), and among them
// the ones VictoriaLogs groups by the outer labels (grouped, routed is that spec). Otherwise routed is spec and the
// step buckets answer.
func (p *Proxy) unwrapWindowRoute(r *http.Request, spec statsCompatSpec, origSpec originalRangeMetricSpec, hasOrigSpec bool, manualFunc, originalLogql string, step time.Duration) (routed statsCompatSpec, grouped, ok bool) {
	if !hasOrigSpec {
		return spec, false, false
	}
	if identityAhead(r.Context()) {
		// A parser's identity whose hash stands before the gate: its stats group by the series hash, and the window
		// evaluator answers it whatever the window (foldDynamicIdentity folds the series and records the budget).
		return spec, false, true
	}
	query := r.FormValue("query")
	if !unwrapWindowBuckets(spec, origSpec, manualFunc, originalLogql, query, step) {
		return spec, false, false
	}
	if manualFunc == "sum" && unwrapSumOfSums(query) {
		// The sum of the series' sums over time is the sum of the lines: one ungrouped bucket series.
		spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit = nil, nil, true
		return spec, true, true
	}
	if pushed, ok := p.unwrapGroupPushdown(spec, manualFunc, query); ok {
		return pushed, true, true
	}
	if slices.Contains(spec.GroupBy, "_msg") || queryUsesParserStages(spec.ungated()) && slices.Contains(spec.GroupBy, "_stream") &&
		!p.listedIdentityWindows(r.Context(), query) {
		// A parser names a series by its stream and the labels it extracts: the window evaluator folds no aggregation
		// over such series from buckets (semantics/unwrap-over-time-range-windows-residual-forms).
		return spec, false, false
	}
	if unwrapFoldsSeries(query) {
		// The vector aggregations fold the series after their windows, and Loki counts the folded series against
		// max_query_series: the series read per stream before the fold are bounded by the raw-row budget, as a
		// without's are (the fold's answer keeps the limit).
		p.recordFoldBudget(r.Context())
	}
	return spec, false, true
}

// unwrapFoldsSeries reports a query whose root (an outer label_replace aside) is a vector aggregation.
func unwrapFoldsSeries(query string) bool {
	expr, ok := sharedLogQLExpr(stripOuterLabelReplace(strings.TrimSpace(query)))
	if !ok {
		return false
	}
	_, ok = expr.(*logqlpkg.VectorAggregation)
	return ok
}

// unwrapSumOfSums reports a sum by no labels directly over sum_over_time.
func unwrapSumOfSums(query string) bool {
	expr, ok := sharedLogQLExpr(strings.TrimSpace(query))
	if !ok {
		return false
	}
	va, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || va.Op != logqlpkg.VectorSum || va.Grouping != nil && (va.Grouping.Without || len(va.Grouping.Labels) > 0) {
		return false
	}
	ra, ok := va.Inner.(*logqlpkg.RangeAggregation)
	return ok && ra.Op == logqlpkg.RangeSumOverTime && ra.Grouping == nil
}

// unwrapGroupPushdownOff turns the outer-label grouping off (tests compare it with the per-series fold).
var unwrapGroupPushdownOff bool

// unwrapGroupPushdown groups the buckets of spec by the outer labels of query in VictoriaLogs: a max of
// max_over_time or a min of min_over_time (decomposableUnwrapAggregation) takes the extreme of the group's lines,
// which is the extreme of its series' extremes, so no series is read. A level label (derived per series) or a
// label the stats would not keep as the clause names it keeps the per-series read.
func (p *Proxy) unwrapGroupPushdown(spec statsCompatSpec, manualFunc, query string) (statsCompatSpec, bool) {
	if unwrapGroupPushdownOff || (manualFunc != "max" && manualFunc != "min") {
		return spec, false
	}
	va, ok := decomposableUnwrapAggregation(query, manualFunc)
	if !ok {
		return spec, false
	}
	var labels []string
	if va.Grouping != nil {
		labels = va.Grouping.Labels
	}
	if slices.ContainsFunc(labels, func(name string) bool { return name == "level" || name == translate.DetectedLevelLabel }) {
		return spec, false
	}
	var label translate.LabelTranslateFunc
	if p.labelTranslator != nil {
		label = p.labelTranslator.ToVL
	}
	kept := translate.AppendNormalizedLabels(nil, labels, label)
	if len(kept) != len(labels) || slices.ContainsFunc(kept, func(name string) bool { return name == "_stream" || name == "_msg" }) {
		return spec, false
	}
	spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit = kept, slices.Clone(labels), len(kept) == 0
	return spec, true
}

// decomposableUnwrapAggregation returns the vector aggregation at the root of query when it is the extreme
// manualFunc ("max" or "min") by labels or none, and reports whether it is directly over a range aggregation with
// no grouping of its own. Anything else is nil.
func decomposableUnwrapAggregation(query, manualFunc string) (*logqlpkg.VectorAggregation, bool) {
	expr, ok := sharedLogQLExpr(strings.TrimSpace(query))
	if !ok {
		return nil, false
	}
	va, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || string(va.Op) != manualFunc || (manualFunc != "max" && manualFunc != "min") || va.Grouping != nil && va.Grouping.Without {
		return nil, false
	}
	ra, ok := va.Inner.(*logqlpkg.RangeAggregation)
	return va, ok && ra.Grouping == nil
}

// unwrapWindowBuckets reports a query whose windows the evaluator merges from buckets: an unwrap function that
// keeps a partial aggregate per bucket (unwrapBucketAgg), a range longer than the step, a gated stats query, and a
// form the evaluator applies after the window (unwrapWindowForm).
func unwrapWindowBuckets(spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc, originalLogql, query string, step time.Duration) bool {
	if !origSpec.HasUnwrap || origSpec.Window <= step || unwrapBucketAgg(manualFunc) == "" || !spec.gated() {
		return false
	}
	form, rangeBy := unwrapWindowForm(query)
	// The deviation buckets and the rows first and last pick fold no aggregation over a range aggregation grouped by
	// its own labels: those keep the step buckets (registered as semantics/aggregation-over-range-by-not-applied).
	rowOrDeviation := manualFunc == "stddev" || manualFunc == "stdvar" || manualFunc == "first" || manualFunc == "last"
	return form && (!rangeBy || !rowOrDeviation)
}

// unwrapWindowForm reads query (an outer label_replace aside): form is a range aggregation alone, under topk or
// bottomk, under one of sum, avg, min, max and count, or under a count, min, max or avg of such an aggregation that
// is not a without (each optionally under topk or bottomk); rangeBy is a range aggregation with a grouping of its own
// under one of those aggregations.
func unwrapWindowForm(query string) (form, rangeBy bool) {
	expr, ok := sharedLogQLExpr(stripOuterLabelReplace(strings.TrimSpace(query)))
	if !ok {
		return false, false
	}
	if va, ok := expr.(*logqlpkg.VectorAggregation); ok && (va.Op == logqlpkg.VectorTopK || va.Op == logqlpkg.VectorBottomK) {
		if va.Grouping != nil && va.Grouping.Without {
			return false, false
		}
		expr = va.Inner
	}
	var chain []*logqlpkg.VectorAggregation
	for {
		va, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		switch va.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount:
		default:
			return false, false
		}
		if va.Grouping != nil && va.Grouping.Without && len(va.Grouping.Labels) == 0 {
			// A without of no label keeps every label: the translator writes it per series.
			return false, false
		}
		chain = append(chain, va)
		expr = va.Inner
	}
	ra, ok := expr.(*logqlpkg.RangeAggregation)
	if !ok {
		return false, false
	}
	rangeBy = len(chain) > 0 && ra.Grouping != nil
	switch len(chain) {
	case 0, 1:
		return true, rangeBy
	case 2:
		outer, inner := chain[0], chain[1]
		seriesLevel := outer.Op != logqlpkg.VectorSum
		return seriesLevel && (inner.Grouping == nil || !inner.Grouping.Without), rangeBy
	}
	return false, rangeBy
}
