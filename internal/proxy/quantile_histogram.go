package proxy

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/jsonparser"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// quantile_over_time (Loki v3.7.7 pkg/logql/range_vector.go:449-487,
// quantileOverTime and Quantile) sorts every sample of the window (t-range, t]
// and interpolates linearly at rank phi*(n-1). VictoriaLogs' quantile stats
// function is not that: it answers the value at rank int(phi*n) without
// interpolation, keeps a random sample above 10,000 values per shard
// (lib/logstorage/stats_quantile.go, the same from v1.24 to v1.50), and a
// quantile of buckets does not combine into a sliding window.
//
// VictoriaLogs answers what it answers exactly instead: the number of rows per
// converted value (the unwrap gate's alias) in every anchored bucket, grouped
// like the series, `stats by (<series fields>, __lvp_v) count()`. Counts add up,
// so the proxy merges the buckets of each window, tumbling or sliding, and
// interpolates like Loki over the merged counts. The answer is as large as the
// distinct values per bucket and series, never more than the rows: it is read
// within -backend-max-buffered-response-bytes and holds at most
// -manual-range-metric-row-limit counts, each over with its documented error.
// The stats_query_range answer holds a series per (series, value); it is read
// whole, not cut at -max-stats-query-series, which bounds the series the
// response returns instead.

// quantileCountName is the stats result holding the rows of one value.
const quantileCountName = "__lvp_c"

// quantileSample is the number of rows holding one value in one bucket,
// labelled by the bucket's left edge (ns).
type quantileSample struct {
	ts    int64
	value float64
	count float64
}

type quantileSeries struct {
	Metric  map[string]string
	Samples []quantileSample // ascending ts
}

// quantileGroupsByFields reports whether the quantile's series are the groups of
// the query's own fields (a range aggregation's by (...), by () included):
// VictoriaLogs then groups the counts by those fields alone. Otherwise the
// series are the streams, derived like the raw evaluator's from the stream and
// level (manualSeriesEntry).
func quantileGroupsByFields(spec statsCompatSpec) bool {
	if len(spec.GroupBy) == 0 {
		return spec.ByExplicit
	}
	for _, g := range spec.GroupBy {
		if g == "_stream" {
			return false
		}
	}
	return true
}

// quantileStatsGroupBy returns the fields VictoriaLogs groups the counts by: the
// series fields and the value.
func quantileStatsGroupBy(spec statsCompatSpec) []string {
	var by []string
	perStream := !quantileGroupsByFields(spec)
	if perStream {
		by = append(by, "_stream", "level")
	}
	for _, g := range spec.GroupBy {
		if isVLInternalField(g) || (g == "level" && perStream) {
			continue
		}
		by = append(by, quoteLogsQLIdent(g))
	}
	return append(by, translator.UnwrapValueAlias)
}

// collectQuantileHistogram asks VictoriaLogs for the value counts of the gated
// query in spec. With bucket > 0 the counts are per stats_query_range bucket of
// that size anchored at start (setSlidingStatsRangeParams), labelled by their
// left edge; with bucket 0 they cover the one window (start, end] through
// stats_query and carry the label start.
func (p *Proxy) collectQuantileHistogram(ctx context.Context, spec statsCompatSpec, start, end time.Time, bucket time.Duration) (map[string]quantileSeries, error) {
	rowLimit, err := p.manualMetricRowBudget()
	if err != nil {
		return nil, err
	}
	body, err := p.fetchQuantileCounts(ctx, spec, start, end, bucket)
	if err != nil {
		return nil, err
	}

	byFields := quantileGroupsByFields(spec)
	vlToLoki := make(map[string]string, len(spec.GroupBy))
	for i, name := range spec.GroupBy {
		vlToLoki[name] = name
		if i < len(spec.OrigGroupBy) {
			vlToLoki[name] = spec.OrigGroupBy[i]
		}
	}
	cache := make(map[string]*metricSeriesCacheEntry, 32)
	series := make(map[string]quantileSeries)
	held := 0
	// A series per group and value, a point per bucket: the points are read in
	// place (eachBucketPoint) and only each series' labels are parsed, with a
	// pooled parser, as collectRangeMetricHits reads its buckets.
	parser := statsQRFJPool.Get()
	defer statsQRFJPool.Put(parser)
	var resultErr error
	_, _ = jsonparser.ArrayEach(body, func(res []byte, _ jsonparser.ValueType, _ int, _ error) {
		if resultErr != nil {
			return
		}
		if resultErr = ctx.Err(); resultErr != nil {
			return
		}
		metricRaw, _, _, _ := jsonparser.Get(res, "metric")
		metric, err := parser.ParseBytes(metricRaw)
		if err != nil {
			resultErr = fmt.Errorf("parse stats response: %w", err)
			return
		}
		value, err := strconv.ParseFloat(string(metric.GetStringBytes(translator.UnwrapValueAlias)), 64)
		if err != nil {
			return // the gate keeps only values that convert
		}
		var labels map[string]string
		if byFields {
			labels = p.statsGroupMetric(metric.GetObject(), vlToLoki)
		} else {
			labels = p.manualSeriesEntry(cache, metric, string(metric.GetStringBytes("_stream")), strings.TrimSpace(string(metric.GetStringBytes("level"))), spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, true).translated
		}
		key := canonicalLabelsKey(labels)
		current, seen := series[key]
		if !seen {
			if resultErr = seriesLimitCollecting(ctx, len(series), p.manualSeriesLimit(ctx)); resultErr != nil {
				return
			}
			current.Metric = labels
		}
		add := func(ts int64, raw []byte) {
			if resultErr != nil {
				return
			}
			count, err := jsonparser.ParseFloat(raw)
			if err != nil || count <= 0 {
				return
			}
			if held++; held > rowLimit {
				resultErr = fmt.Errorf("manual range metric row limit exceeded (%d); narrow the query or increase -manual-range-metric-row-limit", rowLimit)
				return
			}
			current.Samples = append(current.Samples, quantileSample{ts: ts, value: value, count: count})
		}
		if bucket > 0 {
			values, _, _, _ := jsonparser.Get(res, "values")
			eachBucketPoint(values, start, bucket, add)
		} else {
			// An instant answer: its points (a vector's one value, or a
			// matrix's), each at the start.
			if point, _, _, err := jsonparser.Get(res, "value"); err == nil {
				if raw, err := jsonparser.GetString(point, "[1]"); err == nil {
					add(start.UnixNano(), []byte(raw))
				}
			}
			_, _ = jsonparser.ArrayEach(res, func(point []byte, _ jsonparser.ValueType, _ int, _ error) {
				if raw, err := jsonparser.GetString(point, "[1]"); err == nil {
					add(start.UnixNano(), []byte(raw))
				}
			}, "values")
		}
		series[key] = current
	}, "data", "result")
	if resultErr != nil {
		return nil, resultErr
	}
	for key, s := range series {
		s.Samples = sortQuantileSamples(s.Samples, start, bucket)
		series[key] = s
	}
	if limit := p.manualSeriesLimit(ctx); limit > 0 && len(series) > limit {
		if err := seriesLimitReached(ctx, limit); err != nil {
			return nil, err
		}
		series = capQuantileSeries(series, limit)
	}
	return series, nil
}

// sortQuantileSamples orders samples by time. Bucket times lie on the
// start+k*bucket grid, so a counting sort by bucket places them in two passes
// (a series per value arrives in time order, the values one after another);
// the order of the values within a bucket is free, the windows rank them. An
// instant answer has one time.
func sortQuantileSamples(samples []quantileSample, start time.Time, bucket time.Duration) []quantileSample {
	if bucket <= 0 || len(samples) < 2 {
		return samples
	}
	lo, hi := samples[0].ts, samples[0].ts
	size, anchor := int64(bucket), start.UnixNano()
	onGrid := true
	for _, s := range samples {
		if s.ts < lo {
			lo = s.ts
		}
		if s.ts > hi {
			hi = s.ts
		}
		onGrid = onGrid && (s.ts-anchor)%size == 0
	}
	if !onGrid || (hi-lo)/size > int64(len(samples))*4 {
		slices.SortFunc(samples, func(a, b quantileSample) int { return cmp.Compare(a.ts, b.ts) })
		return samples
	}
	offsets := make([]int, (hi-lo)/size+2)
	for _, s := range samples {
		offsets[(s.ts-lo)/size+1]++
	}
	for i := 1; i < len(offsets); i++ {
		offsets[i] += offsets[i-1]
	}
	out := make([]quantileSample, len(samples))
	for _, s := range samples {
		i := (s.ts - lo) / size
		out[offsets[i]] = s
		offsets[i]++
	}
	return out
}

// capQuantileSeries keeps the limit series with the most rows, like
// capSeriesByTotalCount, for a Drilldown request that keeps partial results.
func capQuantileSeries(series map[string]quantileSeries, limit int) map[string]quantileSeries {
	totals := make(map[string]manualSeriesSamples, len(series))
	for key, s := range series {
		var rows float64
		for _, sample := range s.Samples {
			rows += sample.count
		}
		totals[key] = manualSeriesSamples{Samples: []rangeMetricSample{{value: rows}}}
	}
	capped := make(map[string]quantileSeries, limit)
	for key := range capSeriesByTotalCount(totals, limit) {
		capped[key] = series[key]
	}
	return capped
}

// shiftQuantileSeries moves every bucket label forward by shift, in place (see
// shiftSeriesSamples).
func shiftQuantileSeries(series map[string]quantileSeries, shift time.Duration) {
	if shift == 0 {
		return
	}
	for _, s := range series {
		for i := range s.Samples {
			s.Samples[i].ts += int64(shift)
		}
	}
}

// quantileWindow returns the quantile of the counts whose bucket label lies in
// [windowStart, windowEnd): the buckets of the window (windowStart, windowEnd].
// false for a window without rows, which Loki leaves out of the result.
func quantileWindow(phi float64, samples []quantileSample, windowStart, windowEnd int64, scratch *[]quantileSample) (float64, bool) {
	lo := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= windowStart })
	hi := lo + sort.Search(len(samples)-lo, func(i int) bool { return samples[lo+i].ts >= windowEnd })
	if lo == hi {
		return 0, false
	}
	window := append((*scratch)[:0], samples[lo:hi]...)
	*scratch = window
	return quantileOfCounts(phi, window), true
}

// quantileOfCounts is Loki's Quantile (pkg/logql/range_vector.go:465-487) over
// the values in counts, each held count times; counts is reordered. NaN for no
// values, -Inf for phi < 0 and +Inf for phi > 1; otherwise the sorted values
// are interpolated at rank phi*(n-1), with Loki's arithmetic.
func quantileOfCounts(phi float64, counts []quantileSample) float64 {
	n := 0.0
	for _, c := range counts {
		n += c.count
	}
	if n == 0 {
		return math.NaN()
	}
	if phi < 0 {
		return math.Inf(-1)
	}
	if phi > 1 {
		return math.Inf(+1)
	}
	// Loki's order (pkg/logql/vector/heap.go HeapByMaxValue.Less): NaN first.
	sort.Slice(counts, func(i, j int) bool {
		return math.IsNaN(counts[i].value) && !math.IsNaN(counts[j].value) || counts[i].value < counts[j].value
	})
	rank := phi * (n - 1)
	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)
	weight := rank - math.Floor(rank)
	// Written as Loki writes it: the compiler may fuse the multiply-adds on
	// arm64 (not on amd64) as it does in Loki's build for the same platform.
	return valueAtRank(counts, lowerIndex)*(1-weight) + valueAtRank(counts, upperIndex)*weight
}

// valueAtRank returns the value at 0-based rank of the sorted counts.
func valueAtRank(counts []quantileSample, rank float64) float64 {
	seen := 0.0
	for _, c := range counts {
		if seen += c.count; rank < seen {
			return c.value
		}
	}
	return counts[len(counts)-1].value
}

// buildQuantileHistogramMatrix evaluates quantile_over_time at each step from
// the merged bucket counts.
func buildQuantileHistogramMatrix(ctx context.Context, phi float64, series map[string]quantileSeries, start, end time.Time, step, window time.Duration) ([]byte, error) {
	keys, metric, value := quantileSeriesEvaluator(phi, series)
	return encodeWindowMatrix(ctx, keys, metric, value, start, end, step, window)
}

// buildQuantileHistogramVector evaluates an instant quantile_over_time.
func buildQuantileHistogramVector(ctx context.Context, phi float64, series map[string]quantileSeries, evalTime time.Time, window time.Duration) ([]byte, error) {
	keys, metric, value := quantileSeriesEvaluator(phi, series)
	return encodeWindowVector(ctx, keys, metric, value, evalTime, window)
}

func quantileSeriesEvaluator(phi float64, series map[string]quantileSeries) ([]string, func(string) map[string]string, func(string, int64, int64) (float64, bool)) {
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sliders := make(map[string]*quantileSlider, len(series))
	metric := func(key string) map[string]string { return series[key].Metric }
	value := func(key string, windowStart, windowEnd int64) (float64, bool) {
		w := sliders[key]
		if w == nil {
			w = newQuantileSlider(series[key].Samples)
			sliders[key] = w
		}
		return w.quantile(phi, windowStart, windowEnd)
	}
	return keys, metric, value
}

// quantileSlider answers quantileWindow for windows that move forward: the
// counts of the window's buckets are held by value rank in a Fenwick tree, a
// bucket added when the window reaches it and removed when it leaves, so a
// step costs the buckets that enter and leave and a rank search, not a sort
// of the whole window (a [1m] window at a 10 s step over 6 hours is 2,160
// sorts of every value of six buckets). The order is quantileOfCounts' (NaN
// first, then ascending), equal values share a rank, and the counts are whole
// numbers, so the value at each rank is the same as there (0 and -0 compare
// equal and share a rank; the sort leaves their order open as well).
type quantileSlider struct {
	samples []quantileSample // ascending by ts
	ranks   []int            // the value rank of each sample, 1-based
	values  []float64        // the value of each rank
	tree    []float64        // counts by rank (Fenwick, 1-based)
	total   float64
	lo, hi  int // samples[lo:hi] are in the tree
	start   int64
	used    bool
}

func newQuantileSlider(samples []quantileSample) *quantileSlider {
	w := &quantileSlider{samples: samples, ranks: make([]int, len(samples))}
	// A histogram holds far fewer distinct values than samples: rank the
	// distinct values (every NaN one key, 0 and -0 one key, as the order has
	// them equal) and look each sample's rank up.
	key := func(v float64) uint64 {
		switch {
		case math.IsNaN(v):
			return math.Float64bits(math.NaN())
		case v == 0:
			return 0
		}
		return math.Float64bits(v)
	}
	rankOf := make(map[uint64]int)
	for _, s := range samples {
		if k := key(s.value); rankOf[k] == 0 {
			rankOf[k] = 1
			w.values = append(w.values, s.value)
		}
	}
	less := func(a, b float64) bool { return math.IsNaN(a) && !math.IsNaN(b) || a < b }
	sort.Slice(w.values, func(i, j int) bool { return less(w.values[i], w.values[j]) })
	for r, v := range w.values {
		rankOf[key(v)] = r + 1
	}
	for i, s := range samples {
		w.ranks[i] = rankOf[key(s.value)]
	}
	w.tree = make([]float64, len(w.values)+1)
	return w
}

func (w *quantileSlider) add(i int, sign float64) {
	c := sign * w.samples[i].count
	w.total += c
	for r := w.ranks[i]; r < len(w.tree); r += r & -r {
		w.tree[r] += c
	}
}

// at returns the value at 0-based rank of the counts in the tree.
func (w *quantileSlider) at(rank float64) float64 {
	pos, step := 0, 1
	for step*2 < len(w.tree) {
		step *= 2
	}
	for ; step > 0; step /= 2 {
		if next := pos + step; next < len(w.tree) && w.tree[next] <= rank {
			pos = next
			rank -= w.tree[next]
		}
	}
	if pos >= len(w.values) {
		pos = len(w.values) - 1
	}
	return w.values[pos]
}

func (w *quantileSlider) quantile(phi float64, windowStart, windowEnd int64) (float64, bool) {
	if !w.used || windowStart < w.start {
		// The first window, or one that moved back: start again.
		clear(w.tree)
		w.total = 0
		w.lo = sort.Search(len(w.samples), func(i int) bool { return w.samples[i].ts >= windowStart })
		w.hi = w.lo
		w.used = true
	}
	w.start = windowStart
	for ; w.lo < len(w.samples) && w.samples[w.lo].ts < windowStart; w.lo++ {
		if w.lo < w.hi {
			w.add(w.lo, -1)
		}
	}
	if w.hi < w.lo {
		w.hi = w.lo
	}
	for ; w.hi < len(w.samples) && w.samples[w.hi].ts < windowEnd; w.hi++ {
		w.add(w.hi, 1)
	}
	if w.lo == w.hi {
		return 0, false
	}
	switch n := w.total; {
	case phi < 0:
		return math.Inf(-1), true
	case phi > 1:
		return math.Inf(+1), true
	default:
		rank := phi * (n - 1)
		lowerIndex := math.Max(0, math.Floor(rank))
		upperIndex := math.Min(n-1, lowerIndex+1)
		weight := rank - math.Floor(rank)
		// As quantileOfCounts writes it (Loki's arithmetic).
		return w.at(lowerIndex)*(1-weight) + w.at(upperIndex)*weight, true
	}
}

// readsQuantileCounts reports whether a manual range metric is a
// quantile_over_time answered from value counts: its translation converts the
// value in the unwrap gate, and its series are the groups of fields. Per-stream
// series count every value of every stream, which VictoriaLogs holds in
// memory; they stay on the raw evaluator, bounded by the row and byte limits.
func readsQuantileCounts(manualFunc string, spec statsCompatSpec) bool {
	return manualFunc == "quantile" && hasUnwrapGate(spec.BaseQuery) && quantileGroupsByFields(spec)
}

// hasUnwrapGate reports whether a translated base query holds the unwrap gate,
// which converts the value into translator.UnwrapValueAlias.
func hasUnwrapGate(base string) bool {
	_, _, ok := translator.SplitUnwrapGate(base)
	return ok
}

// Series and groups (Loki v3.7.7). A range aggregation's own grouping,
// `quantile_over_time(...) by (x)`, groups the samples before the quantile
// (pkg/logql/syntax/extractor.go:21-60): one quantile over every sample of the
// group. Without it the quantile is taken per series, and a vector aggregation
// over it, `sum by (x) (quantile_over_time(...))`, folds those per-series values
// per step (pkg/logql/evaluator.go newVectorAggEvaluator; a quantile's grouping
// is never handed to the extractor, syntax/ast.go canInjectVectorGrouping). The
// translation groups both forms alike, so the quantile route reads the query's
// shape itself: the series are the streams, and the vector aggregation folds
// the evaluated windows (quantileOuter).

// quantileOuter is the vector aggregation over a quantile_over_time: op is a
// withoutMergeForOp operation over the series sharing the by labels.
type quantileOuter struct {
	op    string
	by    []string
	limit int // -max-stats-query-series on the folded answer
}

type quantileOuterKey struct{}

func quantileOuterFrom(ctx context.Context) *quantileOuter {
	o, _ := ctx.Value(quantileOuterKey{}).(*quantileOuter)
	return o
}

// quantileQueryShape reads a quantile_over_time query: whether the range
// aggregation groups its samples (by, not without) and the vector aggregation
// over it (sum, avg, min, max or count with by, or none). ok is false for any
// other shape (without, label_replace, nested aggregations), which keeps the
// translation's grouping.
func quantileQueryShape(logql string) (rangeGrouped bool, outer *quantileOuter, ok bool) {
	expr, err := logqlpkg.Parse(strings.TrimSpace(logql))
	if err != nil {
		return false, nil, false
	}
	if agg, isAgg := expr.(*logqlpkg.VectorAggregation); isAgg {
		switch agg.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount:
		default:
			return false, nil, false
		}
		if agg.Grouping != nil && agg.Grouping.Without {
			return false, nil, false
		}
		outer = &quantileOuter{op: string(agg.Op)}
		if agg.Grouping != nil {
			outer.by = agg.Grouping.Labels
		}
		expr = agg.Inner
	}
	ra, isRange := expr.(*logqlpkg.RangeAggregation)
	if !isRange || ra.Op != logqlpkg.RangeQuantileOverTime || (ra.Grouping != nil && ra.Grouping.Without) {
		return false, nil, false
	}
	return ra.Grouping != nil, outer, true
}

// withQuantileShape prepares a quantile_over_time evaluation for the query's
// shape: per-stream series unless the range aggregation groups its samples,
// and, under a vector aggregation, a context that folds the evaluated series
// (the series limit then counts the folded answer, as Loki does). ok is false
// for a shape quantileQueryShape does not read: the caller keeps the
// translation's grouping.
func (p *Proxy) withQuantileShape(r *http.Request, logql string, spec statsCompatSpec) (*http.Request, statsCompatSpec, bool) {
	rangeGrouped, outer, ok := quantileQueryShape(logql)
	if !ok {
		return r, spec, false
	}
	if !rangeGrouped {
		spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit = nil, nil, false
	}
	if outer == nil {
		return r, spec, true
	}
	ctx := r.Context()
	outer.limit = p.resolvedMaxStatsQuerySeries(ctx)
	ctx = context.WithValue(context.WithValue(ctx, foldedSumKey{}, true), quantileOuterKey{}, outer)
	return r.WithContext(ctx), spec, true
}

// fold returns the evaluator of the folded series: a group per value set of
// the by labels, whose window is the op over its members' windows that hold
// samples, and which is absent when none does.
func (o *quantileOuter) fold(ctx context.Context, keys []string, metric func(string) map[string]string, value func(string, int64, int64) (float64, bool)) ([]string, func(string) map[string]string, func(string, int64, int64) (float64, bool), error) {
	groupLabels := map[string]map[string]string{}
	members := map[string][]string{}
	for _, key := range keys {
		labels := map[string]string{}
		for _, name := range o.by {
			if v := metric(key)[name]; v != "" {
				labels[name] = v
			}
		}
		gk := canonicalLabelsKey(labels)
		if _, seen := groupLabels[gk]; !seen {
			groupLabels[gk] = labels
		}
		members[gk] = append(members[gk], key)
	}
	groups := make([]string, 0, len(groupLabels))
	for gk := range groupLabels {
		groups = append(groups, gk)
	}
	sort.Strings(groups)
	if o.limit > 0 && len(groups) > o.limit {
		if err := seriesLimitReached(ctx, o.limit); err != nil {
			return nil, nil, nil, err
		}
		groups = groups[:o.limit]
	}
	merge := withoutMergeForOp(o.op)
	folded := func(gk string, windowStart, windowEnd int64) (float64, bool) {
		acc, n := 0.0, 0
		for _, key := range members[gk] {
			if v, ok := value(key, windowStart, windowEnd); ok {
				n++
				acc = merge.combine(acc, v, n)
			}
		}
		return acc, n > 0
	}
	return groups, func(gk string) map[string]string { return groupLabels[gk] }, folded, nil
}

// writeQuantileHistogramRange answers a quantile_over_time range query from the
// value counts of buckets anchored at fetchStart; sampleShift moves their
// labels onto the windows they hold (see shiftSeriesSamples).
func (p *Proxy) writeQuantileHistogramRange(w http.ResponseWriter, ctx context.Context, spec statsCompatSpec, phi float64, fetchStart, start, end time.Time, step, window, bucket, sampleShift time.Duration) {
	series, err := p.collectQuantileHistogram(ctx, spec, fetchStart, end, bucket)
	if err != nil {
		p.writeErr(w, ctx, err)
		return
	}
	shiftQuantileSeries(series, sampleShift)
	result, err := buildQuantileHistogramMatrix(ctx, phi, series, start, end, step, window)
	if err != nil {
		p.writeErr(w, ctx, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
}

// fetchQuantileCounts sends the value count query of collectQuantileHistogram
// and returns its answer, read within -backend-max-buffered-response-bytes and
// checked to be a successful JSON answer.
func (p *Proxy) fetchQuantileCounts(ctx context.Context, spec statsCompatSpec, start, end time.Time, bucket time.Duration) ([]byte, error) {
	params := url.Values{}
	params.Set("query", spec.BaseQuery+" | stats by ("+strings.Join(quantileStatsGroupBy(spec), ", ")+") count() as "+quantileCountName)
	path := "/select/logsql/stats_query"
	if bucket > 0 {
		path = "/select/logsql/stats_query_range"
		setSlidingStatsRangeParams(params, start, end, bucket)
	} else {
		// VictoriaLogs' start is inclusive and its end exclusive.
		params.Set("start", start.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano))
		params.Set("end", end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano))
	}
	if sem := p.statsQueryRangeSem; sem != nil {
		select {
		case <-sem:
			defer func() { sem <- struct{}{} }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	resp, err := p.vlPost(ctx, path, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("stats backend", resp.StatusCode, body)
	}
	maxBytes := p.limits().BufferedBackendBodyBytes
	body, err := readBodyLimited(resp.Body, int64(maxBytes))
	if errors.Is(err, errBodyTooLarge) {
		return nil, fmt.Errorf("manual metric response exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", maxBytes)
	}
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("parse stats response: invalid JSON")
	}
	if status, _ := jsonparser.GetString(body, "status"); status != "success" {
		return nil, fmt.Errorf("stats response non-success status: %s", status)
	}
	return body, nil
}
