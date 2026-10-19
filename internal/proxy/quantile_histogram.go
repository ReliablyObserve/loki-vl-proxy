package proxy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"

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
	if !quantileGroupsByFields(spec) {
		by = append(by, "_stream", "level")
	}
	for _, g := range spec.GroupBy {
		if isVLInternalField(g) || (g == "level" && len(by) > 0) {
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
	v, err := p.fetchQuantileCounts(ctx, spec, start, end, bucket)
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
	for _, res := range v.GetArray("data", "result") {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		metric := res.Get("metric")
		value, err := strconv.ParseFloat(string(metric.GetStringBytes(translator.UnwrapValueAlias)), 64)
		if err != nil {
			continue // the gate keeps only values that convert
		}
		var labels map[string]string
		if byFields {
			labels = p.statsGroupMetric(res.GetObject("metric"), vlToLoki)
		} else {
			labels = p.manualSeriesEntry(cache, metric, string(metric.GetStringBytes("_stream")), strings.TrimSpace(string(metric.GetStringBytes("level"))), spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, true).translated
		}
		key := canonicalLabelsKey(labels)
		points := res.GetArray("values")
		if point := res.Get("value"); point != nil {
			points = []*fj.Value{point}
		}
		current, seen := series[key]
		if !seen {
			if err := seriesLimitCollecting(ctx, len(series), p.manualSeriesLimit(ctx)); err != nil {
				return nil, err
			}
			current.Metric = labels
		}
		for _, point := range points {
			pair := point.GetArray()
			if len(pair) < 2 {
				continue
			}
			ts := start.UnixNano()
			if bucket > 0 {
				var ok bool
				if ts, ok = snapSlidingBucketTimestamp(pair[0], start, bucket); !ok {
					continue
				}
			}
			count, err := strconv.ParseFloat(string(pair[1].GetStringBytes()), 64)
			if err != nil || count <= 0 {
				continue
			}
			if held++; held > rowLimit {
				return nil, fmt.Errorf("manual range metric row limit exceeded (%d); narrow the query or increase -manual-range-metric-row-limit", rowLimit)
			}
			current.Samples = append(current.Samples, quantileSample{ts: ts, value: value, count: count})
		}
		series[key] = current
	}
	for key, s := range series {
		sort.Slice(s.Samples, func(i, j int) bool { return s.Samples[i].ts < s.Samples[j].ts })
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
	var scratch []quantileSample
	metric := func(key string) map[string]string { return series[key].Metric }
	value := func(key string, windowStart, windowEnd int64) (float64, bool) {
		return quantileWindow(phi, series[key].Samples, windowStart, windowEnd, &scratch)
	}
	return keys, metric, value
}

// readsQuantileCounts reports whether a manual range metric is a
// quantile_over_time answered from value counts: its translation converts the
// value in the unwrap gate.
func readsQuantileCounts(manualFunc, base string) bool {
	return manualFunc == "quantile" && hasUnwrapGate(base)
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
		p.writeError(w, badRequestStatusOr(err, http.StatusBadGateway), err.Error())
		return
	}
	shiftQuantileSeries(series, sampleShift)
	result, err := buildQuantileHistogramMatrix(ctx, phi, series, start, end, step, window)
	if err != nil {
		p.writeError(w, badRequestStatusOr(err, http.StatusServiceUnavailable), err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
}

// fetchQuantileCounts sends the value count query of collectQuantileHistogram
// and returns its decoded answer, read within -backend-max-buffered-response-bytes.
func (p *Proxy) fetchQuantileCounts(ctx context.Context, spec statsCompatSpec, start, end time.Time, bucket time.Duration) (*fj.Value, error) {
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
	v, err := fj.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("parse stats response: %w", err)
	}
	if status := string(v.GetStringBytes("status")); status != "success" {
		return nil, fmt.Errorf("stats response non-success status: %s", status)
	}
	return v, nil
}
