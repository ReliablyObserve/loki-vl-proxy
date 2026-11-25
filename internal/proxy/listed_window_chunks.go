package proxy

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Time chunks of an extraction list's series windows (listedIdentityWindows).
//
// On a range longer than the step, VictoriaLogs answers one row per series and sub-step bucket that holds the
// series' lines, and the window evaluator holds those partials until the vector aggregation folds them. A listed
// label of high cardinality over a long range (`| logfmt amount, user_id` over a day) has more such rows than one
// read may hold: the read stops at its bound (-manual-range-metric-row-limit series before the fold, the stats body
// bound), where Loki answers.
//
// Each output step t depends only on the buckets in (t-range, t]. So the steps [start, end] are evaluated in chunks,
// as a query frontend splits a range: a chunk of the steps [a, b] reads its buckets from a-range to b (the range
// before the chunk read again), its windows are evaluated and folded into the one fold of the request
// (matrixFoldSink) as they are read, and the chunk's partials are dropped. The answer is the unchunked one: every
// step is evaluated from the same buckets (the chunk's fetch starts a whole number of steps, so of buckets, after the
// request's), and the fold visits a step's series in the same order (sorted keys of the chunk that holds the step,
// which holds every series with a line in the step's window). The fold keeps a cell per group and sample time, so the
// chunks add to it in turn.
//
// The chunk's size:
//   - known up front: no chunk reads more than heavyQueryBucketThreshold buckets (the bound past which the
//     unchunked request is read from raw rows), and a size this replica measured for the same read before;
//   - measured: a chunk whose read passes its bound is cut to a quarter (listedChunkShrink) and read again; after a
//     read, the next chunk is sized from the share of the bound that read used, at most half of it. The bounds are
//     the series before the fold (the larger of -manual-range-metric-row-limit and the tenant's max_query_series,
//     recordFoldBudget) and the stats body (64 MiB, maxStatsBucketResponseBytes, or
//     -backend-max-buffered-response-bytes where the unwrap check reads the body first). A single step whose window
//     passes one of them is the only refusal left besides Loki's own series limit: Loki's limit error naming
//     -manual-range-metric-row-limit for the series, Loki's message-size error for the body.
//
// Chunks are read one after another (one stats_query_range slot each, takeStatsQueryRangeSlot), so the request holds
// one chunk's partials and the fold's groups at a time. A request that fits one chunk reads exactly as before.

// statsReadMeterKey carries a statsReadMeter in a context.
type statsReadMeterKey struct{}

// statsReadMeter records the stats_query_range reads of a chunk: whether one passed its bound (overBytes: the body
// bound, else the series before the fold), and what they held.
type statsReadMeter struct {
	over, overBytes bool
	bytes           int
	series          int
}

func withStatsReadMeter(ctx context.Context) (context.Context, *statsReadMeter) {
	m := &statsReadMeter{}
	return context.WithValue(ctx, statsReadMeterKey{}, m), m
}

// noteStatsReadOverflow records a read that stopped at its bound: more series than the read may hold, or a body over
// the stats body bound.
func noteStatsReadOverflow(ctx context.Context, err error) {
	if m, _ := ctx.Value(statsReadMeterKey{}).(*statsReadMeter); m != nil {
		switch {
		case errors.Is(err, errBodyTooLarge):
			m.over, m.overBytes = true, true
		case isSeriesLimitError(err):
			m.over = true
		}
	}
}

// noteStatsRead records a read's body and series, and an overflow of its series cap.
func noteStatsRead(ctx context.Context, bytes, series int, err error) {
	if m, _ := ctx.Value(statsReadMeterKey{}).(*statsReadMeter); m != nil {
		m.bytes += bytes
		m.series += series
		noteStatsReadOverflow(ctx, err)
	}
}

// statsBucketRoute reads the stats buckets of a window metric over [from, to]: ok false when the route does not
// apply or VictoriaLogs failed, a non-nil error when it is the request's answer.
type statsBucketRoute func(ctx context.Context, from, to time.Time) (map[string]manualSeriesSamples, bool, error)

// statsBucketRoutes are the bucket reads of spec in the order the window evaluator tries them.
func (p *Proxy) statsBucketRoutes(spec statsCompatSpec, statsAggFunc string, bucket time.Duration, streamBuckets bool) []statsBucketRoute {
	return []statsBucketRoute{
		func(ctx context.Context, from, to time.Time) (map[string]manualSeriesSamples, bool, error) {
			return p.collectStatsFastPathHits(ctx, spec, statsAggFunc, from, to, bucket, streamBuckets, false)
		},
		func(ctx context.Context, from, to time.Time) (map[string]manualSeriesSamples, bool, error) {
			return p.collectParserStageStatsFastPathHits(ctx, spec, statsAggFunc, from, to, bucket)
		},
		func(ctx context.Context, from, to time.Time) (map[string]manualSeriesSamples, bool, error) {
			return p.collectStatsFastPathHits(ctx, spec, statsAggFunc, from, to, bucket, streamBuckets, true)
		},
	}
}

// listedChunkSizeTTL is how long a replica keeps the chunk size a read needed.
const listedChunkSizeTTL = 10 * time.Minute

// listedChunkShrink divides a chunk whose read passed its bound: a read past the bound was computed by VictoriaLogs
// in full and is thrown away, so a quarter (two reads thrown away where halving threw away three, measured over a
// tenfold overflow) rather than a half; the next chunks grow again from what the smaller one held.
const listedChunkShrink = 4

// listedChunkLoad is the share of a read's bound a chunk is sized to use.
const listedChunkLoad = 0.5

// listedWindowChunksOff reads the windows in one read, as before the chunks (tests measure the two).
var listedWindowChunksOff bool

// listedWindowChunksObserve, when set, sees each chunk a request read (tests).
var listedWindowChunksObserve func(first, last int64, m statsReadMeter)

// listedChunkReads is what the window evaluator decided about a request before the chunks: bucketsOK, a bucket grid
// exists; counter, a rate_counter proof reads the rows; quantile, the quantile's counts are read; rawRows, the level is
// read from raw rows (set here when the bucket reads were sent over the whole request and did not answer, so the
// evaluator goes on to the raw rows without sending them again); overflowed, the read of the whole range passed the
// stats body bound (now, or on this replica within listedChunkSizeTTL: windowReadOverflowed).
type listedChunkReads struct {
	bucketsOK, counter, quantile bool
	rawRows                      *bool
	overflowed                   bool
}

// listedWindowChunksRoute answers a window metric in time chunks when the request folds it (a matrix fold), its
// windows are read from stats buckets, and its series identity is an extraction list's or its read over the whole
// range passed the stats body bound (reads.overflowed: the stream's series of a count, min, max or avg over a range
// function over many short-lived streams, which the vector aggregation folds into a few groups), and reports
// whether it did.
func (p *Proxy) listedWindowChunksRoute(w http.ResponseWriter, r *http.Request, reads listedChunkReads, spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc, statsAggFunc string, start, end time.Time, step, bucket, shift time.Duration) bool {
	fold := matrixFoldFrom(r.Context())
	if fold == nil || listedWindowChunksOff || !reads.bucketsOK || statsAggFunc == "" || *reads.rawRows || reads.counter || reads.quantile ||
		!reads.overflowed && !p.identityWindows(r.Context(), r.FormValue("query")) {
		return false
	}
	chunked := p.withSlidingWithoutCut(r, spec, origSpec, manualFunc, statsAggFunc, step)
	routes := p.statsBucketRoutes(spec, chunked, bucket, origSpec.Window != step)
	// A parser's series hash folds, in VictoriaLogs, the series that lie in one bucket (series_fold_read.go).
	r = p.withSeriesFold(r, spec, manualFunc, fold)
	handled, whole := p.writeListedWindowChunks(w, r, routes, manualFunc, start, end, step, origSpec.Window, bucket, shift, fold)
	*reads.rawRows = whole
	return handled
}

// writeListedWindowChunks answers a window metric over an extraction list's series identity in time chunks (see
// above) and reports handled; otherwise the bucket reads did not answer it (no route applies, or VictoriaLogs failed)
// and the window evaluator goes on as without chunks, past the bucket reads when whole is set (they were tried over
// the whole request already, so they are not sent again).
func (p *Proxy) writeListedWindowChunks(w http.ResponseWriter, r *http.Request, routes []statsBucketRoute, manualFunc string, start, end time.Time, step, window, bucket, shift time.Duration, fold *matrixFold) (handled, whole bool) {
	ctx := r.Context()
	n := stepCount(start.UnixNano(), end.UnixNano(), step.Nanoseconds())
	bucketSteps := listedChunkBucketSteps(step, window, bucket)
	if n <= 0 || bucketSteps < 1 || len(fold.levels) == 0 {
		return false, false
	}
	budget := p.resolvedMaxStatsQuerySeries(ctx)
	maxBytes := p.limits().BufferedBackendBodyBytes
	sizeKey := listedChunkSizeKey(ctx, r.FormValue("query"), step, bucket)
	size := min64(n, bucketSteps)
	known, measured := p.listedChunkSize(ctx, sizeKey)
	if measured {
		size = min64(size, known)
	}
	// The tenant's series limit applies to the folded answer (capSeriesToLimit afterwards): a fold of one level
	// whose groups pass it fails as that would, before more chunks are read.
	limit := p.requestQueryLimits(ctx).MaxQuerySeries
	sink := newMatrixFoldSink(fold)
	route, halved := -1, false
	for first := int64(0); first < n; {
		last := min64(n-1, first+size-1)
		from, to := start.Add(time.Duration(first)*step), start.Add(time.Duration(last)*step)
		// The window before the chunk's first step, as the unchunked read starts before the request's.
		fetchFrom := from.Add(-window)
		if shift != 0 {
			fetchFrom = from.Add(-step)
		}
		readCtx, meter := withStatsReadMeter(ctx)
		var series map[string]manualSeriesSamples
		var err error
		answered := -1
		for i, collect := range routes {
			var ok bool
			series, ok, err = collect(readCtx, fetchFrom, to)
			if ok && err == nil && !meter.over {
				answered = i
			}
			if ok || err != nil || meter.over {
				break
			}
		}
		if listedWindowChunksObserve != nil {
			listedWindowChunksObserve(first, last, *meter)
		}
		switch {
		case meter.over && last > first:
			size, halved = max(1, (last-first+1)/listedChunkShrink), true
			continue
		case meter.over:
			p.writeErr(w, ctx, p.listedStepOverflowError(meter.overBytes))
			return true, false
		case err != nil:
			p.writeErr(w, ctx, err)
			return true, false
		case answered < 0 || route >= 0 && answered != route:
			return false, first == 0 && last == n-1
		}
		route = answered
		if _, err := hitsRangeMetricSeries(manualFunc, shiftSeriesSamples(series, shift), from, to, step, window, maxBytes, sink); err != nil {
			p.writeErr(w, ctx, err)
			return true, false
		}
		if len(fold.levels) == 1 && limit > 0 && len(sink.acc.groups) > limit && !requestKeepsPartialSeries(ctx) {
			p.writeErr(w, ctx, seriesLimitReached(tenantSeriesLimitContext(ctx), limit))
			return true, false
		}
		read := last - first + 1
		if halved {
			p.setLocalReadCacheWithTTL(sizeKey, []byte(strconv.FormatInt(read, 10)), listedChunkSizeTTL)
		}
		first = last + 1
		size = nextListedChunkSize(read, bucketSteps, *meter, budget, int(maxStatsBucketResponseBytes))
	}
	markQueryStepsRoute(ctx, "buckets-"+strconv.Itoa(route))
	body, err := sink.finish(fold, maxBytes)
	if err != nil {
		p.writeErr(w, ctx, err)
		return true, false
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true, false
}

// listedStepOverflowError is the answer to a single step whose window passes a chunk's bound: Loki's answer to a
// response over its message size for the stats body (overBytes), Loki's limit error naming
// -manual-range-metric-row-limit in the log for the series.
func (p *Proxy) listedStepOverflowError(overBytes bool) error {
	if overBytes {
		return newManualMetricResponseTooLarge(int64(p.limits().BufferedBackendBodyBytes))
	}
	rows, _ := p.manualMetricRowBudget()
	return newRowLimitError(rows, "series in the window of one step", 0)
}

// listedChunkBucketSteps is the number of steps a chunk may hold so that its read, from the range before its first
// step to its last, holds no more than heavyQueryBucketThreshold buckets; 0 when the range alone holds more.
func listedChunkBucketSteps(step, window, bucket time.Duration) int64 {
	if step <= 0 || bucket <= 0 {
		return 0
	}
	room := time.Duration(heavyQueryBucketThreshold)*bucket - window
	if room < 0 {
		return 0
	}
	return int64(room/step) + 1
}

// nextListedChunkSize sizes the chunk after one of read steps whose reads held m: the steps that use
// listedChunkLoad of the tighter of the two bounds (the series before the fold, the body bytes), at least one step
// and at most bucketSteps.
func nextListedChunkSize(read, bucketSteps int64, m statsReadMeter, budget, bodyBound int) int64 {
	load := 0.0
	if budget > 0 {
		load = float64(m.series) / float64(budget)
	}
	if bodyBound > 0 {
		load = max(load, float64(m.bytes)/float64(bodyBound))
	}
	if load <= 0 {
		return bucketSteps
	}
	next := float64(read) * listedChunkLoad / load
	if next >= float64(bucketSteps) {
		return bucketSteps
	}
	return max(1, int64(next))
}

// listedChunkSizeKey names the chunk size a read of query at step needed, in the tenant's scope.
func listedChunkSizeKey(ctx context.Context, query string, step, bucket time.Duration) string {
	return "listed-window-chunk:v1:" + getOrgID(ctx) + ":" + strconv.FormatInt(int64(step), 10) + ":" + strconv.FormatInt(int64(bucket), 10) + ":" + strings.TrimSpace(query)
}

// listedChunkSize is the chunk size this replica measured for the key, if any: its memory tier only (a hint,
// written there by setLocalReadCacheWithTTL), so a request pays no disk or peer lookup for it.
func (p *Proxy) listedChunkSize(ctx context.Context, key string) (int64, bool) {
	if p.cache == nil {
		return 0, false
	}
	raw, ok := p.cachePeek(ctx, p.cache, key)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	return n, err == nil && n > 0
}

// windowReadOverflowed reports a request whose read over its whole range this replica saw pass the stats body bound
// within listedChunkSizeTTL (noteWindowReadOverflow, or a chunked read that had to shrink): its windows are read in
// time chunks at once, without sending the whole range's read again.
func (p *Proxy) windowReadOverflowed(r *http.Request, step, bucket time.Duration) bool {
	_, measured := p.listedChunkSize(r.Context(), listedChunkSizeKey(r.Context(), r.FormValue("query"), step, bucket))
	return measured
}

// noteWindowReadOverflow records that the read of the steps [start, end] passed the stats body bound: the chunks
// start at a quarter of them (listedChunkShrink), as a chunk whose read passed its bound is cut.
func (p *Proxy) noteWindowReadOverflow(r *http.Request, start, end time.Time, step, bucket time.Duration) {
	n := stepCount(start.UnixNano(), end.UnixNano(), step.Nanoseconds())
	size := max(1, n/listedChunkShrink)
	p.setLocalReadCacheWithTTL(listedChunkSizeKey(r.Context(), r.FormValue("query"), step, bucket), []byte(strconv.FormatInt(size, 10)), listedChunkSizeTTL)
}
