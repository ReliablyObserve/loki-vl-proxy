package proxy

import (
	"bufio"
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// Sliding-window log metrics (count_over_time, rate, bytes_over_time,
// bytes_rate with range != step) need the line count or byte sum of every
// window (T-range, T]. The generic fast path asks VictoriaLogs for buckets of
// gcd(step, range): with Grafana's fractional steps that grid is seconds wide,
// so 24h or 7d queries request tens or hundreds of thousands of buckets per
// series. VictoriaLogs builds those points in memory before responding, and a
// single such query has crashed it.
//
// Two step-sized grids answer every window exactly instead. With evaluation
// times T_k = start + k*step, let A_k be the total over (T_k, T_k+1] and C_k
// the total over (T_k-range, T_k+1-range]. Windows then follow the recurrence
//
//	W_k+1 = W_k + A_k - C_k
//
// seeded by one bucket for W_0 = (T_0-range, T_0]. Counts and byte sums are
// integers, so the recurrence is exact. VictoriaLogs returns at most one row
// per step per series, three stats calls in total, and the proxy keeps one
// compact record per returned row.

// Stats result names: "c" carries the count or byte sum, "__sample_count" the
// line count that proves presence for byte sums.
const (
	slidingStatsValueName = "c"
	slidingStatsCountName = "__sample_count"
)

// slidingStatsByteBudgetError is the row limit's own byte budget: the stats
// stream may carry rowLimit rows of at most slidingStatsMaxRowBytes each, so
// passing it means the grid holds more than -manual-range-metric-row-limit
// rows' worth of data, and only raising that flag raises the budget. It is the
// same limit error as the row count.
func slidingStatsByteBudgetError(rowLimit int) error {
	return newRowLimitError(rowLimit, fmt.Sprintf("stats rows of up to %d bytes in one bucket grid", slidingStatsMaxRowBytes), 0)
}

// slidingStatsMaxRowBytes bounds one stats row: a bucket timestamp, the group
// label values and two numbers.
const slidingStatsMaxRowBytes = 64 << 10

const (
	slidingGridSeed = iota
	slidingGridA
	slidingGridC
)

type rangeTopKKey struct{}

type rangeTopK struct {
	k          int
	descending bool
	group      *logqlpkg.Grouping // k series are kept per group (nil: one group)
}

// withRangeTopK marks a range metric evaluation whose result feeds topk or
// bottomk, so the evaluator ranks every series per step instead of truncating
// to the busiest series first.
func withRangeTopK(ctx context.Context, k int, descending bool, group *logqlpkg.Grouping) context.Context {
	return context.WithValue(ctx, rangeTopKKey{}, rangeTopK{k: k, descending: descending, group: group})
}

func rangeTopKFromContext(ctx context.Context) (rangeTopK, bool) {
	v, ok := ctx.Value(rangeTopKKey{}).(rangeTopK)
	return v, ok && v.k > 0
}

// slidingWindowStatsApplies reports whether the recurrence path should answer a
// window log metric: always for topk/bottomk inputs, otherwise when the stats
// fast path would need a bucket grid finer than Loki's resolution limit or has
// no exact grid at all.
func (p *Proxy) slidingWindowStatsApplies(ctx context.Context, spec statsCompatSpec, field string, start, end time.Time, step, window, gcdBucket time.Duration, gcdOK bool) bool {
	if field != "__count__" && field != "__bytes__" {
		return false
	}
	if step <= 0 || window <= 0 || end.Before(start) {
		return false
	}
	// A parser stage stays in the grids' stats pipe: VictoriaLogs groups by the parsed fields and counts every line,
	// as the bucket reads do, with the parser keeping the fields a line already has (slidingGridParserBase). A query
	// of several parser stages keeps the bucket reads, and so does a request checked for JSON parser errors: its
	// bucket reads carry the check's marks (json_parser_error_check.go), the grids' stats pipe does not.
	if queryUsesParserStages(spec.BaseQuery) {
		if _, ok := slidingGridParserBase(spec.BaseQuery); !ok || jsonErrorCheckFrom(ctx) != nil {
			return false
		}
	}
	if len(spec.GroupBy) == 0 && !spec.ByExplicit {
		return false
	}
	for _, g := range spec.GroupBy {
		if g == "_stream" {
			return false
		}
	}
	// Ranking every series only matters when grouping can produce more series
	// than -max-stats-query-series; an aggregate-all topk keeps its fast path.
	if _, ranked := rangeTopKFromContext(ctx); ranked && len(spec.GroupBy) > 0 {
		return true
	}
	if step == window {
		return false
	}
	if !gcdOK || gcdBucket <= 0 {
		return true
	}
	return (end.Sub(start)+window)/gcdBucket > heavyQueryBucketThreshold
}

// writeRankedRangeFromBuckets answers the input of a topk or bottomk from the
// stats_query_range buckets of the unranked window metric (the route, and the
// stats extent cache, a repeated or moved window reuses) instead of the window
// recurrence, and reports false when the recurrence must answer. The buckets
// hold every series the grouping yields, so the caller's ranking sees all of
// them; they cannot when the grouping yields more series than
// -max-stats-query-series (the read fails: Loki's limit applies to the answer,
// which topk shrinks, so the recurrence ranks them; the verdict is remembered for
// a while so the heavy bucket read is not repeated on every request), for a
// request that keeps only the busiest series (Drilldown), when the window equals
// the step (one grid is read already), or when the unranked metric would use the
// recurrence itself (no exact bucket grid, or more buckets than the heavy-query
// bound), or a VictoriaLogs that has no stats_query_range (404, 405, 501, or an answer that is not a stats_query_range success body). Any
// other failure is the client's answer, as it is for the recurrence: the heavier
// path is not tried against a backend that is failing.
func (p *Proxy) writeRankedRangeFromBuckets(w http.ResponseWriter, ctx context.Context, spec statsCompatSpec, statsAggFunc, field, manualFunc string, fetchStart, start, end time.Time, step, window, bucket time.Duration, bucketsOK bool, sampleShift time.Duration) bool {
	if _, ranked := rangeTopKFromContext(ctx); !ranked || statsAggFunc == "" || window == step || requestKeepsPartialSeries(ctx) {
		return false
	}
	unranked := context.WithValue(ctx, rangeTopKKey{}, rangeTopK{})
	if p.slidingWindowStatsApplies(unranked, spec, field, start, end, step, window, bucket, bucketsOK) {
		return false
	}
	overLimitKey := "ranked-over-limit:" + getOrgID(ctx) + ":" + strconv.Itoa(p.resolvedMaxStatsQuerySeries(ctx)) + ":" + spec.BaseQuery + "|" + strings.Join(spec.GroupBy, ",") + "|" + statsAggFunc
	if p.cache != nil {
		if _, seen := p.cacheGet(ctx, p.cache, overLimitKey); seen {
			return false
		}
	}
	series, err := p.collectRangeMetricHits(ctx, spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, fetchStart, end, bucket)
	if err != nil {
		if isSeriesLimitError(err) || errors.Is(err, errBodyTooLarge) {
			p.setLocalReadCacheWithTTL(overLimitKey, []byte("1"), drilldownOverLimitTTL)
			return false
		}
		var unreadable *statsBucketsUnreadableError
		if errors.As(err, &unreadable) {
			return false // not a stats_query_range answer: the recurrence reads /select/logsql/query
		}
		var upstream *upstreamStatusError
		if errors.As(err, &upstream) && (upstream.status == http.StatusNotFound || upstream.status == http.StatusMethodNotAllowed || upstream.status == http.StatusNotImplemented) {
			return false // a VictoriaLogs without stats_query_range: the recurrence reads /select/logsql/query
		}
		p.writeErr(w, ctx, err)
		return true
	}
	result, err := buildHitsRangeMetricMatrix(manualFunc, shiftSeriesSamples(series, sampleShift), start, end, step, window, p.limits().BufferedBackendBodyBytes)
	if err != nil {
		return false // the matrix does not fit the buffered body bound: the recurrence encodes only the ranked series
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true
}

// slidingRecord is one non-empty bucket of one series in one grid.
type slidingRecord struct {
	series int32
	grid   uint8
	index  int32
	count  float64
	bytes  float64
}

// slidingStats holds every grid of one evaluation. Series are identified by
// their group values joined in Loki label order, so no per-series label map is
// built until a series is selected for the response.
type slidingStats struct {
	// windowGrid marks a single grid whose bucket k already is the window of
	// evaluation step k (tumbling windows), so no recurrence is needed.
	windowGrid  bool
	groupFields []string // VictoriaLogs field names, ordered by Loki label name
	lokiNames   []string
	ids         map[string]int32
	keys        []string
	records     []slidingRecord
}

func newSlidingStats(spec statsCompatSpec) *slidingStats {
	s := &slidingStats{ids: make(map[string]int32)}
	if len(spec.GroupBy) == 0 || spec.ByExplicit {
		return s
	}
	type field struct{ vl, loki string }
	fields := make([]field, 0, len(spec.GroupBy))
	for i, name := range spec.GroupBy {
		loki := name
		if i < len(spec.OrigGroupBy) {
			loki = spec.OrigGroupBy[i]
		}
		fields = append(fields, field{name, loki})
	}
	sort.SliceStable(fields, func(i, j int) bool { return fields[i].loki < fields[j].loki })
	for _, f := range fields {
		s.groupFields = append(s.groupFields, f.vl)
		s.lokiNames = append(s.lokiNames, f.loki)
	}
	return s
}

func (s *slidingStats) metric(series int32) map[string]string {
	metric := make(map[string]string, len(s.lokiNames))
	if len(s.lokiNames) == 0 {
		return metric
	}
	for i, value := range strings.Split(s.keys[series], "\xff") {
		// Loki drops empty label values from aggregation results.
		if value != "" && i < len(s.lokiNames) {
			metric[s.lokiNames[i]] = value
		}
	}
	return metric
}

// writeSlidingWindowStatsRange evaluates the query with the two-grid recurrence
// and writes the Loki matrix. Backend errors are returned to the client rather
// than retried on a heavier path.
// writeSlidingStatsRoute marks the request as answered by the sliding stats
// evaluator for settled steps, then writes the sliding window answer.
func (p *Proxy) writeSlidingStatsRoute(w http.ResponseWriter, ctx context.Context, spec statsCompatSpec, manualFunc, field string, start, end time.Time, step, window time.Duration) {
	markQueryStepsRoute(ctx, "sliding-stats")
	p.writeSlidingWindowStatsRange(w, ctx, spec, manualFunc, field, start, end, step, window)
}

func (p *Proxy) writeSlidingWindowStatsRange(w http.ResponseWriter, ctx context.Context, spec statsCompatSpec, manualFunc, field string, start, end time.Time, step, window time.Duration) {
	// The parser stage keeps the line's own fields (slidingWindowStatsApplies admitted a query with parser stages
	// only when it can).
	if base, ok := slidingGridParserBase(spec.BaseQuery); ok {
		spec.BaseQuery = base
	}
	withBytes := field == "__bytes__"
	steps := int(end.Sub(start) / step)
	// The grids hold a step bucket of each series per step, twice (lines entering and leaving), and the seed.
	gridBuckets := int64(2*steps + 1)
	if window == step {
		gridBuckets = int64(steps + 1)
	}
	noteReadGrid(ctx, "window-grids", step, window, step, gridBuckets)
	stats := newSlidingStats(spec)
	scale := 1.0
	if manualFunc == "rate" || manualFunc == "bytes_rate" {
		scale = window.Seconds()
	}
	if topK, ranked := rangeTopKFromContext(ctx); ranked {
		// A ranking keeps k series per step: its grids are read in time chunks when one read would pass the row
		// bound (sliding_window_chunks.go). With an aggregation around the ranking the series of the query are
		// counted on its answer (manualSeriesLimit), not on this union.
		maxBytes := p.limits().BufferedBackendBodyBytes
		r := newSlidingRanker(topK, steps, maxBytes)
		err := p.rankSlidingWindowChunks(ctx, r, stats, spec.BaseQuery, withBytes, scale, steps, start, step, window)
		var body []byte
		if err == nil {
			body, err = r.encode(ctx, stats, p.manualSeriesLimit(ctx), start, step, maxBytes)
		}
		if err != nil {
			p.writeErr(w, ctx, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
		return
	}
	var err error
	if window == step {
		// Tumbling windows need one grid: its bucket (T(k)-step, T(k)] is the
		// window of step k, so no seed and no leaving-lines grid are fetched.
		stats.windowGrid = true
		err = p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridA, start.Add(-window), steps+1, step)
	} else {
		err = p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridSeed, start.Add(-window), 1, window)
		if err == nil {
			// Lines leaving the windows are read before the lines entering
			// them: ingestion during the two fetches can then only add a line
			// to a window, never subtract one that was never added.
			err = p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridC, start.Add(-window), steps, step)
		}
		if err == nil {
			err = p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridA, start, steps, step)
		}
	}
	if err != nil {
		p.writeErr(w, ctx, err)
		return
	}
	body, err := stats.encodeBusiest(ctx, withBytes, scale, steps, p.resolvedMaxStatsQuerySeries(ctx), start, step, p.limits().BufferedBackendBodyBytes)
	if err != nil {
		p.writeErr(w, ctx, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
}

// fetchSlidingStatsGrid adds the non-empty buckets
// (anchor+k*bucket, anchor+(k+1)*bucket], k in [0, buckets), of one grid.
//
// It runs a stats pipe through /select/logsql/query rather than
// stats_query_range: VictoriaLogs streams one row per non-empty bucket and
// series instead of assembling the whole matrix in memory, bucket timestamps
// keep nanosecond precision, and the explicit start and end filter the exact
// half-open range without being widened to bucket edges. The proxy reads the
// rows as a stream.
func (p *Proxy) fetchSlidingStatsGrid(ctx context.Context, stats *slidingStats, baseQuery string, withBytes bool, grid uint8, anchor time.Time, buckets int, bucket time.Duration) error {
	if buckets <= 0 {
		return nil
	}
	size := int64(bucket)
	anchorNS := anchor.UnixNano()
	// VictoriaLogs buckets are [B, B+bucket) with B = truncate(t+offset)-offset;
	// starting them at anchor+1ns gives Loki's (edge, edge+bucket] windows.
	offset := (size - ((anchorNS+1)%size+size)%size) % size
	query := slidingGridQuery(stats, baseQuery, withBytes, size, offset)
	return p.readSlidingStatsGrid(ctx, stats, query, withBytes, grid, anchorNS, buckets, size)
}

// slidingGridParserBase is baseQuery, whose pipes hold one parser stage (unpack_json, extract, extract_regexp or
// unpack_logfmt), with that stage keeping the fields a line already has (logsql.ParserKeepsFields): Loki writes an
// extracted label whose name a stream label or structured metadata already holds as name_extracted, so a grouping by
// the name reads the line's own value, as the raw-row evaluator reads it (addGroupByParsedLabels). ok is false for a
// query of several parser stages (one stage's label over another's is not read here) or none.
func slidingGridParserBase(baseQuery string) (string, bool) {
	// Without a parser stage (queryUsesParserStages) only an unpack_logfmt is rewritten: a pattern line filter's own
	// extract is left as it is.
	return logsql.ParserKeepsFields(baseQuery, !queryUsesParserStages(baseQuery))
}

// slidingGridQuery is the stats query of one grid: baseQuery's lines counted (and their bytes summed) per series in
// buckets of size nanoseconds starting offset nanoseconds before a multiple of size.
func slidingGridQuery(stats *slidingStats, baseQuery string, withBytes bool, size, offset int64) string {
	by := "_time:" + strconv.FormatInt(size, 10) + "ns"
	if offset != 0 {
		by += " offset " + strconv.FormatInt(offset, 10) + "ns"
	}
	if len(stats.groupFields) > 0 {
		by += ", " + strings.Join(stats.groupFields, ", ")
	}
	agg := "count() as " + slidingStatsValueName
	if withBytes {
		agg = "sum_len(_msg) as " + slidingStatsValueName + ", count() as " + slidingStatsCountName
	}
	return baseQuery + " | stats by (" + by + ") " + agg
}

// readSlidingStatsGrid reads the rows of one grid's stats query into stats.
func (p *Proxy) readSlidingStatsGrid(ctx context.Context, stats *slidingStats, query string, withBytes bool, grid uint8, anchorNS int64, buckets int, size int64) error {
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(anchorNS+1, 10)},
		"end":   {strconv.FormatInt(anchorNS+int64(buckets)*size+1, 10)},
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return p.redactedBackendStatusError("stats backend", resp.StatusCode, body)
	}

	parser := vlFJParserPool.Get()
	defer vlFJParserPool.Put(parser)
	// Rows, not bytes, bound this stream: each row keeps one compact record.
	// The byte guard only stops a backend that sends oversized rows.
	rowLimit, err := p.manualMetricRowBudget()
	if err != nil {
		return err
	}
	limited := &io.LimitedReader{R: resp.Body, N: int64(rowLimit)*slidingStatsMaxRowBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 64<<10), slidingStatsMaxRowBytes)
	var key strings.Builder
	rows := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if limited.N <= 0 {
			return slidingStatsByteBudgetError(rowLimit)
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		row, err := parser.ParseBytes(line)
		if err != nil {
			return fmt.Errorf("parse stats row: %w", err)
		}
		bucketStart, err := time.Parse(time.RFC3339Nano, string(row.GetStringBytes("_time")))
		if err != nil {
			return fmt.Errorf("parse stats bucket time: %w", err)
		}
		// Round to the nearest edge: a backend that truncates the bucket
		// timestamp to milliseconds must not shift every row by one bucket.
		index := (bucketStart.UnixNano() - 1 - anchorNS + size/2) / size
		if index < 0 || index >= int64(buckets) {
			continue
		}
		key.Reset()
		for i, field := range stats.groupFields {
			if i > 0 {
				key.WriteByte('\xff')
			}
			key.Write(row.GetStringBytes(field))
		}
		id, ok := stats.ids[key.String()]
		if !ok {
			k := key.String()
			id = int32(len(stats.keys))
			stats.ids[k] = id
			stats.keys = append(stats.keys, k)
		}
		rec := slidingRecord{series: id, grid: grid, index: int32(index)}
		if rec.count, err = strconv.ParseFloat(string(row.GetStringBytes(slidingStatsValueName)), 64); err != nil {
			return fmt.Errorf("parse stats value: %w", err)
		}
		if withBytes {
			rec.bytes = rec.count
			if rec.count, err = strconv.ParseFloat(string(row.GetStringBytes(slidingStatsCountName)), 64); err != nil {
				return fmt.Errorf("parse stats count: %w", err)
			}
		}
		if rows++; rows > rowLimit {
			return newRowLimitError(rowLimit, "stats rows in one bucket grid", 0)
		}
		stats.records = append(stats.records, rec)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stats rows: %w", err)
	}
	if limited.N <= 0 {
		return slidingStatsByteBudgetError(rowLimit)
	}
	return ctx.Err()
}

// bySeries reorders records by series and bucket index and returns the start
// offset of each series' records.
func (s *slidingStats) bySeries() []int {
	offsets := make([]int, len(s.keys)+1)
	for _, rec := range s.records {
		offsets[rec.series+1]++
	}
	for i := 1; i < len(offsets); i++ {
		offsets[i] += offsets[i-1]
	}
	ordered := make([]slidingRecord, len(s.records))
	next := append([]int(nil), offsets[:len(offsets)-1]...)
	for _, rec := range s.records {
		ordered[next[rec.series]] = rec
		next[rec.series]++
	}
	s.records = ordered
	for series := 0; series < len(s.keys); series++ {
		recs := s.records[offsets[series]:offsets[series+1]]
		if !sort.SliceIsSorted(recs, func(i, j int) bool { return recs[i].index < recs[j].index }) {
			sort.SliceStable(recs, func(i, j int) bool { return recs[i].index < recs[j].index })
		}
	}
	return offsets
}

// slidingSegment is a run of evaluation steps [from, to) sharing one value.
type slidingSegment struct {
	from, to int32
	value    float64
}

// slidingWindows applies the recurrence to one series' records, ordered by
// bucket index, and calls emit for each run of steps whose window holds at
// least one line. Work is proportional to the records, not to the steps.
func slidingWindows(recs []slidingRecord, withBytes bool, scale float64, steps int, windowGrid bool, emit func(slidingSegment)) {
	if windowGrid {
		for _, rec := range recs {
			if rec.count <= 0 || int(rec.index) > steps {
				continue
			}
			emit(slidingSegment{from: rec.index, to: rec.index + 1, value: slidingWindowValue(withBytes, rec.count, rec.bytes) / scale})
		}
		return
	}
	count, value := 0.0, 0.0
	for _, rec := range recs {
		if rec.grid == slidingGridSeed {
			count += rec.count
			value += rec.bytes
		}
	}
	from := int32(0)
	for i := 0; i < len(recs); {
		idx := recs[i].index
		// W is constant for steps [from, idx]; A_idx and C_idx move it into idx+1.
		if count > 0 && idx >= from {
			emit(slidingSegment{from: from, to: idx + 1, value: slidingWindowValue(withBytes, count, value) / scale})
		}
		for ; i < len(recs) && recs[i].index == idx; i++ {
			switch recs[i].grid {
			case slidingGridA:
				count += recs[i].count
				value += recs[i].bytes
			case slidingGridC:
				count -= recs[i].count
				value -= recs[i].bytes
			}
		}
		from = idx + 1
	}
	if count > 0 && int32(steps) >= from {
		emit(slidingSegment{from: from, to: int32(steps) + 1, value: slidingWindowValue(withBytes, count, value) / scale})
	}
}

func slidingWindowValue(withBytes bool, count, bytes float64) float64 {
	if withBytes {
		return bytes
	}
	return count
}

// encodeBusiest encodes the windows of every series. Above maxSeries series it
// returns Loki's series limit error, or for Logs Drilldown keeps the maxSeries
// series with the largest totals, matching the stats fast path, and records
// Loki's partial-result warning.
func (s *slidingStats) encodeBusiest(ctx context.Context, withBytes bool, scale float64, steps, maxSeries int, start time.Time, step time.Duration, maxBytes int) ([]byte, error) {
	offsets := s.bySeries()
	selected := make([]int32, 0, len(s.keys))
	for series := range s.keys {
		selected = append(selected, int32(series))
	}
	if maxSeries > 0 && len(selected) > maxSeries {
		if err := seriesLimitReached(ctx, maxSeries); err != nil {
			return nil, err
		}
		if scope := seriesLimitScopeFrom(ctx); scope != nil && scope.lokiCut {
			// Loki's partial answer: per split, the series met first (their windows' steps); cutLokiSplits keeps
			// Loki's series and steps on the finished answer.
			keys := make([]string, len(selected))
			index := make(map[string]int32, len(selected))
			for i, series := range selected {
				keys[i] = strconv.Itoa(int(series))
				index[keys[i]] = series
			}
			kept := seriesKeptForLimit(ctx, keys, nil, func(k string, yield func(int64)) {
				series := index[k]
				slidingWindows(s.records[offsets[series]:offsets[series+1]], withBytes, scale, steps, s.windowGrid, func(seg slidingSegment) {
					scope.firstStepsInSplits(start.Add(time.Duration(seg.from)*step).UnixNano(), start.Add(time.Duration(seg.to-1)*step).UnixNano(), yield)
				})
			}, maxSeries)
			selected = selected[:0]
			for _, k := range kept {
				selected = append(selected, index[k])
			}
		} else {
			totals := make([]float64, len(s.keys))
			for _, rec := range s.records {
				if rec.grid != slidingGridC {
					totals[rec.series] += slidingWindowValue(withBytes, rec.count, rec.bytes)
				}
			}
			sort.SliceStable(selected, func(i, j int) bool { return totals[selected[i]] > totals[selected[j]] })
			selected = selected[:maxSeries]
		}
	}
	out := make([]slidingOutputSeries, 0, len(selected))
	for n, series := range selected {
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		entry := slidingOutputSeries{metric: s.metric(series)}
		slidingWindows(s.records[offsets[series]:offsets[series+1]], withBytes, scale, steps, s.windowGrid, func(seg slidingSegment) {
			entry.segments = append(entry.segments, seg)
		})
		out = append(out, entry)
	}
	return encodeSlidingWindowMatrix(out, start, step, maxBytes)
}

type topKCandidate struct {
	series int32
	value  float64
}

// topKHeap keeps the best k candidates of one step; its root is the candidate
// the next better one displaces.
type topKHeap struct {
	items      []topKCandidate
	descending bool
	keys       []string
}

func (h *topKHeap) Len() int           { return len(h.items) }
func (h *topKHeap) Less(i, j int) bool { return h.better(h.items[j], h.items[i]) }
func (h *topKHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *topKHeap) Push(x any)         { h.items = append(h.items, x.(topKCandidate)) }
func (h *topKHeap) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}

// better reports whether a ranks before b; NaN ranks last and ties break by
// label order.
func (h *topKHeap) better(a, b topKCandidate) bool {
	if c := topKCompare(a.value, b.value, h.descending); c != 0 {
		return c < 0
	}
	return h.keys[a.series] < h.keys[b.series]
}

// encodeTopK keeps, per step, the k series Loki's topk or bottomk selects
// among every series of each group, with one bounded heap per step and group.
// Like Loki it counts the series of the result, the union of
// the ones that ranked at some step, against maxSeries: Loki's error for a
// plain client, the series with the most selected samples and Loki's warning
// for Logs Drilldown.
func (s *slidingStats) encodeTopK(ctx context.Context, withBytes bool, scale float64, steps, maxSeries int, topK rangeTopK, start time.Time, step time.Duration, maxBytes int) ([]byte, error) {
	r := newSlidingRanker(topK, steps, maxBytes)
	if err := r.rank(ctx, s, withBytes, scale, steps, 0, int32(steps)+1, false); err != nil {
		return nil, err
	}
	return r.encode(ctx, s, maxSeries, start, step, maxBytes)
}

// slidingRanker holds the heaps of a ranking whose windows are ranked in one pass or in time chunks
// (rankSlidingWindowChunks): each step's heaps are filled by the pass that holds the step.
type slidingRanker struct {
	topK rangeTopK
	// One group keeps a heap per step in a slice; groups keep one per step and
	// group that holds a series, in a map.
	byStep  []*topKHeap
	byGroup map[slidingRankSlot]*topKHeap
	groupOf []int32
	groups  map[string]int32
	// Every sample a heap holds is a sample of the response, which costs at
	// least slidingPointOverheadBytes there: the heaps of a response over the
	// limit stop growing. In memory a held sample costs more than in the
	// response (a 16-byte candidate plus its share of a heap header and map
	// slot, about 60-100 bytes when every group is a single series), so this
	// bounds the heaps to a small multiple of maxBytes, not to maxBytes.
	kept, keptLimit, maxBytes int
}

type slidingRankSlot struct{ step, group int32 }

func newSlidingRanker(topK rangeTopK, steps, maxBytes int) *slidingRanker {
	r := &slidingRanker{topK: topK, keptLimit: maxBytes / slidingPointOverheadBytes, maxBytes: maxBytes}
	if topK.group == nil {
		r.byStep = make([]*topKHeap, steps+1)
	} else {
		r.byGroup = make(map[slidingRankSlot]*topKHeap)
		r.groups = map[string]int32{}
	}
	return r
}

// rank adds the windows of s's records to the heaps: the steps base+k of the local steps k in [0, limit), the
// windows slidingWindows derives over localSteps steps.
//
// A chunk of a chunked ranking (compact) visits only the series its records hold: the records are sorted by series and
// bucket index, so the work is the chunk's, not every series the earlier chunks met.
func (r *slidingRanker) rank(ctx context.Context, s *slidingStats, withBytes bool, scale float64, localSteps int, base, limit int32, compact bool) error {
	var offsets []int
	var order []int32
	if compact {
		slices.SortStableFunc(s.records, func(a, b slidingRecord) int {
			if a.series != b.series {
				return cmp.Compare(a.series, b.series)
			}
			return cmp.Compare(a.index, b.index)
		})
		for i, rec := range s.records {
			if i == 0 || rec.series != s.records[i-1].series {
				order = append(order, rec.series)
				offsets = append(offsets, i)
			}
		}
		offsets = append(offsets, len(s.records))
	} else {
		offsets = s.bySeries()
	}
	if r.byGroup != nil {
		for series := len(r.groupOf); series < len(s.keys); series++ {
			key := topKGroupKey(r.topK.group, s.metric(int32(series)))
			id, ok := r.groups[key]
			if !ok {
				id = int32(len(r.groups))
				r.groups[key] = id
			}
			r.groupOf = append(r.groupOf, id)
		}
	}
	tooLarge := false
	visits := len(s.keys)
	if compact {
		visits = len(order)
	}
	for i := 0; i < visits; i++ {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		series, recs := i, s.records[offsets[i]:offsets[i+1]]
		if compact {
			series = int(order[i])
		}
		slidingWindows(recs, withBytes, scale, localSteps, s.windowGrid, func(seg slidingSegment) {
			for k := seg.from; k < seg.to && k < limit && !tooLarge; k++ {
				at := base + k
				var h *topKHeap
				if r.byStep != nil {
					h = r.byStep[at]
				} else {
					h = r.byGroup[slidingRankSlot{at, r.groupOf[series]}]
				}
				if h == nil {
					h = &topKHeap{descending: r.topK.descending, keys: s.keys}
					if r.byStep != nil {
						r.byStep[at] = h
					} else {
						r.byGroup[slidingRankSlot{at, r.groupOf[series]}] = h
					}
				}
				c := topKCandidate{series: int32(series), value: seg.value}
				if h.Len() < r.topK.k {
					if r.kept++; r.kept > r.keptLimit {
						tooLarge = true
						return
					}
					heap.Push(h, c)
				} else if h.better(c, h.items[0]) {
					h.items[0] = c
					heap.Fix(h, 0)
				}
			}
		})
		if tooLarge {
			return errSlidingResponseTooLarge(r.maxBytes)
		}
	}
	return nil
}

// encode writes the ranked samples of every step, s naming the series.
func (r *slidingRanker) encode(ctx context.Context, s *slidingStats, maxSeries int, start time.Time, step time.Duration, maxBytes int) ([]byte, error) {
	points := make(map[int32][]slidingSegment)
	collect := func(step int32, h *topKHeap) {
		for _, c := range h.items {
			points[c.series] = append(points[c.series], slidingSegment{from: step, to: step + 1, value: c.value})
		}
	}
	for k, h := range r.byStep {
		if h != nil {
			collect(int32(k), h)
		}
	}
	for at, h := range r.byGroup {
		collect(at.step, h)
	}
	ranked := make([]int32, 0, len(points))
	for series := range points {
		ranked = append(ranked, series)
	}
	if maxSeries > 0 && len(ranked) > maxSeries {
		if err := seriesLimitReached(ctx, maxSeries); err != nil {
			return nil, err
		}
		sort.Slice(ranked, func(i, j int) bool {
			if a, b := len(points[ranked[i]]), len(points[ranked[j]]); a != b {
				return a > b
			}
			return s.keys[ranked[i]] < s.keys[ranked[j]]
		})
		ranked = ranked[:maxSeries]
	}
	out := make([]slidingOutputSeries, 0, len(ranked))
	for _, series := range ranked {
		out = append(out, slidingOutputSeries{metric: s.metric(series), segments: points[series]})
	}
	return encodeSlidingWindowMatrix(out, start, step, maxBytes)
}

// slidingPointOverheadBytes is what a sample costs in the response besides its
// formatted value: the timestamp, the brackets and the separators.
const slidingPointOverheadBytes = 26

func errSlidingResponseTooLarge(maxBytes int) error {
	return newManualMetricResponseTooLarge(int64(maxBytes))
}

type slidingOutputSeries struct {
	metric   map[string]string
	segments []slidingSegment
}

func encodeSlidingWindowMatrix(series []slidingOutputSeries, start time.Time, step time.Duration, maxBytes int) ([]byte, error) {
	keys := make([]string, len(series))
	order := make([]int, len(series))
	for i := range series {
		keys[i] = canonicalLabelsKey(series[i].metric)
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return keys[order[i]] < keys[order[j]] })

	encodedBytes := 0
	results := make([]map[string]interface{}, 0, len(series))
	for _, i := range order {
		entry := series[i]
		if len(entry.segments) == 0 {
			continue
		}
		sort.Slice(entry.segments, func(a, b int) bool { return entry.segments[a].from < entry.segments[b].from })
		for k, v := range entry.metric {
			encodedBytes += len(k) + len(v) + 6
		}
		var points [][]interface{}
		for _, seg := range entry.segments {
			formatted := strconv.FormatFloat(seg.value, 'f', -1, 64)
			for k := seg.from; k < seg.to; k++ {
				if encodedBytes += len(formatted) + slidingPointOverheadBytes; encodedBytes > maxBytes {
					return nil, errSlidingResponseTooLarge(maxBytes)
				}
				t := start.Add(time.Duration(k) * step)
				points = append(points, []interface{}{lokiSampleSeconds(t.UnixNano()), formatted})
			}
		}
		results = append(results, map[string]interface{}{"metric": entry.metric, "values": points})
	}
	return marshalManualMetricResponse("matrix", results), nil
}
