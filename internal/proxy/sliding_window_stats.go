package proxy

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
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
}

// withRangeTopK marks a range metric evaluation whose result feeds topk or
// bottomk, so the evaluator ranks every series per step instead of truncating
// to the busiest series first.
func withRangeTopK(ctx context.Context, k int, descending bool) context.Context {
	return context.WithValue(ctx, rangeTopKKey{}, rangeTopK{k: k, descending: descending})
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
	if step <= 0 || window <= 0 || end.Before(start) || queryUsesParserStages(spec.BaseQuery) {
		return false
	}
	if len(spec.GroupBy) == 0 && !spec.ByExplicit {
		return false
	}
	for _, g := range spec.GroupBy {
		if g == "_stream" {
			return false
		}
	}
	if !p.supportsStatsRangeOffset() {
		return false
	}
	if _, ranked := rangeTopKFromContext(ctx); ranked {
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
func (p *Proxy) writeSlidingWindowStatsRange(w http.ResponseWriter, ctx context.Context, spec statsCompatSpec, manualFunc, field string, start, end time.Time, step, window time.Duration) {
	withBytes := field == "__bytes__"
	steps := int(end.Sub(start) / step)
	stats := newSlidingStats(spec)
	err := p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridSeed, start.Add(-window), 1, window)
	if err == nil {
		err = p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridA, start, steps, step)
	}
	if err == nil {
		err = p.fetchSlidingStatsGrid(ctx, stats, spec.BaseQuery, withBytes, slidingGridC, start.Add(-window), steps, step)
	}
	if err != nil {
		p.writeError(w, badRequestStatusOr(err, statusFromUpstreamErr(err)), err.Error())
		return
	}
	scale := 1.0
	if manualFunc == "rate" || manualFunc == "bytes_rate" {
		scale = window.Seconds()
	}
	var body []byte
	if topK, ranked := rangeTopKFromContext(ctx); ranked {
		body, err = stats.encodeTopK(ctx, withBytes, scale, steps, topK, start, step)
	} else {
		body, err = stats.encodeBusiest(ctx, withBytes, scale, steps, p.resolvedMaxStatsQuerySeries(), start, step)
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		if isCanceledErr(err) {
			status = statusFromUpstreamErr(err)
		}
		p.writeError(w, status, err.Error())
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
	params := url.Values{
		"query": {baseQuery + " | stats by (" + by + ") " + agg},
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
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if limited.N <= 0 {
			return fmt.Errorf("manual metric stats response exceeds %d bytes (%d rows of %d bytes); narrow the query or increase -manual-range-metric-row-limit", int64(rowLimit)*slidingStatsMaxRowBytes, rowLimit, slidingStatsMaxRowBytes)
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
		index := (bucketStart.UnixNano() - 1 - anchorNS) / size
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
		if len(stats.records) >= rowLimit {
			return fmt.Errorf("manual range metric row limit exceeded (%d); narrow the query or increase -manual-range-metric-row-limit", rowLimit)
		}
		stats.records = append(stats.records, rec)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stats rows: %w", err)
	}
	if limited.N <= 0 {
		return fmt.Errorf("manual metric stats response exceeds %d bytes (%d rows of %d bytes); narrow the query or increase -manual-range-metric-row-limit", int64(rowLimit)*slidingStatsMaxRowBytes, rowLimit, slidingStatsMaxRowBytes)
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
func slidingWindows(recs []slidingRecord, withBytes bool, scale float64, steps int, emit func(slidingSegment)) {
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

// encodeBusiest keeps the maxSeries series with the largest totals, matching
// the stats fast path, and encodes their windows.
func (s *slidingStats) encodeBusiest(ctx context.Context, withBytes bool, scale float64, steps, maxSeries int, start time.Time, step time.Duration) ([]byte, error) {
	offsets := s.bySeries()
	selected := make([]int32, 0, len(s.keys))
	for series := range s.keys {
		selected = append(selected, int32(series))
	}
	if maxSeries > 0 && len(selected) > maxSeries {
		totals := make([]float64, len(s.keys))
		for _, rec := range s.records {
			if rec.grid != slidingGridC {
				totals[rec.series] += slidingWindowValue(withBytes, rec.count, rec.bytes)
			}
		}
		sort.SliceStable(selected, func(i, j int) bool { return totals[selected[i]] > totals[selected[j]] })
		selected = selected[:maxSeries]
	}
	out := make([]slidingOutputSeries, 0, len(selected))
	for n, series := range selected {
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		entry := slidingOutputSeries{metric: s.metric(series)}
		slidingWindows(s.records[offsets[series]:offsets[series+1]], withBytes, scale, steps, func(seg slidingSegment) {
			entry.segments = append(entry.segments, seg)
		})
		out = append(out, entry)
	}
	return encodeSlidingWindowMatrix(out, start, step)
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

// better reports whether a ranks before b; ties break by label order.
func (h *topKHeap) better(a, b topKCandidate) bool {
	if a.value != b.value {
		if h.descending {
			return a.value > b.value
		}
		return a.value < b.value
	}
	return h.keys[a.series] < h.keys[b.series]
}

// encodeTopK keeps, per step, the k series Loki's topk or bottomk selects
// among every series, with one bounded heap per step.
func (s *slidingStats) encodeTopK(ctx context.Context, withBytes bool, scale float64, steps int, topK rangeTopK, start time.Time, step time.Duration) ([]byte, error) {
	offsets := s.bySeries()
	heaps := make([]*topKHeap, steps+1)
	for series := range s.keys {
		if series%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		slidingWindows(s.records[offsets[series]:offsets[series+1]], withBytes, scale, steps, func(seg slidingSegment) {
			for k := seg.from; k < seg.to; k++ {
				h := heaps[k]
				if h == nil {
					h = &topKHeap{descending: topK.descending, keys: s.keys}
					heaps[k] = h
				}
				c := topKCandidate{series: int32(series), value: seg.value}
				if h.Len() < topK.k {
					heap.Push(h, c)
				} else if h.better(c, h.items[0]) {
					h.items[0] = c
					heap.Fix(h, 0)
				}
			}
		})
	}
	points := make(map[int32][]slidingSegment)
	for k, h := range heaps {
		if h == nil {
			continue
		}
		for _, c := range h.items {
			points[c.series] = append(points[c.series], slidingSegment{from: int32(k), to: int32(k) + 1, value: c.value})
		}
	}
	out := make([]slidingOutputSeries, 0, len(points))
	for series, segments := range points {
		out = append(out, slidingOutputSeries{metric: s.metric(series), segments: segments})
	}
	return encodeSlidingWindowMatrix(out, start, step)
}

type slidingOutputSeries struct {
	metric   map[string]string
	segments []slidingSegment
}

func encodeSlidingWindowMatrix(series []slidingOutputSeries, start time.Time, step time.Duration) ([]byte, error) {
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
				if encodedBytes += len(formatted) + 26; encodedBytes > maxBufferedBackendBodyBytes {
					return nil, fmt.Errorf("manual metric response exceeds %d bytes", maxBufferedBackendBodyBytes)
				}
				t := start.Add(time.Duration(k) * step)
				points = append(points, []interface{}{float64(t.UnixNano()) / float64(time.Second), formatted})
			}
		}
		results = append(results, map[string]interface{}{"metric": entry.metric, "values": points})
	}
	return marshalManualMetricResponse("matrix", results), nil
}
