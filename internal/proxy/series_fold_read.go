package proxy

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/grafana/jsonparser"
	fj "github.com/valyala/fastjson"
)

// The folded bucket reads of a parser's series identity over a range longer than the step (lower.SeriesFold).
//
// A request marked identityAheadKey reads the range function's partials per series hash and per anchored bucket, in
// the time chunks of listed_window_chunks.go. Each chunk is one scan of /select/logsql/query (seriesFoldPost) whose
// answer, read back into the shape of a stats_query_range answer (seriesFoldRead.matrix), holds per bucket and group the
// partials of every series with lines in more than one bucket of the read (their hash kept), and one row of the series
// that lie wholly in that bucket, folded by VictoriaLogs: the sum (or extreme) of their values and their count, under
// an empty hash. The window evaluator merges each kept series' buckets into its windows as before, and adds a folded
// row's value to the outer aggregation of every window that holds its bucket, weighted by its count
// (hitsRangeMetricSeries, foldAccumulator.addPartial). The answer is the per-series one: a window holds all of a
// folded series' lines in the read or none, and each window lies in its chunk's read.
//
// The answer then holds a row per bucket and group for the folded series, not one per line: a key of its own on
// every line (a request id) costs about what the stream's series did. The buckets of these reads depend on the
// read's window (a series spans buckets inside one read, not inside another), so the stats extent cache never
// answers them.

// seriesFoldKey carries the seriesFoldRead of a request's bucket reads.
type seriesFoldKey struct{}

// seriesFoldRead is how a request's bucket reads fold the series that lie in one bucket.
type seriesFoldRead struct {
	outer logql.VectorOp
	// plan is the request's plan: the folded read's base unpacks the line once (lower.SeriesFoldBase).
	plan *queryplan.Plan
	// columns are the bucket stats' results; value the math expression of a series' value in a bucket; valueColumn
	// and countColumn the results a folded row's value and count are read into (countColumn may be
	// lower.SeriesFoldCount, which the fold adds).
	columns                  []string
	stats                    []logsql.StatsFuncAlias
	value                    string
	valueColumn, countColumn string
	// stats are the bucket stats' functions (unwrapBucketAgg's, typed).
	// added says the fold adds lower.SeriesFoldCount to the bucket stats' results.
	added bool
	// by is the grouping of one read's bucket stats (foldedQuery sets it on the read's own copy), the labels of its
	// answer.
	by []string
}

// seriesFoldOff reads the series of a parser's identity one by one, as before the fold (tests compare the two).
var seriesFoldOff bool

// seriesFoldHash is the pipe that ends the identity of a parser of every key (lower.Profile IdentityAhead).
var seriesFoldHash = logsql.PipeHash{Fields: []string{dynamicSeriesHashLabel}, Result: dynamicSeriesHashLabel}.String()

// withSeriesFold marks the bucket reads of a request of a parser's identity over a range longer than the step
// (identityAhead) to fold the series that lie in one bucket, when its outer aggregation and function fold (one
// vector aggregation, the matrix fold of the request). The extent cache is off for them.
func (p *Proxy) withSeriesFold(r *http.Request, spec statsCompatSpec, manualFunc string, fold *matrixFold) *http.Request {
	if seriesFoldOff || !identityAhead(r.Context()) || fold == nil || len(fold.levels) != 1 || !spec.gated() {
		return r
	}
	chain, ok := listedIdentityChain(r.FormValue("query"))
	if !ok || len(chain) != 1 {
		return r
	}
	ungated := spec.ungated()
	if !strings.HasSuffix(ungated, seriesFoldHash) {
		return r
	}
	read := &seriesFoldRead{outer: chain[0].Op, plan: resolvedPlan(dynamicQuery(r.FormValue("query")))}
	v := translate.UnwrapValueAlias
	switch manualFunc {
	case "avg":
		read.columns, read.value, read.valueColumn, read.countColumn = []string{"__lvp_sum", "__sample_count"}, "__lvp_sum / __sample_count", "__lvp_sum", "__sample_count"
		read.stats = []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: v}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}}
	case "max", "min":
		read.columns, read.value, read.valueColumn = []string{"c"}, "c", "c"
		read.stats = []logsql.StatsFuncAlias{{Func: logsql.Max{Field: v}, Alias: "c"}}
		if manualFunc == "min" {
			read.stats[0].Func = logsql.Min{Field: v}
		}
		if read.outer == logql.VectorAvg {
			read.countColumn, read.added = lower.SeriesFoldCount, true
		}
	case "stddev", "stdvar":
		read.columns = []string{deviationMeanName, "__sample_count", deviationBucketName}
		read.stats = []logsql.StatsFuncAlias{{Func: logsql.Avg{Field: v}, Alias: deviationMeanName}, {Func: logsql.Count{}, Alias: "__sample_count"},
			{Func: logsql.Stddev{Field: v}, Alias: deviationBucketName}}
		read.value, read.valueColumn, read.countColumn = deviationBucketName, deviationBucketName, "__sample_count"
		if manualFunc == "stdvar" {
			read.value = deviationBucketName + " * " + deviationBucketName
		}
	default:
		return r
	}
	ctx := context.WithValue(context.WithValue(r.Context(), seriesFoldKey{}, read), statsExtentRequestKey{}, false)
	return r.WithContext(ctx)
}

// seriesFoldFrom is the fold of ctx's bucket reads, nil when they read every series.
func seriesFoldFrom(ctx context.Context) *seriesFoldRead {
	read, _ := ctx.Value(seriesFoldKey{}).(*seriesFoldRead)
	return read
}

// query is the folded bucket query of the base query baseQuery and the bucket stats statsAggFunc grouped by groupBy,
// for a read from start in buckets of hitStep; ok is false when the fold does not apply (the read then reads every
// series).
func (s *seriesFoldRead) query(baseQuery string, groupBy []string, statsAggFunc string, start time.Time, hitStep time.Duration) (string, bool) {
	// The bucket stats are the ones the window evaluator asked for (statsAggFunc), written typed.
	if s == nil || !strings.HasSuffix(logsql.PipeStats{Funcs: s.stats}.String(), statsAggFunc) {
		return "", false
	}
	bucket, offset := slidingStatsGrid(start, hitStep)
	read, ok := lower.SeriesFold{Outer: s.outer, By: groupBy, Stats: s.stats, Columns: s.columns, Value: s.value, Bucket: bucket, Offset: offset}.Pipes()
	if !ok {
		return "", false
	}
	base := lower.SeriesFoldBase(s.plan, baseQuery)
	var b strings.Builder
	b.Grow(len(base) + 2048)
	b.WriteString(base)
	for _, p := range read.Pipes {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	return b.String(), true
}

// foldedQuery is statsQuery, or the folded query when s folds the read, and the fold it applies (nil when it reads
// every series); ctx then carries the read's own fold, its grouping set (seriesFoldPost reads the answer by it).
func (s *seriesFoldRead) foldedQuery(ctx context.Context, statsQuery, baseQuery string, groupBy []string, statsAggFunc string, start time.Time, hitStep time.Duration) (context.Context, string, *seriesFoldRead) {
	if folded, ok := s.query(baseQuery, groupBy, statsAggFunc, start, hitStep); ok {
		read := *s
		read.by = groupBy
		return context.WithValue(ctx, seriesFoldKey{}, &read), folded, &read
	}
	return ctx, statsQuery, nil
}

// statsReadSlot takes a stats_query_range slot for a bucket read: a folded read takes it in the call its identical
// reads share (seriesFoldPost), so it takes none here.
func (p *Proxy) statsReadSlot(ctx context.Context, fold *seriesFoldRead) (func(), error) {
	if fold != nil {
		return func() {}, nil
	}
	return p.takeStatsQueryRangeSlot(ctx)
}

// splitFoldedKeys parts keys (sorted) into the series read one by one and the folded rows, each in their order.
func splitFoldedKeys(keys []string, series map[string]manualSeriesSamples) (kept, folded []string) {
	kept = keys[:0:0]
	for _, key := range keys {
		if series[key].Folded {
			folded = append(folded, key)
		} else {
			kept = append(kept, key)
		}
	}
	return kept, folded
}

// addFoldedSeries adds the windows of the folded rows keys (foldedWindows) to the matrix fold sink, after the series
// read one by one.
func addFoldedSeries(sink *matrixFoldSink, series map[string]manualSeriesSamples, keys []string, startNS, endNS, stepNS, windowNS int64) error {
	if len(keys) == 0 {
		return nil
	}
	if sink == nil {
		return &statsBucketsUnreadableError{"a folded series row without a fold"}
	}
	var points []typedPoint
	var weights []float64
	for _, key := range keys {
		entry := series[key]
		points, weights = foldedWindows(entry, sink.acc.level.merge, startNS, endNS, stepNS, windowNS, points[:0], weights[:0])
		sink.addPartial(entry.Metric, points, weights)
	}
	return nil
}

// errSeriesFoldTooLong is a folded read whose query, as it would be sent, passes VictoriaLogs' default query length
// (-search.maxQueryLen): the read is sent again reading every series (collectRangeMetricHitsAux).
var errSeriesFoldTooLong = errors.New("folded series read longer than VictoriaLogs' query length")

// extraResults is the number of results per group the fold adds to the bucket stats'.
func (s *seriesFoldRead) extraResults() int {
	if s != nil && s.added {
		return 1
	}
	return 0
}

// slidingStatsGrid is the step and offset setSlidingStatsRangeParams sends for buckets of hitStep from start.
func slidingStatsGrid(start time.Time, hitStep time.Duration) (step, offset string) {
	if hitStep%time.Second == 0 {
		step = strconv.FormatInt(int64(hitStep/time.Second), 10) + "s"
	} else {
		step = strconv.FormatInt(int64(hitStep), 10) + "ns"
	}
	shift := ((start.UnixNano()+1)%int64(hitStep) + int64(hitStep)) % int64(hitStep)
	return step, strconv.FormatInt(-shift, 10) + "ns"
}

// foldedWindows evaluates a folded row's windows at the steps of [startNS, endNS]: per step the outer
// aggregation's merge of the values of the buckets in its window (their sum, or their extreme for max and min), and
// the series they hold (the counts' sum). A step whose window holds no bucket has no point.
func foldedWindows(entry manualSeriesSamples, merge withoutMerge, startNS, endNS, stepNS, windowNS int64, points []typedPoint, weights []float64) ([]typedPoint, []float64) {
	samples := sortedHitsSamples(entry.Samples)
	counts := sortedHitsSamples(entry.Counts)
	var sampleWindow, countWindow windowCursor
	for i, n := int64(0), stepCount(startNS, endNS, stepNS); i < n; i++ {
		tNS := startNS + i*stepNS
		lo, hi := sampleWindow.span(samples, tNS-windowNS, tNS)
		cLo, cHi := countWindow.span(counts, tNS-windowNS, tNS)
		if lo >= hi {
			continue
		}
		v := samples[lo].value
		for _, s := range samples[lo+1 : hi] {
			switch {
			case merge.max:
				v = math.Max(v, s.value)
			case merge.min:
				v = math.Min(v, s.value)
			default:
				v += s.value
			}
		}
		var w float64
		for _, c := range counts[cLo:cHi] {
			w += c.value
		}
		points = append(points, typedPoint{ts: lokiSampleSeconds(tNS), v: v})
		weights = append(weights, max(w, 1))
	}
	return points, weights
}

// readsRow reads one result of a folded bucket answer into seriesMap and reports whether it took it: the results of a
// folded row (an empty series hash) go into its entry (Folded: the value column into Samples, the count column into
// Counts, the others dropped), and the count the fold adds is dropped from every other row.
func (s *seriesFoldRead) readsRow(seriesMap map[string]manualSeriesSamples, seriesKey string, metricObj *fj.Object, metric map[string]string, name string, values []byte, start time.Time, hitStep time.Duration) bool {
	if hash := metricObj.Get(dynamicSeriesHashLabel); hash == nil || len(hash.GetStringBytes()) > 0 {
		return s.added && name == lower.SeriesFoldCount
	}
	entry := seriesMap[seriesKey]
	entry.Metric, entry.Folded = metric, true
	switch name {
	case s.valueColumn:
		entry.Samples = appendBucketSamples(entry.Samples, values, start, hitStep)
	case s.countColumn:
		entry.Counts = appendBucketSamples(entry.Counts, values, start, hitStep)
	}
	seriesMap[seriesKey] = entry
	return true
}

// seriesFoldPost answers a folded bucket read (params rewritten for the request's unwrap check, sentParams as sent)
// through one VictoriaLogs call shared by the identical reads in flight: the folded answer is the same for every
// request of a tenant scope, so the call holds one stats_query_range slot (collectRangeMetricHitsAux takes none for
// these reads) and its answer, read up to the stats body bound and read back into a stats_query_range answer
// (seriesFoldRead.matrix), is handed to each read, which strips its own unwrap counter from it. Any number of identical
// cold requests make one call; the reads sharing it are not cut short by one another's cancellation.
func (p *Proxy) seriesFoldPost(ctx context.Context, path string, params, sentParams url.Values) (*http.Response, error) {
	if len(params.Get("query")) > vlDefaultMaxQueryLen {
		return nil, errSeriesFoldTooLong
	}
	fold := seriesFoldFrom(ctx)
	key := "series-fold:" + p.contextScopeFingerprint(ctx) + ":" + params.Encode()
	if full, _ := ctx.Value(levelFullChainKey{}).(bool); full {
		key += ":full-chain"
	}
	status, header, body, err := p.coalescer.DoWithGuardCtx(ctx, key, p.breaker.Allow, func(callCtx context.Context) (*http.Response, error) {
		release, err := p.takeStatsQueryRangeSlot(callCtx)
		if err != nil {
			return nil, err
		}
		defer release()
		resp, err := p.vlPostSend(callCtx, path, params, sentParams, nil, nil, true)
		if err != nil {
			return nil, err
		}
		// The coalescer reads the body once, up to the stats body bound.
		resp.Body = &seriesFoldBody{ReadCloser: resp.Body, left: maxStatsBucketResponseBytes}
		if resp.StatusCode != http.StatusOK {
			return resp, nil
		}
		buf := seriesFoldBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		_, err = buf.ReadFrom(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			putSeriesFoldBuf(buf)
			return nil, err
		}
		// The matrix copies what it keeps of the rows: the buffer goes back to the pool.
		matrix, err := fold.matrix(buf.Bytes())
		putSeriesFoldBuf(buf)
		if err != nil {
			return nil, err
		}
		resp.Body, resp.ContentLength = io.NopCloser(bytes.NewReader(matrix)), int64(len(matrix))
		resp.Header.Set("Content-Type", "application/json")
		resp.Header.Del("Content-Length")
		return resp, nil
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return nil, errors.New("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return nil, err
	}
	// The shared header is the call's: each read gets a copy it may set.
	header = header.Clone()
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
}

// seriesFoldBufPool holds the buffers folded answers are read into; putSeriesFoldBuf keeps none larger than
// seriesFoldBufKeep, so a large answer's buffer is not held after its read.
var seriesFoldBufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

const seriesFoldBufKeep = 4 << 20

func putSeriesFoldBuf(buf *bytes.Buffer) {
	if buf.Cap() <= seriesFoldBufKeep {
		seriesFoldBufPool.Put(buf)
	}
}

// seriesFoldQueryPath is the endpoint a folded read is sent to: its stats pipes group by the bucket themselves.
const seriesFoldQueryPath = "/select/logsql/query"

// SeriesFoldQueryParams are the parameters of a folded read sent to seriesFoldQueryPath for its stats_query_range
// parameters: the bounds that endpoint reads (the exclusive end less a nanosecond, both aligned to the step's grid,
// alignedStatsBounds), the end exclusive again, and no step or offset (the query's bucket stats carry the grid).
func SeriesFoldQueryParams(params url.Values) (url.Values, error) {
	start, end, err := alignedStatsBounds(params.Get("start"), params.Get("end"), params.Get("step"), params.Get("offset"))
	if err != nil {
		return nil, err
	}
	out := make(url.Values, len(params))
	for k, v := range params {
		if k != "step" && k != "offset" {
			out[k] = v
		}
	}
	out.Set("start", time.Unix(0, start).UTC().Format(time.RFC3339Nano))
	out.Set("end", time.Unix(0, end+1).UTC().Format(time.RFC3339Nano))
	return out, nil
}

// alignedStatsBounds are the inclusive bounds, in nanoseconds, stats_query_range reads for these parameters:
// VictoriaLogs takes the end as exclusive (one nanosecond less), then moves the start down and the end up to the
// edges of the buckets of step shifted by offset that hold them, the end one nanosecond before the next edge.
func alignedStatsBounds(rawStart, rawEnd, rawStep, rawOffset string) (start, end int64, err error) {
	var ok bool
	if start, ok = parseVLTimeNanos(rawStart); !ok {
		return 0, 0, fmt.Errorf("folded read start %q", rawStart)
	}
	if end, ok = parseVLTimeNanos(rawEnd); !ok {
		return 0, 0, fmt.Errorf("folded read end %q", rawEnd)
	}
	step, err := time.ParseDuration(rawStep)
	if err != nil || step <= 0 {
		return 0, 0, fmt.Errorf("folded read step %q", rawStep)
	}
	var offset time.Duration
	if rawOffset != "" {
		if offset, err = time.ParseDuration(rawOffset); err != nil {
			return 0, 0, fmt.Errorf("folded read offset %q", rawOffset)
		}
	}
	s, off := int64(step), int64(offset)
	end--
	start += off
	if start >= 0 {
		start -= start % s
	} else {
		start -= s + start%s
	}
	start -= off
	end += off
	if end <= 0 {
		end -= end % s
	} else {
		end += s - end%s
	}
	end -= off
	return start, end - 1, nil
}

// parseVLTimeNanos reads a VictoriaLogs time parameter: RFC 3339 or Unix time.
func parseVLTimeNanos(raw string) (int64, bool) {
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UnixNano(), true
	}
	return parseFlexibleUnixNanos(raw)
}

// seriesFoldPoint is one bucket's value of one result of a folded answer: its time and its value as the answer
// writes it (a JSON string's content, escaped).
type seriesFoldPoint struct {
	ts    int64
	value []byte
}

// seriesFoldSeries is one result of a folded answer read back into stats_query_range's shape: its column, name and
// label values (the last stats pipe's grouping less the bucket, as the answer writes them), and its points.
type seriesFoldSeries struct {
	column int
	name   string
	labels [][]byte
	points []seriesFoldPoint
	sort   string
}

// seriesFoldField is one field of a folded answer's row: its name and its value as the row writes them.
type seriesFoldField struct{ key, value []byte }

// matrix reads a folded read's answer (rows of /select/logsql/query, one per bucket and group of the query's last
// stats pipe) back into the answer stats_query_range gives for a stats query of these groups: a result per column and
// group, its labels the grouping less the bucket and its points the buckets, ordered as VictoriaLogs orders them. A
// folded group (an empty series hash) answers its merged value and series count in the value and count columns; a
// group that keeps its hash answers the bucket stats' results (and a count of one when the fold adds the count
// column). A row that is not an object of strings with a bucket time fails the read. The values are copied from the
// answer as it writes them, never decoded.
func (s *seriesFoldRead) matrix(raw []byte) ([]byte, error) {
	if s == nil {
		return nil, &statsBucketsUnreadableError{"folded read without its grouping"}
	}
	labels := make([][]byte, 0, len(s.by)+1)
	for _, b := range s.by {
		if name, err := strconv.Unquote(b); err == nil {
			b = name
		}
		labels = append(labels, []byte(b))
	}
	sid := slices.IndexFunc(labels, func(l []byte) bool { return string(l) == dynamicSeriesHashLabel })
	if sid < 0 {
		return nil, &statsBucketsUnreadableError{"folded read without its series hash"}
	}
	// The unwrap check's counting form groups by its flag too: every row then names it.
	if bytes.Contains(raw, seriesFoldFlagKey) {
		labels = append(labels, []byte(unwrapBadField))
	}
	columns := make([][]byte, len(s.columns))
	for i, c := range s.columns {
		columns[i] = []byte(c)
	}
	valueColumn, countColumn, countIndex := slices.Index(s.columns, s.valueColumn), s.countColumn, s.countIndex()
	var (
		series  []*seriesFoldSeries
		byKey   = map[string]int{}
		times   = map[string]int64{}
		fields  []seriesFoldField
		values  = make([][]byte, len(labels))
		flat    []seriesFoldFlatPoint
		key     []byte
		rowKey  []byte
		scratch = make([]byte, 0, 64)
	)
	field := func(name []byte) []byte {
		for _, f := range fields {
			if bytes.Equal(f.key, name) {
				return f.value
			}
		}
		return nil
	}
	// add files a point under its column, name and the row's label values (rowKey, built once per row).
	add := func(column int, name string, ts int64, value []byte) {
		key = append(key[:0], byte(column>>24), byte(column>>16), byte(column>>8), byte(column))
		key = append(append(key, name...), rowKey...)
		i, ok := byKey[string(key)]
		if !ok {
			i = len(series)
			byKey[string(key)] = i
			series = append(series, &seriesFoldSeries{column: column, name: name, labels: slices.Clone(values)})
		}
		flat = append(flat, seriesFoldFlatPoint{series: i, point: seriesFoldPoint{ts: ts, value: value}})
	}
	unreadable := &statsBucketsUnreadableError{"folded read: a row that is not an object of strings with its bucket"}
	// A row gives a point per column (and the added count), a folded row two: the points fit one slice.
	flat = make([]seriesFoldFlatPoint, 0, (bytes.Count(raw, []byte{'\n'})+1)*(len(s.columns)+1))
	for len(raw) > 0 {
		line := raw
		if i := bytes.IndexByte(raw, '\n'); i >= 0 {
			line, raw = raw[:i], raw[i+1:]
		} else {
			raw = nil
		}
		if line = bytes.TrimSpace(line); len(line) == 0 {
			continue
		}
		if line[0] != '{' || !json.Valid(line) {
			return nil, unreadable
		}
		fields = fields[:0]
		allStrings := true
		if err := jsonparser.ObjectEach(line, func(k, v []byte, t jsonparser.ValueType, _ int) error {
			allStrings = allStrings && t == jsonparser.String
			fields = append(fields, seriesFoldField{k, v})
			return nil
		}); err != nil || !allStrings {
			return nil, unreadable
		}
		at := field(seriesFoldTimeKey)
		ts, ok := times[string(at)]
		if !ok {
			if ts, ok = parseVLTimeNanos(string(at)); !ok {
				return nil, unreadable
			}
			times[string(at)] = ts
		}
		rowKey = rowKey[:0]
		for i, l := range labels {
			values[i] = field(l)
			rowKey = appendSeriesFoldKeyValue(append(rowKey, 0), values[i], &scratch)
		}
		if len(values[sid]) == 0 {
			add(valueColumn, s.valueColumn, ts, field(seriesFoldValueKey))
			if countColumn != "" {
				add(countIndex, countColumn, ts, field(seriesFoldCountKey))
			}
			continue
		}
		for i, c := range columns {
			add(i, s.columns[i], ts, field(c))
		}
		if s.added {
			add(len(s.columns), lower.SeriesFoldCount, ts, seriesFoldOne)
		}
	}
	// Each result's points, in the order the rows gave them, in one slice.
	offsets := make([]int, len(series)+1)
	for _, p := range flat {
		offsets[p.series+1]++
	}
	for i := 1; i < len(offsets); i++ {
		offsets[i] += offsets[i-1]
	}
	points := make([]seriesFoldPoint, len(flat))
	next := slices.Clone(offsets[:len(series)])
	for _, p := range flat {
		points[next[p.series]] = p.point
		next[p.series]++
	}
	for i, ss := range series {
		ss.points = points[offsets[i]:offsets[i+1]:offsets[i+1]]
	}
	return writeSeriesFoldMatrix(series, labels)
}

// seriesFoldFlatPoint is a point of a folded answer filed under its result (an index of the reader's results).
type seriesFoldFlatPoint struct {
	series int
	point  seriesFoldPoint
}

// The keys of a folded answer's rows the reader takes, and a count of one.
var (
	seriesFoldTimeKey  = []byte("_time")
	seriesFoldValueKey = []byte(lower.SeriesFoldField)
	seriesFoldCountKey = []byte(lower.SeriesFoldSeriesCount)
	seriesFoldOne      = []byte("1")
)

// countIndex is the column of the count a folded group answers.
func (s *seriesFoldRead) countIndex() int {
	if i := slices.Index(s.columns, s.countColumn); i >= 0 {
		return i
	}
	return len(s.columns)
}

// appendSeriesFoldKeyValue appends a label value's text: the value as the answer wrote it, decoded when it holds an
// escape, so two spellings of one value are one group. scratch is a buffer the decoding may use.
func appendSeriesFoldKeyValue(dst, v []byte, scratch *[]byte) []byte {
	if bytes.IndexByte(v, '\\') >= 0 {
		if text, err := jsonparser.Unescape(v, (*scratch)[:cap(*scratch)]); err == nil {
			if cap(text) > cap(*scratch) {
				*scratch = text
			}
			return append(dst, text...)
		}
	}
	return append(dst, v...)
}

// appendSeriesFoldSortValue appends a label value as the answer wrote it (a JSON string's content) in the spelling
// appendJSONString gives its decoded text, so results sort by their values whatever escapes VictoriaLogs chose: a
// value holding an escape or a byte appendJSONString escapes is decoded and written again, any other is that spelling
// already.
func appendSeriesFoldSortValue(dst, v []byte) []byte {
	for _, c := range v {
		if c < 0x20 || c == '\\' || c == '"' || c == '<' || c == '>' || c == '&' {
			if text, err := jsonparser.ParseString(v); err == nil {
				return appendJSONString(dst, text)
			}
			break
		}
	}
	return append(append(append(dst, '"'), v...), '"')
}

// writeSeriesFoldMatrix writes the results of a folded answer as stats_query_range writes them: ordered by column,
// name and labels (the non-empty ones, as VictoriaLogs keys a result), each one's points by time, a time in seconds as
// float64(ns)/1e9 writes, the values as the answer wrote them.
func writeSeriesFoldMatrix(results []*seriesFoldSeries, labels [][]byte) ([]byte, error) {
	quoted := make([][]byte, len(labels))
	for i, l := range labels {
		quoted[i] = appendJSONString(nil, string(l))
	}
	size := 64
	for _, ss := range results {
		b := []byte{byte(ss.column >> 24), byte(ss.column >> 16), byte(ss.column >> 8), byte(ss.column)}
		b = append(b, ss.name...)
		b = append(b, '{')
		first := true
		for i, v := range ss.labels {
			if len(v) == 0 {
				continue
			}
			if !first {
				b = append(b, ',')
			}
			first = false
			b = appendSeriesFoldSortValue(append(append(b, quoted[i]...), ':'), v)
		}
		ss.sort = string(append(b, '}'))
		slices.SortStableFunc(ss.points, func(a, b seriesFoldPoint) int { return cmp.Compare(a.ts, b.ts) })
		size += 64 + len(ss.name) + len(ss.sort) + 32*len(ss.points)
		for _, pt := range ss.points {
			size += len(pt.value)
		}
	}
	slices.SortStableFunc(results, func(a, b *seriesFoldSeries) int { return strings.Compare(a.sort, b.sort) })
	if int64(size) > 2*maxStatsBucketResponseBytes {
		return nil, errBodyTooLarge
	}
	out := make([]byte, 0, size)
	out = append(out, `{"status":"success","data":{"resultType":"matrix","result":[`...)
	for i, ss := range results {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, `{"metric":{"__name__":`...)
		out = appendJSONString(out, ss.name)
		for j := range labels {
			out = append(append(append(append(append(out, ','), quoted[j]...), ':', '"'), ss.labels[j]...), '"')
		}
		out = append(out, `},"values":[`...)
		for j, pt := range ss.points {
			if j > 0 {
				out = append(out, ',')
			}
			out = append(out, '[')
			out = strconv.AppendFloat(out, float64(pt.ts)/1e9, 'f', -1, 64)
			out = append(append(append(out, ',', '"'), pt.value...), '"', ']')
		}
		out = append(out, "]}"...)
		if int64(len(out)) > maxStatsBucketResponseBytes {
			return nil, errBodyTooLarge
		}
	}
	return append(out, "]}}"...), nil
}

// seriesFoldFlagKey is the unwrap check's flag as a key of a folded read's rows.
var seriesFoldFlagKey = []byte(`"` + unwrapBadField + `":`)

// seriesFoldBody is a folded answer's body as the coalescer reads it: past limit bytes the read fails with
// errBodyTooLarge, so the shared call holds at most the stats body bound.
type seriesFoldBody struct {
	io.ReadCloser
	left int64
}

func (b *seriesFoldBody) Read(p []byte) (int, error) {
	if b.left < 0 {
		return 0, errBodyTooLarge
	}
	if int64(len(p)) > b.left+1 {
		p = p[:b.left+1]
	}
	n, err := b.ReadCloser.Read(p)
	if b.left -= int64(n); b.left < 0 {
		return n, errBodyTooLarge
	}
	return n, err
}
