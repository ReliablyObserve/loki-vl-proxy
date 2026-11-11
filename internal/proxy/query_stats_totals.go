package proxy

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Processed totals with Loki's meaning, at no VictoriaLogs cost.
//
// Loki's summary counts the lines (and line plus structured-metadata bytes)
// of the query's stream selector it read over the query's window, and, after
// the line and label filters, totalPostFilterLines. The proxy reads only what
// VictoriaLogs returns (aggregated rows for a metric query, at most the limit
// for a log query), so it takes these totals from answers it computes anyway:
//
//   - A count_over_time or bytes_over_time of a log expression without a
//     parser, optionally under sum (by/without), evaluated on tumbling windows
//     (range equal to step) or as an instant query, sums to the lines (bytes)
//     of that expression over (start-range, end]: Loki's window for it. With no
//     line or label filter that is totalLinesProcessed (bytes:
//     totalBytesProcessed, line bytes only); with filters it is
//     totalPostFilterLines.
//   - Such an answer is kept as a companion, keyed by the tenant scope and the
//     log expression, so the log query of the same panel (Explore and Logs
//     Drilldown send the logs query and its volume query together, the volume
//     query wrapping the logs query's expression) takes the totals of its own
//     window from it: the steps wholly inside the part of the window the
//     companion covers, when that part is at least 90% of the window (a
//     refresh moves the window past the last volume answer; that edge is left
//     out, never estimated).
//
// Only answers computed from VictoriaLogs (a cache miss) carry these totals; a
// cache hit reports no lines processed, as Loki's results cache does.

const (
	statsTotalsKeyPrefix = "qs-totals:v1:"
	statsTotalsTTL       = 5 * time.Minute
	statsTotalsMaxSteps  = 11000 // Loki's max points per series
)

// statsTotals are the Loki-meaning totals of one answer; a field whose has*
// is false falls back to what the proxy read (summary).
type statsTotals struct {
	lines, postFilter, bytes          int64
	hasLines, hasPostFilter, hasBytes bool
}

// totalsShape is a query statsTotals can be derived for.
type totalsShape struct {
	op       logqlpkg.RangeOp // count_over_time, bytes_over_time; "" for a log query
	expr     *logqlpkg.LogQuery
	rangeDur time.Duration
	filtered bool // a line or label filter: counts are post-filter
}

// totalsShapeOf reads a query as a totals shape. It is cheap for the queries
// it cannot be (a text check before the parse).
func totalsShapeOf(ctx context.Context, query string) (totalsShape, bool) {
	q := strings.TrimSpace(query)
	if !strings.HasPrefix(q, "{") && !strings.Contains(q, "count_over_time") && !strings.Contains(q, "bytes_over_time") &&
		!strings.Contains(q, "rate") {
		return totalsShape{}, false
	}
	expr, ok := sharedLogQLExpr(q)
	if !ok {
		return totalsShape{}, false
	}
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		if agg.Op != logqlpkg.VectorSum || agg.HasParam {
			return totalsShape{}, false
		}
		expr = agg.Inner
	}
	var shape totalsShape
	switch e := expr.(type) {
	case *logqlpkg.LogQuery:
		shape.expr = e
	case *logqlpkg.RangeAggregation:
		lq, ok := e.Inner.(*logqlpkg.LogQuery)
		if !ok || e.Grouping != nil || e.Offset != "" || e.RangeDuration <= 0 ||
			(e.Op != logqlpkg.RangeCountOverTime && e.Op != logqlpkg.RangeBytesOverTime &&
				e.Op != logqlpkg.RangeRate && e.Op != logqlpkg.RangeBytesRate) {
			return totalsShape{}, false
		}
		shape.op, shape.expr, shape.rangeDur = e.Op, lq, e.RangeDuration
	default:
		return totalsShape{}, false
	}
	stripped, filtered, ok := totalsExpr(shape.expr)
	if !ok {
		return totalsShape{}, false
	}
	shape.expr, shape.filtered = stripped, filtered
	return shape, true
}

// totalsExpr drops `| drop __error__[, __error_details__]` (a no-op without a
// parser) and reports whether the expression filters lines. Any other stage
// (parsers, formatting, unwrap) makes the totals unknown.
func totalsExpr(lq *logqlpkg.LogQuery) (*logqlpkg.LogQuery, bool, bool) {
	if lq == nil || lq.Selector == nil {
		return nil, false, false
	}
	out := &logqlpkg.LogQuery{Selector: lq.Selector}
	filtered := false
	for _, st := range lq.Pipeline {
		switch s := st.(type) {
		case *logqlpkg.LineFilterStage:
			filtered = true
			out.Pipeline = append(out.Pipeline, s)
		case *logqlpkg.LabelFilterStage:
			filtered = true
			out.Pipeline = append(out.Pipeline, s)
		case *logqlpkg.DropStage:
			if !isErrorDropStage(s) {
				return nil, false, false
			}
		default:
			return nil, false, false
		}
	}
	return out, filtered, true
}

// statsTotalsScope is the tenant scope part of the companion keys of one
// request: the org, its routing scope and the request's fingerprint.
type statsTotalsScope [3]string

func (p *Proxy) statsTotalsScopeOf(r *http.Request) statsTotalsScope {
	orgID := r.Header.Get("X-Scope-OrgID")
	return statsTotalsScope{orgID, p.scopeFingerprint(r.Context(), orgID), p.fingerprintFromCtx(r.Context(), r)}
}

// key is the companion key of a log expression in the scope.
func (sc statsTotalsScope) key(kind, expr string) string {
	h := sha256.New()
	for _, part := range [...]string{sc[0], sc[1], sc[2], kind, expr} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	var sum [sha256.Size]byte
	key := make([]byte, len(statsTotalsKeyPrefix)+hex.EncodedLen(len(sum)))
	copy(key, statsTotalsKeyPrefix)
	hex.Encode(key[len(statsTotalsKeyPrefix):], h.Sum(sum[:0]))
	return string(key)
}

// statsTotalsKey is the companion key of a log expression in r's tenant scope.
func (p *Proxy) statsTotalsKey(r *http.Request, kind string, expr string) string {
	return p.statsTotalsScopeOf(r).key(kind, expr)
}

// statsTotalsSteps is a companion: the per-step totals of one answer, over
// the window [start, end] it was asked for (steps cover (t-step, t]).
type statsTotalsSteps struct {
	step, start, end int64 // ns
	ts               []int64
	vals             []float64
}

func (s statsTotalsSteps) encode() []byte {
	out := make([]byte, 0, 32+16*len(s.ts))
	out = binary.AppendVarint(out, s.step)
	out = binary.AppendVarint(out, s.start)
	out = binary.AppendVarint(out, s.end)
	out = binary.AppendUvarint(out, uint64(len(s.ts)))
	for i := range s.ts {
		out = binary.AppendVarint(out, s.ts[i])
		out = binary.LittleEndian.AppendUint64(out, math.Float64bits(s.vals[i]))
	}
	return out
}

func decodeStatsTotalsSteps(raw []byte) (statsTotalsSteps, bool) {
	var s statsTotalsSteps
	for _, dst := range []*int64{&s.step, &s.start, &s.end} {
		v, n := binary.Varint(raw)
		if n <= 0 {
			return statsTotalsSteps{}, false
		}
		*dst, raw = v, raw[n:]
	}
	count, n := binary.Uvarint(raw)
	if n <= 0 || count > statsTotalsMaxSteps || s.step <= 0 {
		return statsTotalsSteps{}, false
	}
	raw = raw[n:]
	s.ts, s.vals = make([]int64, 0, count), make([]float64, 0, count)
	for i := uint64(0); i < count; i++ {
		t, n := binary.Varint(raw)
		if n <= 0 || len(raw[n:]) < 8 {
			return statsTotalsSteps{}, false
		}
		s.ts = append(s.ts, t)
		s.vals = append(s.vals, math.Float64frombits(binary.LittleEndian.Uint64(raw[n:])))
		raw = raw[n+8:]
	}
	return s, true
}

// statsTotalsMinCover is the part of a window a companion must cover for
// its lines to stand for the window's: a refresh moves the window past the
// last volume answer by the refresh interval, which is left out.
const statsTotalsMinCover = 0.9

// within sums the companion's steps wholly inside (start, end]: the steps of
// its grid (origin s.start) between the first boundary at or after
// max(start, s.start) and the last at or before min(end, s.end). False when
// those whole steps span under statsTotalsMinCover of the window.
func (s statsTotalsSteps) within(start, end int64) (float64, bool) {
	if s.step <= 0 || end <= start {
		return 0, false
	}
	from, to := start, end
	if s.start > from {
		from = s.start
	}
	if s.end < to {
		to = s.end
	}
	if to <= from {
		return 0, false
	}
	first := s.start + (from-s.start+s.step-1)/s.step*s.step
	last := s.start + (to-s.start)/s.step*s.step
	if last-first < s.step || float64(last-first) < statsTotalsMinCover*float64(end-start) {
		return 0, false
	}
	var sum float64
	for i, t := range s.ts {
		if t-s.step >= first && t <= last {
			sum += s.vals[i]
		}
	}
	return sum, true
}

var statsTotalsParsers fj.ParserPool

// answerStepSums sums a matrix or vector answer per evaluation time: the
// distinct times in ascending order and their sums.
func answerStepSums(body []byte) (ts []int64, vals []float64, ok bool) {
	parser := statsTotalsParsers.Get()
	defer statsTotalsParsers.Put(parser)
	v, err := parser.ParseBytes(body)
	if err != nil {
		return nil, nil, false
	}
	data := v.Get("data")
	if data == nil {
		return nil, nil, false
	}
	vector := false
	switch string(data.GetStringBytes("resultType")) {
	case "vector":
		vector = true
	case "matrix":
	default:
		return nil, nil, false
	}
	buf := stepSampleBufs.Get().(*[]stepSample)
	samples := (*buf)[:0]
	defer func() { *buf = samples[:0]; stepSampleBufs.Put(buf) }()
	add := func(sample *fj.Value) bool {
		pair := sample.GetArray()
		if len(pair) != 2 {
			return false
		}
		t, err := pair[0].Float64()
		if err != nil {
			return false
		}
		val, err := strconv.ParseFloat(string(pair[1].GetStringBytes()), 64)
		if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
			return false
		}
		samples = append(samples, stepSample{int64(math.Round(t*1e3)) * int64(time.Millisecond), val})
		return true
	}
	for _, series := range data.GetArray("result") {
		if vector {
			if s := series.Get("value"); s == nil || !add(s) {
				return nil, nil, false
			}
			continue
		}
		for _, s := range series.GetArray("values") {
			if !add(s) {
				return nil, nil, false
			}
		}
	}
	slices.SortFunc(samples, func(a, b stepSample) int { return cmp.Compare(a.ts, b.ts) })
	n := 0
	for i := range samples {
		if i == 0 || samples[i].ts != samples[i-1].ts {
			n++
		}
	}
	ts, vals = make([]int64, 0, n), make([]float64, 0, n)
	for i, sm := range samples {
		if i > 0 && sm.ts == samples[i-1].ts {
			vals[len(vals)-1] += sm.val
			continue
		}
		ts, vals = append(ts, sm.ts), append(vals, sm.val)
	}
	return ts, vals, true
}

// stepSample is one sample of an answer; stepSampleBufs keeps their buffers.
type stepSample struct {
	ts  int64
	val float64
}

var stepSampleBufs = sync.Pool{New: func() any { b := make([]stepSample, 0, 256); return &b }}

// requestWindow is a query request's evaluation window and step (ns); an
// instant query's window is its range up to its time.
func requestWindow(r *http.Request, endpoint string, rangeDur time.Duration) (start, end, step int64, ok bool) {
	if endpoint == "query" {
		t, ok := parseLokiTimeToUnixNano(r.FormValue("time"))
		if !ok {
			t = time.Now().UnixNano()
		}
		return t, t, int64(rangeDur), rangeDur > 0
	}
	start, okS := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, okE := parseLokiTimeToUnixNano(r.FormValue("end"))
	step, okStep := parseStepToNanos(r.FormValue("step"))
	return start, end, step, okS && okE && end >= start && (okStep || rangeDur == 0)
}

// deriveQueryStatsTotals sets the request's Loki-meaning totals after its
// answer was computed (body: the answer as the response cache captured it,
// nil when it was not captured). Only an answer computed from VictoriaLogs
// gets them.
func (p *Proxy) deriveQueryStatsTotals(r *http.Request, endpoint string, body []byte) {
	rt := getRequestTelemetry(r.Context())
	if rt == nil || p.cache == nil {
		return
	}
	rt.mu.Lock()
	sw, vlCalls, reused := rt.statsWriter, rt.flow.vlCalls, rt.flow.reusedCachedAnswers()
	rt.mu.Unlock()
	if sw == nil || vlCalls == 0 {
		return
	}
	shape, ok := totalsShapeOf(r.Context(), r.FormValue("query"))
	if !ok {
		return
	}
	var t statsTotals
	if shape.op == "" {
		if !sw.lokiMode || endpoint != "query_range" {
			return // victorialogs mode reports the rows read, not the panel's volume answer
		}
		p.storeMeanLineBytes(r, shape, sw.entries.lineBytes, sw.entries.n)
		run := sw.lokiLogRun()
		t, ok = p.logQueryTotals(r, shape, run.from, run.to)
	} else {
		// The answer's steps are right whichever cache served them, so they
		// are kept as the companion; but an answer partly served from cached
		// steps or extents was not all processed by this request (Loki counts
		// only the part it computed), so it gives this request no totals.
		t, ok = p.metricQueryTotals(r, endpoint, shape, body, sw.lokiMode)
		ok = ok && !reused
		if ok && sw.lokiMode && (t.hasLines || t.hasPostFilter) && !t.hasBytes {
			// The lines read are at least the post-filter lines counted.
			t.bytes, t.hasBytes = p.metricQueryBytes(r, sw, shape, max(t.lines, t.postFilter))
		}
	}
	if !ok {
		return
	}
	rt.mu.Lock()
	rt.statsTotals = t
	rt.mu.Unlock()
}

// reusedCachedAnswers reports whether the request found any cached answer
// or part of one (Loki's result, statsResult, volumeResult or
// instantMetricResult sections).
func (f *flowCounters) reusedCachedAnswers() bool {
	for _, c := range f.lokiCache {
		if c.found > 0 {
			return true
		}
	}
	return false
}

// metricQueryTotals reads a metric answer's lines (count_over_time, rate)
// or bytes (bytes_over_time, bytes_rate) over Loki's window: the sum of its
// samples (a rate times its range in seconds). On sliding windows (range
// other than the step) a line is counted in about range/step samples; Loki's
// accounting (lokiMode) then estimates the window's total as the sum scaled
// by step/range, the measured accounting gives none.
func (p *Proxy) metricQueryTotals(r *http.Request, endpoint string, shape totalsShape, body []byte, lokiMode bool) (statsTotals, bool) {
	if len(body) == 0 {
		return statsTotals{}, false
	}
	start, end, step, ok := requestWindow(r, endpoint, shape.rangeDur)
	sliding := endpoint == "query_range" && step != int64(shape.rangeDur)
	if !ok || (sliding && !lokiMode) || step <= 0 {
		return statsTotals{}, false
	}
	ts, vals, ok := answerStepSums(body)
	if !ok || len(ts) > statsTotalsMaxSteps {
		return statsTotals{}, false
	}
	scale := 1.0
	if shape.op == logqlpkg.RangeRate || shape.op == logqlpkg.RangeBytesRate {
		scale = shape.rangeDur.Seconds()
	}
	if scale != 1 {
		for i := range vals {
			vals[i] *= scale
		}
	}
	steps := statsTotalsSteps{step: step, start: start - step, end: end, ts: ts, vals: vals}
	var total float64
	for _, v := range vals {
		total += v
	}
	if sliding {
		total *= float64(step) / float64(shape.rangeDur)
	}
	kind := "lines"
	if shape.op == logqlpkg.RangeBytesOverTime || shape.op == logqlpkg.RangeBytesRate {
		kind = "bytes"
	}
	if endpoint == "query_range" && !sliding {
		// An instant answer is one step of its range: too coarse to stand
		// for another window, so it is not kept.
		p.storeStatsTotals(r, kind, shape.expr.String(), steps)
	}
	n := int64(math.Round(total))
	var t statsTotals
	switch {
	case kind == "bytes" && !shape.filtered:
		t.bytes, t.hasBytes = n, true
	case kind == "lines" && !shape.filtered:
		t.lines, t.hasLines, t.postFilter, t.hasPostFilter = n, true, n, true
	case kind == "lines":
		t.postFilter, t.hasPostFilter = n, true
		if all, ok := p.statsTotalsWithin(r, p.statsTotalsKey(r, "lines", (&logqlpkg.LogQuery{Selector: shape.expr.Selector}).String()), start-step, end); ok {
			t.lines, t.hasLines = int64(math.Round(all)), true
		}
	default:
		return statsTotals{}, false
	}
	return t, true
}

// logQueryTotals reads a log query's lines over [from, to] (the part of its
// window Loki reads) from the count answers of the panel the proxy holds.
// Its bytes are those lines times the mean size of the lines it returned:
// within 1-2% of Loki's line bytes on the test stack, where a volume_range
// answer's whole steps would leave out up to a tenth of the window.
func (p *Proxy) logQueryTotals(r *http.Request, shape totalsShape, from, to int64) (statsTotals, bool) {
	var t statsTotals
	scope := p.statsTotalsScopeOf(r)
	selector := (&logqlpkg.LogQuery{Selector: shape.expr.Selector}).String()
	if lines, ok := p.statsTotalsWithin(r, scope.key("lines", shape.expr.String()), from, to); ok {
		t.postFilter, t.hasPostFilter = int64(math.Round(lines)), true
		if !shape.filtered {
			t.lines, t.hasLines = t.postFilter, true
		}
	}
	if shape.filtered {
		if lines, ok := p.statsTotalsWithin(r, scope.key("lines", selector), from, to); ok {
			t.lines, t.hasLines = int64(math.Round(lines)), true
		}
	}
	return t, t.hasLines || t.hasPostFilter
}

// The mean line size a log answer measured, kept for the metric queries of
// the same selector (and of the tenant, when the selector has none): Loki
// counts the bytes of the lines a metric query reads, which VictoriaLogs'
// aggregated answer does not tell.
const statsMeanLineKind = "meanline"

// storeMeanLineBytes keeps the mean line size of a log answer: its line
// bytes and lines, under its selector and its tenant.
func (p *Proxy) storeMeanLineBytes(r *http.Request, shape totalsShape, lineBytes, lines int64) {
	if lines <= 0 || lineBytes <= 0 {
		return
	}
	value := binary.AppendUvarint(binary.AppendUvarint(make([]byte, 0, 2*binary.MaxVarintLen64), uint64(lineBytes)), uint64(lines))
	scope := p.statsTotalsScopeOf(r)
	selector := (&logqlpkg.LogQuery{Selector: shape.expr.Selector}).String()
	p.cache.SetReplicatedWithTTL(scope.key(statsMeanLineKind, selector), value, statsMeanLineTTL)
	p.cache.SetReplicatedWithTTL(scope.key(statsMeanLineKind, ""), value, statsMeanLineTTL)
}

// statsMeanLineTTL keeps a mean line size an hour: it changes slowly.
const statsMeanLineTTL = time.Hour

// meanLineBytes is the mean line size kept for a selector, or for its
// tenant.
func (p *Proxy) meanLineBytes(r *http.Request, shape totalsShape) (float64, bool) {
	scope := p.statsTotalsScopeOf(r)
	for _, expr := range [...]string{(&logqlpkg.LogQuery{Selector: shape.expr.Selector}).String(), ""} {
		raw, _, ok := p.cacheGetWithTTL(r.Context(), p.cache, scope.key(statsMeanLineKind, expr))
		if !ok {
			continue
		}
		bytesN, n := binary.Uvarint(raw)
		lines, m := binary.Uvarint(raw[max(n, 0):])
		if n > 0 && m > 0 && lines > 0 {
			return float64(bytesN) / float64(lines), true
		}
	}
	return 0, false
}

// metricQueryBytes is a metric query's bytes processed in Loki's
// accounting: exact from VictoriaLogs (-query-stats-exact-metric-bytes, the
// selector's line bytes over the query's window), else its lines times the
// selector's mean line size.
func (p *Proxy) metricQueryBytes(r *http.Request, sw *queryStatsWriter, shape totalsShape, lines int64) (int64, bool) {
	if sw.exactBytes != nil {
		select {
		case b := <-sw.exactBytes:
			if b >= 0 {
				return b, true
			}
		case <-r.Context().Done():
			return 0, false
		}
	}
	mean, ok := p.meanLineBytes(r, shape)
	if !ok {
		return 0, false
	}
	return int64(math.Round(mean * float64(lines))), true
}

func (p *Proxy) statsTotalsWithin(r *http.Request, key string, start, end int64) (float64, bool) {
	// The memory tier only: a peer read would wait on the network before the
	// answer's stats are written. Companions reach their replicas when stored.
	raw, _, ok := p.cacheGetWithTTL(r.Context(), p.cache, key)
	if !ok {
		return 0, false
	}
	steps, ok := decodeStatsTotalsSteps(raw)
	if !ok {
		return 0, false
	}
	return steps.within(start, end)
}

// storeStatsTotals keeps a companion in memory and on the key's replicas
// (an asynchronous push of a small entry), so the logs query of the panel
// finds it on whichever replica it reaches first.
func (p *Proxy) storeStatsTotals(r *http.Request, kind, expr string, steps statsTotalsSteps) {
	p.cache.SetReplicatedWithTTL(p.statsTotalsKey(r, kind, expr), steps.encode(), statsTotalsTTL)
}

// deriveQueryStatsTotalsFromCapture runs deriveQueryStatsTotals for a query
// or query_range answer the response cache captured.
func (p *Proxy) deriveQueryStatsTotalsFromCapture(r *http.Request, endpoint string, capture *compatCacheCaptureWriter) {
	if endpoint != "query" && endpoint != "query_range" {
		return
	}
	var answer []byte
	if capture.code == http.StatusOK && !capture.flushed {
		answer = capture.CapturedBody()
	}
	p.deriveQueryStatsTotals(r, endpoint, answer)
}
