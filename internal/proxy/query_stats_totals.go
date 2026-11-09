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
	if !strings.HasPrefix(q, "{") && !strings.Contains(q, "count_over_time") && !strings.Contains(q, "bytes_over_time") {
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
			(e.Op != logqlpkg.RangeCountOverTime && e.Op != logqlpkg.RangeBytesOverTime) {
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
	active, vlCalls := rt.statsWriter != nil, rt.flow.vlCalls
	rt.mu.Unlock()
	if !active || vlCalls == 0 {
		return
	}
	shape, ok := totalsShapeOf(r.Context(), r.FormValue("query"))
	if !ok {
		return
	}
	var t statsTotals
	if shape.op == "" {
		t, ok = p.logQueryTotals(r, endpoint, shape)
	} else {
		t, ok = p.metricQueryTotals(r, endpoint, shape, body)
	}
	if !ok {
		return
	}
	rt.mu.Lock()
	rt.statsTotals = t
	rt.mu.Unlock()
}

func (p *Proxy) metricQueryTotals(r *http.Request, endpoint string, shape totalsShape, body []byte) (statsTotals, bool) {
	if len(body) == 0 {
		return statsTotals{}, false
	}
	start, end, step, ok := requestWindow(r, endpoint, shape.rangeDur)
	if !ok || (endpoint == "query_range" && step != int64(shape.rangeDur)) {
		return statsTotals{}, false // sliding windows count a line in several steps
	}
	ts, vals, ok := answerStepSums(body)
	if !ok || len(ts) > statsTotalsMaxSteps {
		return statsTotals{}, false
	}
	steps := statsTotalsSteps{step: step, start: start - step, end: end, ts: ts, vals: vals}
	var total float64
	for _, v := range vals {
		total += v
	}
	kind := "lines"
	if shape.op == logqlpkg.RangeBytesOverTime {
		kind = "bytes"
	}
	if endpoint == "query_range" {
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

func (p *Proxy) logQueryTotals(r *http.Request, endpoint string, shape totalsShape) (statsTotals, bool) {
	if endpoint != "query_range" {
		return statsTotals{}, false
	}
	start, end, _, ok := requestWindow(r, endpoint, 0)
	if !ok {
		return statsTotals{}, false
	}
	var t statsTotals
	scope := p.statsTotalsScopeOf(r)
	selector := (&logqlpkg.LogQuery{Selector: shape.expr.Selector}).String()
	if lines, ok := p.statsTotalsWithin(r, scope.key("lines", shape.expr.String()), start, end); ok {
		t.postFilter, t.hasPostFilter = int64(math.Round(lines)), true
		if !shape.filtered {
			t.lines, t.hasLines = t.postFilter, true
		}
	}
	if shape.filtered {
		if lines, ok := p.statsTotalsWithin(r, scope.key("lines", selector), start, end); ok {
			t.lines, t.hasLines = int64(math.Round(lines)), true
		}
	}
	if bytes, ok := p.statsTotalsWithin(r, scope.key("bytes", selector), start, end); ok {
		t.bytes, t.hasBytes = int64(math.Round(bytes)), true
	}
	return t, t.hasLines || t.hasPostFilter || t.hasBytes
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

// recordVolumeStatsTotals keeps a volume_range answer computed from
// VictoriaLogs as the bytes companion of its selector (Logs Drilldown asks
// for it with the logs query of the same selector). Only an answer that holds
// every line of the selector is kept: grouped by nothing but labels every
// stream has (the selector's own, service_name, detected_level), and not cut
// by the limit. A bucket stamped at its end minus 1ms covers
// (stamp+1ms-step, stamp+1ms].
func (p *Proxy) recordVolumeStatsTotals(r *http.Request, req volumeRequest, result map[string]interface{}) {
	if p.cache == nil || req.stepNs <= 0 || req.byLabels {
		return
	}
	for _, t := range req.targets {
		if t != "service_name" && t != "detected_level" {
			return
		}
	}
	sums, ok := volumeStepSums(result, req.limit)
	if !ok {
		return
	}
	expr, ok := sharedLogQLExpr(req.query)
	if !ok {
		return
	}
	lq, ok := expr.(*logqlpkg.LogQuery)
	if !ok || lq.Selector == nil || len(lq.Pipeline) != 0 || (len(req.targets) == 0 && !selectorRequiresEveryLabel(lq.Selector)) {
		return
	}
	steps := statsTotalsSteps{step: req.stepNs, start: req.startNs, end: req.endNs + int64(time.Millisecond)}
	steps.ts = make([]int64, 0, len(sums))
	for ts := range sums {
		steps.ts = append(steps.ts, ts)
	}
	slices.Sort(steps.ts)
	steps.vals = make([]float64, len(steps.ts))
	for i, ts := range steps.ts {
		steps.vals[i] = sums[ts]
	}
	p.storeStatsTotals(r, "bytes", lq.String(), steps)
}

// selectorRequiresEveryLabel: grouped by the selector's labels, a stream
// without one of them is left out of a volume, so only matchers every
// matching stream carries a value for keep every line.
func selectorRequiresEveryLabel(sel *logqlpkg.StreamSelector) bool {
	for _, m := range sel.Matchers {
		eq := m.Op == logqlpkg.MatchEq && m.Value != ""
		anyValue := m.Op == logqlpkg.MatchRe && m.Value == ".+"
		if !eq && !anyValue {
			return false
		}
	}
	return true
}

// volumeStepSums sums a volume_range result's samples per bucket end
// (stamp + 1ms), across series; false for a vector, an answer cut by its
// limit, or any sample it cannot read.
func volumeStepSums(result map[string]interface{}, limit int) (map[int64]float64, bool) {
	data, _ := result["data"].(map[string]interface{})
	var series []map[string]interface{}
	switch v := data["result"].(type) {
	case []map[string]interface{}:
		series = v
	case []interface{}:
		for _, item := range v {
			m, ok := item.(map[string]interface{})
			if !ok {
				return nil, false
			}
			series = append(series, m)
		}
	default:
		return nil, false
	}
	if limit > 0 && len(series) >= limit {
		return nil, false
	}
	sums := map[int64]float64{}
	for _, m := range series {
		values, ok := m["values"].([][]interface{})
		if !ok {
			return nil, false // a vector (one step) is too coarse
		}
		for _, pair := range values {
			if len(pair) != 2 {
				return nil, false
			}
			sec, okT := pair[0].(float64)
			str, okV := pair[1].(string)
			v, err := strconv.ParseFloat(str, 64)
			if !okT || !okV || err != nil {
				return nil, false
			}
			sums[int64(math.Round(sec*1e3))*int64(time.Millisecond)+int64(time.Millisecond)] += v
		}
	}
	return sums, len(sums) > 0 && len(sums) <= statsTotalsMaxSteps
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
