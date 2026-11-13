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
	"sync/atomic"
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
	bufp := statsTotalsKeyBufs.Get().(*[]byte)
	buf := (*bufp)[:0]
	for _, part := range [...]string{sc[0], sc[1], sc[2], kind, expr} {
		buf = append(append(buf, part...), 0)
	}
	sum := sha256.Sum256(buf)
	*bufp = buf[:0]
	statsTotalsKeyBufs.Put(bufp)
	var key [len(statsTotalsKeyPrefix) + 2*sha256.Size]byte
	copy(key[:], statsTotalsKeyPrefix)
	hex.Encode(key[len(statsTotalsKeyPrefix):], sum[:])
	return string(key[:])
}

// statsTotalsKeyBufs keeps the buffers the companion keys are hashed from.
var statsTotalsKeyBufs = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}

// totalsKeys is what the companion keys of one request are made of, each
// part read on first use and once: its tenant scope, its log expression and
// that expression's stream selector (an instant answer, which keeps no
// companion, reads none of them).
type totalsKeys struct {
	p              *Proxy
	r              *http.Request
	shape          totalsShape
	scopeRead      bool
	scopeV         statsTotalsScope
	expr, selector string
}

func (k *totalsKeys) scope() statsTotalsScope {
	if !k.scopeRead {
		k.scopeV, k.scopeRead = k.p.statsTotalsScopeOf(k.r), true
	}
	return k.scopeV
}

// exprText is the log expression's text, printed once.
func (k *totalsKeys) exprText() string {
	if k.expr == "" {
		k.expr = k.shape.expr.String()
	}
	return k.expr
}

func (k *totalsKeys) exprKey(kind string) string {
	return k.scope().key(kind, k.exprText())
}

func (k *totalsKeys) selectorKey(kind string) string {
	if k.selector == "" {
		k.selector = (&logqlpkg.LogQuery{Selector: k.shape.expr.Selector}).String()
	}
	return k.scope().key(kind, k.selector)
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
	start, end, okWindow := requestRangeWindowNs(r)
	step, okStep := parseStepToNanos(r.FormValue("step"))
	return start, end, step, okWindow && end >= start && (okStep || rangeDur == 0)
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
	if shape.op == "" && (!sw.lokiMode || endpoint != "query_range") {
		return // victorialogs mode reports the rows read, not the panel's volume answer
	}
	keys := &totalsKeys{p: p, r: r, shape: shape}
	if shape.op == "" {
		p.storeMeanLine(r, keys, sw)
		run := sw.lokiLogRun()
		t, ok = p.logQueryTotals(r, keys, shape, run.from, run.to)
		if !t.hasLines && p.exactLogLinesWanted(r, sw, shape) {
			if n, counted := p.countSelectorLines(r.Context(), shape.expr.Selector, run.from, run.to); counted {
				t.lines, t.hasLines, ok = n, true, true
				if !shape.filtered {
					t.postFilter, t.hasPostFilter = n, true
				}
			}
		}
	} else {
		// The answer's steps are right whichever cache served them, so they
		// are kept as the companion; but an answer partly served from cached
		// steps or extents was not all processed by this request (Loki counts
		// only the part it computed), so it gives this request no totals.
		t, ok = p.metricQueryTotals(r, keys, endpoint, shape, body, sw.lokiMode)
		ok = ok && !reused
		if ok && sw.lokiMode && (t.hasLines || t.hasPostFilter) && !t.hasBytes {
			// The lines read are at least the post-filter lines counted.
			t.bytes, t.hasBytes = p.metricQueryBytes(r, keys, sw, max(t.lines, t.postFilter))
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
func (p *Proxy) metricQueryTotals(r *http.Request, keys *totalsKeys, endpoint string, shape totalsShape, body []byte, lokiMode bool) (statsTotals, bool) {
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
		// Kept in memory and pushed to the key's replicas (an asynchronous
		// push of a small entry), so the logs query of the panel finds it on
		// whichever replica it reaches first.
		p.cache.SetReplicatedWithTTL(keys.exprKey(kind), steps.encode(), statsTotalsTTL)
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
		if all, ok := p.statsTotalsWithin(r, keys.selectorKey("lines"), start-step, end); ok {
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
func (p *Proxy) logQueryTotals(r *http.Request, keys *totalsKeys, shape totalsShape, from, to int64) (statsTotals, bool) {
	var t statsTotals
	if lines, ok := p.statsTotalsWithin(r, keys.exprKey("lines"), from, to); ok {
		t.postFilter, t.hasPostFilter = int64(math.Round(lines)), true
		if !shape.filtered {
			t.lines, t.hasLines = t.postFilter, true
		}
	}
	if shape.filtered {
		if lines, ok := p.statsTotalsWithin(r, keys.selectorKey("lines"), from, to); ok {
			t.lines, t.hasLines = int64(math.Round(lines)), true
		}
	}
	return t, t.hasLines || t.hasPostFilter
}

// What a log answer measured of the size of lines, kept for the metric
// queries that follow it: Loki counts the bytes of the lines a metric query
// reads, which VictoriaLogs' aggregated answer does not tell. Each replica
// keeps what the log answers it computed showed (a statistic that changes
// slowly, so no peer copy), in a bounded table of hashes of the tenant scope
// and:
//
//   - the log expression, with the window its lines cover: the lines and
//     bytes of that expression over that window, exact (a log answer under its
//     limit holds every line of its window; one that filled it, every line from
//     its oldest entry on, backward). A metric query of the same expression
//     takes the part of its own window the record covers from it exactly, and
//     prices the rest of its lines at the record's mean line size.
//   - the selector, and the tenant when the selector has none: the mean line
//     size of the last log answer, the estimate for a metric query no record
//     covers (its window or expression was not logged). That answer may be of
//     another window, or of filtered lines, so this one can be off wherever the
//     lines of a selector differ in size from one window or filter to another.
//
// Reading and writing the table allocates nothing.
type meanLineTable struct {
	mu      sync.Mutex
	m       map[[sha256.Size]byte]meanLine
	windows atomic.Bool // a window record was ever stored: a metric query then looks for its own
}

type meanLine struct {
	bytes, lines int64
	from, to     int64 // the window the lines cover (ns); zero for a selector or tenant mean
	expires      int64 // unix ns
}

const (
	meanLineTTL       = time.Hour
	meanLineTableSize = 4096
)

// meanLineKeys are the table keys of a log answer: the selector's, the
// tenant's and, for a record with a window, the expression's.
type meanLineKeys struct {
	selector, tenant, expr [sha256.Size]byte
	hasExpr                bool
}

// store keeps a log answer's lines and their bytes: the mean under the
// selector and tenant keys, the window [from, to] they cover under the
// expression's (a zero window keeps none).
func (t *meanLineTable) store(keys meanLineKeys, lineBytes, lines, from, to int64) {
	if lines <= 0 || lineBytes <= 0 {
		return
	}
	exp := time.Now().Add(meanLineTTL).UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = make(map[[sha256.Size]byte]meanLine, 64)
	}
	t.put(keys.selector, meanLine{bytes: lineBytes, lines: lines, expires: exp})
	t.put(keys.tenant, meanLine{bytes: lineBytes, lines: lines, expires: exp})
	if keys.hasExpr && to > from {
		t.put(keys.expr, meanLine{bytes: lineBytes, lines: lines, from: from, to: to, expires: exp})
		t.windows.Store(true)
	}
}

func (t *meanLineTable) put(k [sha256.Size]byte, v meanLine) {
	if _, ok := t.m[k]; !ok && len(t.m) >= meanLineTableSize {
		for old := range t.m { // a full table drops one entry
			delete(t.m, old)
			break
		}
	}
	t.m[k] = v
}

// mean is the mean line size kept for the selector, else its tenant.
func (t *meanLineTable) mean(keys meanLineKeys) (float64, bool) {
	now := time.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range [...][sha256.Size]byte{keys.selector, keys.tenant} {
		if v, ok := t.m[k]; ok && v.expires > now {
			return float64(v.bytes) / float64(v.lines), true
		}
	}
	return 0, false
}

// within is the record of the expression over the window (from, to]: the
// lines and bytes of the part the record covers (all of them when it lies
// inside the window, else the share of its time inside) and its mean line
// size. False when it has none or does not overlap the window.
func (t *meanLineTable) within(k [sha256.Size]byte, from, to int64) (bytes, lines, mean float64, ok bool) {
	now := time.Now().UnixNano()
	t.mu.Lock()
	v, found := t.m[k]
	t.mu.Unlock()
	if !found || v.expires <= now || v.to <= v.from {
		return 0, 0, 0, false
	}
	overlap := min64(to, v.to) - max(from, v.from)
	if overlap <= 0 {
		return 0, 0, 0, false
	}
	share := float64(overlap) / float64(v.to-v.from)
	return float64(v.bytes) * share, float64(v.lines) * share, float64(v.bytes) / float64(v.lines), true
}

// meanLineKey is the table keys of a log expression (its text, when it is
// to be kept with a window), its selector and its tenant: hashes of the
// tenant org, routing namespace and auth fingerprint, and the selector's
// matchers, built in a pooled buffer.
func (p *Proxy) meanLineKey(r *http.Request, sel *logqlpkg.StreamSelector, expr string) (keys meanLineKeys) {
	routing := p.routingForContext(r.Context())
	namespace := routing.namespace
	if namespace == "" {
		namespace = p.buildRoutingNamespace(routing)
	}
	var org string
	if v := r.Header["X-Scope-Orgid"]; len(v) > 0 {
		org = v[0]
	}
	bufp := statsTotalsKeyBufs.Get().(*[]byte)
	buf := (*bufp)[:0]
	for _, part := range [...]string{statsMeanLineKind, org, namespace, p.fingerprintFromCtx(r.Context(), r)} {
		buf = append(append(buf, part...), 0)
	}
	keys.tenant = sha256.Sum256(buf)
	if expr != "" {
		keys.hasExpr = true
		keys.expr = sha256.Sum256(append(append(buf, statsMeanLineExprKind...), expr...))
	}
	if sel != nil {
		for _, m := range sel.Matchers {
			buf = append(append(append(buf, m.Name...), 1, byte(m.Op)), m.Value...)
			buf = append(buf, 0)
		}
	}
	keys.selector = sha256.Sum256(buf)
	*bufp = buf[:0]
	statsTotalsKeyBufs.Put(bufp)
	return keys
}

const (
	statsMeanLineKind     = "meanline"
	statsMeanLineExprKind = "expr\x00"
)

// storeMeanLine keeps what a log answer showed of the size of the lines of
// its expression (see meanLineTable) after the answer was computed.
func (p *Proxy) storeMeanLine(r *http.Request, keys *totalsKeys, sw *queryStatsWriter) {
	from, to, _ := sw.entriesWindow()
	expr := ""
	if to > from {
		expr = keys.exprText()
	}
	p.meanLines.store(p.meanLineKey(r, keys.shape.expr.Selector, expr), sw.entries.lineBytes, sw.entries.n, from, to)
}

// entriesWindow is the window [from, to] a log answer holds every line of
// its expression for: the request's window when the answer is under its
// limit, else the part from its oldest entry (backward) or to its newest
// (forward) up to the request's end, the lines a filled limit cut off lying
// outside it. False when the request or the entries' times are unread.
func (w *queryStatsWriter) entriesWindow() (from, to int64, ok bool) {
	q := w.request()
	if !q.ok || q.end <= q.start {
		return 0, 0, false
	}
	e := &w.entries
	if q.limit <= 0 || e.n < q.limit {
		return q.start, q.end, true
	}
	if !e.hasTS {
		return 0, 0, false
	}
	from, to = q.start, q.end
	if q.backward {
		from = max(q.start, e.tsMin)
	} else {
		to = min64(q.end, e.tsMax+1)
	}
	return from, to, to > from
}

// metricQueryBytes is a metric query's bytes processed in Loki's
// accounting, from the first source that has them:
//
//  1. VictoriaLogs, exact (-query-stats-exact-metric-bytes, the selector's
//     line bytes over the query's window, one more request);
//  2. the log answer of the same expression over the window (or a part of
//     it): its bytes exactly, the other lines of the metric query at its mean
//     line size;
//  3. its lines times the mean line size of the selector's (else the
//     tenant's) last log answer.
func (p *Proxy) metricQueryBytes(r *http.Request, keys *totalsKeys, sw *queryStatsWriter, lines int64) (int64, bool) {
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
	// Only once a log answer was kept with its window is it looked for: a
	// replica that has not computed one reads nothing more than the means.
	var from, to int64
	expr := ""
	if p.meanLines.windows.Load() {
		if q := sw.request(); q.ok {
			// Loki's window of a metric query: (start - range, end].
			from, to, expr = q.start-int64(keys.shape.rangeDur), q.end, keys.exprText()
		}
	}
	mk := p.meanLineKey(r, keys.shape.expr.Selector, expr)
	if expr != "" {
		if b, l, mean, ok := p.meanLines.within(mk.expr, from, to); ok {
			if rest := float64(lines) - l; rest > 0 {
				return int64(math.Round(b + rest*mean)), true
			}
			return int64(math.Round(mean * float64(lines))), true
		}
	}
	mean, ok := p.meanLines.mean(mk)
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
