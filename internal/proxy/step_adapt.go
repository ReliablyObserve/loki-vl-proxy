package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/maphash"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// The step of a range metric query (issue #1795): the proxy answers a query_range whose read passes a proxy-internal
// bound at the smallest effective step that fits, with Loki's warning, instead of Loki's limit error.
//
// Modes (-range-metric-step-mode, per tenant as range_metric_step_mode):
//   - adaptive (the default): a request is first served as it came, byte for byte as without this layer. Only when
//     its answer is a limit error of a read the proxy bounds itself and Loki would have answered (the bytes it
//     buffers from one VictoriaLogs response, the rows of one evaluation, the ordered JSON evaluator's bytes and
//     samples, a binary operand's bytes, the merged answer of a multi-tenant query), and the read's cost falls with
//     the step, is the request served again at a whole multiple of its step (read_budget.go,
//     effectiveStepMultiple). The step chosen is remembered for the query, its tenant, credentials, step and span on
//     this replica (stepVerdictTTL), so a repeat goes straight to it.
//   - floor: a step below -range-metric-min-step is raised to the smallest multiple of it at or above the floor,
//     before any read. The floor also applies in adaptive mode.
//   - off: the request is served as it came.
//
// What never changes: Loki's own limits and errors (the series limit, the 11,000-point resolution limit, a parse or
// parameter error, a backend failure, a timeout) are answered as before; so is an answer at the requested step that
// would itself pass the bytes the proxy sends (Loki's message size). No limit is raised: every effective read is held
// to the same bounds.
//
// The adjusted answer carries Loki's warnings field ("step raised from 53ms to 19s80ms to fit the query limits"), is
// counted by loki_vl_proxy_step_adjusted_total and the factor histogram, and logged with the steps, the reason and the
// buckets per series. It is never cached as such: the response caches keep the answer of the effective request (keyed
// on its step, without the warning), which a repeat reads, and the warning is added when it is sent
// (stepAdjustedHeader keeps the warning answer out of the compatibility cache).
//
// The layer runs once per client request, around the handler and inside the compatibility cache; the evaluations the
// handler starts itself (binary operands, multi-tenant children, the settled steps, the ordered JSON child) share the
// request's telemetry, so a failure of any of them makes the whole request run again at one effective step.

// Step modes.
const (
	RangeMetricStepModeOff      = "off"
	RangeMetricStepModeFloor    = "floor"
	RangeMetricStepModeAdaptive = "adaptive"
)

// Per-tenant keys of the step layer in -tenant-limits and -tenant-default-limits (never published: Loki has no such
// limit).
const (
	limitRangeMetricStepMode = "range_metric_step_mode"
	limitRangeMetricMinStep  = "range_metric_min_step"
	limitRangeMetricMaxStep  = "range_metric_max_step"
)

// stepAdjustedHeader marks an adjusted answer: the compatibility cache does not keep it (compatCacheCaptureAllowed).
const stepAdjustedHeader = "X-Proxy-Step-Adjusted"

// stepAdaptMaxRetries is how many effective steps one request tries after its own.
const stepAdaptMaxRetries = 3

// stepVerdictTTL is how long a replica remembers the step a request needed; stepVerdictMaxEntries bounds them.
const (
	stepVerdictTTL        = 10 * time.Minute
	stepVerdictMaxEntries = 4096
)

type stepMode uint8

const (
	stepModeOff stepMode = iota
	stepModeFloor
	stepModeAdaptive
)

func (m stepMode) String() string {
	switch m {
	case stepModeFloor:
		return RangeMetricStepModeFloor
	case stepModeAdaptive:
		return RangeMetricStepModeAdaptive
	}
	return RangeMetricStepModeOff
}

// parseStepMode reads a mode; "" is the default, adaptive.
func parseStepMode(s string) (stepMode, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", RangeMetricStepModeAdaptive:
		return stepModeAdaptive, true
	case RangeMetricStepModeFloor:
		return stepModeFloor, true
	case RangeMetricStepModeOff:
		return stepModeOff, true
	}
	return stepModeOff, false
}

// ValidateRangeMetricStepMode reports a -range-metric-step-mode value the proxy does not know.
func ValidateRangeMetricStepMode(s string) error {
	if _, ok := parseStepMode(s); !ok {
		return errors.New("-range-metric-step-mode must be off, floor or adaptive, got " + strconv.Quote(s))
	}
	return nil
}

// stepLimits are the step layer's settings for one request.
type stepLimits struct {
	mode           stepMode
	floor, ceiling time.Duration
}

// stepLimitsFor resolves the settings of an X-Scope-OrgID: the tenant's keys, then -tenant-default-limits, then the
// flags. A multi-tenant header takes the lowest mode (off < floor < adaptive), the highest floor and the lowest
// ceiling of its tenants.
func (p *Proxy) stepLimitsFor(orgID string) stepLimits {
	orgID = strings.TrimSpace(orgID)
	if !hasMultiTenantOrgID(orgID) {
		return p.tenantStepLimits(orgID)
	}
	tenants := splitMultiTenantOrgIDs(orgID)
	if len(tenants) == 0 {
		return p.tenantStepLimits("")
	}
	combined := p.tenantStepLimits(tenants[0])
	for _, tenant := range tenants[1:] {
		l := p.tenantStepLimits(tenant)
		if l.mode < combined.mode {
			combined.mode = l.mode
		}
		if l.floor > combined.floor {
			combined.floor = l.floor
		}
		combined.ceiling = smallestNonZero(combined.ceiling, l.ceiling)
	}
	return combined
}

func (p *Proxy) tenantStepLimits(orgID string) stepLimits {
	limits := stepLimits{mode: p.stepMode, floor: p.stepFloor, ceiling: p.stepCeiling}
	p.configMu.RLock()
	tenant := p.tenantLimits[orgID]
	defaults := p.tenantDefaultLimits
	p.configMu.RUnlock()
	if len(tenant) == 0 && len(defaults) == 0 {
		return limits
	}
	lookup := func(key string) (any, bool) {
		if v, ok := tenant[key]; ok {
			return v, true
		}
		v, ok := defaults[key]
		return v, ok
	}
	if v, ok := lookup(limitRangeMetricStepMode); ok {
		if s, isString := v.(string); isString {
			if m, known := parseStepMode(s); known {
				limits.mode = m
			}
		}
	}
	if v, ok := lookup(limitRangeMetricMinStep); ok {
		if d, err := tenantLimitDuration(v); err == nil {
			limits.floor = d
		}
	}
	if v, ok := lookup(limitRangeMetricMaxStep); ok {
		if d, err := tenantLimitDuration(v); err == nil {
			limits.ceiling = d
		}
	}
	return limits
}

// stepAttempt is the step layer's view of one attempt of a request, in its telemetry (guarded by its mu).
type stepAttempt struct {
	// armed: a failure the step layer can retry is held for it, not logged or counted by the error layer.
	armed bool
	// held: such a failure was answered in this attempt; last: the last failure answered was one.
	held, last bool
	// first is the first such failure, which the layer logs and counts when no retry answers.
	first heldFailure
	// grids are the read grids the failed reads noted.
	grids []readGrid
}

// heldFailure is a failure the error layer answered for the step layer to decide on.
type heldFailure struct {
	ctx    context.Context
	status int
	text   string
	err    error
	typed  *lokierr.Error
	reason string
}

// stepFixableReason names a limit error whose read the step bounds: the flag of the limit, without its dash; "" for
// any other error. It decides by the error's type, never its text.
func stepFixableReason(err error, typed *lokierr.Error) string {
	if typed != nil {
		switch typed.Msg() {
		case lokierr.MsgResponseTooLarge, lokierr.MsgLimitReached:
		default:
			return ""
		}
		flag, _, ok := typed.Limit()
		if !ok {
			return ""
		}
		switch flag {
		case flagBackendMaxBufferedResponseBytes, flagManualRangeMetricRowLimit, flagOrderedJSONMetricMaxBytes,
			flagOrderedJSONMetricMaxSamples, flagBinaryMetricMaxOperandBytes, flagMultiTenantMaxMergedBytes:
			return strings.TrimPrefix(flag, "-")
		}
		return ""
	}
	if errors.Is(err, errBodyTooLarge) {
		return strings.TrimPrefix(flagBackendMaxBufferedResponseBytes, "-")
	}
	if _, ok := asType[*rowLimitError](err); ok {
		return strings.TrimPrefix(flagManualRangeMetricRowLimit, "-")
	}
	return ""
}

// holdStepFixable keeps a step-fixable failure for the step layer when the request's attempt is armed: the error
// layer then writes the answer (the handler's own writers see it as before) but neither logs nor holds it for the
// error cache; the layer does that when it answers the failure after all.
func holdStepFixable(ctx context.Context, err error, typed *lokierr.Error, status int, text string) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st := &rt.stepAdapt
	if !st.armed {
		return false
	}
	reason := stepFixableReason(err, typed)
	st.last = reason != ""
	if reason == "" {
		return false
	}
	if !st.held {
		st.held = true
		st.first = heldFailure{ctx: ctx, status: status, text: text, err: err, typed: typed, reason: reason}
	}
	if g, ok := readGridOf(ctx); ok {
		st.grids = append(st.grids, g)
	}
	return true
}

// stepFailureHeld reports an armed attempt that answered a failure the step layer holds: the handler's request
// metrics skip its error statuses, which the layer counts once for the request.
func stepFailureHeld(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.stepAdapt.armed && rt.stepAdapt.held
}

// recordRangeRequest is the handler's request metric: an error status an armed attempt held is counted by the step
// layer instead.
func (p *Proxy) recordRangeRequest(ctx context.Context, endpoint string, status int, d time.Duration) {
	if status >= http.StatusBadRequest && endpoint == "query_range" && stepFailureHeld(ctx) {
		return
	}
	p.metrics.RecordRequest(endpoint, status, d)
}

func (rt *requestTelemetry) armStep() {
	rt.mu.Lock()
	rt.stepAdapt = stepAttempt{armed: true}
	rt.mu.Unlock()
}

func (rt *requestTelemetry) disarmStep() stepAttempt {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st := rt.stepAdapt
	rt.stepAdapt = stepAttempt{}
	return st
}

// stepHoldWriter passes a successful answer through and holds a failed one (status 400 and above) until the step
// layer decides whether it goes out.
type stepHoldWriter struct {
	http.ResponseWriter
	held bool
	code int
	body []byte
	// entry are the headers the response had when the layer started, restored before a retry's answer.
	entry  [8]stepHeader
	nEntry int
	clone  http.Header
}

type stepHeader struct {
	key    string
	values []string
}

func newStepHoldWriter(w http.ResponseWriter) *stepHoldWriter {
	hw := &stepHoldWriter{ResponseWriter: w}
	h := w.Header()
	if len(h) > len(hw.entry) {
		hw.clone = h.Clone()
		return hw
	}
	for k, v := range h {
		hw.entry[hw.nEntry] = stepHeader{k, v}
		hw.nEntry++
	}
	return hw
}

func (hw *stepHoldWriter) WriteHeader(code int) {
	if hw.held || hw.code != 0 {
		return
	}
	hw.code = code
	if code >= http.StatusBadRequest {
		hw.held = true
		return
	}
	hw.ResponseWriter.WriteHeader(code)
}

// stepHeldBodyMax bounds a held error body (Loki's error answers are short).
const stepHeldBodyMax = 64 << 10

func (hw *stepHoldWriter) Write(b []byte) (int, error) {
	if hw.code == 0 {
		hw.code = http.StatusOK
	}
	if hw.held {
		if room := stepHeldBodyMax - len(hw.body); room > 0 {
			if room > len(b) {
				room = len(b)
			}
			hw.body = append(hw.body, b[:room]...)
		}
		return len(b), nil
	}
	return hw.ResponseWriter.Write(b)
}

func (hw *stepHoldWriter) Flush() {
	if hw.held {
		return
	}
	if f, ok := hw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (hw *stepHoldWriter) Unwrap() http.ResponseWriter { return hw.ResponseWriter }

// restoreHeaders puts back the headers the response had when the layer started.
func (hw *stepHoldWriter) restoreHeaders() {
	h := hw.ResponseWriter.Header()
	clear(h)
	if hw.clone != nil {
		for k, v := range hw.clone {
			h[k] = v
		}
		return
	}
	for _, e := range hw.entry[:hw.nEntry] {
		h[e.key] = e.values
	}
}

// flushHeld writes the held failure.
func (hw *stepHoldWriter) flushHeld() {
	if !hw.held {
		return
	}
	hw.ResponseWriter.WriteHeader(hw.code)
	_, _ = hw.ResponseWriter.Write(hw.body)
}

// stepAnswerWriter buffers the answer of an effective request.
type stepAnswerWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *stepAnswerWriter) Header() http.Header { return w.header }
func (w *stepAnswerWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *stepAnswerWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.body.Write(b)
}

// stepVerdicts remember, per replica, the multiple of the step a request needed.
type stepVerdicts struct {
	seed maphash.Seed
	n    atomic.Int64
	mu   sync.Mutex
	m    map[uint64]stepVerdict
}

type stepVerdict struct {
	k       int64
	reason  string
	expires int64
}

func newStepVerdicts() *stepVerdicts {
	return &stepVerdicts{seed: maphash.MakeSeed(), m: map[uint64]stepVerdict{}}
}

// get is the verdict of key, if one is fresh.
func (v *stepVerdicts) get(key uint64, now time.Time) (stepVerdict, bool) {
	if v == nil || v.n.Load() == 0 {
		return stepVerdict{}, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.m[key]
	if !ok {
		return stepVerdict{}, false
	}
	if now.UnixNano() > e.expires {
		delete(v.m, key)
		v.n.Store(int64(len(v.m)))
		return stepVerdict{}, false
	}
	return e, true
}

func (v *stepVerdicts) put(key uint64, k int64, reason string, now time.Time) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.m) >= stepVerdictMaxEntries {
		for key, e := range v.m {
			if now.UnixNano() > e.expires || len(v.m) >= stepVerdictMaxEntries {
				delete(v.m, key)
			}
		}
	}
	v.m[key] = stepVerdict{k: k, reason: reason, expires: now.Add(stepVerdictTTL).UnixNano()}
	v.n.Store(int64(len(v.m)))
}

// verdictKey keys a request's verdict: its tenant, credentials, response profile, query, step and span, and the
// bounds the reads are held to (a changed bound learns again).
func (p *Proxy) verdictKey(r *http.Request, query, step string, span int64) uint64 {
	var h maphash.Hash
	h.SetSeed(p.stepVerdicts.seed)
	h.WriteString(r.Header.Get("X-Scope-OrgID"))
	h.WriteByte(0)
	h.WriteString(p.fingerprintFromCtx(r.Context(), r))
	h.WriteByte(0)
	var profile [256]byte
	_, _ = h.Write(p.appendResponseProfileCacheKey(profile[:0], r, "query_range"))
	h.WriteByte(0)
	h.WriteString(query)
	h.WriteByte(0)
	h.WriteString(step)
	var nums [64]byte
	b := strconv.AppendInt(nums[:0], span, 10)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(p.limits().BufferedBackendBodyBytes), 10)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(p.rangeMetricRowLimit), 10)
	_, _ = h.Write(b)
	return h.Sum64()
}

// rangeWindows are the range windows of a metric query's range aggregations; ok is false for a query that is not a
// metric query, a window the query does not state as a duration (a Grafana token such as $__auto, which follows the
// step) or a call whose operand the parser does not type.
func rangeWindows(expr logqlpkg.Expr) ([]time.Duration, bool) {
	var out []time.Duration
	var walk func(logqlpkg.Expr) bool
	walk = func(e logqlpkg.Expr) bool {
		switch x := e.(type) {
		case *logqlpkg.RangeAggregation:
			if x.RangeDuration <= 0 {
				return false
			}
			out = append(out, x.RangeDuration)
			return true
		case *logqlpkg.VectorAggregation:
			return walk(x.Inner)
		case *logqlpkg.BinOpExpr:
			return walk(x.Left) && walk(x.Right)
		case *logqlpkg.LiteralExpr:
			return true
		case *logqlpkg.OpaqueMetricExpr:
			if !x.Typed || x.Inner == nil {
				return x.Typed && x.Func == "vector"
			}
			return walk(x.Inner)
		}
		return false
	}
	if expr == nil || !walk(expr) || len(out) == 0 {
		return nil, false
	}
	return out, true
}

// stepRequest is what the layer knows of a request it may serve at another step.
type stepRequest struct {
	in       stepCandidateInput
	query    string
	stepRaw  string
	verdict  uint64
	maxBytes int
}

// stepRequestOf reads a request the layer may adjust; ok is false for a request without a step in whole
// milliseconds, both bounds and a metric query whose windows are stated.
func (p *Proxy) stepRequestOf(r *http.Request, lim stepLimits) (stepRequest, bool) {
	stepRaw := r.FormValue("step")
	step, ok := stepWithoutAllocating(stepRaw)
	if !ok || step < time.Millisecond || step%time.Millisecond != 0 {
		return stepRequest{}, false
	}
	startNs, okS := lokiBoundUnixNano(strings.TrimSpace(r.FormValue("start")))
	endNs, okE := lokiBoundUnixNano(strings.TrimSpace(r.FormValue("end")))
	if !okS || !okE || endNs <= startNs {
		return stepRequest{}, false
	}
	query := r.FormValue("query")
	expr, err := parseLogQLExpr(r.Context(), query)
	if err != nil {
		return stepRequest{}, false
	}
	windows, ok := rangeWindows(expr)
	if !ok {
		return stepRequest{}, false
	}
	share := 0.5
	if p.stepBudgetBytes > 0 && p.limits().BufferedBackendBodyBytes > 0 {
		share = float64(p.stepBudgetBytes) / float64(p.limits().BufferedBackendBodyBytes)
	}
	onGrid := p.alignQueriesWithStep && startNs%int64(step) == 0 && endNs%int64(step) == 0
	in := stepCandidateInput{step: step, prev: 1, startNs: startNs, endNs: endNs, onGrid: onGrid, windows: windows,
		floor: lim.floor, ceiling: lim.ceiling, maxBuckets: int64(p.stepMaxPoints), share: share}
	return stepRequest{in: in, query: query, stepRaw: stepRaw, maxBytes: p.limits().BufferedBackendBodyBytes}, true
}

// effectiveStepRequest is r at k times its step, on the bounds effectiveBounds gives.
func effectiveStepRequest(r *http.Request, sr stepRequest, k int64) *http.Request {
	startNs, endNs, _ := effectiveBounds(sr.in, k)
	child := r.Clone(r.Context())
	_ = child.ParseForm()
	form := url.Values{}
	for key, v := range child.Form {
		form[key] = append([]string(nil), v...)
	}
	form.Set("step", strconv.FormatInt(sr.in.step.Milliseconds()*k, 10)+"ms")
	form.Set("start", encodeAlignedBound(startNs))
	form.Set("end", encodeAlignedBound(endNs))
	child.Form, child.PostForm = form, nil
	child.URL.RawQuery = form.Encode()
	// The answer is read whole for its warning: never one of the cache's encoded variants.
	child.Header.Del("Accept-Encoding")
	return child
}

// rangeStepLayer is the step layer around next (the handler behind the time bounds); cached is next behind the
// compatibility cache, which serves an effective request.
func (p *Proxy) rangeStepLayer(next, cached http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()
		rt := getRequestTelemetry(r.Context())
		if rt == nil || r.FormValue("step") == "" {
			next(w, r)
			return
		}
		lim := p.stepLimitsFor(r.Header.Get("X-Scope-OrgID"))
		if lim.mode == stepModeOff {
			next(w, r)
			return
		}
		if lim.floor > 0 {
			if sr, ok := p.stepRequestOf(r, lim); ok && sr.in.step < lim.floor {
				if k, fits := floorStepMultiple(sr.in); fits {
					if p.serveAtStep(w, r, rt, cached, sr, k, lim.mode, "range-metric-min-step", "floor", nil) {
						return
					}
				}
			}
		}
		if lim.mode != stepModeAdaptive {
			next(w, r)
			return
		}
		// A request this replica already served at a coarser step goes straight to it.
		if p.stepVerdicts != nil && p.stepVerdicts.n.Load() > 0 {
			if sr, ok := p.stepRequestOf(r, lim); ok {
				sr.verdict = p.verdictKey(r, sr.query, sr.stepRaw, sr.in.endNs-sr.in.startNs)
				if v, found := p.stepVerdicts.get(sr.verdict, time.Now()); found && v.k > 1 {
					if p.serveAtStep(w, r, rt, cached, sr, v.k, lim.mode, v.reason, "learned", nil) {
						return
					}
				}
			}
		}
		hw := newStepHoldWriter(w)
		rt.armStep()
		next(hw, r)
		st := rt.disarmStep()
		if !hw.held {
			return
		}
		if st.held && st.last && r.Context().Err() == nil {
			if sr, ok := p.stepRequestOf(r, lim); ok {
				sr.in.failed = st.grids
				if k, fits := effectiveStepMultiple(sr.in); fits {
					if p.stepVerdicts != nil {
						sr.verdict = p.verdictKey(r, sr.query, sr.stepRaw, sr.in.endNs-sr.in.startNs)
					}
					if p.serveAtStep(w, r, rt, cached, sr, k, lim.mode, st.first.reason, "retry", hw) {
						return
					}
				}
			}
		}
		p.answerHeldFailure(st, began)
		hw.flushHeld()
	}
}

// answerHeldFailure logs, holds and counts a failure the layer answers after all, as the error layer would have.
func (p *Proxy) answerHeldFailure(st stepAttempt, began time.Time) {
	if !st.held {
		return
	}
	f := st.first
	p.logRequestError(f.ctx, f.status, f.text, f.err, f.typed)
	recordFlightFailure(f.ctx, f.err, f.status, f.typed)
	p.metrics.RecordRequest("query_range", f.status, time.Since(began))
}

// serveAtStep answers r at k times its step, trying coarser multiples while the effective request fails a bound the
// step sizes. It reports false, having written nothing, when no effective step answers: the caller answers as
// without the layer. hw is the writer of the failed attempt (nil when the request was not served as it came).
func (p *Proxy) serveAtStep(w http.ResponseWriter, r *http.Request, rt *requestTelemetry, cached http.HandlerFunc, sr stepRequest, k int64, mode stepMode, reason, source string, hw *stepHoldWriter) bool {
	began := time.Now()
	for attempt := 0; attempt < stepAdaptMaxRetries; attempt++ {
		if r.Context().Err() != nil {
			return false
		}
		child := effectiveStepRequest(r, sr, k)
		out := &stepAnswerWriter{header: make(http.Header)}
		rt.armStep()
		cached(out, child)
		st := rt.disarmStep()
		if out.code == 0 {
			out.code = http.StatusOK
		}
		if out.code < http.StatusBadRequest {
			body := out.body.Bytes()
			// An answer at the requested step that would itself pass the bytes the proxy sends is Loki's
			// message size error, not a read bound: answered as before.
			if fullAnswerBytes(body, k) > int64(sr.maxBytes) && sr.maxBytes > 0 {
				p.metrics.RecordStepAdjusted("query_range", mode.String(), "answer-too-large", 0)
				return false
			}
			if sr.verdict != 0 && p.stepVerdicts != nil {
				p.stepVerdicts.put(sr.verdict, k, reason, time.Now())
			}
			p.writeStepAdjusted(w, out, sr, k, source, hw)
			p.metrics.RecordStepAdjusted("query_range", mode.String(), reason, k)
			p.logStepAdjusted(r.Context(), sr, k, mode, reason, source, len(body), time.Since(began))
			return true
		}
		if !st.held || !st.last {
			// A failure the step does not size: the effective request answers it, as the request itself would.
			if hw == nil {
				p.writeStepAnswer(w, out, nil)
				return true
			}
			return false
		}
		in := sr.in
		in.prev, in.failed = k, st.grids
		next, fits := effectiveStepMultiple(in)
		if !fits || attempt == stepAdaptMaxRetries-1 {
			if hw == nil {
				p.answerHeldFailure(st, began)
				p.writeStepAnswer(w, out, nil)
				return true
			}
			return false
		}
		k = next
	}
	return false
}

// fullAnswerBytes estimates the answer at the requested step from the answer at k times it: every point of it k
// times, at most 32 bytes each.
func fullAnswerBytes(body []byte, k int64) int64 {
	points := int64(bytes.Count(body, []byte("],[")) + bytes.Count(body, []byte(`"values":[`)))
	return int64(len(body)) + points*(k-1)*32
}

// writeStepAnswer writes an effective request's answer as it is, with the warning when warning is set.
func (p *Proxy) writeStepAnswer(w http.ResponseWriter, out *stepAnswerWriter, warning []byte) {
	h := w.Header()
	for key, v := range out.header {
		h[key] = v
	}
	body := out.body.Bytes()
	if warning != nil {
		body = warning
	}
	w.WriteHeader(out.code)
	_, _ = w.Write(body)
}

// writeStepAdjusted writes the adjusted answer: the effective request's body with Loki's warning, never kept by the
// compatibility cache.
func (p *Proxy) writeStepAdjusted(w http.ResponseWriter, out *stepAnswerWriter, sr stepRequest, k int64, source string, hw *stepHoldWriter) {
	if hw != nil {
		hw.restoreHeaders()
	}
	requested, effective := formatLokiDuration(sr.in.step), formatLokiDuration(sr.in.step*time.Duration(k))
	text := "step raised from " + requested + " to " + effective + " to fit the query limits"
	if source == "floor" {
		text = "step raised from " + requested + " to " + effective + ", the minimum step of range metric queries"
	}
	w.Header().Set(stepAdjustedHeader, requested+";"+effective)
	p.writeStepAnswer(w, out, withLokiWarning(out.body.Bytes(), text))
}

// floorStepMultiple is the smallest multiple of the step at or above the floor, within the ceiling and the range.
func floorStepMultiple(in stepCandidateInput) (int64, bool) {
	if in.step <= 0 || in.floor <= in.step {
		return 0, false
	}
	k := ceilDiv(int64(in.floor), int64(in.step))
	if in.ceiling > 0 && time.Duration(k)*in.step > in.ceiling {
		return 0, false
	}
	_, _, ok := effectiveBounds(in, k)
	return k, ok
}

// withLokiWarning adds a warning to a Loki JSON answer's warnings, creating the field when the answer has none.
func withLokiWarning(body []byte, text string) []byte {
	warning, _ := json.Marshal(text)
	if len(body) < 2 || body[0] != '{' {
		return body
	}
	if hasTopLevelWarnings(body) {
		var fields map[string]json.RawMessage
		var warnings []json.RawMessage
		if json.Unmarshal(body, &fields) != nil || json.Unmarshal(fields["warnings"], &warnings) != nil {
			return body
		}
		fields["warnings"], _ = json.Marshal(append(warnings, warning))
		out, err := marshalOrderedJSONObject(fields, []string{"status", "warnings", "data"})
		if err != nil {
			return body
		}
		return out
	}
	separator := []byte{}
	if len(body) > 2 {
		separator = []byte{','}
	}
	return bytes.Join([][]byte{[]byte(`{"warnings":[`), warning, []byte{']'}, separator, body[1:]}, nil)
}

// logStepAdjusted writes the log line of an adjusted answer.
func (p *Proxy) logStepAdjusted(ctx context.Context, sr stepRequest, k int64, mode stepMode, reason, source string, answerBytes int, took time.Duration) {
	if p.log == nil || !p.log.Enabled(ctx, slog.LevelInfo) {
		return
	}
	var requestedBuckets, effectiveBuckets int64
	for _, w := range sr.in.windows {
		requestedBuckets = max(requestedBuckets, bucketsAt(sr.in, 1, w))
		effectiveBuckets = max(effectiveBuckets, bucketsAt(sr.in, k, w))
	}
	p.log.LogAttrs(ctx, slog.LevelInfo, "query step adjusted",
		slog.String("endpoint", "query_range"),
		slog.String("mode", mode.String()),
		slog.String("reason", reason),
		slog.String("estimate_source", source),
		slog.Duration("requested_step", sr.in.step),
		slog.Duration("effective_step", sr.in.step*time.Duration(k)),
		slog.Int64("factor", k),
		slog.Int64("requested_buckets_per_series", requestedBuckets),
		slog.Int64("effective_buckets_per_series", effectiveBuckets),
		slog.Int("answer_bytes", answerBytes),
		slog.Duration("adjust_duration", took))
}
