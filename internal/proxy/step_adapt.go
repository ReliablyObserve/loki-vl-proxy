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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The step of a range metric query (issue #1795): the proxy answers a query_range whose read passes a proxy-internal
// bound at the smallest effective step that fits, with Loki's warning, instead of Loki's limit error.
//
// Modes (-range-metric-step-mode, per tenant as range_metric_step_mode):
//   - adaptive (the default): a request is first served as it came, byte for byte as without this layer. Only when
//     its answer is a limit error of a read the proxy bounds itself and Loki would have answered (the bytes it
//     buffers from one VictoriaLogs response, the rows of one evaluation, the ordered JSON evaluator's bytes and
//     samples, a binary operand's bytes, the merged answer of a multi-tenant query), and the read's cost falls with
//     the step, is the request served again at the closest step that fits: a whole multiple of its step, or the
//     finest fitting step when that multiple is more than twice coarser (read_budget.go, effectiveStep). The step
//     chosen is remembered for the query, its tenant, credentials, step and span on
//     this replica (stepVerdictTTL), so a repeat goes straight to it.
//   - floor: a step below -range-metric-min-step is raised to the smallest multiple of it at or above the floor,
//     before any read. The floor also applies in adaptive mode.
//   - off: the request is served as it came.
//
// What never changes: Loki's own limits and errors (the series limit, the 11,000-point resolution limit, a parse or
// parameter error, a backend failure, a timeout) are answered as before, and so is a failed read whose cost a coarser
// step does not lower (raw rows read for another reason than the step, one step's window, the series of a window).
// No limit is raised: every effective read is held to the same bounds.
//
// The adjusted answer carries Loki's warnings field ("step raised from 53ms to 360ms to fit the query limits"), is
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
	// held: such a failure was answered in this attempt; last: the last failure answered was one; halve: one of them
	// bounded the answer, not a read.
	held, last, halve bool
	// failed holds the failures, set by the first one (the request's telemetry stays its size for every request).
	failed *stepFailures
}

// stepFailures are the failures an attempt held.
type stepFailures struct {
	// first is the first such failure, which the layer logs and counts when no retry answers.
	first heldFailure
	// grids are the read grids the failed reads noted.
	grids []readGrid
}

// first is the attempt's first held failure (zero when it held none).
func (st stepAttempt) first() heldFailure {
	if st.failed == nil {
		return heldFailure{}
	}
	return st.failed.first
}

// grids are the read grids the attempt's failed reads noted.
func (st stepAttempt) grids() []readGrid {
	if st.failed == nil {
		return nil
	}
	return st.failed.grids
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

// stepFixableReason names a limit error whose read or answer the step sizes: the flag of the limit, without its
// dash; "" for any other error. answer reports a bound on the answer (its samples, an operand's or a merged answer's
// bytes), which holds fewer points at a coarser step whatever was read; the other bounds are on one read, whose cost
// falls with the step only on a bucket grid (effectiveStep). It decides by the error's type, never its text.
func stepFixableReason(err error, typed *lokierr.Error) (reason string, answer bool) {
	flag := ""
	if typed != nil {
		switch typed.Msg() {
		case lokierr.MsgResponseTooLarge, lokierr.MsgLimitReached:
		default:
			return "", false
		}
		f, _, ok := typed.Limit()
		if !ok {
			return "", false
		}
		flag = f
	} else if errors.Is(err, errBodyTooLarge) {
		flag = flagBackendMaxBufferedResponseBytes
	} else if _, ok := asType[*rowLimitError](err); ok {
		flag = flagManualRangeMetricRowLimit
	}
	switch flag {
	case flagBackendMaxBufferedResponseBytes, flagManualRangeMetricRowLimit, flagOrderedJSONMetricMaxBytes:
		return strings.TrimPrefix(flag, "-"), false
	case flagOrderedJSONMetricMaxSamples, flagBinaryMetricMaxOperandBytes, flagMultiTenantMaxMergedBytes:
		return strings.TrimPrefix(flag, "-"), true
	}
	return "", false
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
	st := rt.stepAdapt
	if st == nil || !st.armed {
		return false
	}
	reason, answer := stepFixableReason(err, typed)
	st.last = reason != ""
	if reason == "" {
		return false
	}
	st.halve = st.halve || answer
	if !st.held {
		st.held = true
		st.failed = &stepFailures{first: heldFailure{ctx: ctx, status: status, text: text, err: err, typed: typed, reason: reason}}
	}
	if g, ok := readGridOf(ctx); ok {
		st.failed.grids = append(st.failed.grids, g)
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
	return rt.stepAdapt != nil && rt.stepAdapt.armed && rt.stepAdapt.held
}

// recordRangeRequest is the handler's request metric: an error status an armed attempt held is counted by the step
// layer instead.
func (p *Proxy) recordRangeRequest(ctx context.Context, endpoint string, status int, d time.Duration) {
	if status >= http.StatusBadRequest && endpoint == "query_range" && stepFailureHeld(ctx) {
		return
	}
	p.metrics.RecordRequest(endpoint, status, d)
}

// armStep makes st the request's armed attempt; the caller owns st (the hold writer carries the first one, so a
// request that fits allocates no attempt of its own).
func (rt *requestTelemetry) armStep(st *stepAttempt) {
	rt.mu.Lock()
	*st = stepAttempt{armed: true}
	rt.stepAdapt = st
	rt.mu.Unlock()
}

func (rt *requestTelemetry) disarmStep() stepAttempt {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stepAdapt == nil {
		return stepAttempt{}
	}
	st := *rt.stepAdapt
	rt.stepAdapt = nil
	return st
}

// stepHoldWriter passes a successful answer through and holds a failed one (status 400 and above) until the step
// layer decides whether it goes out.
type stepHoldWriter struct {
	http.ResponseWriter
	held bool
	code int
	body []byte
	// attempt is the request's armed attempt (requestTelemetry.stepAdapt points at it while the handler runs).
	attempt stepAttempt
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
	plainIfUntyped(hw.Header())
	hw.ResponseWriter.WriteHeader(code)
}

// plainIfUntyped names the type of an answer whose writer named none as what the server would send for it: Go's
// content sniffing and the request logger's statusCapture both answer a Loki JSON body as text/plain; charset=utf-8.
func plainIfUntyped(h http.Header) {
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "text/plain; charset=utf-8")
	}
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
	if h := hw.Header(); h.Get("Content-Type") == "" {
		h.Set("Content-Type", "text/plain; charset=utf-8")
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
	h := hw.Header()
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
	if hw.Header().Get("Content-Type") == "" {
		setReplayContentType(hw.ResponseWriter, hw.code)
	}
	hw.ResponseWriter.WriteHeader(hw.code)
	_, _ = hw.ResponseWriter.Write(hw.body)
}

// stepAnswerWriter buffers the answer of an effective request.
type stepAnswerWriter struct {
	header  http.Header
	code    int
	body    bytes.Buffer
	attempt stepAttempt
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
	eff     time.Duration
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

func (v *stepVerdicts) put(key uint64, eff time.Duration, reason string, now time.Time) {
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
	v.m[key] = stepVerdict{eff: eff, reason: reason, expires: now.Add(stepVerdictTTL).UnixNano()}
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

// rangeWindows are the range windows of a metric query's range aggregations, read from its resolved plan; ok is false
// for a query that is not a metric query, a window the query does not state as a duration (a Grafana token such as
// $__auto, which follows the step) or a call the plan keeps as text.
func rangeWindows(root queryplan.Node) ([]time.Duration, bool) {
	var out []time.Duration
	var walk func(queryplan.Node) bool
	walk = func(n queryplan.Node) bool {
		switch x := n.(type) {
		case *queryplan.RangeAgg:
			if x.Range <= 0 {
				return false
			}
			out = append(out, x.Range)
			return true
		case *queryplan.VectorAgg:
			return walk(x.In)
		case *queryplan.Rank:
			return walk(x.In)
		case *queryplan.LabelReplace:
			return walk(x.In)
		case *queryplan.BinOp:
			return walk(x.L) && walk(x.R)
		case *queryplan.Scalar, *queryplan.VectorLit:
			return true
		}
		return false
	}
	if root == nil || !walk(root) || len(out) == 0 {
		return nil, false
	}
	return out, true
}

// stepRequest is what the layer knows of a request it may serve at another step.
type stepRequest struct {
	in      stepCandidateInput
	query   string
	stepRaw string
	verdict uint64
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
	plan := resolvedPlan(query)
	if plan == nil {
		return stepRequest{}, false
	}
	windows, ok := rangeWindows(plan.Root)
	if !ok {
		return stepRequest{}, false
	}
	share := 0.5
	if p.stepBudgetBytes > 0 && p.limits().BufferedBackendBodyBytes > 0 {
		share = float64(p.stepBudgetBytes) / float64(p.limits().BufferedBackendBodyBytes)
	}
	// The effective request is anchored as Loki anchors a request at that step: at its multiples, as Loki's step
	// alignment does and as its interval splitting (split_queries_by_interval, on by default) floors the start to the
	// step anyway; the bounds stay inside the requested range.
	onGrid := true
	in := stepCandidateInput{step: step, startNs: startNs, endNs: endNs, onGrid: onGrid, windows: windows,
		floor: lim.floor, ceiling: lim.ceiling, maxBuckets: int64(p.stepMaxPoints), share: share}
	return stepRequest{in: in, query: query, stepRaw: stepRaw}, true
}

// effectiveStepRequest is r at the effective step eff, on the bounds effectiveBounds gives.
func effectiveStepRequest(r *http.Request, sr stepRequest, eff time.Duration) *http.Request {
	startNs, endNs, _ := effectiveBounds(sr.in, eff)
	child := r.Clone(r.Context())
	_ = child.ParseForm()
	form := url.Values{}
	for key, v := range child.Form {
		form[key] = append([]string(nil), v...)
	}
	// The query string the client sent with the step and bounds replaced: the effective request keys the response
	// cache as the same request sent at that step would (a client asking the effective step reads its answer).
	values := map[string]string{"step": strconv.FormatInt(eff.Milliseconds(), 10) + "ms", "start": encodeAlignedBound(startNs), "end": encodeAlignedBound(endNs)}
	for key, v := range values {
		form.Set(key, v)
	}
	child.Form, child.PostForm = form, nil
	child.URL.RawQuery = replaceQueryParams(child.URL.RawQuery, values, form)
	// The answer is read whole for its warning: never one of the cache's encoded variants.
	child.Header.Del("Accept-Encoding")
	return child
}

// replaceQueryParams is raw with the values of the keys in values replaced in place, keeping the order and the
// spelling of every other parameter; a key raw lacks is appended. An empty raw (a form body) is form encoded.
func replaceQueryParams(raw string, values map[string]string, form url.Values) string {
	if raw == "" {
		return form.Encode()
	}
	parts := strings.Split(raw, "&")
	seen := map[string]bool{}
	out := parts[:0]
	for _, part := range parts {
		key, _, _ := strings.Cut(part, "=")
		name, err := url.QueryUnescape(key)
		if v, ok := values[name]; ok && err == nil {
			if seen[name] {
				continue
			}
			seen[name] = true
			part = key + "=" + url.QueryEscape(v)
		}
		out = append(out, part)
	}
	for _, name := range []string{"query", "start", "end", "step"} {
		if v, ok := values[name]; ok && !seen[name] {
			out = append(out, name+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(out, "&")
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
				if eff, fits := floorStep(sr.in); fits {
					if p.serveAtStep(w, r, rt, cached, sr, eff, lim.mode, "range-metric-min-step", "floor", nil) {
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
				if v, found := p.stepVerdicts.get(sr.verdict, time.Now()); found && v.eff > sr.in.step {
					if p.serveAtStep(w, r, rt, cached, sr, v.eff, lim.mode, v.reason, "learned", nil) {
						return
					}
				}
			}
		}
		hw := newStepHoldWriter(w)
		rt.armStep(&hw.attempt)
		next(hw, r)
		st := rt.disarmStep()
		if !hw.held {
			return
		}
		if st.held && st.last && r.Context().Err() == nil {
			if sr, ok := p.stepRequestOf(r, lim); ok {
				sr.in.failed, sr.in.halve = st.grids(), st.halve
				if eff, fits := effectiveStep(sr.in); fits {
					if p.stepVerdicts != nil {
						sr.verdict = p.verdictKey(r, sr.query, sr.stepRaw, sr.in.endNs-sr.in.startNs)
					}
					if p.serveAtStep(w, r, rt, cached, sr, eff, lim.mode, st.first().reason, "retry", hw) {
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
	f := st.first()
	p.logRequestError(f.ctx, f.status, f.text, f.err, f.typed)
	recordFlightFailure(f.ctx, f.err, f.status, f.typed)
	p.metrics.RecordRequest("query_range", f.status, time.Since(began))
}

// serveAtStep answers r at the effective step eff, trying coarser multiples while the effective request fails a bound the
// step sizes. It reports false, having written nothing, when no effective step answers: the caller answers as
// without the layer. hw is the writer of the failed attempt (nil when the request was not served as it came).
func (p *Proxy) serveAtStep(w http.ResponseWriter, r *http.Request, rt *requestTelemetry, cached http.HandlerFunc, sr stepRequest, eff time.Duration, mode stepMode, reason, source string, hw *stepHoldWriter) bool {
	began := time.Now()
	for attempt := 0; attempt < stepAdaptMaxRetries; attempt++ {
		if r.Context().Err() != nil {
			return false
		}
		child := effectiveStepRequest(r, sr, eff)
		out := &stepAnswerWriter{header: make(http.Header)}
		rt.armStep(&out.attempt)
		cached(out, child)
		st := rt.disarmStep()
		if out.code == 0 {
			out.code = http.StatusOK
		}
		if out.code < http.StatusBadRequest {
			body := out.body.Bytes()
			if sr.verdict != 0 && p.stepVerdicts != nil {
				p.stepVerdicts.put(sr.verdict, eff, reason, time.Now())
			}
			p.writeStepAdjusted(w, out, sr, eff, source, hw)
			p.metrics.RecordStepAdjusted("query_range", mode.String(), reason, float64(eff)/float64(sr.in.step))
			p.logStepAdjusted(r.Context(), sr, eff, mode, reason, source, len(body), time.Since(began))
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
		in.prev, in.failed, in.halve = eff, st.grids(), st.halve
		next, fits := effectiveStep(in)
		if !fits || attempt == stepAdaptMaxRetries-1 {
			if hw == nil {
				p.answerHeldFailure(st, began)
				p.writeStepAnswer(w, out, nil)
				return true
			}
			return false
		}
		eff = next
	}
	return false
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
	if h.Get("Content-Type") == "" {
		setReplayContentType(w, out.code)
	}
	w.WriteHeader(out.code)
	_, _ = w.Write(body)
}

// writeStepAdjusted writes the adjusted answer: the effective request's body with Loki's warning, never kept by the
// compatibility cache.
func (p *Proxy) writeStepAdjusted(w http.ResponseWriter, out *stepAnswerWriter, sr stepRequest, eff time.Duration, source string, hw *stepHoldWriter) {
	if hw != nil {
		hw.restoreHeaders()
	}
	requested, effective := formatLokiDuration(sr.in.step), formatLokiDuration(eff)
	text := "step raised from " + requested + " to " + effective + " to fit the query limits"
	if source == "floor" {
		text = "step raised from " + requested + " to " + effective + ", the minimum step of range metric queries"
	}
	w.Header().Set(stepAdjustedHeader, requested+";"+effective)
	p.writeStepAnswer(w, out, withLokiWarning(out.body.Bytes(), text))
}

// floorStep is the smallest multiple of the step at or above the floor, within the ceiling and the range.
func floorStep(in stepCandidateInput) (time.Duration, bool) {
	if in.step <= 0 || in.floor <= in.step {
		return 0, false
	}
	eff := time.Duration(ceilDiv(int64(in.floor), int64(in.step))) * in.step
	if in.ceiling > 0 && eff > in.ceiling {
		return 0, false
	}
	_, _, ok := effectiveBounds(in, eff)
	return eff, ok
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
	return bytes.Join([][]byte{[]byte(`{"warnings":[`), warning, {']'}, separator, body[1:]}, nil)
}

// logStepAdjusted writes the log line of an adjusted answer.
func (p *Proxy) logStepAdjusted(ctx context.Context, sr stepRequest, eff time.Duration, mode stepMode, reason, source string, answerBytes int, took time.Duration) {
	if p.log == nil || !p.log.Enabled(ctx, slog.LevelInfo) {
		return
	}
	var requestedBuckets, effectiveBuckets int64
	for _, w := range sr.in.windows {
		requestedBuckets = max(requestedBuckets, bucketsAt(sr.in, sr.in.step, w))
		effectiveBuckets = max(effectiveBuckets, bucketsAt(sr.in, eff, w))
	}
	p.log.LogAttrs(ctx, slog.LevelInfo, "query step adjusted",
		slog.String("endpoint", "query_range"),
		slog.String("mode", mode.String()),
		slog.String("reason", reason),
		slog.String("estimate_source", source),
		slog.Duration("requested_step", sr.in.step),
		slog.Duration("effective_step", eff),
		slog.Float64("factor", float64(eff)/float64(sr.in.step)),
		slog.Bool("on_requested_grid", eff%sr.in.step == 0),
		slog.Int64("requested_buckets_per_series", requestedBuckets),
		slog.Int64("effective_buckets_per_series", effectiveBuckets),
		slog.Int("answer_bytes", answerBytes),
		slog.Duration("adjust_duration", took))
}
