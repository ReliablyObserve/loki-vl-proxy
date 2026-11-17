package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/maphash"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// Request flights and the error cache.
//
// Identical read requests that arrive while one of them runs wait for it
// instead of running again (the request's response key, which carries the
// tenant, the credentials' fingerprint and the response profile, is the
// flight's key). When the leader ends:
//
//   - its success stored in the response cache is read from there by every
//     follower; a success the cache does not keep (a POST, a streamed answer)
//     is handed to them in memory when it is complete and small enough, and
//     otherwise each follower runs on its own as soon as the leader flushes
//     or outgrows what can be kept;
//   - its failure is handed to every follower as written, byte for byte,
//     when the failure has a shareable class, and is held for that class's
//     TTL so the next identical requests answer it without running;
//   - a leader that was cancelled or ran out of its own time hands nothing:
//     a follower whose own request is alive starts again as the new leader.
//
// The class of a failure is decided by the type of the error the request
// answered through the error layer (writeFailure records it), and kept only
// when the request's query plan says the request can fail in that class
// (queryplan.Failures). Held answers stay in this replica's memory: a peer
// push would take about as long as the transient TTL, and each replica asks
// VictoriaLogs at most once per TTL per failing request.

// Error cache defaults, the flags' defaults in cmd/proxy.
const (
	// DefaultErrorCacheTransientTTL holds a backend failure (VictoriaLogs 5xx,
	// 429, a refused or reset connection) for one second: a storm of
	// identical requests (refresh loops, many dashboards, retries) asks
	// VictoriaLogs once per second per request, and the first request after
	// the backend recovers answers within a second.
	DefaultErrorCacheTransientTTL = time.Second
	// DefaultErrorCacheDataTTL holds a failure read from the data in the
	// request's window (a pipeline error over the lines read, a series or
	// line limit reached) for five seconds: the same window fails the same
	// way until new lines arrive, and each such check reads the window's
	// lines again.
	DefaultErrorCacheDataTTL = 5 * time.Second
	// DefaultErrorCacheMaxEntries and DefaultErrorCacheMaxBytes bound the
	// held answers per replica.
	DefaultErrorCacheMaxEntries = 4096
	DefaultErrorCacheMaxBytes   = 8 << 20
	// errorFlightMaxAnswerBytes is the largest failed body held or shared
	// (Loki's error texts are short; a parse error quotes the query, which
	// Loki's input size limit bounds).
	errorFlightMaxAnswerBytes = 64 << 10
	// uncachedFlightShareBytes is the largest success handed to followers in
	// memory when the response cache does not keep the answer.
	uncachedFlightShareBytes = 1 << 20
	// maxFlightRounds bounds how often a follower starts again after leaders
	// that handed nothing; it then runs on its own.
	maxFlightRounds = 3
	// errorFlightKeyVersion prefixes every flight key; bump it when what a
	// held answer depends on changes.
	errorFlightKeyVersion = "errf:v1:"
)

// ErrorCacheConfig configures request flights and the error cache.
type ErrorCacheConfig struct {
	// Disabled holds no failed answer (identical requests in flight still
	// share one run unless CoalescerDisabled is set).
	Disabled bool
	// TransientTTL and DataTTL hold a failure of each class; zero holds none.
	TransientTTL time.Duration
	DataTTL      time.Duration
	// MaxEntries and MaxBytes bound the held answers; zero uses the defaults.
	MaxEntries int
	MaxBytes   int
}

// errorFlightCounters are the flight outcomes, for metrics.
type errorFlightCounters struct {
	held      [4]atomic.Int64 // requests answered from a held failure, by class
	coalesced [4]atomic.Int64 // followers handed the leader's failure, by class
	stored    [4]atomic.Int64 // failures held, by class
	succeeded atomic.Int64    // followers that read the leader's success
}

func newErrorFlights(cfg Config) *cache.ErrorFlights {
	if cfg.CoalescerDisabled && cfg.ErrorCache.Disabled {
		return nil
	}
	maxEntries, maxBytes := cfg.ErrorCache.MaxEntries, cfg.ErrorCache.MaxBytes
	if maxEntries <= 0 {
		maxEntries = DefaultErrorCacheMaxEntries
	}
	if maxBytes <= 0 {
		maxBytes = DefaultErrorCacheMaxBytes
	}
	return cache.NewErrorFlights(maxEntries, maxBytes, errorFlightMaxAnswerBytes, !cfg.CoalescerDisabled)
}

// errorPolicy is the error cache policy of r: the operator's TTL for each
// class the request's plan can fail in.
func (p *Proxy) errorPolicy(endpoint string, r *http.Request) (cache.Policy, queryplan.Failures) {
	reach := failureReach(endpoint, r)
	transient, data := p.errorCacheTransientTTL, p.errorCacheDataTTL
	if p.errorCacheDisabled {
		transient, data = 0, 0
	}
	pol := cache.NewPolicy(transient, data, 0).Only(func(c cache.ErrorClass) bool {
		switch c {
		case cache.ErrorTransient:
			return reach.Has(queryplan.FailBackend)
		case cache.ErrorData:
			return reach&(queryplan.FailLimit|queryplan.FailPipeline) != 0
		}
		return true // a failure the request alone decides: any request can have one
	})
	return pol, reach
}

// failureReach is what r can fail in: the plan's classes for a query, and a
// backend failure or a limit for every other read.
func failureReach(endpoint string, r *http.Request) queryplan.Failures {
	switch endpoint {
	case "query", "query_range":
		if plan := resolvedPlan(r.FormValue("query")); plan != nil {
			return plan.Failures()
		}
		return 0 // a query that does not parse fails only as the request decides
	}
	return queryplan.FailBackend | queryplan.FailLimit
}

// flightFailure is the failure a request answered through the error layer.
type flightFailure struct {
	status int
	class  cache.ErrorClass
	need   queryplan.Failures
	// windowFree: the failure does not depend on the request's time window
	// (a VictoriaLogs 5xx or 429, a refused or reset connection).
	windowFree bool
	count      int
	mixed      bool
}

// recordFlightFailure keeps the class of a failure the request answered; a
// request that answered failures of more than one class or status holds none.
func recordFlightFailure(ctx context.Context, err error, status int, typed *lokierr.Error) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	class, need := failureClassOf(ctx, err, typed)
	windowFree := class == cache.ErrorTransient && windowFreeFailure(err, typed)
	rt.mu.Lock()
	f := &rt.failure
	if f.count > 0 && (f.status != status || f.class != class || f.need != need) {
		f.mixed = true
	}
	f.windowFree = (f.count == 0 || f.windowFree) && windowFree
	f.status, f.class, f.need = status, class, need
	f.count++
	rt.mu.Unlock()
}

// failureClassOf is the class of a failure by the type of its error, and the
// part of the plan it needs: a class the request's plan rules out is not held.
// It never reads the error's text.
func failureClassOf(ctx context.Context, err error, typed *lokierr.Error) (cache.ErrorClass, queryplan.Failures) {
	if ctx != nil && ctx.Err() != nil {
		return cache.ErrorNever, 0 // the client's own cancellation or deadline
	}
	if typed != nil {
		switch typed.Msg() {
		case lokierr.MsgBackendFailed:
			if timedOutOrAborted(typed.Cause()) {
				return cache.ErrorNever, 0
			}
			return cache.ErrorTransient, queryplan.FailBackend
		case lokierr.MsgSeriesLimit, lokierr.MsgIndexSeriesLimit, lokierr.MsgLimitReached, lokierr.MsgResponseTooLarge:
			return cache.ErrorData, queryplan.FailLimit
		case lokierr.MsgNoOrgID:
			return cache.ErrorNever, 0 // per-client credentials
		}
		switch typed.Kind() {
		case lokierr.KindParse, lokierr.KindParam, lokierr.KindForm:
			return cache.ErrorRequest, 0
		case lokierr.KindLimit:
			// The query length and lookback limits: the request and the
			// configured limit decide.
			return cache.ErrorRequest, 0
		}
		// The proxy's own queue and load shedding, a cancellation, a timeout
		// (the request's or the backend's: it depends on how long the query
		// ran, and a retry may answer), an internal failure: never shared.
		return cache.ErrorNever, 0
	}
	if timedOutOrAborted(err) {
		return cache.ErrorNever, 0
	}
	if _, defect := asType[*planDefectError](err); defect {
		return cache.ErrorNever, 0
	}
	if isOrderedJSONPipelineError(err) {
		return cache.ErrorData, queryplan.FailPipeline
	}
	if isTranslatorParseError(err) {
		return cache.ErrorRequest, 0
	}
	if statusErr, ok := asType[*upstreamStatusError](err); ok {
		if statusErr.class == vlErrorQueryRejected {
			return cache.ErrorNever, 0
		}
		return cache.ErrorTransient, queryplan.FailBackend // a 5xx, 429 or queue timeout of VictoriaLogs
	}
	if isCanceledErr(err) {
		return cache.ErrorNever, 0
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return cache.ErrorTransient, queryplan.FailBackend // a refused or reset connection
	}
	return cache.ErrorNever, 0
}

// timedOutOrAborted reports a failure that depends on how long the query ran
// or on where its body broke: a timeout (VictoriaLogs' query deadline, a 504,
// a transport timeout, an expired deadline) or a body VictoriaLogs aborted
// after its headers. Such a failure is never held or shared: an immediate
// retry may answer.
func timedOutOrAborted(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := asType[*vlResponseAbortedError](err); ok {
		return true
	}
	if statusErr, ok := asType[*upstreamStatusError](err); ok {
		return statusErr.class == vlErrorDeadline || statusErr.status == http.StatusGatewayTimeout
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// windowFreeFailure reports a backend failure whose answer does not depend on
// the request's time window: a VictoriaLogs 5xx or 429 other than its query
// deadline, or a connection refused or reset. A timeout, an aborted body and
// anything else may depend on the window's size.
func windowFreeFailure(err error, typed *lokierr.Error) bool {
	if typed != nil {
		if typed.Msg() != lokierr.MsgBackendFailed {
			return false
		}
		if cause := typed.Cause(); cause != nil {
			err = cause
		}
	}
	if statusErr, ok := asType[*upstreamStatusError](err); ok {
		return statusErr.class != vlErrorDeadline && statusErr.status != http.StatusGatewayTimeout
	}
	var netErr net.Error
	return errors.As(err, &netErr) && !netErr.Timeout()
}

// flightLead is the flight a request leads, and what its answer was.
type flightLead struct {
	// fl is the flight the lead runs, f the flight it ended up with (fl when
	// it leads); key is what the flight was joined on.
	fl         cache.Flight
	f          *cache.Flight
	key        flightKey
	code       int
	body       []byte
	overflowed bool
	stored     bool
	flushed    bool
	// windowFreeProbe: the flight is the probe of a window-free key.
	windowFreeProbe bool
	// w is the tap, kept here so a lead is one allocation.
	w flightTap
	// The answer to build when one is needed (BuildAnswer).
	answerCode   int
	answerHeader http.Header
	answerClass  cache.ErrorClass
}

// BuildAnswer is the leader's answer, carrying its exact key when the flight
// was joined on a pre-key.
func (l *flightLead) BuildAnswer() *cache.Answer {
	a := answerOf(l.answerCode, l.answerHeader, l.body, l.answerClass)
	if l.key.r != nil {
		a.Key = l.key.exactKey()
	}
	return a
}

func (l *flightLead) release() {
	if l != nil {
		l.f.Release()
	}
}

func (l *flightLead) markStored() {
	if l != nil {
		l.stored = true
	}
}

// observe keeps what the response cache's capture saw: the status, and the
// body of a failure or of a success the cache did not store (a stale or
// partial answer), when it can be shared.
func (l *flightLead) observe(code int, body []byte, overflowed, flushed bool) {
	if l == nil {
		return
	}
	l.code, l.overflowed, l.flushed = code, overflowed, flushed
	limit := uncachedFlightShareBytes
	if code >= http.StatusBadRequest {
		limit = errorFlightMaxAnswerBytes
	}
	if len(body) > limit {
		l.overflowed = true // too large to share or hold
		return
	}
	if !overflowed && !flushed && !l.stored && len(body) > 0 &&
		(code >= http.StatusBadRequest || l.f.Joined() > 0) {
		l.body = append([]byte(nil), body...)
	}
}

// tap is w for a leader whose answer no response cache captures: it keeps the
// status, a failed body, and a success up to shareLimit bytes, and releases
// the followers once a success streams or outgrows it.
func (l *flightLead) tap(w http.ResponseWriter, shareLimit int) http.ResponseWriter {
	if l == nil {
		return w
	}
	l.w = flightTap{ResponseWriter: w, lead: l, limit: shareLimit}
	return &l.w
}

type flightTap struct {
	http.ResponseWriter
	lead  *flightLead
	limit int
}

func (t *flightTap) WriteHeader(code int) {
	if t.lead.code == 0 {
		t.lead.code = code
		defaultAnswerHeaders(t.Header(), code)
	}
	t.ResponseWriter.WriteHeader(code)
}

func (t *flightTap) Write(b []byte) (int, error) {
	l := t.lead
	if l.code == 0 {
		// The implicit 200 of a first Write: Loki's JSON, never sniffed (the
		// same defaults as compatCacheCaptureWriter.Write).
		l.code = http.StatusOK
		if strings.TrimSpace(t.Header().Get("Content-Type")) == "" {
			t.Header().Set("Content-Type", "application/json")
		}
		if strings.TrimSpace(t.Header().Get("X-Content-Type-Options")) == "" {
			t.Header().Set("X-Content-Type-Options", "nosniff")
		}
	}
	if !l.overflowed && !l.flushed && len(l.body) == 0 && l.code < http.StatusBadRequest && l.f.Joined() == 0 {
		// Nobody waits for this success: it is not kept, and a request that
		// joins later runs on its own.
		l.overflowed = true
	}
	if !l.overflowed && !l.flushed {
		limit := t.limit
		if l.code >= http.StatusBadRequest {
			limit = errorFlightMaxAnswerBytes
		}
		if len(l.body)+len(b) > limit {
			l.overflowed, l.body = true, nil
			if l.code < http.StatusBadRequest {
				l.f.Release()
			}
		} else {
			l.body = append(l.body, b...)
		}
	}
	return t.ResponseWriter.Write(b)
}

func (t *flightTap) Flush() {
	if t.lead.code < http.StatusBadRequest {
		// A streamed success: the followers run on their own, and nothing
		// more is kept.
		t.lead.flushed, t.lead.body = true, nil
		t.lead.f.Release()
	}
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (t *flightTap) Unwrap() http.ResponseWriter { return t.ResponseWriter }

// uncachedShareLimit is the largest success shared in memory with followers of
// a request no response cache keeps.
func (p *Proxy) uncachedShareLimit() int { return uncachedFlightShareBytes }

// flightKey is what identical requests join on. join is the exact response
// key, or for a request no response cache keys (a POST, -stream-response, a
// disabled cache) a cheap pre-key (method, endpoint, path, parameters, the
// form body's digest, tenant): the exact key, which reads the credentials'
// fingerprint and the response profile, is built only when another request
// with the same pre-key is in flight or a failure of it is held, and an
// answer is taken only by a request whose exact key is the leader's.
type flightKey struct {
	join string
	// Set for a pre-key: what the exact key is built from.
	p        *Proxy
	r        *http.Request
	endpoint string
	raw      string
	digest   uint64
	post     bool
	memo     string
}

// exactKey is the exact key of the request (join when join is exact).
func (k *flightKey) exactKey() string {
	if k.r == nil {
		return k.join
	}
	if k.memo == "" {
		// The raw parameters and digest as the request arrived; the rest
		// (credentials, profile, names, fences) from the request.
		var tail string
		if k.post {
			tail = digestTail(k.digest)
		}
		k.memo = errorFlightKeyVersion + k.p.buildResponseKey(k.r.Method+":", k.endpoint, k.r, k.raw, tail, k.p.flightNamesKey)
	}
	return k.memo
}

// uncachedFlightKey is the flight key of a read request the response cache
// does not keep (a POST, -stream-response, a disabled cache): a pre-key of its
// method, endpoint, path, every parameter (the form body's as a digest) and
// tenant, and its exact response key built when needed.
func (p *Proxy) uncachedFlightKey(endpoint string, r *http.Request) flightKey {
	if !compatCacheableEndpoint(endpoint) || strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") {
		return flightKey{}
	}
	switch r.Method {
	case http.MethodGet, http.MethodPost:
	default:
		return flightKey{}
	}
	var digest uint64
	post := r.Method == http.MethodPost
	if post {
		if r.PostForm == nil {
			return flightKey{} // the body was not read as a form: the request is not known
		}
		digest = formDigest(r.PostForm)
	}
	raw, tenant := r.URL.RawQuery, r.Header.Get("X-Scope-OrgID")
	var b strings.Builder
	b.Grow(len(errorFlightKeyVersion) + len(r.Method) + len(endpoint) + len(r.URL.Path) + len(raw) + len(tenant) + 32)
	b.WriteString(errorFlightKeyVersion)
	b.WriteString(r.Method)
	b.WriteByte(':')
	b.WriteString(endpoint)
	b.WriteByte(':')
	b.WriteString(r.URL.Path)
	b.WriteByte('?')
	b.WriteString(raw)
	if post {
		var n [24]byte
		b.Write(strconv.AppendUint(append(n[:0], "&post:"...), digest, 36))
	}
	b.WriteByte('@')
	b.WriteString(tenant)
	return flightKey{join: b.String(), p: p, r: r, endpoint: endpoint, raw: raw, digest: digest, post: post}
}

// formSeed keys formDigest: random per process, so no client can aim two
// bodies at one digest; flight keys never leave the process.
var formSeed = maphash.MakeSeed()

// formDigest is a 64-bit digest of a form body: every name and value, in name
// order, each delimited by its length.
func formDigest(v url.Values) uint64 {
	var room [16]string
	names := room[:0]
	for name := range v {
		names = append(names, name)
	}
	slices.Sort(names)
	var h maphash.Hash
	h.SetSeed(formSeed)
	var n [binary.MaxVarintLen64]byte
	for _, name := range names {
		h.Write(n[:binary.PutUvarint(n[:], uint64(len(name)))])
		h.WriteString(name)
		values := v[name]
		h.Write(n[:binary.PutUvarint(n[:], uint64(len(values)))])
		for _, value := range values {
			h.Write(n[:binary.PutUvarint(n[:], uint64(len(value)))])
			h.WriteString(value)
		}
	}
	return h.Sum64()
}

// digestTail is a form digest as the exact key writes it after the URL query.
func digestTail(d uint64) string {
	return "&post:" + strconv.FormatUint(d, 36)
}

// serveFlight runs r through the flight of key: it answers a held failure,
// waits for a leader in flight, or leads a new flight with run. run gets a
// nil lead when it runs as a follower or on its own.
func (p *Proxy) serveFlight(endpoint, route string, w http.ResponseWriter, r *http.Request, k flightKey, start time.Time, run func(http.ResponseWriter, *http.Request, *flightLead)) {
	ef := p.errorFlights
	if ef == nil || k.join == "" {
		run(w, r, nil)
		return
	}
	key := k.join
	if k.r != nil && ef.Peek(heldHintKey(key)) != nil {
		// A failure of this pre-key is held: read it under the exact key.
		if held := ef.Peek(k.exactKey()); held != nil {
			p.errorFlightCounters.held[held.Class].Add(1)
			p.writeFlightAnswer(endpoint, route, w, r, held, start)
			return
		}
	}
	if now := cache.Now().UnixNano(); p.windowFreeSeenUntil.Load() > now {
		// A backend failure that does not depend on the window is held for
		// the request whatever its window (a refresh moves end=now).
		wk := p.windowFreeKey(endpoint, r)
		if p.windowFreeHeldUntil.Load() > now {
			if held := ef.Peek(wk); held != nil {
				p.errorFlightCounters.held[held.Class].Add(1)
				p.writeFlightAnswer(endpoint, route, w, r, held, start)
				return
			}
		}
		// Such a failure of this very query was held moments ago: the
		// backend may still be down. Requests of every window join one probe
		// instead of each asking VictoriaLogs when the hold expires; the
		// probe's failure answers them all, its success ends the probing.
		if ef.Peek(probeMarkerKey(wk)) != nil && p.serveWindowFreeProbe(endpoint, route, w, r, wk, start, run) {
			return
		}
	}
	for round := 0; round < maxFlightRounds; round++ {
		lead := &flightLead{key: k}
		held, f, leads := ef.Join(key, &lead.fl)
		if held != nil {
			p.errorFlightCounters.held[held.Class].Add(1)
			p.writeFlightAnswer(endpoint, route, w, r, held, start)
			return
		}
		if leads {
			lead.f = f
			p.leadFlight(endpoint, w, r, lead, run)
			return
		}
		outcome, answer, ok := f.Wait(r.Context())
		if !ok {
			// This client went away while it waited: its own cancellation.
			status := p.fail(w, r, r.Context().Err())
			p.metrics.RecordRequestWithRoute(endpoint, route, status, time.Since(start))
			return
		}
		if answer != nil && answer.Key != "" && answer.Key != lead.key.exactKey() {
			// Same pre-key, another exact request (other credentials or
			// profile): the leader's answer is not this request's.
			run(w, r, nil)
			return
		}
		switch outcome {
		case cache.OutcomeFailed:
			p.errorFlightCounters.coalesced[answer.Class].Add(1)
			p.writeFlightAnswer(endpoint, route, w, r, answer, start)
			return
		case cache.OutcomeSucceeded:
			p.errorFlightCounters.succeeded.Add(1)
			p.writeFlightAnswer(endpoint, route, w, r, answer, start)
			return
		case cache.OutcomeStored:
			p.errorFlightCounters.succeeded.Add(1)
			run(w, r, nil)
			return
		case cache.OutcomeReleased:
			run(w, r, nil)
			return
		}
		// The leader handed nothing (it went away, or failed in a class
		// never shared): start again.
	}
	run(w, r, nil)
}

// serveWindowFreeProbe runs r through the probe flight of its window-free key
// wk; it reports whether r was answered (as the probe, or by the probe's
// window-free failure).
func (p *Proxy) serveWindowFreeProbe(endpoint, route string, w http.ResponseWriter, r *http.Request, wk string, start time.Time, run func(http.ResponseWriter, *http.Request, *flightLead)) bool {
	probe := &flightLead{key: flightKey{join: wk}, windowFreeProbe: true}
	held, f, leads := p.errorFlights.Join(wk, &probe.fl)
	if held != nil {
		p.errorFlightCounters.held[held.Class].Add(1)
		p.writeFlightAnswer(endpoint, route, w, r, held, start)
		return true
	}
	if leads {
		probe.f = f
		p.leadFlight(endpoint, w, r, probe, run)
		return true
	}
	outcome, answer, ok := f.Wait(r.Context())
	if !ok {
		status := p.fail(w, r, r.Context().Err())
		p.metrics.RecordRequestWithRoute(endpoint, route, status, time.Since(start))
		return true
	}
	if outcome == cache.OutcomeFailed {
		p.errorFlightCounters.coalesced[answer.Class].Add(1)
		p.writeFlightAnswer(endpoint, route, w, r, answer, start)
		return true
	}
	return false // the backend answered the probe: this request runs as usual
}

// windowFreeProbeTTLs is how many transient TTLs after a window-free failure
// was held its query still probes: with the 1 s default, 10 s, about the
// circuit breaker's default open time, while an outage lasts. A probe that
// the backend answers ends it at once.
const windowFreeProbeTTLs = 10

// probeMarkerKey marks a window-free key whose query probes.
func probeMarkerKey(wk string) string { return wk + "\x00probe" }

// leadFlight runs r as the leader of f and ends f with its answer. Only a
// leader whose own request ended (its client went away or ran out of time)
// sends the followers back to start again; any other answer it cannot share
// lets them run on their own at once, so a follower never waits longer than
// it would have run.
func (p *Proxy) leadFlight(endpoint string, w http.ResponseWriter, r *http.Request, lead *flightLead, run func(http.ResponseWriter, *http.Request, *flightLead)) {
	key, f := lead.key.join, lead.f
	outcome := cache.OutcomeReleased
	var (
		build      cache.AnswerBuilder
		pol        cache.Policy
		windowFree bool
	)
	defer func() {
		// A panic in the handler ends the flight too: its followers run on their own.
		answer, ttl := p.errorFlights.Finish(key, f, outcome, build, pol)
		if ttl > 0 {
			p.errorFlightCounters.stored[answer.Class].Add(1)
			if lead.key.r != nil {
				// Requests with this pre-key look for the held failure under
				// their exact key while it is held.
				p.errorFlights.Hold(heldHintKey(key), heldHint, ttl)
			}
			if windowFree {
				p.holdWindowFree(endpoint, r, answer, ttl)
			}
		}
	}()
	run(w, r, lead)
	if r.Context().Err() != nil {
		outcome = cache.OutcomeUnshared // the leader's own cancellation or deadline: nothing to hand
		return
	}
	code := lead.code
	if code == 0 {
		code = http.StatusOK
	}
	header := w.Header()
	switch {
	case code == http.StatusOK && lead.stored:
		outcome = cache.OutcomeStored
	case code == http.StatusOK:
		if !lead.overflowed && !lead.flushed && lead.body != nil && len(header.Values("Set-Cookie")) == 0 {
			lead.answerCode, lead.answerHeader, lead.answerClass = code, header, cache.ErrorNever
			outcome, build = cache.OutcomeSucceeded, lead
		}
	case code >= http.StatusBadRequest && !lead.overflowed:
		var reach queryplan.Failures
		pol, reach = p.errorPolicy(endpoint, r)
		var class cache.ErrorClass
		if class, windowFree = heldFailureClass(r.Context(), code, reach); class != cache.ErrorNever {
			lead.answerCode, lead.answerHeader, lead.answerClass = code, header, class
			outcome, build = cache.OutcomeFailed, lead
		}
	}
	if lead.windowFreeProbe && (outcome != cache.OutcomeFailed || !windowFree) {
		// A probe hands its waiters only a failure of every window; anything
		// else is its own window's answer, and the backend answered: the
		// query stops probing.
		outcome, build = cache.OutcomeReleased, nil
		p.errorFlights.Drop(probeMarkerKey(lead.key.join))
	}
}

// heldFailureClass is the class of the failure the request answered with
// status code, or ErrorNever when the answer is not the one failure the
// error layer recorded or the plan rules its class out; windowFree reports a
// failure that does not depend on the request's time window.
func heldFailureClass(ctx context.Context, code int, reach queryplan.Failures) (class cache.ErrorClass, windowFree bool) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return cache.ErrorNever, false
	}
	rt.mu.Lock()
	f := rt.failure
	rt.mu.Unlock()
	if f.count == 0 || f.mixed || f.status != code || !reach.Has(f.need) {
		return cache.ErrorNever, false
	}
	return f.class, f.windowFree
}

// flightNamesKey is responseNamesCacheKey from the stream label names last
// known, read without starting their refresh: building a flight key never
// asks VictoriaLogs anything (a request refused from its text, a burst during
// an outage). Every request of a flight reads the names the same way; one
// that reads newer names gets another key and runs on its own.
func (p *Proxy) flightNamesKey(r *http.Request, query string) string {
	if names := p.plainStreamLabelNamesFrom(r.Context(), query, p.knownTenantStreamLabelNames); len(names) > 0 {
		return streamLabelsCacheKey(names)
	}
	return ""
}

// windowParams are the parameters that place a request in time.
var windowParams = [...]string{"start", "end", "time", "since"}

// windowFreeKey is the flight key of r without its time window.
func (p *Proxy) windowFreeKey(endpoint string, r *http.Request) string {
	var v url.Values
	if r.Method == http.MethodPost && r.Form != nil {
		v = make(url.Values, len(r.Form))
		for k, vs := range r.Form {
			v[k] = vs
		}
	} else {
		v = r.URL.Query()
	}
	for _, name := range windowParams {
		v.Del(name)
	}
	return errorFlightKeyVersion + "nowindow:" + p.buildResponseKey(r.Method+":", endpoint, r, v.Encode(), "", p.flightNamesKey)
}

// holdWindowFree holds a backend failure that does not depend on the window
// for every window of the request, for the same TTL.
func (p *Proxy) holdWindowFree(endpoint string, r *http.Request, a *cache.Answer, ttl time.Duration) {
	wk := p.windowFreeKey(endpoint, r)
	until := p.errorFlights.Hold(wk, a, ttl)
	probe := p.errorFlights.Hold(probeMarkerKey(wk), heldHint, ttl*windowFreeProbeTTLs)
	raiseTo(&p.windowFreeHeldUntil, until.UnixNano())
	raiseTo(&p.windowFreeSeenUntil, probe.UnixNano())
}

// raiseTo sets v to at least t.
func raiseTo(v *atomic.Int64, t int64) {
	for {
		cur := v.Load()
		if t <= cur || v.CompareAndSwap(cur, t) {
			return
		}
	}
}

func answerOf(code int, header http.Header, body []byte, class cache.ErrorClass) *cache.Answer {
	return &cache.Answer{Status: code, Header: flightHeader(header), Body: body, Class: class}
}

// defaultAnswerHeaders gives an answer about to be written without a
// Content-Type Loki's for its status (text/plain with nosniff for an error,
// JSON otherwise), so no body is ever sniffed.
func defaultAnswerHeaders(h http.Header, code int) {
	if h.Get("Content-Type") == "" {
		if code >= http.StatusBadRequest {
			h.Set("Content-Type", "text/plain; charset=utf-8")
		} else {
			h.Set("Content-Type", "application/json")
		}
	}
	if h.Get("X-Content-Type-Options") == "" {
		h.Set("X-Content-Type-Options", "nosniff")
	}
}

// heldHint marks a pre-key with a held failure; it is never answered.
var heldHint = &cache.Answer{}

func heldHintKey(join string) string { return join + "\x00held" }

// flightPerRequestHeaders are the response headers each request writes for
// itself (framing, encoding, the client's rate limit, the debug counters): a
// shared answer never carries the leader's.
var flightPerRequestHeaders = canonicalSet(map[string]bool{
	"Content-Length":        true,
	"Content-Encoding":      true,
	"Transfer-Encoding":     true,
	"Vary":                  true,
	"Date":                  true,
	"Connection":            true,
	"Set-Cookie":            true,
	"X-Ratelimit-Limit":     true,
	"X-Ratelimit-Remaining": true,
	vlBackendCallsHeader:    true,
	vlCacheResultHeader:     true,
})

// canonicalSet is set with its names in canonical header form, the form
// http.Header keys hold.
func canonicalSet(set map[string]bool) map[string]bool {
	out := make(map[string]bool, len(set))
	for name := range set {
		out[http.CanonicalHeaderKey(name)] = true
	}
	return out
}

// flightHeader is the part of a leader's response header a follower answers
// with.
func flightHeader(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for name, values := range h {
		if !flightPerRequestHeaders[http.CanonicalHeaderKey(name)] {
			out[name] = append([]string(nil), values...)
		}
	}
	return out
}

// writeFlightAnswer writes an answer another request produced, as it wrote it.
func (p *Proxy) writeFlightAnswer(endpoint, route string, w http.ResponseWriter, r *http.Request, a *cache.Answer, start time.Time) {
	setCacheResult(r.Context(), "hit")
	h := w.Header()
	for name, values := range a.Header {
		if name != "Content-Type" && name != "X-Content-Type-Options" {
			h[name] = values
		}
	}
	// The body is another request's answer, the client's query text possibly
	// echoed in it: it is written only as Loki's text/plain error (nosniff) or
	// as JSON, never sniffed.
	setReplayContentType(w, a.Status)
	w.WriteHeader(a.Status)
	if len(a.Body) > 0 {
		_, _ = w.Write(a.Body)
	}
	elapsed := time.Since(start)
	p.metrics.RecordRequestWithRoute(endpoint, route, a.Status, elapsed)
	if endpoint == "query" || endpoint == "query_range" {
		p.queryTracker.Record(endpoint, r.FormValue("query"), elapsed, a.Status >= http.StatusBadRequest)
	}
}

// clearHeldErrors drops every held failure: a configuration change may answer
// the same request differently.
func (p *Proxy) clearHeldErrors() {
	if p.errorFlights != nil {
		p.errorFlights.Clear()
	}
}

// errorFlightStats are the flight counters and the held answers' size.
func (p *Proxy) errorFlightStats() metrics.ErrorFlightStats {
	var s metrics.ErrorFlightStats
	for i, c := range cache.ErrorClasses() {
		s.Classes[i] = c.String()
		s.Held[i] = p.errorFlightCounters.held[c].Load()
		s.Coalesced[i] = p.errorFlightCounters.coalesced[c].Load()
		s.Stored[i] = p.errorFlightCounters.stored[c].Load()
	}
	s.Succeeded = p.errorFlightCounters.succeeded.Load()
	if p.errorFlights != nil {
		entries, bytes := p.errorFlights.Len()
		s.Entries, s.Bytes = int64(entries), int64(bytes)
	}
	return s
}
