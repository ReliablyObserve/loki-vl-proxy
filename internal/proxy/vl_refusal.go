package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// VictoriaLogs refusals: prediction, a long hold, and peer sync.
//
// A query VictoriaLogs refuses before reading any data (a parse error, a stats
// shape it does not run, a query over its -search.maxQueryLen) is refused the
// same way every time the same text reaches the same VictoriaLogs release.
//
//   - Prediction. Before a call leaves the proxy (vlGetInner, vlPostSend, after
//     the outbound guard), logsql.PredictRefusal reads the text, the endpoint and
//     the detected release; a call it refuses is answered with VictoriaLogs'
//     status and a message its error classification reads the same way, without
//     the call, so every caller takes the route it takes for VictoriaLogs' own
//     answer (its fallback, or Loki's answer for the failure). A call VictoriaLogs
//     would not answer at all (its handler panics on the query's reprint) gets the
//     transport failure a dropped connection gives. Nothing is predicted while the
//     release is unknown: the call is sent and its answer classified.
//   - Hold. Such a failure (predicted or answered) is of the ErrorRefusal class:
//     held for -error-cache-vl-refusal-ttl under the request's flight key and its
//     window-free key (a refresh that moves end=now is refused the same way), both
//     carrying the configuration epoch and the VictoriaLogs release, so a release
//     change or a reload never answers an earlier verdict.
//   - Peer sync. A refusal VictoriaLogs answered (a peer would pay the same call;
//     a predicted one costs a peer nothing, so it is not sent) and held for at
//     least refusalSyncMinTTL is pushed to every peer through the peer cache's
//     everywhere class. A peer holds it only when it runs the same proxy build,
//     talks to the same VictoriaLogs release and has the same routing
//     configuration; the key keeps the tenant and the response profile, and an
//     answer computed with forwarded credentials stays on its replica.

// refusalSyncMinTTL is the shortest hold pushed to peers (the fleet rule): a
// push reaches them in about a second, so they are covered for most of a hold
// of thirty seconds or more.
const refusalSyncMinTTL = 30 * time.Second

// refusalSyncRate bounds the refusals one replica pushes per second (each to
// every peer): a refusal is pushed once per hold, so the bound only matters
// for a flood of distinct refused queries, whose surplus is held locally.
const refusalSyncRate float64 = 20

// refusalSyncJitter is the share of the hold a peer adds to it at random, so
// the replica that learned a refusal (or the first peer to see its copy
// expire) asks VictoriaLogs again and pushes the new verdict before the other
// copies expire: about one call per hold for the fleet.
const refusalSyncJitter = 0.15

// vlRelease is the VictoriaLogs release the proxy detected (not Known before).
func (p *Proxy) vlRelease() logsql.Version {
	if v := p.vlReleaseSeen.Load(); v != nil {
		return *v
	}
	return logsql.Version{}
}

// noteVLRelease records the detected release semver and rebuilds the flight
// key prefix with it.
func (p *Proxy) noteVLRelease(semver string) {
	v := logsql.ParseVersion(semver)
	if !v.Known {
		return
	}
	if cur := p.vlReleaseSeen.Load(); cur != nil && *cur == v {
		return
	}
	p.vlReleaseSeen.Store(&v)
	p.refreshFlightPrefix()
}

// vlQueryLen is VictoriaLogs' -search.maxQueryLen as its last refusal named it,
// for the release it was named by, until when it is trusted.
type vlQueryLen struct {
	bytes   int
	release logsql.Version
	until   int64
}

// learnedMaxQueryLen is VictoriaLogs' query length limit when a refusal of
// release v named it within the refusal TTL; zero otherwise (an operator may
// change it without a release change, so it is learned again after that).
func (p *Proxy) learnedMaxQueryLen(v logsql.Version) int {
	l := p.vlMaxQueryLen.Load()
	if l == nil || l.release != v || cache.Now().UnixNano() >= l.until {
		return 0
	}
	return l.bytes
}

// vlMaxQueryLenMarker is how VictoriaLogs names its limit in a refusal.
const vlMaxQueryLenMarker = "-search.maxQueryLen="

// noteVLRefusalMessage learns VictoriaLogs' query length limit from the raw
// message of a refusal it answered.
func (p *Proxy) noteVLRefusalMessage(raw string) {
	i := strings.Index(raw, vlMaxQueryLenMarker)
	if i < 0 || !strings.Contains(raw[:i], "length cannot exceed") {
		return
	}
	digits := raw[i+len(vlMaxQueryLenMarker):]
	n := 0
	for n < len(digits) && digits[n] >= '0' && digits[n] <= '9' {
		n++
	}
	limit, err := strconv.Atoi(digits[:n])
	if err != nil || limit <= 0 {
		return
	}
	v := p.vlRelease()
	if !v.Known {
		return
	}
	if cur := p.vlMaxQueryLen.Load(); cur != nil && cur.bytes == limit && cur.release == v && cache.Now().UnixNano() < cur.until {
		// Known already (a predicted refusal names it too): the fact expires
		// with the TTL it was learned with, and the next call after that is
		// sent and learns it again.
		return
	}
	ttl := p.errorCacheRefusalTTL
	if ttl <= 0 {
		ttl = DefaultErrorCacheVLRefusalTTL
	}
	p.vlMaxQueryLen.Store(&vlQueryLen{bytes: limit, release: v, until: cache.Now().Add(ttl).UnixNano()})
}

// isVLRefusalMessage reports a VictoriaLogs answer (status, raw message) that
// the same query text always gets from the same release, before any data is
// read: a parse error of the query or an argument, a stats shape
// GetStatsLabelsAddGroupingByTime refuses, a query over -search.maxQueryLen.
// A failure while executing (memory limits, "cannot execute query") is not.
func isVLRefusalMessage(status int, raw string) bool {
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return false
	}
	msg := strings.TrimSpace(raw)
	if reason, ok := stripVLParseEcho(msg); ok {
		msg = "cannot parse query arg:" + reason
	}
	msg = strings.ReplaceAll(msg, "`", "")
	if hasAnyPrefix(msg, vlQueryRejectedPrefixes) || strings.HasPrefix(msg, "cannot parse query arg:") {
		return true
	}
	switch {
	case strings.HasPrefix(msg, "missing ") && strings.HasSuffix(strings.SplitN(msg, " in the query", 2)[0], "stats ... pipe"),
		strings.HasPrefix(msg, "the query arg length cannot exceed -search.maxQueryLen="),
		strings.HasPrefix(msg, "the extra_filters arg length cannot exceed -search.maxQueryLen="),
		strings.HasPrefix(msg, "the extra_stream_filters arg length cannot exceed -search.maxQueryLen="):
		return true
	case strings.HasPrefix(msg, "the "):
		// The stats shape refusals name a pipe, then say why; the pipe's text
		// comes first, so only the reason after it is read.
		for _, reason := range vlStatsShapeReasons {
			if strings.Contains(msg, reason) {
				return true
			}
		}
	}
	return false
}

// vlStatsShapeReasons end the stats shape refusals of
// GetStatsLabelsAddGroupingByTime (VictoriaLogs v1.49.0 to v1.54.0).
var vlStatsShapeReasons = []string{
	"cannot be put in front of",
	"pipe cannot be put after",
	"isn't allowed in range queries",
	"must contain the same list of fields as | stats pipe",
}

// windowFreeRefusal reports a refusal that does not depend on the request's
// window: a parse error of the query itself and a stats shape refusal (the
// pipes the proxy writes for a query). A refused start, end, time, step or
// offset arg, and a length refusal (the text can grow with the window, as an
// in() list built from the window's data does), are held for the exact
// request only.
func windowFreeRefusal(err error, typed *lokierr.Error) bool {
	for _, e := range []error{err, typedCause(typed)} {
		if e == nil {
			continue
		}
		if statusErr, ok := asType[*upstreamStatusError](e); ok && statusErr.refusal {
			return statusErr.windowFree
		}
	}
	return false
}

// isWindowFreeRefusalMessage is isVLRefusalMessage for a refusal that does
// not depend on the request's window (windowFreeRefusal).
func isWindowFreeRefusalMessage(status int, raw string) bool {
	if !isVLRefusalMessage(status, raw) {
		return false
	}
	msg := strings.TrimSpace(raw)
	if strings.Contains(msg, vlMaxQueryLenMarker) {
		return false
	}
	if reason, ok := stripVLParseEcho(msg); ok {
		return !strings.Contains(reason, vlMaxQueryLenMarker)
	}
	msg = strings.ReplaceAll(msg, "`", "")
	for _, arg := range vlArgumentRefusalPrefixes {
		if strings.HasPrefix(msg, arg) {
			return false
		}
	}
	return true
}

// vlArgumentRefusalPrefixes start VictoriaLogs' refusals of an argument other
// than the query text (they may depend on the window).
var vlArgumentRefusalPrefixes = []string{
	"cannot parse start=", "cannot parse end=", "cannot parse time=", "'step' must be bigger than zero",
	"cannot parse duration from the arg", "missing 'field' query arg", "query arg cannot be empty",
}

// vlPredictedCrashError is a call not sent because VictoriaLogs' handler
// panics on it (the connection drops without an answer): it fails as that
// dropped connection does, and is held as a refusal.
type vlPredictedCrashError struct {
	rule string
	err  error
}

func (e *vlPredictedCrashError) Error() string { return e.err.Error() }

func (e *vlPredictedCrashError) Unwrap() error { return e.err }

// vlRefusalFailure reports a failure that is a VictoriaLogs refusal (answered
// or predicted), wherever it is in err's tree or the typed error's cause.
func vlRefusalFailure(err error, typed *lokierr.Error) bool {
	for _, e := range []error{err, typedCause(typed)} {
		if e == nil {
			continue
		}
		if _, ok := asType[*vlPredictedCrashError](e); ok {
			return true
		}
		if statusErr, ok := asType[*upstreamStatusError](e); ok && statusErr.refusal {
			return true
		}
	}
	return false
}

func typedCause(typed *lokierr.Error) error {
	if typed == nil {
		return nil
	}
	return typed.Cause()
}

// predictVLRefusal answers a call VictoriaLogs refuses before reading data
// without sending it (see the top of this file): a response with VictoriaLogs'
// status and a message classified as its own, or for a call VictoriaLogs does
// not answer the transport failure a dropped connection gives. ok is false for
// a call to send.
func (p *Proxy) predictVLRefusal(ctx context.Context, method, path string, params url.Values) (resp *http.Response, err error, ok bool) {
	if !p.vlRefusalPrediction {
		return nil, nil, false
	}
	v := p.vlRelease()
	query := params.Get("query")
	if query == "" {
		return nil, nil, false
	}
	in := logsql.RefusalInput{Path: path, Query: query, MaxQueryLen: p.learnedMaxQueryLen(v),
		ExtraFilters: params["extra_filters"], ExtraStreamFilters: params["extra_stream_filters"]}
	if path == logsql.PathQuery {
		if limit, perr := strconv.Atoi(params.Get("limit")); perr == nil && limit > 0 {
			in.Limit = true
		}
	}
	refusal, predicted := logsql.PredictRefusal(in, v)
	if !predicted || p.breaker != nil && p.breaker.State() != "closed" {
		// While the circuit breaker probes VictoriaLogs (half open), a call it
		// let through is a probe: it is sent, so the breaker learns from it.
		return nil, nil, false
	}
	p.countPredictedRefusal(refusal.Rule)
	if rt := getRequestTelemetry(ctx); rt != nil {
		rt.mu.Lock()
		rt.predictedRefusal = true
		rt.mu.Unlock()
	}
	if refusal.Crash {
		u := backendRequestURL(p.backend, path)
		return nil, &vlPredictedCrashError{rule: refusal.Rule, err: &url.Error{Op: method, URL: u.String(), Err: io.EOF}}, true
	}
	recordVLRefusalCall(ctx, path, query)
	body := refusal.Message
	header := http.Header{"Content-Type": {"text/plain; charset=utf-8"}, predictedRefusalHeader: {refusal.Rule}}
	if refusal.Status == http.StatusUnprocessableEntity {
		// The stats endpoints answer Prometheus' error shape.
		b, _ := json.Marshal(map[string]string{"status": "error", "errorType": "422", "error": refusal.Message})
		body = string(b)
		header.Set("Content-Type", "application/json")
	}
	return &http.Response{
		Status:        strconv.Itoa(refusal.Status) + " " + http.StatusText(refusal.Status),
		StatusCode:    refusal.Status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}, nil, true
}

// predictedRefusalHeader marks a predicted answer inside the proxy (never
// written to a client: callers read VictoriaLogs answers' status and body).
const predictedRefusalHeader = "X-Loki-Vl-Predicted-Refusal"

// predictedVLAnswer reports a predicted refusal or crash: no call reached
// VictoriaLogs, so the circuit breaker records neither a success nor a failure.
func predictedVLAnswer(resp *http.Response, err error) bool {
	if err != nil {
		_, ok := asType[*vlPredictedCrashError](err)
		return ok
	}
	return resp != nil && resp.Header.Get(predictedRefusalHeader) != ""
}

// countPredictedRefusal counts a predicted refusal by its rule.
func (p *Proxy) countPredictedRefusal(rule string) {
	for i, r := range logsql.RefusalRules() {
		if r == rule && i < len(p.vlRefusalsPredicted) {
			p.vlRefusalsPredicted[i].Add(1)
			return
		}
	}
}

// predictedRefusal reports a request one of whose calls was answered by a
// predicted refusal.
func predictedRefusal(ctx context.Context) bool {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.predictedRefusal
}

// refusalQuery names the queries with a held window-free refusal.
type refusalQuery struct{ endpoint, query string }

// refusalQuerySet is the queries with a held window-free refusal and until
// when, so only their requests build a window-free key; bounded, expired
// entries are dropped when room is needed.
type refusalQuerySet struct {
	mu sync.RWMutex
	m  map[refusalQuery]int64
}

func (s *refusalQuerySet) add(endpoint, query string, until int64, max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[refusalQuery]int64)
	}
	if len(s.m) >= max {
		now := cache.Now().UnixNano()
		for k, u := range s.m {
			if u <= now {
				delete(s.m, k)
			}
		}
		if len(s.m) >= max {
			return // the request's own flight key still holds it
		}
	}
	k := refusalQuery{endpoint, query}
	if s.m[k] < until {
		s.m[k] = until
	}
}

func (s *refusalQuerySet) has(endpoint, query string, now int64) bool {
	s.mu.RLock()
	until, ok := s.m[refusalQuery{endpoint, query}]
	s.mu.RUnlock()
	return ok && until > now
}

func (s *refusalQuerySet) clear() {
	s.mu.Lock()
	s.m = nil
	s.mu.Unlock()
}

// heldRefusal is the refusal held for r's query whatever its window, if any.
func (p *Proxy) heldRefusal(prefix, endpoint string, r *http.Request) *cache.Answer {
	now := cache.Now().UnixNano()
	if p.refusalSeenUntil.Load() <= now || !p.refusalQueries.has(endpoint, r.FormValue("query"), now) {
		return nil
	}
	return p.errorFlights.Peek(p.windowFreeKey(prefix, endpoint, r))
}

// holdRefusal holds a refusal r's flight ended with for every window of r's
// query, and pushes it to the peers when VictoriaLogs answered it.
func (p *Proxy) holdRefusal(k flightKey, endpoint string, r *http.Request, a *cache.Answer, ttl time.Duration, windowFree bool) {
	predicted := predictedRefusal(r.Context())
	if !windowFree {
		// Held under the request's flight key by Finish; pushed under it when
		// that key is the exact one (a pre-key carries a per-process digest).
		if !predicted && k.r == nil {
			p.syncRefusal(endpoint, "", strings.TrimPrefix(k.join, k.prefix), a, cache.Now().Add(ttl))
		}
		return
	}
	wk := p.windowFreeKey(k.prefix, endpoint, r)
	until := p.errorFlights.Hold(wk, a, ttl)
	if until.IsZero() {
		return
	}
	p.refusalQueries.add(endpoint, r.FormValue("query"), until.UnixNano(), p.errorFlightsMaxEntries())
	raiseTo(&p.refusalSeenUntil, until.UnixNano())
	if !predicted {
		p.syncRefusal(endpoint, r.FormValue("query"), strings.TrimPrefix(wk, k.prefix), a, until)
	}
}

func (p *Proxy) errorFlightsMaxEntries() int {
	if p.errorCacheMaxEntries > 0 {
		return p.errorCacheMaxEntries
	}
	return DefaultErrorCacheMaxEntries
}

// refusalSync is a held refusal as it is pushed to the peers.
type refusalSync struct {
	Build    string `json:"b"`
	Release  string `json:"r"`
	Scope    string `json:"s"`
	Endpoint string `json:"e"`
	// Query is the query of a window-free refusal; empty for one held for the
	// exact request.
	Query   string      `json:"q,omitempty"`
	Key     string      `json:"k"`
	Expires int64       `json:"x"`
	Status  int         `json:"c"`
	Header  http.Header `json:"h,omitempty"`
	Body    []byte      `json:"y,omitempty"`
}

// refusalSyncKeyPrefix starts the peer keys of held refusals.
const refusalSyncKeyPrefix = cache.EverywherePrefix + "vlref:v1:"

// routingScope is the digest of the routing configuration (backend, tenant
// map, label translation), the part of the configuration a peer must share.
func (p *Proxy) routingScope() string {
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return p.routingNamespace
}

// syncRefusal pushes a refusal VictoriaLogs answered to every peer when it is
// held long enough for the push to protect them (refusalSyncMinTTL), the
// release is known, and the answer was computed without forwarded
// credentials; within the replica's send budget.
func (p *Proxy) syncRefusal(endpoint, query, key string, a *cache.Answer, until time.Time) {
	if p.peerCache == nil || !p.errorCachePeerSync || until.Sub(cache.Now()) < refusalSyncMinTTL || !peerPushAllowed(key) {
		return
	}
	v := p.vlRelease()
	if !v.Known {
		return
	}
	if !p.refusalSyncBudget.take(cache.Now()) {
		p.peerSyncCounts[1].Add(1)
		return
	}
	msg := refusalSync{
		Build: p.buildVersion, Release: v.String(), Scope: p.routingScope(), Endpoint: endpoint, Query: query, Key: key,
		Expires: until.UnixMilli(), Status: a.Status, Header: peerAnswerHeader(a.Header), Body: a.Body,
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return
	}
	sum := sha256.Sum256([]byte(msg.Build + "\x00" + msg.Release + "\x00" + msg.Scope + "\x00" + key))
	if p.peerCache.Broadcast(refusalSyncKeyPrefix+hex.EncodeToString(sum[:16]), value, until.Sub(cache.Now())) {
		p.peerSyncCounts[0].Add(1)
	}
}

// receiveRefusal holds a refusal a peer pushed, when this replica runs the
// same build against the same VictoriaLogs release with the same routing
// configuration; its hold is the peer's plus a random share
// (refusalSyncJitter), so one replica asks VictoriaLogs again first.
func (p *Proxy) receiveRefusal(key string, value []byte, _ time.Duration) {
	if !strings.HasPrefix(key, refusalSyncKeyPrefix) || p.errorFlights == nil {
		return
	}
	if p.errorCacheDisabled || p.errorCacheRefusalTTL <= 0 || !p.errorCachePeerSync {
		// This replica holds no refusal, or takes none from its peers.
		p.peerSyncCounts[3].Add(1)
		return
	}
	var msg refusalSync
	if err := json.Unmarshal(value, &msg); err != nil {
		p.peerSyncCounts[3].Add(1)
		return
	}
	v := p.vlRelease()
	ttl := time.UnixMilli(msg.Expires).Sub(cache.Now())
	if !v.Known || msg.Release != v.String() || msg.Build != p.buildVersion || msg.Scope != p.routingScope() ||
		ttl <= 0 || !peerPushAllowed(msg.Key) ||
		msg.Status < http.StatusBadRequest || msg.Status > 599 || msg.Endpoint == "" || len(msg.Body) > errorFlightMaxAnswerBytes {
		p.peerSyncCounts[3].Add(1)
		return
	}
	// Never longer than this replica's own hold, plus the share that lets one
	// replica ask again first.
	if ttl > p.errorCacheRefusalTTL {
		ttl = p.errorCacheRefusalTTL
	}
	ttl += time.Duration(float64(ttl) * refusalSyncJitter * rand.Float64()) // #nosec G404 -- spreads expiries, not a secret
	a := &cache.Answer{Status: msg.Status, Header: peerAnswerHeader(msg.Header), Body: msg.Body, Class: cache.ErrorRefusal}
	until := p.errorFlights.Hold(p.flightPrefix()+msg.Key, a, ttl)
	if until.IsZero() {
		p.peerSyncCounts[3].Add(1)
		return
	}
	if msg.Query != "" {
		// A window-free refusal: its query's requests of every window read it.
		p.refusalQueries.add(msg.Endpoint, msg.Query, until.UnixNano(), p.errorFlightsMaxEntries())
		raiseTo(&p.refusalSeenUntil, until.UnixNano())
	}
	p.peerSyncCounts[2].Add(1)
}

// peerAnswerHeaders are the only headers a held refusal from a peer answers
// with: an error answer's content headers. Anything else a peer sends (a
// cookie, a redirect, CORS) is dropped.
var peerAnswerHeaders = []string{"Content-Type", "X-Content-Type-Options"}

// peerAnswerHeader is h reduced to peerAnswerHeaders.
func peerAnswerHeader(h http.Header) http.Header {
	out := make(http.Header, len(peerAnswerHeaders))
	for _, name := range peerAnswerHeaders {
		if v := h.Values(name); len(v) > 0 {
			out[name] = append([]string(nil), v[0])
		}
	}
	return out
}

// syncBudget is a token bucket of refusalSyncRate pushes per second.
type syncBudget struct {
	mu     sync.Mutex
	tokens float64
	at     time.Time
}

func (b *syncBudget) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.at.IsZero() {
		b.tokens, b.at = refusalSyncRate, now
	}
	if b.tokens += now.Sub(b.at).Seconds() * refusalSyncRate; b.tokens > refusalSyncRate {
		b.tokens = refusalSyncRate
	}
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// vlRefusalCounters are the prediction and sync counters of a Proxy.
type vlRefusalCounters struct {
	vlRefusalsPredicted [8]atomic.Int64
	peerSyncCounts      [4]atomic.Int64
	refusalSyncBudget   syncBudget
}
