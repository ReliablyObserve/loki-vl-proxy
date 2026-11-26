package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sync"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The automatic service_name stream filter (-service-name-stream-route, issue #1763).
//
// A {service_name="x"} selector is the derived filter (translate.ServiceNameDerivedFilterExpr), which VictoriaLogs
// evaluates on every block of every stream in the window. translate.ServiceNameStreamFilter(x) names the streams
// that can hold x's rows when VictoriaLogs stores service_name only as a stream label and service.name never as a
// regular field beside its own stream label; VictoriaLogs reads those from its stream index. Whether that holds is
// a fact about the rows, so the proxy proves it per read instead of assuming it:
//
//   - a log query read (/select/logsql/query, /select/logsql/hits) of a selector with a positive service_name
//     matcher is sent with the stream filter in extra_filters (an argument VictoriaLogs ANDs with the query, so
//     the query text, the translation and every cache key of the request stay what they are without the route);
//   - beside it, at the same time, the proxy reads translate.ServiceNameStreamFilterProof(x) over the same window
//     and tenant scope: the rows the narrowed read leaves out that the derived filter keeps;
//   - the narrowed answer is returned only when that read found no row; a row, an error, a timeout or a refused
//     narrowed read sends the read again without the stream filter, which is the read the proxy makes without the
//     route. So the answer is the derived filter's answer over the rows VictoriaLogs holds while the request runs:
//     no verdict computed before the request arrived is reused, and a row that arrives while it runs may be in or
//     out of it, as for any VictoriaLogs read.
//   - a found row is remembered per tenant scope and service for -service-name-stream-route-violation-ttl (memory,
//     pushed to the peers), and the selector keeps the derived filter alone meanwhile: rows are only added, so the
//     verdict stays true (a deletion only makes the derived filter alone exact again).
//
// The proof reads every other stream of the window, so the route does not lower VictoriaLogs' work: it shortens a
// log read, whose pipes and sort run on the service's rows only, and costs about what the derived filter costs on
// a read too short to gain (-service-name-stream-route-min-window keeps those on the derived filter).

// serviceNameRouteKey holds a request's *serviceNameRoute in its context; a nil one marks the reads the route
// itself makes (the narrowed read, its proof and the read without the stream filter).
type serviceNameRouteKey struct{}

// serviceNameRoute is a log query request's route: the service its selector reads and the proofs run so far.
type serviceNameRoute struct {
	value     string
	filter    string // ServiceNameStreamFilter(value), sent in extra_filters
	proof     string // the proof query
	minWindow int64  // the shortest read window narrowed, in nanoseconds
	// reqFrom and reqTo are the request's bounds (hasReq when it has both): a request's second proof covers them,
	// so the later windows of a split read join it.
	reqFrom, reqTo int64
	hasReq         bool

	mu     sync.Mutex
	proofs []*serviceNameProof
}

// serviceNameProof is one proof read: over [from, to] of one tenant scope, sent with the bounds of the read that
// started it.
type serviceNameProof struct {
	scope      string
	from, to   int64
	start, end string
	done       chan struct{}
	verdict    serviceNameVerdict
}

type serviceNameVerdict uint8

const (
	serviceNameProofFailed serviceNameVerdict = iota // an error, a timeout or an answer that is not the expected one
	serviceNameProofClean                            // no row the narrowed read leaves out
	serviceNameProofFound                            // a row the narrowed read leaves out
)

// maxServiceNameProofs bounds the proof reads of one request: a read past it is sent without the stream filter.
const maxServiceNameProofs = 4

// maxServiceNameProofBody bounds the proof's answer (one row at most, | limit 1).
const maxServiceNameProofBody = 64 << 10

const serviceNameRouteOperation = "service_name_stream_route"

// withServiceNameRoute arms the route for a log query request whose selector qualifies (serviceNameRouteValue);
// every other request is returned as it is.
func (p *Proxy) withServiceNameRoute(r *http.Request, parsed logqlpkg.Expr) *http.Request {
	if !p.serviceNameStreamRoute || p.serviceNameStreamLabelOnly || p.coldRouter != nil {
		return r
	}
	q, ok := parsed.(*logqlpkg.LogQuery)
	if !ok || q.Selector == nil {
		return r
	}
	p.configMu.RLock()
	labelFn := translate.LabelTranslateFunc(p.labelTranslator.ToVL)
	streamFields := p.streamFieldsMap
	p.configMu.RUnlock()
	value, ok := serviceNameRouteValue(q.Selector.Matchers, labelFn, streamFields)
	if !ok {
		return r
	}
	ctx := r.Context()
	if p.serviceNameRouteOff(ctx, value) {
		p.observeInternalOperation(ctx, serviceNameRouteOperation, "remembered_violation", 0)
		return r
	}
	st := &serviceNameRoute{
		value:     value,
		filter:    translate.ServiceNameStreamFilter(value).String(),
		proof:     logsql.Query{Filter: translate.ServiceNameStreamFilterProof(value), Pipes: []logsql.Pipe{logsql.PipeLimit{N: 1}}}.String(),
		minWindow: int64(p.serviceNameRouteMinWindow),
	}
	st.reqFrom, st.reqTo, st.hasReq = parseLokiTimeRangeToUnixNano(r.FormValue("start"), r.FormValue("end"))
	return r.WithContext(context.WithValue(ctx, serviceNameRouteKey{}, st))
}

// serviceNameRouteValue is the value of the selector's first positive service_name matcher with a value, when the
// selector has no matcher the stream filter already reads (a declared -stream-fields label, where a second stream
// filter costs VictoriaLogs more than it saves). Negated, regexp and empty matchers give none.
func serviceNameRouteValue(ms []logqlpkg.LabelMatcher, labelFn translate.LabelTranslateFunc, streamFields map[string]bool) (string, bool) {
	value, found := "", false
	for _, m := range ms {
		if translate.StreamFilterMatcher(m, labelFn, streamFields) {
			return "", false
		}
		if !found && m.Op == logqlpkg.MatchEq && m.Value != "" && translate.SanitizeFieldIdentifier(m.Name) == "service_name" {
			value, found = m.Value, true
		}
	}
	return value, found
}

func serviceNameRouteOf(ctx context.Context) *serviceNameRoute {
	st, _ := ctx.Value(serviceNameRouteKey{}).(*serviceNameRoute)
	return st
}

// window reports the bounds of a read the route narrows: a log read or hits count with both bounds, at least the
// minimum window long.
func (st *serviceNameRoute) window(path string, params url.Values) (from, to int64, ok bool) {
	if path != "/select/logsql/query" && path != "/select/logsql/hits" {
		return 0, 0, false
	}
	from, okFrom := parseFlexibleUnixNanos(params.Get("start"))
	to, okTo := parseFlexibleUnixNanos(params.Get("end"))
	if !okFrom || !okTo || to < from || to-from < st.minWindow {
		return 0, 0, false
	}
	return from, to, true
}

// proofFor returns the proof covering [from, to] in scope, starting one over the read's own bounds when none does;
// nil once the request ran maxServiceNameProofs.
func (st *serviceNameRoute) proofFor(p *Proxy, ctx context.Context, scope string, from, to int64, params url.Values) *serviceNameProof {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range st.proofs {
		if e.scope == scope && e.from <= from && to <= e.to {
			return e
		}
	}
	if len(st.proofs) >= maxServiceNameProofs {
		return nil
	}
	e := &serviceNameProof{scope: scope, from: from, to: to, start: params.Get("start"), end: params.Get("end"), done: make(chan struct{})}
	if len(st.proofs) > 0 && st.hasReq {
		// The first read of a request proves its own window (a split log read that fills its limit in the newest
		// window reads no other); a read past it proves the whole request, so every later window joins one proof.
		if st.reqFrom < e.from {
			e.from = st.reqFrom
		}
		if st.reqTo > e.to {
			e.to = st.reqTo
		}
		e.start = time.Unix(0, e.from).UTC().Format(time.RFC3339Nano)
		e.end = time.Unix(0, e.to).UTC().Format(time.RFC3339Nano)
	}
	st.proofs = append(st.proofs, e)
	go func() {
		e.verdict = p.runServiceNameProof(ctx, st.proof, e.start, e.end)
		close(e.done)
	}()
	return e
}

// wait returns the proof's verdict, or a failed one when ctx ends first.
func (e *serviceNameProof) wait(ctx context.Context) serviceNameVerdict {
	select {
	case <-e.done:
		return e.verdict
	case <-ctx.Done():
		return serviceNameProofFailed
	}
}

// runServiceNameProof reads the proof query over [start, end]: a row is a found violation, an empty 200 answer a
// clean window, anything else a failed proof.
func (p *Proxy) runServiceNameProof(ctx context.Context, query, start, end string) serviceNameVerdict {
	resp, err := p.vlPost(ctx, "/select/logsql/query", url.Values{"query": {query}, "start": {start}, "end": {end}})
	if err != nil {
		return serviceNameProofFailed
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readBodyLimited(resp.Body, maxServiceNameProofBody)
	if err != nil || resp.StatusCode != http.StatusOK {
		return serviceNameProofFailed
	}
	if len(bytes.TrimSpace(body)) > 0 {
		return serviceNameProofFound
	}
	return serviceNameProofClean
}

// sendServiceNameRouted sends a read of the route: narrowed by the stream filter, beside its proof, and again
// without the stream filter unless the proof is clean and the narrowed read answered.
func (p *Proxy) sendServiceNameRouted(ctx context.Context, st *serviceNameRoute, path string, params url.Values, send func(context.Context, url.Values) (*http.Response, error)) (*http.Response, error) {
	inner := context.WithValue(ctx, serviceNameRouteKey{}, (*serviceNameRoute)(nil))
	from, to, ok := st.window(path, params)
	if !ok {
		return send(inner, params)
	}
	began := time.Now()
	proof := st.proofFor(p, inner, p.contextScopeFingerprint(ctx), from, to, params)
	if proof == nil {
		p.observeInternalOperation(ctx, serviceNameRouteOperation, "proof_bound", time.Since(began))
		return send(inner, params)
	}
	narrowed := cloneURLValues(params)
	narrowed.Add("extra_filters", st.filter)
	resp, err := send(inner, narrowed)
	verdict := proof.wait(ctx)
	if verdict == serviceNameProofClean && err == nil && resp.StatusCode < http.StatusBadRequest {
		p.observeInternalOperation(ctx, serviceNameRouteOperation, "narrowed", time.Since(began))
		return resp, nil
	}
	refused := err == nil && resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError
	if resp != nil {
		_ = resp.Body.Close()
	}
	outcome := "proof_failed"
	switch {
	case verdict == serviceNameProofFound:
		outcome = "violation"
		p.rememberServiceNameRouteOff(inner, st.value)
	case verdict == serviceNameProofClean:
		outcome = "narrowed_read_failed"
	}
	full, err := send(inner, params)
	if refused && err == nil && full.StatusCode < http.StatusBadRequest {
		// VictoriaLogs refused the stream filter and answered the read without it: an older release.
		outcome = "narrowed_read_refused"
		p.rememberServiceNameRouteOff(inner, st.value)
	}
	p.observeInternalOperation(ctx, serviceNameRouteOperation, outcome, time.Since(began))
	return full, err
}

// serviceNameRouteOffKey is the key of a remembered violation: per tenant scope and service.
func (p *Proxy) serviceNameRouteOffKey(ctx context.Context, value string) string {
	sum := sha256.Sum256([]byte(value))
	return "service-name-route-off:v1:" + hex.EncodeToString(sum[:12]) + ":scope:" + p.contextScopeFingerprint(ctx)
}

func (p *Proxy) serviceNameRouteOff(ctx context.Context, value string) bool {
	if p.cache == nil || p.serviceNameRouteViolationTTL <= 0 {
		return false
	}
	_, _, ok := p.cacheGetWithTTL(ctx, p.cache, p.serviceNameRouteOffKey(ctx, value))
	return ok
}

func (p *Proxy) rememberServiceNameRouteOff(ctx context.Context, value string) {
	if p.cache == nil || p.serviceNameRouteViolationTTL <= 0 {
		return
	}
	p.cache.SetReplicatedWithTTL(p.serviceNameRouteOffKey(ctx, value), []byte("1"), p.serviceNameRouteViolationTTL)
}
