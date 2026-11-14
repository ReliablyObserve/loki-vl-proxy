package proxy

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// A plan defect is a request the proxy validated as Loki does and whose
// LogsQL (or a VictoriaLogs argument the proxy wrote) VictoriaLogs then
// refused. The query is one Loki accepts, so the refusal is the proxy's
// defect, never the client's: the client reads what Loki answers for a failed
// backend call, 500 `Internal Server Error`, and never VictoriaLogs' status or
// text. The proxy counts it (loki_vl_proxy_plan_defects_total) and writes the
// VictoriaLogs route, a hash of the LogsQL and VictoriaLogs' message to the
// log line, so operators see which plans VictoriaLogs cannot run.
//
// The failure stays final: a refused query is not retried, does not fall back
// to another backend path and is not masked with a stale answer
// (isUpstreamQueryRejected). The routes that already have a fallback which
// gives the same answer keep it (the plain _stream volume query, the field
// values listing without an unpack stage).

// planDefectError is a VictoriaLogs refusal of what the proxy wrote, typed
// where it is classified (redactedBackendStatusError). Its text is
// VictoriaLogs' message, redacted, so every log line that names the error (a
// fallback's included) keeps the reason; the client never reads it: the error
// layer's one writer answers it with planDefectAnswer.
type planDefectError struct{ cause *upstreamStatusError }

func (e *planDefectError) Error() string { return e.cause.Error() }

func (e *planDefectError) Unwrap() error { return e.cause }

// planDefect types a VictoriaLogs refusal as a plan defect.
func planDefect(cause *upstreamStatusError) *planDefectError {
	return &planDefectError{cause: cause}
}

// planDefectAnswer is the client error for a plan defect: Loki's fixed 500 (a
// failure of the proxy's own work), with the refusal as its cause.
func planDefectAnswer(defect *planDefectError) *lokierr.Error {
	return lokierr.Internal(defect)
}

// isPlanDefect reports a plan defect anywhere in err's tree.
func isPlanDefect(err error) bool {
	_, ok := asType[*planDefectError](err)
	return ok
}

// vlRejectedCall is the first VictoriaLogs call of a request that VictoriaLogs
// answered 400 or 422 (the statuses of its refusals): the route the proxy sent
// it to and a hash of its LogsQL, for the log line of a plan defect.
type vlRejectedCall struct {
	route string
	hash  uint64
	set   bool
}

// isVLRefusalStatus reports a VictoriaLogs status its refusals use: 400 from
// httpserver.Errorf, 422 from the stats endpoints.
func isVLRefusalStatus(status int) bool {
	return status == http.StatusBadRequest || status == http.StatusUnprocessableEntity
}

// recordVLRefusalCall keeps the route and the LogsQL hash of the request's
// first VictoriaLogs call answered with a refusal status. Only those calls get
// here, so the hash costs nothing on an answered call.
func recordVLRefusalCall(ctx context.Context, route, logsql string) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	if !rt.rejectedCall.set {
		rt.rejectedCall = vlRejectedCall{route: route, hash: logsqlHash(logsql), set: true}
	}
	rt.mu.Unlock()
}

// rejectedVLCall is the call recordVLRefusalCall kept for ctx's request.
func rejectedVLCall(ctx context.Context) vlRejectedCall {
	if ctx == nil {
		return vlRejectedCall{}
	}
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return vlRejectedCall{}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.rejectedCall
}

// logsqlHash is the 64-bit FNV-1a hash of a LogsQL text: it names the text in
// the log line without writing the query (which can hold user literals).
func logsqlHash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// planDefectUnknown is the label and log value of a fact the request did not
// record (a call made without the request's telemetry).
const planDefectUnknown = "unknown"

// notePlanDefect counts the plan defect err holds, if any, and returns its
// log attributes: the VictoriaLogs route of the refused call and its LogsQL
// hash. ok is false when err is no plan defect.
func (p *Proxy) notePlanDefect(ctx context.Context, err error) (route, hash string, ok bool) {
	if !isPlanDefect(err) {
		return "", "", false
	}
	route, hash = planDefectUnknown, planDefectUnknown
	if call := rejectedVLCall(ctx); call.set {
		route, hash = call.route, strconv.FormatUint(call.hash, 16)
	}
	endpoint := planDefectUnknown
	if ctx != nil {
		if meta := requestRouteMetaFromContext(ctx); meta.endpoint != "" {
			endpoint = meta.endpoint
		}
	}
	if p.metrics != nil {
		p.metrics.RecordPlanDefect(endpoint, route)
	}
	return route, hash, true
}

// planDefectAttrs appends the plan defect fields of a log line.
func planDefectAttrs(attrs []slog.Attr, route, hash string) []slog.Attr {
	return append(attrs, slog.Bool("plan_defect", true), slog.String("plan_route", route), slog.String("logsql_hash", hash))
}

// logTailPlanDefect counts and logs the plan defect that ended an upgraded
// tail (closed with Loki's 1011 and its text); it reports false when err is
// no plan defect.
func (p *Proxy) logTailPlanDefect(ctx context.Context, err error) bool {
	route, hash, ok := p.notePlanDefect(ctx, err)
	if ok && p.log != nil {
		p.log.LogAttrs(context.Background(), slog.LevelError, "tail ended with a plan defect",
			planDefectAttrs([]slog.Attr{slog.String("upstream_error", err.Error())}, route, hash)...)
	}
	return ok
}
