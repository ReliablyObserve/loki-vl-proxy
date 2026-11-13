package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	gzip "github.com/klauspost/compress/gzip"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/klauspost/compress/zstd"
)

// jsonBuilderPool recycles strings.Builder values used to build JSON bodies
// without per-call heap allocations.
var jsonBuilderPool = sync.Pool{New: func() interface{} { return new(strings.Builder) }}

// gzipReaderPool reuses gzip decompression readers across VL responses to avoid
// per-response allocation. New is nil — Get returns nil until readers are returned.
var gzipReaderPool sync.Pool

// seriesKeysPool recycles the sorted-keys slice used per stream entry in handleSeries.
var seriesKeysPool = sync.Pool{New: func() interface{} { s := make([]string, 0, 16); return &s }}

// Pre-canonicalized VL request header names eliminate net/textproto.CanonicalMIMEHeaderKey
// allocations in applyBackendHeaders. Custom headers with non-canonical capitalisation
// (e.g. "VL" → "Vl") would allocate a new string on every Header.Set call otherwise.
var (
	hdrVLClientID         = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Client-ID")
	hdrVLClientSource     = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Client-Source")
	hdrVLGrafanaSurface   = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Grafana-Surface")
	hdrVLGrafanaVersion   = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Grafana-Version")
	hdrVLGrafanaRTFamily  = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Grafana-Runtime-Family")
	hdrVLDrilldownProfile = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Drilldown-Profile")
	hdrVLAuthUser         = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Auth-User")
	hdrVLAuthSource       = textproto.CanonicalMIMEHeaderKey("X-Loki-VL-Auth-Source")
	hdrAcceptEncoding     = textproto.CanonicalMIMEHeaderKey("Accept-Encoding")
)

// Pre-allocated single-element header value slices for static Accept-Encoding values.
// Reusing these avoids one []string{value} heap allocation per VL sub-request.
var (
	hdrValIdentity = []string{"identity"}
	hdrValGzip     = []string{"gzip"}
	hdrValZstd     = []string{"zstd"}
	hdrValZstdGzip = []string{"zstd, gzip"}
)

// lowercaseQueryKeywords writes the keywords of the query parameter in lower
// case, and removes its `#` comments and logfmt flags, before any handler reads
// it (the handlers answer `| logfmt --strict` as `| logfmt`). Loki's lexer
// is case-insensitive for keywords (SUM BY, TOPK(...), RATE(...), OFFSET) and
// skips comments; the translator and the handlers match the lower-case spelling
// and read the query text without comments.
func lowercaseQueryKeywords(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if query := r.FormValue("query"); query != "" {
			if lower := logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(query))); lower != query {
				setRequestQuery(r, lower)
			}
		}
		next(w, r)
	}
}

// setRequestQuery replaces the query parameter of a parsed request wherever a
// handler reads it.
func setRequestQuery(r *http.Request, query string) {
	traceRewrite(r.Context(), r.Form.Get("query"), query)
	r.Form.Set("query", query)
	if r.PostForm.Has("query") {
		r.PostForm.Set("query", query)
	}
	if values := r.URL.Query(); values.Has("query") { // read again by the multi-tenant fan-out
		values.Set("query", query)
		r.URL.RawQuery = values.Encode()
	}
}

// validateQuery checks query string length and returns a sanitized version.
// It also rewrites queries that Loki accepts but VL would reject (e.g. phi>1).
func (p *Proxy) validateQuery(w http.ResponseWriter, query string, endpoint string) (string, bool) {
	if msg := queryLengthError(query, p.limits().QueryLengthBytes); msg != "" {
		p.writeError(w, http.StatusBadRequest, msg)
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	if err := p.lokiNameError(query); err != "" {
		p.writeError(w, http.StatusBadRequest, truncateQueryError(err))
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	if err := validateLogQLSyntax(query); err != "" {
		p.writeError(w, http.StatusBadRequest, truncateQueryError(err))
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	// Clamp quantile_over_time phi > 1 to 1.0.
	// Loki allows phi > 1 (extrapolation beyond p100); VL rejects it with 422.
	// Clamping preserves the p100 value and keeps queries working.
	query = rewriteQuantilePhiGT1(query)
	return query, true
}

// quantileOverTimePhiRE extracts the phi literal from quantile_over_time(phi, ...).
var quantileOverTimePhiRE = regexp.MustCompile(`\bquantile_over_time\(\s*(-?[\d]+(?:\.[\d]+)?(?:e[+\-]?\d+)?)\s*,`)

// validationCache stores the result of ValidateLogQL for recently-seen query
// strings. Validation is deterministic (same input → same output), so caching
// is safe. The cache is bounded at validationCacheMaxSize entries to prevent
// unbounded growth from uniquely-parameterized queries.
const validationCacheMaxSize = 1024

// validationCacheMaxQueryBytes is the longest query whose validation result is
// cached; longer queries are validated on every request so the cache memory
// stays bounded by validationCacheMaxSize small entries.
const validationCacheMaxQueryBytes = 4096

var (
	validationCache     sync.Map
	validationCacheSize atomic.Int32
)

// validateLogQLSyntax validates LogQL syntax and semantics using the typed AST
// parser, returning a Loki-compatible error string or "" if valid.
// Results are cached by query string to avoid repeated AST allocations for
// identical queries (the common case in real workloads and benchmarks).
func validateLogQLSyntax(query string) string {
	return cachedQueryValidation(query).msg
}

// queryValidation is the validationCache entry of a raw query: Loki's error
// for it, or "" and whether it is a metric (sample) expression, which
// stepAlignMiddleware reads before the handler validates the same string, and
// the offsets and ranges of its range selectors (requestTimeBounds).
type queryValidation struct {
	msg    string
	sample bool
	shift  queryShift
}

// cachedQueryValidation validates query once per distinct string: it parses
// and checks the query on a miss and stores the result while the query is at
// most validationCacheMaxQueryBytes long.
func cachedQueryValidation(query string) queryValidation {
	cacheable := len(query) <= validationCacheMaxQueryBytes
	if cacheable {
		if v, ok := validationCache.Load(query); ok {
			if cached, ok := v.(queryValidation); ok {
				return cached
			}
		}
	}
	expr, msg := logql.ValidateLogQLExpr(query)
	result := queryValidation{msg: msg, sample: msg == "" && isSampleExpr(expr)}
	if msg == "" && expr != nil {
		result.shift = queryShiftOf(expr)
	}
	if cacheable {
		storeValidationResult(query, result)
	}
	return result
}

// parsedLogQueryKey keys a parsed log query in validationCache, apart from its validation result.
type parsedLogQueryKey string

type parsedLogQueryResult struct {
	lq  *logql.LogQuery
	err error
}

// parsedLogQuery is a log query's AST, parsed once per distinct query string while it is at most
// validationCacheMaxQueryBytes long. The AST is shared between callers: they read it and never change it.
func parsedLogQuery(query string) (*logql.LogQuery, error) {
	cacheable := len(query) <= validationCacheMaxQueryBytes
	if cacheable {
		if v, ok := validationCache.Load(parsedLogQueryKey(query)); ok {
			if cached, ok := v.(parsedLogQueryResult); ok {
				return cached.lq, cached.err
			}
		}
	}
	lq, err := parseLogQLQuery(noFlowCtx, query)
	if cacheable {
		storeValidationResult(parsedLogQueryKey(query), parsedLogQueryResult{lq: lq, err: err})
	}
	return lq, err
}

// storeValidationResult adds a validation result while the cache is below
// validationCacheMaxSize entries.
func storeValidationResult(key any, result any) {
	if validationCacheSize.Load() < validationCacheMaxSize {
		if _, loaded := validationCache.LoadOrStore(key, result); !loaded {
			validationCacheSize.Add(1)
		}
	}
}

// rewriteQuantilePhiGT1 replaces phi > 1 in quantile_over_time() with 1.0.
// Loki allows phi > 1 (extrapolates beyond p100 using linear interpolation);
// VictoriaLogs rejects it with 422. Clamping to 1.0 returns the p100 value,
// which is the closest semantically valid result VL can produce.
func rewriteQuantilePhiGT1(query string) string {
	return quantileOverTimePhiRE.ReplaceAllStringFunc(query, func(match string) string {
		m := quantileOverTimePhiRE.FindStringSubmatch(match)
		if len(m) < 2 {
			return match
		}
		phi, err := strconv.ParseFloat(m[1], 64)
		if err != nil || phi <= 1 {
			return match
		}
		return strings.Replace(match, m[1], "1", 1)
	})
}

// RewriteQuantilePhiGT1 is the request rewrite validateQuery applies to every query before it is translated: a
// quantile_over_time with phi above 1 is clamped to 1. The query corpus pairs the plan with the translator on the
// text the handlers translate.
func RewriteQuantilePhiGT1(query string) string { return rewriteQuantilePhiGT1(query) }

// sanitizeLimit caps and validates the limit parameter against the request
// tenant's max_entries_limit_per_query; 0 is unlimited, as in Loki.
func sanitizeLimit(limitStr string, maxLimit int) string {
	if limitStr == "" {
		return "1000"
	}
	n, err := strconv.Atoi(limitStr)
	if err != nil || n <= 0 {
		return "1000"
	}
	if maxLimit > 0 && n > maxLimit {
		return strconv.Itoa(maxLimit)
	}
	return limitStr
}

func (p *Proxy) writeError(w http.ResponseWriter, code int, msg string) {
	p.writeErrorCause(w, code, msg, nil)
}

// writeErrorCause writes the error as Loki's query frontend does (see
// writeLokiErrorBody); cause, when set, is the full error behind msg and goes to
// the log line only.
func (p *Proxy) writeErrorCause(w http.ResponseWriter, code int, msg string, cause error) {
	p.writeErrorShaped(w, code, msg, cause, writeLokiErrorBody)
}

// errorShape writes an error body in the shape one Loki component uses.
type errorShape func(w http.ResponseWriter, code int, msg string)

// writeErrorShaped is writeErrorCause for a route where Loki answers errors in
// another shape than its query frontend does: the ruler's Prometheus JSON, and
// the http.Error text of the compactor and ruler.
func (p *Proxy) writeErrorShaped(w http.ResponseWriter, code int, msg string, cause error, shape errorShape) {
	// Call sites that map backend failures to a fixed 5xx still surface the
	// heavy-query queue rejection with Loki's queue-full status.
	if code != http.StatusTooManyRequests && strings.HasPrefix(msg, lokiTooManyOutstandingRequests+": heavy VictoriaLogs queries") {
		code = http.StatusTooManyRequests
	}
	level := slog.LevelInfo
	switch {
	case code >= http.StatusInternalServerError:
		level = slog.LevelError
	case code >= http.StatusBadRequest:
		level = slog.LevelWarn
	}
	if p.log != nil && p.log.Enabled(context.Background(), level) {
		attrs := []any{"code", code, "error", msg}
		if cause != nil {
			// The full error (VictoriaLogs' message, the transport error, the
			// flag of a proxy queue) is for operators only.
			attrs = append(attrs, "upstream_error", cause.Error())
		}
		if strings.HasPrefix(msg, seriesLimitMessagePrefix) {
			// Loki's message carries no operator hint; the log says which flag
			// sets the limit that rejected this query.
			attrs = append(attrs, "limit_flag", "-max-stats-query-series")
		}
		p.log.Log(context.Background(), level, "request error", attrs...)
	}
	shape(w, code, msg)
}

// writeErrShaped is writeErr in the shape of one Loki component.
func (p *Proxy) writeErrShaped(w http.ResponseWriter, ctx context.Context, err error, shape errorShape) int {
	return p.writeFailure(w, ctx, err, shape)
}

// fail answers a failed request as Loki does, in the body shape Loki uses on
// the request's route (routeErrorShapes), and returns the status written. It
// is the error layer's one writer: a *lokierr.Error is written as it is (its
// status, its text, its own shape for the messages Loki's router writes); any
// other error goes through lokiClientError, which answers Loki's text for the
// failure kinds the proxy types and Loki's fixed 500 for an error it does not
// know. The error's internal cause goes to the log line only.
func (p *Proxy) fail(w http.ResponseWriter, r *http.Request, err error) int {
	return p.writeFailure(w, r.Context(), err, routeErrorShape(r))
}

// writeFailure writes err in shape (unless err's message has a shape of its
// own) and logs it; it returns the status written.
func (p *Proxy) writeFailure(w http.ResponseWriter, ctx context.Context, err error, shape errorShape) int {
	var (
		status int
		text   string
		typed  *lokierr.Error
	)
	if e, ok := lokierr.As(err); ok && (ctx == nil || ctx.Err() == nil) {
		typed, status, text = e, e.Status(), e.Text()
		if own := e.Shape(); own != lokierr.ShapeRoute {
			shape = errorShapeWriter(own)
		}
	} else {
		// A request its client cancelled or that ran out of time is 499 or
		// 504 whatever error it ended with, typed or not.
		status, _, text = lokiClientError(ctx, err)
	}
	p.logRequestError(ctx, status, text, err, typed)
	shape(w, status, text)
	return status
}

// logRequestError writes the error layer's log line: the status and client
// text, the internal error behind them, the message and kind of a typed
// error, the route, and the flag of a limit whose Loki text names none.
func (p *Proxy) logRequestError(ctx context.Context, status int, text string, err error, typed *lokierr.Error) {
	level := slog.LevelInfo
	switch {
	case status >= http.StatusInternalServerError:
		level = slog.LevelError
	case status >= http.StatusBadRequest:
		level = slog.LevelWarn
	}
	if p.log == nil || !p.log.Enabled(context.Background(), level) {
		return
	}
	attrs := []any{"code", status, "error", text}
	cause := err
	if typed != nil {
		attrs = append(attrs, "kind", typed.Kind().String(), "msg_id", typed.Msg().ID())
		cause = typed.Cause()
	}
	if cause != nil {
		// The full error (VictoriaLogs' message, the transport error, the
		// flag of a proxy queue) is for operators only.
		attrs = append(attrs, "upstream_error", cause.Error())
	}
	if strings.HasPrefix(text, seriesLimitMessagePrefix) {
		// Loki's message carries no operator hint; the log says which flag
		// sets the limit that rejected this query.
		attrs = append(attrs, "limit_flag", "-max-stats-query-series")
	}
	if ctx != nil {
		if meta := requestRouteMetaFromContext(ctx); meta.endpoint != "" {
			attrs = append(attrs, "endpoint", meta.endpoint, "route", meta.route)
		}
	}
	p.log.Log(context.Background(), level, "request error", attrs...)
}

// routeErrorShapes is the body shape Loki answers errors in, per route (the
// endpoint name the route registers with). A route that is not listed answers
// as Loki's query frontend does (writeLokiErrorBody).
var routeErrorShapes = map[string]lokierr.Shape{
	// format_query answers a query it cannot parse as JSON.
	"format_query": lokierr.ShapeFormatQuery,
	// The ruler's legacy YAML views answer with http.Error, its Prometheus
	// views with the Prometheus JSON.
	"rules":             lokierr.ShapeHTTPError,
	"rules_nested":      lokierr.ShapeHTTPError,
	"rules_prom":        lokierr.ShapeHTTPError,
	"rules_prom_nested": lokierr.ShapeHTTPError,
	"rules_prometheus":  lokierr.ShapeRulerJSON,
	"alerts":            lokierr.ShapeAlertsJSON,
	"alerts_prom":       lokierr.ShapeAlertsJSON,
	"alerts_prometheus": lokierr.ShapeAlertsJSON,
	// The compactor's delete API and the tenant limits view answer with
	// http.Error.
	"delete":        lokierr.ShapeHTTPError,
	"tenant_limits": lokierr.ShapeHTTPError,
}

// routeErrorShape is the error body shape of r's route.
func routeErrorShape(r *http.Request) errorShape {
	if shape, ok := routeErrorShapes[requestRouteMetaFromContext(r.Context()).endpoint]; ok {
		return errorShapeWriter(shape)
	}
	return writeLokiErrorBody
}

// errorShapeWriter writes the body shape s.
func errorShapeWriter(s lokierr.Shape) errorShape {
	switch s {
	case lokierr.ShapeHTTPError:
		return writeLokiHTTPError
	case lokierr.ShapeBodyless:
		return writeBodylessShape
	case lokierr.ShapeFormatQuery:
		return writeLokiFormatQueryError
	case lokierr.ShapeRulerJSON:
		return writeLokiRulerError
	case lokierr.ShapeAlertsJSON:
		return writeLokiAlertsError
	}
	return writeLokiErrorBody
}

// writeBodylessShape is writeBodylessError as an errorShape: the message is
// not written.
func writeBodylessShape(w http.ResponseWriter, code int, _ string) {
	writeBodylessError(w, code)
}

// writeLokiFormatQueryError answers format_query's parse failure as Loki
// does (recorded from Loki 3.7.9): application/json; charset=utf-8, no
// nosniff, {"status":"invalid-query","error":<message>} and a newline.
func writeLokiFormatQueryError(w http.ResponseWriter, code int, msg string) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	_ = enc.Encode(struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}{"invalid-query", msg})
	w.Header().Del("X-Content-Type-Options")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(b.Bytes())
}

// writeLokiErrorBody answers an error as Loki's query frontend does
// (pkg/util/server/error.go WriteError, v3.7.9): the status, text/plain, nosniff
// and the message alone, with no JSON envelope, no errorType and no trailing
// newline. Grafana's Loki datasource reads that text as it is
// (pkg/tsdb/loki/api.go makeLokiError). The text is valid UTF-8 as Loki's is:
// every byte that is not part of a UTF-8 character (a regexp of a query that
// holds one quotes it) is written as U+FFFD.
func writeLokiErrorBody(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, validUTF8Text(msg))
}

// validUTF8Text is s with each byte that is not part of a UTF-8 character
// replaced by U+FFFD, one per byte (strings.ToValidUTF8 would write one per run).
func validUTF8Text(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		b.WriteRune(r)
	}
	return b.String()
}

// writeBodylessError answers as gorilla/mux does for an unregistered method:
// the status alone, with no body and no Content-Type or nosniff. The
// middleware presets both headers, so they are removed (and not re-added at
// write time).
func writeBodylessError(w http.ResponseWriter, code int) {
	w.Header().Del("Content-Type")
	w.Header().Del("X-Content-Type-Options")
	w.WriteHeader(code)
}

// setReplayContentType sets the Content-Type of a replayed buffered answer:
// JSON for a result, Loki's error shape for a failure (the buffered body is the
// error layer's text, which must not be relabelled as JSON).
func setReplayContentType(w http.ResponseWriter, code int) {
	if code >= http.StatusBadRequest {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		return
	}
	w.Header().Set("Content-Type", "application/json")
}

// writeLokiHTTPError answers like Go's http.Error, which Loki's compactor
// (delete API) and ruler (rule group API) use: text/plain, nosniff and the
// message followed by a newline.
func writeLokiHTTPError(w http.ResponseWriter, code int, msg string) {
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")
}

// writeLokiRulerError answers a Prometheus-compatible rules request as Loki's
// ruler does (pkg/ruler/base/api.go respondError, v3.7.9 :106-123): the JSON
// {"status","data","errorType","error"} in that order. PrometheusRules sets no
// Content-Type (:150-285), so Go sniffs text/plain from the body. The ruler's
// only failure classes are respondInvalidRequest (400, bad_data) and
// respondServerError (500, server_error), so any other status is 500.
func writeLokiRulerError(w http.ResponseWriter, code int, msg string) {
	writeLokiRulerJSON(w, code, msg, "text/plain; charset=utf-8")
}

// writeLokiAlertsError is writeLokiRulerError for PrometheusAlerts, which sets
// Content-Type: application/json before it asks for the rules
// (pkg/ruler/base/api.go:286), so a failure after that keeps it.
func writeLokiAlertsError(w http.ResponseWriter, code int, msg string) {
	writeLokiRulerJSON(w, code, msg, "application/json")
}

func writeLokiRulerJSON(w http.ResponseWriter, code int, msg, contentType string) {
	errorType := "server_error"
	if code == http.StatusBadRequest {
		errorType = "bad_data"
	} else {
		code = http.StatusInternalServerError
	}
	body, err := json.Marshal(struct {
		Status    string      `json:"status"`
		Data      interface{} `json:"data"`
		ErrorType string      `json:"errorType"`
		Error     string      `json:"error"`
	}{"error", nil, errorType, msg})
	if err != nil {
		writeLokiHTTPError(w, http.StatusInternalServerError, lokiErrInternal)
		return
	}
	// Loki's ruler writes no X-Content-Type-Options here; the middleware
	// presets it, so it is removed (and not re-added at write time).
	w.Header().Del("X-Content-Type-Options")
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// statusFromUpstreamErr classifies a failed backend call (or the proxy work
// on its answer) into Loki's status set, following Loki v3.7.9
// pkg/util/server/error.go ClientHTTPStatusAndError: a cancellation is 499
// (:103-105), a deadline 504 (:106-107), a rejected query or a Loki limit 400
// (:108-123), a full queue 429 (pkg/queue/queue.go:21) and every other failure
// 500 (:124-128). Loki never answers a query with 502, 503 or 422. The status
// is also the one recorded in upstream metrics and logs (upstreamErrorStatus).
func statusFromUpstreamErr(err error) int {
	// A VictoriaLogs body that failed after its headers: Loki's 504 when the
	// query ran out of time, a failed backend call otherwise.
	var aborted *vlResponseAbortedError
	if errors.As(err, &aborted) {
		if aborted.timedOut {
			return http.StatusGatewayTimeout
		}
		return http.StatusInternalServerError
	}
	// Loki's querier fails a label values response above
	// grpc_server_max_send_msg_size with 500 ResourceExhausted.
	if isLabelValuesResponseTooLarge(err) {
		return http.StatusInternalServerError
	}
	if isUpstreamQueryRejected(err) || isOrderedJSONPipelineError(err) || isRowLimitError(err) || isBinaryBudgetError(err) {
		return http.StatusBadRequest
	}
	if isHeavyQueryQueueFull(err) || isUpstreamQueueFull(err) {
		return http.StatusTooManyRequests
	}
	var matchingErr vectorMatchError
	if errors.As(err, &matchingErr) {
		return http.StatusInternalServerError
	}
	if err == nil {
		return http.StatusInternalServerError
	}
	if isCanceledErr(err) {
		return 499
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return http.StatusGatewayTimeout
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "timeout") {
		return http.StatusGatewayTimeout
	}
	// Everything else, an open circuit breaker and a guard rejection included,
	// is a failed backend call.
	return http.StatusInternalServerError
}

// Loki's fixed client texts (v3.7.9).
const (
	// pkg/util/server/error.go:26
	lokiErrClientCanceled = "the request was cancelled by the client"
	// pkg/logqlmodel/error.go:13 (logqlmodel.ErrParse): the text of a query
	// VictoriaLogs rejected, without VictoriaLogs' reason or query echo.
	lokiErrParse = "failed to parse the log query"
	// Loki returns a failed storage call's own error text (error.go:128) and
	// has no fixed constant for it; the proxy never forwards the backend's
	// text, so it answers the status line's reason phrase.
	lokiErrInternal = "Internal Server Error"
)

// proxyLimitMarker ends every proxy limit message that names its flag
// ("...; narrow the query or increase -<flag>").
const proxyLimitMarker = "; narrow the query or increase -"

// lokiClientError is the proxy's ClientHTTPStatusAndError: the status,
// errorType and text a client sees for a failed request, with nothing of the
// backend in it (no VictoriaLogs status, body, LogsQL, host:port or Go error).
// The request context is checked first, as upstreamErrorStatus does: a call
// aborted by the client or by the request deadline is 499 or 504 whatever the
// transport returned.
func lokiClientError(ctx context.Context, err error) (status int, errorType, text string) {
	status = upstreamErrorStatus(ctx, err)
	return status, lokiErrorType(status), lokiClientText(status, err)
}

// lokiClientText is the client text for err answered with status. Only texts
// the proxy itself wrote in Loki's words pass through; every other error gets
// Loki's fixed text for its status.
func lokiClientText(status int, err error) string {
	switch status {
	case 499:
		return lokiErrClientCanceled
	case http.StatusGatewayTimeout:
		return lokiErrDeadlineExceeded
	case http.StatusTooManyRequests:
		return lokiTooManyOutstandingRequests
	}
	if err == nil {
		return lokiErrInternal
	}
	var (
		seriesErr   *seriesLimitError
		parseErr    *translator.ParseError
		pipelineErr *orderedJSONPipelineError
		matchingErr vectorMatchError
		tooLarge    *labelValuesResponseTooLargeError
		bodyLimit   *backendBodyTooLargeError
		rowLimitErr *rowLimitError
		budgetErr   *binaryBudgetError
	)
	switch {
	case errors.As(err, &seriesErr):
		return seriesErr.Error()
	case errors.As(err, &parseErr):
		return parseErr.Error()
	case errors.As(err, &pipelineErr):
		return pipelineErr.Error()
	case errors.As(err, &matchingErr):
		return matchingErr.Error()
	case errors.As(err, &tooLarge):
		return tooLarge.Error()
	case errors.As(err, &bodyLimit):
		return bodyLimit.Error()
	case errors.As(err, &rowLimitErr):
		return rowLimitErr.Error()
	case errors.As(err, &budgetErr):
		return budgetErr.Error()
	case status == http.StatusBadRequest:
		return lokiErrParse
	}
	if msg, ok := proxyLimitText(err); ok {
		return msg
	}
	return lokiErrInternal
}

// proxyLimitText returns the message of a proxy limit error that names its
// flag, built by the proxy with no backend or transport error inside it.
func proxyLimitText(err error) (string, bool) {
	var (
		statusErr *upstreamStatusError
		urlErr    *url.Error
		netErr    net.Error
		aborted   *vlResponseAbortedError
	)
	if errors.As(err, &statusErr) || errors.As(err, &urlErr) || errors.As(err, &netErr) || errors.As(err, &aborted) {
		return "", false
	}
	msg := err.Error()
	i := strings.Index(msg, proxyLimitMarker)
	if i < 0 || strings.ContainsAny(msg, "\n\"`{") {
		return "", false
	}
	// The flag is the last word: nothing but its name follows the marker.
	flag := msg[i+len(proxyLimitMarker):]
	if flag == "" || strings.ContainsAny(flag, " ;:/") {
		return "", false
	}
	return msg, true
}

// vlQueueTimeoutPrefix starts the 503 VictoriaLogs answers when a query waited
// -search.maxQueueDuration for one of -search.maxConcurrentRequests slots
// (app/vlselect/main.go:268 in v1.49.0, :313-318 in v1.53.0).
const vlQueueTimeoutPrefix = "couldn't start executing the request in "

// isUpstreamQueueFull reports a full VictoriaLogs (or front proxy) queue: a
// 429, or VictoriaLogs' 503 for a request that never got a slot. Loki answers
// its own full queue with 429 (pkg/queue/queue.go:21).
func isUpstreamQueueFull(err error) bool {
	var statusErr *upstreamStatusError
	return errors.As(err, &statusErr) && (statusErr.status == http.StatusTooManyRequests || statusErr.class == vlErrorQueueFull)
}

func isOrderedJSONPipelineError(err error) bool {
	var pipelineErr *orderedJSONPipelineError
	return errors.As(err, &pipelineErr)
}

// writeErr answers a failed request through the single error layer:
// lokiClientError picks Loki's status and text, the full error goes to the
// request error log line. It returns the status written.
func (p *Proxy) writeErr(w http.ResponseWriter, ctx context.Context, err error) int {
	return p.writeFailure(w, ctx, err, writeLokiErrorBody)
}

// upstreamErrorStatus maps a failed backend call to the status recorded in
// upstream metrics and logs. A call aborted by its own context is 499 (or 504
// when the context hit its deadline), whatever error the transport returned,
// so proxy-side cancellations are not reported as 502 backend failures.
func upstreamErrorStatus(ctx context.Context, err error) int {
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return http.StatusGatewayTimeout
			}
			return 499
		}
	}
	return statusFromUpstreamErr(err)
}

func isCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "context canceled")
}

// httpStatusCoder is implemented by errors that carry an upstream HTTP status
// (e.g. upstreamStatusError). An HTTP-level error from VL proves the
// backend is reachable and therefore must not trip the circuit breaker.
type httpStatusCoder interface{ StatusCode() int }

// upstreamStatusError is a completed VictoriaLogs HTTP response with an error
// status. msg is already redacted (redactedBackendStatusError) so it is safe to
// log and to return to clients; class comes from the raw message.
type upstreamStatusError struct {
	status int
	msg    string
	class  vlErrorClass
}

// vlErrorClass tells apart the failures VictoriaLogs reports with the same
// status: 400 from httpserver.Errorf, 422 from httpserver.SendPrometheusError
// on stats_query and stats_query_range.
type vlErrorClass uint8

const (
	vlErrorUnclassified vlErrorClass = iota
	// vlErrorQueryRejected: the query or an argument is invalid (Loki: 400).
	vlErrorQueryRejected
	// vlErrorResourceLimit: the query parsed but hit a limit or failed while
	// executing; Loki answers Drilldown limit hits with partial results.
	vlErrorResourceLimit
	// vlErrorUnsupportedPath: an older VictoriaLogs lacks the endpoint.
	vlErrorUnsupportedPath
	// vlErrorQueueFull: the query never got one of VictoriaLogs'
	// -search.maxConcurrentRequests slots (Loki: 429).
	vlErrorQueueFull
)

// vlQueryRejectedPrefixes start VictoriaLogs messages for invalid queries and
// arguments (source references: VictoriaLogs v1.50.0). They are prefixes
// because VictoriaLogs writes the message verbatim, and parsing fails before
// execution, so a user literal echoed later in the text cannot fake one.
// Backticks are stripped before matching so redacted text classifies the same.
var vlQueryRejectedPrefixes = []string{
	// app/vlselect/logsql/logsql.go:117 wraps every LogsQL parser error, e.g.
	// "unexpected token" (lib/logstorage/pipe.go:130), "unexpected pipe"
	// (pipe.go:164), "missing ')'" (parser.go:1891), "invalid regexp"
	// (parser.go:2675), "cannot parse 'pattern'" (pipe_extract.go:244).
	// VictoriaLogs v1.52.0 moved the query echo in front of the reason (v1.47.0
	// words it "cannot parse query [<query>]: <reason>"); see stripVLParseEcho,
	// which normalizes both forms to this prefix.
	"cannot parse query arg:",
	"query arg cannot be empty",          // logsql.go:110
	"missing 'field' query arg",          // logsql.go:472, 554
	"'step' must be bigger than zero",    // logsql.go:230, 891
	"cannot parse duration from the arg", // logsql.go:1837 (step, offset)
	"cannot parse start=",                // logsql.go:1650
	"cannot parse end=",                  // logsql.go:1650
	"cannot parse time=",                 // logsql.go:1650
}

// vlProxyGapMarkers are parse errors only proxy-built LogsQL can cause: users
// send LogQL, and the translator alone picks pipes and stats functions. They
// mean a translation or fast-path gap (for example a stats function
// VictoriaLogs lacks), not an invalid user query (VictoriaLogs v1.50.0).
var vlProxyGapMarkers = []string{
	"unknown stats func", // lib/logstorage/pipe_stats.go:1549, pipe_running_stats.go:461
	"unexpected pipe ",   // lib/logstorage/pipe.go:164
}

// vlResourceLimitMarkers occur in VictoriaLogs messages for queries that parsed
// but exceeded a limit or failed during execution (VictoriaLogs v1.50.0).
var vlResourceLimitMarkers = []string{
	// lib/logstorage/pipe_stats.go:1123, pipe_sort.go:478, pipe_sort_topk.go:383,
	// pipe_top.go:300, pipe_uniq.go:268, pipe_facets.go:355,
	// pipe_running_stats.go:224, pipe_stream_context.go:638
	"since it requires more than",
	"of memory is needed",           // pipe_stream_context.go:181, 335
	"because they occupy more than", // storage_search.go:427
	"passed to 'stream_context'",    // pipe_stream_context.go:669, 683
	// -search.maxQueryDuration / maxQueueDuration / maxConcurrentRequests
	// (app/vlselect/main.go:237, 268-272), -search.maxQueryLen (logsql.go:113),
	// -search.maxQueryTimeRange (logsql.go:1577)
	"-search.max",
	"cannot execute query [", // logsql.go:194, 302, 1011, 1147: any failure after parsing
	"cannot obtain ",         // logsql.go:445, 493, 527, 575, 613, 651, 1466
}

// classifyVLError classifies a raw (unredacted) VictoriaLogs error message.
func classifyVLError(status int, rawMsg string) vlErrorClass {
	if strings.HasPrefix(strings.TrimSpace(rawMsg), vlQueueTimeoutPrefix) {
		return vlErrorQueueFull
	}
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return vlErrorUnclassified
	}
	msg := strings.TrimSpace(rawMsg)
	if reason, ok := stripVLParseEcho(msg); ok {
		msg = "cannot parse query arg:" + reason
	}
	msg = strings.ReplaceAll(msg, "`", "")
	switch {
	case strings.HasPrefix(msg, "unsupported path requested"): // app/vlselect/main.go:373
		return vlErrorUnsupportedPath
	case containsAny(reErrQueryEcho.ReplaceAllString(msg, ""), vlProxyGapMarkers):
		// Checked on the message without its query echo, so a user literal
		// quoting a marker cannot change the class.
		return vlErrorUnclassified
	case hasAnyPrefix(msg, vlQueryRejectedPrefixes):
		return vlErrorQueryRejected
	case containsAny(msg, vlResourceLimitMarkers):
		return vlErrorResourceLimit
	}
	return vlErrorUnclassified
}

// vlParseEchoPrefixes start VictoriaLogs' parse-error wrapper from v1.52.0:
// "cannot parse `query` arg [<query>]: <reason>" (app/vlselect/logsql/logsql.go:117).
// Up to v1.51.1 the query followed the reason as "; query=<query>". VictoriaLogs
// v1.47.0 (captured from the image; v1.48.0 and newer word it as above) writes
// "cannot parse query [<query>]: <reason>" with the echo in front too.
var vlParseEchoPrefixes = []string{"cannot parse `query` arg [", "cannot parse query arg [", "cannot parse query ["}

// stripVLParseEcho removes the leading query echo of a VictoriaLogs v1.52+ (or
// v1.47) parse error and returns the text after it: ": <reason>", with the parser
// context still attached. ok is false for any other message.
// The echoed LogsQL can hold "]: " inside quoted literals and the reason can
// hold "[...]: " ("unexpected token after [fields a]: ..."), so the end of the
// echo is the first "]: " outside quotes with balanced brackets. When no such
// end exists the reason is dropped: the whole message may be user text. The
// scan assumes brackets outside quoted literals are balanced in proxy-built
// LogsQL; an unbalanced one also drops the reason, which still classifies as a
// rejected query and never exposes the echo.
func stripVLParseEcho(msg string) (string, bool) {
	for _, prefix := range vlParseEchoPrefixes {
		rest, found := strings.CutPrefix(msg, prefix)
		if !found {
			continue
		}
		depth := 0
		var quote byte
		for i := 0; i < len(rest); i++ {
			c := rest[i]
			switch {
			case quote != 0:
				if c == '\\' && quote != '`' {
					i++
				} else if c == quote {
					quote = 0
				}
			case c == '"' || c == '\'' || c == '`':
				quote = c
			case c == '[':
				depth++
			case c == ']' && depth > 0:
				depth--
			case c == ']' && strings.HasPrefix(rest[i+1:], ": "):
				return rest[i+1:], true
			}
		}
		return ":", true
	}
	return "", false
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func containsAny(s string, markers []string) bool {
	for _, marker := range markers {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func (e *upstreamStatusError) Error() string { return e.msg }

func (e *upstreamStatusError) StatusCode() int { return e.status }

// isUpstreamQueryRejected reports whether err is certainly the query's fault, a
// client mistake Loki answers with 400 bad_data for every client: a VictoriaLogs
// parse or argument error (vlErrorQueryRejected) or a translator parse error.
// translator.UnsupportedError is excluded: it marks valid LogQL (for example
// count_values) that Loki accepts. A rejected query fails on every tenant and
// every retry, so callers must not retry it, fall back to another backend path,
// or mask it with a stale answer. VictoriaLogs also answers resource limits and
// execution failures with 400/422; those, and unrecognised messages, are not
// rejections and keep the backend-failure handling (fallbacks, stale reads,
// partial results, per-tenant skipping, 5xx mapping).
func isUpstreamQueryRejected(err error) bool {
	if isSeriesLimitError(err) {
		return true
	}
	var statusErr *upstreamStatusError
	if errors.As(err, &statusErr) {
		return statusErr.class == vlErrorQueryRejected
	}
	var parseErr *translator.ParseError
	return errors.As(err, &parseErr)
}

// writeBackendError answers a completed VictoriaLogs error response through
// the error layer and returns the status written: 400 bad_data for a rejected
// query (including VL's 422 from stats endpoints), 429 for a full VictoriaLogs
// queue, Loki's 500 otherwise.
func (p *Proxy) writeBackendError(w http.ResponseWriter, status int, body []byte) int {
	return p.writeErr(w, context.Background(), p.redactedBackendStatusError("", status, body))
}

// writeGrafanaStatsFailure answers failed Grafana stats calls through the same
// error contract as other Loki clients. Series-limit partial results are
// produced by the series-limit paths, which still have an actual result.
func (p *Proxy) writeGrafanaStatsFailure(w http.ResponseWriter, err error) {
	p.writeErr(w, context.Background(), err)
}

// shouldRecordBreakerFailure reports whether a failed backend call is evidence
// that the backend is unavailable. ctx is the context the call ran with; nil
// means none is known.
//
// A call whose own context is already done was aborted on the proxy side: a
// client disconnect, a deadline, or an internal cancellation such as an
// errgroup sibling error or an evaluation budget. None of these say anything
// about backend health. Go's http.Client reports such a request with
// context.Cause(ctx), so the error need not wrap context.Canceled and may carry
// an arbitrary message; checking the context itself is the only reliable test.
func shouldRecordBreakerFailure(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	// A rejected heavy-query admission never reached the backend, so it says
	// nothing about backend health. Loki's own queue-full 429 likewise leaves
	// other queries untouched.
	if isHeavyQueryQueueFull(err) {
		return false
	}
	// HTTP responses from VL (even 4xx/5xx) prove the backend is up.
	var hsc httpStatusCoder
	if errors.As(err, &hsc) {
		return false
	}
	if isCanceledErr(err) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "context canceled") ||
		strings.Contains(lower, "deadline exceeded") ||
		strings.Contains(lower, "timeout") {
		return false
	}
	return true
}

// extractVLErrorMsg parses a VictoriaLogs JSON error body and returns the "error"
// field value. If parsing fails or the field is absent, the raw body is returned
// as a string so callers always get a usable message string.
func extractVLErrorMsg(body []byte) string {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return ""
	}
	// Fast path: look for {"error":"..."} without a full JSON parse.
	// Uses json.Unmarshal on just the object so escaped quotes (e.g. \")
	// inside the error value are handled correctly.
	if body[0] == '{' {
		var obj struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &obj); err == nil && obj.Error != "" {
			return obj.Error
		}
	}
	return strings.TrimSpace(string(body))
}

// Compiled once; used by redactBackendError to strip query-like content from a
// VictoriaLogs error message before it is logged or returned.
var (
	// {namespace="prod", pod=~"api.*"} — any brace block in an extracted error
	// message is a LogQL/LogsQL stream selector echo.
	reErrSelector = regexp.MustCompile(`\{[^{}]*\}`)
	// Long quoted literals — line-filter values, field values, an echoed query.
	// Short quotes (e.g. unknown field "id") are left readable.
	reErrQuoted         = regexp.MustCompile(`"[^"]{12,}"`)
	reErrSingleQuoted   = regexp.MustCompile(`'[^']{12,}'`)
	reErrBacktickQuoted = regexp.MustCompile("`[^`]{12,}`")
	// Short backticked identifiers such as VL's "cannot parse `query` arg" are
	// unquoted first so they cannot shift backtick pairing and expose the long
	// literal that follows. At most 11 characters, so anything reErrBacktickQuoted
	// hides stays hidden.
	reErrBacktickIdent = regexp.MustCompile("`([A-Za-z_][A-Za-z0-9_.]{0,10})`")
	// Long hex/id runs (request ids, hashes).
	reErrLongHex = regexp.MustCompile(`\b[0-9a-fA-F]{16,}\b`)
	// VictoriaLogs parse errors end with the whole query echoed back
	// ("; context: [...]; query=...", app/vlselect/logsql/logsql.go:117). Quote
	// pairing cannot be trusted across that echo, so it is dropped entirely.
	reErrQueryEcho = regexp.MustCompile(`(?s);\s*(?:context: \[|query=).*$`)
	// Bracketed echoes of the query or a pipe in execution errors: "cannot execute
	// query [<LogsQL>]: …", "cannot execute tail query [...]: …" and "the query
	// [...] cannot be used in live tailing" (app/vlselect/logsql/logsql.go:194,
	// 302, 678, 733, 738, 1011, 1147), "cannot calculate [<pipe>], since …"
	// (lib/logstorage/pipe_stats.go:1123 and the other pipe memory limits) and
	// "cannot load rows for [<LogsQL>] because …" (storage_search.go:427).
	// The echoed LogsQL can itself contain "]:", "]," or "] because" (a line
	// filter such as "[ERROR]: "), so the span is greedy: from the first echo
	// keyword to the LAST "]" followed by a terminator. Over-redacting part of
	// the reason is safe; the reason after that terminator is kept.
	reErrBracketEcho = regexp.MustCompile(`(?s)\b(query|calculate|rows for) \[.*\](:|,| because| cannot)`)
	// Quoted filters in metadata errors: "with filter=%q:" and "with filter %q:"
	// (logsql.go:445, 493, 527, 575); %q escapes inner quotes, which defeats
	// the quote pairing of reErrQuoted.
	reErrFilterEcho = regexp.MustCompile(`filter[= ]"(?:[^"\\]|\\.)*"`)
)

// redactBackendError extracts a VictoriaLogs error message and strips query-like
// content so the LogQL/LogsQL query (which may carry sensitive log selectors or
// filter values) can't leak into error logs or handler error responses. This is
// the application-level analogue of sanitizeUpstreamError, which only covers
// transport *url.Error. No-op under -debug-log-raw-queries.
func (p *Proxy) redactBackendError(body []byte) string {
	msg := extractVLErrorMsg(body)
	if msg == "" || p.debugLogRawQueries {
		return msg
	}
	if reason, ok := stripVLParseEcho(msg); ok {
		msg = "cannot parse `query` arg […]" + reason
	}
	msg = RedactSecrets(msg)
	msg = reErrQueryEcho.ReplaceAllString(msg, "")
	msg = reErrBracketEcho.ReplaceAllString(msg, "$1 […]$2")
	msg = reErrFilterEcho.ReplaceAllString(msg, `filter="…"`)
	msg = reErrSelector.ReplaceAllString(msg, "{…}")
	msg = reErrQuoted.ReplaceAllString(msg, `"…"`)
	msg = reErrSingleQuoted.ReplaceAllString(msg, `'…'`)
	msg = reErrBacktickIdent.ReplaceAllString(msg, "$1")
	msg = reErrBacktickQuoted.ReplaceAllString(msg, "`…`")
	msg = reErrLongHex.ReplaceAllString(msg, "…")
	if len(msg) > 500 {
		msg = msg[:500] + "…"
	}
	return msg
}

func (p *Proxy) redactedBackendErrorMessage(status int, body []byte) string {
	msg := p.redactBackendError(body)
	if msg == "" {
		return fmt.Sprintf("VL backend returned %d", status)
	}
	return msg
}

func (p *Proxy) redactedBackendStatusError(prefix string, status int, body []byte) error {
	msg := p.redactedBackendErrorMessage(status, body)
	if prefix != "" {
		msg = fmt.Sprintf("%s %d: %s", prefix, status, msg)
	}
	return &upstreamStatusError{status: status, msg: msg, class: classifyVLError(status, extractVLErrorMsg(body))}
}

// lokiErrorType returns the Loki/Prometheus-style errorType for an HTTP status code.
// Matches the exact errorType strings from Loki's Prometheus API handler:
// vendor/github.com/prometheus/prometheus/web/api/v1/api.go
func lokiErrorType(code int) string {
	switch code {
	case 400:
		return "bad_data"
	case 404:
		return "not_found"
	case 406:
		return "not_acceptable"
	case 422:
		return "execution"
	case 499:
		return "canceled"
	case 500:
		return "internal"
	case 502:
		return "unavailable"
	case 503:
		return "timeout" // Loki maps 503 to ErrQueryTimeout
	case 504:
		return "timeout"
	default:
		if code >= 400 && code < 500 {
			return "bad_data"
		}
		return "internal"
	}
}

func (p *Proxy) writeJSON(w http.ResponseWriter, data interface{}) {
	marshalJSON(w, data)
}

func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// proxyControlledResponseHeaders is the set of response headers that the proxy
// sets via withSecurityHeaders. copyBackendHeaders must not overwrite them with
// backend values, otherwise the security posture set by the proxy is silently
// erased by whatever the backend returns.
var proxyControlledResponseHeaders = map[string]bool{
	"X-Content-Type-Options":       true,
	"X-Frame-Options":              true,
	"Cross-Origin-Resource-Policy": true,
	"Cache-Control":                true,
	"Pragma":                       true,
	"Expires":                      true,
}

// copyBackendHeaders copies backend response headers to dst while skipping
// headers in proxyControlledResponseHeaders that the proxy itself manages.
// Use this (instead of copyHeaders) when copying a backend response to the
// client so proxy-set security headers are not overwritten.
func copyBackendHeaders(dst, src http.Header) {
	for key, values := range src {
		if proxyControlledResponseHeaders[http.CanonicalHeaderKey(key)] {
			continue
		}
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

var trustedIdentityHeaders = []string{
	"X-Grafana-User",
	"X-Forwarded-User",
	"X-Webauth-User",
	"X-Auth-Request-User",
}

var trustedProxyForwardHeaders = []string{
	"X-Forwarded-For",
	"X-Forwarded-Proto",
	"X-Forwarded-Host",
	"X-Real-Ip",
	"Forwarded",
}

// isVLInternalField returns true for VictoriaLogs core internal field names
// that should never be exposed in Loki-compatible responses.
func isVLInternalField(name string) bool {
	return name == "_time" || name == "_msg" || name == "_stream" || name == "_stream_id" || name == translator.StoredLineField
}

// appendJSONStringToBuilder writes s as a JSON-encoded string into a strings.Builder.
// Matches json.Marshal semantics for HTML-safe output without allocating a []byte.
// Note: unlike encoding/json's HTML encoder, this does NOT escape U+2028 / U+2029
// (line/paragraph separators); callers concerned about embedding output in <script>
// tags must escape those code points themselves.
func appendJSONStringToBuilder(b *strings.Builder, s string) {
	b.WriteByte('"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&' {
			continue
		}
		if start < i {
			b.WriteString(s[start:i])
		}
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '<':
			b.WriteString(`\u003c`)
		case '>':
			b.WriteString(`\u003e`)
		case '&':
			b.WriteString(`\u0026`)
		default:
			b.WriteString(`\u00`)
			b.WriteByte("0123456789abcdef"[c>>4])
			b.WriteByte("0123456789abcdef"[c&0xf])
		}
		start = i + 1
	}
	if start < len(s) {
		b.WriteString(s[start:])
	}
	b.WriteByte('"')
}

// isVLNonLokiLabelField returns true for fields that VictoriaLogs exposes in
// its field_names endpoint but that should not appear in the Loki /labels API.
// This includes OTel semantic convention fields that VL stores as regular log
// fields — Loki never surfaces these as label names.
func isVLNonLokiLabelField(name string) bool {
	if isVLInternalField(name) {
		return true
	}
	// VL auto-derives this from log content; Loki never exposes it as a label.
	if name == "detected_level" {
		return true
	}
	return false
}

// ShouldFilterTranslatedLabel returns true if a label should be filtered from Loki-compatible
// responses. Only VL-internal fields and detected_level are filtered; user/system fields
// (including those with OTel-like naming patterns) are preserved. Declared fields are
// always kept even if they match filter criteria.
//
// This is exported for testing purposes to validate label filtering logic.
func (p *Proxy) ShouldFilterTranslatedLabel(name string) bool {
	// VL internal fields should be filtered
	if isVLNonLokiLabelField(name) {
		// But respect explicitly declared fields
		for _, declared := range p.declaredLabelFields {
			if declared == name {
				return false
			}
			if strings.Contains(declared, ".") && strings.ReplaceAll(declared, ".", "_") == name {
				return false
			}
		}
		return true
	}
	return false
}

// applyBackendHeaders adds static backend headers and forwarded client headers to a VL request.
// Uses pre-canonicalized header keys and pre-allocated static value slices to avoid
// net/textproto.CanonicalMIMEHeaderKey and []string{value} allocations per sub-request.
func (p *Proxy) applyBackendHeaders(vlReq *http.Request) {
	for k, v := range p.backendHeaders {
		vlReq.Header.Set(k, v)
	}
	if vlReq.Header.Get("Accept-Encoding") == "" {
		switch p.backendCompression {
		case "none":
			vlReq.Header[hdrAcceptEncoding] = hdrValIdentity
		case "gzip":
			vlReq.Header[hdrAcceptEncoding] = hdrValGzip
		case "zstd":
			vlReq.Header[hdrAcceptEncoding] = hdrValZstd
		default: // "auto"
			if p.isBackendLoopback() {
				// Co-located VL (loopback): skip compression entirely.
				// Saves 25–35% CPU on both proxy and VL with zero bandwidth cost.
				vlReq.Header[hdrAcceptEncoding] = hdrValIdentity
			} else {
				vlReq.Header[hdrAcceptEncoding] = hdrValZstdGzip
			}
		}
	}
	if origReq, ok := vlReq.Context().Value(origRequestKey).(*http.Request); ok && origReq != nil {
		clientID, clientSource := metrics.ResolveClientContext(origReq, p.metricsTrustProxyHeaders)
		vlReq.Header[hdrVLClientID] = []string{clientID}
		vlReq.Header[hdrVLClientSource] = []string{clientSource}
		gp := grafanaClientProfileFromContext(origReq.Context())
		if gp.surface != "" {
			vlReq.Header[hdrVLGrafanaSurface] = []string{gp.surface}
		}
		if gp.version != "" {
			vlReq.Header[hdrVLGrafanaVersion] = []string{gp.version}
		}
		if gp.runtimeFamily != "" {
			vlReq.Header[hdrVLGrafanaRTFamily] = []string{gp.runtimeFamily}
		}
		if gp.drilldownProfile != "" {
			vlReq.Header[hdrVLDrilldownProfile] = []string{gp.drilldownProfile}
		}
		// The basic-auth user name is part of the client's credentials: it
		// reaches VictoriaLogs only when the operator trusts and forwards
		// client identity (-metrics.trust-proxy-headers), never by default.
		if p.metricsTrustProxyHeaders {
			if authUser, authSource := metrics.ResolveAuthContext(origReq); authUser != "" {
				vlReq.Header[hdrVLAuthUser] = []string{authUser}
				vlReq.Header[hdrVLAuthSource] = []string{authSource}
			}
		}
		for name, values := range p.forwardedIdentityHeaders(origReq) {
			vlReq.Header[name] = values
		}
		for _, cookie := range origReq.Cookies() {
			if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
				vlReq.AddCookie(cookie)
			}
		}
	}
}

// forwardedAuthFingerprint includes immutable routing and all forwarded identity.
func (p *Proxy) forwardedAuthFingerprint(r *http.Request) string {
	// Use the canonical spelling to avoid allocating a normalized header key.
	scope := p.scopeFingerprint(r.Context(), r.Header.Get("X-Scope-Orgid"))
	if len(r.Header) == 0 || (!p.metricsTrustProxyHeaders && len(p.forwardHeaders) == 0 && len(p.forwardCookies) == 0) {
		return scope
	}
	identity := p.forwardedIdentityHeaders(r)
	cookies := make([][2]string, 0)
	for _, cookie := range r.Cookies() {
		if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
			cookies = append(cookies, [2]string{cookie.Name, cookie.Value})
		}
	}
	if len(identity) == 0 && len(cookies) == 0 {
		return scope
	}
	// JSON encoding is unambiguous even when header values contain delimiters.
	data, _ := json.Marshal([]any{scope, identity, cookies})
	sum := sha256.Sum256(data)
	if len(cookies) > 0 || p.forwardsCredentialHeader(r) {
		return identityFingerprintPrefix + hex.EncodeToString(sum[:])
	}
	return hex.EncodeToString(sum[:])
}

// forwardsCredentialHeader reports whether r carries a header the proxy
// forwards to VictoriaLogs (-forward-headers); trusted proxy identity headers
// (-metrics-trust-proxy-headers) only label metrics and logs.
func (p *Proxy) forwardsCredentialHeader(r *http.Request) bool {
	for _, name := range p.forwardHeaders {
		if r.Header.Get(name) != "" {
			return true
		}
	}
	return false
}

// identityFingerprintPrefix starts the auth fingerprint of a request that
// forwards a user's credentials or identity to VictoriaLogs (a
// -forward-headers header or a -forward-cookies cookie). Cache keys carry
// the fingerprint as ":auth:<fingerprint>", so entries computed with a
// user's credentials are recognisable and never pushed to peer replicas
// (peerPushAllowed): they stay on the replica that computed them.
const identityFingerprintPrefix = "fwdid."

// peerPushAllowed reports whether a cache entry may be pushed to the key's
// peer replicas: not when it was computed with a user's forwarded
// credentials or identity.
func peerPushAllowed(key string) bool {
	return !strings.Contains(key, ":auth:"+identityFingerprintPrefix)
}

// injectAuthFingerprint precomputes the forwardedAuthFingerprint for r and
// stores it in the request context. Call this once at the handler entry point
// (after withOrgID) so every downstream cache-key helper pays only a
// context.Value lookup instead of a full header-parse + SHA-256.
func (p *Proxy) injectAuthFingerprint(r *http.Request) *http.Request {
	fp := p.forwardedAuthFingerprint(r)
	return r.WithContext(context.WithValue(r.Context(), authFingerprintKey, fp))
}

// fingerprintFromCtx returns the memoized auth fingerprint from r's context if
// injectAuthFingerprint was called earlier in the request chain, otherwise falls
// back to computing it live. Safe to call even when no fingerprint was injected.
func (p *Proxy) fingerprintFromCtx(ctx context.Context, r *http.Request) string {
	if v, ok := ctx.Value(authFingerprintKey).(string); ok {
		return v
	}
	return p.forwardedAuthFingerprint(r)
}

func normalizeBackendCompression(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "auto":
		return "auto"
	case "none", "gzip", "zstd":
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return "auto"
	}
}

// decodedBackendBodyFloor is the least a decoded backend response may hold: the
// request coalescer's own ceiling for a shared body, so the transport never
// rejects a body the consumers behind it accept. A variable so tests can use a
// small one.
var decodedBackendBodyFloor int64 = 256 << 20

// backendZstdMaxWindow bounds the history a zstd response may declare in its
// frame header; the decoder allocates it before decoding a byte. The encoders
// VictoriaLogs and its proxies run (klauspost, 4-8 MiB) stay inside.
const backendZstdMaxWindow = 16 << 20

// decodedBodyLimit is the decoded size a backend response may reach: the larger
// of decodedBackendBodyFloor and -backend-max-buffered-response-bytes.
func (p *Proxy) decodedBodyLimit() int64 {
	return max(decodedBackendBodyFloor, int64(p.limits().BufferedBackendBodyBytes))
}

// decodeCompressedHTTPResponse replaces a gzip or zstd body with its decoded
// stream. A decoded stream longer than maxDecoded fails the read with a
// backendBodyTooLargeError, so a small compressed body cannot expand without
// bound; maxDecoded <= 0 leaves the length unbounded, for a stream that has
// none (tail). zstd frames declaring a window above backendZstdMaxWindow are
// rejected before the decoder allocates it.
func decodeCompressedHTTPResponse(resp *http.Response, maxDecoded int64) error {
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	switch encoding {
	case "", "identity":
		return nil
	case "gzip", "x-gzip":
		var zr *gzip.Reader
		if v := gzipReaderPool.Get(); v != nil {
			zr = v.(*gzip.Reader)
			if err := zr.Reset(resp.Body); err != nil {
				gzipReaderPool.Put(zr)
				return err
			}
		} else {
			var err error
			zr, err = gzip.NewReader(resp.Body)
			if err != nil {
				return err
			}
		}
		resp.Body = &pooledGzipReadCloser{r: zr, upstream: resp.Body, limit: newDecodedLimit(maxDecoded)}
	case "zstd":
		zr, err := zstd.NewReader(resp.Body,
			zstd.WithDecoderMaxWindow(backendZstdMaxWindow),
			zstd.WithDecoderMaxMemory(backendZstdMaxWindow))
		if err != nil {
			return err
		}
		resp.Body = &zstdReadCloser{zr: zr, upstream: resp.Body, limit: newDecodedLimit(maxDecoded)}
	default:
		return nil
	}
	resp.Header.Del("Content-Encoding")
	resp.Header.Del("Content-Length")
	// ContentLength and Uncompressed stay as the transport set them: its
	// read loop reads ContentLength after the body is closed, from another
	// goroutine, so writing either here is a data race. Nothing here reads
	// them, and the header deletions above are not read after delivery.
	return nil
}

// decodedLimit fails a read once the decoded stream passes its limit. It hands
// on at most one byte past the limit, so the consumer never holds more than the
// limit of a body it rejects. A limit of 0 passes reads through. It lives in the
// decoding wrappers, so a limited body costs no allocation of its own.
type decodedLimit struct {
	remaining int64
	limit     int64
	err       error
}

func newDecodedLimit(limit int64) decodedLimit {
	return decodedLimit{remaining: limit, limit: limit}
}

func (d *decodedLimit) read(r io.Reader, b []byte) (int, error) {
	if d.limit <= 0 {
		return r.Read(b)
	}
	if d.err != nil {
		return 0, d.err
	}
	if int64(len(b)) > d.remaining+1 {
		b = b[:d.remaining+1]
	}
	n, err := r.Read(b)
	if int64(n) > d.remaining {
		d.err = &backendBodyTooLargeError{limit: d.limit}
		return int(d.remaining), d.err
	}
	d.remaining -= int64(n)
	return n, err
}

// zstdReadCloser reads a zstd body through its decoder and closes both on Close.
type zstdReadCloser struct {
	zr       *zstd.Decoder
	upstream io.Closer
	limit    decodedLimit
}

func (z *zstdReadCloser) Read(b []byte) (int, error) { return z.limit.read(z.zr, b) }
func (z *zstdReadCloser) Close() error {
	z.zr.Close()
	return z.upstream.Close()
}

// pooledGzipReadCloser wraps a pooled gzip.Reader and returns it to gzipReaderPool on Close.
type pooledGzipReadCloser struct {
	r        *gzip.Reader
	upstream io.Closer
	limit    decodedLimit
}

func (p *pooledGzipReadCloser) Read(b []byte) (int, error) { return p.limit.read(p.r, b) }
func (p *pooledGzipReadCloser) Close() error {
	err := p.upstream.Close()
	// Reset to empty reader to clear the upstream reference before pooling.
	_ = p.r.Reset(strings.NewReader(""))
	gzipReaderPool.Put(p.r)
	return err
}

// statusCapture wraps ResponseWriter to capture the status code and bytes written.
type statusCapture struct {
	http.ResponseWriter
	code         int
	bytesWritten int
	wroteHeader  bool
}

func (sc *statusCapture) WriteHeader(code int) {
	sc.code = code
	sc.wroteHeader = true
	sc.ResponseWriter.WriteHeader(code)
}

func (sc *statusCapture) Write(b []byte) (int, error) {
	// The defaults belong to the implicit 200 of a first Write. After an
	// explicit WriteHeader the headers are already sent, and a writer that
	// removed them on purpose (Loki's ruler JSON carries no nosniff) must
	// stay as it wrote them.
	if !sc.wroteHeader {
		if sc.ResponseWriter.Header().Get("Content-Type") == "" {
			sc.ResponseWriter.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		sc.ResponseWriter.Header().Set("X-Content-Type-Options", "nosniff")
		sc.wroteHeader = true
	}
	n, err := sc.ResponseWriter.Write(b)
	sc.bytesWritten += n
	return n, err
}

// Flush implements http.Flusher for chunked streaming support.
func (sc *statusCapture) Flush() {
	if f, ok := sc.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (sc *statusCapture) Unwrap() http.ResponseWriter {
	return sc.ResponseWriter
}

// Hijack implements http.Hijacker for WebSocket upgrade support.
func (sc *statusCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := sc.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijack not supported")
}

func buildConcurrencyLimiter(limit int) chan struct{} {
	if limit <= 0 {
		return nil
	}
	return make(chan struct{}, limit)
}
