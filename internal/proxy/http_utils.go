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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/klauspost/compress/zstd"
	fj "github.com/valyala/fastjson"
)

// jsonBuilderPool recycles strings.Builder values used by reconstructLogLine to
// avoid per-entry heap allocations when building the flat JSON log body.
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

// validateQuery checks query string length and returns a sanitized version.
// It also rewrites queries that Loki accepts but VL would reject (e.g. phi>1).
func (p *Proxy) validateQuery(w http.ResponseWriter, query string, endpoint string) (string, bool) {
	if msg := queryLengthError(query); msg != "" {
		p.writeError(w, http.StatusBadRequest, msg)
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
	if len(query) > validationCacheMaxQueryBytes {
		return logql.ValidateLogQL(query)
	}
	if v, ok := validationCache.Load(query); ok {
		return v.(string)
	}
	result := logql.ValidateLogQL(query)
	storeValidationResult(query, result)
	return result
}

// storeValidationResult adds a validation result while the cache is below
// validationCacheMaxSize entries.
func storeValidationResult(key, result string) {
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

// sanitizeLimit caps and validates the limit parameter.
func sanitizeLimit(limitStr string) string {
	if limitStr == "" {
		return "1000"
	}
	n, err := strconv.Atoi(limitStr)
	if err != nil || n <= 0 {
		return "1000"
	}
	if n > maxLimitValue {
		return strconv.Itoa(maxLimitValue)
	}
	return limitStr
}

// writeDrilldownPartialFromUpstream converts an upstream VL 4xx/5xx error into
// a Loki-compatible "200 OK with partial results" reply. Mirrors what Loki
// itself does for Drilldown / Logs Explore traffic — see
// pkg/querier/queryrange/limits.go::seriesLimiter.Do() upstream, which calls
// IsLogsDrilldownRequest() and converts max-series-exceeded errors from
// HTTP 500 ("too_many_series") into HTTP 200 + Warning header + partial
// results so the panel renders an empty chart instead of erroring.
//
// Use this from any path that talks to VL on behalf of Grafana/Drilldown
// traffic and would otherwise return a raw VL error. Non-Grafana / API
// clients (curl, internal tooling) still see the real error via writeError —
// we only swallow upstream failures for graceful-degradation paths.
//
// What Loki emits for partial results, and what we mirror:
//
//   - **`warnings: ["..."]` in the JSON body** (LokiResponse proto field 7).
//     This is the authoritative channel — the Grafana Loki datasource and
//     the Drilldown plugin both read warnings from the body, NOT from the
//     HTTP header. Without this field present, Drilldown silently ignores
//     the partial-results signal and renders the empty chart with no badge.
//   - **`Warning` HTTP header** (RFC 7234 `199 - "..."`). Required for
//     non-Grafana HTTP-aware clients and for log aggregation; Loki sets
//     this in addition to the body field.
//   - **`X-Proxy-Upstream-Status` / `X-Proxy-Upstream-Error`** (our debug
//     headers). Operators tailing access logs see what VL actually said,
//     so the conversion is observable but transparent to clients.
//   - **`Cache-Control: no-store`**. Loki sets this on error/partial paths
//     so frontend caches don't pin the empty reply. Without it, the next
//     identical user query would hit our compat cache and silently
//     re-serve the empty matrix even after VL recovered.
//
// vlStatus is the upstream code we suppressed. vlMsg comes from VL's
// extracted error message and is surfaced verbatim in the body's warnings
// array so operators and panel users see what actually failed.
func (p *Proxy) writeDrilldownPartialFromUpstream(w http.ResponseWriter, vlStatus int, vlMsg string) {
	if p.log != nil && p.log.Enabled(context.Background(), slog.LevelWarn) {
		p.log.Log(context.Background(), slog.LevelWarn,
			"converting VL upstream error to Drilldown partial-results reply",
			"vl_status", vlStatus, "vl_msg", vlMsg)
	}

	// Sanitize VL message for header and body inclusion: first line only, trim
	// to a sane length. Some VL errors include multi-line stack-trace detail
	// which would break HTTP header serialization and bloat the body.
	cleanMsg := vlMsg
	if i := strings.IndexByte(cleanMsg, '\n'); i >= 0 {
		cleanMsg = cleanMsg[:i]
	}
	if len(cleanMsg) > 200 {
		cleanMsg = cleanMsg[:200]
	}

	// Headers Loki sets for partial-results / error-recovery paths.
	w.Header().Set("Warning", `199 - "maximum query bounds exceeded; returning partial results"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Proxy-Upstream-Status", strconv.Itoa(vlStatus))
	if cleanMsg != "" {
		w.Header().Set("X-Proxy-Upstream-Error", cleanMsg)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	// Build the body with the same `warnings` array Loki emits. JSON-escape
	// the upstream message so embedded quotes/backslashes don't break the
	// payload (Drilldown will simply not render the badge if it can't parse).
	warning := "maximum query bounds exceeded; returning partial results"
	if cleanMsg != "" {
		warning = warning + " (upstream: " + cleanMsg + ")"
	}
	warningJSON, _ := json.Marshal(warning)
	_, _ = w.Write([]byte(`{"status":"success","warnings":[`))
	_, _ = w.Write(warningJSON)
	_, _ = w.Write([]byte(`],"data":{"resultType":"matrix","result":[]}}`))
}

func (p *Proxy) writeError(w http.ResponseWriter, code int, msg string) {
	level := slog.LevelInfo
	switch {
	case code >= http.StatusInternalServerError:
		level = slog.LevelError
	case code >= http.StatusBadRequest:
		level = slog.LevelWarn
	}
	if p.log != nil && p.log.Enabled(context.Background(), level) {
		p.log.Log(context.Background(), level, "request error", "code", code, "error", msg)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "error",
		"errorType": lokiErrorType(code),
		"error":     msg,
	})
}

func statusFromUpstreamErr(err error) int {
	if isUpstreamQueryRejected(err) {
		return http.StatusBadRequest
	}
	var matchingErr vectorMatchError
	if errors.As(err, &matchingErr) {
		return http.StatusInternalServerError
	}
	if err == nil {
		return http.StatusBadGateway
	}
	if errors.Is(err, mw.ErrGuardRejected) {
		return http.StatusServiceUnavailable
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
	if strings.Contains(lower, "circuit breaker") {
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
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
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return vlErrorUnclassified
	}
	msg := strings.ReplaceAll(strings.TrimSpace(rawMsg), "`", "")
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
	var statusErr *upstreamStatusError
	if errors.As(err, &statusErr) {
		return statusErr.class == vlErrorQueryRejected
	}
	var parseErr *translator.ParseError
	return errors.As(err, &parseErr)
}

// statusFromBackendErr maps an error for a client: a rejected query is 400, a
// completed backend response keeps its status, anything else goes through
// statusFromUpstreamErr.
func statusFromBackendErr(err error) int {
	if isUpstreamQueryRejected(err) {
		return http.StatusBadRequest
	}
	var hsc httpStatusCoder
	if errors.As(err, &hsc) {
		return hsc.StatusCode()
	}
	return statusFromUpstreamErr(err)
}

// writeBackendError answers a completed VictoriaLogs error response and returns
// the status written: 400 bad_data for a rejected query (including VL's 422 from
// stats endpoints), the backend status otherwise.
func (p *Proxy) writeBackendError(w http.ResponseWriter, status int, body []byte) int {
	err := p.redactedBackendStatusError("", status, body)
	code := statusFromBackendErr(err)
	p.writeError(w, code, err.Error())
	return code
}

// writeGrafanaStatsFailure answers a failed stats call for Grafana-sourced
// traffic: a rejected query is Loki's 400, every other failure keeps the
// partial-results reply (writeDrilldownPartialFromUpstream).
func (p *Proxy) writeGrafanaStatsFailure(w http.ResponseWriter, err error) {
	if isUpstreamQueryRejected(err) {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	status := statusFromUpstreamErr(err)
	var hsc httpStatusCoder
	if errors.As(err, &hsc) {
		status = hsc.StatusCode()
	}
	p.writeDrilldownPartialFromUpstream(w, status, err.Error())
}

// badRequestStatusOr returns 400 for a rejected query and fallback otherwise, for
// call sites that map every other backend failure to one fixed status.
func badRequestStatusOr(err error, fallback int) int {
	if isUpstreamQueryRejected(err) {
		return http.StatusBadRequest
	}
	return fallback
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
	return name == "_time" || name == "_msg" || name == "_stream" || name == "_stream_id"
}

// reconstructLogLine returns a Loki-compatible log line for a VL entry.
//
// streamLabels is the pre-parsed set of stream label keys for this entry (from
// the caller's logQueryStreamDescriptor cache). Passing them in avoids
// re-parsing the _stream value and avoids allocating a fresh map per call.
//
// When VL auto-parses a JSON log at ingestion time it stores all JSON fields as
// top-level VL fields while keeping only the _msg value as the log-line string.
// Loki, by contrast, stores the original raw JSON bytes and returns them as-is.
// This causes |= text-filter and | json parser mismatches: a user who pushes
// {"method":"GET","status":401} expects |= "method=GET" to match, but the proxy
// would return only the _msg string.
//
// Detection: if any top-level VL field is neither a VL internal (_time/_msg/…)
// nor a stream label (from _stream), it was extracted from the original JSON log
// body at ingestion time → the original log was JSON-formatted.
//
// When reconstruction applies, a flat JSON object is returned with _msg and the
// extra non-stream fields. Stream label fields (app, namespace, pod, …) are
// excluded because they were part of the Loki stream metadata, not the log line
// body — matching Loki's native format. Values are always strings because VL
// does not preserve original JSON types (numbers, booleans become strings).
//
// originalQuery is the raw Loki LogQL query string. Reconstruction is skipped
// when the query contains text-extraction parsers other than | json: the
// extracted fields in the VL response would come from logfmt/regexp/pattern
// parsing at query time rather than from JSON ingestion, so wrapping the
// original text line in JSON would be incorrect.
func reconstructLogLine(msg string, entry map[string]interface{}, streamLabels map[string]string, originalQuery string) string {
	return reconstructLogLineWithFlag(msg, entry, streamLabels, hasTextExtractionParser(originalQuery))
}

// reconstructLogLineWithFlag is the hot-path variant of reconstructLogLine for
// use in tight per-entry loops where the hasTextExtractionParser result is
// constant for the entire response and can be precomputed once by the caller.
//
// Uses appendJSONStringToBuilder for zero-allocation JSON string escaping and
// the startLen trick (mirroring reconstructLogLineWithFlagFJ) to avoid a
// separate hasExtra scan pass over the map.
func reconstructLogLineWithFlag(msg string, entry map[string]interface{}, streamLabels map[string]string, skipReconstruction bool) string {
	if skipReconstruction {
		return msg
	}
	// Key-only pre-scan: avoids pool allocation for the common case where all
	// fields are stream labels or VL internals. Value work happens in the
	// write loop below, with startLen as a safety net for empty/invalid values.
	hasExtra := false
	for key := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, ok := streamLabels[key]; !ok {
			hasExtra = true
			break
		}
	}
	if !hasExtra {
		return msg
	}
	b := jsonBuilderPool.Get().(*strings.Builder)
	b.Reset()
	// Pre-grow to msg length + overhead so growSlice is not called on typical entries.
	// Pool reuse means this is free once the builder reaches steady-state capacity.
	if need := len(msg) + 64; b.Cap() < need {
		b.Grow(need)
	}
	b.WriteString(`{"_msg":`)
	appendJSONStringToBuilder(b, msg)
	startLen := b.Len()
	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, ok := streamLabels[key]; ok {
			continue
		}
		sv, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(sv) == "" {
			continue
		}
		b.WriteByte(',')
		appendJSONStringToBuilder(b, key)
		b.WriteByte(':')
		appendJSONStringToBuilder(b, sv)
	}
	if b.Len() == startLen {
		jsonBuilderPool.Put(b)
		return msg
	}
	b.WriteByte('}')
	result := b.String()
	jsonBuilderPool.Put(b)
	return result
}

// reconstructLogLineWithFlagFJ is the fastjson variant of reconstructLogLineWithFlag.
// obj must be the parsed fastjson Object for the current VL NDJSON entry.
// It avoids map[string]interface{} allocations by visiting fields directly via Object.Visit.
func reconstructLogLineWithFlagFJ(msg string, obj *fj.Object, streamLabels map[string]string, skipReconstruction bool) string {
	if skipReconstruction {
		return msg
	}
	b := jsonBuilderPool.Get().(*strings.Builder)
	b.Reset()
	// Pre-grow to msg length + overhead so growSlice is not called on typical entries.
	// Pool reuse means this is free once the builder reaches steady-state capacity.
	if need := len(msg) + 64; b.Cap() < need {
		b.Grow(need)
	}
	b.WriteString(`{"_msg":`)
	appendJSONStringToBuilder(b, msg)
	startLen := b.Len()
	obj.Visit(func(k []byte, v *fj.Value) {
		key := string(k)
		if isVLInternalField(key) || key == "_stream_id" {
			return
		}
		if _, isStreamLabel := streamLabels[key]; isStreamLabel {
			return
		}
		sv, ok := stringifyFJValue(v)
		if !ok || strings.TrimSpace(sv) == "" {
			return
		}
		b.WriteByte(',')
		appendJSONStringToBuilder(b, key)
		b.WriteByte(':')
		appendJSONStringToBuilder(b, sv)
	})
	if b.Len() == startLen {
		jsonBuilderPool.Put(b)
		return msg
	}
	b.WriteByte('}')
	result := b.String()
	jsonBuilderPool.Put(b)
	return result
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
		if authUser, authSource := metrics.ResolveAuthContext(origReq); authUser != "" {
			vlReq.Header[hdrVLAuthUser] = []string{authUser}
			vlReq.Header[hdrVLAuthSource] = []string{authSource}
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
	return hex.EncodeToString(sum[:])
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

func decodeCompressedHTTPResponse(resp *http.Response) error {
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
		resp.Body = &pooledGzipReadCloser{r: zr, upstream: resp.Body}
	case "zstd":
		zr, err := zstd.NewReader(resp.Body)
		if err != nil {
			return err
		}
		resp.Body = &readCloserChain{
			Reader: zr,
			closers: []io.Closer{
				closerFunc(func() error {
					zr.Close()
					return nil
				}),
				resp.Body,
			},
		}
	default:
		return nil
	}
	resp.Header.Del("Content-Encoding")
	resp.Header.Del("Content-Length")
	resp.ContentLength = -1
	resp.Uncompressed = true
	return nil
}

type readCloserChain struct {
	io.Reader
	closers []io.Closer
}

func (r *readCloserChain) Close() error {
	var firstErr error
	for _, closer := range r.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// pooledGzipReadCloser wraps a pooled gzip.Reader and returns it to gzipReaderPool on Close.
type pooledGzipReadCloser struct {
	r        *gzip.Reader
	upstream io.Closer
}

func (p *pooledGzipReadCloser) Read(b []byte) (int, error) { return p.r.Read(b) }
func (p *pooledGzipReadCloser) Close() error {
	err := p.upstream.Close()
	// Reset to empty reader to clear the upstream reference before pooling.
	_ = p.r.Reset(strings.NewReader(""))
	gzipReaderPool.Put(p.r)
	return err
}

type closerFunc func() error

func (f closerFunc) Close() error {
	return f()
}

// statusCapture wraps ResponseWriter to capture the status code and bytes written.
type statusCapture struct {
	http.ResponseWriter
	code         int
	bytesWritten int
}

func (sc *statusCapture) WriteHeader(code int) {
	sc.code = code
	sc.ResponseWriter.WriteHeader(code)
}

func (sc *statusCapture) Write(b []byte) (int, error) {
	if sc.ResponseWriter.Header().Get("Content-Type") == "" {
		sc.ResponseWriter.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	sc.ResponseWriter.Header().Set("X-Content-Type-Options", "nosniff")
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
