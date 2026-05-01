package proxy

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/klauspost/compress/zstd"
)

// jsonBuilderPool recycles strings.Builder values used by reconstructLogLine to
// avoid per-entry heap allocations when building the flat JSON log body.
var jsonBuilderPool = sync.Pool{New: func() interface{} { return new(strings.Builder) }}

// validateQuery checks query string length and returns a sanitized version.
func (p *Proxy) validateQuery(w http.ResponseWriter, query string, endpoint string) (string, bool) {
	if len(query) > maxQueryLength {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("query exceeds max length (%d > %d)", len(query), maxQueryLength))
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	if err := validateLogQLSyntax(query); err != "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, err)
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	return query, true
}

// validateLogQLSyntax performs lightweight LogQL syntax validation to catch
// queries that Loki would reject with a parse error. Returns the error message
// (matching Loki's format) or empty string if valid.
//
// This catches:
// - Empty queries
// - Queries without a stream selector (e.g., "| json")
// - Malformed stream selectors (e.g., "{app=}")
// - Binary operations on log/pipeline expressions (e.g., "{app=...} == 2")
func validateLogQLSyntax(query string) string {
	query = strings.TrimSpace(query)

	// Empty query
	if query == "" {
		return "parse error : syntax error: unexpected $end"
	}

	// No stream selector — starts with pipeline
	if strings.HasPrefix(query, "|") {
		return "parse error at line 1, col 1: syntax error: unexpected |"
	}

	// Malformed selector: unclosed or empty values
	if strings.HasPrefix(query, "{") {
		braceDepth := 0
		selectorEnd := -1
		for i, ch := range query {
			if ch == '{' {
				braceDepth++
			} else if ch == '}' {
				braceDepth--
				if braceDepth == 0 {
					selectorEnd = i
					break
				}
			}
		}
		if selectorEnd < 0 {
			return "parse error at line 1, col 1: syntax error: unexpected end of input"
		}
		selector := query[:selectorEnd+1]
		// Check for empty values like {app=} or {app= }
		if emptyValueRe.MatchString(selector) {
			return "parse error at line 1, col " + strconv.Itoa(strings.Index(selector, "=}")+2) + ": syntax error: unexpected }, expecting STRING"
		}

		// Check for binary operations on log queries
		// Pattern: {selector} [| pipeline...] <binary_op> <number>
		// This is invalid unless wrapped in a metric function
		rest := strings.TrimSpace(query[selectorEnd+1:])
		if isBinaryOpOnLogQuery(rest) {
			op := extractBinaryOp(rest)
			exprType := "*syntax.MatchersExpr"
			if strings.Contains(rest, "|") {
				exprType = "*syntax.PipelineExpr"
			}
			return "parse error : unexpected type for left leg of binary operation (" + op + "): " + exprType
		}
	}

	return ""
}

var emptyValueRe = regexp.MustCompile(`[=!~]=?\s*[,}]`)

// binaryOpOnLogRe matches a binary operator followed by a number at the end
// of a pipeline expression (not inside a metric function).
var binaryOpOnLogRe = regexp.MustCompile(`(?:^|\s)\s*(==|!=|<=|>=|<|>|\+|-|\*|/|%|\^)\s*\d+\s*$`)

// labelFilterRe matches a standalone label filter stage: identifier <op> number.
var labelFilterRe = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:==|!=|<=|>=|<|>)\s*\d+(\.\d+)?\s*$`)

// logParserKeywords is the set of LogQL parser stage keywords that cannot be field names
// in a label filter comparison (e.g. "| json == 2" is invalid; "| status >= 400" is valid).
var logParserKeywords = map[string]bool{
	"json": true, "logfmt": true, "unpack": true, "regexp": true,
	"pattern": true, "decolorize": true,
}

func isBinaryOpOnLogQuery(rest string) bool {
	if rest == "" {
		return false
	}
	if !binaryOpOnLogRe.MatchString(rest) {
		return false
	}
	// If the last pipe-separated segment is a label filter (field op number) where
	// the field is not a parser keyword, it's a valid stage — e.g. "| json | status >= 400".
	lastPipe := strings.LastIndex(rest, "|")
	lastSeg := strings.TrimSpace(rest)
	if lastPipe >= 0 {
		lastSeg = strings.TrimSpace(rest[lastPipe+1:])
	}
	if m := labelFilterRe.FindStringSubmatch(lastSeg); m != nil {
		if !logParserKeywords[m[1]] {
			return false
		}
	}
	return true
}

func extractBinaryOp(rest string) string {
	m := binaryOpOnLogRe.FindStringSubmatch(rest)
	if len(m) >= 2 {
		return m[1]
	}
	return "?"
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

func isCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "context canceled")
}

func shouldRecordBreakerFailure(err error) bool {
	if err == nil {
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
	if hasTextExtractionParser(originalQuery) {
		return msg
	}

	hasExtra := false
	for key := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, ok := streamLabels[key]; ok {
			continue
		}
		hasExtra = true
		break
	}
	if !hasExtra {
		return msg
	}

	// Build flat JSON directly into a pooled builder, avoiding an intermediate
	// map[string]interface{} allocation and the reflection-heavy json.Marshal
	// map encoder. Per-field json.Marshal calls for individual strings are cheap
	// (no reflection) and correct for all UTF-8 input including escape sequences.
	b := jsonBuilderPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString(`{"_msg":`)
	msgJSON, _ := json.Marshal(msg)
	b.Write(msgJSON)
	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if _, isStreamLabel := streamLabels[key]; isStreamLabel {
			continue
		}
		sv, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(sv) == "" {
			continue
		}
		b.WriteByte(',')
		keyJSON, _ := json.Marshal(key)
		b.Write(keyJSON)
		b.WriteByte(':')
		valJSON, _ := json.Marshal(sv)
		b.Write(valJSON)
	}
	b.WriteByte('}')
	result := b.String()
	jsonBuilderPool.Put(b)
	return result
}

// reconstructLogLineWithFlag is the hot-path variant of reconstructLogLine for
// use in tight per-entry loops where the hasTextExtractionParser result is
// constant for the entire response and can be precomputed once by the caller.
func reconstructLogLineWithFlag(msg string, entry map[string]interface{}, streamLabels map[string]string, skipReconstruction bool) string {
	if skipReconstruction {
		return msg
	}
	hasExtra := false
	for key := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, ok := streamLabels[key]; ok {
			continue
		}
		hasExtra = true
		break
	}
	if !hasExtra {
		return msg
	}
	b := jsonBuilderPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString(`{"_msg":`)
	msgJSON, _ := json.Marshal(msg)
	b.Write(msgJSON)
	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, ok := streamLabels[key]; ok {
			continue
		}
		sv, ok := stringifyEntryValue(value)
		if !ok {
			continue
		}
		b.WriteByte(',')
		keyJSON, _ := json.Marshal(key)
		b.Write(keyJSON)
		b.WriteByte(':')
		valJSON, _ := json.Marshal(sv)
		b.Write(valJSON)
	}
	b.WriteByte('}')
	result := b.String()
	jsonBuilderPool.Put(b)
	return result
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
func (p *Proxy) applyBackendHeaders(vlReq *http.Request) {
	for k, v := range p.backendHeaders {
		vlReq.Header.Set(k, v)
	}
	if vlReq.Header.Get("Accept-Encoding") == "" {
		switch p.backendCompression {
		case "none":
			vlReq.Header.Set("Accept-Encoding", "identity")
		case "gzip":
			vlReq.Header.Set("Accept-Encoding", "gzip")
		case "zstd":
			vlReq.Header.Set("Accept-Encoding", "zstd")
		default:
			vlReq.Header.Set("Accept-Encoding", "zstd, gzip")
		}
	}
	if origReq, ok := vlReq.Context().Value(origRequestKey).(*http.Request); ok && origReq != nil {
		clientID, clientSource := metrics.ResolveClientContext(origReq, p.metricsTrustProxyHeaders)
		vlReq.Header.Set("X-Loki-VL-Client-ID", clientID)
		vlReq.Header.Set("X-Loki-VL-Client-Source", clientSource)
		gp := grafanaClientProfileFromContext(origReq.Context())
		if gp.surface != "" {
			vlReq.Header.Set("X-Loki-VL-Grafana-Surface", gp.surface)
		}
		if gp.version != "" {
			vlReq.Header.Set("X-Loki-VL-Grafana-Version", gp.version)
		}
		if gp.runtimeFamily != "" {
			vlReq.Header.Set("X-Loki-VL-Grafana-Runtime-Family", gp.runtimeFamily)
		}
		if gp.drilldownProfile != "" {
			vlReq.Header.Set("X-Loki-VL-Drilldown-Profile", gp.drilldownProfile)
		}
		if authUser, authSource := metrics.ResolveAuthContext(origReq); authUser != "" {
			vlReq.Header.Set("X-Loki-VL-Auth-User", authUser)
			vlReq.Header.Set("X-Loki-VL-Auth-Source", authSource)
		}
		if p.metricsTrustProxyHeaders {
			for _, headerName := range trustedIdentityHeaders {
				if value := strings.TrimSpace(origReq.Header.Get(headerName)); value != "" {
					vlReq.Header.Set(headerName, value)
				}
			}
			for _, headerName := range trustedProxyForwardHeaders {
				if value := strings.TrimSpace(origReq.Header.Get(headerName)); value != "" {
					vlReq.Header.Set(headerName, value)
				}
			}
		}
		// Forward configured client headers from the original request
		if len(p.forwardHeaders) > 0 {
			for _, hdr := range p.forwardHeaders {
				if val := origReq.Header.Get(hdr); val != "" {
					vlReq.Header.Set(hdr, val)
				}
			}
		}
		for _, cookie := range origReq.Cookies() {
			if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
				vlReq.AddCookie(cookie)
			}
		}
	}
}

// forwardedAuthFingerprint returns a short hash (16 hex chars) of the
// per-user auth context forwarded with a request (configured forward headers
// and cookies). Returns "" when no forwarding is configured, so callers can
// skip the extra allocation when the cache namespace is already user-agnostic.
func (p *Proxy) forwardedAuthFingerprint(r *http.Request) string {
	if len(p.forwardHeaders) == 0 && len(p.forwardCookies) == 0 {
		return ""
	}
	var b strings.Builder
	for _, hdr := range p.forwardHeaders {
		if val := r.Header.Get(hdr); val != "" {
			b.WriteString(hdr)
			b.WriteByte('=')
			b.WriteString(val)
			b.WriteByte(';')
		}
	}
	for _, cookie := range r.Cookies() {
		if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
			b.WriteString("cookie:")
			b.WriteString(cookie.Name)
			b.WriteByte('=')
			b.WriteString(cookie.Value)
			b.WriteByte(';')
		}
	}
	if b.Len() == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])[:16]
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
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		resp.Body = &readCloserChain{Reader: zr, closers: []io.Closer{zr, resp.Body}}
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

