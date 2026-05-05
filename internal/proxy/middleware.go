package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

type compatCacheActiveCtxKey struct{}

// compatCacheIsActive reports whether the compat cache middleware is handling
// caching for this request. Inner handlers can skip their own cache allocation
// and lookup when this is true.
func compatCacheIsActive(ctx context.Context) bool {
	v, _ := ctx.Value(compatCacheActiveCtxKey{}).(bool)
	return v
}

func setSecurityHeaders(header http.Header) {
	if strings.TrimSpace(header.Get("X-Content-Type-Options")) == "" {
		header.Set("X-Content-Type-Options", "nosniff")
	}
	if strings.TrimSpace(header.Get("X-Frame-Options")) == "" {
		header.Set("X-Frame-Options", "DENY")
	}
	if strings.TrimSpace(header.Get("Cross-Origin-Resource-Policy")) == "" {
		header.Set("Cross-Origin-Resource-Policy", "same-origin")
	}
	if strings.TrimSpace(header.Get("Cache-Control")) == "" {
		header.Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	}
	if strings.TrimSpace(header.Get("Pragma")) == "" {
		header.Set("Pragma", "no-cache")
	}
	if strings.TrimSpace(header.Get("Expires")) == "" {
		header.Set("Expires", "0")
	}
}

// securityHeaders wraps a handler with baseline security response headers.
func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setSecurityHeaders(w.Header())
		h.ServeHTTP(w, r)
	})
}

type requestPolicyError struct {
	status int
	msg    string
}

func (e *requestPolicyError) Error() string { return e.msg }

// validateTenantHeader applies Loki-style tenant presence checks and proxy-specific
// mapping validation before any backend call is made.
func (p *Proxy) validateTenantHeader(r *http.Request) error {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if orgID == "" {
		if p.authEnabled {
			return &requestPolicyError{
				status: http.StatusUnauthorized,
				msg:    "missing X-Scope-OrgID header",
			}
		}
		return nil
	}
	if strings.Contains(orgID, "|") {
		if !isMultiTenantQueryPath(r.URL.Path) {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    "multi-tenant X-Scope-OrgID values are only supported on query endpoints",
			}
		}
		tenantIDs := splitMultiTenantOrgIDs(orgID)
		if len(tenantIDs) < 2 {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    "multi-tenant X-Scope-OrgID must include at least two tenant IDs",
			}
		}
		for _, tenantID := range tenantIDs {
			if tenantID == "*" {
				return &requestPolicyError{
					status: http.StatusBadRequest,
					msg:    `wildcard "*" is not supported inside multi-tenant X-Scope-OrgID values`,
				}
			}
			if err := p.validateSingleTenantOrgID(tenantID); err != nil {
				return err
			}
		}
		return nil
	}
	return p.validateSingleTenantOrgID(orgID)
}

func (p *Proxy) validateSingleTenantOrgID(orgID string) error {
	p.configMu.RLock()
	_, ok := p.tenantMap[orgID]
	p.configMu.RUnlock()
	if ok {
		return nil
	}

	if isDefaultTenantAlias(orgID) {
		return nil
	}
	if orgID == "*" {
		if p.globalTenantAllowed() {
			return nil
		}
		return &requestPolicyError{
			status: http.StatusForbidden,
			msg:    `global tenant bypass ("*") is disabled`,
		}
	}
	if _, err := strconv.Atoi(orgID); err == nil {
		return nil
	}

	return &requestPolicyError{
		status: http.StatusForbidden,
		msg:    fmt.Sprintf("unknown tenant %q", orgID),
	}
}

func splitMultiTenantOrgIDs(orgID string) []string {
	parts := strings.Split(orgID, "|")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func hasMultiTenantOrgID(orgID string) bool {
	return strings.IndexByte(orgID, '|') >= 0
}

var multiTenantQueryPaths = map[string]bool{
	"/loki/api/v1/query":              true,
	"/loki/api/v1/query_range":        true,
	"/loki/api/v1/series":             true,
	"/loki/api/v1/labels":             true,
	"/loki/api/v1/index/stats":        true,
	"/loki/api/v1/index/volume":       true,
	"/loki/api/v1/index/volume_range": true,
	"/loki/api/v1/detected_fields":    true,
	"/loki/api/v1/detected_labels":    true,
	"/loki/api/v1/patterns":           true,
}

func isMultiTenantQueryPath(path string) bool {
	if multiTenantQueryPaths[path] {
		return true
	}
	return strings.HasPrefix(path, "/loki/api/v1/label/") ||
		strings.HasPrefix(path, "/loki/api/v1/detected_field/")
}

func isDefaultTenantAlias(orgID string) bool {
	switch orgID {
	case "0", "fake", "default":
		return true
	default:
		return false
	}
}

func (p *Proxy) globalTenantAllowed() bool {
	return p.allowGlobalTenant
}

func (p *Proxy) tenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := p.validateTenantHeader(r); err != nil {
			if rpe, ok := err.(*requestPolicyError); ok {
				p.writeError(w, rpe.status, rpe.msg)
				return
			}
			p.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) adminMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.adminAuthToken != "" {
			got := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
			if got == "" {
				auth := strings.TrimSpace(r.Header.Get("Authorization"))
				if strings.HasPrefix(auth, "Bearer ") {
					got = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
				}
			}
			if got != p.adminAuthToken {
				p.writeError(w, http.StatusUnauthorized, "admin authentication required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func hostOnly(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

func (p *Proxy) isKnownPeerHost(host string) bool {
	if p.peerCache == nil {
		return false
	}
	for _, peer := range p.peerCache.Peers() {
		if hostOnly(peer) == host {
			return true
		}
	}
	return false
}

func (p *Proxy) peerCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.peerAuthToken != "" {
			if r.Header.Get("X-Peer-Token") != p.peerAuthToken {
				p.writeError(w, http.StatusUnauthorized, "peer authentication required")
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if !p.isKnownPeerHost(hostOnly(r.RemoteAddr)) {
			p.writeError(w, http.StatusForbidden, "peer cache endpoint is restricted to configured peers")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) isAllowedTailOrigin(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return true
	}
	if _, ok := p.tailAllowedOrigins["*"]; ok {
		return true
	}
	_, ok := p.tailAllowedOrigins[origin]
	return ok
}

func (p *Proxy) tailUpgrader() websocket.Upgrader {
	return websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return p.isAllowedTailOrigin(r.Header.Get("Origin"))
		},
	}
}

func compatCacheableEndpoint(endpoint string) bool {
	switch endpoint {
	case "query", "query_range", "series", "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels", "patterns":
		return true
	default:
		return false
	}
}

func (p *Proxy) shouldUseCompatCache(endpoint string, r *http.Request) bool {
	if p.compatCache == nil || r.Method != http.MethodGet || !compatCacheableEndpoint(endpoint) {
		return false
	}
	if (endpoint == "query" || endpoint == "query_range") && p.streamResponse {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") {
		return false
	}
	return true
}

func (p *Proxy) compatCacheKey(endpoint string, r *http.Request) (string, bool) {
	if !p.shouldUseCompatCache(endpoint, r) {
		return "", false
	}
	key := "compat:v1:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.Path + "?" + r.URL.RawQuery
	if fp := p.fingerprintFromCtx(r.Context(), r); fp != "" {
		key += ":auth:" + fp
	}
	return key, true
}

func compatCacheVariantKey(baseKey, encoding string) string {
	return baseKey + ":enc:" + encoding
}

func (p *Proxy) compatCacheResponseEncoding(r *http.Request) (string, int) {
	return mw.PlanResponseCompression(r, mw.CompressionOptions{
		Mode:     p.clientResponseCompression,
		MinBytes: p.clientResponseCompressionMinBytes,
	})
}

func (p *Proxy) compatCacheEncodedVariant(cacheKey string, body []byte, ttl time.Duration, encoding string, minBytes int) ([]byte, string, bool) {
	if encoding == "" || len(body) == 0 || len(body) < minBytes {
		return body, "", true
	}
	variantKey := compatCacheVariantKey(cacheKey, encoding)
	if cachedVariant, ok := p.compatCache.Get(variantKey); ok {
		return cachedVariant, encoding, true
	}
	encoded, err := mw.EncodeResponseBody(encoding, body)
	if err != nil || len(encoded) >= len(body) {
		return body, "", false
	}
	p.compatCache.SetWithTTL(variantKey, encoded, ttl)
	return encoded, encoding, true
}

func compatCacheResponseAllowed(rec *httptest.ResponseRecorder) bool {
	if rec == nil || rec.Code != http.StatusOK || rec.Flushed {
		return false
	}
	if len(rec.Result().Cookies()) > 0 {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(rec.Header().Get("Content-Type")))
	return contentType == "" || strings.Contains(contentType, "application/json")
}

type compatCacheCaptureWriter struct {
	http.ResponseWriter
	body       []byte
	bufHolder  *pooledCompatCaptureBuf
	code       int
	flushed    bool
	limit      int
	overflowed bool
}

func newCompatCacheCaptureWriter(w http.ResponseWriter, limit int) *compatCacheCaptureWriter {
	cw := &compatCacheCaptureWriter{
		ResponseWriter: w,
		limit:          limit,
	}
	if limit <= 0 {
		// limit=0 means the cache is disabled or has no capacity — mark overflowed
		// immediately so capture() is a no-op and no memory is allocated.
		cw.overflowed = true
		return cw
	}
	body, holder := acquireCompatCaptureBuf(limit)
	cw.body = body
	cw.bufHolder = holder
	return cw
}

func (w *compatCacheCaptureWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *compatCacheCaptureWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	if strings.TrimSpace(w.Header().Get("X-Content-Type-Options")) == "" {
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}
	w.capture(b)
	return w.ResponseWriter.Write(b)
}

func (w *compatCacheCaptureWriter) Flush() {
	w.flushed = true
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *compatCacheCaptureWriter) capture(b []byte) {
	if w.overflowed || len(b) == 0 {
		return
	}
	if w.limit > 0 && len(w.body)+len(b) > w.limit {
		w.overflow()
		return
	}
	w.body = append(w.body, b...)
}

func (w *compatCacheCaptureWriter) overflow() {
	if w.overflowed {
		return
	}
	w.overflowed = true
	releaseCompatCaptureBuf(w.body, w.bufHolder)
	w.body = nil
	w.bufHolder = nil
}

func (w *compatCacheCaptureWriter) CapturedBody() []byte {
	if w.overflowed {
		return nil
	}
	return w.body
}

func (w *compatCacheCaptureWriter) Release() {
	releaseCompatCaptureBuf(w.body, w.bufHolder)
	w.body = nil
	w.bufHolder = nil
}

func acquireCompatCaptureBuf(limit int) ([]byte, *pooledCompatCaptureBuf) {
	size := defaultCompatCaptureBufSize
	if limit > 0 {
		size = min(size, limit)
	}
	holder := compatCaptureBufPool.Get().(*pooledCompatCaptureBuf)
	if cap(holder.data) < size {
		holder.data = make([]byte, 0, size)
	}
	holder.data = holder.data[:0]
	return holder.data, holder
}

func releaseCompatCaptureBuf(buf []byte, holder *pooledCompatCaptureBuf) {
	if holder == nil {
		return
	}
	if buf != nil && cap(buf) <= maxPooledCompatCaptureBufSize {
		holder.data = buf[:0]
	} else {
		holder.data = make([]byte, 0, defaultCompatCaptureBufSize)
	}
	compatCaptureBufPool.Put(holder)
}

func compatCacheCaptureAllowed(code int, flushed bool, header http.Header) bool {
	if code == 0 {
		code = http.StatusOK
	}
	if code != http.StatusOK || flushed {
		return false
	}
	if len(header.Values("Set-Cookie")) > 0 {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(header.Get("Content-Type")))
	return contentType == "" || strings.Contains(contentType, "application/json")
}

func patternsPayloadEmpty(body []byte) bool {
	if len(body) == 0 {
		return true
	}
	var resp struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	return len(resp.Data) == 0
}

func (p *Proxy) compatCacheMiddleware(endpoint, route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		cacheKey, cacheable := p.compatCacheKey(endpoint, r)
		if !cacheable {
			setCacheResult(r.Context(), "bypass")
			next(w, r)
			return
		}
		ttl := CacheTTLs[endpoint]
		if ttl <= 0 {
			setCacheResult(r.Context(), "bypass")
			next(w, r)
			return
		}
		if cached, remainingTTL, ok := p.compatCache.GetWithTTL(cacheKey); ok {
			setCacheResult(r.Context(), "hit")
			w.Header().Set("Content-Type", "application/json")
			body := cached
			if encoding, minBytes := p.compatCacheResponseEncoding(r); encoding != "" {
				if encodedBody, encodedAs, ok := p.compatCacheEncodedVariant(cacheKey, cached, remainingTTL, encoding, minBytes); ok && encodedAs != "" {
					w.Header().Set("Content-Encoding", encodedAs)
					w.Header().Set("Vary", "Accept-Encoding")
					body = encodedBody
				}
			}
			_, _ = w.Write(body)
			elapsed := time.Since(start)
			p.metrics.RecordRequestWithRoute(endpoint, route, http.StatusOK, elapsed)
			p.metrics.RecordCacheHit()
			if endpoint == "query" || endpoint == "query_range" {
				p.queryTracker.Record(endpoint, r.FormValue("query"), elapsed, false)
			}
			return
		}

		setCacheResult(r.Context(), "miss")
		var (
			capture            *compatCacheCaptureWriter
			captureAllowed     bool
			capturePatternsNil bool
			captureBodyLen     int
		)
		if encoding, _ := p.compatCacheResponseEncoding(r); encoding != "" {
			_ = mw.RegisterEncodedResponseCapture(w, encoding, func(encodedAs string, encoded []byte) {
				if !captureAllowed || capturePatternsNil || captureBodyLen == 0 {
					return
				}
				if len(encoded) == 0 || len(encoded) >= captureBodyLen {
					return
				}
				p.compatCache.SetWithTTL(compatCacheVariantKey(cacheKey, encodedAs), encoded, ttl)
			})
		}
		capture = newCompatCacheCaptureWriter(w, p.compatCache.MaxEntrySizeBytes())
		rWithFlag := r.WithContext(context.WithValue(r.Context(), compatCacheActiveCtxKey{}, true))
		next(capture, rWithFlag)
		if compatCacheCaptureAllowed(capture.code, capture.flushed, w.Header()) {
			body := capture.CapturedBody()
			captureBodyLen = len(body)
			captureAllowed = captureBodyLen > 0
			if endpoint == "patterns" && captureAllowed {
				capturePatternsNil = patternsPayloadEmpty(body)
				if capturePatternsNil {
					capture.Release()
					return
				}
			}
			if captureAllowed {
				p.compatCache.SetWithTTL(cacheKey, append([]byte(nil), body...), ttl)
			}
		}
		capture.Release()
	}
}

