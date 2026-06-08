package proxy

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/gorilla/websocket"
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
		if p.authEnabled || p.requireTenantHeader {
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
	tenantLabel := p.tenantLabel
	p.configMu.RUnlock()
	if ok {
		return nil
	}

	// In label-based routing mode any org ID is accepted: isolation is enforced
	// by injecting a LogsQL label filter into the query, not via AccountID headers.
	if tenantLabel != "" {
		// orgID is injected into LogsQL queries as a label value.
		// Reject characters that cannot be safely embedded: newlines, carriage returns, null bytes.
		for _, ch := range orgID {
			if ch == '\n' || ch == '\r' || ch == 0 {
				return &requestPolicyError{msg: "invalid character in X-Scope-OrgID for tenant-label mode", status: http.StatusBadRequest}
			}
		}
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
	return p.peerCache.HasPeerHost(host)
}

func (p *Proxy) peerCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.peerAuthToken != "" {
			// Constant-time compare to avoid timing-distinguishing the token
			// prefix. Reject any header that differs in length or content.
			got := r.Header.Get("X-Peer-Token")
			if subtle.ConstantTimeEq(int32(len(got)), int32(len(p.peerAuthToken))) != 1 ||
				subtle.ConstantTimeCompare([]byte(got), []byte(p.peerAuthToken)) != 1 {
				p.writeError(w, http.StatusUnauthorized, "peer authentication required")
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		// No shared token configured. By default (peerInsecureIPAllowlist=false)
		// we refuse the request — the startup validator should have prevented
		// this configuration in the first place. The legacy IP-membership
		// fallback is only engaged when the operator explicitly opted in via
		// --peer-insecure-ip-allowlist=true.
		if !p.peerInsecureIPAllowlist {
			p.writeError(w, http.StatusUnauthorized, "peer authentication required")
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
	// Bucket both `start` and `end` to a stable granularity. Grafana's sliding
	// "now-12h to now" window drifts both by a few seconds on every panel refresh,
	// producing a unique cache key every time even when the logical query is identical.
	// query_range/query: 5-minute bucket (or step, whichever is larger).
	// detected_*: 30-second bucket matching fieldNamesCacheBucket used internally.
	rawQuery := r.URL.RawQuery
	switch endpoint {
	case "query_range", "query":
		endRaw := r.FormValue("end")
		startRaw := r.FormValue("start")
		if endRaw != "" || startRaw != "" {
			stepRaw := r.FormValue("step")
			bucket := 5 * time.Minute
			if stepRaw != "" {
				if d, ok := parsePositiveStepDuration(stepRaw); ok && d > bucket {
					bucket = d
				}
			}
			q := r.URL.Query()
			changed := false
			if endRaw != "" {
				if endB := bucketTimestampString(endRaw, bucket); endB != endRaw {
					q.Set("end", endB)
					changed = true
				}
			}
			if startRaw != "" {
				if startB := bucketTimestampString(startRaw, bucket); startB != startRaw {
					q.Set("start", startB)
					changed = true
				}
			}
			if changed {
				rawQuery = q.Encode()
			}
		}
	case "labels", "label_values", "detected_fields", "detected_field_values", "detected_labels":
		endRaw := r.FormValue("end")
		startRaw := r.FormValue("start")
		if endRaw != "" || startRaw != "" {
			q := r.URL.Query()
			changed := false
			if endRaw != "" {
				if endB := bucketTimestampString(endRaw, fieldNamesCacheBucket); endB != endRaw {
					q.Set("end", endB)
					changed = true
				}
			}
			if startRaw != "" {
				if startB := bucketTimestampString(startRaw, fieldNamesCacheBucket); startB != startRaw {
					q.Set("start", startB)
					changed = true
				}
			}
			if changed {
				rawQuery = q.Encode()
			}
		}
	}
	key := "compat:v1:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.Path + "?" + rawQuery
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

var compatCacheCapturePool = sync.Pool{
	New: func() interface{} { return &compatCacheCaptureWriter{} },
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
	cw := compatCacheCapturePool.Get().(*compatCacheCaptureWriter)
	cw.ResponseWriter = w
	cw.code = 0
	cw.flushed = false
	cw.limit = limit
	cw.overflowed = false
	cw.body = nil
	cw.bufHolder = nil
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
	w.ResponseWriter = nil // avoid retaining reference to the underlying writer
	compatCacheCapturePool.Put(w)
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

// compatCacheShouldBypassForFreshness reports whether a compat-cache hit for a
// NEAR-NOW request should be re-fetched instead of served, so live-tail Explore
// and now-relative dashboards pick up new data on refresh. The compat cache holds
// query_range for 5min (for chart stability), and the hit path has no freshness
// check of its own; without this, near-now refreshes would serve up-to-5-min-stale
// data (the reported "refresh doesn't add new logs"). Gated by recent-tail-refresh;
// historical (not-near-now) queries keep the full cache for stability.
func (p *Proxy) compatCacheShouldBypassForFreshness(r *http.Request, ttl, remaining time.Duration) bool {
	if p == nil || !p.recentTailRefreshEnabled || ttl <= 0 || remaining <= 0 {
		return false
	}
	if !p.requestEndsNearNow(r) {
		return false
	}
	return ttl-remaining >= p.recentTailRefreshMaxStaleness
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
		// For query_range (and query), use a fixed 5-minute TTL so the cache entry
		// outlives the bucket used to generate the key (max(5min, step)) while keeping
		// staleness imperceptible: on a 2-day chart a 5-min gap is 0.17% of width.
		// Using step (up to 1h) as TTL caused visible empty spaces at the chart right edge.
		if endpoint == "query_range" || endpoint == "query" {
			ttl = 5 * time.Minute
		}
		if ttl <= 0 {
			setCacheResult(r.Context(), "bypass")
			next(w, r)
			return
		}
		if cached, remainingTTL, ok := p.compatCache.GetWithTTL(cacheKey); ok && !p.compatCacheShouldBypassForFreshness(r, ttl, remainingTTL) {
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
