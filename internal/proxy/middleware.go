package proxy

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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
	// loki is set when Loki itself refuses the tenant this way; the answer
	// then takes the shape Loki's route gives it (writeLokiTenantRefusal).
	loki lokiTenantRefusalKind
}

func (e *requestPolicyError) Error() string { return e.msg }

// validateTenantHeader checks X-Scope-OrgID as Loki does first (presence with
// -auth.enabled or -require-tenant-header, the tenant ID syntax of every
// member, one tenant on tail and delete), then the proxy's own routing rules
// (tenant map, strict routing, tenant-label mode, the global bypass), before
// any cache lookup or backend call. Call normalizeTenantHeader first.
func (p *Proxy) validateTenantHeader(r *http.Request) error {
	orgID := r.Header.Get("X-Scope-OrgID")
	if orgID == "" {
		if p.authEnabled || p.requireTenantHeader {
			return lokiTenantRefusal(lokiTenantMissing, lokiNoOrgID)
		}
		// An absent org is a default-alias request: tenant-label mode and strict
		// tenant routing refuse it unless the global bypass is allowed or the
		// tenant map maps it.
		return p.validateSingleTenantOrgIDWithRouting("", p.routingForContext(r.Context()))
	}
	if !hasMultiTenantOrgID(orgID) {
		if msg := lokiTenantIDError(orgID); msg != "" {
			return lokiTenantRefusal(lokiTenantInvalid, msg)
		}
		// The org is judged exactly as the request context carries it into the
		// backend headers, so what is admitted is what gets routed.
		return p.validateSingleTenantOrgIDWithRouting(orgID, p.routingForContext(r.Context()))
	}
	if tenantRulerView(r.URL.Path) != 0 {
		// The ruler refuses a multi-tenant header before reading its members.
		return lokiTenantRefusal(lokiTenantMultiple, lokiMultipleOrgIDs)
	}
	members := lokiTenantMembers(orgID)
	if err := validateLokiTenantMembers(r.URL.Path, members); err != nil {
		return err
	}
	if !isMultiTenantQueryPath(r.URL.Path) {
		if r.URL.Path == "/loki/api/v1/tail" || r.URL.Path == "/loki/api/v1/delete" {
			return lokiTenantRefusal(lokiTenantMultiple, lokiMultipleOrgIDs)
		}
		return &requestPolicyError{
			status: http.StatusBadRequest,
			msg:    "multi-tenant X-Scope-OrgID values are only supported on query endpoints",
		}
	}
	for _, tenantID := range members {
		if tenantID == "" {
			continue // Loki reads an empty member as a tenant that holds nothing
		}
		if tenantID == "*" {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    `wildcard "*" is not supported inside multi-tenant X-Scope-OrgID values`,
			}
		}
		if err := p.validateSingleTenantOrgIDWithRouting(tenantID, p.routingForContext(r.Context())); err != nil {
			return err
		}
	}
	return nil
}

func (p *Proxy) validateSingleTenantOrgID(orgID string) error {
	return p.validateSingleTenantOrgIDWithRouting(orgID, p.routingForContext(context.Background()))
}

func (p *Proxy) validateSingleTenantOrgIDWithRouting(orgID string, routing requestRouting) error {
	route := p.resolveTenant(orgID, routing)
	if route.err == nil {
		return nil
	}
	if route.warn != refusalQuiet {
		reason, fix := route.warn.reasonAndFix()
		p.tenantRefusals.warn(p.log, orgID, reason, fix)
	}
	return route.err
}

// tenantRefusalLog says once per org and window why an org was refused, so an
// operator can act on it and a client cycling through orgs cannot flood the log
// or grow memory: the table is capped, and past the cap new orgs are not logged
// until entries age out.
type tenantRefusalLog struct {
	mu   sync.Mutex
	seen map[string]time.Time
	// fullUntil is the earliest time an entry can age out while the table is
	// full: until then a new org is skipped without scanning the table.
	fullUntil time.Time
}

const (
	tenantRefusalLogWindow = 10 * time.Minute
	tenantRefusalLogMax    = 1024
	tenantRefusalLogOrgMax = 150 // Loki's longest tenant ID
)

func (l *tenantRefusalLog) warn(log *slog.Logger, orgID, reason, fix string) {
	if log == nil {
		return
	}
	if len(orgID) > tenantRefusalLogOrgMax {
		orgID = orgID[:tenantRefusalLogOrgMax]
	}
	now := time.Now()
	l.mu.Lock()
	if last, ok := l.seen[orgID]; ok && now.Sub(last) < tenantRefusalLogWindow {
		l.mu.Unlock()
		return
	}
	if l.seen == nil {
		l.seen = make(map[string]time.Time)
	}
	if len(l.seen) >= tenantRefusalLogMax {
		if now.Before(l.fullUntil) {
			l.mu.Unlock()
			return
		}
		earliest := time.Time{}
		for k, t := range l.seen {
			if now.Sub(t) >= tenantRefusalLogWindow {
				delete(l.seen, k)
			} else if earliest.IsZero() || t.Before(earliest) {
				earliest = t
			}
		}
		if len(l.seen) >= tenantRefusalLogMax {
			l.fullUntil = earliest.Add(tenantRefusalLogWindow)
			l.mu.Unlock()
			return
		}
	}
	l.seen[orgID] = now
	l.mu.Unlock()
	log.Warn("tenant refused", "org_id", orgID, "reason", reason, "fix", fix)
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
		normalizeTenantHeader(r)
		r = p.withRequestScope(r)
		if err := p.validateTenantHeader(r); err != nil {
			if rpe, ok := err.(*requestPolicyError); ok {
				if rpe.loki != lokiTenantNotRefused {
					p.writeLokiTenantRefusal(w, r.URL.Path, rpe)
					return
				}
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
			// Constant-time compare so response timing does not reveal how much
			// of a guessed token matched. A missing or empty header never
			// equals a configured (non-empty) token.
			if subtle.ConstantTimeCompare([]byte(got), []byte(p.adminAuthToken)) != 1 {
				p.writeError(w, http.StatusUnauthorized, "admin authentication required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// deleteTokenHeader carries the delete API's token; Authorization: Bearer is
// the other place it is read from.
const deleteTokenHeader = "X-Delete-Token"

// bearerToken is the token of an Authorization: Bearer header, or "".
func bearerToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
}

// deleteTokenMatches reports whether r carries the configured delete token, in
// X-Delete-Token or as Authorization: Bearer. It is false when no token is
// configured. Constant-time compare: response timing does not reveal how much
// of a guessed token matched.
func (p *Proxy) deleteTokenMatches(r *http.Request) bool {
	if p.deleteAPIAuthToken == "" {
		return false
	}
	got := strings.TrimSpace(r.Header.Get(deleteTokenHeader))
	if got == "" {
		got = bearerToken(r)
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(p.deleteAPIAuthToken)) == 1
}

// errDeleteAPIWithoutToken refuses a delete API with nothing to authenticate
// its callers.
var errDeleteAPIWithoutToken = errors.New("-delete-api-enabled=true requires -delete-api-auth-token (Helm: deleteAuth.existingSecret): every delete request must carry the token")

// validateDeleteAPIConfig refuses DeleteAPIEnabled without a delete token.
func validateDeleteAPIConfig(enabled bool, deleteAPIAuthToken string) error {
	if enabled && strings.TrimSpace(deleteAPIAuthToken) == "" {
		return errDeleteAPIWithoutToken
	}
	return nil
}

// deleteAuthMiddleware closes the delete API to every request that does not
// carry the delete token (-delete-api-auth-token), and to all of them when none
// is configured. Bare Loki has no authentication of its own; deleting logs is
// a write the proxy opens only to holders of that token, so this check answers
// before Loki's tenant and parameter checks. The token is removed from the
// request before the handler runs, so it can never be forwarded to
// VictoriaLogs (-forward-headers, -forward-authorization) or logged.
func (p *Proxy) deleteAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !p.deleteTokenMatches(r) {
			p.writeError(w, http.StatusUnauthorized, "delete authentication required")
			return
		}
		r.Header.Del(deleteTokenHeader)
		// An Authorization that carries the token goes too, also when the
		// token was read from X-Delete-Token.
		if subtle.ConstantTimeCompare([]byte(bearerToken(r)), []byte(p.deleteAPIAuthToken)) == 1 {
			r.Header.Del("Authorization")
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
	// Only metadata has a deliberately approximate time policy. Final query
	// responses must preserve exact bounds and the evaluation grid.
	rawQuery := r.URL.RawQuery
	switch endpoint {
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
	prefix := "compat:v2:" + endpoint + ":"
	if endpoint == "query" || endpoint == "query_range" {
		// Align final-response cache invalidation with the exact and windowed
		// log-query keys after an output-semantic change.
		prefix += logLineCacheKeyVersion + ":"
		site := responseKeyCompatQuery
		if endpoint == "query_range" {
			site = responseKeyCompatQueryRange
		}
		prefix += responseKeyFences(r, r.FormValue("query"), site)
	}
	if version := readCacheKeyVersion(endpoint); version != "" {
		prefix += version + ":"
	}
	key := prefix + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.Path + "?" + rawQuery
	if fp := p.fingerprintFromCtx(r.Context(), r); fp != "" {
		key += ":auth:" + fp
	}
	// queryStatsCompatFence: query and query_range values carry the
	// entry-count trailer (query_stats_cache.go).
	fence := ""
	if endpoint == "query" || endpoint == "query_range" {
		fence = ":" + queryStatsCompatFence
	}
	key += ":profile:" + p.responseProfileCacheKey(r) + fence
	if q := r.FormValue("query"); q != "" {
		key += p.responseNamesCacheKey(r, q)
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

func (p *Proxy) compatCacheEncodedVariant(ctx context.Context, cacheKey string, body []byte, ttl time.Duration, encoding string, minBytes int) ([]byte, string, bool) {
	if encoding == "" || len(body) == 0 || len(body) < minBytes {
		return body, "", true
	}
	variantKey := compatCacheVariantKey(cacheKey, encoding)
	if cachedVariant, ok := p.cacheGet(ctx, p.compatCache, variantKey); ok {
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
		// The implicit 200 of a first Write: defaults apply. After an explicit
		// WriteHeader the headers are sent and stay as the writer left them.
		w.code = http.StatusOK
		if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		if strings.TrimSpace(w.Header().Get("X-Content-Type-Options")) == "" {
			w.Header().Set("X-Content-Type-Options", "nosniff")
		}
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
	if header.Get(staleResponseHeader) != "" {
		return false
	}
	// Drilldown's 200 partial reply represents an upstream failure, not a
	// reusable answer. Caching it hides a later successful query for five minutes.
	if header.Get(drilldownPartialUpstreamStatusHeader) != "" {
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
// historical (not-near-now) queries keep the full cache for stability. Labels,
// label values and series follow Loki's max_metadata_cache_freshness instead
// (see nearNowWindow).
//
// age is the time since the entry was stored; a negative age falls back to ttl
// minus remaining. The stored age matters for an entry stored with a shorter
// TTL than ttl (an empty label list, stored for the negative TTL): derived from
// ttl it would look old and be refetched on every request.
func (p *Proxy) compatCacheShouldBypassForFreshness(endpoint string, r *http.Request, ttl, remaining, age time.Duration) bool {
	window, ok := p.nearNowWindow(endpoint)
	if !ok || ttl <= 0 || remaining <= 0 || !requestEndsWithin(r, window) {
		return false
	}
	if age < 0 {
		age = ttl - remaining
	}
	return age >= p.recentTailRefreshMaxStaleness
}

// compatCacheLookup checks the edge cache first, then the routing-scoped
// shared cache for completed query and series replies. The same freshness
// rule applies to both tiers, including replies recovered from disk or peers.
func (p *Proxy) compatCacheLookup(endpoint string, r *http.Request, key string, ttl time.Duration) ([]byte, time.Duration, string, bool) {
	value, remaining, age, ok := p.cacheGetWithAge(r.Context(), p.compatCache, key)
	if ok && !p.compatCacheShouldBypassForFreshness(endpoint, r, ttl, remaining, age) {
		return value, remaining, "", true
	}
	if p.cache == nil {
		return nil, 0, "", false
	}
	switch endpoint {
	case "query", "query_range", "series":
	default:
		return nil, 0, "", false
	}
	sharedKey := key + ":routing:" + p.scopeFingerprint(r.Context(), r.Header.Get("X-Scope-OrgID"))
	value, remaining, age, _, ok = p.cacheGetSharedWithAge(r.Context(), p.cache, sharedKey)
	if ok && !p.compatCacheShouldBypassForFreshness(endpoint, r, ttl, remaining, age) {
		return value, remaining, sharedKey, true
	}
	return nil, 0, sharedKey, false
}

func (p *Proxy) storeCompatSharedReply(key string, body []byte, ttl time.Duration) {
	if key != "" && len(body) <= 128<<10 && p.cache != nil {
		p.cache.SetWithTTL(key, body, ttl)
	}
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
		cached, remainingTTL, sharedKey, ok := p.compatCacheLookup(endpoint, r, cacheKey, ttl)
		if ok {
			setCacheResult(r.Context(), "hit")
			// A query answer gets this request's stats (query_stats_cache.go).
			if !p.serveCompatHitWithStats(w, r, cacheKey, cached, remainingTTL) {
				w.Header().Set("Content-Type", "application/json")
				cached = compatHitBody(endpoint, cached)
				body := cached
				if encoding, minBytes := p.compatCacheResponseEncoding(r); encoding != "" {
					if encodedBody, encodedAs, ok := p.compatCacheEncodedVariant(r.Context(), cacheKey, cached, remainingTTL, encoding, minBytes); ok && encodedAs != "" {
						w.Header().Set("Content-Encoding", encodedAs)
						w.Header().Set("Vary", "Accept-Encoding")
						body = encodedBody
					}
				}
				_, _ = w.Write(body)
			}
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
		if encoding, _ := p.compatCacheResponseEncoding(r); encoding != "" && !queryStatsActive(r.Context()) {
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
		var storedBody []byte // the body this miss stored, for the gzip stats sidecar
		p.captureGzipStatsSidecar(w, r, cacheKey, func() ([]byte, time.Duration) { return storedBody, ttl })
		capture = newCompatCacheCaptureWriter(w, p.compatCache.MaxEntrySizeBytes())
		rWithFlag := r.WithContext(context.WithValue(r.Context(), compatCacheActiveCtxKey{}, true))
		next(capture, rWithFlag)
		p.deriveQueryStatsTotalsFromCapture(r, endpoint, capture)
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
			// Empty label lists get the same short negative TTL as the endpoint
			// cache, so labels that appear later are not hidden for the full TTL.
			if captureAllowed && (endpoint == "labels" || endpoint == "label_values") && metadataListPayloadEmpty(body) {
				ttl = p.metadataNegativeTTL()
			}
			if captureAllowed {
				value := p.compatCacheValue(w, r, cacheKey, body)
				p.compatCache.SetWithTTL(cacheKey, value, ttl)
				p.storeCompatSharedReply(sharedKey, value, ttl)
				recordFlowCacheStore(r.Context(), cacheKey, len(value))
				storedBody = body
			}
		}
		capture.Release()
	}
}
