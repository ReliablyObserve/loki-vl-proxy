package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// The caller of a client request: who sent it (the end user and how the proxy knows it), for which tenant, and
// from which Grafana app. The request, upstream_request, slow_request and slow_upstream_request lines and the
// /debug/queries view carry it, so one line answers "who did this and what did it cost". It never labels a
// metric: users are unbounded.

const (
	// callerIdentityMaxBytes caps a user name or tenant as the lines carry it.
	callerIdentityMaxBytes = 128
	// callerTagsMaxBytes caps Grafana's X-Query-Tags as the lines carry it.
	callerTagsMaxBytes = 256
)

// requestCaller is resolved once per client request, before its handler runs, every value already sanitized.
type requestCaller struct {
	user       string // enduser.id
	userSource string // enduser.source: grafana_user, forwarded_user, ..., tenant, remote_addr
	tenant     string // the X-Scope-OrgID the client sent
	queryTags  string // Grafana's X-Query-Tags, as sent
	sourceTag  string // the Source tag of X-Query-Tags (Explore, Drilldown, a dashboard's datasource)
	surface    string // grafana_drilldown, grafana_loki_datasource, or "" when unknown
}

// newRequestCaller sanitizes what the request log, the slow lines and /debug/queries read about the caller.
func newRequestCaller(r *http.Request, tenant, user, userSource string, profile grafanaClientProfile) requestCaller {
	c := requestCaller{
		user:       sanitizeCallerValue(user, callerIdentityMaxBytes),
		userSource: userSource,
		tenant:     sanitizeCallerValue(tenant, callerIdentityMaxBytes),
		sourceTag:  sanitizeCallerValue(profile.sourceTag, callerIdentityMaxBytes),
	}
	if tags := r.Header.Values("X-Query-Tags"); len(tags) > 0 {
		c.queryTags = sanitizeCallerValue(strings.Join(tags, ","), callerTagsMaxBytes)
	}
	if profile.surface != "unknown" {
		c.surface = profile.surface
	}
	return c
}

// logUserHeader is -log-user-header: the header naming the end user for the logs and /debug/queries only, and
// the TCP peers trusted to send it (-log-user-header-trusted-cidrs; none listed trusts every peer).
type logUserHeader struct {
	name    string // canonical header name; "" when unset
	source  string // enduser.source of a user it names
	trusted []netip.Prefix
}

// userHeaderSources are the enduser.source names of the user headers the proxy knows; another header is
// user_header.
var userHeaderSources = map[string]string{
	"X-Grafana-User":      "grafana_user",
	"X-Forwarded-User":    "forwarded_user",
	"X-Webauth-User":      "webauth_user",
	"X-Auth-Request-User": "auth_request_user",
}

// parseLogUserHeader reads -log-user-header and -log-user-header-trusted-cidrs at startup, so a bad value fails
// fast.
func parseLogUserHeader(name, cidrs string) (logUserHeader, error) {
	name = strings.TrimSpace(name)
	cidrs = strings.TrimSpace(cidrs)
	if name == "" {
		if cidrs != "" {
			return logUserHeader{}, fmt.Errorf("-log-user-header-trusted-cidrs %q needs -log-user-header", cidrs)
		}
		return logUserHeader{}, nil
	}
	h := logUserHeader{name: http.CanonicalHeaderKey(name), source: "user_header"}
	if src, ok := userHeaderSources[h.name]; ok {
		h.source = src
	}
	for _, part := range strings.Split(cidrs, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(part)
		if err != nil {
			addr, addrErr := netip.ParseAddr(part)
			if addrErr != nil {
				return logUserHeader{}, fmt.Errorf("-log-user-header-trusted-cidrs: %q is neither a CIDR nor an address", part)
			}
			prefix = netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen())
		}
		h.trusted = append(h.trusted, prefix.Masked())
	}
	return h, nil
}

// peerTrusted reports whether the request's TCP peer (RemoteAddr: the client or the last load balancer, never
// X-Forwarded-For, which the client writes) may name the user.
func (h logUserHeader) peerTrusted(remoteAddr string) bool {
	if len(h.trusted) == 0 {
		return true
	}
	var addr netip.Addr
	if ap, err := netip.ParseAddrPort(remoteAddr); err == nil {
		addr = ap.Addr()
	} else if a, err := netip.ParseAddr(remoteAddr); err == nil {
		addr = a
	} else {
		return false
	}
	addr = addr.Unmap()
	for _, prefix := range h.trusted {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// callerUser is the end user the logs and /debug/queries name: the -log-user-header value from a trusted peer,
// otherwise the client identity the metrics use (the trusted user headers of -metrics.trust-proxy-headers, the
// tenant, or the client address).
func (p *Proxy) callerUser(r *http.Request, clientID, clientSource string) (string, string) {
	if p.logUser.name == "" {
		return clientID, clientSource
	}
	user := strings.TrimSpace(r.Header.Get(p.logUser.name))
	if user == "" || !p.logUser.peerTrusted(r.RemoteAddr) {
		return clientID, clientSource
	}
	return user, p.logUser.source
}

// requestCallerFromContext is the caller of the client request ctx belongs to; zero outside one. It is resolved
// when a line or the callers view needs it, so a request that logs nothing pays nothing. Work shared by coalesced
// requests runs in the context of the request that started it, so it is attributed to that caller.
func (p *Proxy) requestCallerFromContext(ctx context.Context) requestCaller {
	meta := requestRouteMetaFromContext(ctx)
	if meta.req == nil {
		return requestCaller{}
	}
	r := meta.req
	clientID, clientSource := metrics.ResolveClientContext(r, p.metricsTrustProxyHeaders)
	user, userSource := p.callerUser(r, clientID, clientSource)
	return newRequestCaller(r, r.Header.Get("X-Scope-Orgid"), user, userSource, grafanaClientProfileFromContext(ctx))
}

// vlCost is the VictoriaLogs part of a request's flow, passed by value so the request's telemetry snapshot stays
// on the stack.
type vlCost struct {
	calls       int
	dur         time.Duration
	bytes, rows int64
}

// callerLogAttrs are the caller fields of a log line of the request ctx belongs to (appendCallerAttrs). The first
// line of a request resolves them and its later lines (one upstream_request per VictoriaLogs call) reuse them, so a
// request pays for its caller once, and only when it writes a line.
func (p *Proxy) callerLogAttrs(ctx context.Context) []any {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.callerResolved {
		rt.setCallerAttrs(p.requestCallerFromContext(ctx))
	}
	return rt.callerAttrs
}

// setCallerAttrs stores the caller's log fields; the caller holds rt.mu.
func (rt *requestTelemetry) setCallerAttrs(c requestCaller) {
	rt.callerResolved = true
	if n := callerAttrCount(c); n > 0 {
		rt.callerAttrs = appendCallerAttrs(make([]any, 0, n), c)
	}
}

// recordCallerCost adds what a finished client request cost to its tenant's VictoriaLogs cost counters
// (loki_vl_proxy_tenant_vl_*_total, with -metrics.export-sensitive-labels) and, with
// -server.enable-query-analytics, to the /debug/queries callers view. Live tail is left out: it lasts as long as
// the client keeps it open.
// The metric takes the tenant as sent (metricTenant), like loki_vl_proxy_tenant_requests_total.
func (p *Proxy) recordCallerCost(ctx context.Context, metricTenant, query string, status int, elapsed time.Duration, vl vlCost) {
	meta := requestRouteMetaFromContext(ctx)
	endpoint, route := meta.endpoint, meta.route
	if slowRequestExempt(endpoint, route) {
		return
	}
	p.metrics.RecordTenantVLCost(metricTenant, vl.calls, vl.dur, vl.bytes, vl.rows)
	if !p.enableQueryAnalytics || p.queryTracker == nil {
		return
	}
	c := p.requestCallerFromContext(ctx)
	if rt := getRequestTelemetry(ctx); rt != nil {
		// The request line, written next, reuses this resolution.
		rt.mu.Lock()
		if !rt.callerResolved {
			rt.setCallerAttrs(c)
		}
		rt.mu.Unlock()
	}
	req := metrics.CallerRequest{
		Tenant: c.tenant, User: c.user, UserSource: c.userSource, GrafanaSource: c.sourceTag,
		Endpoint: endpoint, Route: route, Status: status, Query: query,
		CallerCost: metrics.CallerCost{
			DurationMs: elapsed.Milliseconds(), VLCalls: int64(vl.calls), VLMs: vl.dur.Milliseconds(),
			VLBytes: vl.bytes, VLRows: vl.rows,
		},
	}
	if status >= http.StatusBadRequest {
		req.Errors = 1
	}
	if p.slowQueryThreshold > 0 && elapsed >= p.slowQueryThreshold {
		req.Slow = 1
	}
	p.queryTracker.RecordRequest(req)
}

// callerAttrCount is the length appendCallerAttrs gives c, so the shared slice is allocated at its size.
func callerAttrCount(c requestCaller) int {
	n := 0
	if c.user != "" {
		n += 4
		if deriveEnduserName(c.user, c.userSource) != "" {
			n += 2
		}
	}
	if c.sourceTag != "" {
		n += 2
	}
	if c.surface != "" {
		n += 2
	}
	if c.queryTags != "" && !queryTagsAreSource(c.queryTags, c.sourceTag) {
		n += 2
	}
	return n
}

// appendCallerAttrs adds the caller to a log line: the end user and its source, its name when a user header
// named it, and the Grafana app's tags when present.
func appendCallerAttrs(attrs []any, c requestCaller) []any {
	if c.user != "" {
		user := any(c.user) // boxed once: enduser.name is the same value when a user header named the user
		attrs = append(attrs, "enduser.id", user, "enduser.source", c.userSource)
		if deriveEnduserName(c.user, c.userSource) != "" {
			attrs = append(attrs, "enduser.name", user)
		}
	}
	if c.sourceTag != "" {
		attrs = append(attrs, "grafana.client.source_tag", c.sourceTag)
	}
	if c.surface != "" {
		attrs = append(attrs, "grafana.client.surface", c.surface)
	}
	if c.queryTags != "" && !queryTagsAreSource(c.queryTags, c.sourceTag) {
		attrs = append(attrs, "grafana.query_tags", c.queryTags)
	}
	return attrs
}

// queryTagsAreSource reports X-Query-Tags that hold nothing but the Source tag the line already carries.
func queryTagsAreSource(tags, source string) bool {
	k, v, ok := strings.Cut(tags, "=")
	return ok && strings.EqualFold(strings.TrimSpace(k), "source") && strings.Trim(strings.TrimSpace(v), `"`) == source
}

// appendTenantScopeAttrs adds where the tenant's queries go in VictoriaLogs: the AccountID and ProjectID the
// proxy sends for it, or the tenant label field in label routing. Nothing is added for a request without a
// tenant scope of its own: a multi-tenant org (a|b; its VictoriaLogs calls carry each tenant's scope) or a refused
// one.
func (p *Proxy) appendTenantScopeAttrs(ctx context.Context, attrs []any, orgID string) []any {
	if strings.Contains(orgID, "|") {
		return attrs
	}
	routing := p.routingForContext(ctx)
	route := p.resolveTenant(orgID, routing)
	if route.err != nil {
		return attrs
	}
	switch {
	case routing.label != "":
		return append(attrs, "vl.tenant_label", routing.label)
	case route.kind == tenantMapped:
		return append(attrs, "vl.account_id", route.mapping.AccountID, "vl.project_id", route.mapping.ProjectID)
	case route.kind == tenantNumeric:
		return append(attrs, "vl.account_id", orgID, "vl.project_id", "0")
	case route.kind == tenantDefault:
		return append(attrs, "vl.account_id", "0", "vl.project_id", "0")
	}
	return attrs
}

// sanitizeCallerValue makes a client-sent value safe for a log line and the admin view: trimmed, capped at max
// bytes (on a rune boundary, "..." marking the cut), control characters (C0, DEL, C1, U+2028, U+2029) and invalid
// UTF-8 escaped as \xNN or \uNNNN so a value cannot forge a line or a field. A clean value is returned as is,
// without allocating.
func sanitizeCallerValue(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max && !callerValueNeedsEscape(s) {
		return s
	}
	var b strings.Builder
	b.Grow(min(len(s), max) + 8)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		var piece string
		switch {
		case r == utf8.RuneError && size == 1:
			piece = `\x` + hexByte(s[i])
		case callerRuneEscaped(r):
			piece = `\u` + hexByte(byte(r>>8)) + hexByte(byte(r))
		default:
			piece = s[i : i+size]
		}
		if b.Len()+len(piece) > max {
			b.WriteString("...")
			break
		}
		b.WriteString(piece)
		i += size
	}
	return b.String()
}

func callerValueNeedsEscape(s string) bool {
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c < 0x7f {
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if (r == utf8.RuneError && size == 1) || callerRuneEscaped(r) {
			return true
		}
		i += size
	}
	return false
}

func callerRuneEscaped(r rune) bool {
	return r < 0x20 || (r >= 0x7f && r <= 0x9f) || r == 0x2028 || r == 0x2029
}

func hexByte(c byte) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[c>>4], digits[c&0xf]})
}
