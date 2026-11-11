package proxy

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"
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
func newRequestCaller(r *http.Request, user, userSource string, profile grafanaClientProfile) requestCaller {
	c := requestCaller{
		user:       sanitizeCallerValue(user, callerIdentityMaxBytes),
		userSource: userSource,
		tenant:     sanitizeCallerValue(r.Header.Get("X-Scope-OrgID"), callerIdentityMaxBytes),
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

// requestCallerFromContext is the caller of the client request ctx belongs to; zero outside one. Work shared by
// coalesced requests runs in the context of the request that started it, so it is attributed to that caller.
func requestCallerFromContext(ctx context.Context) requestCaller {
	return requestRouteMetaFromContext(ctx).caller
}

// appendCallerAttrs adds the caller to a log line: the end user and its source, its name when a user header
// named it, and the Grafana app's tags when present.
func appendCallerAttrs(attrs []any, c requestCaller) []any {
	if c.user != "" {
		attrs = append(attrs, "enduser.id", c.user, "enduser.source", c.userSource)
		if name := deriveEnduserName(c.user, c.userSource); name != "" {
			attrs = append(attrs, "enduser.name", name)
		}
	}
	if c.sourceTag != "" {
		attrs = append(attrs, "grafana.client.source_tag", c.sourceTag)
	}
	if c.surface != "" {
		attrs = append(attrs, "grafana.client.surface", c.surface)
	}
	if c.queryTags != "" {
		attrs = append(attrs, "grafana.query_tags", c.queryTags)
	}
	return attrs
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
