package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type requestRoutingKey struct{}

// Configuration maps are immutable after publication. A request and its child
// work retain the same map across reload; a child tenant resolves within it.
type requestRouting struct {
	tenants    map[string]TenantMapping
	label      string
	translator *LabelTranslator
	namespace  string
}

func (p *Proxy) routingForContext(ctx context.Context) requestRouting {
	if routing, ok := ctx.Value(requestRoutingKey{}).(requestRouting); ok {
		return routing
	}
	if original, ok := ctx.Value(origRequestKey).(*http.Request); ok && original != nil {
		if routing, ok := original.Context().Value(requestRoutingKey{}).(requestRouting); ok {
			return routing
		}
	}
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return requestRouting{p.tenantMap, p.tenantLabel, p.labelTranslator, p.routingNamespace}
}

func (p *Proxy) withRequestScope(r *http.Request) *http.Request {
	routing := p.routingForContext(r.Context())
	if getOrgID(r.Context()) != r.Header.Get("X-Scope-OrgID") {
		r = r.WithContext(context.WithValue(r.Context(), authFingerprintKey, nil))
	}
	r = r.WithContext(context.WithValue(r.Context(), requestRoutingKey{}, routing))
	r = withOrgID(r)
	if _, ok := r.Context().Value(authFingerprintKey).(string); !ok {
		r = p.injectAuthFingerprint(r)
	}
	return r
}

func (p *Proxy) contextScopeFingerprint(ctx context.Context) string {
	if original, ok := ctx.Value(origRequestKey).(*http.Request); ok && original != nil {
		return p.fingerprintFromCtx(ctx, original.WithContext(ctx))
	}
	r := (&http.Request{Header: make(http.Header)}).WithContext(ctx)
	r.Header.Set("X-Scope-OrgID", getOrgID(ctx))
	return p.forwardedAuthFingerprint(r)
}

// scopeFingerprint is deterministic across replicas/restarts, excludes learned
// aliases, and versions all existing response/cache identities. Never expose its
// input (which can contain backend credentials) in logs or cache key text.
func (p *Proxy) scopeFingerprint(ctx context.Context, orgID string) string {

	routing := p.routingForContext(ctx)
	namespace := routing.namespace
	if namespace == "" {
		namespace = p.buildRoutingNamespace(routing)
	}
	if orgID == "" {
		return namespace
	}
	sum := sha256.Sum256([]byte(namespace + orgID))
	return hex.EncodeToString(sum[:])
}

// Called at startup/reload. Only a namespace digest is retained; credentials
// never become cache-key text. Including all mappings also invalidates sibling
// tenant caches on reload, without any unbounded per-tenant memo table.
func (p *Proxy) buildRoutingNamespace(routing requestRouting) string {
	backend, cold := "", ""
	if p.backend != nil {
		backend = p.backend.String()
	}
	if p.coldRouter != nil {
		cold = p.coldRouter.coldBackend.String()
	}
	var style LabelStyle
	var fields map[string]string
	var otel bool
	if routing.translator != nil {
		style, fields, otel = routing.translator.style, routing.translator.vlToLoki, routing.translator.translateOTel
	}
	data, _ := json.Marshal([]any{"scope-v3", backend, cold, p.backendHeaders, routing.tenants, routing.label, style, fields, otel, p.forwardTenantHeader})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// tenantFilterNeverMatches is a tenant-label value no request can carry (org IDs
// with a newline are refused), used where a request must see no stream.
const tenantFilterNeverMatches = "\n"

func (p *Proxy) scopedTenantParams(ctx context.Context, params url.Values) url.Values {
	routing := p.routingForContext(ctx)
	orgID := getOrgID(ctx)
	if _, mapped := routing.tenants[orgID]; (mapped && orgID != "") || routing.label == "" {
		return params
	}
	// Defensive twin of the admission rule: only the explicit global bypass may
	// leave a label-mode request unfiltered; anything else is confined to its org.
	if orgID == "" || isDefaultTenantAlias(orgID) || orgID == "*" {
		if p.globalTenantAllowed() {
			return params
		}
		// Not admitted, so it must read nothing: an empty value would match streams
		// that lack the label. A newline is rejected in every org ID, so no tenant
		// owns it.
		orgID = tenantFilterNeverMatches
	}
	return injectTenantLabelFilter(params, routing.label, orgID)
}

// tenantRouteKind is how an org ID reaches VictoriaLogs.
type tenantRouteKind uint8

const (
	tenantRefused tenantRouteKind = iota // not served: admission answers 403
	tenantMapped                         // AccountID/ProjectID of its tenant-map entry
	tenantNumeric                        // AccountID is the org itself, ProjectID 0
	tenantDefault                        // VictoriaLogs default tenant 0:0, on purpose
)

// tenantRoute is the outcome of resolveTenant. warn is set only on a refusal an
// operator can act on, for the one log line it earns.
type tenantRoute struct {
	kind    tenantRouteKind
	warn    tenantRefusalKind
	mapping TenantMapping
	err     *requestPolicyError
}

type tenantRefusalKind uint8

const (
	refusalQuiet        tenantRefusalKind = iota
	refusalNonCanonical                   // a number spelled other than canonically
	refusalStrict                         // strict routing: not in the tenant map
)

// reasonAndFix is the operator-facing text of a refusal log line.
func (k tenantRefusalKind) reasonAndFix() (reason, fix string) {
	if k == refusalNonCanonical {
		return "non-canonical numeric tenant; Loki treats 007 and 7 as different tenants, so it is not mapped to VictoriaLogs account 7",
			"add it to -tenant-map, or send the canonical number"
	}
	return "strict tenant routing (-tenant.numeric-passthrough=false) routes only the tenants listed in the tenant map",
		"add it to -tenant-map, or set -tenant.numeric-passthrough=true"
}

// IsCanonicalTenantNumber reports whether s is a plain decimal uint32: digits
// only, no sign, no leading zero except "0" itself, at most 4294967295. Loki
// keeps "007" and "7" as two tenants, so only this one spelling of a number may
// name the VictoriaLogs AccountID of that number.
func IsCanonicalTenantNumber(s string) bool {
	if _, err := strconv.ParseUint(s, 10, 32); err != nil {
		return false
	}
	return s == "0" || s[0] != '0'
}

// isNumericTenantLike reports the shapes strconv.Atoi used to accept: an
// optional sign followed by decimal digits only. They are refused unless
// canonical; a name such as 2024-01-01 is an ordinary unknown tenant.
func isNumericTenantLike(s string) bool {
	if s != "" && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// resolveTenant is the one place an org ID becomes a VictoriaLogs tenant. Both
// admission and the AccountID/ProjectID headers of every backend request
// (queries, tail, ruler/alerts passthrough, cold tier) read its answer, so an
// admitted org is always either routed to a tenant or deliberately the default
// tenant, and never falls through to 0:0 by accident.
func (p *Proxy) resolveTenant(orgID string, routing requestRouting) tenantRoute {
	tenantLabel := routing.label
	strict := p.strictTenantRouting && tenantLabel == ""
	// An absent org never receives a mapping's tenant headers, so a mapping under
	// the empty key never serves it: it is the default tenant where that is
	// allowed, and refused where only an explicit tenant may be served.
	if orgID == "" {
		switch {
		case tenantLabel != "" && !p.globalTenantAllowed():
			return tenantRoute{err: &requestPolicyError{status: http.StatusForbidden, msg: "missing X-Scope-OrgID is not allowed in tenant-label mode"}}
		case strict && !p.globalTenantAllowed():
			return tenantRoute{err: &requestPolicyError{status: http.StatusForbidden, msg: "missing X-Scope-OrgID is not allowed when -tenant.numeric-passthrough=false"}}
		}
		return tenantRoute{kind: tenantDefault}
	}
	if mapping, ok := routing.tenants[orgID]; ok {
		return tenantRoute{kind: tenantMapped, mapping: mapping}
	}
	// Explicit mappings take precedence, but label routing must not turn an
	// unmapped wildcard into an implicit authorization to query every tenant.
	if orgID == "*" {
		if p.globalTenantAllowed() {
			return tenantRoute{kind: tenantDefault}
		}
		return tenantRoute{err: &requestPolicyError{status: http.StatusForbidden, msg: `global tenant bypass ("*") is disabled`}}
	}

	// In label-based routing mode any org ID is accepted: isolation is enforced
	// by injecting a LogsQL label filter into the query, not via AccountID headers.
	if tenantLabel != "" {
		// The default aliases carry no tenant label to filter on, so they are
		// held to the same gate as "*".
		if isDefaultTenantAlias(orgID) && !p.globalTenantAllowed() {
			return tenantRoute{err: &requestPolicyError{status: http.StatusForbidden, msg: fmt.Sprintf("global tenant bypass (%q) is disabled", orgID)}}
		}
		// orgID is injected into LogsQL queries as a label value.
		// Reject characters that cannot be safely embedded: newlines, carriage returns, null bytes.
		for _, ch := range orgID {
			if ch == '\n' || ch == '\r' || ch == 0 {
				return tenantRoute{err: &requestPolicyError{msg: "invalid character in X-Scope-OrgID for tenant-label mode", status: http.StatusBadRequest}}
			}
		}
		return tenantRoute{kind: tenantDefault}
	}

	if isDefaultTenantAlias(orgID) {
		// Strict routing sends only mapped tenants to a VictoriaLogs account, so
		// the default tenant is reached only on purpose, like "*".
		if strict && !p.globalTenantAllowed() {
			return tenantRoute{err: &requestPolicyError{status: http.StatusForbidden, msg: fmt.Sprintf("global tenant bypass (%q) is disabled", orgID)}}
		}
		return tenantRoute{kind: tenantDefault}
	}
	switch {
	case strict:
		return tenantRoute{
			err:  &requestPolicyError{status: http.StatusForbidden, msg: fmt.Sprintf("unknown tenant %q: -tenant.numeric-passthrough=false routes only the tenants listed in the tenant map", orgID)},
			warn: refusalStrict,
		}
	case IsCanonicalTenantNumber(orgID):
		return tenantRoute{kind: tenantNumeric}
	case isNumericTenantLike(orgID):
		return tenantRoute{
			err:  &requestPolicyError{status: http.StatusForbidden, msg: fmt.Sprintf("unknown tenant %q: numeric tenant IDs must be canonical decimal numbers up to 4294967295 (no sign, no leading zeros) unless listed in the tenant map", orgID)},
			warn: refusalNonCanonical,
		}
	}
	return tenantRoute{err: &requestPolicyError{status: http.StatusForbidden, msg: fmt.Sprintf("unknown tenant %q", orgID)}}
}

// tenantRefusedHeader is an AccountID VictoriaLogs rejects. It is set when a
// request reaches the backend with an org that admission would have refused
// (not possible through a routed handler), so the backend fails the request
// instead of reading the default tenant.
const tenantRefusedHeader = "refused"

func (p *Proxy) setResolvedTenantHeaders(req *http.Request, forwardOrg bool) {
	orgID := getOrgID(req.Context())
	if orgID == "" {
		return
	}
	if forwardOrg {
		req.Header.Set("X-Scope-OrgID", orgID)
	}
	route := p.resolveTenant(orgID, p.routingForContext(req.Context()))
	switch route.kind {
	case tenantMapped:
		req.Header.Set("AccountID", route.mapping.AccountID)
		req.Header.Set("ProjectID", route.mapping.ProjectID)
	case tenantNumeric:
		req.Header.Set("AccountID", orgID)
		req.Header.Set("ProjectID", "0")
	case tenantRefused:
		req.Header.Set("AccountID", tenantRefusedHeader)
	}
}

// forwardedIdentityHeaders is shared by dispatch, fingerprints and background
// snapshots. Explicit forwarding wins over implicitly trusted header values.
func (p *Proxy) forwardedIdentityHeaders(r *http.Request) http.Header {
	headers := make(http.Header)
	if p.metricsTrustProxyHeaders {
		for _, names := range [][]string{trustedIdentityHeaders, trustedProxyForwardHeaders} {
			for _, name := range names {
				if value := strings.TrimSpace(r.Header.Get(name)); value != "" {
					headers.Set(name, value)
				}
			}
		}
	}
	for _, name := range p.forwardHeaders {
		if value := r.Header.Get(name); value != "" {
			headers.Set(name, value)
		}
	}
	return headers
}

func (p *Proxy) scopedIndexOrg(r *http.Request, orgID string) string {
	if r == nil {
		r = &http.Request{Header: http.Header{"X-Scope-Orgid": {orgID}}}
	}
	return orgID + ":scope:" + p.fingerprintFromCtx(r.Context(), r)
}

// coldPost carries routing only. Hot-backend service credentials must never be
// copied to a separately configured cold host.
func (p *Proxy) coldPost(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	routing := p.routingForContext(ctx)
	ctx = context.WithValue(ctx, requestRoutingKey{}, routing)
	params = p.withBackendTimeoutArg(ctx, path, p.scopedTenantParams(ctx, params))
	// Cold-tier fetches read the same shapes from another VictoriaLogs node and
	// go through the same heavy-query admission.
	admission, err := p.admitBackendRequest(ctx, path, params)
	if err != nil {
		return nil, err
	}
	resp, err := p.coldRouter.coldRequest(ctx, http.MethodPost, path, params, func(req *http.Request) { p.setResolvedTenantHeaders(req, true) }, p.doBackendRequest)
	admission.observe(ctx, resp, err)
	if err != nil {
		admission.Release()
		return nil, err
	}
	return attachRelease(resp, admission.Release), nil
}
