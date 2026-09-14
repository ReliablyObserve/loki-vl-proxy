package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func (p *Proxy) scopedTenantParams(ctx context.Context, params url.Values) url.Values {
	routing := p.routingForContext(ctx)
	orgID := getOrgID(ctx)
	if _, mapped := routing.tenants[orgID]; !mapped && routing.label != "" && orgID != "" && !isDefaultTenantAlias(orgID) && orgID != "*" {
		return injectTenantLabelFilter(params, routing.label, orgID)
	}
	return params
}

func (p *Proxy) setResolvedTenantHeaders(req *http.Request, forwardOrg bool) {
	orgID := getOrgID(req.Context())
	if orgID == "" {
		return
	}
	if forwardOrg {
		req.Header.Set("X-Scope-OrgID", orgID)
	}
	routing := p.routingForContext(req.Context())
	if mapping, ok := routing.tenants[orgID]; ok {
		req.Header.Set("AccountID", mapping.AccountID)
		req.Header.Set("ProjectID", mapping.ProjectID)
		return
	}
	if routing.label != "" || isDefaultTenantAlias(orgID) || orgID == "*" {
		return
	}
	if _, err := strconv.Atoi(orgID); err == nil {
		req.Header.Set("AccountID", orgID)
		req.Header.Set("ProjectID", "0")
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
	return p.coldRouter.coldRequest(ctx, http.MethodPost, path, p.scopedTenantParams(ctx, params), func(req *http.Request) { p.setResolvedTenantHeaders(req, true) }, p.doBackendRequest)
}
