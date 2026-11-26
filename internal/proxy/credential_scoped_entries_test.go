package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// credentialScopedKeys builds the cache keys one request leads to, through
// every key builder that reads the fingerprint: in the key text under
// several spellings, or hashed.
func credentialScopedKeys(t *testing.T, p *Proxy, r *http.Request) map[string]string {
	t.Helper()
	r = p.withRequestScope(r)
	org := r.Header.Get("X-Scope-OrgID")
	ctx := context.WithValue(r.Context(), origRequestKey, r)
	extent, ok := p.statsExtentRequestFor(ctx, url.Values{
		"query": {`app:="api" | stats count() hits`}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60s"},
	})
	if !ok {
		t.Fatal("stats extent request not built")
	}
	scope, ok := p.patternScope(`{app="api"}`, "1700000000", "1700003600", "60")
	if !ok {
		t.Fatal("pattern scope not built")
	}
	return map[string]string{
		"response":        p.responseKey("query_range", r, r.URL.RawQuery),
		"canonical read":  p.canonicalReadCacheKey("labels", org, r),
		"detected fields": p.detectedFieldsCacheKey(ctx, `{app="api"}`, "1", "2", 10),
		"field names":     p.metadataFieldNamesCacheKey(ctx, "field_names", url.Values{"query": {`{app="api"}`}}),
		"label index":     "label_values_index:" + p.scopedIndexOrg(r, org),
		"window probe":    p.windowProbeKey(ctx, "probe"),
		"level chain":     levelNeedsChainKey(p, ctx, "base"),
		"volume segment":  p.volumeSegmentKeyBase(ctx, `* | stats count()`),
		"stats extent":    extent.key,
		"stats totals":    p.statsTotalsScopeOf(r).key("lines", `{app="api"}`),
		"patterns window": p.patternWindowCacheKey(r, `{app="api"}`, "60", 10, 1000, 1, 2),
		"patterns state":  p.patternStateKey(r, scope),
	}
}

// Entries computed with a caller's credentials stay on the replica that
// computed them, whichever builder made their key; keys without credentials
// stay shareable.
//
// conformance: cache-key-credential-digest
func TestCredentialScopedKeysAreNeverPushed(t *testing.T) {
	p := credentialTestProxy(t, func(c *Config) { c.CacheKeySecret = "fleet-1" })
	target := "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="api"}`) + "&start=1700000000&end=1700003600&step=60"
	withCredentials := httptest.NewRequest(http.MethodGet, target, nil)
	withCredentials.Header.Set("X-Scope-OrgID", "team-a")
	withCredentials.Header.Set("Authorization", "Basic YWxpY2U6cHc=")
	for name, key := range credentialScopedKeys(t, p, withCredentials) {
		if peerPushAllowed(key) {
			t.Errorf("%s: credential-scoped key %q may leave the replica", name, key)
		}
	}
	without := httptest.NewRequest(http.MethodGet, target, nil)
	without.Header.Set("X-Scope-OrgID", "team-a")
	for name, key := range credentialScopedKeys(t, p, without) {
		if !peerPushAllowed(key) {
			t.Errorf("%s: key %q without credentials must stay shareable", name, key)
		}
		if strings.Contains(key, identityFingerprintPrefix) {
			t.Errorf("%s: key %q without credentials carries the credential mark", name, key)
		}
	}
}

// User information in the -backend URL is a credential: it decides the
// routing namespace, but cache keys hold neither it nor an unkeyed digest of
// it, and a URL without it enters keys as before.
//
// conformance: cache-key-credential-digest
func TestBackendURLUserinfoIsNotInCacheKeys(t *testing.T) {
	plain := credentialTestProxy(t, func(c *Config) { c.ForwardHeaders, c.ForwardCookies = nil, nil })
	if got := plain.backendInKeys(); got != "http://vl.example:9428" {
		t.Fatalf("a backend URL without user information enters keys as %q", got)
	}
	withUser := func(password string) *Proxy {
		return credentialTestProxy(t, func(c *Config) {
			c.ForwardHeaders, c.ForwardCookies = nil, nil
			c.BackendURL = "http://vlreader:" + password + "@vl.example:9428"
			c.CacheKeySecret = "fleet-1"
		})
	}
	a, b := withUser("correct-horse"), withUser("other")
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1&end=2&step=60", nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	scope, _ := a.patternScope(`{app="api"}`, "1700000000", "1700003600", "60")
	for name, text := range map[string]string{
		"namespace":       a.routingNamespace,
		"backend in keys": a.backendInKeys(),
		"tenant state":    a.patternStateTenantKey(a.withRequestScope(r)),
		"patterns window": a.patternWindowCacheKey(a.withRequestScope(r), `{app="api"}`, "60", 10, 1000, 1, 2),
		"patterns state":  a.patternStateKey(a.withRequestScope(r), scope),
	} {
		if strings.Contains(text, "correct-horse") || strings.Contains(text, "vlreader:") {
			t.Errorf("%s holds the URL credentials: %q", name, text)
		}
	}
	if !strings.HasPrefix(a.backendInKeys(), "http://vl.example:9428#"+credentialDigestVersion) {
		t.Errorf("backend in keys %q: want the bare URL and the keyed digest", a.backendInKeys())
	}
	if a.routingNamespace == b.routingNamespace || a.backendInKeys() == b.backendInKeys() {
		t.Error("two URL credentials must give two namespaces")
	}
	if a.routingNamespace == plain.routingNamespace {
		t.Error("URL credentials must still decide the namespace")
	}
	// The requests themselves still carry the URL's credentials.
	if u := backendRequestURL(a.backend, "/select/logsql/query"); u.User == nil || u.User.String() != "vlreader:correct-horse" {
		t.Errorf("backend requests lost the URL credentials: %v", u.User)
	}
}
