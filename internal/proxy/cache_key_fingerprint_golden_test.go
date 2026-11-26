package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// cacheKeyGoldenPath holds cache keys recorded from the build before cache key
// digests of credentials were keyed: configurations that carry no credential
// must keep every key byte for byte (persisted disk entries, snapshots and
// peers' copies stay valid across the upgrade).
var cacheKeyGoldenPath = filepath.Join("testdata", "cache_keys_without_credentials.json")

type cacheKeyGoldenConfig struct {
	name    string
	cfg     func() Config
	headers map[string]string
}

func cacheKeyGoldenConfigs() []cacheKeyGoldenConfig {
	base := func() Config {
		return Config{BackendURL: "http://vl.example:9428", LogLevel: "error"}
	}
	return []cacheKeyGoldenConfig{
		{name: "default", cfg: base},
		{name: "default-org", cfg: base, headers: map[string]string{"X-Scope-OrgID": "team-a"}},
		{name: "tenant-map", cfg: func() Config {
			c := base()
			c.TenantMap = map[string]TenantMapping{"team-a": {AccountID: "7", ProjectID: "3"}}
			return c
		}, headers: map[string]string{"X-Scope-OrgID": "team-a"}},
		{name: "tenant-label", cfg: func() Config {
			c := base()
			c.TenantLabel = "tenant"
			return c
		}, headers: map[string]string{"X-Scope-OrgID": "team-b"}},
		// Trusted proxy identity labels metrics and logs: a user name, not a
		// credential.
		{name: "trusted-identity", cfg: func() Config {
			c := base()
			c.MetricsTrustProxyHeaders = true
			return c
		}, headers: map[string]string{"X-Scope-OrgID": "team-a", "X-Grafana-User": "alice", "X-Forwarded-For": "10.0.0.1"}},
		// Forwarding is configured but the request carries nothing to forward.
		{name: "forwarding-configured-absent", cfg: func() Config {
			c := base()
			c.ForwardHeaders = []string{"Authorization"}
			c.ForwardCookies = []string{"session"}
			return c
		}, headers: map[string]string{"X-Scope-OrgID": "team-a", "Cookie": "other=1"}},
	}
}

func cacheKeyGoldenRecord(t *testing.T, c cacheKeyGoldenConfig) map[string]string {
	t.Helper()
	cfg := c.cfg()
	cfg.Cache = cache.New(30*time.Second, 100)
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("%s: new proxy: %v", c.name, err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	target := "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="api"} |= "x"`) + "&start=1700000000&end=1700003600&step=60"
	r := httptest.NewRequest(http.MethodGet, target, nil)
	for k, v := range c.headers {
		r.Header.Set(k, v)
	}
	r = p.withRequestScope(r)
	org := r.Header.Get("X-Scope-OrgID")
	ctx := context.WithValue(r.Context(), origRequestKey, r)
	return map[string]string{
		"forwardedAuthFingerprint": p.forwardedAuthFingerprint(r),
		"scopeFingerprint":         p.scopeFingerprint(r.Context(), org),
		"contextScopeFingerprint":  p.contextScopeFingerprint(ctx),
		"responseKey":              p.responseKey("query_range", r, r.URL.RawQuery),
		"canonicalReadCacheKey":    p.canonicalReadCacheKey("labels", org, r),
		"scopedIndexOrg":           p.scopedIndexOrg(r, org),
		"patternStateTenantKey":    p.patternStateTenantKey(r),
		"detectedFieldsCacheKey":   p.detectedFieldsCacheKey(ctx, `{app="api"}`, "1700000000", "1700003600", 1000),
		"metadataFieldNamesKey":    p.metadataFieldNamesCacheKey(ctx, "field_names", url.Values{"query": {`{app="api"}`}}),
	}
}

// conformance: cache-key-credential-digest
func TestCacheKeysWithoutCredentialsMatchRecordedKeys(t *testing.T) {
	got := map[string]map[string]string{}
	for _, c := range cacheKeyGoldenConfigs() {
		got[c.name] = cacheKeyGoldenRecord(t, c)
	}
	if os.Getenv("LVP_CACHE_KEY_GOLDEN_UPDATE") == "1" {
		data, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cacheKeyGoldenPath, append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(cacheKeyGoldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var want map[string]map[string]string
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(got) {
		t.Fatalf("golden has %d configurations, test has %d", len(want), len(got))
	}
	for name, keys := range want {
		for builder, key := range keys {
			if got[name][builder] != key {
				t.Errorf("%s/%s changed:\n got %q\nwant %q", name, builder, got[name][builder], key)
			}
		}
		if len(got[name]) != len(keys) {
			t.Errorf("%s: %d builders recorded, %d now", name, len(keys), len(got[name]))
		}
	}
}
