package proxy

import (
	"context"
	"net/http/httptest"
	"testing"
)

// Every cache key family that carries the auth fingerprint is kept off the
// peer replicas when the request forwards a user's credentials, and pushed
// when it does not: the read cache keys (labels, values, series, query
// results), the detected fields and labels keys and the patterns keys.
//
// conformance: peer-cache-replication
func TestPeerPush_IdentityKeysStayLocal(t *testing.T) {
	p := newOpt5Proxy(t)
	p.metricsTrustProxyHeaders = true
	for _, tc := range []struct {
		name     string
		header   string
		identity bool
	}{
		{"forwarded header", "X-Auth-Token", true},
		{"forwarded cookie", "Cookie", true},
		{"trusted proxy identity only (metrics labels)", "X-Grafana-User", false},
		{"no credentials", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/loki/api/v1/labels?start=1&end=2", nil)
			r.Header.Set("X-Scope-OrgID", "team-a")
			switch tc.header {
			case "":
			case "Cookie":
				r.Header.Set("Cookie", "session=s1")
			default:
				r.Header.Set(tc.header, "user-a")
			}
			r = p.injectAuthFingerprint(r)
			ctx := context.WithValue(r.Context(), origRequestKey, r)
			ctx = context.WithValue(ctx, orgIDKey, "team-a")
			fp := p.fingerprintFromCtx(r.Context(), r)
			keys := map[string]string{
				"read cache":      p.canonicalReadCacheKey("labels", "team-a", r),
				"detected fields": p.detectedFieldsCacheKey(ctx, "{app=\"a\"}", "1", "2", 100),
				"detected labels": p.detectedLabelsCacheKey(ctx, "{app=\"a\"}", "1", "2", 100),
				"patterns":        p.patternsAutodetectCacheKey("team-a", fp, "{app=\"a\"}", "1", "2", "60s"),
			}
			for family, key := range keys {
				if key == "" {
					t.Fatalf("%s: no key", family)
				}
				if got := peerPushAllowed(key); got == tc.identity {
					t.Errorf("%s key %q: push allowed %v", family, key, got)
				}
			}
		})
	}
}
