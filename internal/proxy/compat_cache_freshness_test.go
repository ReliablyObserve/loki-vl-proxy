package proxy

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// TestCompatCacheFreshnessBypass covers the live-tail Explore "refresh doesn't add
// new logs" regression: compatCacheMiddleware caches query_range for 5min and the
// hit path had no freshness check, so near-now refreshes served stale data. The
// bypass re-fetches a near-now request once the cached entry exceeds max-staleness,
// while leaving historical (not-near-now) queries fully cached.
func TestCompatCacheFreshnessBypass(t *testing.T) {
	p := &Proxy{
		recentTailRefreshEnabled:      true,
		recentTailRefreshWindow:       2 * time.Minute,
		recentTailRefreshMaxStaleness: 2 * time.Second,
	}
	const ttl = 5 * time.Minute // compat cache TTL for query_range
	nowNs := time.Now().UnixNano()
	near := httptest.NewRequest("GET", "/loki/api/v1/query_range?end="+strconv.FormatInt(nowNs, 10), nil)
	old := httptest.NewRequest("GET", "/loki/api/v1/query_range?end="+strconv.FormatInt(nowNs-(10*time.Minute).Nanoseconds(), 10), nil)

	// near-now, entry 1s old (< 2s staleness) -> serve cached.
	if p.compatCacheShouldBypassForFreshness("query_range", near, ttl, ttl-1*time.Second) {
		t.Error("1s-old near-now entry should still be served (below max-staleness)")
	}
	// near-now, entry 3s old (>= 2s) -> bypass (fetch fresh).
	if !p.compatCacheShouldBypassForFreshness("query_range", near, ttl, ttl-3*time.Second) {
		t.Error("3s-old near-now entry should bypass for freshness")
	}
	// historical query: never bypass, even when very stale.
	if p.compatCacheShouldBypassForFreshness("query_range", old, ttl, ttl-4*time.Minute) {
		t.Error("historical (not near-now) query must keep the cache")
	}
	// feature disabled -> never bypass.
	pOff := &Proxy{recentTailRefreshEnabled: false, recentTailRefreshWindow: 2 * time.Minute, recentTailRefreshMaxStaleness: 2 * time.Second}
	if pOff.compatCacheShouldBypassForFreshness("query_range", near, ttl, ttl-3*time.Second) {
		t.Error("disabled recent-tail-refresh must never bypass")
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestCompatCacheFreshnessBypass_MetadataEndpointsUseTheMetadataWindow(t *testing.T) {
	p := &Proxy{metadataCacheFreshness: 24 * time.Hour, recentTailRefreshMaxStaleness: 2 * time.Second}
	const ttl = 5 * time.Minute
	nowNs := time.Now().UnixNano()
	at := func(ago time.Duration) *http.Request {
		return httptest.NewRequest("GET", "/x?end="+strconv.FormatInt(nowNs-ago.Nanoseconds(), 10), nil)
	}
	for _, endpoint := range []string{"labels", "label_values", "series"} {
		if !p.compatCacheShouldBypassForFreshness(endpoint, at(10*time.Hour), ttl, ttl-3*time.Second) {
			t.Errorf("%s: a request ending 10h ago is inside the 24h window and 3s old: bypass", endpoint)
		}
		if p.compatCacheShouldBypassForFreshness(endpoint, at(10*time.Hour), ttl, ttl-time.Second) {
			t.Errorf("%s: a 1s-old entry is below max-staleness", endpoint)
		}
		if p.compatCacheShouldBypassForFreshness(endpoint, at(25*time.Hour), ttl, ttl-4*time.Minute) {
			t.Errorf("%s: a request ending 25h ago keeps the cache", endpoint)
		}
	}
	if p.compatCacheShouldBypassForFreshness("query_range", at(10*time.Hour), ttl, ttl-3*time.Second) {
		t.Error("query_range is not governed by the metadata window")
	}
	p.metadataCacheFreshness = 0
	if p.compatCacheShouldBypassForFreshness("labels", at(0), ttl, ttl-3*time.Second) {
		t.Error("freshness 0 must keep the previous caching")
	}
}
