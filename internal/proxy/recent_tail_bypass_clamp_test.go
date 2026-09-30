package proxy

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// TestRecentTailBypass_ClampedWhenStalenessExceedsTTL covers the live-tail
// Explore "refresh doesn't add new data" regression: with the default
// max-staleness (15s) >= query_range TTL (10s) the freshness bypass could never
// fire before the entry expired, so near-now refreshes served stale cached logs
// for the full TTL. The clamp makes the bypass fire by mid-TTL.
func TestRecentTailBypass_ClampedWhenStalenessExceedsTTL(t *testing.T) {
	p := &Proxy{
		recentTailRefreshEnabled:      true,
		recentTailRefreshWindow:       2 * time.Minute,
		recentTailRefreshMaxStaleness: 15 * time.Second, // > query_range TTL (10s)
	}
	ttl := CacheTTLs["query_range"] // 10s
	nowNs := time.Now().UnixNano()
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?end="+strconv.FormatInt(nowNs, 10), nil)

	// Clamped max-staleness = ttl/2 = 5s. cacheAge = ttl - remaining.
	// remaining=8s -> cacheAge=2s (< 5s) -> NO bypass.
	if p.shouldBypassRecentTailCache("query_range", CacheTTLs["query_range"], ttl-2*time.Second, r) {
		t.Error("cacheAge 2s should not bypass (below clamped 5s)")
	}
	// remaining=3s -> cacheAge=7s (>= 5s) -> bypass (near-now). Pre-fix this was
	// false because 7s < 15s configured max-staleness, so live tail went stale.
	if !p.shouldBypassRecentTailCache("query_range", CacheTTLs["query_range"], ttl-7*time.Second, r) {
		t.Error("cacheAge 7s near-now should bypass with the TTL clamp (live-tail freshness)")
	}

	// Sanity: a request NOT ending near now must never bypass (historical cache preserved).
	oldEnd := nowNs - (10 * time.Minute).Nanoseconds()
	rOld := httptest.NewRequest("GET", "/loki/api/v1/query_range?end="+strconv.FormatInt(oldEnd, 10), nil)
	if p.shouldBypassRecentTailCache("query_range", CacheTTLs["query_range"], ttl-7*time.Second, rOld) {
		t.Error("a non-near-now request must not bypass the cache")
	}
}

// Label listings are stored with a window-scaled TTL, not CacheTTLs[endpoint].
// Deriving the age from the endpoint TTL made it negative for them (remaining
// TTL above the base TTL), so the near-now bypass never fired.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshnessBypass_AgeUsesTheTTLTheEntryWasStoredWith(t *testing.T) {
	p := &Proxy{metadataCacheFreshness: 24 * time.Hour, recentTailRefreshMaxStaleness: 2 * time.Second}
	nowNs := time.Now().UnixNano()
	ttl := metadataWindowTTL(strconv.FormatInt(nowNs-(7*24*time.Hour).Nanoseconds(), 10), strconv.FormatInt(nowNs, 10), 5*time.Minute)
	if ttl != time.Hour {
		t.Fatalf("7d labels TTL = %v, want 1h", ttl)
	}
	near := httptest.NewRequest("GET", "/loki/api/v1/labels?end="+strconv.FormatInt(nowNs, 10), nil)

	if p.shouldBypassRecentTailCache("labels", ttl, ttl-time.Second, near) {
		t.Error("1s-old entry must be served")
	}
	if !p.shouldBypassRecentTailCache("labels", ttl, ttl-3*time.Second, near) {
		t.Error("3s-old scaled-TTL entry must bypass")
	}
	if !p.shouldBypassRecentTailCache("labels", ttl, ttl-30*time.Minute, near) {
		t.Error("30m-old scaled-TTL entry must bypass")
	}
	// The endpoint base TTL (5m) against a remaining TTL above it is the old bug:
	// the age comes out negative and nothing ever bypasses.
	if p.shouldBypassRecentTailCache("labels", CacheTTLs["labels"], ttl-3*time.Second, near) {
		t.Error("guard: a base TTL below the remaining TTL yields no age")
	}
	// Just inside the window: near now; just outside: historical.
	in := httptest.NewRequest("GET", "/x?end="+strconv.FormatInt(nowNs-(23*time.Hour).Nanoseconds(), 10), nil)
	out := httptest.NewRequest("GET", "/x?end="+strconv.FormatInt(nowNs-(25*time.Hour).Nanoseconds(), 10), nil)
	if !p.shouldBypassRecentTailCache("series", ttl, ttl-time.Minute, in) {
		t.Error("a request ending 23h ago is inside the 24h window")
	}
	if p.shouldBypassRecentTailCache("series", ttl, ttl-time.Minute, out) {
		t.Error("a request ending 25h ago is outside the 24h window")
	}
	// Independent of recent-tail-refresh-enabled, and 0 disables.
	p.recentTailRefreshEnabled = false
	if !p.shouldBypassRecentTailCache("label_values", ttl, ttl-time.Minute, near) {
		t.Error("the metadata rule must not depend on recent-tail-refresh-enabled")
	}
	p.metadataCacheFreshness = 0
	if p.shouldBypassRecentTailCache("labels", ttl, ttl-time.Minute, near) {
		t.Error("freshness 0 must disable the bypass")
	}
	// query_range keeps the recent-tail window, untouched by the metadata window.
	q := &Proxy{metadataCacheFreshness: 24 * time.Hour, recentTailRefreshMaxStaleness: 2 * time.Second}
	if q.shouldBypassRecentTailCache("query_range", CacheTTLs["query_range"], time.Second, near) {
		t.Error("the metadata window must not enable the query_range bypass")
	}
}
