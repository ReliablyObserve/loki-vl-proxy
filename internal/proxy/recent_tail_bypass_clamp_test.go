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
	if p.shouldBypassRecentTailCache("query_range", ttl-2*time.Second, r) {
		t.Error("cacheAge 2s should not bypass (below clamped 5s)")
	}
	// remaining=3s -> cacheAge=7s (>= 5s) -> bypass (near-now). Pre-fix this was
	// false because 7s < 15s configured max-staleness, so live tail went stale.
	if !p.shouldBypassRecentTailCache("query_range", ttl-7*time.Second, r) {
		t.Error("cacheAge 7s near-now should bypass with the TTL clamp (live-tail freshness)")
	}

	// Sanity: a request NOT ending near now must never bypass (historical cache preserved).
	oldEnd := nowNs - (10 * time.Minute).Nanoseconds()
	rOld := httptest.NewRequest("GET", "/loki/api/v1/query_range?end="+strconv.FormatInt(oldEnd, 10), nil)
	if p.shouldBypassRecentTailCache("query_range", ttl-7*time.Second, rOld) {
		t.Error("a non-near-now request must not bypass the cache")
	}
}
