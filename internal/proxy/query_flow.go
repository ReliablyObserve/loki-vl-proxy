package proxy

import (
	"context"
	"log/slog"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// The query flow: what one client request caused. Each request counts its LogQL parses, translations, cache
// lookups (by tier and result) and VictoriaLogs calls, with the time each took and the bytes the cache and
// VictoriaLogs returned. The request log, the slow-query log and loki_vl_proxy_request_phase_seconds /
// loki_vl_proxy_request_fanout carry it.

// The cache tiers a lookup is counted under; "unknown" is a lookup that does not say which tier answered (a miss
// of the full stack consulted all of them).
const (
	flowTierL1 = iota
	flowTierL2
	flowTierL3
	flowTierUnknown
	flowTierCount
)

var flowTierNames = [flowTierCount]string{"l1_memory", "l2_disk", "l3_peer", "unknown"}

func flowTierIndex(tier string) int {
	for i, name := range flowTierNames {
		if name == tier {
			return i
		}
	}
	return flowTierUnknown
}

type flowTierCounts struct{ hits, misses, stale int }

// flowCounters is a request's flow. Times are sums of the calls' own times: calls that ran in parallel add up to
// more than the request lasted.
type flowCounters struct {
	parseCount, translateCount int
	parseDur, translateDur     time.Duration
	cache                      [flowTierCount]flowTierCounts
	cacheDur                   time.Duration
	cacheBytes                 int64
	vlCalls                    int
	vlDur                      time.Duration
	vlBytes                    int64
}

func (f *flowCounters) cacheLookups() int { return f.cacheHits() + f.cacheMisses() + f.cacheStale() }

func (f *flowCounters) cacheHits() (n int) {
	for _, t := range f.cache {
		n += t.hits
	}
	return n
}

func (f *flowCounters) cacheMisses() (n int) {
	for _, t := range f.cache {
		n += t.misses
	}
	return n
}

func (f *flowCounters) cacheStale() (n int) {
	for _, t := range f.cache {
		n += t.stale
	}
	return n
}

// proxyDur estimates the request's own compute time: what is left of its duration after the VictoriaLogs and cache
// time, never below zero (parallel calls add up to more than the request lasted).
func (f *flowCounters) proxyDur(elapsed time.Duration) time.Duration {
	return max(elapsed-f.vlDur-f.cacheDur, 0)
}

func (f *flowCounters) metricsFlow(elapsed time.Duration) metrics.RequestFlow {
	return metrics.RequestFlow{
		ParseCount: f.parseCount, TranslateCount: f.translateCount, CacheLookups: f.cacheLookups(),
		CacheHits: f.cacheHits(), VLCalls: f.vlCalls,
		Parse: f.parseDur, Translate: f.translateDur, Cache: f.cacheDur, VL: f.vlDur, Proxy: f.proxyDur(elapsed),
	}
}

// flowByTier is the by_tier field: per tier that was read, its hits, misses and stale reads. It writes itself as a
// group, which the JSON handler nests as an object without reflecting over a map.
type flowByTier [flowTierCount]flowTierCounts

func (t flowByTier) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, flowTierCount)
	for i, c := range t {
		if c.hits+c.misses+c.stale > 0 {
			attrs = append(attrs, slog.Attr{Key: flowTierNames[i], Value: slog.GroupValue(
				slog.Int("hits", c.hits), slog.Int("misses", c.misses), slog.Int("stale", c.stale))})
		}
	}
	return slog.GroupValue(attrs...)
}

// logAttrs is the flow group of the request and slow_request lines.
func (f *flowCounters) logAttrs(elapsed time.Duration) []any {
	return []any{
		"flow.parse.count", f.parseCount,
		"flow.parse.ms", f.parseDur.Milliseconds(),
		"flow.translate.count", f.translateCount,
		"flow.translate.ms", f.translateDur.Milliseconds(),
		"flow.cache.lookups", f.cacheLookups(),
		"flow.cache.hits", f.cacheHits(),
		"flow.cache.misses", f.cacheMisses(),
		"flow.cache.stale", f.cacheStale(),
		"flow.cache.ms", f.cacheDur.Milliseconds(),
		"flow.cache.bytes", f.cacheBytes,
		"flow.cache.by_tier", flowByTier(f.cache),
		"flow.vl.calls", f.vlCalls,
		"flow.vl.ms", f.vlDur.Milliseconds(),
		"flow.vl.bytes", f.vlBytes,
		"flow.proxy.ms", f.proxyDur(elapsed).Milliseconds(),
	}
}

// noFlowCtx is the context of a parse made where no request context is in reach: it is not counted in a request's
// flow. A call site passing it is one to give a request context when its callers have one.
var noFlowCtx = context.Background()

// parseLogQLExpr, parseLogQLQuery and validateLogQL are the proxy's only calls of the LogQL parser: they time the
// parse and count it in the request's flow (when ctx belongs to a request). The result is the parser's own.
func parseLogQLExpr(ctx context.Context, query string) (logqlpkg.Expr, error) {
	if rt := getRequestTelemetry(ctx); rt != nil {
		defer rt.recordParseSince(time.Now())
	}
	return logqlpkg.Parse(query)
}

func parseLogQLQuery(ctx context.Context, query string) (*logqlpkg.LogQuery, error) {
	if rt := getRequestTelemetry(ctx); rt != nil {
		defer rt.recordParseSince(time.Now())
	}
	return logqlpkg.ParseLogQuery(query)
}

func validateLogQL(ctx context.Context, query string) error {
	if rt := getRequestTelemetry(ctx); rt != nil {
		defer rt.recordParseSince(time.Now())
	}
	return logqlpkg.ParseAndValidate(query)
}

func (rt *requestTelemetry) recordParseSince(start time.Time) {
	d := time.Since(start)
	rt.mu.Lock()
	rt.flow.parseCount++
	rt.flow.parseDur += d
	rt.mu.Unlock()
}

// recordFlowVL counts a VictoriaLogs call timed to the end of its body (the slow-query log's definition).
func recordFlowVL(ctx context.Context, d time.Duration, bytes int64) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.flow.vlCalls++
	rt.flow.vlDur += d
	rt.flow.vlBytes += max(bytes, 0)
	rt.mu.Unlock()
}

// The cache reads below are the proxy's only ones: each times the lookup and records it (tier, result, bytes) in
// the request's flow, its trace and loki_vl_proxy_cache_lookup_seconds, then returns what the cache returned. A
// lookup that stands for several tiers is one lookup, counted under the tier that answered ("unknown" on a miss
// of the full stack). A stale read counts as "stale" when it finds an entry, whatever the entry's remaining TTL:
// it is only used after the fresh read missed.

func (p *Proxy) observeCacheLookup(ctx context.Context, start time.Time, tier string, found, stale bool, n int) {
	d := time.Since(start)
	result := "miss"
	switch {
	case found && stale:
		result = "stale"
	case found:
		result = "hit"
	}
	if p != nil && p.metrics != nil {
		p.metrics.RecordCacheLookup(tier, result, d)
	}
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	i := flowTierIndex(tier)
	rt.mu.Lock()
	switch result {
	case "hit":
		rt.flow.cache[i].hits++
		rt.flow.cacheBytes += int64(n)
	case "stale":
		rt.flow.cache[i].stale++
		rt.flow.cacheBytes += int64(n)
	default:
		rt.flow.cache[i].misses++
	}
	rt.flow.cacheDur += d
	rt.mu.Unlock()
	if rt.traceOn {
		traceStep(ctx, slowTraceStep{kind: "cache", route: flowTierNames[i], outcome: result, bytes: int64(n), duration: d})
	}
}

// cacheGet reads c through every tier (cache.Cache.GetWithTier).
func (p *Proxy) cacheGet(ctx context.Context, c *cache.Cache, key string) ([]byte, bool) {
	start := time.Now()
	v, tier, ok := c.GetWithTier(key)
	p.observeCacheLookup(ctx, start, tier, ok, false, len(v))
	return v, ok
}

// cacheGetWithTTL reads c's memory tier (cache.Cache.GetWithTTL).
func (p *Proxy) cacheGetWithTTL(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, bool) {
	start := time.Now()
	v, ttl, ok := c.GetWithTTL(key)
	p.observeCacheLookup(ctx, start, "l1_memory", ok, false, len(v))
	return v, ttl, ok
}

// cacheGetWithAge reads c's memory tier (cache.Cache.GetWithAge).
func (p *Proxy) cacheGetWithAge(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, time.Duration, bool) {
	start := time.Now()
	v, ttl, age, ok := c.GetWithAge(key)
	p.observeCacheLookup(ctx, start, "l1_memory", ok, false, len(v))
	return v, ttl, age, ok
}

// cacheGetShared reads c through every tier (cache.Cache.GetSharedWithTTL).
func (p *Proxy) cacheGetShared(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, string, bool) {
	start := time.Now()
	v, ttl, tier, ok := c.GetSharedWithTTL(key)
	p.observeCacheLookup(ctx, start, tier, ok, false, len(v))
	return v, ttl, tier, ok
}

// cacheGetSharedWithAge reads c through every tier (cache.Cache.GetSharedWithAge).
func (p *Proxy) cacheGetSharedWithAge(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, time.Duration, string, bool) {
	start := time.Now()
	v, ttl, age, tier, ok := c.GetSharedWithAge(key)
	p.observeCacheLookup(ctx, start, tier, ok, false, len(v))
	return v, ttl, age, tier, ok
}

// cacheGetStale reads c's memory tier, expired entries included (cache.Cache.GetStaleWithTTL).
func (p *Proxy) cacheGetStale(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, bool) {
	start := time.Now()
	v, ttl, ok := c.GetStaleWithTTL(key)
	p.observeCacheLookup(ctx, start, "l1_memory", ok, true, len(v))
	return v, ttl, ok
}

// cacheGetRecoverableStale reads c's memory and disk tiers, expired entries included
// (cache.Cache.GetRecoverableStaleWithTTLMatching).
func (p *Proxy) cacheGetRecoverableStale(ctx context.Context, c *cache.Cache, key string, usable func([]byte) bool) ([]byte, time.Duration, string, bool) {
	start := time.Now()
	v, ttl, tier, ok := c.GetRecoverableStaleWithTTLMatching(key, usable)
	p.observeCacheLookup(ctx, start, tier, ok, true, len(v))
	return v, ttl, tier, ok
}
