package proxy

import (
	"context"
	"log/slog"
	"math"
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

// flowReads counts a request's tier reads (memory, disk, peer). Three 16-bit counts fit in padding of the
// request's telemetry, which keeps its allocation size class; a count stops at the maximum, far above the top
// histogram bucket.
type flowReads [flowTierUnknown]uint16

func (r *flowReads) add(tier int) {
	if r[tier] < math.MaxUint16 {
		r[tier]++
	}
}

func (r flowReads) counts() (out [flowTierUnknown]int) {
	for i, n := range r {
		out[i] = int(n)
	}
	return out
}

// cacheReach is the tiers a cache read method can consult, in order.
type cacheReach uint8

const (
	reachMemory     cacheReach = iota // the memory tier only
	reachLocalStale                   // memory, then disk (stale reads)
	reachStack                        // memory, disk, peers
)

// countReads adds the tier reads of one lookup of c: every attached tier the method reaches, up to the one that
// answered ("" or an unnamed tier: it read them all and none had a usable value).
func countReads(reads *flowReads, c *cache.Cache, reach cacheReach, answered string) {
	disk, peer, enabled := c.Tiers()
	if !enabled {
		return
	}
	reads.add(flowTierL1)
	if reach == reachMemory || answered == flowTierNames[flowTierL1] {
		return
	}
	if disk {
		reads.add(flowTierL2)
	}
	if reach == reachStack && answered != flowTierNames[flowTierL2] && peer {
		reads.add(flowTierL3)
	}
}

// flowCounters is a request's flow. Times are sums of the calls' own times: calls that ran in parallel add up to
// more than the request lasted.
type flowCounters struct {
	parseCount, translateCount int
	parseDur, translateDur     time.Duration
	cache                      [flowTierCount]flowTierCounts
	cacheDur                   time.Duration
	cacheBytes                 int64
	vlCalls                    int
	// vlStarted counts the VictoriaLogs calls the request handed to the
	// transport, counted as each starts (only with -debug-vl-calls-header).
	vlStarted int
	vlDur     time.Duration
	vlBytes   int64
	// vlRows counts the log rows read from VictoriaLogs raw query bodies, queueDur the time spent waiting for
	// heavy-query admission, lokiCache the lookups and stores by Loki cache section (query_stats.go).
	vlRows    int64
	queueDur  time.Duration
	lokiCache [lokiCacheSlotCount]flowLokiCache
}

// The pieces of a metadata inventory listing, by how each was answered.
const (
	flowInventoryListings    = iota // listings the inventory planned
	flowInventoryMemory             // listings answered from memory with no VictoriaLogs call
	flowInventoryCached             // buckets read from the cache
	flowInventoryRevalidated        // cached buckets a row count confirmed
	flowInventoryScanned            // buckets read from VictoriaLogs
	flowInventoryEdges              // unaligned edges, read from VictoriaLogs on every listing
	flowInventoryCount
)

// flowInventoryNames are the fields' names, held as interface values so a line appends them without converting
// (and allocating) each.
var flowInventoryNames = [flowInventoryCount]any{
	"flow.inventory.listings", "flow.inventory.memory", "flow.inventory.buckets_cached",
	"flow.inventory.buckets_revalidated", "flow.inventory.buckets_scanned", "flow.inventory.edges",
}

// flowInventory counts a request's inventory pieces (the bucket plan of #1821). Six 8-bit counts fit in padding of
// the request's telemetry, which keeps its allocation size class; a count stops at 255, above any plan of one
// listing of a week.
type flowInventory [flowInventoryCount]uint8

func (f *flowInventory) add(kind, n int) {
	f[kind] = uint8(min(int(f[kind])+max(n, 0), math.MaxUint8))
}

// appendLogAttrs appends the bucket plan to the attributes of a request or slow_request line: nothing for a
// request that planned no inventory listing.
func (f flowInventory) appendLogAttrs(attrs []any) []any {
	if f[flowInventoryListings] == 0 {
		return attrs
	}
	for i, n := range f {
		attrs = append(attrs, flowInventoryNames[i], int(n))
	}
	return attrs
}

// recordInventoryPlan counts one inventory listing of the request and how its pieces were answered.
func recordInventoryPlan(ctx context.Context, memory bool, cached, revalidated, scanned, edges int) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.inventory.add(flowInventoryListings, 1)
	if memory {
		rt.inventory.add(flowInventoryMemory, 1)
	}
	rt.inventory.add(flowInventoryCached, cached)
	rt.inventory.add(flowInventoryRevalidated, revalidated)
	rt.inventory.add(flowInventoryScanned, scanned)
	rt.inventory.add(flowInventoryEdges, edges)
	rt.mu.Unlock()
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

// parseLogQLExprTemplates is parseLogQLExpr; untyped leaves the line_format templates untyped
// (logql.ParseUntypedTemplates).
func parseLogQLExprTemplates(ctx context.Context, query string, untyped bool) (logqlpkg.Expr, error) {
	if rt := getRequestTelemetry(ctx); rt != nil {
		defer rt.recordParseSince(time.Now())
	}
	if untyped {
		return logqlpkg.ParseUntypedTemplates(query)
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

func (p *Proxy) observeCacheLookup(ctx context.Context, start time.Time, c *cache.Cache, reach cacheReach, tier, key string, found, stale bool, n int) {
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
	countReads(&rt.cacheReads, c, reach, tier)
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
	if class := lokiCacheClassOf(key); class >= 0 && lokiCacheSlots[class] >= 0 {
		lc := &rt.flow.lokiCache[lokiCacheSlots[class]]
		lc.requested++
		lc.dur += d
		if result != "miss" {
			lc.found++
			lc.received += int64(n)
		}
	}
	rt.mu.Unlock()
	if rt.traceOn {
		traceStep(ctx, slowTraceStep{kind: "cache", route: flowTierNames[i], outcome: result, bytes: int64(n), duration: d})
	}
}

// cacheGet reads c through every tier (cache.Cache.GetWithTier).
func (p *Proxy) cacheGet(ctx context.Context, c *cache.Cache, key string) ([]byte, bool) {
	start := time.Now()
	v, tier, ok := c.GetWithTier(key)
	p.observeCacheLookup(ctx, start, c, reachStack, tier, key, ok, false, len(v))
	return v, ok
}

// cachePeek reads c's memory tier without an LRU promotion (cache.Cache.Peek).
func (p *Proxy) cachePeek(ctx context.Context, c *cache.Cache, key string) ([]byte, bool) {
	start := time.Now()
	v, ok := c.Peek(key)
	p.observeCacheLookup(ctx, start, c, reachMemory, "l1_memory", key, ok, false, len(v))
	return v, ok
}

// cacheGetWithTTL reads c's memory tier (cache.Cache.GetWithTTL).
func (p *Proxy) cacheGetWithTTL(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, bool) {
	start := time.Now()
	v, ttl, ok := c.GetWithTTL(key)
	p.observeCacheLookup(ctx, start, c, reachMemory, "l1_memory", key, ok, false, len(v))
	return v, ttl, ok
}

// cacheGetWithAge reads c's memory tier (cache.Cache.GetWithAge).
func (p *Proxy) cacheGetWithAge(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, time.Duration, bool) {
	start := time.Now()
	v, ttl, age, ok := c.GetWithAge(key)
	p.observeCacheLookup(ctx, start, c, reachMemory, "l1_memory", key, ok, false, len(v))
	return v, ttl, age, ok
}

// cacheGetShared reads c through every tier (cache.Cache.GetSharedWithTTL).
func (p *Proxy) cacheGetShared(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, string, bool) {
	start := time.Now()
	v, ttl, tier, ok := c.GetSharedWithTTL(key)
	p.observeCacheLookup(ctx, start, c, reachStack, tier, key, ok, false, len(v))
	return v, ttl, tier, ok
}

// cacheGetSharedWithAge reads c through every tier (cache.Cache.GetSharedWithAge).
func (p *Proxy) cacheGetSharedWithAge(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, time.Duration, string, bool) {
	start := time.Now()
	v, ttl, age, tier, ok := c.GetSharedWithAge(key)
	p.observeCacheLookup(ctx, start, c, reachStack, tier, key, ok, false, len(v))
	return v, ttl, age, tier, ok
}

// cacheGetStale reads c's memory tier, expired entries included (cache.Cache.GetStaleWithTTL).
func (p *Proxy) cacheGetStale(ctx context.Context, c *cache.Cache, key string) ([]byte, time.Duration, bool) {
	start := time.Now()
	v, ttl, ok := c.GetStaleWithTTL(key)
	p.observeCacheLookup(ctx, start, c, reachMemory, "l1_memory", key, ok, true, len(v))
	return v, ttl, ok
}

// cacheGetRecoverableStale reads c's memory and disk tiers, expired entries included
// (cache.Cache.GetRecoverableStaleWithTTLMatching).
func (p *Proxy) cacheGetRecoverableStale(ctx context.Context, c *cache.Cache, key string, usable func([]byte) bool) ([]byte, time.Duration, string, bool) {
	start := time.Now()
	v, ttl, tier, ok := c.GetRecoverableStaleWithTTLMatching(key, usable)
	p.observeCacheLookup(ctx, start, c, reachLocalStale, tier, key, ok, true, len(v))
	return v, ttl, tier, ok
}
