package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Where a client request spent its time and how much work it caused: per route, the time in each phase (parse,
// translate, cache, vl, proxy) and the number of parses, translations, cache lookups, cache hits and
// VictoriaLogs calls; per cache tier and result, the time of one lookup. Every label comes from a fixed set (or
// the route set of the other client metrics), so the series count stays bounded.

var (
	flowPhases        = [...]string{"parse", "translate", "cache", "vl", "proxy"}
	flowFanoutKind    = [...]string{"parse", "translate", "cache_lookup", "cache_hit", "vl_call"}
	cacheLookupTier   = [...]string{"l1_memory", "l2_disk", "l3_peer", "unknown"}
	cacheLookupResult = [...]string{"hit", "miss", "stale"}

	// flowFanoutBuckets are small-integer buckets: most requests cause a handful of each, a wide Drilldown
	// page a few dozen.
	flowFanoutBuckets = []float64{0, 1, 2, 3, 5, 8, 13, 21, 34, 55}
	// cacheLookupBuckets reach below defaultBuckets' 1 ms: a memory hit takes microseconds.
	cacheLookupBuckets = []float64{0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.25, 1}
)

// RequestFlow is what one client request caused, as RecordRequestFlow takes it. Each duration is the sum of the
// calls' own times: calls that ran in parallel add up to more than the request lasted, and Proxy (the request's
// time minus VL and Cache, never below zero) is then an estimate.
type RequestFlow struct {
	ParseCount, TranslateCount, CacheLookups, CacheHits, VLCalls int
	Parse, Translate, Cache, VL, Proxy                           time.Duration
}

type flowRouteHistograms struct {
	phase  [len(flowPhases)]*histogram
	fanout [len(flowFanoutKind)]*histogram
}

type flowMetrics struct {
	mu          sync.RWMutex
	routes      map[string]*flowRouteHistograms
	cacheLookup [len(cacheLookupTier)][len(cacheLookupResult)]*histogram
}

func newFlowMetrics() *flowMetrics {
	f := &flowMetrics{routes: make(map[string]*flowRouteHistograms)}
	for i := range f.cacheLookup {
		for j := range f.cacheLookup[i] {
			f.cacheLookup[i][j] = newHistogramWithBuckets(cacheLookupBuckets)
		}
	}
	return f
}

func (f *flowMetrics) route(route string) *flowRouteHistograms {
	f.mu.RLock()
	h := f.routes[route]
	f.mu.RUnlock()
	if h != nil {
		return h
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if h = f.routes[route]; h == nil {
		h = &flowRouteHistograms{}
		for i := range h.phase {
			h.phase[i] = newHistogram()
		}
		for i := range h.fanout {
			h.fanout[i] = newHistogramWithBuckets(flowFanoutBuckets)
		}
		f.routes[route] = h
	}
	return h
}

// RecordRequestFlow records one client request: a phase's time when the phase ran (the proxy's own time always),
// and each fan-out count, zero included: a cache-served refresh has no VictoriaLogs calls.
func (m *Metrics) RecordRequestFlow(endpoint, route string, flow RequestFlow) {
	if m == nil || m.flow == nil {
		return
	}
	h := m.flow.route(normalizeMetricRoute(route, endpoint))
	for i, phase := range [...]struct {
		n int
		d time.Duration
	}{{flow.ParseCount, flow.Parse}, {flow.TranslateCount, flow.Translate}, {flow.CacheLookups, flow.Cache}, {flow.VLCalls, flow.VL}, {1, flow.Proxy}} {
		if phase.n > 0 {
			h.phase[i].observe(max(phase.d, 0).Seconds())
		}
	}
	for i, n := range [...]int{flow.ParseCount, flow.TranslateCount, flow.CacheLookups, flow.CacheHits, flow.VLCalls} {
		h.fanout[i].observe(float64(max(n, 0)))
	}
}

// RecordCacheLookup records the time of one cache lookup by the tier that answered it (unknown when the lookup
// does not tell) and its result: hit, miss, or stale (an expired entry served after a backend failure).
// Anything outside those sets folds into "unknown" and "miss".
func (m *Metrics) RecordCacheLookup(tier, result string, d time.Duration) {
	if m == nil || m.flow == nil {
		return
	}
	i, j := len(cacheLookupTier)-1, 1
	for k, t := range cacheLookupTier {
		if t == tier {
			i = k
		}
	}
	for k, r := range cacheLookupResult {
		if r == result {
			j = k
		}
	}
	m.flow.cacheLookup[i][j].observe(max(d, 0).Seconds())
}

func writeHistogramSeries(sb *strings.Builder, name, labels string, h histogramSnapshot) {
	for i, b := range h.buckets {
		fmt.Fprintf(sb, "%s_bucket{%s,le=\"%g\"} %d\n", name, labels, b, h.counts[i])
	}
	fmt.Fprintf(sb, "%s_bucket{%s,le=\"+Inf\"} %d\n", name, labels, h.loadCount())
	fmt.Fprintf(sb, "%s_sum{%s} %g\n", name, labels, h.loadSum())
	fmt.Fprintf(sb, "%s_count{%s} %d\n", name, labels, h.loadCount())
}

func (m *Metrics) writeRequestFlowMetrics(sb *strings.Builder) {
	if m.flow == nil {
		return
	}
	m.flow.mu.RLock()
	routes := make([]string, 0, len(m.flow.routes))
	snaps := make(map[string]*flowRouteHistograms, len(m.flow.routes))
	for route, h := range m.flow.routes {
		routes = append(routes, route)
		snaps[route] = h
	}
	m.flow.mu.RUnlock()
	sort.Strings(routes)

	sb.WriteString("# HELP loki_vl_proxy_request_phase_seconds Time one client request spent in each phase (parse, translate, cache, vl, proxy); a phase that did not run is not observed. proxy is the request's time minus vl and cache, so it includes parse and translate; parallel calls add up, so it is an estimate.\n")
	sb.WriteString("# TYPE loki_vl_proxy_request_phase_seconds histogram\n")
	for _, route := range routes {
		for i, phase := range flowPhases {
			if s := snaps[route].phase[i].snapshot(); s.count > 0 {
				writeHistogramSeries(sb, "loki_vl_proxy_request_phase_seconds", fmt.Sprintf("route=%q,phase=%q", route, phase), s)
			}
		}
	}

	sb.WriteString("# HELP loki_vl_proxy_request_fanout Work one client request caused: LogQL parses, translations, cache lookups, cache hits and VictoriaLogs calls.\n")
	sb.WriteString("# TYPE loki_vl_proxy_request_fanout histogram\n")
	for _, route := range routes {
		for i, kind := range flowFanoutKind {
			if s := snaps[route].fanout[i].snapshot(); s.count > 0 {
				writeHistogramSeries(sb, "loki_vl_proxy_request_fanout", fmt.Sprintf("route=%q,kind=%q", route, kind), s)
			}
		}
	}

	sb.WriteString("# HELP loki_vl_proxy_cache_lookup_seconds Time of one cache lookup by the tier that answered it and its result.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_lookup_seconds histogram\n")
	for i, tier := range cacheLookupTier {
		for j, result := range cacheLookupResult {
			if s := m.flow.cacheLookup[i][j].snapshot(); s.count > 0 {
				writeHistogramSeries(sb, "loki_vl_proxy_cache_lookup_seconds", fmt.Sprintf("tier=%q,result=%q", tier, result), s)
			}
		}
	}
}
