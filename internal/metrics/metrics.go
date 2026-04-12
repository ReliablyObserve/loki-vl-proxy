package metrics

import (
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects proxy metrics in Prometheus exposition format.
type Metrics struct {
	mu sync.RWMutex

	// Request counters by endpoint and status
	requestsTotal    map[string]*atomic.Int64 // "endpoint:status" → count
	requestDurations map[string]*histogram    // "endpoint" → duration histogram

	// Per-tenant request counters
	tenantRequests  map[string]*atomic.Int64 // "tenant:endpoint:status" → count
	tenantDurations map[string]*histogram    // "tenant:endpoint" → duration histogram

	// Cache stats (global)
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64

	// Cache stats (per-endpoint)
	endpointCacheHits   map[string]*atomic.Int64 // "endpoint" → hits
	endpointCacheMisses map[string]*atomic.Int64 // "endpoint" → misses

	// Backend latency (VL response time, separate from total proxy latency)
	backendDurations map[string]*histogram // "endpoint" → VL duration histogram

	// Singleflight coalescing stats
	coalescedTotal atomic.Int64 // requests served from coalesced results
	coalescedSaved atomic.Int64 // backend requests saved by coalescing

	// Translation stats
	translationsTotal atomic.Int64
	translationErrors atomic.Int64

	// Tuple emission mode stats
	tupleModes map[string]*atomic.Int64 // "mode" -> count

	// Query-range windowing stats
	windowCacheHits               atomic.Int64
	windowCacheMisses             atomic.Int64
	windowFetch                   *histogram
	windowMerge                   *histogram
	windowCount                   *histogram
	windowPrefilterAttempts       atomic.Int64
	windowPrefilterErrors         atomic.Int64
	windowPrefilterKept           atomic.Int64
	windowPrefilterSkipped        atomic.Int64
	windowPrefilterDuration       *histogram
	windowAdaptiveParallelCurrent atomic.Int64
	windowAdaptiveLatencyEWMAms   atomic.Int64
	windowAdaptiveErrorEWMAppm    atomic.Int64

	// Client error tracking
	clientErrors map[string]*atomic.Int64 // "endpoint:reason" → count

	// Per-client identity metrics
	clientRequests     map[string]*atomic.Int64 // "client:endpoint" → count
	clientDurations    map[string]*histogram    // "client:endpoint" → duration histogram
	clientBytes        map[string]*atomic.Int64 // "client" → response bytes
	clientStatuses     map[string]*atomic.Int64 // "client:endpoint:status" → count
	clientInflight     map[string]*atomic.Int64 // "client" → in-flight requests
	clientQueryLengths map[string]*histogram    // "client:endpoint" → query length histogram

	// Active connections
	activeRequests atomic.Int64

	// Circuit breaker state function (injected)
	cbStateFunc func() string

	// System-level metrics (/proc)
	system *SystemMetrics

	// Startup time
	startTime time.Time

	maxTenantLabels int
	maxClientLabels int
	knownTenants    map[string]struct{}
	knownClients    map[string]struct{}
}

type histogram struct {
	mu    sync.Mutex
	sum   float64
	count int64
	// Buckets: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
	buckets []float64
	counts  []int64
}

var defaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

const (
	defaultMaxTenantLabels = 256
	defaultMaxClientLabels = 256
	overflowMetricLabel    = "__overflow__"
)

func newHistogram() *histogram {
	return &histogram{
		buckets: defaultBuckets,
		counts:  make([]int64, len(defaultBuckets)),
	}
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.count++
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

func NewMetrics() *Metrics {
	return NewMetricsWithLimits(defaultMaxTenantLabels, defaultMaxClientLabels)
}

func NewMetricsWithLimits(maxTenantLabels, maxClientLabels int) *Metrics {
	if maxTenantLabels <= 0 {
		maxTenantLabels = defaultMaxTenantLabels
	}
	if maxClientLabels <= 0 {
		maxClientLabels = defaultMaxClientLabels
	}
	return &Metrics{
		requestsTotal:           make(map[string]*atomic.Int64),
		requestDurations:        make(map[string]*histogram),
		tenantRequests:          make(map[string]*atomic.Int64),
		tenantDurations:         make(map[string]*histogram),
		clientErrors:            make(map[string]*atomic.Int64),
		endpointCacheHits:       make(map[string]*atomic.Int64),
		endpointCacheMisses:     make(map[string]*atomic.Int64),
		backendDurations:        make(map[string]*histogram),
		tupleModes:              make(map[string]*atomic.Int64),
		clientRequests:          make(map[string]*atomic.Int64),
		clientDurations:         make(map[string]*histogram),
		clientBytes:             make(map[string]*atomic.Int64),
		clientStatuses:          make(map[string]*atomic.Int64),
		clientInflight:          make(map[string]*atomic.Int64),
		clientQueryLengths:      make(map[string]*histogram),
		windowFetch:             newHistogram(),
		windowMerge:             newHistogram(),
		windowCount:             newHistogram(),
		windowPrefilterDuration: newHistogram(),
		system:                  NewSystemMetrics(),
		startTime:               time.Now(),
		maxTenantLabels:         maxTenantLabels,
		maxClientLabels:         maxClientLabels,
		knownTenants:            make(map[string]struct{}),
		knownClients:            make(map[string]struct{}),
	}
}

// SetCircuitBreakerFunc sets the function to query CB state for metrics export.
func (m *Metrics) SetCircuitBreakerFunc(fn func() string) {
	m.cbStateFunc = fn
}

func (m *Metrics) RecordRequest(endpoint string, statusCode int, duration time.Duration) {
	key := fmt.Sprintf("%s:%d", endpoint, statusCode)
	m.mu.RLock()
	counter, ok := m.requestsTotal[key]
	hist, hok := m.requestDurations[endpoint]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		counter, ok = m.requestsTotal[key]
		if !ok {
			counter = &atomic.Int64{}
			m.requestsTotal[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)

	if !hok {
		m.mu.Lock()
		hist, hok = m.requestDurations[endpoint]
		if !hok {
			hist = newHistogram()
			m.requestDurations[endpoint] = hist
		}
		m.mu.Unlock()
	}
	hist.observe(duration.Seconds())
}

// RecordTenantRequest records a request for a specific tenant (X-Scope-OrgID).
// Empty tenant is recorded as "__none__".
func (m *Metrics) RecordTenantRequest(tenant, endpoint string, statusCode int, duration time.Duration) {
	tenant = m.canonicalTenantLabel(tenant)
	key := fmt.Sprintf("%s:%s:%d", tenant, endpoint, statusCode)
	m.mu.RLock()
	counter, ok := m.tenantRequests[key]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		counter, ok = m.tenantRequests[key]
		if !ok {
			counter = &atomic.Int64{}
			m.tenantRequests[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)

	durKey := fmt.Sprintf("%s:%s", tenant, endpoint)
	m.mu.RLock()
	hist, hok := m.tenantDurations[durKey]
	m.mu.RUnlock()
	if !hok {
		m.mu.Lock()
		hist, hok = m.tenantDurations[durKey]
		if !hok {
			hist = newHistogram()
			m.tenantDurations[durKey] = hist
		}
		m.mu.Unlock()
	}
	hist.observe(duration.Seconds())
}

// RecordClientIdentity records a request for a specific client identity.
// Client is identified by trusted user header > tenant > client IP.
func (m *Metrics) RecordClientIdentity(clientID, endpoint string, duration time.Duration, responseBytes int64) {
	clientID = m.canonicalClientLabel(clientID)

	key := fmt.Sprintf("%s:%s", clientID, endpoint)
	m.mu.RLock()
	counter, ok := m.clientRequests[key]
	hist, hok := m.clientDurations[key]
	bytesCounter, bok := m.clientBytes[clientID]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		counter, ok = m.clientRequests[key]
		if !ok {
			counter = &atomic.Int64{}
			m.clientRequests[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)

	if !hok {
		m.mu.Lock()
		hist, hok = m.clientDurations[key]
		if !hok {
			hist = newHistogram()
			m.clientDurations[key] = hist
		}
		m.mu.Unlock()
	}
	hist.observe(duration.Seconds())

	if !bok {
		m.mu.Lock()
		bytesCounter, bok = m.clientBytes[clientID]
		if !bok {
			bytesCounter = &atomic.Int64{}
			m.clientBytes[clientID] = bytesCounter
		}
		m.mu.Unlock()
	}
	bytesCounter.Add(responseBytes)
}

// ResolveClientContext extracts the client identity from an HTTP request and describes its source.
// Priority with trusted proxy headers: Grafana/auth-proxy user headers > X-Scope-OrgID > X-Forwarded-For > remote IP.
// Priority without trusted proxy headers: X-Scope-OrgID > remote IP.
//
// Intentionally does not use datasource/basic-auth credentials as client identity.
// Those credentials authenticate backend access and should not be attributed as the
// end user in per-client telemetry.
func ResolveClientContext(r *http.Request, trustProxyHeaders bool) (string, string) {
	if trustProxyHeaders {
		type candidate struct {
			header string
			source string
		}
		for _, c := range []candidate{
			{header: "X-Grafana-User", source: "grafana_user"},
			{header: "X-Forwarded-User", source: "forwarded_user"},
			{header: "X-Webauth-User", source: "webauth_user"},
			{header: "X-Auth-Request-User", source: "auth_request_user"},
		} {
			if user := strings.TrimSpace(r.Header.Get(c.header)); user != "" {
				return user, c.source
			}
		}
	}
	// Tenant ID (X-Scope-OrgID)
	if tenant := strings.TrimSpace(r.Header.Get("X-Scope-OrgID")); tenant != "" {
		return tenant, "tenant"
	}
	// Forwarded client IP
	if trustProxyHeaders {
		if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
			// Take first IP (original client)
			if idx := strings.IndexByte(fwd, ','); idx > 0 {
				return strings.TrimSpace(fwd[:idx]), "forwarded_for"
			}
			return strings.TrimSpace(fwd), "forwarded_for"
		}
	}
	// Remote address (IP:port → strip port)
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return host, "remote_addr"
	}
	return strings.TrimSpace(r.RemoteAddr), "remote_addr"
}

// ResolveAuthContext extracts the request authentication principal (if available)
// and describes its source. This is intentionally separate from ResolveClientContext.
func ResolveAuthContext(r *http.Request) (string, string) {
	if user, _, ok := r.BasicAuth(); ok && strings.TrimSpace(user) != "" {
		return user, "basic_auth"
	}
	return "", ""
}

// ResolveClientID extracts the client identity from an HTTP request.
func ResolveClientID(r *http.Request, trustProxyHeaders bool) string {
	clientID, _ := ResolveClientContext(r, trustProxyHeaders)
	return clientID
}

func sanitizeMetricIdentity(v string, emptyFallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return emptyFallback
	}
	v = strings.ReplaceAll(v, ":", "_")
	if len(v) > 64 {
		v = v[:64]
	}
	return v
}

func (m *Metrics) canonicalTenantLabel(tenant string) string {
	tenant = sanitizeMetricIdentity(tenant, "__none__")
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.knownTenants[tenant]; ok {
		return tenant
	}
	if len(m.knownTenants) >= m.maxTenantLabels {
		return overflowMetricLabel
	}
	m.knownTenants[tenant] = struct{}{}
	return tenant
}

func (m *Metrics) canonicalClientLabel(clientID string) string {
	clientID = sanitizeMetricIdentity(clientID, "__anonymous__")
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.knownClients[clientID]; ok {
		return clientID
	}
	if len(m.knownClients) >= m.maxClientLabels {
		return overflowMetricLabel
	}
	m.knownClients[clientID] = struct{}{}
	return clientID
}

// RecordClientError records a client-side error with a reason category.
func (m *Metrics) RecordClientError(endpoint, reason string) {
	key := fmt.Sprintf("%s:%s", endpoint, reason)
	m.mu.RLock()
	counter, ok := m.clientErrors[key]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		counter, ok = m.clientErrors[key]
		if !ok {
			counter = &atomic.Int64{}
			m.clientErrors[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordClientStatus records the final HTTP status observed for a client request.
func (m *Metrics) RecordClientStatus(clientID, endpoint string, statusCode int) {
	clientID = m.canonicalClientLabel(clientID)
	key := fmt.Sprintf("%s:%s:%d", clientID, endpoint, statusCode)
	m.mu.RLock()
	counter, ok := m.clientStatuses[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		counter, ok = m.clientStatuses[key]
		if !ok {
			counter = &atomic.Int64{}
			m.clientStatuses[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordClientInflight tracks currently active requests for a client.
func (m *Metrics) RecordClientInflight(clientID string, delta int64) {
	clientID = m.canonicalClientLabel(clientID)
	m.mu.RLock()
	gauge, ok := m.clientInflight[clientID]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		gauge, ok = m.clientInflight[clientID]
		if !ok {
			gauge = &atomic.Int64{}
			m.clientInflight[clientID] = gauge
		}
		m.mu.Unlock()
	}
	if gauge.Add(delta) < 0 {
		gauge.Store(0)
	}
}

// RecordClientQueryLength records the LogQL query length seen for a client and endpoint.
func (m *Metrics) RecordClientQueryLength(clientID, endpoint string, queryLength int) {
	clientID = m.canonicalClientLabel(clientID)
	key := fmt.Sprintf("%s:%s", clientID, endpoint)
	m.mu.RLock()
	hist, ok := m.clientQueryLengths[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		hist, ok = m.clientQueryLengths[key]
		if !ok {
			hist = newHistogram()
			m.clientQueryLengths[key] = hist
		}
		m.mu.Unlock()
	}
	hist.observe(float64(queryLength))
}

func (m *Metrics) RecordCacheHit()         { m.cacheHits.Add(1) }
func (m *Metrics) RecordCacheMiss()        { m.cacheMisses.Add(1) }
func (m *Metrics) RecordTranslation()      { m.translationsTotal.Add(1) }
func (m *Metrics) RecordTranslationError() { m.translationErrors.Add(1) }

// RecordTupleMode records the emitted tuple mode for a log query response.
func (m *Metrics) RecordTupleMode(mode string) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return
	}
	m.mu.RLock()
	counter, ok := m.tupleModes[mode]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		counter, ok = m.tupleModes[mode]
		if !ok {
			counter = &atomic.Int64{}
			m.tupleModes[mode] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordEndpointCacheHit records a cache hit for a specific endpoint.
func (m *Metrics) RecordEndpointCacheHit(endpoint string) {
	m.cacheHits.Add(1)
	m.mu.RLock()
	counter, ok := m.endpointCacheHits[endpoint]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		counter, ok = m.endpointCacheHits[endpoint]
		if !ok {
			counter = &atomic.Int64{}
			m.endpointCacheHits[endpoint] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordEndpointCacheMiss records a cache miss for a specific endpoint.
func (m *Metrics) RecordEndpointCacheMiss(endpoint string) {
	m.cacheMisses.Add(1)
	m.mu.RLock()
	counter, ok := m.endpointCacheMisses[endpoint]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		counter, ok = m.endpointCacheMisses[endpoint]
		if !ok {
			counter = &atomic.Int64{}
			m.endpointCacheMisses[endpoint] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordBackendDuration records VL backend response time for an endpoint.
func (m *Metrics) RecordBackendDuration(endpoint string, d time.Duration) {
	m.mu.RLock()
	h, ok := m.backendDurations[endpoint]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		h, ok = m.backendDurations[endpoint]
		if !ok {
			h = newHistogram()
			m.backendDurations[endpoint] = h
		}
		m.mu.Unlock()
	}
	h.observe(d.Seconds())
}

// RecordCoalesced records a request that was served from a coalesced result.
func (m *Metrics) RecordCoalesced() { m.coalescedTotal.Add(1) }

// RecordCoalescedSaved records a backend request that was saved by coalescing.
func (m *Metrics) RecordCoalescedSaved() { m.coalescedSaved.Add(1) }

// RecordQueryRangeWindowCacheHit records a query_range window cache hit.
func (m *Metrics) RecordQueryRangeWindowCacheHit() { m.windowCacheHits.Add(1) }

// RecordQueryRangeWindowCacheMiss records a query_range window cache miss.
func (m *Metrics) RecordQueryRangeWindowCacheMiss() { m.windowCacheMisses.Add(1) }

// RecordQueryRangeWindowFetchDuration records backend fetch latency for one query_range window.
func (m *Metrics) RecordQueryRangeWindowFetchDuration(d time.Duration) {
	if m.windowFetch == nil {
		return
	}
	m.windowFetch.observe(d.Seconds())
}

// RecordQueryRangeWindowMergeDuration records merge latency for one windowed query_range response.
func (m *Metrics) RecordQueryRangeWindowMergeDuration(d time.Duration) {
	if m.windowMerge == nil {
		return
	}
	m.windowMerge.observe(d.Seconds())
}

// RecordQueryRangeWindowCount records how many windows were used for a query_range request.
func (m *Metrics) RecordQueryRangeWindowCount(n int) {
	if m.windowCount == nil || n <= 0 {
		return
	}
	m.windowCount.observe(float64(n))
}

// RecordQueryRangeWindowPrefilterAttempt records one query_range window prefilter attempt.
func (m *Metrics) RecordQueryRangeWindowPrefilterAttempt() {
	m.windowPrefilterAttempts.Add(1)
}

// RecordQueryRangeWindowPrefilterDuration records query_range window prefilter duration.
func (m *Metrics) RecordQueryRangeWindowPrefilterDuration(d time.Duration) {
	if m.windowPrefilterDuration == nil {
		return
	}
	m.windowPrefilterDuration.observe(d.Seconds())
}

// RecordQueryRangeWindowPrefilterError records one query_range window prefilter error.
func (m *Metrics) RecordQueryRangeWindowPrefilterError() {
	m.windowPrefilterErrors.Add(1)
}

// RecordQueryRangeWindowPrefilterOutcome records kept/skipped windows after prefiltering.
func (m *Metrics) RecordQueryRangeWindowPrefilterOutcome(kept, skipped int) {
	if kept > 0 {
		m.windowPrefilterKept.Add(int64(kept))
	}
	if skipped > 0 {
		m.windowPrefilterSkipped.Add(int64(skipped))
	}
}

// RecordQueryRangeAdaptiveState records adaptive query_range controller state.
func (m *Metrics) RecordQueryRangeAdaptiveState(currentParallel int, latencyEWMA time.Duration, errorEWMA float64) {
	if currentParallel < 0 {
		currentParallel = 0
	}
	if latencyEWMA < 0 {
		latencyEWMA = 0
	}
	if errorEWMA < 0 {
		errorEWMA = 0
	}
	if errorEWMA > 1 {
		errorEWMA = 1
	}
	m.windowAdaptiveParallelCurrent.Store(int64(currentParallel))
	m.windowAdaptiveLatencyEWMAms.Store(latencyEWMA.Milliseconds())
	m.windowAdaptiveErrorEWMAppm.Store(int64(errorEWMA * 1_000_000))
}

// Handler serves Prometheus metrics at /metrics.
func (m *Metrics) Handler(w http.ResponseWriter, r *http.Request) {
	var sb strings.Builder

	sb.WriteString("# HELP loki_vl_proxy_requests_total Total number of proxied requests.\n")
	sb.WriteString("# TYPE loki_vl_proxy_requests_total counter\n")

	m.mu.RLock()
	keys := make([]string, 0, len(m.requestsTotal))
	for k := range m.requestsTotal {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 2)
		endpoint := parts[0]
		status := parts[1]
		count := m.requestsTotal[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_requests_total{endpoint=%q,status=%q} %d\n", endpoint, status, count)
	}

	sb.WriteString("# HELP loki_vl_proxy_request_duration_seconds Request duration histogram.\n")
	sb.WriteString("# TYPE loki_vl_proxy_request_duration_seconds histogram\n")
	endpoints := make([]string, 0, len(m.requestDurations))
	for ep := range m.requestDurations {
		endpoints = append(endpoints, ep)
	}
	sort.Strings(endpoints)
	for _, ep := range endpoints {
		h := m.requestDurations[ep]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_bucket{endpoint=%q,le=\"%g\"} %d\n", ep, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_bucket{endpoint=%q,le=\"+Inf\"} %d\n", ep, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_sum{endpoint=%q} %g\n", ep, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_count{endpoint=%q} %d\n", ep, h.count)
		h.mu.Unlock()
	}

	sb.WriteString("# HELP loki_vl_proxy_cache_hits_total Cache hits.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_hits_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_cache_hits_total %d\n", m.cacheHits.Load())

	sb.WriteString("# HELP loki_vl_proxy_cache_misses_total Cache misses.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_misses_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_cache_misses_total %d\n", m.cacheMisses.Load())

	sb.WriteString("# HELP loki_vl_proxy_translations_total LogQL to LogsQL translations.\n")
	sb.WriteString("# TYPE loki_vl_proxy_translations_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_translations_total %d\n", m.translationsTotal.Load())

	sb.WriteString("# HELP loki_vl_proxy_translation_errors_total Failed translations.\n")
	sb.WriteString("# TYPE loki_vl_proxy_translation_errors_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_translation_errors_total %d\n", m.translationErrors.Load())

	sb.WriteString("# HELP loki_vl_proxy_response_tuple_mode_total Log response tuple mode emissions by client behavior.\n")
	sb.WriteString("# TYPE loki_vl_proxy_response_tuple_mode_total counter\n")
	tmKeys := make([]string, 0, len(m.tupleModes))
	for mode := range m.tupleModes {
		tmKeys = append(tmKeys, mode)
	}
	sort.Strings(tmKeys)
	for _, mode := range tmKeys {
		fmt.Fprintf(&sb, "loki_vl_proxy_response_tuple_mode_total{mode=%q} %d\n", mode, m.tupleModes[mode].Load())
	}

	sb.WriteString("# HELP loki_vl_proxy_uptime_seconds Proxy uptime.\n")
	sb.WriteString("# TYPE loki_vl_proxy_uptime_seconds gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_uptime_seconds %g\n", time.Since(m.startTime).Seconds())

	// Go runtime / GC metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	sb.WriteString("# HELP go_memstats_alloc_bytes Current heap allocation in bytes.\n")
	sb.WriteString("# TYPE go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_alloc_bytes %d\n", memStats.Alloc)

	sb.WriteString("# HELP go_memstats_sys_bytes Total memory from OS in bytes.\n")
	sb.WriteString("# TYPE go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_sys_bytes %d\n", memStats.Sys)

	sb.WriteString("# HELP go_memstats_heap_inuse_bytes Heap in-use bytes.\n")
	sb.WriteString("# TYPE go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_heap_inuse_bytes %d\n", memStats.HeapInuse)

	sb.WriteString("# HELP go_memstats_heap_idle_bytes Heap idle bytes.\n")
	sb.WriteString("# TYPE go_memstats_heap_idle_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_heap_idle_bytes %d\n", memStats.HeapIdle)

	sb.WriteString("# HELP go_gc_duration_seconds GC pause duration summary.\n")
	sb.WriteString("# TYPE go_gc_duration_seconds summary\n")
	fmt.Fprintf(&sb, "go_gc_duration_seconds_count %d\n", memStats.NumGC)

	sb.WriteString("# HELP go_goroutines Current number of goroutines.\n")
	sb.WriteString("# TYPE go_goroutines gauge\n")
	fmt.Fprintf(&sb, "go_goroutines %d\n", runtime.NumGoroutine())

	sb.WriteString("# HELP go_gc_cycles_total Total GC cycles completed.\n")
	sb.WriteString("# TYPE go_gc_cycles_total counter\n")
	fmt.Fprintf(&sb, "go_gc_cycles_total %d\n", memStats.NumGC)

	// Per-tenant request counters
	sb.WriteString("# HELP loki_vl_proxy_tenant_requests_total Requests by tenant.\n")
	sb.WriteString("# TYPE loki_vl_proxy_tenant_requests_total counter\n")
	tenantKeys := make([]string, 0, len(m.tenantRequests))
	for k := range m.tenantRequests {
		tenantKeys = append(tenantKeys, k)
	}
	sort.Strings(tenantKeys)
	for _, key := range tenantKeys {
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		count := m.tenantRequests[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_requests_total{tenant=%q,endpoint=%q,status=%q} %d\n",
			parts[0], parts[1], parts[2], count)
	}

	// Per-tenant latency histograms
	sb.WriteString("# HELP loki_vl_proxy_tenant_request_duration_seconds Per-tenant request duration.\n")
	sb.WriteString("# TYPE loki_vl_proxy_tenant_request_duration_seconds histogram\n")
	tenantDurKeys := make([]string, 0, len(m.tenantDurations))
	for k := range m.tenantDurations {
		tenantDurKeys = append(tenantDurKeys, k)
	}
	sort.Strings(tenantDurKeys)
	for _, key := range tenantDurKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		tenant, ep := parts[0], parts[1]
		h := m.tenantDurations[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_bucket{tenant=%q,endpoint=%q,le=\"%g\"} %d\n",
				tenant, ep, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_bucket{tenant=%q,endpoint=%q,le=\"+Inf\"} %d\n",
			tenant, ep, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_sum{tenant=%q,endpoint=%q} %g\n",
			tenant, ep, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_count{tenant=%q,endpoint=%q} %d\n",
			tenant, ep, h.count)
		h.mu.Unlock()
	}

	// Client error breakdown
	sb.WriteString("# HELP loki_vl_proxy_client_errors_total Client errors by reason.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_errors_total counter\n")
	ceKeys := make([]string, 0, len(m.clientErrors))
	for k := range m.clientErrors {
		ceKeys = append(ceKeys, k)
	}
	sort.Strings(ceKeys)
	for _, key := range ceKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		count := m.clientErrors[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_client_errors_total{endpoint=%q,reason=%q} %d\n",
			parts[0], parts[1], count)
	}
	// Per-endpoint cache stats
	sb.WriteString("# HELP loki_vl_proxy_cache_hits_by_endpoint Cache hits per endpoint.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_hits_by_endpoint counter\n")
	for ep, counter := range m.endpointCacheHits {
		fmt.Fprintf(&sb, "loki_vl_proxy_cache_hits_by_endpoint{endpoint=%q} %d\n", ep, counter.Load())
	}
	sb.WriteString("# HELP loki_vl_proxy_cache_misses_by_endpoint Cache misses per endpoint.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_misses_by_endpoint counter\n")
	for ep, counter := range m.endpointCacheMisses {
		fmt.Fprintf(&sb, "loki_vl_proxy_cache_misses_by_endpoint{endpoint=%q} %d\n", ep, counter.Load())
	}

	// Backend latency histogram
	sb.WriteString("# HELP loki_vl_proxy_backend_duration_seconds VL backend response time.\n")
	sb.WriteString("# TYPE loki_vl_proxy_backend_duration_seconds histogram\n")
	bdKeys := make([]string, 0, len(m.backendDurations))
	for k := range m.backendDurations {
		bdKeys = append(bdKeys, k)
	}
	sort.Strings(bdKeys)
	for _, ep := range bdKeys {
		h := m.backendDurations[ep]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_bucket{endpoint=%q,le=\"%g\"} %d\n", ep, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_bucket{endpoint=%q,le=\"+Inf\"} %d\n", ep, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_sum{endpoint=%q} %g\n", ep, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_count{endpoint=%q} %d\n", ep, h.count)
		h.mu.Unlock()
	}

	// Singleflight coalescing stats
	sb.WriteString("# HELP loki_vl_proxy_coalesced_total Requests served from coalesced results.\n")
	sb.WriteString("# TYPE loki_vl_proxy_coalesced_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_coalesced_total %d\n", m.coalescedTotal.Load())
	sb.WriteString("# HELP loki_vl_proxy_coalesced_saved_total Backend requests saved by coalescing.\n")
	sb.WriteString("# TYPE loki_vl_proxy_coalesced_saved_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_coalesced_saved_total %d\n", m.coalescedSaved.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_cache_hit_total Query-range window cache hits.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_cache_hit_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_cache_hit_total %d\n", m.windowCacheHits.Load())
	sb.WriteString("# HELP loki_vl_proxy_window_cache_miss_total Query-range window cache misses.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_cache_miss_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_cache_miss_total %d\n", m.windowCacheMisses.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_fetch_seconds Query-range window backend fetch duration.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_fetch_seconds histogram\n")
	if m.windowFetch != nil {
		m.windowFetch.mu.Lock()
		for i, b := range m.windowFetch.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_window_fetch_seconds_bucket{le=\"%g\"} %d\n", b, m.windowFetch.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_window_fetch_seconds_bucket{le=\"+Inf\"} %d\n", m.windowFetch.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_fetch_seconds_sum %g\n", m.windowFetch.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_fetch_seconds_count %d\n", m.windowFetch.count)
		m.windowFetch.mu.Unlock()
	}

	sb.WriteString("# HELP loki_vl_proxy_window_merge_seconds Query-range window merge duration.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_merge_seconds histogram\n")
	if m.windowMerge != nil {
		m.windowMerge.mu.Lock()
		for i, b := range m.windowMerge.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_window_merge_seconds_bucket{le=\"%g\"} %d\n", b, m.windowMerge.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_window_merge_seconds_bucket{le=\"+Inf\"} %d\n", m.windowMerge.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_merge_seconds_sum %g\n", m.windowMerge.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_merge_seconds_count %d\n", m.windowMerge.count)
		m.windowMerge.mu.Unlock()
	}

	sb.WriteString("# HELP loki_vl_proxy_window_count Query-range window count per request.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_count histogram\n")
	if m.windowCount != nil {
		m.windowCount.mu.Lock()
		for i, b := range m.windowCount.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_window_count_bucket{le=\"%g\"} %d\n", b, m.windowCount.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_window_count_bucket{le=\"+Inf\"} %d\n", m.windowCount.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_count_sum %g\n", m.windowCount.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_count_count %d\n", m.windowCount.count)
		m.windowCount.mu.Unlock()
	}
	sb.WriteString("# HELP loki_vl_proxy_window_prefilter_attempt_total Query-range window prefilter attempts.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_prefilter_attempt_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_attempt_total %d\n", m.windowPrefilterAttempts.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_prefilter_error_total Query-range window prefilter errors.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_prefilter_error_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_error_total %d\n", m.windowPrefilterErrors.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_prefilter_kept_total Query-range windows kept after prefilter.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_prefilter_kept_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_kept_total %d\n", m.windowPrefilterKept.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_prefilter_skipped_total Query-range windows skipped after prefilter.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_prefilter_skipped_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_skipped_total %d\n", m.windowPrefilterSkipped.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_prefilter_duration_seconds Query-range window prefilter duration.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_prefilter_duration_seconds histogram\n")
	if m.windowPrefilterDuration != nil {
		m.windowPrefilterDuration.mu.Lock()
		for i, b := range m.windowPrefilterDuration.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_duration_seconds_bucket{le=\"%g\"} %d\n", b, m.windowPrefilterDuration.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.windowPrefilterDuration.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_duration_seconds_sum %g\n", m.windowPrefilterDuration.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_duration_seconds_count %d\n", m.windowPrefilterDuration.count)
		m.windowPrefilterDuration.mu.Unlock()
	}
	sb.WriteString("# HELP loki_vl_proxy_window_adaptive_parallel_current Current adaptive query-range window parallelism.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_adaptive_parallel_current gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_adaptive_parallel_current %d\n", m.windowAdaptiveParallelCurrent.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_adaptive_latency_ewma_seconds EWMA backend fetch latency for query-range windows.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_adaptive_latency_ewma_seconds gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_adaptive_latency_ewma_seconds %g\n", float64(m.windowAdaptiveLatencyEWMAms.Load())/1000.0)

	sb.WriteString("# HELP loki_vl_proxy_window_adaptive_error_ewma Adaptive backend window fetch error EWMA ratio (0-1).\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_adaptive_error_ewma gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_adaptive_error_ewma %g\n", float64(m.windowAdaptiveErrorEWMAppm.Load())/1000000.0)

	// Per-client identity metrics
	sb.WriteString("# HELP loki_vl_proxy_client_requests_total Requests by client identity.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_requests_total counter\n")
	crKeys := make([]string, 0, len(m.clientRequests))
	for k := range m.clientRequests {
		crKeys = append(crKeys, k)
	}
	sort.Strings(crKeys)
	for _, key := range crKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_requests_total{client=%q,endpoint=%q} %d\n",
			parts[0], parts[1], m.clientRequests[key].Load())
	}

	sb.WriteString("# HELP loki_vl_proxy_client_response_bytes_total Response bytes by client.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_response_bytes_total counter\n")
	cbKeys := make([]string, 0, len(m.clientBytes))
	for k := range m.clientBytes {
		cbKeys = append(cbKeys, k)
	}
	sort.Strings(cbKeys)
	for _, client := range cbKeys {
		fmt.Fprintf(&sb, "loki_vl_proxy_client_response_bytes_total{client=%q} %d\n",
			client, m.clientBytes[client].Load())
	}

	sb.WriteString("# HELP loki_vl_proxy_client_status_total Requests by client identity and final HTTP status.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_status_total counter\n")
	csKeys := make([]string, 0, len(m.clientStatuses))
	for k := range m.clientStatuses {
		csKeys = append(csKeys, k)
	}
	sort.Strings(csKeys)
	for _, key := range csKeys {
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_status_total{client=%q,endpoint=%q,status=%q} %d\n",
			parts[0], parts[1], parts[2], m.clientStatuses[key].Load())
	}

	sb.WriteString("# HELP loki_vl_proxy_client_inflight_requests In-flight requests by client identity.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_inflight_requests gauge\n")
	ciKeys := make([]string, 0, len(m.clientInflight))
	for k := range m.clientInflight {
		ciKeys = append(ciKeys, k)
	}
	sort.Strings(ciKeys)
	for _, client := range ciKeys {
		fmt.Fprintf(&sb, "loki_vl_proxy_client_inflight_requests{client=%q} %d\n",
			client, m.clientInflight[client].Load())
	}

	sb.WriteString("# HELP loki_vl_proxy_client_request_duration_seconds Per-client request duration.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_request_duration_seconds histogram\n")
	cdKeys := make([]string, 0, len(m.clientDurations))
	for k := range m.clientDurations {
		cdKeys = append(cdKeys, k)
	}
	sort.Strings(cdKeys)
	for _, key := range cdKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		client, ep := parts[0], parts[1]
		h := m.clientDurations[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_bucket{client=%q,endpoint=%q,le=\"%g\"} %d\n",
				client, ep, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_bucket{client=%q,endpoint=%q,le=\"+Inf\"} %d\n",
			client, ep, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_sum{client=%q,endpoint=%q} %g\n",
			client, ep, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_count{client=%q,endpoint=%q} %d\n",
			client, ep, h.count)
		h.mu.Unlock()
	}

	sb.WriteString("# HELP loki_vl_proxy_client_query_length_chars LogQL query length by client identity.\n")
	sb.WriteString("# TYPE loki_vl_proxy_client_query_length_chars histogram\n")
	cqKeys := make([]string, 0, len(m.clientQueryLengths))
	for k := range m.clientQueryLengths {
		cqKeys = append(cqKeys, k)
	}
	sort.Strings(cqKeys)
	for _, key := range cqKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		client, ep := parts[0], parts[1]
		h := m.clientQueryLengths[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_bucket{client=%q,endpoint=%q,le=\"%g\"} %d\n",
				client, ep, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_bucket{client=%q,endpoint=%q,le=\"+Inf\"} %d\n",
			client, ep, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_sum{client=%q,endpoint=%q} %g\n",
			client, ep, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_count{client=%q,endpoint=%q} %d\n",
			client, ep, h.count)
		h.mu.Unlock()
	}

	// Circuit breaker state
	if m.cbStateFunc != nil {
		cbState := m.cbStateFunc()
		cbVal := 0
		switch cbState {
		case "closed":
			cbVal = 0
		case "open":
			cbVal = 1
		case "half_open", "half-open":
			cbVal = 2
		}
		sb.WriteString("# HELP loki_vl_proxy_circuit_breaker_state Circuit breaker state (0=closed, 1=open, 2=half-open).\n")
		sb.WriteString("# TYPE loki_vl_proxy_circuit_breaker_state gauge\n")
		fmt.Fprintf(&sb, "loki_vl_proxy_circuit_breaker_state %d\n", cbVal)
	}

	m.mu.RUnlock()

	// System-level metrics (/proc on Linux)
	m.system.WritePrometheus(&sb)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(sb.String()))
}
