package metrics

import (
	"fmt"
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
	requestDurations map[string]*histogram     // "endpoint" → duration histogram

	// Per-tenant request counters
	tenantRequests   map[string]*atomic.Int64 // "tenant:endpoint:status" → count
	tenantDurations  map[string]*histogram     // "tenant:endpoint" → duration histogram

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
	translationsTotal  atomic.Int64
	translationErrors  atomic.Int64

	// Client error tracking
	clientErrors map[string]*atomic.Int64 // "endpoint:reason" → count

	// Per-client identity metrics
	clientRequests  map[string]*atomic.Int64 // "client:endpoint" → count
	clientDurations map[string]*histogram    // "client:endpoint" → duration histogram
	clientBytes     map[string]*atomic.Int64 // "client" → response bytes

	// Active connections
	activeRequests atomic.Int64

	// Circuit breaker state function (injected)
	cbStateFunc func() string

	// System-level metrics (/proc)
	system *SystemMetrics

	// Startup time
	startTime time.Time
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
	return &Metrics{
		requestsTotal:      make(map[string]*atomic.Int64),
		requestDurations:   make(map[string]*histogram),
		tenantRequests:     make(map[string]*atomic.Int64),
		tenantDurations:    make(map[string]*histogram),
		clientErrors:       make(map[string]*atomic.Int64),
		endpointCacheHits:  make(map[string]*atomic.Int64),
		endpointCacheMisses: make(map[string]*atomic.Int64),
		backendDurations:   make(map[string]*histogram),
		clientRequests:     make(map[string]*atomic.Int64),
		clientDurations:    make(map[string]*histogram),
		clientBytes:        make(map[string]*atomic.Int64),
		system:             NewSystemMetrics(),
		startTime:          time.Now(),
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
	if tenant == "" {
		tenant = "__none__"
	}
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
// Client is identified by: X-Grafana-User > tenant > basic auth user > IP.
func (m *Metrics) RecordClientIdentity(clientID, endpoint string, duration time.Duration, responseBytes int64) {
	if clientID == "" {
		clientID = "__anonymous__"
	}
	// Limit cardinality: truncate to first 64 chars
	if len(clientID) > 64 {
		clientID = clientID[:64]
	}

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

// ResolveClientID extracts the client identity from an HTTP request.
// Priority: X-Grafana-User > X-Scope-OrgID > basic auth user > IP.
func ResolveClientID(r *http.Request) string {
	// Grafana sets this header when proxying datasource requests
	if user := r.Header.Get("X-Grafana-User"); user != "" {
		return user
	}
	// Tenant ID (X-Scope-OrgID)
	if tenant := r.Header.Get("X-Scope-OrgID"); tenant != "" {
		return tenant
	}
	// Basic auth username
	if user, _, ok := r.BasicAuth(); ok && user != "" {
		return user
	}
	// Forwarded client IP
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		// Take first IP (original client)
		if idx := strings.IndexByte(fwd, ','); idx > 0 {
			return strings.TrimSpace(fwd[:idx])
		}
		return strings.TrimSpace(fwd)
	}
	// Remote address (IP:port → strip port)
	if idx := strings.LastIndexByte(r.RemoteAddr, ':'); idx > 0 {
		return r.RemoteAddr[:idx]
	}
	return r.RemoteAddr
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

func (m *Metrics) RecordCacheHit()  { m.cacheHits.Add(1) }
func (m *Metrics) RecordCacheMiss() { m.cacheMisses.Add(1) }
func (m *Metrics) RecordTranslation()      { m.translationsTotal.Add(1) }
func (m *Metrics) RecordTranslationError() { m.translationErrors.Add(1) }

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
