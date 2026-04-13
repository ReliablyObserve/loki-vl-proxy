package metrics

import (
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects proxy metrics in Prometheus exposition format.
type Metrics struct {
	mu sync.RWMutex

	// Request counters by system, direction, request type, normalized route, and status
	requestsTotal    map[string]*atomic.Int64 // "system<sep>direction<sep>endpoint<sep>route<sep>status" → count
	requestDurations map[string]*histogram    // "system<sep>direction<sep>endpoint<sep>route" → duration histogram

	// Per-tenant request counters
	tenantRequests  map[string]*atomic.Int64 // "tenant<sep>endpoint<sep>route<sep>status" → count
	tenantDurations map[string]*histogram    // "tenant<sep>endpoint<sep>route" → duration histogram

	// Cache stats (global)
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64

	// Cache stats (per-endpoint + route)
	endpointCacheHits   map[string]*atomic.Int64 // "endpoint<sep>route" → hits
	endpointCacheMisses map[string]*atomic.Int64 // "endpoint<sep>route" → misses

	// Backend latency (VL response time, separate from total proxy latency)
	backendDurations map[string]*histogram // "endpoint<sep>route" → VL duration histogram

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
	windowPrefilterHitRatioPpm    atomic.Int64
	windowRetries                 atomic.Int64
	windowDegradedBatches         atomic.Int64
	windowPartialResponses        atomic.Int64
	windowPrefilterDuration       *histogram
	windowAdaptiveParallelCurrent atomic.Int64
	windowAdaptiveLatencyEWMAms   atomic.Int64
	windowAdaptiveErrorEWMAppm    atomic.Int64

	// Client error tracking
	clientErrors map[string]*atomic.Int64 // "endpoint<sep>route<sep>reason" → count

	// Per-client identity metrics
	clientRequests     map[string]*atomic.Int64 // "client<sep>endpoint<sep>route" → count
	clientDurations    map[string]*histogram    // "client<sep>endpoint<sep>route" → duration histogram
	clientBytes        map[string]*atomic.Int64 // "client" → response bytes
	clientStatuses     map[string]*atomic.Int64 // "client<sep>endpoint<sep>route<sep>status" → count
	clientInflight     map[string]*atomic.Int64 // "client" → in-flight requests
	clientQueryLengths map[string]*histogram    // "client<sep>endpoint<sep>route" → query length histogram

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
	metricKeySep           = "\x1f"
	lokiSystemLabel        = "loki"
	vlSystemLabel          = "vl"
	downstreamDirection    = "downstream"
	upstreamDirection      = "upstream"
)

type routeSeed struct {
	endpoint string
	route    string
}

var (
	defaultMetricSeedRoutes = []routeSeed{
		{endpoint: "query_range", route: "/loki/api/v1/query_range"},
		{endpoint: "query", route: "/loki/api/v1/query"},
		{endpoint: "series", route: "/loki/api/v1/series"},
		{endpoint: "labels", route: "/loki/api/v1/labels"},
		{endpoint: "label_values", route: "/loki/api/v1/label/{name}/values"},
		{endpoint: "detected_fields", route: "/loki/api/v1/detected_fields"},
		{endpoint: "detected_field_values", route: "/loki/api/v1/detected_field/{name}/values"},
		{endpoint: "index_stats", route: "/loki/api/v1/index/stats"},
		{endpoint: "volume", route: "/loki/api/v1/index/volume"},
		{endpoint: "volume_range", route: "/loki/api/v1/index/volume_range"},
		{endpoint: "patterns", route: "/loki/api/v1/patterns"},
		{endpoint: "tail", route: "/loki/api/v1/tail"},
		{endpoint: "format_query", route: "/loki/api/v1/format_query"},
		{endpoint: "detected_labels", route: "/loki/api/v1/detected_labels"},
		{endpoint: "drilldown_limits", route: "/loki/api/v1/drilldown-limits"},
		{endpoint: "delete", route: "/loki/api/v1/delete"},
		{endpoint: "rules", route: "/loki/api/v1/rules"},
		{endpoint: "rules_nested", route: "/loki/api/v1/rules/{namespace}"},
		{endpoint: "rules_prom", route: "/api/prom/rules"},
		{endpoint: "rules_prom_nested", route: "/api/prom/rules/{namespace}"},
		{endpoint: "rules_prometheus", route: "/prometheus/api/v1/rules"},
		{endpoint: "alerts", route: "/loki/api/v1/alerts"},
		{endpoint: "alerts_prom", route: "/api/prom/alerts"},
		{endpoint: "alerts_prometheus", route: "/prometheus/api/v1/alerts"},
	}
	defaultMetricSeedStatuses = []int{200, 400, 429, 500, 502}
	defaultClientErrorReasons = []string{"bad_request", "rate_limited", "not_found", "body_too_large", "timeout"}
	defaultTupleModes         = []string{"default_2tuple", "categorize_labels_3tuple"}
	defaultRouteByEndpoint    = map[string]string{
		"query_range":           "/loki/api/v1/query_range",
		"query":                 "/loki/api/v1/query",
		"series":                "/loki/api/v1/series",
		"labels":                "/loki/api/v1/labels",
		"label_values":          "/loki/api/v1/label/{name}/values",
		"detected_fields":       "/loki/api/v1/detected_fields",
		"detected_field_values": "/loki/api/v1/detected_field/{name}/values",
		"index_stats":           "/loki/api/v1/index/stats",
		"volume":                "/loki/api/v1/index/volume",
		"volume_range":          "/loki/api/v1/index/volume_range",
		"patterns":              "/loki/api/v1/patterns",
		"tail":                  "/loki/api/v1/tail",
		"format_query":          "/loki/api/v1/format_query",
		"detected_labels":       "/loki/api/v1/detected_labels",
		"drilldown_limits":      "/loki/api/v1/drilldown-limits",
		"delete":                "/loki/api/v1/delete",
		"rules":                 "/loki/api/v1/rules",
		"rules_nested":          "/loki/api/v1/rules/{namespace}",
		"rules_prom":            "/api/prom/rules",
		"rules_prom_nested":     "/api/prom/rules/{namespace}",
		"rules_prometheus":      "/prometheus/api/v1/rules",
		"alerts":                "/loki/api/v1/alerts",
		"alerts_prom":           "/api/prom/alerts",
		"alerts_prometheus":     "/prometheus/api/v1/alerts",
	}
)

func joinMetricKey(parts ...string) string {
	return strings.Join(parts, metricKeySep)
}

func splitMetricKey(key string, want int) []string {
	parts := strings.Split(key, metricKeySep)
	if len(parts) != want {
		return nil
	}
	return parts
}

func normalizeMetricRoute(route, endpoint string) string {
	route = strings.TrimSpace(route)
	if route != "" {
		return route
	}
	if mapped, ok := defaultRouteByEndpoint[endpoint]; ok {
		return mapped
	}
	if endpoint == "" {
		return "/unknown"
	}
	return endpoint
}

func normalizeMetricSystem(system string) string {
	switch strings.ToLower(strings.TrimSpace(system)) {
	case lokiSystemLabel:
		return lokiSystemLabel
	case vlSystemLabel:
		return vlSystemLabel
	default:
		return lokiSystemLabel
	}
}

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
	m := &Metrics{
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
	m.preRegisterZeroSeries()
	return m
}

func (m *Metrics) preRegisterZeroSeries() {
	for _, seed := range defaultMetricSeedRoutes {
		requestRouteKey := joinMetricKey(lokiSystemLabel, downstreamDirection, seed.endpoint, seed.route)
		if _, ok := m.requestDurations[requestRouteKey]; !ok {
			m.requestDurations[requestRouteKey] = newHistogram()
		}
		routeKey := joinMetricKey(seed.endpoint, seed.route)
		if _, ok := m.backendDurations[routeKey]; !ok {
			m.backendDurations[routeKey] = newHistogram()
		}
		if _, ok := m.endpointCacheHits[routeKey]; !ok {
			m.endpointCacheHits[routeKey] = &atomic.Int64{}
		}
		if _, ok := m.endpointCacheMisses[routeKey]; !ok {
			m.endpointCacheMisses[routeKey] = &atomic.Int64{}
		}
		for _, status := range defaultMetricSeedStatuses {
			key := joinMetricKey(lokiSystemLabel, downstreamDirection, seed.endpoint, seed.route, strconv.Itoa(status))
			if _, ok := m.requestsTotal[key]; !ok {
				m.requestsTotal[key] = &atomic.Int64{}
			}
		}
		for _, reason := range defaultClientErrorReasons {
			key := joinMetricKey(seed.endpoint, seed.route, reason)
			if _, ok := m.clientErrors[key]; !ok {
				m.clientErrors[key] = &atomic.Int64{}
			}
		}
		for _, status := range defaultMetricSeedStatuses {
			tk := joinMetricKey("__none__", seed.endpoint, seed.route, strconv.Itoa(status))
			if _, ok := m.tenantRequests[tk]; !ok {
				m.tenantRequests[tk] = &atomic.Int64{}
			}
			ck := joinMetricKey("__none__", seed.endpoint, seed.route, strconv.Itoa(status))
			if _, ok := m.clientStatuses[ck]; !ok {
				m.clientStatuses[ck] = &atomic.Int64{}
			}
		}
		tdk := joinMetricKey("__none__", seed.endpoint, seed.route)
		if _, ok := m.tenantDurations[tdk]; !ok {
			m.tenantDurations[tdk] = newHistogram()
		}
		cdk := joinMetricKey("__none__", seed.endpoint, seed.route)
		if _, ok := m.clientRequests[cdk]; !ok {
			m.clientRequests[cdk] = &atomic.Int64{}
		}
		if _, ok := m.clientDurations[cdk]; !ok {
			m.clientDurations[cdk] = newHistogram()
		}
		if _, ok := m.clientQueryLengths[cdk]; !ok {
			m.clientQueryLengths[cdk] = newHistogram()
		}
	}
	if _, ok := m.clientBytes["__none__"]; !ok {
		m.clientBytes["__none__"] = &atomic.Int64{}
	}
	if _, ok := m.clientInflight["__none__"]; !ok {
		m.clientInflight["__none__"] = &atomic.Int64{}
	}
	for _, mode := range defaultTupleModes {
		if _, ok := m.tupleModes[mode]; !ok {
			m.tupleModes[mode] = &atomic.Int64{}
		}
	}
}

// SetCircuitBreakerFunc sets the function to query CB state for metrics export.
func (m *Metrics) SetCircuitBreakerFunc(fn func() string) {
	m.cbStateFunc = fn
}

func (m *Metrics) RecordRequest(endpoint string, statusCode int, duration time.Duration) {
	m.RecordRequestWithRoute(endpoint, "", statusCode, duration)
}

func (m *Metrics) RecordRequestWithRoute(endpoint, route string, statusCode int, duration time.Duration) {
	m.recordRequestWithLabels(lokiSystemLabel, downstreamDirection, endpoint, route, statusCode, duration)
}

func (m *Metrics) RecordUpstreamRequest(system, endpoint, route string, statusCode int, duration time.Duration) {
	m.recordRequestWithLabels(system, upstreamDirection, endpoint, route, statusCode, duration)
}

func (m *Metrics) recordRequestWithLabels(system, direction, endpoint, route string, statusCode int, duration time.Duration) {
	system = normalizeMetricSystem(system)
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(system, direction, endpoint, route, strconv.Itoa(statusCode))
	m.mu.RLock()
	counter, ok := m.requestsTotal[key]
	histKey := joinMetricKey(system, direction, endpoint, route)
	hist, hok := m.requestDurations[histKey]
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
		hist, hok = m.requestDurations[histKey]
		if !hok {
			hist = newHistogram()
			m.requestDurations[histKey] = hist
		}
		m.mu.Unlock()
	}
	hist.observe(duration.Seconds())
}

// RecordTenantRequest records a request for a specific tenant (X-Scope-OrgID).
// Empty tenant is recorded as "__none__".
func (m *Metrics) RecordTenantRequest(tenant, endpoint string, statusCode int, duration time.Duration) {
	m.RecordTenantRequestWithRoute(tenant, endpoint, "", statusCode, duration)
}

func (m *Metrics) RecordTenantRequestWithRoute(tenant, endpoint, route string, statusCode int, duration time.Duration) {
	tenant = m.canonicalTenantLabel(tenant)
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(tenant, endpoint, route, strconv.Itoa(statusCode))
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

	durKey := joinMetricKey(tenant, endpoint, route)
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
	m.RecordClientIdentityWithRoute(clientID, endpoint, "", duration, responseBytes)
}

func (m *Metrics) RecordClientIdentityWithRoute(clientID, endpoint, route string, duration time.Duration, responseBytes int64) {
	clientID = m.canonicalClientLabel(clientID)
	route = normalizeMetricRoute(route, endpoint)

	key := joinMetricKey(clientID, endpoint, route)
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
	m.RecordClientErrorWithRoute(endpoint, "", reason)
}

func (m *Metrics) RecordClientErrorWithRoute(endpoint, route, reason string) {
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(endpoint, route, reason)
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
	m.RecordClientStatusWithRoute(clientID, endpoint, "", statusCode)
}

func (m *Metrics) RecordClientStatusWithRoute(clientID, endpoint, route string, statusCode int) {
	clientID = m.canonicalClientLabel(clientID)
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(clientID, endpoint, route, strconv.Itoa(statusCode))
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
	m.RecordClientQueryLengthWithRoute(clientID, endpoint, "", queryLength)
}

func (m *Metrics) RecordClientQueryLengthWithRoute(clientID, endpoint, route string, queryLength int) {
	clientID = m.canonicalClientLabel(clientID)
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(clientID, endpoint, route)
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
	m.RecordEndpointCacheHitWithRoute(endpoint, "")
}

func (m *Metrics) RecordEndpointCacheHitWithRoute(endpoint, route string) {
	m.cacheHits.Add(1)
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(endpoint, route)
	m.mu.RLock()
	counter, ok := m.endpointCacheHits[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		counter, ok = m.endpointCacheHits[key]
		if !ok {
			counter = &atomic.Int64{}
			m.endpointCacheHits[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordEndpointCacheMiss records a cache miss for a specific endpoint.
func (m *Metrics) RecordEndpointCacheMiss(endpoint string) {
	m.RecordEndpointCacheMissWithRoute(endpoint, "")
}

func (m *Metrics) RecordEndpointCacheMissWithRoute(endpoint, route string) {
	m.cacheMisses.Add(1)
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(endpoint, route)
	m.mu.RLock()
	counter, ok := m.endpointCacheMisses[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		counter, ok = m.endpointCacheMisses[key]
		if !ok {
			counter = &atomic.Int64{}
			m.endpointCacheMisses[key] = counter
		}
		m.mu.Unlock()
	}
	counter.Add(1)
}

// RecordBackendDuration records VL backend response time for an endpoint.
func (m *Metrics) RecordBackendDuration(endpoint string, d time.Duration) {
	m.RecordBackendDurationWithRoute(endpoint, "", d)
}

func (m *Metrics) RecordBackendDurationWithRoute(endpoint, route string, d time.Duration) {
	route = normalizeMetricRoute(route, endpoint)
	key := joinMetricKey(endpoint, route)
	m.mu.RLock()
	h, ok := m.backendDurations[key]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		h, ok = m.backendDurations[key]
		if !ok {
			h = newHistogram()
			m.backendDurations[key] = h
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
	total := kept + skipped
	if total > 0 {
		ratio := float64(kept) / float64(total)
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		m.windowPrefilterHitRatioPpm.Store(int64(ratio * 1_000_000))
	}
}

// RecordQueryRangeWindowRetry records retry attempts for query_range window fetches.
func (m *Metrics) RecordQueryRangeWindowRetry() {
	m.windowRetries.Add(1)
}

// RecordQueryRangeWindowDegradedBatch records degraded batch execution events.
func (m *Metrics) RecordQueryRangeWindowDegradedBatch() {
	m.windowDegradedBatches.Add(1)
}

// RecordQueryRangeWindowPartialResponse records partial query_range responses.
func (m *Metrics) RecordQueryRangeWindowPartialResponse() {
	m.windowPartialResponses.Add(1)
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
		parts := splitMetricKey(key, 5)
		if parts == nil {
			continue
		}
		count := m.requestsTotal[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_requests_total{system=%q,direction=%q,endpoint=%q,route=%q,status=%q} %d\n", parts[0], parts[1], parts[2], parts[3], parts[4], count)
	}

	sb.WriteString("# HELP loki_vl_proxy_request_duration_seconds Request duration histogram.\n")
	sb.WriteString("# TYPE loki_vl_proxy_request_duration_seconds histogram\n")
	endpoints := make([]string, 0, len(m.requestDurations))
	for ep := range m.requestDurations {
		endpoints = append(endpoints, ep)
	}
	sort.Strings(endpoints)
	for _, key := range endpoints {
		parts := splitMetricKey(key, 4)
		if parts == nil {
			continue
		}
		h := m.requestDurations[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_bucket{system=%q,direction=%q,endpoint=%q,route=%q,le=\"%g\"} %d\n", parts[0], parts[1], parts[2], parts[3], b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_bucket{system=%q,direction=%q,endpoint=%q,route=%q,le=\"+Inf\"} %d\n", parts[0], parts[1], parts[2], parts[3], h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_sum{system=%q,direction=%q,endpoint=%q,route=%q} %g\n", parts[0], parts[1], parts[2], parts[3], h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_request_duration_seconds_count{system=%q,direction=%q,endpoint=%q,route=%q} %d\n", parts[0], parts[1], parts[2], parts[3], h.count)
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
	sb.WriteString("# HELP loki_vl_proxy_go_memstats_alloc_bytes Current heap allocation in bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_memstats_alloc_bytes %d\n", memStats.Alloc)

	sb.WriteString("# HELP go_memstats_sys_bytes Total memory from OS in bytes.\n")
	sb.WriteString("# TYPE go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_sys_bytes %d\n", memStats.Sys)
	sb.WriteString("# HELP loki_vl_proxy_go_memstats_sys_bytes Total memory from OS in bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_memstats_sys_bytes %d\n", memStats.Sys)

	sb.WriteString("# HELP go_memstats_heap_inuse_bytes Heap in-use bytes.\n")
	sb.WriteString("# TYPE go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_heap_inuse_bytes %d\n", memStats.HeapInuse)
	sb.WriteString("# HELP loki_vl_proxy_go_memstats_heap_inuse_bytes Heap in-use bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_memstats_heap_inuse_bytes %d\n", memStats.HeapInuse)

	sb.WriteString("# HELP go_memstats_heap_idle_bytes Heap idle bytes.\n")
	sb.WriteString("# TYPE go_memstats_heap_idle_bytes gauge\n")
	fmt.Fprintf(&sb, "go_memstats_heap_idle_bytes %d\n", memStats.HeapIdle)
	sb.WriteString("# HELP loki_vl_proxy_go_memstats_heap_idle_bytes Heap idle bytes.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_memstats_heap_idle_bytes gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_memstats_heap_idle_bytes %d\n", memStats.HeapIdle)

	sb.WriteString("# HELP go_gc_duration_seconds GC pause duration summary.\n")
	sb.WriteString("# TYPE go_gc_duration_seconds summary\n")
	fmt.Fprintf(&sb, "go_gc_duration_seconds_count %d\n", memStats.NumGC)
	sb.WriteString("# HELP loki_vl_proxy_go_gc_duration_seconds GC pause duration summary.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_gc_duration_seconds summary\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_gc_duration_seconds_count %d\n", memStats.NumGC)

	sb.WriteString("# HELP go_goroutines Current number of goroutines.\n")
	sb.WriteString("# TYPE go_goroutines gauge\n")
	fmt.Fprintf(&sb, "go_goroutines %d\n", runtime.NumGoroutine())
	sb.WriteString("# HELP loki_vl_proxy_go_goroutines Current number of goroutines.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_goroutines gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_goroutines %d\n", runtime.NumGoroutine())

	sb.WriteString("# HELP go_gc_cycles_total Total GC cycles completed.\n")
	sb.WriteString("# TYPE go_gc_cycles_total counter\n")
	fmt.Fprintf(&sb, "go_gc_cycles_total %d\n", memStats.NumGC)
	sb.WriteString("# HELP loki_vl_proxy_go_gc_cycles_total Total GC cycles completed.\n")
	sb.WriteString("# TYPE loki_vl_proxy_go_gc_cycles_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_go_gc_cycles_total %d\n", memStats.NumGC)

	// Per-tenant request counters
	sb.WriteString("# HELP loki_vl_proxy_tenant_requests_total Requests by tenant.\n")
	sb.WriteString("# TYPE loki_vl_proxy_tenant_requests_total counter\n")
	tenantKeys := make([]string, 0, len(m.tenantRequests))
	for k := range m.tenantRequests {
		tenantKeys = append(tenantKeys, k)
	}
	sort.Strings(tenantKeys)
	for _, key := range tenantKeys {
		parts := splitMetricKey(key, 4)
		if parts == nil {
			continue
		}
		count := m.tenantRequests[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_requests_total{system=%q,direction=%q,tenant=%q,endpoint=%q,route=%q,status=%q} %d\n",
			lokiSystemLabel, downstreamDirection, parts[0], parts[1], parts[2], parts[3], count)
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
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		tenant, endpoint, route := parts[0], parts[1], parts[2]
		h := m.tenantDurations[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_bucket{system=%q,direction=%q,tenant=%q,endpoint=%q,route=%q,le=\"%g\"} %d\n",
				lokiSystemLabel, downstreamDirection, tenant, endpoint, route, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_bucket{system=%q,direction=%q,tenant=%q,endpoint=%q,route=%q,le=\"+Inf\"} %d\n",
			lokiSystemLabel, downstreamDirection, tenant, endpoint, route, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_sum{system=%q,direction=%q,tenant=%q,endpoint=%q,route=%q} %g\n",
			lokiSystemLabel, downstreamDirection, tenant, endpoint, route, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_tenant_request_duration_seconds_count{system=%q,direction=%q,tenant=%q,endpoint=%q,route=%q} %d\n",
			lokiSystemLabel, downstreamDirection, tenant, endpoint, route, h.count)
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
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		count := m.clientErrors[key].Load()
		fmt.Fprintf(&sb, "loki_vl_proxy_client_errors_total{system=%q,direction=%q,endpoint=%q,route=%q,reason=%q} %d\n",
			lokiSystemLabel, downstreamDirection, parts[0], parts[1], parts[2], count)
	}
	// Per-endpoint cache stats
	sb.WriteString("# HELP loki_vl_proxy_cache_hits_by_endpoint Cache hits per endpoint.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_hits_by_endpoint counter\n")
	cacheHitKeys := make([]string, 0, len(m.endpointCacheHits))
	for k := range m.endpointCacheHits {
		cacheHitKeys = append(cacheHitKeys, k)
	}
	sort.Strings(cacheHitKeys)
	for _, key := range cacheHitKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_cache_hits_by_endpoint{system=%q,direction=%q,endpoint=%q,route=%q} %d\n", lokiSystemLabel, downstreamDirection, parts[0], parts[1], m.endpointCacheHits[key].Load())
	}
	sb.WriteString("# HELP loki_vl_proxy_cache_misses_by_endpoint Cache misses per endpoint.\n")
	sb.WriteString("# TYPE loki_vl_proxy_cache_misses_by_endpoint counter\n")
	cacheMissKeys := make([]string, 0, len(m.endpointCacheMisses))
	for k := range m.endpointCacheMisses {
		cacheMissKeys = append(cacheMissKeys, k)
	}
	sort.Strings(cacheMissKeys)
	for _, key := range cacheMissKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_cache_misses_by_endpoint{system=%q,direction=%q,endpoint=%q,route=%q} %d\n", lokiSystemLabel, downstreamDirection, parts[0], parts[1], m.endpointCacheMisses[key].Load())
	}

	// Backend latency histogram
	sb.WriteString("# HELP loki_vl_proxy_backend_duration_seconds VL backend response time.\n")
	sb.WriteString("# TYPE loki_vl_proxy_backend_duration_seconds histogram\n")
	bdKeys := make([]string, 0, len(m.backendDurations))
	for k := range m.backendDurations {
		bdKeys = append(bdKeys, k)
	}
	sort.Strings(bdKeys)
	for _, key := range bdKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		endpoint := parts[0]
		route := parts[1]
		h := m.backendDurations[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_bucket{system=%q,direction=%q,endpoint=%q,route=%q,le=\"%g\"} %d\n", vlSystemLabel, upstreamDirection, endpoint, route, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_bucket{system=%q,direction=%q,endpoint=%q,route=%q,le=\"+Inf\"} %d\n", vlSystemLabel, upstreamDirection, endpoint, route, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_sum{system=%q,direction=%q,endpoint=%q,route=%q} %g\n", vlSystemLabel, upstreamDirection, endpoint, route, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_backend_duration_seconds_count{system=%q,direction=%q,endpoint=%q,route=%q} %d\n", vlSystemLabel, upstreamDirection, endpoint, route, h.count)
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

	sb.WriteString("# HELP loki_vl_proxy_window_prefilter_hit_ratio Prefilter hit ratio (kept / total windows).\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_prefilter_hit_ratio gauge\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_prefilter_hit_ratio %g\n", float64(m.windowPrefilterHitRatioPpm.Load())/1000000.0)

	sb.WriteString("# HELP loki_vl_proxy_window_retry_total Query-range window retry attempts.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_retry_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_retry_total %d\n", m.windowRetries.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_degraded_batch_total Query-range batches degraded to lower parallelism.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_degraded_batch_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_degraded_batch_total %d\n", m.windowDegradedBatches.Load())

	sb.WriteString("# HELP loki_vl_proxy_window_partial_response_total Query-range partial responses due to retryable backend failures.\n")
	sb.WriteString("# TYPE loki_vl_proxy_window_partial_response_total counter\n")
	fmt.Fprintf(&sb, "loki_vl_proxy_window_partial_response_total %d\n", m.windowPartialResponses.Load())

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
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_requests_total{system=%q,direction=%q,client=%q,endpoint=%q,route=%q} %d\n",
			lokiSystemLabel, downstreamDirection, parts[0], parts[1], parts[2], m.clientRequests[key].Load())
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
		parts := splitMetricKey(key, 4)
		if parts == nil {
			continue
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_status_total{system=%q,direction=%q,client=%q,endpoint=%q,route=%q,status=%q} %d\n",
			lokiSystemLabel, downstreamDirection, parts[0], parts[1], parts[2], parts[3], m.clientStatuses[key].Load())
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
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		client, endpoint, route := parts[0], parts[1], parts[2]
		h := m.clientDurations[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_bucket{system=%q,direction=%q,client=%q,endpoint=%q,route=%q,le=\"%g\"} %d\n",
				lokiSystemLabel, downstreamDirection, client, endpoint, route, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_bucket{system=%q,direction=%q,client=%q,endpoint=%q,route=%q,le=\"+Inf\"} %d\n",
			lokiSystemLabel, downstreamDirection, client, endpoint, route, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_sum{system=%q,direction=%q,client=%q,endpoint=%q,route=%q} %g\n",
			lokiSystemLabel, downstreamDirection, client, endpoint, route, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_request_duration_seconds_count{system=%q,direction=%q,client=%q,endpoint=%q,route=%q} %d\n",
			lokiSystemLabel, downstreamDirection, client, endpoint, route, h.count)
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
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		client, endpoint, route := parts[0], parts[1], parts[2]
		h := m.clientQueryLengths[key]
		h.mu.Lock()
		for i, b := range h.buckets {
			fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_bucket{system=%q,direction=%q,client=%q,endpoint=%q,route=%q,le=\"%g\"} %d\n",
				lokiSystemLabel, downstreamDirection, client, endpoint, route, b, h.counts[i])
		}
		fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_bucket{system=%q,direction=%q,client=%q,endpoint=%q,route=%q,le=\"+Inf\"} %d\n",
			lokiSystemLabel, downstreamDirection, client, endpoint, route, h.count)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_sum{system=%q,direction=%q,client=%q,endpoint=%q,route=%q} %g\n",
			lokiSystemLabel, downstreamDirection, client, endpoint, route, h.sum)
		fmt.Fprintf(&sb, "loki_vl_proxy_client_query_length_chars_count{system=%q,direction=%q,client=%q,endpoint=%q,route=%q} %d\n",
			lokiSystemLabel, downstreamDirection, client, endpoint, route, h.count)
		h.mu.Unlock()
	}

	// Circuit breaker state (export a default closed=0 value when callback is unavailable).
	cbState := "closed"
	if m.cbStateFunc != nil {
		cbState = m.cbStateFunc()
	}
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

	m.mu.RUnlock()

	// System-level metrics (/proc on Linux)
	m.system.WritePrometheus(&sb)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(sb.String()))
}
