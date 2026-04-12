package metrics

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// OTLPCompression defines the compression algorithm for OTLP push.
type OTLPCompression string

const (
	OTLPCompressionNone OTLPCompression = "none"
	OTLPCompressionGzip OTLPCompression = "gzip"
	OTLPCompressionZstd OTLPCompression = "zstd"
)

// OTLPPusher periodically exports proxy metrics to an OTLP HTTP endpoint.
// Uses lightweight JSON payloads — no heavy OTel SDK dependency.
type OTLPPusher struct {
	endpoint              string
	interval              time.Duration
	metrics               *Metrics
	client                *http.Client
	log                   *slog.Logger
	cancel                context.CancelFunc
	headers               map[string]string
	compression           OTLPCompression
	serviceName           string
	serviceNamespace      string
	serviceVersion        string
	serviceInstanceID     string
	deploymentEnvironment string
	systemMu              sync.Mutex
	prevCPU               cpuStat
	prevProcCPU           processCPUStat
	prevSystemAt          time.Time
	hasPrevSystem         bool
}

// OTLPConfig configures the OTLP metrics pusher.
type OTLPConfig struct {
	Endpoint              string            // OTLP HTTP endpoint (e.g., http://otel-collector:4318/v1/metrics)
	Interval              time.Duration     // Push interval (default: 30s)
	Headers               map[string]string // Extra headers (e.g., auth tokens)
	Timeout               time.Duration     // HTTP request timeout (default: 10s)
	Compression           OTLPCompression   // Compression: "none", "gzip", "zstd" (default: "none")
	TLSSkipVerify         bool              // Skip TLS verification (for self-signed certs)
	ServiceName           string
	ServiceNamespace      string
	ServiceVersion        string
	ServiceInstanceID     string
	DeploymentEnvironment string
}

// NewOTLPPusher creates a new OTLP metrics pusher.
func NewOTLPPusher(cfg OTLPConfig, m *Metrics) *OTLPPusher {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.TLSSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	logger := slog.Default().With("component", "otlp_metrics")
	compression := cfg.Compression
	if compression == "" {
		compression = OTLPCompressionNone
	}
	endpoint := normalizeMetricsEndpoint(cfg.Endpoint)

	p := &OTLPPusher{
		endpoint:              endpoint,
		interval:              cfg.Interval,
		metrics:               m,
		client:                &http.Client{Timeout: timeout, Transport: transport},
		log:                   logger,
		headers:               cfg.Headers,
		compression:           compression,
		serviceName:           strings.TrimSpace(cfg.ServiceName),
		serviceNamespace:      strings.TrimSpace(cfg.ServiceNamespace),
		serviceVersion:        strings.TrimSpace(cfg.ServiceVersion),
		serviceInstanceID:     strings.TrimSpace(cfg.ServiceInstanceID),
		deploymentEnvironment: strings.TrimSpace(cfg.DeploymentEnvironment),
	}
	if runtime.GOOS == "linux" {
		if cpu, cpuErr := readCPUStat(); cpuErr == nil {
			if procCPU, procErr := readProcessCPUStat(); procErr == nil {
				p.prevCPU = cpu
				p.prevProcCPU = procCPU
				p.prevSystemAt = time.Now()
				p.hasPrevSystem = true
			}
		}
	}
	return p
}

// Start begins periodic metric export in a background goroutine.
func (p *OTLPPusher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.push(ctx); err != nil {
					p.log.Error("otlp push failed", "error", err)
				}
			}
		}
	}()
}

// Stop stops the periodic pusher.
func (p *OTLPPusher) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// push sends current metrics as OTLP JSON.
func (p *OTLPPusher) push(ctx context.Context) error {
	payload := p.buildPayload()
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal otlp payload: %w", err)
	}

	var reqBody *bytes.Reader
	contentEncoding := ""

	switch p.compression {
	case OTLPCompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(body); err != nil {
			return fmt.Errorf("gzip compress: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
		reqBody = bytes.NewReader(buf.Bytes())
		contentEncoding = "gzip"
	case OTLPCompressionZstd:
		var buf bytes.Buffer
		w, err := zstd.NewWriter(&buf)
		if err != nil {
			return fmt.Errorf("zstd writer: %w", err)
		}
		if _, err := w.Write(body); err != nil {
			return fmt.Errorf("zstd compress: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("zstd close: %w", err)
		}
		reqBody = bytes.NewReader(buf.Bytes())
		contentEncoding = "zstd"
	default:
		reqBody = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", p.endpoint, reqBody)
	if err != nil {
		return fmt.Errorf("create otlp request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("otlp request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("otlp endpoint returned %d", resp.StatusCode)
	}
	return nil
}

// buildPayload constructs an OTLP ExportMetricsServiceRequest as JSON.
func (p *OTLPPusher) buildPayload() map[string]interface{} {
	now := time.Now().UnixNano()

	metrics := make([]map[string]interface{}, 0, 32)
	metrics = append(metrics,
		p.sumMetric("loki_vl_proxy_cache_hits_total", "Cache hits.", "", p.counterDP("loki_vl_proxy_cache_hits_total", p.metrics.cacheHits.Load(), now)),
		p.sumMetric("loki_vl_proxy_cache_misses_total", "Cache misses.", "", p.counterDP("loki_vl_proxy_cache_misses_total", p.metrics.cacheMisses.Load(), now)),
		p.sumMetric("loki_vl_proxy_window_cache_hit_total", "Query-range window cache hits.", "", p.counterDP("loki_vl_proxy_window_cache_hit_total", p.metrics.windowCacheHits.Load(), now)),
		p.sumMetric("loki_vl_proxy_window_cache_miss_total", "Query-range window cache misses.", "", p.counterDP("loki_vl_proxy_window_cache_miss_total", p.metrics.windowCacheMisses.Load(), now)),
		p.sumMetric("loki_vl_proxy_translations_total", "LogQL to LogsQL translations.", "", p.counterDP("loki_vl_proxy_translations_total", p.metrics.translationsTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_translation_errors_total", "Failed translations.", "", p.counterDP("loki_vl_proxy_translation_errors_total", p.metrics.translationErrors.Load(), now)),
		p.sumMetric("loki_vl_proxy_coalesced_total", "Requests served from coalesced results.", "", p.counterDP("loki_vl_proxy_coalesced_total", p.metrics.coalescedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_coalesced_saved_total", "Backend requests saved by coalescing.", "", p.counterDP("loki_vl_proxy_coalesced_saved_total", p.metrics.coalescedSaved.Load(), now)),
		p.gaugeMetric("loki_vl_proxy_uptime_seconds", "Proxy uptime.", "s", p.gaugeDP("loki_vl_proxy_uptime_seconds", time.Since(p.metrics.startTime).Seconds(), now)),
		p.gaugeMetric("loki_vl_proxy_active_requests", "Current in-flight requests.", "{request}", p.gaugeDP("loki_vl_proxy_active_requests", float64(p.metrics.activeRequests.Load()), now)),
	)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	metrics = append(metrics,
		p.gaugeMetric("go_memstats_alloc_bytes", "Current heap allocation in bytes.", "By", p.gaugeDP("go_memstats_alloc_bytes", float64(memStats.Alloc), now)),
		p.gaugeMetric("loki_vl_proxy_go_memstats_alloc_bytes", "Current heap allocation in bytes.", "By", p.gaugeDP("loki_vl_proxy_go_memstats_alloc_bytes", float64(memStats.Alloc), now)),
		p.gaugeMetric("go_memstats_sys_bytes", "Total memory from OS in bytes.", "By", p.gaugeDP("go_memstats_sys_bytes", float64(memStats.Sys), now)),
		p.gaugeMetric("loki_vl_proxy_go_memstats_sys_bytes", "Total memory from OS in bytes.", "By", p.gaugeDP("loki_vl_proxy_go_memstats_sys_bytes", float64(memStats.Sys), now)),
		p.gaugeMetric("go_memstats_heap_inuse_bytes", "Heap in-use bytes.", "By", p.gaugeDP("go_memstats_heap_inuse_bytes", float64(memStats.HeapInuse), now)),
		p.gaugeMetric("loki_vl_proxy_go_memstats_heap_inuse_bytes", "Heap in-use bytes.", "By", p.gaugeDP("loki_vl_proxy_go_memstats_heap_inuse_bytes", float64(memStats.HeapInuse), now)),
		p.gaugeMetric("go_memstats_heap_idle_bytes", "Heap idle bytes.", "By", p.gaugeDP("go_memstats_heap_idle_bytes", float64(memStats.HeapIdle), now)),
		p.gaugeMetric("loki_vl_proxy_go_memstats_heap_idle_bytes", "Heap idle bytes.", "By", p.gaugeDP("loki_vl_proxy_go_memstats_heap_idle_bytes", float64(memStats.HeapIdle), now)),
		p.gaugeMetric("go_goroutines", "Current number of goroutines.", "{goroutine}", p.gaugeDP("go_goroutines", float64(runtime.NumGoroutine()), now)),
		p.gaugeMetric("loki_vl_proxy_go_goroutines", "Current number of goroutines.", "{goroutine}", p.gaugeDP("loki_vl_proxy_go_goroutines", float64(runtime.NumGoroutine()), now)),
		p.sumMetric("go_gc_cycles_total", "Total GC cycles completed.", "", p.counterDP("go_gc_cycles_total", int64(memStats.NumGC), now)),
		p.sumMetric("loki_vl_proxy_go_gc_cycles_total", "Total GC cycles completed.", "", p.counterDP("loki_vl_proxy_go_gc_cycles_total", int64(memStats.NumGC), now)),
	)

	if circuitBreakerMetric := p.circuitBreakerMetric(now); circuitBreakerMetric != nil {
		metrics = append(metrics, circuitBreakerMetric)
	}
	metrics = append(metrics, p.queryRangeWindowMetrics(now)...)

	metrics = append(metrics, p.systemMetrics(now)...)

	p.metrics.mu.RLock()
	metrics = append(metrics, p.requestMetrics(now)...)
	metrics = append(metrics, p.tenantMetrics(now)...)
	metrics = append(metrics, p.clientErrorMetrics(now)...)
	metrics = append(metrics, p.endpointCacheMetrics(now)...)
	metrics = append(metrics, p.backendDurationMetrics(now)...)
	metrics = append(metrics, p.clientMetrics(now)...)
	p.metrics.mu.RUnlock()

	return map[string]interface{}{
		"resourceMetrics": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": p.resourceAttributes(),
				},
				"scopeMetrics": []map[string]interface{}{
					{
						"scope": map[string]interface{}{
							"name":    "loki-vl-proxy",
							"version": p.metricsScopeVersion(),
						},
						"metrics": metrics,
					},
				},
			},
		},
	}
}

func (p *OTLPPusher) counterDP(name string, value int64, timeUnixNano int64, attrs ...map[string]interface{}) map[string]interface{} {
	dp := map[string]interface{}{
		"asInt":        value,
		"timeUnixNano": fmt.Sprintf("%d", timeUnixNano),
	}
	if len(attrs) > 0 {
		dp["attributes"] = attrs
	}
	return dp
}

func (p *OTLPPusher) gaugeDP(name string, value float64, timeUnixNano int64, attrs ...map[string]interface{}) map[string]interface{} {
	dp := map[string]interface{}{
		"asDouble":     value,
		"timeUnixNano": fmt.Sprintf("%d", timeUnixNano),
	}
	if len(attrs) > 0 {
		dp["attributes"] = attrs
	}
	return dp
}

func attr(key, value string) map[string]interface{} {
	return map[string]interface{}{
		"key":   key,
		"value": map[string]interface{}{"stringValue": value},
	}
}

func normalizeMetricsEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	switch strings.TrimRight(u.Path, "/") {
	case "":
		u.Path = "/v1/metrics"
	case "/v1":
		u.Path = strings.TrimRight(u.Path, "/") + "/metrics"
	}
	return u.String()
}

func (p *OTLPPusher) resourceAttributes() []map[string]interface{} {
	attrs := []map[string]interface{}{
		attr("service.name", valueOrFallback(p.serviceName, "loki-vl-proxy", "")),
	}
	if v := strings.TrimSpace(p.serviceNamespace); v != "" {
		attrs = append(attrs, attr("service.namespace", v))
	}
	if v := p.metricsScopeVersion(); v != "" {
		attrs = append(attrs, attr("service.version", v))
	}
	if v := strings.TrimSpace(p.serviceInstanceID); v != "" {
		attrs = append(attrs, attr("service.instance.id", v))
	}
	if v := strings.TrimSpace(p.deploymentEnvironment); v != "" {
		attrs = append(attrs, attr("deployment.environment.name", v))
	}
	attrs = append(attrs,
		attr("telemetry.sdk.name", "loki-vl-proxy"),
		attr("telemetry.sdk.language", "go"),
		attr("telemetry.sdk.version", runtime.Version()),
	)
	return attrs
}

func valueOrFallback(primary, fallback, unset string) string {
	primary = strings.TrimSpace(primary)
	if primary != "" && primary != unset {
		return primary
	}
	return fallback
}

func (p *OTLPPusher) metricsScopeVersion() string {
	v := strings.TrimSpace(p.serviceVersion)
	if v == "" {
		return "dev"
	}
	return v
}

func (p *OTLPPusher) sumMetric(name, description, unit string, dataPoints ...map[string]interface{}) map[string]interface{} {
	metric := map[string]interface{}{
		"name":        name,
		"description": description,
		"sum": map[string]interface{}{
			"dataPoints":             dataPoints,
			"aggregationTemporality": 2,
			"isMonotonic":            true,
		},
	}
	if unit != "" {
		metric["unit"] = unit
	}
	return metric
}

func (p *OTLPPusher) gaugeMetric(name, description, unit string, dataPoints ...map[string]interface{}) map[string]interface{} {
	metric := map[string]interface{}{
		"name":        name,
		"description": description,
		"gauge": map[string]interface{}{
			"dataPoints": dataPoints,
		},
	}
	if unit != "" {
		metric["unit"] = unit
	}
	return metric
}

func (p *OTLPPusher) histogramMetric(name, description, unit string, dataPoints ...map[string]interface{}) map[string]interface{} {
	metric := map[string]interface{}{
		"name":        name,
		"description": description,
		"histogram": map[string]interface{}{
			"aggregationTemporality": 2,
			"dataPoints":             dataPoints,
		},
	}
	if unit != "" {
		metric["unit"] = unit
	}
	return metric
}

func (p *OTLPPusher) histogramDP(h *histogram, timeUnixNano int64, attrs ...map[string]interface{}) map[string]interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	bucketCounts := make([]int64, 0, len(h.counts)+1)
	prev := int64(0)
	for _, cumulative := range h.counts {
		bucketCounts = append(bucketCounts, cumulative-prev)
		prev = cumulative
	}
	bucketCounts = append(bucketCounts, h.count-prev)
	dp := map[string]interface{}{
		"timeUnixNano":   fmt.Sprintf("%d", timeUnixNano),
		"count":          fmt.Sprintf("%d", h.count),
		"sum":            h.sum,
		"explicitBounds": append([]float64(nil), h.buckets...),
		"bucketCounts":   bucketCounts,
	}
	if len(attrs) > 0 {
		dp["attributes"] = attrs
	}
	return dp
}

func (p *OTLPPusher) requestMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 2)
	reqKeys := sortedKeys(mapsFromCounters(p.metrics.requestsTotal))
	requestPoints := make([]map[string]interface{}, 0, len(reqKeys))
	for _, key := range reqKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		requestPoints = append(requestPoints, p.counterDP("loki_vl_proxy_requests_total", p.metrics.requestsTotal[key].Load(), now, attr("endpoint", parts[0]), attr("status", parts[1])))
	}
	if len(requestPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_requests_total", "Total number of proxied requests.", "", requestPoints...))
	}
	durationKeys := sortedKeys(mapsFromHists(p.metrics.requestDurations))
	durationPoints := make([]map[string]interface{}, 0, len(durationKeys))
	for _, endpoint := range durationKeys {
		durationPoints = append(durationPoints, p.histogramDP(p.metrics.requestDurations[endpoint], now, attr("endpoint", endpoint)))
	}
	if len(durationPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_request_duration_seconds", "Request duration histogram.", "s", durationPoints...))
	}
	return metrics
}

func (p *OTLPPusher) tenantMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 2)
	reqKeys := sortedKeys(mapsFromCounters(p.metrics.tenantRequests))
	reqPoints := make([]map[string]interface{}, 0, len(reqKeys))
	for _, key := range reqKeys {
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		reqPoints = append(reqPoints, p.counterDP("loki_vl_proxy_tenant_requests_total", p.metrics.tenantRequests[key].Load(), now, attr("tenant", parts[0]), attr("endpoint", parts[1]), attr("status", parts[2])))
	}
	if len(reqPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_tenant_requests_total", "Requests by tenant.", "", reqPoints...))
	}
	durKeys := sortedKeys(mapsFromHists(p.metrics.tenantDurations))
	durPoints := make([]map[string]interface{}, 0, len(durKeys))
	for _, key := range durKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		durPoints = append(durPoints, p.histogramDP(p.metrics.tenantDurations[key], now, attr("tenant", parts[0]), attr("endpoint", parts[1])))
	}
	if len(durPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_tenant_request_duration_seconds", "Per-tenant request duration.", "s", durPoints...))
	}
	return metrics
}

func (p *OTLPPusher) clientErrorMetrics(now int64) []map[string]interface{} {
	keys := sortedKeys(mapsFromCounters(p.metrics.clientErrors))
	points := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		points = append(points, p.counterDP("loki_vl_proxy_client_errors_total", p.metrics.clientErrors[key].Load(), now, attr("endpoint", parts[0]), attr("reason", parts[1])))
	}
	if len(points) == 0 {
		return nil
	}
	return []map[string]interface{}{p.sumMetric("loki_vl_proxy_client_errors_total", "Client errors by reason.", "", points...)}
}

func (p *OTLPPusher) endpointCacheMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 2)
	hitKeys := sortedKeys(mapsFromCounters(p.metrics.endpointCacheHits))
	hitPoints := make([]map[string]interface{}, 0, len(hitKeys))
	for _, endpoint := range hitKeys {
		hitPoints = append(hitPoints, p.counterDP("loki_vl_proxy_cache_hits_by_endpoint", p.metrics.endpointCacheHits[endpoint].Load(), now, attr("endpoint", endpoint)))
	}
	if len(hitPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_cache_hits_by_endpoint", "Cache hits per endpoint.", "", hitPoints...))
	}
	missKeys := sortedKeys(mapsFromCounters(p.metrics.endpointCacheMisses))
	missPoints := make([]map[string]interface{}, 0, len(missKeys))
	for _, endpoint := range missKeys {
		missPoints = append(missPoints, p.counterDP("loki_vl_proxy_cache_misses_by_endpoint", p.metrics.endpointCacheMisses[endpoint].Load(), now, attr("endpoint", endpoint)))
	}
	if len(missPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_cache_misses_by_endpoint", "Cache misses per endpoint.", "", missPoints...))
	}
	return metrics
}

func (p *OTLPPusher) backendDurationMetrics(now int64) []map[string]interface{} {
	keys := sortedKeys(mapsFromHists(p.metrics.backendDurations))
	points := make([]map[string]interface{}, 0, len(keys))
	for _, endpoint := range keys {
		points = append(points, p.histogramDP(p.metrics.backendDurations[endpoint], now, attr("endpoint", endpoint)))
	}
	if len(points) == 0 {
		return nil
	}
	return []map[string]interface{}{p.histogramMetric("loki_vl_proxy_backend_duration_seconds", "VL backend response time.", "s", points...)}
}

func (p *OTLPPusher) queryRangeWindowMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 6)
	if p.metrics.windowFetch != nil {
		metrics = append(metrics, p.histogramMetric(
			"loki_vl_proxy_window_fetch_seconds",
			"Query-range window backend fetch duration.",
			"s",
			p.histogramDP(p.metrics.windowFetch, now),
		))
	}
	if p.metrics.windowMerge != nil {
		metrics = append(metrics, p.histogramMetric(
			"loki_vl_proxy_window_merge_seconds",
			"Query-range window merge duration.",
			"s",
			p.histogramDP(p.metrics.windowMerge, now),
		))
	}
	if p.metrics.windowCount != nil {
		metrics = append(metrics, p.histogramMetric(
			"loki_vl_proxy_window_count",
			"Query-range window count per request.",
			"{window}",
			p.histogramDP(p.metrics.windowCount, now),
		))
	}
	metrics = append(metrics,
		p.gaugeMetric(
			"loki_vl_proxy_window_adaptive_parallel_current",
			"Current adaptive query-range window parallelism.",
			"{window}",
			p.gaugeDP("loki_vl_proxy_window_adaptive_parallel_current", float64(p.metrics.windowAdaptiveParallelCurrent.Load()), now),
		),
		p.gaugeMetric(
			"loki_vl_proxy_window_adaptive_latency_ewma_seconds",
			"EWMA backend fetch latency for query-range windows.",
			"s",
			p.gaugeDP("loki_vl_proxy_window_adaptive_latency_ewma_seconds", float64(p.metrics.windowAdaptiveLatencyEWMAms.Load())/1000.0, now),
		),
		p.gaugeMetric(
			"loki_vl_proxy_window_adaptive_error_ewma",
			"Adaptive backend window fetch error EWMA ratio (0-1).",
			"",
			p.gaugeDP("loki_vl_proxy_window_adaptive_error_ewma", float64(p.metrics.windowAdaptiveErrorEWMAppm.Load())/1000000.0, now),
		),
	)
	return metrics
}

func (p *OTLPPusher) clientMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 6)
	reqKeys := sortedKeys(mapsFromCounters(p.metrics.clientRequests))
	reqPoints := make([]map[string]interface{}, 0, len(reqKeys))
	for _, key := range reqKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		reqPoints = append(reqPoints, p.counterDP("loki_vl_proxy_client_requests_total", p.metrics.clientRequests[key].Load(), now, attr("client", parts[0]), attr("endpoint", parts[1])))
	}
	if len(reqPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_client_requests_total", "Requests by client identity.", "", reqPoints...))
	}
	byteKeys := sortedKeys(mapsFromCounters(p.metrics.clientBytes))
	bytePoints := make([]map[string]interface{}, 0, len(byteKeys))
	for _, client := range byteKeys {
		bytePoints = append(bytePoints, p.counterDP("loki_vl_proxy_client_response_bytes_total", p.metrics.clientBytes[client].Load(), now, attr("client", client)))
	}
	if len(bytePoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_client_response_bytes_total", "Response bytes by client.", "By", bytePoints...))
	}
	statusKeys := sortedKeys(mapsFromCounters(p.metrics.clientStatuses))
	statusPoints := make([]map[string]interface{}, 0, len(statusKeys))
	for _, key := range statusKeys {
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		statusPoints = append(statusPoints, p.counterDP("loki_vl_proxy_client_status_total", p.metrics.clientStatuses[key].Load(), now, attr("client", parts[0]), attr("endpoint", parts[1]), attr("status", parts[2])))
	}
	if len(statusPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_client_status_total", "Requests by client identity and final HTTP status.", "", statusPoints...))
	}
	inflightKeys := sortedKeys(mapsFromCounters(p.metrics.clientInflight))
	inflightPoints := make([]map[string]interface{}, 0, len(inflightKeys))
	for _, client := range inflightKeys {
		inflightPoints = append(inflightPoints, p.gaugeDP("loki_vl_proxy_client_inflight_requests", float64(p.metrics.clientInflight[client].Load()), now, attr("client", client)))
	}
	if len(inflightPoints) > 0 {
		metrics = append(metrics, p.gaugeMetric("loki_vl_proxy_client_inflight_requests", "In-flight requests by client identity.", "{request}", inflightPoints...))
	}
	durationKeys := sortedKeys(mapsFromHists(p.metrics.clientDurations))
	durationPoints := make([]map[string]interface{}, 0, len(durationKeys))
	for _, key := range durationKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		durationPoints = append(durationPoints, p.histogramDP(p.metrics.clientDurations[key], now, attr("client", parts[0]), attr("endpoint", parts[1])))
	}
	if len(durationPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_client_request_duration_seconds", "Per-client request duration.", "s", durationPoints...))
	}
	queryLenKeys := sortedKeys(mapsFromHists(p.metrics.clientQueryLengths))
	queryLenPoints := make([]map[string]interface{}, 0, len(queryLenKeys))
	for _, key := range queryLenKeys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		queryLenPoints = append(queryLenPoints, p.histogramDP(p.metrics.clientQueryLengths[key], now, attr("client", parts[0]), attr("endpoint", parts[1])))
	}
	if len(queryLenPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_client_query_length_chars", "LogQL query length by client identity.", "{character}", queryLenPoints...))
	}
	return metrics
}

func (p *OTLPPusher) systemMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 12)
	if runtime.GOOS != "linux" {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		metrics = append(metrics,
			p.gaugeMetric("process_resident_memory_bytes", "Resident memory size.", "By", p.gaugeDP("process_resident_memory_bytes", float64(mem.Sys), now)),
			p.gaugeMetric("loki_vl_proxy_process_resident_memory_bytes", "Resident memory size.", "By", p.gaugeDP("loki_vl_proxy_process_resident_memory_bytes", float64(mem.Sys), now)),
		)
		return metrics
	}
	readBytes, writeBytes := readDiskIO()
	rxBytes, txBytes := readNetIO()
	memTotal, memAvail, memFree := readMemInfo()
	usageRatio := 0.0
	if memTotal > 0 {
		usageRatio = float64(memTotal-memAvail) / float64(memTotal)
	}
	metrics = append(metrics,
		p.gaugeMetric("process_memory_total_bytes", "Total memory.", "By", p.gaugeDP("process_memory_total_bytes", float64(memTotal), now)),
		p.gaugeMetric("loki_vl_proxy_process_memory_total_bytes", "Total memory.", "By", p.gaugeDP("loki_vl_proxy_process_memory_total_bytes", float64(memTotal), now)),
		p.gaugeMetric("process_memory_available_bytes", "Available memory.", "By", p.gaugeDP("process_memory_available_bytes", float64(memAvail), now)),
		p.gaugeMetric("loki_vl_proxy_process_memory_available_bytes", "Available memory.", "By", p.gaugeDP("loki_vl_proxy_process_memory_available_bytes", float64(memAvail), now)),
		p.gaugeMetric("process_memory_free_bytes", "Free memory.", "By", p.gaugeDP("process_memory_free_bytes", float64(memFree), now)),
		p.gaugeMetric("loki_vl_proxy_process_memory_free_bytes", "Free memory.", "By", p.gaugeDP("loki_vl_proxy_process_memory_free_bytes", float64(memFree), now)),
		p.gaugeMetric("process_memory_usage_ratio", "Memory usage ratio (0-1).", "1", p.gaugeDP("process_memory_usage_ratio", usageRatio, now)),
		p.gaugeMetric("loki_vl_proxy_process_memory_usage_ratio", "Memory usage ratio (0-1).", "1", p.gaugeDP("loki_vl_proxy_process_memory_usage_ratio", usageRatio, now)),
	)
	rss := readProcessRSS()
	metrics = append(metrics,
		p.gaugeMetric("process_resident_memory_bytes", "Process RSS.", "By", p.gaugeDP("process_resident_memory_bytes", float64(rss), now)),
		p.gaugeMetric("loki_vl_proxy_process_resident_memory_bytes", "Process RSS.", "By", p.gaugeDP("loki_vl_proxy_process_resident_memory_bytes", float64(rss), now)),
	)
	metrics = append(metrics,
		p.sumMetric("process_disk_read_bytes_total", "Disk read bytes.", "By", p.counterDP("process_disk_read_bytes_total", readBytes, now)),
		p.sumMetric("loki_vl_proxy_process_disk_read_bytes_total", "Disk read bytes.", "By", p.counterDP("loki_vl_proxy_process_disk_read_bytes_total", readBytes, now)),
		p.sumMetric("process_disk_written_bytes_total", "Disk written bytes.", "By", p.counterDP("process_disk_written_bytes_total", writeBytes, now)),
		p.sumMetric("loki_vl_proxy_process_disk_written_bytes_total", "Disk written bytes.", "By", p.counterDP("loki_vl_proxy_process_disk_written_bytes_total", writeBytes, now)),
		p.sumMetric("process_network_receive_bytes_total", "Network receive bytes.", "By", p.counterDP("process_network_receive_bytes_total", rxBytes, now)),
		p.sumMetric("loki_vl_proxy_process_network_receive_bytes_total", "Network receive bytes.", "By", p.counterDP("loki_vl_proxy_process_network_receive_bytes_total", rxBytes, now)),
		p.sumMetric("process_network_transmit_bytes_total", "Network transmit bytes.", "By", p.counterDP("process_network_transmit_bytes_total", txBytes, now)),
		p.sumMetric("loki_vl_proxy_process_network_transmit_bytes_total", "Network transmit bytes.", "By", p.counterDP("loki_vl_proxy_process_network_transmit_bytes_total", txBytes, now)),
	)

	userCPU := 0.0
	systemCPU := 0.0
	p.systemMu.Lock()
	if cur, cpuErr := readCPUStat(); cpuErr == nil {
		if curProc, procErr := readProcessCPUStat(); procErr == nil {
			if p.hasPrevSystem {
				totalPrev := p.prevCPU.user + p.prevCPU.nice + p.prevCPU.system + p.prevCPU.idle + p.prevCPU.iowait + p.prevCPU.irq + p.prevCPU.softirq + p.prevCPU.steal
				totalCur := cur.user + cur.nice + cur.system + cur.idle + cur.iowait + cur.irq + cur.softirq + cur.steal
				totalDiff := totalCur - totalPrev
				userDiff := curProc.user - p.prevProcCPU.user
				sysDiff := curProc.system - p.prevProcCPU.system
				if totalDiff > 0 && userDiff >= 0 {
					userCPU = userDiff / totalDiff
				}
				if totalDiff > 0 && sysDiff >= 0 {
					systemCPU = sysDiff / totalDiff
				}
			}
			p.prevCPU = cur
			p.prevProcCPU = curProc
			p.prevSystemAt = time.Unix(0, now)
			p.hasPrevSystem = true
		}
	}
	p.systemMu.Unlock()
	metrics = append(metrics, p.gaugeMetric("process_cpu_usage_ratio", "CPU usage ratio (0-1).", "1",
		p.gaugeDP("process_cpu_usage_ratio", userCPU, now, attr("mode", "user")),
		p.gaugeDP("process_cpu_usage_ratio", systemCPU, now, attr("mode", "system")),
		p.gaugeDP("process_cpu_usage_ratio", 0, now, attr("mode", "iowait")),
	))
	metrics = append(metrics, p.gaugeMetric("loki_vl_proxy_process_cpu_usage_ratio", "CPU usage ratio (0-1).", "1",
		p.gaugeDP("loki_vl_proxy_process_cpu_usage_ratio", userCPU, now, attr("mode", "user")),
		p.gaugeDP("loki_vl_proxy_process_cpu_usage_ratio", systemCPU, now, attr("mode", "system")),
		p.gaugeDP("loki_vl_proxy_process_cpu_usage_ratio", 0, now, attr("mode", "iowait")),
	))
	for _, resource := range []string{"cpu", "memory", "io"} {
		some10, some60, some300, full10, full60, full300 := readPSI(resource)
		if some10 < 0 {
			some10, some60, some300 = 0, 0, 0
		}
		if full10 < 0 {
			full10, full60, full300 = 0, 0, 0
		}
		metrics = append(metrics, p.gaugeMetric("process_pressure_"+resource+"_some_ratio", "PSI some pressure.", "1",
			p.gaugeDP("", some10/100, now, attr("window", "10s")),
			p.gaugeDP("", some60/100, now, attr("window", "60s")),
			p.gaugeDP("", some300/100, now, attr("window", "300s")),
		))
		metrics = append(metrics, p.gaugeMetric("loki_vl_proxy_process_pressure_"+resource+"_some_ratio", "PSI some pressure.", "1",
			p.gaugeDP("", some10/100, now, attr("window", "10s")),
			p.gaugeDP("", some60/100, now, attr("window", "60s")),
			p.gaugeDP("", some300/100, now, attr("window", "300s")),
		))
		metrics = append(metrics, p.gaugeMetric("process_pressure_"+resource+"_full_ratio", "PSI full pressure.", "1",
			p.gaugeDP("", full10/100, now, attr("window", "10s")),
			p.gaugeDP("", full60/100, now, attr("window", "60s")),
			p.gaugeDP("", full300/100, now, attr("window", "300s")),
		))
		metrics = append(metrics, p.gaugeMetric("loki_vl_proxy_process_pressure_"+resource+"_full_ratio", "PSI full pressure.", "1",
			p.gaugeDP("", full10/100, now, attr("window", "10s")),
			p.gaugeDP("", full60/100, now, attr("window", "60s")),
			p.gaugeDP("", full300/100, now, attr("window", "300s")),
		))
	}
	fds := countOpenFDs()
	if fds < 0 {
		fds = 0
	}
	metrics = append(metrics,
		p.gaugeMetric("process_open_fds", "Open file descriptors.", "{file}", p.gaugeDP("process_open_fds", float64(fds), now)),
		p.gaugeMetric("loki_vl_proxy_process_open_fds", "Open file descriptors.", "{file}", p.gaugeDP("loki_vl_proxy_process_open_fds", float64(fds), now)),
	)
	return metrics
}

func (p *OTLPPusher) circuitBreakerMetric(now int64) map[string]interface{} {
	cbState := "closed"
	if p.metrics.cbStateFunc != nil {
		cbState = p.metrics.cbStateFunc()
	}
	value := 0.0
	switch cbState {
	case "open":
		value = 1
	case "half_open", "half-open":
		value = 2
	}
	return p.gaugeMetric("loki_vl_proxy_circuit_breaker_state", "Circuit breaker state (0=closed, 1=open, 2=half-open).", "1", p.gaugeDP("loki_vl_proxy_circuit_breaker_state", value, now))
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func mapsFromCounters(m map[string]*atomic.Int64) map[string]*atomic.Int64 { return m }
func mapsFromHists(m map[string]*histogram) map[string]*histogram          { return m }
