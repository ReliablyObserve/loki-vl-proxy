package metrics

import (
	"bytes"
	gzip "github.com/klauspost/compress/gzip"
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
	wg                    sync.WaitGroup
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
		// #nosec G402 — operator-explicit override; log a warning so it is visible.
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // nosemgrep: problem-based-packs.insecure-transport.go-stdlib.bypass-tls-verification,go.lang.security.audit.crypto.missing-ssl-minversion.missing-ssl-minversion
		slog.Warn("OTLP TLS verification disabled; MITM attacks on metrics traffic are possible",
			"endpoint", redactEndpointURL(cfg.Endpoint))
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

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
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
	p.wg.Wait()
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
		p.sumMetric("loki_vl_proxy_patterns_detected_total", "Unique patterns detected from pattern mining.", "", p.counterDP("loki_vl_proxy_patterns_detected_total", p.metrics.patternsDetectedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_stored_total", "Pattern entries stored in proxy cache/snapshot updates.", "", p.counterDP("loki_vl_proxy_patterns_stored_total", p.metrics.patternsStoredTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_restored_from_disk_total", "Pattern entries restored from on-disk snapshots.", "", p.counterDP("loki_vl_proxy_patterns_restored_from_disk_total", p.metrics.patternsRestoredFromDiskTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_restored_from_peers_total", "Pattern entries restored from peer snapshots.", "", p.counterDP("loki_vl_proxy_patterns_restored_from_peers_total", p.metrics.patternsRestoredFromPeersTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_restored_disk_entries_total", "Snapshot cache keys restored from disk.", "", p.counterDP("loki_vl_proxy_patterns_restored_disk_entries_total", p.metrics.patternsRestoredDiskEntriesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_restored_peer_entries_total", "Snapshot cache keys restored from peers.", "", p.counterDP("loki_vl_proxy_patterns_restored_peer_entries_total", p.metrics.patternsRestoredPeerEntriesTotal.Load(), now)),
		p.sumMetric(
			"loki_vl_proxy_patterns_deduplicated_total",
			"Duplicate pattern snapshot entries removed by source.",
			"",
			p.counterDP("loki_vl_proxy_patterns_deduplicated_total", p.metrics.patternsDeduplicatedMemTotal.Load(), now, attr("source", "mem")),
			p.counterDP("loki_vl_proxy_patterns_deduplicated_total", p.metrics.patternsDeduplicatedDiskTotal.Load(), now, attr("source", "disk")),
			p.counterDP("loki_vl_proxy_patterns_deduplicated_total", p.metrics.patternsDeduplicatedPeerTotal.Load(), now, attr("source", "peer")),
		),
		p.gaugeMetric("loki_vl_proxy_patterns_in_memory", "Current number of patterns held in in-memory snapshot state.", "{pattern}", p.gaugeDP("loki_vl_proxy_patterns_in_memory", float64(p.metrics.patternsInMemory.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_cache_keys", "Current number of pattern cache keys held in in-memory snapshot state.", "{entry}", p.gaugeDP("loki_vl_proxy_patterns_cache_keys", float64(p.metrics.patternsCacheKeys.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_in_memory_bytes", "Current bytes used by in-memory pattern snapshot payloads.", "By", p.gaugeDP("loki_vl_proxy_patterns_in_memory_bytes", float64(p.metrics.patternsInMemoryBytes.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_last_response_patterns", "Pattern entries returned in the last /patterns response.", "{pattern}", p.gaugeDP("loki_vl_proxy_patterns_last_response_patterns", float64(p.metrics.patternsLastResponsePatterns.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_last_response_bytes", "Encoded bytes returned in the last /patterns response.", "By", p.gaugeDP("loki_vl_proxy_patterns_last_response_bytes", float64(p.metrics.patternsLastResponseBytes.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_persisted_disk_entries", "Current number of pattern snapshot cache keys present in the last persisted disk snapshot.", "{entry}", p.gaugeDP("loki_vl_proxy_patterns_persisted_disk_entries", float64(p.metrics.patternsPersistedDiskEntries.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_persisted_disk_patterns", "Current number of pattern entries present in the last persisted disk snapshot.", "{pattern}", p.gaugeDP("loki_vl_proxy_patterns_persisted_disk_patterns", float64(p.metrics.patternsPersistedDiskPatterns.Load()), now)),
		p.gaugeMetric("loki_vl_proxy_patterns_persisted_disk_bytes", "Last persisted pattern snapshot size on disk in bytes.", "By", p.gaugeDP("loki_vl_proxy_patterns_persisted_disk_bytes", float64(p.metrics.patternsPersistedDiskBytes.Load()), now)),
		p.sumMetric("loki_vl_proxy_patterns_persist_writes_total", "Pattern snapshot writes completed to disk.", "", p.counterDP("loki_vl_proxy_patterns_persist_writes_total", p.metrics.patternsPersistWritesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_persist_write_bytes_total", "Pattern snapshot bytes written to disk.", "By", p.counterDP("loki_vl_proxy_patterns_persist_write_bytes_total", p.metrics.patternsPersistWriteBytesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_restored_disk_bytes_total", "Pattern snapshot bytes restored from disk.", "By", p.counterDP("loki_vl_proxy_patterns_restored_disk_bytes_total", p.metrics.patternsRestoredDiskBytesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_restored_peer_bytes_total", "Pattern snapshot bytes restored from peers.", "By", p.counterDP("loki_vl_proxy_patterns_restored_peer_bytes_total", p.metrics.patternsRestoredPeerBytesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_source_lines_requested_total", "Pattern source lines requested from backend pattern fetches.", "", p.counterDP("loki_vl_proxy_patterns_source_lines_requested_total", p.metrics.patternsSourceLinesRequestedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_source_lines_scanned_total", "Pattern source lines scanned from backend responses.", "", p.counterDP("loki_vl_proxy_patterns_source_lines_scanned_total", p.metrics.patternsSourceLinesScannedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_source_lines_observed_total", "Pattern source lines accepted into the miner.", "", p.counterDP("loki_vl_proxy_patterns_source_lines_observed_total", p.metrics.patternsSourceLinesObservedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_windows_attempted_total", "Pattern fetch windows attempted.", "", p.counterDP("loki_vl_proxy_patterns_windows_attempted_total", p.metrics.patternsWindowsAttemptedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_windows_accepted_total", "Pattern fetch windows accepted into the merged response.", "", p.counterDP("loki_vl_proxy_patterns_windows_accepted_total", p.metrics.patternsWindowsAcceptedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_windows_capped_total", "Pattern fetch windows that hit the per-window source line cap.", "", p.counterDP("loki_vl_proxy_patterns_windows_capped_total", p.metrics.patternsWindowsCappedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_second_pass_windows_total", "Pattern fetch windows retried with a higher line limit.", "", p.counterDP("loki_vl_proxy_patterns_second_pass_windows_total", p.metrics.patternsSecondPassWindowsTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_mined_pre_merge_total", "Pattern entries mined before cross-window merge.", "", p.counterDP("loki_vl_proxy_patterns_mined_pre_merge_total", p.metrics.patternsMinedPreMergeTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_mined_post_merge_total", "Pattern entries after cross-window merge.", "", p.counterDP("loki_vl_proxy_patterns_mined_post_merge_total", p.metrics.patternsMinedPostMergeTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_snapshot_hits_total", "Pattern snapshot fallback lookups that found a cached payload.", "", p.counterDP("loki_vl_proxy_patterns_snapshot_hits_total", p.metrics.patternsSnapshotHitsTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_snapshot_misses_total", "Pattern snapshot fallback lookups that missed.", "", p.counterDP("loki_vl_proxy_patterns_snapshot_misses_total", p.metrics.patternsSnapshotMissesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_snapshot_reused_total", "Pattern responses served from snapshot fallback payloads.", "", p.counterDP("loki_vl_proxy_patterns_snapshot_reused_total", p.metrics.patternsSnapshotReusedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_patterns_low_coverage_responses_total", "Pattern responses flagged as low coverage and likely degraded.", "", p.counterDP("loki_vl_proxy_patterns_low_coverage_responses_total", p.metrics.patternsLowCoverageResponsesTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_coalesced_total", "Requests served from coalesced results.", "", p.counterDP("loki_vl_proxy_coalesced_total", p.metrics.coalescedTotal.Load(), now)),
		p.sumMetric("loki_vl_proxy_coalesced_saved_total", "Backend requests saved by coalescing.", "", p.counterDP("loki_vl_proxy_coalesced_saved_total", p.metrics.coalescedSaved.Load(), now)),
		p.gaugeMetric("loki_vl_proxy_uptime_seconds", "Proxy uptime.", "s", p.gaugeDP("loki_vl_proxy_uptime_seconds", time.Since(p.metrics.startTime).Seconds(), now)),
		p.gaugeMetric("loki_vl_proxy_active_requests", "Current in-flight requests.", "{request}", p.gaugeDP("loki_vl_proxy_active_requests", float64(p.metrics.activeRequests.Load()), now)),
	)
	metrics = append(metrics, p.cacheTierMetrics(now)...)

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
	metrics = append(metrics, p.connectionMetrics(now)...)
	metrics = append(metrics, p.requestMetrics(now)...)
	metrics = append(metrics, p.upstreamFanoutMetrics(now)...)
	metrics = append(metrics, p.tenantMetrics(now)...)
	metrics = append(metrics, p.clientErrorMetrics(now)...)
	metrics = append(metrics, p.endpointCacheMetrics(now)...)
	metrics = append(metrics, p.backendDurationMetrics(now)...)
	metrics = append(metrics, p.internalOperationMetrics(now)...)
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

func (p *OTLPPusher) cacheTierMetrics(now int64) []map[string]interface{} {
	stats, ok := p.metrics.cacheStatsSnapshot()
	if !ok {
		return nil
	}
	return []map[string]interface{}{
		p.sumMetric(
			"loki_vl_proxy_cache_tier_requests_total",
			"Cache tier lookup attempts by tier.",
			"",
			p.counterDP("loki_vl_proxy_cache_tier_requests_total", stats.L1.Requests, now, attr("tier", "l1_memory")),
			p.counterDP("loki_vl_proxy_cache_tier_requests_total", stats.L2.Requests, now, attr("tier", "l2_disk")),
			p.counterDP("loki_vl_proxy_cache_tier_requests_total", stats.L3.Requests, now, attr("tier", "l3_peer")),
		),
		p.sumMetric(
			"loki_vl_proxy_cache_tier_hits_total",
			"Cache hits by tier.",
			"",
			p.counterDP("loki_vl_proxy_cache_tier_hits_total", stats.L1.Hits, now, attr("tier", "l1_memory")),
			p.counterDP("loki_vl_proxy_cache_tier_hits_total", stats.L2.Hits, now, attr("tier", "l2_disk")),
			p.counterDP("loki_vl_proxy_cache_tier_hits_total", stats.L3.Hits, now, attr("tier", "l3_peer")),
		),
		p.sumMetric(
			"loki_vl_proxy_cache_tier_misses_total",
			"Cache misses by tier.",
			"",
			p.counterDP("loki_vl_proxy_cache_tier_misses_total", stats.L1.Misses, now, attr("tier", "l1_memory")),
			p.counterDP("loki_vl_proxy_cache_tier_misses_total", stats.L2.Misses, now, attr("tier", "l2_disk")),
			p.counterDP("loki_vl_proxy_cache_tier_misses_total", stats.L3.Misses, now, attr("tier", "l3_peer")),
		),
		p.sumMetric(
			"loki_vl_proxy_cache_tier_stale_hits_total",
			"Stale responses served from local cache tiers.",
			"",
			p.counterDP("loki_vl_proxy_cache_tier_stale_hits_total", stats.L1.StaleHits, now, attr("tier", "l1_memory")),
			p.counterDP("loki_vl_proxy_cache_tier_stale_hits_total", stats.L2.StaleHits, now, attr("tier", "l2_disk")),
			p.counterDP("loki_vl_proxy_cache_tier_stale_hits_total", stats.L3.StaleHits, now, attr("tier", "l3_peer")),
		),
		p.sumMetric(
			"loki_vl_proxy_cache_backend_fallthrough_total",
			"Cache lookups that missed every tier and fell through to backend.",
			"",
			p.counterDP("loki_vl_proxy_cache_backend_fallthrough_total", stats.BackendFallthrough, now),
		),
		p.gaugeMetric(
			"loki_vl_proxy_cache_objects",
			"Current object count by cache tier.",
			"",
			p.gaugeDP("loki_vl_proxy_cache_objects", float64(stats.Entries), now, attr("tier", "l1_memory")),
			p.gaugeDP("loki_vl_proxy_cache_objects", float64(stats.DiskEntries), now, attr("tier", "l2_disk")),
		),
		p.gaugeMetric(
			"loki_vl_proxy_cache_bytes",
			"Current stored bytes by cache tier.",
			"By",
			p.gaugeDP("loki_vl_proxy_cache_bytes", float64(stats.Bytes), now, attr("tier", "l1_memory")),
			p.gaugeDP("loki_vl_proxy_cache_bytes", float64(stats.DiskBytes), now, attr("tier", "l2_disk")),
		),
	}
}

func (p *OTLPPusher) connectionMetrics(now int64) []map[string]interface{} {
	gaugeKeys := make([]string, 0, len(p.metrics.connectionStates))
	for state := range p.metrics.connectionStates {
		gaugeKeys = append(gaugeKeys, state)
	}
	sort.Strings(gaugeKeys)
	gaugePoints := make([]map[string]interface{}, 0, len(gaugeKeys))
	for _, state := range gaugeKeys {
		gaugePoints = append(gaugePoints,
			p.gaugeDP("loki_vl_proxy_http_connections", float64(p.metrics.connectionStates[state].Load()), now, attr("state", state)),
		)
	}

	transitionKeys := make([]string, 0, len(p.metrics.connectionTransitions))
	for state := range p.metrics.connectionTransitions {
		transitionKeys = append(transitionKeys, state)
	}
	sort.Strings(transitionKeys)
	transitionPoints := make([]map[string]interface{}, 0, len(transitionKeys))
	for _, state := range transitionKeys {
		transitionPoints = append(transitionPoints,
			p.counterDP("loki_vl_proxy_http_connection_transitions_total", p.metrics.connectionTransitions[state].Load(), now, attr("state", state)),
		)
	}

	return []map[string]interface{}{
		p.gaugeMetric("loki_vl_proxy_http_connections", "Current HTTP server connections by state.", "{connection}", gaugePoints...),
		p.sumMetric("loki_vl_proxy_http_connection_transitions_total", "HTTP server connection state transitions.", "", transitionPoints...),
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

func redactEndpointURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.UserPassword("***", "***")
	u.RawQuery = ""
	return u.String()
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
		parts := splitMetricKey(key, 5)
		if parts == nil {
			continue
		}
		requestPoints = append(requestPoints, p.counterDP("loki_vl_proxy_requests_total", p.metrics.requestsTotal[key].Load(), now, attr("loki.api.system", parts[0]), attr("proxy.direction", parts[1]), attr("loki.request.type", parts[2]), attr("http.route", parts[3]), attr("http.response.status_code", parts[4])))
	}
	if len(requestPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_requests_total", "Total number of proxied requests.", "", requestPoints...))
	}
	durationKeys := sortedKeys(mapsFromHists(p.metrics.requestDurations))
	durationPoints := make([]map[string]interface{}, 0, len(durationKeys))
	for _, key := range durationKeys {
		parts := splitMetricKey(key, 4)
		if parts == nil {
			continue
		}
		durationPoints = append(durationPoints, p.histogramDP(p.metrics.requestDurations[key], now, attr("loki.api.system", parts[0]), attr("proxy.direction", parts[1]), attr("loki.request.type", parts[2]), attr("http.route", parts[3])))
	}
	if len(durationPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_request_duration_seconds", "Request duration histogram.", "s", durationPoints...))
	}
	return metrics
}

func (p *OTLPPusher) upstreamFanoutMetrics(now int64) []map[string]interface{} {
	keys := sortedKeys(mapsFromHists(p.metrics.upstreamCallsPerRequest))
	points := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		points = append(points, p.histogramDP(
			p.metrics.upstreamCallsPerRequest[key],
			now,
			attr("loki.api.system", lokiSystemLabel),
			attr("proxy.direction", downstreamDirection),
			attr("loki.request.type", parts[0]),
			attr("http.route", parts[1]),
		))
	}
	if len(points) == 0 {
		return nil
	}
	return []map[string]interface{}{
		p.histogramMetric("loki_vl_proxy_upstream_calls_per_request", "Upstream call count per downstream request.", "{request}", points...),
	}
}

func (p *OTLPPusher) tenantMetrics(now int64) []map[string]interface{} {
	if !p.metrics.ExportSensitiveLabels() {
		return nil
	}
	metrics := make([]map[string]interface{}, 0, 2)
	reqKeys := sortedKeys(mapsFromCounters(p.metrics.tenantRequests))
	reqPoints := make([]map[string]interface{}, 0, len(reqKeys))
	for _, key := range reqKeys {
		parts := splitMetricKey(key, 4)
		if parts == nil {
			continue
		}
		reqPoints = append(reqPoints, p.counterDP("loki_vl_proxy_tenant_requests_total", p.metrics.tenantRequests[key].Load(), now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("loki.tenant.id", parts[0]), attr("loki.request.type", parts[1]), attr("http.route", parts[2]), attr("http.response.status_code", parts[3])))
	}
	if len(reqPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_tenant_requests_total", "Requests by tenant.", "", reqPoints...))
	}
	durKeys := sortedKeys(mapsFromHists(p.metrics.tenantDurations))
	durPoints := make([]map[string]interface{}, 0, len(durKeys))
	for _, key := range durKeys {
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		durPoints = append(durPoints, p.histogramDP(p.metrics.tenantDurations[key], now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("loki.tenant.id", parts[0]), attr("loki.request.type", parts[1]), attr("http.route", parts[2])))
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
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		points = append(points, p.counterDP("loki_vl_proxy_client_errors_total", p.metrics.clientErrors[key].Load(), now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("loki.request.type", parts[0]), attr("http.route", parts[1]), attr("error.type", parts[2])))
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
	for _, key := range hitKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		hitPoints = append(hitPoints, p.counterDP("loki_vl_proxy_cache_hits_by_endpoint", p.metrics.endpointCacheHits[key].Load(), now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("loki.request.type", parts[0]), attr("http.route", parts[1])))
	}
	if len(hitPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_cache_hits_by_endpoint", "Cache hits per endpoint.", "", hitPoints...))
	}
	missKeys := sortedKeys(mapsFromCounters(p.metrics.endpointCacheMisses))
	missPoints := make([]map[string]interface{}, 0, len(missKeys))
	for _, key := range missKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		missPoints = append(missPoints, p.counterDP("loki_vl_proxy_cache_misses_by_endpoint", p.metrics.endpointCacheMisses[key].Load(), now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("loki.request.type", parts[0]), attr("http.route", parts[1])))
	}
	if len(missPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_cache_misses_by_endpoint", "Cache misses per endpoint.", "", missPoints...))
	}
	return metrics
}

func (p *OTLPPusher) backendDurationMetrics(now int64) []map[string]interface{} {
	keys := sortedKeys(mapsFromHists(p.metrics.backendDurations))
	points := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		points = append(points, p.histogramDP(p.metrics.backendDurations[key], now, attr("loki.api.system", vlSystemLabel), attr("proxy.direction", upstreamDirection), attr("loki.request.type", parts[0]), attr("http.route", parts[1])))
	}
	if len(points) == 0 {
		return nil
	}
	return []map[string]interface{}{p.histogramMetric("loki_vl_proxy_backend_duration_seconds", "VL backend response time.", "s", points...)}
}

func (p *OTLPPusher) internalOperationMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 2)
	counterKeys := sortedKeys(mapsFromCounters(p.metrics.internalOperationTotal))
	counterPoints := make([]map[string]interface{}, 0, len(counterKeys))
	for _, key := range counterKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		counterPoints = append(counterPoints, p.counterDP(
			"loki_vl_proxy_internal_operation_total",
			p.metrics.internalOperationTotal[key].Load(),
			now,
			attr("operation", parts[0]),
			attr("outcome", parts[1]),
		))
	}
	if len(counterPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_internal_operation_total", "Proxy-side internal operations by operation and outcome.", "", counterPoints...))
	}
	durationKeys := sortedKeys(mapsFromHists(p.metrics.internalOperationDurations))
	durationPoints := make([]map[string]interface{}, 0, len(durationKeys))
	for _, key := range durationKeys {
		parts := splitMetricKey(key, 2)
		if parts == nil {
			continue
		}
		durationPoints = append(durationPoints, p.histogramDP(
			p.metrics.internalOperationDurations[key],
			now,
			attr("operation", parts[0]),
			attr("outcome", parts[1]),
		))
	}
	if len(durationPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_internal_operation_duration_seconds", "Proxy-side internal operation duration.", "s", durationPoints...))
	}
	return metrics
}

func (p *OTLPPusher) queryRangeWindowMetrics(now int64) []map[string]interface{} {
	metrics := make([]map[string]interface{}, 0, 15)
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
	if p.metrics.windowPrefilterDuration != nil {
		metrics = append(metrics, p.histogramMetric(
			"loki_vl_proxy_window_prefilter_duration_seconds",
			"Query-range window prefilter duration.",
			"s",
			p.histogramDP(p.metrics.windowPrefilterDuration, now),
		))
	}
	metrics = append(metrics,
		p.sumMetric(
			"loki_vl_proxy_window_prefilter_attempt_total",
			"Query-range window prefilter attempts.",
			"",
			p.counterDP("loki_vl_proxy_window_prefilter_attempt_total", p.metrics.windowPrefilterAttempts.Load(), now),
		),
		p.sumMetric(
			"loki_vl_proxy_window_prefilter_error_total",
			"Query-range window prefilter errors.",
			"",
			p.counterDP("loki_vl_proxy_window_prefilter_error_total", p.metrics.windowPrefilterErrors.Load(), now),
		),
		p.sumMetric(
			"loki_vl_proxy_window_prefilter_kept_total",
			"Query-range windows kept after prefilter.",
			"{window}",
			p.counterDP("loki_vl_proxy_window_prefilter_kept_total", p.metrics.windowPrefilterKept.Load(), now),
		),
		p.sumMetric(
			"loki_vl_proxy_window_prefilter_skipped_total",
			"Query-range windows skipped after prefilter.",
			"{window}",
			p.counterDP("loki_vl_proxy_window_prefilter_skipped_total", p.metrics.windowPrefilterSkipped.Load(), now),
		),
		p.gaugeMetric(
			"loki_vl_proxy_window_prefilter_hit_ratio",
			"Prefilter hit ratio (kept / total windows).",
			"",
			p.gaugeDP("loki_vl_proxy_window_prefilter_hit_ratio", float64(p.metrics.windowPrefilterHitRatioPpm.Load())/1000000.0, now),
		),
		p.sumMetric(
			"loki_vl_proxy_window_retry_total",
			"Query-range window retry attempts.",
			"",
			p.counterDP("loki_vl_proxy_window_retry_total", p.metrics.windowRetries.Load(), now),
		),
		p.sumMetric(
			"loki_vl_proxy_window_degraded_batch_total",
			"Query-range batches degraded to lower parallelism.",
			"",
			p.counterDP("loki_vl_proxy_window_degraded_batch_total", p.metrics.windowDegradedBatches.Load(), now),
		),
		p.sumMetric(
			"loki_vl_proxy_window_partial_response_total",
			"Query-range partial responses due to retryable backend failures.",
			"",
			p.counterDP("loki_vl_proxy_window_partial_response_total", p.metrics.windowPartialResponses.Load(), now),
		),
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
	if !p.metrics.ExportSensitiveLabels() {
		return nil
	}
	metrics := make([]map[string]interface{}, 0, 6)
	reqKeys := sortedKeys(mapsFromCounters(p.metrics.clientRequests))
	reqPoints := make([]map[string]interface{}, 0, len(reqKeys))
	for _, key := range reqKeys {
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		reqPoints = append(reqPoints, p.counterDP("loki_vl_proxy_client_requests_total", p.metrics.clientRequests[key].Load(), now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("enduser.id", parts[0]), attr("loki.request.type", parts[1]), attr("http.route", parts[2])))
	}
	if len(reqPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_client_requests_total", "Requests by client identity.", "", reqPoints...))
	}
	byteKeys := sortedKeys(mapsFromCounters(p.metrics.clientBytes))
	bytePoints := make([]map[string]interface{}, 0, len(byteKeys))
	for _, client := range byteKeys {
		bytePoints = append(bytePoints, p.counterDP("loki_vl_proxy_client_response_bytes_total", p.metrics.clientBytes[client].Load(), now, attr("enduser.id", client)))
	}
	if len(bytePoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_client_response_bytes_total", "Response bytes by client.", "By", bytePoints...))
	}
	statusKeys := sortedKeys(mapsFromCounters(p.metrics.clientStatuses))
	statusPoints := make([]map[string]interface{}, 0, len(statusKeys))
	for _, key := range statusKeys {
		parts := splitMetricKey(key, 4)
		if parts == nil {
			continue
		}
		statusPoints = append(statusPoints, p.counterDP("loki_vl_proxy_client_status_total", p.metrics.clientStatuses[key].Load(), now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("enduser.id", parts[0]), attr("loki.request.type", parts[1]), attr("http.route", parts[2]), attr("http.response.status_code", parts[3])))
	}
	if len(statusPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_client_status_total", "Requests by client identity and final HTTP status.", "", statusPoints...))
	}
	inflightKeys := sortedKeys(mapsFromCounters(p.metrics.clientInflight))
	inflightPoints := make([]map[string]interface{}, 0, len(inflightKeys))
	for _, client := range inflightKeys {
		inflightPoints = append(inflightPoints, p.gaugeDP("loki_vl_proxy_client_inflight_requests", float64(p.metrics.clientInflight[client].Load()), now, attr("enduser.id", client)))
	}
	if len(inflightPoints) > 0 {
		metrics = append(metrics, p.gaugeMetric("loki_vl_proxy_client_inflight_requests", "In-flight requests by client identity.", "{request}", inflightPoints...))
	}
	rotationKeys := sortedKeys(mapsFromCounters(p.metrics.connectionRotations))
	rotationPoints := make([]map[string]interface{}, 0, len(rotationKeys))
	for _, reason := range rotationKeys {
		rotationPoints = append(rotationPoints, p.counterDP("loki_vl_proxy_http_connection_rotations_total", p.metrics.connectionRotations[reason].Load(), now, attr("reason", reason)))
	}
	if len(rotationPoints) > 0 {
		metrics = append(metrics, p.sumMetric("loki_vl_proxy_http_connection_rotations_total", "Downstream HTTP/1.x connection rotations triggered by the proxy.", "", rotationPoints...))
	}
	durationKeys := sortedKeys(mapsFromHists(p.metrics.clientDurations))
	durationPoints := make([]map[string]interface{}, 0, len(durationKeys))
	for _, key := range durationKeys {
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		durationPoints = append(durationPoints, p.histogramDP(p.metrics.clientDurations[key], now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("enduser.id", parts[0]), attr("loki.request.type", parts[1]), attr("http.route", parts[2])))
	}
	if len(durationPoints) > 0 {
		metrics = append(metrics, p.histogramMetric("loki_vl_proxy_client_request_duration_seconds", "Per-client request duration.", "s", durationPoints...))
	}
	queryLenKeys := sortedKeys(mapsFromHists(p.metrics.clientQueryLengths))
	queryLenPoints := make([]map[string]interface{}, 0, len(queryLenKeys))
	for _, key := range queryLenKeys {
		parts := splitMetricKey(key, 3)
		if parts == nil {
			continue
		}
		queryLenPoints = append(queryLenPoints, p.histogramDP(p.metrics.clientQueryLengths[key], now, attr("loki.api.system", lokiSystemLabel), attr("proxy.direction", downstreamDirection), attr("enduser.id", parts[0]), attr("loki.request.type", parts[1]), attr("http.route", parts[2])))
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
	readBytes, writeBytes, readOps, writeOps := readDiskIO()
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
		p.sumMetric("loki_vl_proxy_process_disk_read_operations_total", "Disk read operations.", "{operation}", p.counterDP("loki_vl_proxy_process_disk_read_operations_total", readOps, now)),
		p.sumMetric("loki_vl_proxy_process_disk_write_operations_total", "Disk write operations.", "{operation}", p.counterDP("loki_vl_proxy_process_disk_write_operations_total", writeOps, now)),
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
