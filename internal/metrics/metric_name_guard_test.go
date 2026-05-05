package metrics

import (
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

var legacyUnprefixedMetricAllowlist = map[string]struct{}{
	"go_gc_cycles_total":                   {},
	"go_gc_duration_seconds_count":         {},
	"go_goroutines":                        {},
	"go_memstats_alloc_bytes":              {},
	"go_memstats_heap_idle_bytes":          {},
	"go_memstats_heap_inuse_bytes":         {},
	"go_memstats_sys_bytes":                {},
	"process_cpu_seconds_total":            {},
	"process_cpu_usage_ratio":              {},
	"process_disk_read_bytes_total":        {},
	"process_disk_written_bytes_total":     {},
	"process_memory_available_bytes":       {},
	"process_memory_free_bytes":            {},
	"process_memory_total_bytes":           {},
	"process_memory_usage_ratio":           {},
	"process_network_receive_bytes_total":  {},
	"process_network_transmit_bytes_total": {},
	"process_open_fds":                     {},
	"process_pressure_cpu_full_ratio":      {},
	"process_pressure_cpu_some_ratio":      {},
	"process_pressure_io_full_ratio":       {},
	"process_pressure_io_some_ratio":       {},
	"process_pressure_memory_full_ratio":   {},
	"process_pressure_memory_some_ratio":   {},
	"process_resident_memory_bytes":        {},
}

func TestMetrics_Handler_RejectsUnexpectedUnprefixedMetricFamilies(t *testing.T) {
	m := seededMetricsForMetricNameGuard()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	assertOnlyPrefixedOrLegacyMetricNames(t, prometheusMetricNames(w.Body.String()))
}

func TestOTLPPusher_RejectsUnexpectedUnprefixedMetricFamilies(t *testing.T) {
	lockProcEnvTest(t)

	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, seededMetricsForMetricNameGuard())
	payload := pusher.buildPayload()

	assertOnlyPrefixedOrLegacyMetricNames(t, otlpMetricNamesFromPayload(t, payload))
}

func seededMetricsForMetricNameGuard() *Metrics {
	m := NewMetrics()
	m.RecordRequestWithRoute("query_range", "/loki/api/v1/query_range", 200, 35*time.Millisecond)
	m.RecordUpstreamRequest("vl", "query_range", "/select/logsql/query", 200, 20*time.Millisecond)
	m.RecordTenantRequestWithRoute("tenant-a", "query_range", "/loki/api/v1/query_range", 200, 35*time.Millisecond)
	m.RecordClientIdentityWithRoute("grafana-user", "query_range", "/loki/api/v1/query_range", 18*time.Millisecond, 2048)
	m.RecordClientStatusWithRoute("grafana-user", "query_range", "/loki/api/v1/query_range", 429)
	m.RecordClientErrorWithRoute("query_range", "/loki/api/v1/query_range", "timeout")
	m.RecordClientInflight("grafana-user", 1)
	m.RecordClientQueryLengthWithRoute("grafana-user", "query_range", "/loki/api/v1/query_range", len(`{app="api"}`))
	m.RecordCacheHit()
	m.RecordCacheMiss()
	m.RecordEndpointCacheHitWithRoute("query_range", "/loki/api/v1/query_range")
	m.RecordEndpointCacheMissWithRoute("query_range", "/loki/api/v1/query_range")
	m.RecordBackendDurationWithRoute("query_range", "/loki/api/v1/query_range", 20*time.Millisecond)
	m.RecordUpstreamCallsPerRequestWithRoute("query_range", "/loki/api/v1/query_range", 3)
	m.RecordInternalOperation("translate_query", "translated", 5*time.Millisecond)
	m.RecordTranslation()
	m.RecordTranslationError()
	m.RecordCoalesced()
	m.RecordCoalescedSaved()
	m.RecordQueryRangeWindowCacheHit()
	m.RecordQueryRangeWindowCacheMiss()
	m.RecordQueryRangeWindowFetchDuration(25 * time.Millisecond)
	m.RecordQueryRangeWindowMergeDuration(12 * time.Millisecond)
	m.RecordQueryRangeWindowPrefilterAttempt()
	m.RecordQueryRangeWindowPrefilterDuration(3 * time.Millisecond)
	m.RecordQueryRangeWindowPrefilterOutcome(4, 2)
	m.RecordQueryRangeWindowRetry()
	m.RecordQueryRangeWindowDegradedBatch()
	m.RecordQueryRangeWindowPartialResponse()
	m.RecordQueryRangeAdaptiveState(3, 40*time.Millisecond, 0.02)
	return m
}

func assertOnlyPrefixedOrLegacyMetricNames(t *testing.T, names []string) {
	t.Helper()

	unexpected := make([]string, 0)
	for _, name := range names {
		if strings.HasPrefix(name, "loki_vl_proxy_") {
			continue
		}
		if _, ok := legacyUnprefixedMetricAllowlist[name]; ok {
			continue
		}
		unexpected = append(unexpected, name)
	}

	if len(unexpected) == 0 {
		return
	}
	sort.Strings(unexpected)
	t.Fatalf("unexpected unprefixed metric families exported: %v", unexpected)
}

func prometheusMetricNames(body string) []string {
	seen := make(map[string]struct{})
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := line
		if idx := strings.IndexAny(name, "{ "); idx >= 0 {
			name = name[:idx]
		}
		if name == "" {
			continue
		}
		seen[name] = struct{}{}
	}
	return sortedMetricNames(seen)
}

func otlpMetricNamesFromPayload(t *testing.T, payload map[string]interface{}) []string {
	t.Helper()

	seen := make(map[string]struct{})

	resourceMetrics, ok := payload["resourceMetrics"].([]map[string]interface{})
	if !ok || len(resourceMetrics) == 0 {
		t.Fatalf("expected resourceMetrics in OTLP payload, got %#v", payload["resourceMetrics"])
	}
	scopeMetrics, ok := resourceMetrics[0]["scopeMetrics"].([]map[string]interface{})
	if !ok || len(scopeMetrics) == 0 {
		t.Fatalf("expected scopeMetrics in OTLP payload, got %#v", resourceMetrics[0]["scopeMetrics"])
	}
	metrics, ok := scopeMetrics[0]["metrics"].([]map[string]interface{})
	if !ok || len(metrics) == 0 {
		t.Fatalf("expected metrics in OTLP payload, got %#v", scopeMetrics[0]["metrics"])
	}
	for _, metric := range metrics {
		name, _ := metric["name"].(string)
		if name == "" {
			continue
		}
		seen[name] = struct{}{}
	}

	return sortedMetricNames(seen)
}

func sortedMetricNames(seen map[string]struct{}) []string {
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
