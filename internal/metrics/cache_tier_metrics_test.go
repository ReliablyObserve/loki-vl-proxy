package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestMetrics_Handler_EmitsCacheTierMetricsFromProvider(t *testing.T) {
	m := NewMetrics()
	m.SetCacheStatsProvider(func() cache.StatsSnapshot {
		return cache.StatsSnapshot{
			L1:                 cache.TierStatsSnapshot{Requests: 11, Hits: 7, Misses: 4, StaleHits: 1},
			L2:                 cache.TierStatsSnapshot{Requests: 5, Hits: 3, Misses: 2, StaleHits: 1},
			L3:                 cache.TierStatsSnapshot{Requests: 2, Hits: 1, Misses: 1},
			BackendFallthrough: 6,
			Entries:            13,
			Bytes:              17,
			DiskEntries:        19,
			DiskBytes:          23,
		}
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)
	body := w.Body.String()

	for _, needle := range []string{
		`loki_vl_proxy_cache_tier_requests_total{tier="l1_memory"} 11`,
		`loki_vl_proxy_cache_tier_hits_total{tier="l2_disk"} 3`,
		`loki_vl_proxy_cache_tier_misses_total{tier="l3_peer"} 1`,
		`loki_vl_proxy_cache_tier_stale_hits_total{tier="l1_memory"} 1`,
		`loki_vl_proxy_cache_backend_fallthrough_total 6`,
		`loki_vl_proxy_cache_objects{tier="l2_disk"} 19`,
		`loki_vl_proxy_cache_bytes{tier="l1_memory"} 17`,
	} {
		if !strings.Contains(body, needle) {
			t.Fatalf("expected cache tier metric %q in output", needle)
		}
	}
}

func TestOTLPPusher_BuildPayload_IncludesCacheTierMetrics(t *testing.T) {
	m := NewMetrics()
	m.SetCacheStatsProvider(func() cache.StatsSnapshot {
		return cache.StatsSnapshot{
			L1:                 cache.TierStatsSnapshot{Requests: 9, Hits: 5, Misses: 4, StaleHits: 1},
			L2:                 cache.TierStatsSnapshot{Requests: 3, Hits: 2, Misses: 1, StaleHits: 1},
			L3:                 cache.TierStatsSnapshot{Requests: 1, Hits: 1},
			BackendFallthrough: 2,
			Entries:            7,
			Bytes:              29,
			DiskEntries:        5,
			DiskBytes:          31,
		}
	})

	payload := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload()
	resourceMetrics, ok := payload["resourceMetrics"].([]map[string]interface{})
	if !ok || len(resourceMetrics) == 0 {
		t.Fatalf("expected resourceMetrics in payload, got %#v", payload["resourceMetrics"])
	}
	scopeMetrics, ok := resourceMetrics[0]["scopeMetrics"].([]map[string]interface{})
	if !ok || len(scopeMetrics) == 0 {
		t.Fatalf("expected scopeMetrics in payload, got %#v", resourceMetrics[0]["scopeMetrics"])
	}
	data, err := json.Marshal(scopeMetrics[0]["metrics"])
	if err != nil {
		t.Fatalf("marshal metrics slice: %v", err)
	}
	var metricsList []interface{}
	if err := json.Unmarshal(data, &metricsList); err != nil {
		t.Fatalf("decode metrics slice: %v", err)
	}
	names := metricNames(metricsList)
	for _, required := range []string{
		"loki_vl_proxy_cache_tier_requests_total",
		"loki_vl_proxy_cache_tier_hits_total",
		"loki_vl_proxy_cache_tier_misses_total",
		"loki_vl_proxy_cache_tier_stale_hits_total",
		"loki_vl_proxy_cache_backend_fallthrough_total",
		"loki_vl_proxy_cache_objects",
		"loki_vl_proxy_cache_bytes",
	} {
		if !names[required] {
			t.Fatalf("expected OTLP payload to include %s; names=%v", required, names)
		}
	}
}
