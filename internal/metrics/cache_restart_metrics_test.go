package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func restartStats(tracked bool) cache.StatsSnapshot {
	return cache.StatsSnapshot{Handover: cache.HandoverStats{
		PreviousFlushFound: true, PreviousFlushComplete: true,
		PreviousFlushRanked: 50, PreviousFlushDiskWrites: 2, PreviousFlushMemoryEntries: 5,
		PreviousFlushMemoryBytes: 1 << 20, PreviousFlushDuration: 3 * time.Millisecond,
		WarmDone: true, WarmDiskEntries: 48, WarmMemoryEntries: 5, WarmSkipped: 2,
		WarmBytes: 2 << 20, WarmDuration: 6 * time.Millisecond,
		StartupTracked: tracked, StartupL1Requests: 10, StartupL1Hits: 7,
	}}
}

// conformance: cache-restart-handover
func TestMetrics_Handler_EmitsCacheRestartMetrics(t *testing.T) {
	for _, tracked := range []bool{true, false} {
		m := NewMetrics()
		m.SetCacheStatsProvider(func() cache.StatsSnapshot { return restartStats(tracked) })
		w := httptest.NewRecorder()
		m.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
		body := w.Body.String()
		for _, needle := range []string{
			`loki_vl_proxy_cache_restart_previous_flush_records{kind="ranked"} 50`,
			`loki_vl_proxy_cache_restart_previous_flush_records{kind="disk_write"} 2`,
			`loki_vl_proxy_cache_restart_previous_flush_records{kind="memory"} 5`,
			`loki_vl_proxy_cache_restart_previous_flush_memory_bytes 1048576`,
			`loki_vl_proxy_cache_restart_previous_flush_duration_seconds 0.003`,
			`loki_vl_proxy_cache_restart_previous_flush_complete 1`,
			`loki_vl_proxy_cache_restart_warm_entries{kind="disk"} 48`,
			`loki_vl_proxy_cache_restart_warm_entries{kind="memory"} 5`,
			`loki_vl_proxy_cache_restart_warm_entries{kind="skipped"} 2`,
			`loki_vl_proxy_cache_restart_warm_bytes 2097152`,
			`loki_vl_proxy_cache_restart_warm_duration_seconds 0.006`,
			`loki_vl_proxy_cache_restart_warm_done 1`,
		} {
			if !strings.Contains(body, needle) {
				t.Fatalf("missing %q", needle)
			}
		}
		hasStartup := strings.Contains(body, "loki_vl_proxy_cache_startup_l1_requests 10") && strings.Contains(body, "loki_vl_proxy_cache_startup_l1_hits 7")
		if hasStartup != tracked {
			t.Fatalf("startup window metrics present=%v, tracked=%v", hasStartup, tracked)
		}
	}
}

// conformance: cache-restart-handover
func TestOTLPPusher_IncludesCacheRestartMetrics(t *testing.T) {
	for _, tracked := range []bool{true, false} {
		m := NewMetrics()
		m.SetCacheStatsProvider(func() cache.StatsSnapshot { return restartStats(tracked) })
		payload := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload()
		data, err := json.Marshal(payload["resourceMetrics"].([]map[string]interface{})[0]["scopeMetrics"].([]map[string]interface{})[0]["metrics"])
		if err != nil {
			t.Fatal(err)
		}
		var list []interface{}
		if err := json.Unmarshal(data, &list); err != nil {
			t.Fatal(err)
		}
		names := metricNames(list)
		for _, required := range []string{
			"loki_vl_proxy_cache_restart_previous_flush_records",
			"loki_vl_proxy_cache_restart_previous_flush_memory_bytes",
			"loki_vl_proxy_cache_restart_previous_flush_duration_seconds",
			"loki_vl_proxy_cache_restart_previous_flush_complete",
			"loki_vl_proxy_cache_restart_warm_entries",
			"loki_vl_proxy_cache_restart_warm_bytes",
			"loki_vl_proxy_cache_restart_warm_duration_seconds",
			"loki_vl_proxy_cache_restart_warm_done",
		} {
			if !names[required] {
				t.Fatalf("OTLP payload misses %s", required)
			}
		}
		if names["loki_vl_proxy_cache_startup_l1_requests"] != tracked || names["loki_vl_proxy_cache_startup_l1_hits"] != tracked {
			t.Fatalf("startup window metrics tracked=%v names=%v", tracked, names)
		}
	}
}
