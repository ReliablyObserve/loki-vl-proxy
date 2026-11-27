package metrics

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func sinceStartStats(tracked bool) cache.StatsSnapshot {
	h := cache.HandoverStats{StartupTracked: tracked}
	h.SinceStart[0][0] = cache.TierLookups{Requests: 4, Hits: 1}  // 1 min, memory
	h.SinceStart[1][0] = cache.TierLookups{Requests: 8, Hits: 4}  // 5 min, memory
	h.SinceStart[2][0] = cache.TierLookups{Requests: 10, Hits: 0} // 15 min, memory: a measured zero
	h.SinceStart[1][1] = cache.TierLookups{Requests: 4, Hits: 3}  // disk, not read in the first minute
	h.SinceStart[2][1] = cache.TierLookups{Requests: 5, Hits: 5}
	return cache.StatsSnapshot{Handover: h}
}

// A ratio per tier and window; a tier without lookups in a window has no series, and nothing is exported when
// the window is not tracked.
//
// conformance: cache-restart-handover
func TestCacheHitRatioSinceStartExposition(t *testing.T) {
	want := map[[2]string]float64{
		{"l1_memory", "1"}: 0.25, {"l1_memory", "5"}: 0.5, {"l1_memory", "15"}: 0,
		{"l2_disk", "5"}: 0.75, {"l2_disk", "15"}: 1,
	}
	for _, tracked := range []bool{true, false} {
		m := NewMetrics()
		m.SetCacheStatsProvider(func() cache.StatsSnapshot { return sinceStartStats(tracked) })
		body := metricsText(m)
		var series []string
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "loki_vl_proxy_cache_hit_ratio_since_start{") {
				series = append(series, line)
			}
		}
		data, err := json.Marshal(NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload())
		if err != nil {
			t.Fatal(err)
		}
		inOTLP := strings.Contains(string(data), `"name":"loki_vl_proxy_cache_hit_ratio_since_start"`)
		if !tracked {
			if len(series) != 0 || inOTLP {
				t.Fatalf("untracked: %v, otlp %v", series, inOTLP)
			}
			continue
		}
		if got := strings.Join(series, "\n"); got != strings.Join([]string{
			`loki_vl_proxy_cache_hit_ratio_since_start{tier="l1_memory",minutes="1"} 0.25`,
			`loki_vl_proxy_cache_hit_ratio_since_start{tier="l1_memory",minutes="5"} 0.5`,
			`loki_vl_proxy_cache_hit_ratio_since_start{tier="l1_memory",minutes="15"} 0`,
			`loki_vl_proxy_cache_hit_ratio_since_start{tier="l2_disk",minutes="5"} 0.75`,
			`loki_vl_proxy_cache_hit_ratio_since_start{tier="l2_disk",minutes="15"} 1`,
		}, "\n") {
			t.Fatalf("series:\n%s", got)
		}
		points := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).sinceStartRatioMetrics(sinceStartStats(true).Handover, 1)
		var decoded []struct {
			Gauge struct {
				DataPoints []struct {
					AsDouble   float64 `json:"asDouble"`
					Attributes []struct {
						Key   string `json:"key"`
						Value struct {
							StringValue string `json:"stringValue"`
						} `json:"value"`
					} `json:"attributes"`
				} `json:"dataPoints"`
			} `json:"gauge"`
		}
		raw, err := json.Marshal(points)
		if err != nil || json.Unmarshal(raw, &decoded) != nil || len(decoded) != 1 || !inOTLP {
			t.Fatalf("otlp %s %v", raw, err)
		}
		got := map[[2]string]float64{}
		for _, dp := range decoded[0].Gauge.DataPoints {
			var key [2]string
			for _, a := range dp.Attributes {
				switch a.Key {
				case "tier":
					key[0] = a.Value.StringValue
				case "minutes":
					key[1] = a.Value.StringValue
				}
			}
			got[key] = dp.AsDouble
		}
		if len(got) != len(want) {
			t.Fatalf("otlp points %v", got)
		}
		for k, v := range want {
			if g, ok := got[k]; !ok || g != v {
				t.Fatalf("otlp %v = %v (present %v), want %v: %s", k, g, ok, v, raw)
			}
		}
	}
}

// conformance: observability-query-flow
func TestCacheReadsPerRequestExposition(t *testing.T) {
	m := NewMetrics()
	m.RecordRequestFlow("query_range", "/loki/api/v1/query_range", RequestFlow{CacheLookups: 4, CacheReads: [3]int{4, 3, 1}})
	m.RecordRequestFlow("query_range", "/loki/api/v1/query_range", RequestFlow{CacheLookups: 60, CacheReads: [3]int{60, 0, 0}})
	body := metricsText(m)
	for _, line := range []string{
		"# TYPE loki_vl_proxy_cache_reads_per_request histogram",
		`loki_vl_proxy_cache_reads_per_request_bucket{route="/loki/api/v1/query_range",tier="l1_memory",le="5"} 1`,
		`loki_vl_proxy_cache_reads_per_request_bucket{route="/loki/api/v1/query_range",tier="l1_memory",le="+Inf"} 2`,
		`loki_vl_proxy_cache_reads_per_request_sum{route="/loki/api/v1/query_range",tier="l1_memory"} 64`,
		`loki_vl_proxy_cache_reads_per_request_sum{route="/loki/api/v1/query_range",tier="l2_disk"} 3`,
		`loki_vl_proxy_cache_reads_per_request_bucket{route="/loki/api/v1/query_range",tier="l3_peer",le="0"} 1`,
		`loki_vl_proxy_cache_reads_per_request_count{route="/loki/api/v1/query_range",tier="l3_peer"} 2`,
	} {
		if !strings.Contains(body, line+"\n") {
			t.Fatalf("metrics lack %s\n%s", line, body)
		}
	}
	data, err := json.Marshal(NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).cacheReadsPerRequestMetrics(1))
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{`"name":"loki_vl_proxy_cache_reads_per_request"`, `"stringValue":"l2_disk"`, `"key":"http.route"`, `"stringValue":"/loki/api/v1/query_range"`, `"sum":64`, `"count":"2"`} {
		if !strings.Contains(string(data), part) {
			t.Fatalf("otlp lacks %s: %s", part, data)
		}
	}
	if got := strings.Count(string(data), `"explicitBounds"`); got != 3 {
		t.Fatalf("otlp data points = %d, want one per tier", got)
	}
	full, err := json.Marshal(NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload())
	if err != nil || !strings.Contains(string(full), `"name":"loki_vl_proxy_cache_reads_per_request"`) {
		t.Fatalf("payload lacks the histogram: %v", err)
	}
}
