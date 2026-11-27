package metrics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func sizingFixture(scale int64) cache.SizingSnapshot {
	s := cache.SizingSnapshot{Enabled: true}
	for t := range s.GhostHits {
		for g := range s.GhostHits[t] {
			s.GhostHits[t][g] = scale * int64(10*t+g+1)
		}
		s.Dropped[t] = scale * int64(t+1)
	}
	for w := range s.WorkingSetKeys {
		s.WorkingSetKeys[w], s.WorkingSetBytes[w] = scale*int64(w+1), scale*int64(1000*(w+1))
	}
	for l := range s.ReuseInterval {
		s.ReuseInterval[l] = signalHistogramFixture(len(cache.ReuseIntervalBounds), scale*int64(l+1))
	}
	for k := range s.HitBytes {
		for t := range s.HitBytes[k] {
			s.HitBytes[k][t] = scale * int64(100*k+t+1)
		}
		s.RequestedBytes[k] = scale * int64(1000*(k+1))
	}
	return s
}

// The sizing families: a fixed series set from fixed label sets, summed over
// the enabled caches (a disabled snapshot adds nothing), Prometheus = OTLP.
//
// conformance: cache-sizing-signals
func TestCacheSizingMetricsExporterParityAndCardinality(t *testing.T) {
	m := NewMetrics()
	m.SetCacheSizingStatsProvider(func() []cache.SizingSnapshot {
		return []cache.SizingSnapshot{sizingFixture(1), {}, sizingFixture(100)}
	})
	prom, promTypes := scrapeSignalMetrics(t, m)
	otlp, otlpTypes := pushSignalMetrics(t, m)
	if fmt.Sprint(promTypes) != fmt.Sprint(otlpTypes) || len(prom) != len(otlp) {
		t.Fatalf("prom %v (%d) otlp %v (%d)", promTypes, len(prom), otlpTypes, len(otlp))
	}
	for key, want := range prom {
		if got, ok := otlp[key]; !ok || got != want {
			t.Fatalf("%s: prom %g otlp %g (%v)", key, want, got, ok)
		}
	}
	perFamily := map[string]int{}
	for key := range prom {
		name, _, _ := strings.Cut(key, "{")
		perFamily[strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")]++
	}
	// 2 tiers x 2 scenarios; 3 windows twice; 3 layers x (11 bounds + Inf + sum
	// + count); 8 classes x 3 tiers; 8 classes; 2 tiers.
	want := map[string]int{
		"loki_vl_proxy_cache_ghost_hits_total":            4,
		"loki_vl_proxy_cache_working_set_keys":            3,
		"loki_vl_proxy_cache_working_set_bytes":           3,
		"loki_vl_proxy_cache_reuse_interval_seconds":      42,
		"loki_vl_proxy_cache_hit_bytes_total":             24,
		"loki_vl_proxy_cache_requested_bytes_total":       8,
		"loki_vl_proxy_cache_sizing_dropped_events_total": 2,
	}
	if fmt.Sprint(perFamily) != fmt.Sprint(want) {
		t.Fatalf("series per family %v, want %v", perFamily, want)
	}
	for key, value := range map[string]float64{
		`loki_vl_proxy_cache_ghost_hits_total{scenario="2x_ttl",tier="l2_disk"}`: 101 * 12,
		`loki_vl_proxy_cache_working_set_bytes{window="24h"}`:                    101 * 3000,
		`loki_vl_proxy_cache_hit_bytes_total{class="labels",tier="l3_peer"}`:     101 * 303,
		`loki_vl_proxy_cache_requested_bytes_total{class="other"}`:               101 * 8000,
		`loki_vl_proxy_cache_sizing_dropped_events_total{tier="l1_memory"}`:      101,
	} {
		if got := prom[key]; got != value {
			t.Errorf("%s=%g want %g", key, got, value)
		}
	}
	off := NewMetrics()
	off.SetCacheSizingStatsProvider(func() []cache.SizingSnapshot { return []cache.SizingSnapshot{{}} })
	if prom, _ := scrapeSignalMetrics(t, off); len(prom) != 0 {
		t.Fatalf("series with every cache's signals off: %d", len(prom))
	}
}
