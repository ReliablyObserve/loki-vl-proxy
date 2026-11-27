package metrics

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"

// SetCacheSizingStatsProvider attaches the sizing signals of the caches that
// have them on (-cache-sizing-signals). The provider must read atomics only
// (cache.Cache.SizingStats): it runs on every scrape and OTLP push.
func (m *Metrics) SetCacheSizingStatsProvider(fn func() []cache.SizingSnapshot) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.cacheSizingStatsProvider = fn
	m.mu.Unlock()
}

// cacheSizingMetrics sums the enabled snapshots into the sizing families; with
// none enabled it exports nothing. Labels come from fixed sets only: tier,
// scenario, window, layer and the answer class.
func cacheSizingMetrics(snapshots []cache.SizingSnapshot) []signalMetric {
	var sum cache.SizingSnapshot
	reuseBounds := durationBoundsSeconds(cache.ReuseIntervalBounds[:])
	var reuse [cache.LayerCount]*signalHistogram
	for l := range reuse {
		reuse[l] = newSignalHistogram(reuseBounds)
	}
	enabled := false
	for _, s := range snapshots {
		if !s.Enabled {
			continue
		}
		enabled = true
		for t := range s.GhostHits {
			for g := range s.GhostHits[t] {
				sum.GhostHits[t][g] += s.GhostHits[t][g]
			}
		}
		for w := range s.WorkingSetKeys {
			sum.WorkingSetKeys[w] += s.WorkingSetKeys[w]
			sum.WorkingSetBytes[w] += s.WorkingSetBytes[w]
		}
		for l := range s.ReuseInterval {
			reuse[l].add(s.ReuseInterval[l], 1e-9)
		}
		for k := range s.HitBytes {
			for t := range s.HitBytes[k] {
				sum.HitBytes[k][t] += s.HitBytes[k][t]
			}
			sum.RequestedBytes[k] += s.RequestedBytes[k]
		}
		for t := range s.Dropped {
			sum.Dropped[t] += s.Dropped[t]
		}
	}
	if !enabled {
		return nil
	}
	ghost := signalMetric{name: "loki_vl_proxy_cache_ghost_hits_total", kind: "counter",
		help: "Estimated lookups a tier missed that the scenario would have answered: 2x_bytes, twice the tier's entry and byte budgets; 2x_ttl, the same budgets with every TTL doubled (a sizing signal, never served: a longer TTL means staler answers). From a 1-in-16 hash sample of the keys, scaled; -cache-sizing-signals."}
	for t, tier := range cache.SizingTiers {
		for g := cache.GhostScenario(0); g < cache.GhostScenarioCount; g++ {
			ghost.points = append(ghost.points, signalPoint{labels: []signalLabel{{"tier", tier}, {"scenario", g.String()}}, value: sum.GhostHits[t][g]})
		}
	}
	keys := signalMetric{name: "loki_vl_proxy_cache_working_set_keys", kind: "gauge",
		help: "Estimated distinct memory-cache keys looked up or stored in the window (5m, 1h, 24h), from a 1-in-16 hash sample, scaled; a lower bound past 16384 sampled keys; -cache-sizing-signals."}
	bytes := signalMetric{name: "loki_vl_proxy_cache_working_set_bytes", kind: "gauge", unit: "By",
		help: "Estimated answer and key bytes of those keys: compare with -cache-max-bytes; -cache-sizing-signals."}
	for w, name := range cache.WorkingSetWindowNames {
		keys.points = append(keys.points, signalPoint{labels: []signalLabel{{"window", name}}, gauge: float64(sum.WorkingSetKeys[w])})
		bytes.points = append(bytes.points, signalPoint{labels: []signalLabel{{"window", name}}, gauge: float64(sum.WorkingSetBytes[w])})
	}
	reuseMetric := signalMetric{name: "loki_vl_proxy_cache_reuse_interval_seconds", kind: "histogram", unit: "s",
		help: "Time between two request-level lookups of one key (hit or miss), by semantic layer, from a 1-in-16 hash sample (not scaled): compare with loki_vl_proxy_cache_ttl_seconds; -cache-sizing-signals."}
	for l := cache.LayerUnknown; l < cache.LayerCount; l++ {
		reuseMetric.points = append(reuseMetric.points, signalPoint{labels: []signalLabel{{"layer", l.String()}}, hist: reuse[l]})
	}
	hit := signalMetric{name: "loki_vl_proxy_cache_hit_bytes_total", kind: "counter", unit: "By",
		help: "Answer bytes request-level cache lookups got from each tier, by answer class; with loki_vl_proxy_cache_requested_bytes_total, the byte hit ratio; -cache-sizing-signals."}
	requested := signalMetric{name: "loki_vl_proxy_cache_requested_bytes_total", kind: "counter", unit: "By",
		help: "Answer bytes request-level lookups asked for, by answer class: the hit bytes plus the bytes stored in memory after a full miss (a miss not followed by a store, or beyond 4096 pending misses, counts no bytes); -cache-sizing-signals."}
	for k := cache.EndpointClass(0); k < cache.EndpointClassCount; k++ {
		class := signalLabel{"class", k.String()}
		for t, tier := range cache.HitTiers {
			hit.points = append(hit.points, signalPoint{labels: []signalLabel{class, {"tier", tier}}, value: sum.HitBytes[k][t]})
		}
		requested.points = append(requested.points, signalPoint{labels: []signalLabel{class}, value: sum.RequestedBytes[k]})
	}
	dropped := signalMetric{name: "loki_vl_proxy_cache_sizing_dropped_events_total", kind: "counter",
		help: "Sampled sizing events of a tier dropped because the buffer was full (the ghost, working-set and reuse figures are then a lower bound); -cache-sizing-signals."}
	for t, tier := range cache.SizingTiers {
		dropped.points = append(dropped.points, signalPoint{labels: []signalLabel{{"tier", tier}}, value: sum.Dropped[t]})
	}
	return []signalMetric{ghost, keys, bytes, reuseMetric, hit, requested, dropped}
}
