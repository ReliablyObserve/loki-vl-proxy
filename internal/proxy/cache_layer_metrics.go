package proxy

import (
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// distinctCacheStores calls fn once per physical cache instance. Alternate
// response profiles share these stores; their existence is not another resident
// allocation. diskAliased says an earlier store owns the same L2 instance, whose
// counters that earlier store already reports.
func (p *Proxy) distinctCacheStores(fn func(component string, store *cache.Cache, diskAliased bool)) {
	stores := [...]struct {
		component string
		store     *cache.Cache
	}{
		{"primary", p.cache},
		{"compat", p.compatCache},
		{"translation", p.translationCache},
		{"field_names", p.streamFieldNamesCache},
	}
	for i, item := range stores {
		if item.store == nil {
			continue
		}
		duplicate := false
		for _, earlier := range stores[:i] {
			if earlier.store == item.store {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		diskAliased := false
		if disk := item.store.DiskCache(); disk != nil {
			for _, earlier := range stores[:i] {
				if earlier.store != nil && earlier.store.DiskCache() == disk {
					diskAliased = true
					break
				}
			}
		}
		fn(item.component, item.store, diskAliased)
	}
}

// cacheLayerStats counts physical instances once.
func (p *Proxy) cacheLayerStats() []metrics.CacheComponentStats {
	out := make([]metrics.CacheComponentStats, 0, 4)
	p.distinctCacheStores(func(component string, store *cache.Cache, diskAliased bool) {
		layers := store.LayerStats()
		if diskAliased {
			for layer := range layers {
				layers[layer].L2 = cache.LayerTierStatsSnapshot{}
				layers[layer].DiskIndexBytes = 0
				layers[layer].DiskQueueBytes = 0
				layers[layer].DiskEncodedReadBytes = 0
				layers[layer].DiskEncodedWriteBytes = 0
			}
		}
		out = append(out, metrics.CacheComponentStats{Component: component, Layers: layers})
	})
	return out
}

// cacheSignalStats is cacheLayerStats for the reason, eviction and promotion
// signals; it reads atomics only and takes no cache lock.
func (p *Proxy) cacheSignalStats() []metrics.CacheComponentSignals {
	out := make([]metrics.CacheComponentSignals, 0, 4)
	p.distinctCacheStores(func(component string, store *cache.Cache, diskAliased bool) {
		layers := store.SignalStats()
		if diskAliased {
			for layer := range layers {
				layers[layer].L2 = cache.TierSignalsSnapshot{}
				layers[layer].DiskReadSeconds = cache.HistogramSnapshot{}
				layers[layer].DiskReadBytes = cache.HistogramSnapshot{}
			}
		}
		out = append(out, metrics.CacheComponentSignals{Component: component, Layers: layers})
	})
	return out
}

// cacheSizingStats returns the sizing signals of the caches that have them on
// (the primary cache with -cache-sizing-signals); atomics only, no cache lock.
func (p *Proxy) cacheSizingStats() []cache.SizingSnapshot {
	var out []cache.SizingSnapshot
	p.distinctCacheStores(func(_ string, store *cache.Cache, _ bool) {
		if s := store.SizingStats(); s.Enabled {
			out = append(out, s)
		}
	})
	return out
}

// compatAnswerTTL is the Loki answer cache's TTL for an endpoint. query_range
// and query use a fixed 5 minutes so the entry outlives the bucket used to
// generate the key (max(5min, step)) while keeping staleness imperceptible: on
// a 2-day chart a 5-min gap is 0.17% of width. Using step (up to 1h) as TTL
// caused visible empty spaces at the chart right edge.
func compatAnswerTTL(endpoint string) time.Duration {
	if endpoint == "query_range" || endpoint == "query" {
		return 5 * time.Minute
	}
	return CacheTTLs[endpoint]
}

// cacheTTLs is the base TTL of each answer class as the proxy stores it: the
// Loki answer cache by endpoint, then the inner caches with their own TTLs.
// Every value is set in New and not changed afterwards.
func (p *Proxy) cacheTTLs() []metrics.CacheTTL {
	out := make([]metrics.CacheTTL, 0, len(metrics.CacheTTLClasses))
	for _, endpoint := range [...]string{"query_range", "query", "labels", "label_values", "series", "detected_fields", "detected_field_values", "detected_labels", "patterns", "index_stats", "volume", "volume_range"} {
		out = append(out, metrics.CacheTTL{Class: endpoint, TTL: compatAnswerTTL(endpoint)})
	}
	return append(out,
		metrics.CacheTTL{Class: "labels_handler", TTL: p.cacheTTLLabels},
		metrics.CacheTTL{Class: "label_values_handler", TTL: p.cacheTTLLabelValues},
		metrics.CacheTTL{Class: "label_inventory", TTL: CacheTTLs["label_inventory"]},
		metrics.CacheTTL{Class: "metadata_negative", TTL: p.metadataNegativeTTL()},
		metrics.CacheTTL{Class: "query_range_window_recent", TTL: p.queryRangeRecentCacheTTL},
		metrics.CacheTTL{Class: "query_range_window_history", TTL: p.queryRangeHistoryCacheTTL},
		metrics.CacheTTL{Class: "query_range_extent", TTL: p.queryRangeExtentCacheTTL},
		metrics.CacheTTL{Class: "parse_risk_probe", TTL: p.parseRiskProbeCacheTTL},
	)
}
