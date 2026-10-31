package proxy

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// cacheLayerStats counts physical instances once. Alternate response profiles
// share these stores; their existence is not another resident allocation.
func (p *Proxy) cacheLayerStats() []metrics.CacheComponentStats {
	stores := [...]struct {
		component string
		store     *cache.Cache
	}{
		{"primary", p.cache},
		{"compat", p.compatCache},
		{"translation", p.translationCache},
		{"field_names", p.streamFieldNamesCache},
	}
	out := make([]metrics.CacheComponentStats, 0, len(stores))
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
		layers := item.store.LayerStats()
		if disk := item.store.DiskCache(); disk != nil {
			for _, earlier := range stores[:i] {
				if earlier.store != nil && earlier.store.DiskCache() == disk {
					for layer := range layers {
						layers[layer].L2 = cache.LayerTierStatsSnapshot{}
						layers[layer].DiskIndexBytes = 0
						layers[layer].DiskQueueBytes = 0
						layers[layer].DiskEncodedReadBytes = 0
						layers[layer].DiskEncodedWriteBytes = 0
					}
					break
				}
			}
		}
		out = append(out, metrics.CacheComponentStats{Component: item.component, Layers: layers})
	}
	return out
}
