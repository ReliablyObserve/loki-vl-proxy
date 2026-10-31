package metrics

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// CacheComponentStats identifies one distinct physical cache instance. Component
// names are restricted to primary, compat, translation and field_names. Callers
// must not register the same cache pointer under several names.
type CacheComponentStats struct {
	Component string
	Layers    [cache.LayerCount]cache.LayerStatsSnapshot
}

// SetCacheLayerStatsProvider attaches semantic-layer snapshots. An absent
// provider emits no layer series; legacy aggregate metrics remain independent.
func (m *Metrics) SetCacheLayerStatsProvider(fn func() []CacheComponentStats) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.cacheLayerStatsProvider = fn
	m.mu.Unlock()
}

// SetCacheLayerCoalescerStatsProvider attaches instrumented backend coalescer
// counters by semantic layer, independently of cache occupancy and storage tier.
func (m *Metrics) SetCacheLayerCoalescerStatsProvider(fn func() [cache.LayerCount]CoalescerStats) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.cacheLayerCoalescerStatsProvider = fn
	m.mu.Unlock()
}

type cacheLayerMetricSample struct {
	layer, component, tier string
	value                  int64
}

type cacheLayerMetric struct {
	name, help, unit string
	gauge, seconds   bool
	samples          []cacheLayerMetricSample
}

type cacheLayerTierMetric struct {
	suffix, help, unit string
	gauge, seconds     bool
	value              func(cache.LayerTierStatsSnapshot) int64
}

// Definitions are shared by both exporters so labels, units and measured values
// cannot drift between the Prometheus and OTLP representations.
var cacheLayerTierMetrics = []cacheLayerTierMetric{
	{"requests_total", "Cache lookup attempts by semantic layer, component and tier. Disabled tiers do not count misses.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Requests }},
	{"hits_total", "Cache lookups delivering values by semantic layer, component and tier.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Hits }},
	{"misses_total", "Cache lookup attempts without a value by semantic layer, component and tier.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Misses }},
	{"stale_hits_total", "Stale cache values delivered by semantic layer, component and tier.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.StaleHits }},
	{"read_bytes_total", "Decoded value bytes delivered by cache lookups. L2 includes pending RAM queue reads; these are not physical SSD bytes.", "By", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.ReadBytes }},
	{"write_bytes_total", "Decoded value bytes accepted by cache admission. L2 counts queue admission, not committed device writes.", "By", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.WriteBytes }},
	{"admissions_total", "Cache value admissions accepted by semantic layer, component and tier.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Admissions }},
	{"rejections_total", "Cache value admissions rejected by semantic layer, component and tier.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Rejections }},
	{"evictions_total", "Cache entries removed by expiry or capacity eviction, excluding rejected admissions.", "", false, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Evictions }},
	{"objects", "Current cache entries: L1 resident entries; L2 committed records, excluding the pending queue. Peer occupancy is not measured here.", "{entry}", true, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Entries }},
	{"bytes", "Current accounted cache bytes: L1 keys and decoded values; L2 keys and encoded committed records. Excludes process RSS and physical file allocation.", "By", true, false, func(s cache.LayerTierStatsSnapshot) int64 { return s.Bytes }},
	{"lookup_seconds_total", "Cumulative local L2 or peer L3 lookup time, including misses. L2 can read queued RAM values; L1 time is not measured.", "s", false, true, func(s cache.LayerTierStatsSnapshot) int64 { return s.LookupNanoseconds }},
}

var cacheLayerDiskMetrics = []struct {
	suffix, help string
	gauge        bool
	value        func(cache.LayerStatsSnapshot) int64
}{
	{"disk_index_bytes", "Accounted RAM bytes for the disk index by layer and component; not the physical SSD index size.", true, func(s cache.LayerStatsSnapshot) int64 { return s.DiskIndexBytes }},
	{"disk_queue_bytes", "Accounted RAM bytes for pending and flushing disk writes by layer and component.", true, func(s cache.LayerStatsSnapshot) int64 { return s.DiskQueueBytes }},
	{"disk_encoded_read_bytes_total", "Encoded cache record bytes copied during application-level disk reads; excludes queued RAM hits and is not device throughput or IOPS.", false, func(s cache.LayerStatsSnapshot) int64 { return s.DiskEncodedReadBytes }},
	{"disk_encoded_write_bytes_total", "Encoded cache record bytes committed at the application layer; excludes filesystem overhead and is not device throughput or IOPS.", false, func(s cache.LayerStatsSnapshot) int64 { return s.DiskEncodedWriteBytes }},
}

func (m *Metrics) cacheLayerMetricSnapshots() []cacheLayerMetric {
	m.mu.RLock()
	statsFn := m.cacheLayerStatsProvider
	coalescerFn := m.cacheLayerCoalescerStatsProvider
	m.mu.RUnlock()
	var out []cacheLayerMetric
	if statsFn != nil {
		// Fixed slots reject unbounded component labels and duplicate series,
		// including accidental duplicate registration of a component name.
		var components [4]*CacheComponentStats
		for _, stats := range statsFn() {
			index := -1
			switch stats.Component {
			case "primary":
				index = 0
			case "compat":
				index = 1
			case "translation":
				index = 2
			case "field_names":
				index = 3
			}
			if index >= 0 && components[index] == nil {
				components[index] = &stats
			}
		}
		for _, definition := range cacheLayerTierMetrics {
			metric := cacheLayerMetric{name: "loki_vl_proxy_cache_layer_" + definition.suffix, help: definition.help, unit: definition.unit, gauge: definition.gauge, seconds: definition.seconds}
			for _, component := range components {
				if component == nil {
					continue
				}
				for layer := cache.LayerUnknown; layer < cache.LayerCount; layer++ {
					stats := component.Layers[layer]
					tiers := [...]cache.LayerTierStatsSnapshot{stats.L1, stats.L2, stats.L3}
					for i, tier := range [...]string{"l1_memory", "l2_disk", "l3_peer"} {
						if definition.gauge && i == 2 || definition.seconds && i == 0 {
							continue
						}
						// The peer lookup boundary measures only attempts, outcomes,
						// delivered bytes and elapsed time. Other L3 snapshot zeros
						// mean uninstrumented, not measured zero writes/evictions.
						if i == 2 {
							switch definition.suffix {
							case "write_bytes_total", "admissions_total", "rejections_total", "evictions_total", "stale_hits_total":
								continue
							}
						}
						metric.samples = append(metric.samples, cacheLayerMetricSample{layer.String(), component.Component, tier, definition.value(tiers[i])})
					}
				}
			}
			if len(metric.samples) > 0 {
				out = append(out, metric)
			}
		}
		for _, definition := range cacheLayerDiskMetrics {
			metric := cacheLayerMetric{name: "loki_vl_proxy_cache_layer_" + definition.suffix, help: definition.help, unit: "By", gauge: definition.gauge}
			for _, component := range components {
				if component == nil {
					continue
				}
				for layer := cache.LayerUnknown; layer < cache.LayerCount; layer++ {
					metric.samples = append(metric.samples, cacheLayerMetricSample{layer: layer.String(), component: component.Component, value: definition.value(component.Layers[layer])})
				}
			}
			if len(metric.samples) > 0 {
				out = append(out, metric)
			}
		}
	}
	if coalescerFn != nil {
		stats := coalescerFn()
		for _, definition := range []struct {
			suffix, help string
			seconds      bool
			value        func(CoalescerStats) int64
		}{
			{"coalesced_total", "Successful instrumented backend requests whose coalescer result was shared, by semantic layer; excludes shared failures counted by saved and wait counters.", false, func(s CoalescerStats) int64 { return s.Total }},
			{"coalesced_saved_total", "Backend calls avoided by instrumented coalescer followers, by semantic layer.", false, func(s CoalescerStats) int64 { return s.Saved }},
			{"coalesced_wait_total", "Instrumented backend coalescer followers that waited, by semantic layer.", false, func(s CoalescerStats) int64 { return s.WaitCount }},
			{"coalesced_wait_seconds_total", "Cumulative instrumented backend coalescer follower waiting time, by semantic layer.", true, func(s CoalescerStats) int64 { return s.WaitNanos }},
		} {
			metric := cacheLayerMetric{name: "loki_vl_proxy_cache_layer_" + definition.suffix, help: definition.help, seconds: definition.seconds}
			if definition.seconds {
				metric.unit = "s"
			}
			for layer := cache.LayerUnknown; layer < cache.LayerCount; layer++ {
				metric.samples = append(metric.samples, cacheLayerMetricSample{layer: layer.String(), value: definition.value(stats[layer])})
			}
			out = append(out, metric)
		}
	}
	return out
}

func (m *Metrics) writeCacheLayerMetrics(sb *strings.Builder) {
	for _, metric := range m.cacheLayerMetricSnapshots() {
		kind := "counter"
		if metric.gauge {
			kind = "gauge"
		}
		fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n", metric.name, metric.help, metric.name, kind)
		for _, sample := range metric.samples {
			fmt.Fprintf(sb, "%s{layer=%q", metric.name, sample.layer)
			if sample.component != "" {
				fmt.Fprintf(sb, ",component=%q", sample.component)
			}
			if sample.tier != "" {
				fmt.Fprintf(sb, ",tier=%q", sample.tier)
			}
			if metric.seconds {
				fmt.Fprintf(sb, "} %s\n", strconv.FormatFloat(float64(sample.value)/1e9, 'g', -1, 64))
			} else {
				fmt.Fprintf(sb, "} %d\n", sample.value)
			}
		}
	}
}

func (p *OTLPPusher) cacheLayerMetrics(now int64) []map[string]interface{} {
	var out []map[string]interface{}
	for _, metric := range p.metrics.cacheLayerMetricSnapshots() {
		points := make([]map[string]interface{}, 0, len(metric.samples))
		for _, sample := range metric.samples {
			attributes := []map[string]interface{}{attr("layer", sample.layer)}
			if sample.component != "" {
				attributes = append(attributes, attr("component", sample.component))
			}
			if sample.tier != "" {
				attributes = append(attributes, attr("tier", sample.tier))
			}
			point := p.counterDP(metric.name, sample.value, now, attributes...)
			if metric.seconds {
				delete(point, "asInt")
				point["asDouble"] = float64(sample.value) / 1e9
			}
			points = append(points, point)
		}
		if metric.gauge {
			out = append(out, p.gaugeMetric(metric.name, metric.help, metric.unit, points...))
		} else {
			out = append(out, p.sumMetric(metric.name, metric.help, metric.unit, points...))
		}
	}
	return out
}
