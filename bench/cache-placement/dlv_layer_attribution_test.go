package cacheplacement

import (
	"math"
	"strconv"
	"strings"
)

// Only actually exported bounded semantic metrics appear here. Counter values
// are interval deltas; gauges retain both endpoints, never a synthetic delta.
// Nil values mean missing, reset, non-finite or invalid interval, not zero work.
type dlvLayerMetrics struct {
	CounterDeltas map[string]*float64
	BeforeGauges  map[string]*float64
	AfterGauges   map[string]*float64
}

type dlvLayerMetricDescription struct {
	Layer, Component, Tier, Family string
	Counter                        bool
}

func dlvParseLayerMetric(key string) (description dlvLayerMetricDescription, ok bool) {
	name, raw, hasLabels := strings.Cut(key, "{")
	if !hasLabels || !strings.HasPrefix(name, "loki_vl_proxy_cache_layer_") || !strings.HasSuffix(raw, "}") {
		return description, false
	}
	description.Family = strings.TrimPrefix(name, "loki_vl_proxy_cache_layer_")
	labels := map[string]string{}
	for _, label := range strings.Split(strings.TrimSuffix(raw, "}"), ",") {
		key, quoted, found := strings.Cut(label, "=")
		if !found {
			return description, false
		}
		value, err := strconv.Unquote(quoted)
		if err != nil {
			return description, false
		}
		if _, duplicate := labels[key]; duplicate {
			return description, false
		}
		labels[key] = value
	}
	description.Layer = labels["layer"]
	if description.Layer != "loki" && description.Layer != "vl" && description.Layer != "unknown" {
		return description, false
	}
	switch description.Family {
	case "coalesced_total", "coalesced_saved_total", "coalesced_wait_total", "coalesced_wait_seconds_total":
		description.Counter = true
		return description, len(labels) == 1
	}
	description.Component = labels["component"]
	switch description.Component {
	case "primary", "compat", "translation", "field_names":
	default:
		return description, false
	}
	switch description.Family {
	case "disk_index_bytes", "disk_queue_bytes":
		return description, len(labels) == 2
	case "disk_encoded_read_bytes_total", "disk_encoded_write_bytes_total":
		description.Counter = true
		return description, len(labels) == 2
	}
	description.Tier = labels["tier"]
	if len(labels) != 3 || description.Tier != "l1_memory" && description.Tier != "l2_disk" && description.Tier != "l3_peer" {
		return description, false
	}
	switch description.Family {
	case "objects", "bytes":
		return description, description.Tier != "l3_peer"
	case "lookup_seconds_total":
		description.Counter = true
		return description, description.Tier != "l1_memory"
	case "requests_total", "hits_total", "misses_total", "read_bytes_total":
		description.Counter = true
		return description, true
	case "write_bytes_total", "admissions_total", "rejections_total", "evictions_total", "stale_hits_total":
		description.Counter = true
		return description, description.Tier != "l3_peer"
	default:
		return description, false
	}
}

func dlvLayerValue(metrics map[string]float64, key string) *float64 {
	value, ok := metrics[key]
	if !ok || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

func dlvLayerSourcesPresent(r resources, key string) bool {
	if len(r.PerNode) == 0 {
		return true
	}
	if r.ExpectedNodes > 0 && len(r.PerNode) != r.ExpectedNodes {
		return false
	}
	for _, metrics := range r.PerNode {
		if dlvLayerValue(metrics, key) == nil {
			return false
		}
	}
	return true
}

func dlvLayerCounterDelta(before, after resources, key string) *float64 {
	if dlvLayerValue(before.Metrics, key) == nil || dlvLayerValue(after.Metrics, key) == nil || !dlvLayerSourcesPresent(before, key) || !dlvLayerSourcesPresent(after, key) {
		return nil
	}
	if len(before.PerNode) != len(after.PerNode) {
		return nil
	}
	for node, metrics := range before.PerNode {
		// An aggregate can hide one process resetting while another increments.
		if dlvDelta(metrics, after.PerNode[node], key) == nil {
			return nil
		}
	}
	return dlvDelta(before.Metrics, after.Metrics, key)
}

func dlvAttributeLayerMetrics(o *dlvObservation, c dlvCell) {
	keys := map[string]dlvLayerMetricDescription{}
	for _, metrics := range []map[string]float64{o.Before.Metrics, o.After.Metrics} {
		for key := range metrics {
			if description, ok := dlvParseLayerMetric(key); ok {
				keys[key] = description
			}
		}
	}
	byLayer := map[string]*dlvLayerMetrics{}
	for key, description := range keys {
		stats := byLayer[description.Layer]
		if stats == nil {
			stats = &dlvLayerMetrics{CounterDeltas: map[string]*float64{}, BeforeGauges: map[string]*float64{}, AfterGauges: map[string]*float64{}}
			byLayer[description.Layer] = stats
		}
		valid := o.Valid && o.Before.Valid && o.After.Valid
		if description.Counter {
			stats.CounterDeltas[key] = nil
			if valid {
				stats.CounterDeltas[key] = dlvLayerCounterDelta(o.Before, o.After, key)
			}
		} else {
			stats.BeforeGauges[key], stats.AfterGauges[key] = nil, nil
			if valid {
				if dlvLayerSourcesPresent(o.Before, key) {
					stats.BeforeGauges[key] = dlvLayerValue(o.Before.Metrics, key)
				}
				if dlvLayerSourcesPresent(o.After, key) {
					stats.AfterGauges[key] = dlvLayerValue(o.After.Metrics, key)
				}
			}
		}
		// Compatibility Tier0 is its own component, not the physical l0 sidecar.
		// A disabled Tier0 never becomes a measured miss even if a stale scrape
		// happens to contain compatibility counters from another configuration.
		if c.Tier0 && description.Layer == "loki" && description.Component == "compat" && description.Tier == "l1_memory" {
			o.ProxyLoki.Tier0.UnsupportedReason = "Only dedicated compatibility metrics are measured; missing or reset samples remain null."
			switch description.Family {
			case "hits_total":
				o.ProxyLoki.Tier0.Hits = stats.CounterDeltas[key]
			case "misses_total":
				o.ProxyLoki.Tier0.Misses = stats.CounterDeltas[key]
			case "requests_total":
				o.ProxyLoki.Tier0.Requests = stats.CounterDeltas[key]
			case "objects":
				o.ProxyLoki.Tier0.Objects = stats.AfterGauges[key]
			case "bytes":
				o.ProxyLoki.Tier0.Bytes = stats.AfterGauges[key]
			}
		}
	}
	if stats := byLayer["loki"]; stats != nil {
		o.ProxyLoki.OtherCacheLayerMetrics = stats
	}
	if stats := byLayer["vl"]; stats != nil {
		o.ProxyVL.CacheLayerMetrics = stats
		o.ProxyVL.UnsupportedReason = "Only explicitly exported semantic-layer metrics are measured; unavailable counter intervals remain null."
	}
	if stats := byLayer["unknown"]; stats != nil {
		o.SharedStorage.UnknownCacheLayerMetrics = stats
	}
	if len(byLayer) > 0 {
		o.SharedStorage.Attribution = "Legacy aggregate physical tiers remain separate; semantic cache metrics use only explicit exported layer labels."
	}
	if c.Tier0 && o.ProxyLoki.Tier0.Hits != nil && o.ProxyLoki.Tier0.Misses != nil && o.ProxyLoki.Tier0.Requests != nil && o.ProxyLoki.Tier0.Objects != nil && o.ProxyLoki.Tier0.Bytes != nil {
		o.ProxyLoki.Tier0.UnsupportedReason = ""
	}
}
