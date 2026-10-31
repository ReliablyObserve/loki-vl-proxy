package cacheplacement

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDLVLayerMetricScrapeRecognition(t *testing.T) {
	accepted := []string{
		`loki_vl_proxy_cache_layer_hits_total{layer="loki",component="compat",tier="l1_memory"}`,
		`loki_vl_proxy_cache_layer_disk_queue_bytes{component="primary",layer="vl"}`,
		`loki_vl_proxy_cache_layer_lookup_seconds_total{layer="unknown",component="primary",tier="l3_peer"}`,
		`loki_vl_proxy_cache_layer_coalesced_wait_seconds_total{layer="vl"}`,
	}
	for _, key := range accepted {
		if !usefulMetric(key) {
			t.Errorf("layer sample discarded: %s", key)
		}
	}
	for _, key := range []string{
		`loki_vl_proxy_cache_layer_fake_total{layer="vl"}`,
		`loki_vl_proxy_cache_layer_hits_total{layer="secret",component="primary",tier="l1_memory"}`,
		`loki_vl_proxy_cache_layer_hits_total{layer="vl",component="tenant-a",tier="l1_memory"}`,
		`loki_vl_proxy_cache_layer_hits_total{layer="vl",component="primary",tier="l1_memory",tenant="secret"}`,
		`loki_vl_proxy_cache_layer_hits_total{layer="vl",layer="loki",component="primary",tier="l1_memory"}`,
		`loki_vl_proxy_cache_layer_write_bytes_total{layer="vl",component="primary",tier="l3_peer"}`,
		`loki_vl_proxy_cache_layer_lookup_seconds_total{layer="loki",component="primary",tier="l1_memory"}`,
	} {
		if usefulMetric(key) {
			t.Errorf("unbounded/unmeasured sample accepted: %s", key)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "loki_vl_proxy_process_cpu_seconds_total 1\nloki_vl_proxy_process_resident_memory_bytes 2")
		for _, key := range accepted {
			fmt.Fprintln(w, key+" 7")
		}
	}))
	defer server.Close()
	got, err := scrape(server.URL)
	must(t, err)
	for _, key := range accepted {
		if got[key] != 7 {
			t.Errorf("scrape missing %s", key)
		}
	}
}

func layerSample(suffix, layer, component, tier string) string {
	key := `loki_vl_proxy_cache_layer_` + suffix + `{layer="` + layer + `"`
	if component != "" {
		key += `,component="` + component + `"`
	}
	if tier != "" {
		key += `,tier="` + tier + `"`
	}
	return key + "}"
}

func TestDLVLayerAttributionUsesOnlyExportedDomains(t *testing.T) {
	loki := layerSample("hits_total", "loki", "primary", "l1_memory")
	vl := layerSample("read_bytes_total", "vl", "primary", "l2_disk")
	unknown := layerSample("hits_total", "unknown", "primary", "l1_memory")
	queue := layerSample("disk_queue_bytes", "vl", "primary", "")
	o := dlvObservation{Valid: true, Before: resources{Valid: true, Metrics: map[string]float64{loki: 2, vl: 10, unknown: 8, queue: 40}}, After: resources{Valid: true, Metrics: map[string]float64{loki: 7, vl: 50, unknown: 9, queue: 20}}}
	dlvAttributeObservation(&o, dlvCell{})
	lokiStats := o.ProxyLoki.OtherCacheLayerMetrics.(*dlvLayerMetrics)
	vlStats := o.ProxyVL.CacheLayerMetrics.(*dlvLayerMetrics)
	unknownStats := o.SharedStorage.UnknownCacheLayerMetrics.(*dlvLayerMetrics)
	if got := lokiStats.CounterDeltas[loki]; got == nil || *got != 5 {
		t.Fatal("Loki delta unavailable")
	}
	if got := vlStats.CounterDeltas[vl]; got == nil || *got != 40 {
		t.Fatal("VL delta unavailable")
	}
	if got := unknownStats.CounterDeltas[unknown]; got == nil || *got != 1 {
		t.Fatal("unknown layer lost")
	}
	if _, ok := vlStats.CounterDeltas[loki]; ok {
		t.Fatal("Loki counter reclassified as VL")
	}
	if *vlStats.BeforeGauges[queue] != 40 || *vlStats.AfterGauges[queue] != 20 {
		t.Fatal("gauge incorrectly treated as counter delta")
	}
	if len(o.SharedStorage.TierCounterDeltas) != 0 {
		t.Fatal("layer metrics mixed into legacy unattributed tier totals")
	}
}

func TestDLVLayerCounterAvailabilityAndResets(t *testing.T) {
	key := layerSample("hits_total", "vl", "primary", "l1_memory")
	for _, tc := range []struct {
		name          string
		before, after map[string]float64
		valid         bool
	}{
		{"missing before", nil, map[string]float64{key: 4}, true},
		{"missing after", map[string]float64{key: 4}, nil, true},
		{"counter reset", map[string]float64{key: 5}, map[string]float64{key: 4}, true},
		{"nan", map[string]float64{key: 4}, map[string]float64{key: math.NaN()}, true},
		{"inf", map[string]float64{key: 4}, map[string]float64{key: math.Inf(1)}, true},
		{"negative", map[string]float64{key: -5}, map[string]float64{key: 4}, true},
		{"invalid interval", map[string]float64{key: 4}, map[string]float64{key: 8}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := dlvObservation{Valid: tc.valid, Before: resources{Valid: true, Metrics: tc.before}, After: resources{Valid: true, Metrics: tc.after}}
			dlvAttributeObservation(&o, dlvCell{})
			stats := o.ProxyVL.CacheLayerMetrics.(*dlvLayerMetrics)
			value, present := stats.CounterDeltas[key]
			if !present || value != nil {
				t.Fatalf("unsupported/reset counter fabricated: present=%t value=%v", present, value)
			}
		})
	}
}

func TestDLVCompatTier0DedicatedMetricsAndDisabled(t *testing.T) {
	before, after := map[string]float64{}, map[string]float64{}
	for _, suffix := range []string{"hits_total", "misses_total", "requests_total", "objects", "bytes"} {
		key := layerSample(suffix, "loki", "compat", "l1_memory")
		before[key] = 10
		after[key] = 15
	}
	for _, enabled := range []bool{true, false} {
		o := dlvObservation{Valid: true, Before: resources{Valid: true, Metrics: before}, After: resources{Valid: true, Metrics: after}}
		dlvAttributeObservation(&o, dlvCell{Tier0: enabled})
		if enabled {
			if o.ProxyLoki.Tier0.Hits == nil || *o.ProxyLoki.Tier0.Hits != 5 || *o.ProxyLoki.Tier0.Misses != 5 || *o.ProxyLoki.Tier0.Requests != 5 || *o.ProxyLoki.Tier0.Objects != 15 || *o.ProxyLoki.Tier0.Bytes != 15 {
				t.Fatal("compat Tier0 not populated from dedicated metrics")
			}
			if o.ProxyLoki.Tier0.UnsupportedReason != "" {
				t.Fatal("measured compat Tier0 reported unsupported")
			}
		} else if o.ProxyLoki.Tier0.Hits != nil || o.ProxyLoki.Tier0.Misses != nil || o.ProxyLoki.Tier0.Objects != nil {
			t.Fatal("disabled Tier0 assigned fake lookup measurements")
		}
	}
}

func TestDLVLayerCounterRejectsMaskedPerNodeResetsAndMissingSources(t *testing.T) {
	key := layerSample("hits_total", "vl", "primary", "l1_memory")
	before := resources{Valid: true, ExpectedNodes: 2, Metrics: map[string]float64{key: 100}, PerNode: map[string]map[string]float64{"a": {key: 90}, "b": {key: 10}}}
	for _, after := range []resources{
		{Valid: true, ExpectedNodes: 2, Metrics: map[string]float64{key: 110}, PerNode: map[string]map[string]float64{"a": {key: 1}, "b": {key: 109}}},
		{Valid: true, ExpectedNodes: 2, Metrics: map[string]float64{key: 110}, PerNode: map[string]map[string]float64{"a": {key: 110}, "b": {}}},
		{Valid: true, ExpectedNodes: 2, Metrics: map[string]float64{key: 110}, PerNode: map[string]map[string]float64{"a": {key: 110}}},
	} {
		o := dlvObservation{Valid: true, Before: before, After: after}
		dlvAttributeObservation(&o, dlvCell{})
		if got := o.ProxyVL.CacheLayerMetrics.(*dlvLayerMetrics).CounterDeltas[key]; got != nil {
			t.Fatalf("aggregate hid per-node reset/missing metric: %g", *got)
		}
	}
}

func TestDLVLayerGaugesRejectNonfiniteAndMissingValues(t *testing.T) {
	key := layerSample("disk_queue_bytes", "vl", "primary", "")
	for _, after := range []map[string]float64{nil, {key: math.NaN()}, {key: math.Inf(1)}, {key: -1}} {
		o := dlvObservation{Valid: true, Before: resources{Valid: true, Metrics: map[string]float64{key: 20}}, After: resources{Valid: true, Metrics: after}}
		dlvAttributeObservation(&o, dlvCell{})
		stats := o.ProxyVL.CacheLayerMetrics.(*dlvLayerMetrics)
		if stats.AfterGauges[key] != nil {
			t.Fatal("missing/nonfinite/negative gauge fabricated")
		}
		if got := stats.BeforeGauges[key]; got == nil || *got != 20 {
			t.Fatal("valid gauge endpoint lost")
		}
	}
}
