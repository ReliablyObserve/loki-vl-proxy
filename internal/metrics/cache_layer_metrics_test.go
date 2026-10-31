package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func layerFixture() []CacheComponentStats {
	var layers [cache.LayerCount]cache.LayerStatsSnapshot
	for i := range layers {
		n := int64(i + 1)
		layers[i] = cache.LayerStatsSnapshot{
			L1:             cache.LayerTierStatsSnapshot{Requests: n * 10, Hits: n * 7, Misses: n * 3, StaleHits: n, ReadBytes: n * 100, WriteBytes: n * 200, Admissions: n * 2, Rejections: n, Evictions: n, Entries: n * 4, Bytes: n * 400},
			L2:             cache.LayerTierStatsSnapshot{Requests: n * 5, Hits: n * 3, Misses: n * 2, StaleHits: n, ReadBytes: n * 50, WriteBytes: n * 60, Admissions: n * 2, Rejections: n, Evictions: n, Entries: n * 6, Bytes: n * 600, LookupNanoseconds: n * 1250000000},
			L3:             cache.LayerTierStatsSnapshot{Requests: n * 2, Hits: n, Misses: n, ReadBytes: n * 30, WriteBytes: n * 40, Admissions: n, Rejections: n, LookupNanoseconds: n * 2500000000},
			DiskIndexBytes: n * 11, DiskQueueBytes: n * 12, DiskEncodedReadBytes: n * 13, DiskEncodedWriteBytes: n * 14,
		}
	}
	return []CacheComponentStats{{Component: "primary", Layers: layers}}
}

// These helpers inspect actual exported wire payloads rather than internal descriptors.
func scrapeLayerMetrics(t *testing.T, m *Metrics) (map[string]float64, map[string]string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(m.Handler))
	defer server.Close()
	response, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body strings.Builder
	if _, err := io.Copy(&body, response.Body); err != nil {
		t.Fatal(err)
	}
	values, types := map[string]float64{}, map[string]string{}
	for _, line := range strings.Split(body.String(), "\n") {
		if strings.HasPrefix(line, "# TYPE loki_vl_proxy_cache_layer_") {
			fields := strings.Fields(line)
			types[fields[2]] = fields[3]
		}
		if !strings.HasPrefix(line, "loki_vl_proxy_cache_layer_") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("bad sample %q", line)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatal(err)
		}
		if _, duplicate := values[fields[0]]; duplicate {
			t.Fatalf("duplicate series %s", fields[0])
		}
		values[fields[0]] = value
	}
	return values, types
}

func pushLayerMetrics(t *testing.T, m *Metrics) (map[string]float64, map[string]string) {
	t.Helper()
	values, types := map[string]float64{}, map[string]string{}
	var inspectErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ResourceMetrics []struct {
				ScopeMetrics []struct {
					Metrics []struct {
						Name string `json:"name"`
						Unit string `json:"unit"`
						Sum  *struct {
							IsMonotonic            bool              `json:"isMonotonic"`
							AggregationTemporality int               `json:"aggregationTemporality"`
							DataPoints             []json.RawMessage `json:"dataPoints"`
						} `json:"sum"`
						Gauge *struct {
							DataPoints []json.RawMessage `json:"dataPoints"`
						} `json:"gauge"`
					} `json:"metrics"`
				} `json:"scopeMetrics"`
			} `json:"resourceMetrics"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			inspectErr = err
			return
		}
		for _, resource := range payload.ResourceMetrics {
			for _, scope := range resource.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if !strings.HasPrefix(metric.Name, "loki_vl_proxy_cache_layer_") {
						continue
					}
					var points []json.RawMessage
					if metric.Sum != nil {
						types[metric.Name] = "counter"
						points = metric.Sum.DataPoints
						if !metric.Sum.IsMonotonic || metric.Sum.AggregationTemporality != 2 {
							inspectErr = fmt.Errorf("bad counter temporality for %s", metric.Name)
						}
					}
					if metric.Gauge != nil {
						types[metric.Name] = "gauge"
						points = metric.Gauge.DataPoints
					}
					expectedUnit := ""
					if strings.Contains(metric.Name, "bytes") {
						expectedUnit = "By"
					} else if strings.Contains(metric.Name, "seconds") {
						expectedUnit = "s"
					} else if strings.HasSuffix(metric.Name, "objects") {
						expectedUnit = "{entry}"
					}
					if metric.Unit != expectedUnit {
						inspectErr = fmt.Errorf("%s unit %q want %q", metric.Name, metric.Unit, expectedUnit)
					}
					for _, raw := range points {
						var dp struct {
							AsInt      *int64   `json:"asInt"`
							AsDouble   *float64 `json:"asDouble"`
							Attributes []struct {
								Key   string `json:"key"`
								Value struct {
									StringValue string `json:"stringValue"`
								} `json:"value"`
							} `json:"attributes"`
						}
						if err := json.Unmarshal(raw, &dp); err != nil {
							inspectErr = err
							continue
						}
						labels := make(map[string]string)
						for _, attribute := range dp.Attributes {
							if attribute.Key != "layer" && attribute.Key != "component" && attribute.Key != "tier" {
								inspectErr = fmt.Errorf("unexpected OTLP label %q", attribute.Key)
							}
							labels[attribute.Key] = attribute.Value.StringValue
						}
						key := metric.Name + `{layer="` + labels["layer"] + `"`
						if component, ok := labels["component"]; ok {
							key += `,component="` + component + `"`
						}
						if tier, ok := labels["tier"]; ok {
							key += `,tier="` + tier + `"`
						}
						key += "}"
						if len(labels) < 1 || len(labels) > 3 {
							inspectErr = fmt.Errorf("unexpected labels for %s", key)
						}
						if _, duplicate := values[key]; duplicate {
							inspectErr = fmt.Errorf("duplicate OTLP series %s", key)
						}
						if dp.AsInt != nil {
							values[key] = float64(*dp.AsInt)
						} else if dp.AsDouble != nil {
							values[key] = *dp.AsDouble
						} else {
							inspectErr = fmt.Errorf("no value for %s", key)
						}
					}
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := NewOTLPPusher(OTLPConfig{Endpoint: server.URL}, m).push(context.Background()); err != nil {
		t.Fatal(err)
	}
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	return values, types
}

func TestCacheLayerMetricsExporterParity(t *testing.T) {
	m := NewMetrics()
	m.SetCacheLayerStatsProvider(layerFixture)
	prom, promTypes := scrapeLayerMetrics(t, m)
	otlp, otlpTypes := pushLayerMetrics(t, m)
	if len(prom) != 96 {
		t.Fatalf("got %d layer series, want 96", len(prom))
	}
	if fmt.Sprint(sortedLayerSamples(prom)) != fmt.Sprint(sortedLayerSamples(otlp)) {
		t.Fatalf("exporters disagree:\nprom=%v\notlp=%v", prom, otlp)
	}
	if fmt.Sprint(sortedStringMap(promTypes)) != fmt.Sprint(sortedStringMap(otlpTypes)) {
		t.Fatalf("metric types differ: %v vs %v", promTypes, otlpTypes)
	}
	for name, kind := range promTypes {
		want := "counter"
		if name == "loki_vl_proxy_cache_layer_objects" || name == "loki_vl_proxy_cache_layer_bytes" || name == "loki_vl_proxy_cache_layer_disk_index_bytes" || name == "loki_vl_proxy_cache_layer_disk_queue_bytes" {
			want = "gauge"
		}
		if kind != want {
			t.Errorf("%s type=%s want %s", name, kind, want)
		}
	}
	for key, want := range map[string]float64{
		`loki_vl_proxy_cache_layer_hits_total{layer="loki",component="primary",tier="l1_memory"}`:         14,
		`loki_vl_proxy_cache_layer_bytes{layer="vl",component="primary",tier="l2_disk"}`:                  1800,
		`loki_vl_proxy_cache_layer_lookup_seconds_total{layer="loki",component="primary",tier="l2_disk"}`: 2.5,
		`loki_vl_proxy_cache_layer_disk_encoded_read_bytes_total{layer="vl",component="primary"}`:         39,
	} {
		if got := prom[key]; got != want {
			t.Errorf("%s=%g want %g", key, got, want)
		}
	}
	for key := range prom {
		if strings.Contains(key, `tier="l0"`) || strings.Contains(key, "lookup_seconds_total") && strings.Contains(key, "l1_memory") || strings.Contains(key, "objects") && strings.Contains(key, "l3_peer") {
			t.Errorf("unsupported measurement exported: %s", key)
		}
		for _, unsupported := range []string{"write_bytes_total", "admissions_total", "rejections_total", "evictions_total", "stale_hits_total"} {
			if strings.Contains(key, unsupported) && strings.Contains(key, `tier="l3_peer"`) {
				t.Errorf("unmeasured L3 quantity exported: %s", key)
			}
		}
	}
}

func sortedLayerSamples(values map[string]float64) []string {
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, fmt.Sprintf("%s %g", key, value))
	}
	sort.Strings(out)
	return out
}
func sortedStringMap(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+" "+value)
	}
	sort.Strings(out)
	return out
}

func TestCacheLayerMetricsOptionalAndBounded(t *testing.T) {
	m := NewMetrics()
	if values, _ := scrapeLayerMetrics(t, m); len(values) != 0 {
		t.Fatal("absent provider invented measurements")
	}
	if values, _ := pushLayerMetrics(t, m); len(values) != 0 {
		t.Fatal("absent provider invented OTLP measurements")
	}
	m.SetCacheLayerStatsProvider(func() []CacheComponentStats {
		fixture := layerFixture()
		return append(fixture, fixture[0], CacheComponentStats{Component: `tenant-secret"\nquery`})
	})
	values, _ := scrapeLayerMetrics(t, m)
	if len(values) != 96 {
		t.Fatalf("duplicates or arbitrary components expanded series: %d", len(values))
	}
	values, _ = pushLayerMetrics(t, m)
	if len(values) != 96 {
		t.Fatalf("OTLP duplicates or arbitrary components expanded series: %d", len(values))
	}
	m.SetCacheLayerStatsProvider(nil)
	if values, _ := scrapeLayerMetrics(t, m); len(values) != 0 {
		t.Fatal("nil provider retained stale measurements")
	}
	var nilMetrics *Metrics
	nilMetrics.SetCacheLayerStatsProvider(nil)
}

func TestCacheLayerMetricsProviderConcurrentReplacement(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 30 {
			m.SetCacheLayerStatsProvider(layerFixture)
			m.SetCacheLayerStatsProvider(nil)
			m.SetCacheLayerCoalescerStatsProvider(func() [cache.LayerCount]CoalescerStats { return [cache.LayerCount]CoalescerStats{} })
			m.SetCacheLayerCoalescerStatsProvider(nil)
		}
	}()
	go func() {
		defer wg.Done()
		for range 30 {
			w := httptest.NewRecorder()
			m.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
			NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload()
		}
	}()
	wg.Wait()
}

func TestCacheLayerMetricsKnownComponentsAndEmptyProvider(t *testing.T) {
	m := NewMetrics()
	m.SetCacheLayerStatsProvider(func() []CacheComponentStats { return nil })
	if values, _ := scrapeLayerMetrics(t, m); len(values) != 0 {
		t.Fatal("empty provider invented measurements")
	}
	if values, _ := pushLayerMetrics(t, m); len(values) != 0 {
		t.Fatal("empty provider invented OTLP measurements")
	}
	m.SetCacheLayerStatsProvider(func() []CacheComponentStats {
		fixture := layerFixture()[0]
		var out []CacheComponentStats
		for _, name := range []string{"primary", "compat", "translation", "field_names"} {
			fixture.Component = name
			out = append(out, fixture)
		}
		return out
	})
	prom, _ := scrapeLayerMetrics(t, m)
	otlp, _ := pushLayerMetrics(t, m)
	if len(prom) != 384 || len(otlp) != 384 {
		t.Fatalf("all component series prom=%d otlp=%d", len(prom), len(otlp))
	}
	if fmt.Sprint(sortedLayerSamples(prom)) != fmt.Sprint(sortedLayerSamples(otlp)) {
		t.Fatal("multiple component exporter mismatch")
	}
	for _, name := range []string{"primary", "compat", "translation", "field_names"} {
		key := `loki_vl_proxy_cache_layer_hits_total{layer="loki",component="` + name + `",tier="l1_memory"}`
		if prom[key] != 14 {
			t.Errorf("component %s missing distinct snapshot", name)
		}
	}
}

func TestCacheLayerCoalescerMetricsExporterParity(t *testing.T) {
	m := NewMetrics()
	m.SetCacheLayerCoalescerStatsProvider(func() [cache.LayerCount]CoalescerStats {
		var out [cache.LayerCount]CoalescerStats
		out[cache.LayerVL] = CoalescerStats{Total: 12, Saved: 5, WaitCount: 7, WaitNanos: 1750000000}
		return out
	})
	prom, promTypes := scrapeLayerMetrics(t, m)
	otlp, otlpTypes := pushLayerMetrics(t, m)
	if len(prom) != 12 || len(otlp) != 12 {
		t.Fatalf("coalescer series: prom=%d otlp=%d", len(prom), len(otlp))
	}
	if fmt.Sprint(sortedLayerSamples(prom)) != fmt.Sprint(sortedLayerSamples(otlp)) {
		t.Fatalf("coalescer exporters disagree: %v vs %v", prom, otlp)
	}
	if fmt.Sprint(sortedStringMap(promTypes)) != fmt.Sprint(sortedStringMap(otlpTypes)) {
		t.Fatal("coalescer metric types differ")
	}
	for name, want := range map[string]float64{"coalesced_total": 12, "coalesced_saved_total": 5, "coalesced_wait_total": 7, "coalesced_wait_seconds_total": 1.75} {
		key := `loki_vl_proxy_cache_layer_` + name + `{layer="vl"}`
		if got := prom[key]; got != want {
			t.Errorf("%s=%g want %g", key, got, want)
		}
	}
	m.SetCacheLayerCoalescerStatsProvider(nil)
	if values, _ := scrapeLayerMetrics(t, m); len(values) != 0 {
		t.Fatal("removed coalescer provider retained samples")
	}
	var nilMetrics *Metrics
	nilMetrics.SetCacheLayerCoalescerStatsProvider(nil)
}

func TestCacheLayerMetricsPreserveLegacyCounters(t *testing.T) {
	m := NewMetrics()
	m.SetCacheStatsProvider(func() cache.StatsSnapshot {
		return cache.StatsSnapshot{L1: cache.TierStatsSnapshot{Requests: 17, Hits: 11, Misses: 6}, DiskBytes: 29}
	})
	m.SetCoalescerStatsProvider(func() CoalescerStats { return CoalescerStats{Total: 31, Saved: 7, WaitCount: 9, WaitNanos: 123} })
	scrape := func() string {
		w := httptest.NewRecorder()
		m.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
		var lines []string
		for _, line := range strings.Split(w.Body.String(), "\n") {
			if strings.HasPrefix(line, "loki_vl_proxy_cache_") && !strings.HasPrefix(line, "loki_vl_proxy_cache_layer_") || strings.HasPrefix(line, "loki_vl_proxy_coalesced_") {
				lines = append(lines, line)
			}
		}
		return strings.Join(lines, "\n")
	}
	before := scrape()
	m.SetCacheLayerStatsProvider(layerFixture)
	m.SetCacheLayerCoalescerStatsProvider(func() [cache.LayerCount]CoalescerStats { return [cache.LayerCount]CoalescerStats{} })
	if after := scrape(); after != before {
		t.Fatalf("legacy counters changed:\nbefore %s\nafter %s", before, after)
	}
}
