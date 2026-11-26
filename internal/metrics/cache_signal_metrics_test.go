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
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

var signalFamilies = map[string]string{
	"loki_vl_proxy_cache_evictions_total":               "counter",
	"loki_vl_proxy_cache_admission_rejections_total":    "counter",
	"loki_vl_proxy_cache_promotions_total":              "counter",
	"loki_vl_proxy_cache_entry_age_at_eviction_seconds": "histogram",
	"loki_vl_proxy_cache_entry_hits_at_eviction":        "histogram",
	"loki_vl_proxy_cache_l2_read_seconds":               "histogram",
	"loki_vl_proxy_cache_l2_read_bytes":                 "histogram",
	"loki_vl_proxy_cache_ttl_seconds":                   "gauge",
}

func signalHistogramFixture(buckets int, seed int64) cache.HistogramSnapshot {
	counts := make([]int64, buckets+1)
	for i := range counts {
		counts[i] = seed + int64(i)
	}
	return cache.NewHistogramSnapshot(counts, seed*1_000_000_000)
}

func signalFixture() []CacheComponentSignals {
	component := func(name string, scale int64) CacheComponentSignals {
		out := CacheComponentSignals{Component: name}
		for l := range out.Layers {
			n := scale * int64(l+1)
			s := &out.Layers[l]
			for r := range s.L1.Evictions {
				s.L1.Evictions[r], s.L2.Evictions[r] = n*int64(r+1), n*int64(r+10)
			}
			for r := range s.L1.Rejections {
				s.L1.Rejections[r], s.L2.Rejections[r] = n*int64(r+1), n*int64(r+20)
			}
			for r := range s.L1.AgeAtEviction {
				s.L1.AgeAtEviction[r] = signalHistogramFixture(len(cache.EvictionAgeBounds), n)
				s.L2.AgeAtEviction[r] = signalHistogramFixture(len(cache.EvictionAgeBounds), n+1)
				s.L1.HitsAtEviction[r] = signalHistogramFixture(len(cache.EvictionHitsBounds), n)
				s.L2.HitsAtEviction[r] = signalHistogramFixture(len(cache.EvictionHitsBounds), n+2)
			}
			for p := range s.Promotions {
				s.Promotions[p] = n * int64(p+1)
			}
			s.DiskReadSeconds = signalHistogramFixture(len(cache.DiskReadSecondsBounds), n)
			s.DiskReadBytes = signalHistogramFixture(len(cache.DiskReadBytesBounds), n)
		}
		return out
	}
	// compat appears twice: a duplicate registration is counted once.
	return []CacheComponentSignals{component("primary", 1), component("compat", 100), component("compat", 10000)}
}

func ttlFixture() []CacheTTL {
	out := []CacheTTL{{Class: "not_a_class", TTL: time.Hour}}
	for i, class := range CacheTTLClasses {
		out = append(out, CacheTTL{Class: class, TTL: time.Duration(i+1) * time.Second})
	}
	return out
}

var signalSampleLabels = map[string]bool{"layer": true, "tier": true, "reason": true, "from": true, "to": true, "class": true, "le": true}

// scrapeSignalMetrics returns every sample of the signal families, keyed by
// name and sorted labels, and the families' TYPE lines.
func scrapeSignalMetrics(t *testing.T, m *Metrics) (map[string]float64, map[string]string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	m.Handler(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	values, types := map[string]float64{}, map[string]string{}
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			if _, ok := signalFamilies[fields[2]]; ok {
				types[fields[2]] = fields[3]
			}
			continue
		}
		name, rest, ok := strings.Cut(line, "{")
		if !ok {
			continue
		}
		family := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
		if _, ok := signalFamilies[family]; !ok {
			if _, ok := signalFamilies[name]; !ok {
				continue
			}
		}
		labelText, valueText, _ := strings.Cut(rest, "} ")
		labels := strings.Split(labelText, ",")
		for _, label := range labels {
			key, _, _ := strings.Cut(label, "=")
			if !signalSampleLabels[key] {
				t.Fatalf("unexpected label %q in %s", key, line)
			}
		}
		sort.Strings(labels)
		value, err := strconv.ParseFloat(valueText, 64)
		if err != nil {
			t.Fatalf("bad sample %q: %v", line, err)
		}
		series := name + "{" + strings.Join(labels, ",") + "}"
		if _, dup := values[series]; dup {
			t.Fatalf("duplicate series %s", series)
		}
		values[series] = value
	}
	return values, types
}

// pushSignalMetrics decodes the OTLP payload into the same keys as the scrape:
// histogram bucket counts are made cumulative and keyed by le.
func pushSignalMetrics(t *testing.T, m *Metrics) (map[string]float64, map[string]string) {
	t.Helper()
	values, types := map[string]float64{}, map[string]string{}
	var inspectErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload struct {
			ResourceMetrics []struct {
				ScopeMetrics []struct {
					Metrics []struct {
						Name      string                                  `json:"name"`
						Sum       *struct{ DataPoints []json.RawMessage } `json:"sum"`
						Gauge     *struct{ DataPoints []json.RawMessage } `json:"gauge"`
						Histogram *struct{ DataPoints []json.RawMessage } `json:"histogram"`
					} `json:"metrics"`
				} `json:"scopeMetrics"`
			} `json:"resourceMetrics"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			inspectErr = err
			return
		}
		for _, resource := range payload.ResourceMetrics {
			for _, scope := range resource.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if _, ok := signalFamilies[metric.Name]; !ok {
						continue
					}
					var points []json.RawMessage
					switch {
					case metric.Sum != nil:
						types[metric.Name], points = "counter", metric.Sum.DataPoints
					case metric.Gauge != nil:
						types[metric.Name], points = "gauge", metric.Gauge.DataPoints
					case metric.Histogram != nil:
						types[metric.Name], points = "histogram", metric.Histogram.DataPoints
					}
					for _, raw := range points {
						var dp struct {
							AsInt          *int64   `json:"asInt"`
							AsDouble       *float64 `json:"asDouble"`
							Count          string   `json:"count"`
							Sum            float64  `json:"sum"`
							ExplicitBounds []float64
							BucketCounts   []int64
							Attributes     []struct {
								Key   string
								Value struct{ StringValue string }
							}
						}
						if err := json.Unmarshal(raw, &dp); err != nil {
							inspectErr = err
							continue
						}
						var labels []string
						for _, a := range dp.Attributes {
							labels = append(labels, fmt.Sprintf("%s=%q", a.Key, a.Value.StringValue))
						}
						key := func(name string, extra ...string) string {
							all := append(append([]string(nil), labels...), extra...)
							sort.Strings(all)
							return name + "{" + strings.Join(all, ",") + "}"
						}
						switch {
						case metric.Histogram != nil:
							cumulative := int64(0)
							for i, bound := range dp.ExplicitBounds {
								cumulative += dp.BucketCounts[i]
								values[key(metric.Name+"_bucket", `le="`+strconv.FormatFloat(bound, 'g', -1, 64)+`"`)] = float64(cumulative)
							}
							count, _ := strconv.ParseFloat(dp.Count, 64)
							values[key(metric.Name+"_bucket", `le="+Inf"`)] = count
							values[key(metric.Name+"_count")] = count
							values[key(metric.Name+"_sum")] = dp.Sum
						case dp.AsInt != nil:
							values[key(metric.Name)] = float64(*dp.AsInt)
						case dp.AsDouble != nil:
							values[key(metric.Name)] = *dp.AsDouble
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

// conformance: cache-eviction-signals
func TestCacheSignalMetricsExporterParityAndCardinality(t *testing.T) {
	m := NewMetrics()
	m.SetCacheSignalStatsProvider(signalFixture)
	m.SetCacheTTLProvider(ttlFixture)
	prom, promTypes := scrapeSignalMetrics(t, m)
	otlp, otlpTypes := pushSignalMetrics(t, m)
	if fmt.Sprint(promTypes) != fmt.Sprint(signalFamilies) || fmt.Sprint(otlpTypes) != fmt.Sprint(signalFamilies) {
		t.Fatalf("types: prom %v otlp %v", promTypes, otlpTypes)
	}
	if len(prom) != len(otlp) {
		t.Fatalf("series: prom %d otlp %d", len(prom), len(otlp))
	}
	for key, want := range prom {
		if got, ok := otlp[key]; !ok || got != want {
			t.Fatalf("%s: prom %g otlp %g (%v)", key, want, got, ok)
		}
	}
	// The series set is fixed: 3 layers x (L1 5 + L2 5 eviction reasons, L1 3 +
	// L2 9 rejection reasons, 3 promotions, 2 tiers x 2 reasons x (age 11+3,
	// hits 8+3), disk reads 12+3 and 9+3), plus 20 TTL classes.
	perFamily := map[string]int{}
	for key := range prom {
		name, _, _ := strings.Cut(key, "{")
		perFamily[strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")]++
	}
	want := map[string]int{
		"loki_vl_proxy_cache_evictions_total":               30,
		"loki_vl_proxy_cache_admission_rejections_total":    36,
		"loki_vl_proxy_cache_promotions_total":              9,
		"loki_vl_proxy_cache_entry_age_at_eviction_seconds": 168,
		"loki_vl_proxy_cache_entry_hits_at_eviction":        132,
		"loki_vl_proxy_cache_l2_read_seconds":               45,
		"loki_vl_proxy_cache_l2_read_bytes":                 36,
		"loki_vl_proxy_cache_ttl_seconds":                   len(CacheTTLClasses),
	}
	if fmt.Sprint(perFamily) != fmt.Sprint(want) {
		t.Fatalf("series per family %v, want %v", perFamily, want)
	}
	// Components are summed, a duplicate component name counted once.
	for key, value := range map[string]float64{
		`loki_vl_proxy_cache_evictions_total{layer="loki",reason="expired",tier="l1_memory"}`:            202,  // (1+100) x 2 x 1
		`loki_vl_proxy_cache_admission_rejections_total{layer="vl",reason="memory_only",tier="l2_disk"}`: 9090, // 3 x 30 + 300 x 30
		`loki_vl_proxy_cache_promotions_total{from="l3_peer",layer="unknown",to="l2_disk"}`:              303,
		`loki_vl_proxy_cache_ttl_seconds{class="query_range"}`:                                           1,
		`loki_vl_proxy_cache_l2_read_seconds_sum{layer="unknown"}`:                                       101,
	} {
		if got := prom[key]; got != value {
			t.Errorf("%s=%g want %g", key, got, value)
		}
	}
	if _, ok := prom[`loki_vl_proxy_cache_ttl_seconds{class="not_a_class"}`]; ok {
		t.Fatal("unknown TTL class exported")
	}
	// Cumulative buckets end at the count.
	for key, value := range prom {
		if !strings.Contains(key, `le="+Inf"`) {
			continue
		}
		name, labels, _ := strings.Cut(key, "{")
		count := strings.TrimSuffix(name, "_bucket") + "_count{" + strings.Replace(strings.Replace(labels, `le="+Inf",`, "", 1), `,le="+Inf"`, "", 1)
		if prom[count] != value {
			t.Fatalf("%s=%g, %s=%g", key, value, count, prom[count])
		}
	}
}

// conformance: cache-eviction-signals
func TestCacheSignalMetricsAbsentProviders(t *testing.T) {
	m := NewMetrics()
	if prom, _ := scrapeSignalMetrics(t, m); len(prom) != 0 {
		t.Fatalf("series without providers: %d", len(prom))
	}
	m.SetCacheSignalStatsProvider(func() []CacheComponentSignals {
		return []CacheComponentSignals{{Component: "primary"}} // histograms never initialised
	})
	prom, _ := scrapeSignalMetrics(t, m)
	for key, value := range prom {
		if value != 0 {
			t.Fatalf("%s=%g from an empty snapshot", key, value)
		}
	}
	if len(prom) != 456 {
		t.Fatalf("empty snapshot series %d, want 456", len(prom))
	}
}
