package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func scrapeProxyCacheLayers(t *testing.T, p *Proxy) string {
	t.Helper()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("metrics status %d: %s", w.Code, w.Body)
	}
	return w.Body.String()
}

func proxyLayerMetric(t *testing.T, body, name, layer, component, tier string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "loki_vl_proxy_cache_layer_"+name+"{") {
			continue
		}
		if !strings.Contains(line, `layer="`+layer+`"`) || !strings.Contains(line, `component="`+component+`"`) || !strings.Contains(line, `tier="`+tier+`"`) {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	t.Fatalf("missing %s layer=%s component=%s tier=%s", name, layer, component, tier)
	return 0
}

func TestCacheLayerMetricsRouteComponents(t *testing.T) {
	primary := cache.New(time.Hour, 100)
	compat := cache.New(time.Hour, 100)
	defer primary.Close()
	defer compat.Close()
	p, err := New(Config{BackendURL: "http://unused", Cache: primary, CompatCache: compat, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	for _, item := range []struct {
		store                        *cache.Cache
		key, value, layer, component string
	}{
		{primary, "query-steps:v1:scoped", "loki-points", "loki", "primary"},
		{primary, "vlinv-v1:scoped", "vl-bucket", "vl", "primary"},
		{primary, "unclassified:scoped", "aux", "unknown", "primary"},
		{compat, "compat:v2:scoped", "response", "loki", "compat"},
		{p.translationCache, `{app="api"}`, "translation", "loki", "translation"},
		{p.streamFieldNamesCache, "raw auxiliary identity", "fields", "vl", "field_names"},
	} {
		item.store.Set(item.key, []byte(item.value))
		if got, ok := item.store.Get(item.key); !ok || string(got) != item.value {
			t.Fatalf("cache response %s: %q/%t", item.component, got, ok)
		}
		body := scrapeProxyCacheLayers(t, p)
		if got := proxyLayerMetric(t, body, "objects", item.layer, item.component, "l1_memory"); got != 1 {
			t.Fatalf("%s/%s objects=%v want 1", item.component, item.layer, got)
		}
		if got := proxyLayerMetric(t, body, "bytes", item.layer, item.component, "l1_memory"); got != float64(len(item.key)+len(item.value)) {
			t.Fatalf("%s/%s byte charge=%v", item.component, item.layer, got)
		}
		if got := proxyLayerMetric(t, body, "hits_total", item.layer, item.component, "l1_memory"); got != 1 {
			t.Fatalf("%s/%s hits=%v", item.component, item.layer, got)
		}
	}
}

func TestCacheLayerMetricsRouteDeduplicatesAliasedInstances(t *testing.T) {
	c := cache.New(time.Hour, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: "http://unused", Cache: c, CompatCache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	c.Set("compat:v2:scoped", []byte("one physical allocation"))
	body := scrapeProxyCacheLayers(t, p)
	if got := proxyLayerMetric(t, body, "objects", "loki", "primary", "l1_memory"); got != 1 {
		t.Fatalf("objects=%v", got)
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "loki_vl_proxy_cache_layer_") && strings.Contains(line, `component="compat"`) {
			t.Fatalf("aliased cache counted twice: %s", line)
		}
	}
}

func TestCacheLayerMetricsRouteCompleteResponseReuse(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[2]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, true)
	origins := traceVLCallOrigins(p)
	target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(3*time.Minute))
	first := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "cold")))
	second := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "repeat")))
	want := dlvBaselineWant(fixture.rows, shape, base.Add(time.Minute), base.Add(3*time.Minute), "10", "alpha")
	if len(first) == 0 || !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) || origins.made("cold") == 0 || origins.made("repeat") != 0 {
		t.Fatal("complete response repeat changed or read backend again")
	}
	body := scrapeProxyCacheLayers(t, p)
	if hits := proxyLayerMetric(t, body, "hits_total", "loki", "compat", "l1_memory"); hits < 1 {
		t.Fatalf("complete response cache hit was not attributed: %v", hits)
	}
}

func TestCacheLayerMetricsSharedDiskCountedOnce(t *testing.T) {
	disk, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "shared.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = disk.Close() }()
	primary, compat := cache.New(time.Hour, 100), cache.New(time.Hour, 100)
	defer primary.Close()
	defer compat.Close()
	primary.SetL2(disk)
	compat.SetL2(disk)
	p, err := New(Config{BackendURL: "http://unused", Cache: primary, CompatCache: compat, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	disk.Set("vlinv-v1:scoped", []byte("backend-bucket"), time.Hour)
	disk.Flush()
	var objects int64
	for _, component := range p.cacheLayerStats() {
		objects += component.Layers[cache.LayerVL].L2.Entries
	}
	if objects != 1 {
		t.Fatalf("shared disk records=%d, want one physical record", objects)
	}
}

// conformance: cache-eviction-signals
func TestCacheSignalMetricsSharedDiskCountedOnce(t *testing.T) {
	disk, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "shared.db"), MinTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = disk.Close() }()
	primary, compat := cache.New(time.Hour, 100), cache.New(time.Hour, 100)
	defer primary.Close()
	defer compat.Close()
	primary.SetL2(disk)
	compat.SetL2(disk)
	p, err := New(Config{BackendURL: "http://unused", Cache: primary, CompatCache: compat, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	disk.Set("vlinv-v1:short", []byte("backend-bucket"), time.Second)
	var rejected int64
	for _, component := range p.cacheSignalStats() {
		rejected += component.Layers[cache.LayerVL].L2.Rejections[cache.RejectionTTLBelowMinimum]
	}
	if rejected != 1 {
		t.Fatalf("shared disk rejections=%d, want one", rejected)
	}
	body := scrapeProxyCacheLayers(t, p)
	if !strings.Contains(body, `loki_vl_proxy_cache_admission_rejections_total{layer="vl",tier="l2_disk",reason="ttl_below_minimum"} 1`+"\n") {
		t.Fatalf("scrape does not hold the one rejection:\n%s", body)
	}
}

// The TTL gauge states what the handlers store with: the configured inner TTLs
// and the Loki answer cache's fixed ones.
//
// conformance: cache-eviction-signals
func TestCacheTTLGaugeMatchesTheConfiguredTTLs(t *testing.T) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Hour, 100), LogLevel: "error",
		LabelCacheTTL: 7 * time.Minute, QueryRangeRecentCacheTTL: 3 * time.Second, QueryRangeHistoryCacheTTL: 2 * time.Hour, QueryRangeExtentCacheTTL: 45 * time.Minute, ParseRiskProbeCacheTTL: 20 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	got := map[string]time.Duration{}
	for _, entry := range p.cacheTTLs() {
		got[entry.Class] = entry.TTL
	}
	for class, want := range map[string]time.Duration{
		"query_range": 5 * time.Minute, "query": 5 * time.Minute, "labels": CacheTTLs["labels"], "volume": CacheTTLs["volume"],
		"labels_handler": 7 * time.Minute, "label_values_handler": 7 * time.Minute, "metadata_negative": p.metadataNegativeTTL(),
		"query_range_window_recent": 3 * time.Second, "query_range_window_history": 2 * time.Hour, "query_range_extent": 45 * time.Minute, "parse_risk_probe": 20 * time.Minute,
	} {
		if got[class] != want {
			t.Errorf("%s=%s want %s", class, got[class], want)
		}
	}
	if len(got) != 20 {
		t.Fatalf("classes %d: %v", len(got), got)
	}
	for _, endpoint := range []string{"query_range", "query", "labels", "series"} {
		if compatAnswerTTL(endpoint) != got[endpoint] {
			t.Fatalf("%s: gauge %s, answer cache %s", endpoint, got[endpoint], compatAnswerTTL(endpoint))
		}
	}
	body := scrapeProxyCacheLayers(t, p)
	if !strings.Contains(body, `loki_vl_proxy_cache_ttl_seconds{class="labels_handler"} 420`+"\n") {
		t.Fatal("scrape does not state the label handler TTL")
	}
}

// The proxy exports the sizing signals of the caches that have them on, and
// nothing while none has.
//
// conformance: cache-sizing-signals
func TestCacheSizingMetricsFromTheProxy(t *testing.T) {
	for _, on := range []bool{false, true} {
		primary := cache.New(time.Hour, 100)
		if on {
			primary.EnableSizingSignals()
		}
		p, err := New(Config{BackendURL: "http://unused", Cache: primary, LogLevel: "error"})
		if err != nil {
			t.Fatal(err)
		}
		primary.SetWithTTL("query_range:a", []byte("12345"), time.Hour)
		primary.GetWithTier("query_range:a")
		body := scrapeProxyCacheLayers(t, p)
		hit := `loki_vl_proxy_cache_hit_bytes_total{class="query_range",tier="l1_memory"} 5` + "\n"
		if strings.Contains(body, hit) != on || strings.Contains(body, "loki_vl_proxy_cache_ghost_hits_total") != on {
			t.Fatalf("sizing=%v: scrape:\n%s", on, body)
		}
		_ = p.Shutdown(context.Background())
		primary.Close()
	}
}
