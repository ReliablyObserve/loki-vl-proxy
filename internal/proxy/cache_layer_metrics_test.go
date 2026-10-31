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
	target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(3*time.Minute))
	first := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
	before := fixture.calls.Load()
	second := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
	want := dlvBaselineWant(fixture.rows, shape, base.Add(time.Minute), base.Add(3*time.Minute), "10", "alpha")
	if len(first) == 0 || !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) || fixture.calls.Load() != before {
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
