package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newLocalOnlyCacheFixture(t *testing.T, cacheKey string) (*cache.Cache, *cache.DiskCache, *cache.PeerCache, *atomic.Int64) {
	t.Helper()

	remoteSetCalls := &atomic.Int64{}
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cache/set" {
			remoteSetCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ownerSrv.Close)

	ownerURL, err := url.Parse(ownerSrv.URL)
	if err != nil {
		t.Fatalf("parse owner URL: %v", err)
	}

	dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
	if err != nil {
		t.Fatalf("new disk cache: %v", err)
	}
	t.Cleanup(func() { _ = dc.Close() })

	pc := newNonOwnerPeerCacheForKey(t, ownerURL.Host, cacheKey)
	t.Cleanup(pc.Close)

	c := cache.New(60*time.Second, 1000)
	c.SetL2(dc)
	c.SetL3(pc)
	t.Cleanup(c.Close)

	return c, dc, pc, remoteSetCalls
}

func TestQueryRangeCache_StoresFullResponsesLocallyWhenPeerWriteThroughEnabled(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"_time\":\"2026-04-19T10:00:00Z\",\"_msg\":\"ok\",\"_stream\":\"{app=\\\"api\\\"}\"}\n"))
	}))
	defer backend.Close()

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&limit=100", nil)
	cacheKey := (&Proxy{}).queryRangeCacheKey(req, `{app="api"}`)
	c, dc, pc, remoteSetCalls := newLocalOnlyCacheFixture(t, cacheKey)

	p, err := New(Config{
		BackendURL: backend.URL,
		Cache:      c,
		PeerCache:  pc,
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", w.Code, w.Body.String())
	}

	if _, _, ok := p.cache.GetWithTTL(cacheKey); !ok {
		t.Fatalf("expected local cache entry for %q", cacheKey)
	}
	if _, ok := dc.Get(cacheKey); ok {
		t.Fatalf("expected query_range cache entry %q to skip L2 disk cache", cacheKey)
	}
	if got := remoteSetCalls.Load(); got != 0 {
		t.Fatalf("expected query_range cache entry to skip remote /_cache/set, got %d", got)
	}
	if got := pc.WTPushes.Load(); got != 0 {
		t.Fatalf("expected query_range cache entry to skip write-through pushes, got %d", got)
	}
}

func TestLabelsCache_StoresResponsesInLocalDiskCacheWhenPeerWriteThroughEnabled(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"service.name","hits":10},{"value":"k8s.namespace.name","hits":5}]}`))
		default:
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	req := httptest.NewRequest("GET", "/loki/api/v1/labels?query=%7Bservice_name%3D%22api%22%7D", nil)
	cacheKey := (&Proxy{}).canonicalReadCacheKey("labels", "", req)
	c, dc, pc, remoteSetCalls := newLocalOnlyCacheFixture(t, cacheKey)

	p, err := New(Config{
		BackendURL: backend.URL,
		Cache:      c,
		PeerCache:  pc,
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	p.handleLabels(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", w.Code, w.Body.String())
	}

	if _, _, _, ok := p.endpointReadCacheEntry("labels", cacheKey); !ok {
		t.Fatalf("expected labels cache entry for %q", cacheKey)
	}
	if _, ok := dc.Get(cacheKey); !ok {
		t.Fatalf("expected labels cache entry %q to persist in L2 disk cache", cacheKey)
	}
	if got := remoteSetCalls.Load(); got != 0 {
		t.Fatalf("expected labels cache entry to skip remote /_cache/set, got %d", got)
	}
	if got := pc.WTPushes.Load(); got != 0 {
		t.Fatalf("expected labels cache entry to skip write-through pushes, got %d", got)
	}
}

func TestSetJSONCacheWithTTL_StoresHelperResponsesInLocalDiskCache(t *testing.T) {
	cacheKey := "detected_labels:tenant-a:test"
	c, dc, pc, remoteSetCalls := newLocalOnlyCacheFixture(t, cacheKey)
	p := &Proxy{cache: c}

	p.setJSONCacheWithTTL(cacheKey, time.Minute, map[string]interface{}{
		"status": "success",
		"data":   []string{"service_name"},
	})

	body, _, ok := p.cache.GetWithTTL(cacheKey)
	if !ok {
		t.Fatalf("expected local JSON cache entry for %q", cacheKey)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode cached JSON: %v", err)
	}
	if decoded["status"] != "success" {
		t.Fatalf("unexpected cached JSON payload: %v", decoded)
	}
	if _, ok := dc.Get(cacheKey); !ok {
		t.Fatalf("expected JSON cache entry %q to persist in L2 disk cache", cacheKey)
	}
	if got := remoteSetCalls.Load(); got != 0 {
		t.Fatalf("expected JSON cache entry to skip remote /_cache/set, got %d", got)
	}
	if got := pc.WTPushes.Load(); got != 0 {
		t.Fatalf("expected JSON cache entry to skip write-through pushes, got %d", got)
	}
}
