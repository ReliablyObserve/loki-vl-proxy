package proxy

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestCompatCacheMiddleware_CachesSeriesResponses(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/streams" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		backendCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\"}","hits":1}]}`))
	}))
	defer backend.Close()

	p := newCompatTestProxy(t, backend.URL)
	first := doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D", map[string]string{"X-Scope-OrgID": "team-a"})
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", first.Code, first.Body.String())
	}

	second := doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D", map[string]string{"X-Scope-OrgID": "team-a"})
	if second.Code != http.StatusOK {
		t.Fatalf("expected cached 200, got %d: %s", second.Code, second.Body.String())
	}
	if backendCalls != 1 {
		t.Fatalf("expected 1 backend call after compat cache hit, got %d", backendCalls)
	}
}

func TestCompatCacheMiddleware_CachesCompressedVariantOnHit(t *testing.T) {
	var backendCalls int
	payload := `{"values":[` + strings.Repeat(`{"value":"{app=\"api\"}","hits":1},`, 64) + `{"value":"{app=\"api\"}","hits":1}]}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer backend.Close()

	p := newCompatTestProxyWithCompression(t, backend.URL, "gzip", 128)
	target := "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D"

	first := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"})
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", first.Code, first.Body.String())
	}

	second := doCompatProxyRequest(p, target, map[string]string{
		"X-Scope-OrgID":   "team-a",
		"Accept-Encoding": "gzip",
	})
	if second.Code != http.StatusOK {
		t.Fatalf("expected cached 200, got %d: %s", second.Code, second.Body.String())
	}
	if got := second.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip compat-cache hit, got %q", got)
	}
	gr, err := gzip.NewReader(second.Body)
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	decoded, err := io.ReadAll(gr)
	_ = gr.Close()
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	if string(decoded) != first.Body.String() {
		t.Fatalf("unexpected decoded compat-cache body length %d", len(decoded))
	}

	third := doCompatProxyRequest(p, target, map[string]string{
		"X-Scope-OrgID":   "team-a",
		"Accept-Encoding": "gzip",
	})
	if third.Code != http.StatusOK {
		t.Fatalf("expected cached 200 on repeated gzip hit, got %d: %s", third.Code, third.Body.String())
	}
	if backendCalls != 1 {
		t.Fatalf("expected gzip compat-cache variant to avoid backend retry, got %d backend calls", backendCalls)
	}
}

func TestCompatCacheMiddleware_IsTenantAware(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\"}","hits":1}]}`))
	}))
	defer backend.Close()

	p := newCompatTestProxy(t, backend.URL)
	doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D", map[string]string{"X-Scope-OrgID": "team-a"})
	doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D", map[string]string{"X-Scope-OrgID": "team-b"})

	if backendCalls != 2 {
		t.Fatalf("expected tenant-specific cache keys, got %d backend calls", backendCalls)
	}
}

func TestCompatCacheMiddleware_DoesNotCacheErrors(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		if backendCalls == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"backend unavailable"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\"}","hits":1}]}`))
	}))
	defer backend.Close()

	p := newCompatTestProxy(t, backend.URL)
	first := doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D", nil)
	if first.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 on first request, got %d", first.Code)
	}
	second := doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D", nil)
	if second.Code != http.StatusOK {
		t.Fatalf("expected uncached retry to succeed, got %d", second.Code)
	}
	if backendCalls != 2 {
		t.Fatalf("expected error response to bypass cache, got %d backend calls", backendCalls)
	}
}

func TestCompatCacheMiddleware_DoesNotCacheEmptyPatterns(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			backendCalls++
			w.Header().Set("Content-Type", "application/json")
			if backendCalls == 1 {
				_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
			return
		case "/select/logsql/query_range":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backend.Close()

	p := newCompatTestProxy(t, backend.URL)
	target := "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1&end=2"

	first := doCompatProxyRequest(p, target, nil)
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200 for first request, got %d: %s", first.Code, first.Body.String())
	}
	var firstBody map[string]interface{}
	mustUnmarshal(t, first.Body.Bytes(), &firstBody)
	firstData, _ := firstBody["data"].([]interface{})
	if len(firstData) != 0 {
		t.Fatalf("expected first patterns response to be empty, got %v", firstBody)
	}

	second := doCompatProxyRequest(p, target, nil)
	if second.Code != http.StatusOK {
		t.Fatalf("expected 200 for second request, got %d: %s", second.Code, second.Body.String())
	}
	var secondBody map[string]interface{}
	mustUnmarshal(t, second.Body.Bytes(), &secondBody)
	secondData, _ := secondBody["data"].([]interface{})
	if len(secondData) == 0 {
		t.Fatalf("expected second patterns response to be non-empty (empty must not be compat-cached), got %v", secondBody)
	}
	if backendCalls < 2 {
		t.Fatalf("expected backend re-query after empty patterns response, got %d calls", backendCalls)
	}
}

func TestCompatCacheMiddleware_InvalidatedOnFieldMappingReload(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"{service.name=\"api\"}","hits":1}]}`))
	}))
	defer backend.Close()

	p := newCompatTestProxy(t, backend.URL)
	doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bservice_name%3D%22api%22%7D", nil)
	p.ReloadFieldMappings([]FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}})
	doCompatProxyRequest(p, "/loki/api/v1/series?match[]=%7Bservice_name%3D%22api%22%7D", nil)

	if backendCalls != 2 {
		t.Fatalf("expected field mapping reload to invalidate compat cache, got %d backend calls", backendCalls)
	}
}

func TestShouldUseCompatCache_SkipsStreamingAndUnsafeEndpoints(t *testing.T) {
	p := &Proxy{
		compatCache:    cache.New(10*time.Second, 10),
		streamResponse: true,
	}
	t.Cleanup(p.compatCache.Close)

	queryReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D", nil)
	if p.shouldUseCompatCache("query", queryReq) {
		t.Fatal("expected streaming query path to skip compat cache")
	}

	tailReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D", nil)
	if p.shouldUseCompatCache("tail", tailReq) {
		t.Fatal("expected tail endpoint to skip compat cache")
	}
}

func newCompatTestProxy(t *testing.T, backendURL string) *Proxy {
	return newCompatTestProxyWithCompression(t, backendURL, "auto", 1024)
}

func newCompatTestProxyWithCompression(t *testing.T, backendURL, compression string, minBytes int) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                        backendURL,
		Cache:                             cache.New(60*time.Second, 1000),
		CompatCache:                       cache.New(60*time.Second, 100),
		LogLevel:                          "error",
		ClientResponseCompression:         compression,
		ClientResponseCompressionMinBytes: minBytes,
		TenantMap: map[string]TenantMapping{
			"team-a": {AccountID: "10", ProjectID: "0"},
			"team-b": {AccountID: "20", ProjectID: "0"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() {
		p.cache.Close()
		if p.compatCache != nil {
			p.compatCache.Close()
		}
		if p.translationCache != nil {
			p.translationCache.Close()
		}
	})
	return p
}

func doCompatProxyRequest(p *Proxy, target string, headers map[string]string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}
