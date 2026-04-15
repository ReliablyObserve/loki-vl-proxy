package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestHardening_QueryLengthLimit(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	// Generate a query > maxQueryLength (64KB)
	longQuery := strings.Repeat("a", maxQueryLength+1)
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+longQuery, nil)
	p.handleQueryRange(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for oversized query, got %d", w.Code)
	}
}

func TestHardening_SanitizeLimit(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "1000"},
		{"50", "50"},
		{"0", "1000"},
		{"-1", "1000"},
		{"abc", "1000"},
		{"99999", "10000"}, // capped at maxLimitValue
		{"10000", "10000"},
	}
	for _, tc := range tests {
		got := sanitizeLimit(tc.input)
		if got != tc.expected {
			t.Errorf("sanitizeLimit(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestHardening_SecurityHeaders(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{BackendURL: "http://localhost:9428", Cache: c, LogLevel: "error"})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// /ready is not behind rl() wrapper, so check a rate-limited endpoint
	resp2, err := http.Get(srv.URL + "/loki/api/v1/labels")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("expected X-Content-Type-Options: nosniff")
	}
	if resp2.Header.Get("X-Frame-Options") != "DENY" {
		t.Error("expected X-Frame-Options: DENY")
	}
}

func TestHardening_EmptyQuery(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	p.handleQueryRange(w, r)

	// Empty query should be handled gracefully (translator returns error)
	if w.Code != http.StatusBadRequest {
		t.Logf("empty query returned status %d (acceptable)", w.Code)
	}
}

func TestHardening_QueryAnalyticsDisabledByDefault(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/debug/queries", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /debug/queries when disabled, got %d", w.Code)
	}
}

func TestHardening_MetricsHideSensitiveLabelsByDefault(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.metrics.RecordTenantRequest("team-a", "query_range", 200, 5*time.Millisecond)
	p.metrics.RecordClientIdentity("grafana-user", "query_range", 5*time.Millisecond, 128)

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for /metrics, got %d body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if strings.Contains(body, "loki_vl_proxy_tenant_requests_total") {
		t.Fatalf("expected tenant-sensitive metrics to be hidden by default\n%s", body)
	}
	if strings.Contains(body, "loki_vl_proxy_client_requests_total") {
		t.Fatalf("expected client-sensitive metrics to be hidden by default\n%s", body)
	}
}

func TestHardening_QueryAnalyticsRequiresAdminToken(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:           "http://unused",
		Cache:                c,
		LogLevel:             "error",
		EnableQueryAnalytics: true,
		AdminAuthToken:       "secret",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/debug/queries", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d body=%s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest("GET", "/debug/queries", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with admin token, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestHardening_PprofDisabledByDefault(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/debug/pprof/", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /debug/pprof/ when disabled, got %d", w.Code)
	}
}

func TestHardening_PprofRequiresAdminToken(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	registerInstrumentation := true
	p, err := New(Config{
		BackendURL:              "http://unused",
		Cache:                   c,
		LogLevel:                "error",
		RegisterInstrumentation: &registerInstrumentation,
		EnablePprof:             true,
		AdminAuthToken:          "secret",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/debug/pprof/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d body=%s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest("GET", "/debug/pprof/", nil)
	req.Header.Set("X-Admin-Token", "secret")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with admin token, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestHardening_PeerCacheRequiresTokenWhenConfigured(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	c.Set("test-key", []byte("cached-value"))
	p, err := New(Config{
		BackendURL:    "http://unused",
		Cache:         c,
		LogLevel:      "error",
		PeerCache:     cache.NewPeerCache(cache.PeerConfig{}),
		PeerAuthToken: "peer-secret",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/_cache/get?key=test-key", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without peer token, got %d body=%s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest("GET", "/_cache/get?key=test-key", nil)
	req.Header.Set("X-Peer-Token", "peer-secret")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with peer token, got %d body=%s", w.Code, w.Body.String())
	}
	body, err := io.ReadAll(w.Result().Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if string(body) != "cached-value" {
		t.Fatalf("expected cached value, got %q", string(body))
	}

	req = httptest.NewRequest(http.MethodPost, "/_cache/set?key=remote-key&ttl_ms=60000", strings.NewReader("remote-value"))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on /_cache/set without peer token, got %d body=%s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/_cache/set?key=remote-key&ttl_ms=60000", strings.NewReader("remote-value"))
	req.Header.Set("X-Peer-Token", "peer-secret")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on /_cache/set with peer token, got %d body=%s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/_cache/get?key=remote-key", nil)
	req.Header.Set("X-Peer-Token", "peer-secret")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "remote-value" {
		t.Fatalf("expected remote value roundtrip after /_cache/set, got %d/%q", w.Code, w.Body.String())
	}
}

func TestHardening_ReadyUsesBackendAuthHeaders(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Basic dXNlcjpwYXNz" {
			http.Error(w, "missing backend auth", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:       vlBackend.URL,
		Cache:            c,
		LogLevel:         "error",
		BackendBasicAuth: "user:pass",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ready", nil)
	p.handleReady(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected ready check to succeed with backend auth, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestHardening_QueryAnalyticsTracksErrors(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend overloaded", http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}`, nil)
	p.handleQueryRange(w, r)

	top := p.GetQueryTracker().TopByErrors(1)
	if len(top) == 0 {
		t.Fatal("expected query error analytics entry to be recorded")
	}
	if top[0].Errors != 1 {
		t.Fatalf("expected query error count to be 1, got %d", top[0].Errors)
	}
}
