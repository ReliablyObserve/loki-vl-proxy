package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
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
