package proxy

import (
	"encoding/json"
	"net/http"
	"strings"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Loki error response format — must match exactly what Grafana expects
// =============================================================================

func TestLokiErrorType_Mapping(t *testing.T) {
	tests := []struct {
		code     int
		wantType string
	}{
		{400, "bad_data"},
		{422, "execution"},
		{429, "bad_data"},   // 429 is not a standard Loki errorType; falls through to client error default
		{499, "canceled"},
		{500, "internal"},
		{502, "unavailable"},
		{503, "timeout"},
		{504, "timeout"},
		{404, "not_found"},
		{501, "internal"},   // server errors default to internal
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.code), func(t *testing.T) {
			got := lokiErrorType(tt.code)
			if got != tt.wantType {
				t.Errorf("lokiErrorType(%d) = %q, want %q", tt.code, got, tt.wantType)
			}
		})
	}
}

func TestLokiError_ResponseFormat(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Send a request with a query that has an unmatched brace — triggers parse error
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={unclosed&start=1&end=2&step=1`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	body := w.Body.String()
	// Loki returns plain text "parse error ..." for syntax errors.
	// Accept either plain text parse error or JSON error response.
	if !strings.Contains(body, "parse error") && !strings.Contains(body, "bad_data") {
		t.Errorf("expected parse error or bad_data in response, got %q", body)
	}
}

func TestLokiError_BackendDown_ReturnsUnavailable(t *testing.T) {
	// Use a backend URL that will fail
	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: "http://unused", Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse: %v\nbody: %s", err, w.Body.String())
	}
	if resp.ErrorType != "unavailable" {
		t.Errorf("backend down should return errorType=unavailable, got %q", resp.ErrorType)
	}
}

func TestLokiError_BadQuery_ReturnsBadData(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={unclosed&start=1&end=2&step=1`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("bad query should return 400, got %d", w.Code)
	}
	body := w.Body.String()
	// Loki returns plain text parse errors for syntax issues.
	// Accept either plain text "parse error" or JSON errorType=bad_data.
	var resp struct {
		ErrorType string `json:"errorType"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.ErrorType != "bad_data" && !strings.Contains(body, "parse error") {
		t.Errorf("bad query should return errorType=bad_data or parse error, got %q", body)
	}
}
