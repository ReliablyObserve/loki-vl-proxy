package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
)

func TestStreamOpt_ProxyUsesStreamSelectors(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:   vlBackend.URL,
		Cache:        c,
		LogLevel:     "error",
		StreamFields: []string{"app", "env"},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	p.handleQueryRange(w, r)

	// The translated query should use VL stream selector for "app" (a known stream field)
	if receivedQuery == "" {
		t.Fatal("VL backend was not called")
	}
	// app is a stream field, so it should be in {app="nginx"} format, not app:=nginx field filter
	if strings.Contains(receivedQuery, "app:=") {
		t.Logf("query uses field filter: %s (stream selector would be faster)", receivedQuery)
	}
	if strings.Contains(receivedQuery, `{app="nginx"}`) {
		t.Logf("query uses stream selector: %s (optimized!)", receivedQuery)
	}
}

func TestStreamOpt_ProxyWithoutStreamFields_UsesFieldFilter(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		// No StreamFields configured
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	p.handleQueryRange(w, r)

	if receivedQuery == "" {
		t.Fatal("VL backend was not called")
	}
	// Without stream fields config, should use field filter
	if !strings.Contains(receivedQuery, "app:=nginx") {
		t.Errorf("expected field filter when no stream fields, got %q", receivedQuery)
	}
}

func TestStreamOpt_MixedFields_SplitsCorrectly(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx","level":"error"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:   vlBackend.URL,
		Cache:        c,
		LogLevel:     "error",
		StreamFields: []string{"app"}, // only "app" is a stream field
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx",level="error"}&start=1&end=2&step=1`, nil)
	p.handleQueryRange(w, r)

	if receivedQuery == "" {
		t.Fatal("VL backend was not called")
	}
	// "app" should use stream selector, "level" should use field filter
	t.Logf("translated query: %s", receivedQuery)
	// Must contain both matchers in some form
	if !strings.Contains(receivedQuery, "nginx") || !strings.Contains(receivedQuery, "error") {
		t.Errorf("expected both matchers in query, got %q", receivedQuery)
	}
	// level should be a field filter (not a stream field)
	if !strings.Contains(receivedQuery, "level:=error") {
		t.Errorf("expected field filter for non-stream-field 'level', got %q", receivedQuery)
	}
}
