package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestDerivedFields_ExtractsTraceID(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return NDJSON with trace IDs in the log line
		lines := []string{
			`{"_time":"2024-01-15T10:30:00Z","_msg":"request completed trace_id=abc123def456 status=200","app":"api"}`,
			`{"_time":"2024-01-15T10:30:01Z","_msg":"request completed trace_id=789abc000111 status=500","app":"api"}`,
			`{"_time":"2024-01-15T10:30:02Z","_msg":"no trace here","app":"api"}`,
		}
		w.Write([]byte(strings.Join(lines, "\n")))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	defer c.Close()
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		DerivedFields: []DerivedField{
			{
				Name:         "traceID",
				MatcherRegex: `trace_id=([a-f0-9]+)`,
				URL:          "http://tempo:3200/trace/${__value.raw}",
			},
		},
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)

	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})

	if len(result) == 0 {
		t.Fatal("expected results")
	}

	// Check that at least one stream has the traceID label
	found := false
	for _, s := range result {
		stream, _ := s.(map[string]interface{})
		labels, _ := stream["stream"].(map[string]interface{})
		if traceID, ok := labels["traceID"]; ok {
			found = true
			if traceID != "abc123def456" && traceID != "789abc000111" {
				t.Errorf("unexpected traceID: %v", traceID)
			}
		}
	}
	if !found {
		t.Error("expected derived traceID label in at least one stream")
	}
}

func TestDerivedFields_NoMatch(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"no trace","app":"api"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	defer c.Close()
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		DerivedFields: []DerivedField{
			{Name: "traceID", MatcherRegex: `trace_id=([a-f0-9]+)`},
		},
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	// Should succeed without error even when no matches
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestDerivedFields_MultipleFields(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"trace_id=aaa111 span_id=bbb222","app":"api"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	defer c.Close()
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		DerivedFields: []DerivedField{
			{Name: "traceID", MatcherRegex: `trace_id=([a-f0-9]+)`},
			{Name: "spanID", MatcherRegex: `span_id=([a-f0-9]+)`},
		},
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})

	if len(result) == 0 {
		t.Fatal("expected results")
	}

	stream0, _ := result[0].(map[string]interface{})
	labels, _ := stream0["stream"].(map[string]interface{})

	if labels["traceID"] != "aaa111" {
		t.Errorf("expected traceID=aaa111, got %v", labels["traceID"])
	}
	if labels["spanID"] != "bbb222" {
		t.Errorf("expected spanID=bbb222, got %v", labels["spanID"])
	}
}

func TestStreamResponse_ChunkedTransfer(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := range 5 {
			fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:0%dZ","_msg":"line %d","app":"api"}`+"\n", i, i)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	defer c.Close()
	p, _ := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		StreamResponse: true,
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	body := w.Body.String()

	// Must be valid JSON
	var resp map[string]interface{}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("streamed response is not valid JSON: %v\nbody: %s", err, body[:200])
	}

	if resp["status"] != "success" {
		t.Error("expected status=success")
	}

	data, _ := resp["data"].(map[string]interface{})
	if data["resultType"] != "streams" {
		t.Error("expected resultType=streams")
	}

	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Error("expected results in streamed response")
	}
}

func TestStreamResponse_EmptyResult(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Empty response
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	defer c.Close()
	p, _ := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		StreamResponse: true,
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="none"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("empty streamed response not valid JSON: %v", err)
	}
	if resp["status"] != "success" {
		t.Error("expected success for empty stream")
	}
}
