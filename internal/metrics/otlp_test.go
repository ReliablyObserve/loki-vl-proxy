package metrics

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOTLPPusher_SendsMetrics(t *testing.T) {
	var received atomic.Int32
	var mu sync.Mutex
	var lastBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		lastBody = body
		mu.Unlock()

		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %q", ct)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := NewMetrics()
	m.RecordRequest("labels", 200, 5*time.Millisecond)
	m.RecordCacheHit()
	m.RecordTranslation()

	pusher := NewOTLPPusher(OTLPConfig{
		Endpoint: srv.URL,
		Interval: 50 * time.Millisecond,
	}, m)
	pusher.Start()
	defer pusher.Stop()

	time.Sleep(120 * time.Millisecond)

	if received.Load() < 1 {
		t.Fatal("expected at least 1 push")
	}

	// Validate OTLP JSON structure
	mu.Lock()
	bodySnapshot := make([]byte, len(lastBody))
	copy(bodySnapshot, lastBody)
	mu.Unlock()

	var payload map[string]interface{}
	if err := json.Unmarshal(bodySnapshot, &payload); err != nil {
		t.Fatalf("invalid JSON payload: %v", err)
	}

	rm, ok := payload["resourceMetrics"].([]interface{})
	if !ok || len(rm) == 0 {
		t.Fatal("expected resourceMetrics array")
	}

	rm0, ok := rm[0].(map[string]interface{})
	if !ok {
		t.Fatal("expected resourceMetrics[0] to be object")
	}

	// Check resource attributes
	res, ok := rm0["resource"].(map[string]interface{})
	if !ok {
		t.Fatal("expected resource object")
	}
	attrs, ok := res["attributes"].([]interface{})
	if !ok || len(attrs) == 0 {
		t.Fatal("expected resource attributes")
	}

	// Check scopeMetrics
	sm, ok := rm0["scopeMetrics"].([]interface{})
	if !ok || len(sm) == 0 {
		t.Fatal("expected scopeMetrics array")
	}
}

func TestOTLPPusher_CustomHeaders(t *testing.T) {
	var mu sync.Mutex
	var receivedAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := NewMetrics()
	pusher := NewOTLPPusher(OTLPConfig{
		Endpoint: srv.URL,
		Interval: 50 * time.Millisecond,
		Headers:  map[string]string{"Authorization": "Bearer test-token"},
	}, m)
	pusher.Start()
	defer pusher.Stop()

	time.Sleep(120 * time.Millisecond)

	mu.Lock()
	auth := receivedAuth
	mu.Unlock()
	if auth != "Bearer test-token" {
		t.Errorf("expected auth header, got %q", auth)
	}
}

func TestOTLPPusher_HandlesErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	m := NewMetrics()
	pusher := NewOTLPPusher(OTLPConfig{
		Endpoint: srv.URL,
		Interval: 50 * time.Millisecond,
	}, m)
	pusher.Start()
	defer pusher.Stop()

	// Should not panic on errors
	time.Sleep(120 * time.Millisecond)
}

func TestOTLPPusher_BuildPayload(t *testing.T) {
	m := NewMetrics()
	m.RecordCacheHit()
	m.RecordCacheHit()
	m.RecordCacheMiss()
	m.RecordTranslation()
	m.RecordRequest("query_range", 200, 100*time.Millisecond)

	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m)
	payload := pusher.buildPayload()

	// Verify it marshals to valid JSON
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	if len(data) < 100 {
		t.Errorf("payload too small (%d bytes), expected meaningful content", len(data))
	}
}
