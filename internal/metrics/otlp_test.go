package metrics

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOTLPPusher_SendsMetrics(t *testing.T) {
	var received atomic.Int32
	var mu sync.Mutex
	var lastBody []byte
	var lastPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		lastBody = body
		lastPath = r.URL.Path
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
		Endpoint:              srv.URL,
		Interval:              50 * time.Millisecond,
		ServiceName:           "loki-vl-proxy",
		ServiceNamespace:      "platform",
		ServiceVersion:        "1.2.3",
		ServiceInstanceID:     "proxy-1",
		DeploymentEnvironment: "prod",
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
	pathSnapshot := lastPath
	mu.Unlock()

	if pathSnapshot != "/v1/metrics" {
		t.Fatalf("expected normalized OTLP metrics path, got %q", pathSnapshot)
	}

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
	attrMap := flattenAttrs(t, attrs)
	if attrMap["service.name"] != "loki-vl-proxy" {
		t.Fatalf("expected service.name resource attr, got %#v", attrMap["service.name"])
	}
	if attrMap["service.namespace"] != "platform" {
		t.Fatalf("expected service.namespace resource attr, got %#v", attrMap["service.namespace"])
	}
	if attrMap["service.version"] != "1.2.3" {
		t.Fatalf("expected service.version resource attr, got %#v", attrMap["service.version"])
	}

	// Check scopeMetrics
	sm, ok := rm0["scopeMetrics"].([]interface{})
	if !ok || len(sm) == 0 {
		t.Fatal("expected scopeMetrics array")
	}
	sm0, ok := sm[0].(map[string]interface{})
	if !ok {
		t.Fatal("expected scopeMetrics[0] object")
	}
	metrics, ok := sm0["metrics"].([]interface{})
	if !ok || len(metrics) == 0 {
		t.Fatal("expected otlp metrics list")
	}
	names := metricNames(metrics)
	for _, required := range []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_request_duration_seconds",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_uptime_seconds",
		"go_goroutines",
	} {
		if !names[required] {
			t.Fatalf("expected OTLP payload to include %s; names=%v", required, names)
		}
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
	m.RecordTenantRequest("team-a", "query_range", 200, 100*time.Millisecond)
	m.RecordClientIdentity("grafana-user", "query_range", 50*time.Millisecond, 128)
	m.RecordClientStatus("grafana-user", "query_range", 200)
	m.RecordClientInflight("grafana-user", 1)
	m.RecordClientQueryLength("grafana-user", "query_range", 42)

	pusher := NewOTLPPusher(OTLPConfig{
		Endpoint:              "http://unused",
		ServiceName:           "proxy",
		ServiceVersion:        "9.9.9",
		DeploymentEnvironment: "test",
	}, m)
	payload := pusher.buildPayload()

	// Verify it marshals to valid JSON
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	if len(data) < 100 {
		t.Errorf("payload too small (%d bytes), expected meaningful content", len(data))
	}

	rm := payload["resourceMetrics"].([]map[string]interface{})
	scopeMetrics := rm[0]["scopeMetrics"].([]map[string]interface{})
	metricsList := scopeMetrics[0]["metrics"].([]map[string]interface{})
	foundClientHistogram := false
	foundTenantCounter := false
	for _, metric := range metricsList {
		switch metric["name"] {
		case "loki_vl_proxy_client_query_length_chars":
			foundClientHistogram = true
		case "loki_vl_proxy_tenant_requests_total":
			foundTenantCounter = true
		}
	}
	if !foundClientHistogram || !foundTenantCounter {
		t.Fatalf("expected richer OTLP payload, foundClientHistogram=%v foundTenantCounter=%v", foundClientHistogram, foundTenantCounter)
	}
}

func TestOTLPPusher_GzipCompression(t *testing.T) {
	var mu sync.Mutex
	var receivedEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedEncoding = r.Header.Get("Content-Encoding")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := NewMetrics()
	pusher := NewOTLPPusher(OTLPConfig{
		Endpoint:    srv.URL,
		Interval:    50 * time.Millisecond,
		Compression: OTLPCompressionGzip,
	}, m)
	pusher.Start()
	defer pusher.Stop()

	time.Sleep(120 * time.Millisecond)

	mu.Lock()
	enc := receivedEncoding
	mu.Unlock()
	if enc != "gzip" {
		t.Errorf("expected Content-Encoding: gzip, got %q", enc)
	}
}

func TestOTLPPusher_ZstdCompression(t *testing.T) {
	var mu sync.Mutex
	var receivedEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedEncoding = r.Header.Get("Content-Encoding")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := NewMetrics()
	pusher := NewOTLPPusher(OTLPConfig{
		Endpoint:    srv.URL,
		Interval:    50 * time.Millisecond,
		Compression: OTLPCompressionZstd,
	}, m)
	pusher.Start()
	defer pusher.Stop()

	time.Sleep(120 * time.Millisecond)

	mu.Lock()
	enc := receivedEncoding
	mu.Unlock()
	if enc != "zstd" {
		t.Errorf("expected Content-Encoding: zstd, got %q", enc)
	}
}

func TestNormalizeMetricsEndpoint(t *testing.T) {
	cases := map[string]string{
		"http://collector:4318":             "http://collector:4318/v1/metrics",
		"http://collector:4318/":            "http://collector:4318/v1/metrics",
		"http://collector:4318/v1":          "http://collector:4318/v1/metrics",
		"http://collector:4318/v1/metrics":  "http://collector:4318/v1/metrics",
		"http://collector:4318/custom/path": "http://collector:4318/custom/path",
	}
	for in, want := range cases {
		if got := normalizeMetricsEndpoint(in); got != want {
			t.Fatalf("normalizeMetricsEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

func flattenAttrs(t *testing.T, attrs []interface{}) map[string]string {
	t.Helper()
	flat := make(map[string]string, len(attrs))
	for _, raw := range attrs {
		attrMap, ok := raw.(map[string]interface{})
		if !ok {
			t.Fatalf("attribute is not object: %#v", raw)
		}
		key, _ := attrMap["key"].(string)
		valueMap, ok := attrMap["value"].(map[string]interface{})
		if !ok {
			t.Fatalf("attribute value is not object: %#v", raw)
		}
		for _, value := range valueMap {
			if s, ok := value.(string); ok {
				flat[key] = s
			}
		}
	}
	return flat
}

func metricNames(metrics []interface{}) map[string]bool {
	names := make(map[string]bool, len(metrics))
	for _, raw := range metrics {
		metric, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := metric["name"].(string)
		if strings.TrimSpace(name) != "" {
			names[name] = true
		}
	}
	return names
}
