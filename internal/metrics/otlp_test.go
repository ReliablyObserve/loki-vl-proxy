package metrics

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
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

func TestOTLPPusher_SpecializedMetricFamilies(t *testing.T) {
	m := NewMetrics()
	m.RecordClientError("query_range", "timeout")
	m.RecordClientError("query_range", "timeout")
	m.RecordEndpointCacheHit("query_range")
	m.RecordEndpointCacheMiss("query")
	m.RecordBackendDuration("query_range", 25*time.Millisecond)
	m.RecordQueryRangeWindowFetchDuration(20 * time.Millisecond)
	m.RecordQueryRangeWindowMergeDuration(5 * time.Millisecond)
	m.RecordQueryRangeWindowCount(4)
	m.SetCircuitBreakerFunc(func() string { return "half-open" })

	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m)
	now := time.Now().UnixNano()

	clientErrors := pusher.clientErrorMetrics(now)
	if len(clientErrors) != 1 {
		t.Fatalf("expected one client error metric family, got %d", len(clientErrors))
	}
	points := metricPointsByType(t, clientErrors[0], "sum")
	if len(points) != 1 {
		t.Fatalf("expected one client error datapoint, got %d", len(points))
	}
	point0, ok := points[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected client error datapoint map, got %#v", points[0])
	}
	attrs := flattenAnyAttrs(t, point0["attributes"])
	if attrs["endpoint"] != "query_range" || attrs["reason"] != "timeout" {
		t.Fatalf("unexpected client error attrs: %#v", attrs)
	}

	endpointFamilies := append(pusher.endpointCacheMetrics(now), pusher.backendDurationMetrics(now)...)
	endpointFamilies = append(endpointFamilies, pusher.queryRangeWindowMetrics(now)...)
	names := metricNamesFromMaps(endpointFamilies)
	for _, required := range []string{
		"loki_vl_proxy_cache_hits_by_endpoint",
		"loki_vl_proxy_cache_misses_by_endpoint",
		"loki_vl_proxy_backend_duration_seconds",
		"loki_vl_proxy_window_fetch_seconds",
		"loki_vl_proxy_window_merge_seconds",
		"loki_vl_proxy_window_count",
		"loki_vl_proxy_window_adaptive_parallel_current",
		"loki_vl_proxy_window_adaptive_latency_ewma_seconds",
		"loki_vl_proxy_window_adaptive_error_ewma",
	} {
		if !names[required] {
			t.Fatalf("expected %s in specialized families, names=%v", required, names)
		}
	}

	cb := pusher.circuitBreakerMetric(now)
	if cb == nil {
		t.Fatal("expected circuit breaker metric")
	}
	cbPoints := metricPointsByType(t, cb, "gauge")
	if len(cbPoints) != 1 {
		t.Fatalf("expected one circuit breaker datapoint, got %d", len(cbPoints))
	}
	cbPoint, ok := cbPoints[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected circuit breaker datapoint map, got %#v", cbPoints[0])
	}
	if got := cbPoint["asDouble"]; got != float64(2) {
		t.Fatalf("expected half-open circuit breaker value 2, got %#v", got)
	}
}

func TestOTLPPusher_CircuitBreakerMetricStates(t *testing.T) {
	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, NewMetrics())
	now := time.Now().UnixNano()

	if metric := pusher.circuitBreakerMetric(now); metric != nil {
		t.Fatal("expected nil metric when no callback is configured")
	}

	cases := map[string]float64{
		"closed":    0,
		"open":      1,
		"half_open": 2,
	}
	for state, want := range cases {
		t.Run(state, func(t *testing.T) {
			pusher.metrics.SetCircuitBreakerFunc(func() string { return state })
			metric := pusher.circuitBreakerMetric(now)
			points := metricPointsByType(t, metric, "gauge")
			point0, ok := points[0].(map[string]interface{})
			if !ok {
				t.Fatalf("expected circuit breaker datapoint map, got %#v", points[0])
			}
			if got := point0["asDouble"]; got != want {
				t.Fatalf("circuit breaker state %q encoded as %#v, want %v", state, got, want)
			}
		})
	}
}

func TestOTLPPusher_SystemMetrics(t *testing.T) {
	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, NewMetrics())
	names := metricNamesFromMaps(pusher.systemMetrics(time.Now().UnixNano()))
	if !names["process_resident_memory_bytes"] {
		t.Fatalf("expected process_resident_memory_bytes, names=%v", names)
	}
	if runtime.GOOS == "linux" {
		for _, required := range []string{
			"process_disk_read_bytes_total",
			"process_disk_written_bytes_total",
			"process_network_receive_bytes_total",
			"process_network_transmit_bytes_total",
		} {
			if !names[required] {
				t.Fatalf("expected linux system metric %s, names=%v", required, names)
			}
		}
	}
}

func TestOTLPPusher_SystemMetrics_WithSyntheticProcRoot(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-only synthetic /proc test")
	}

	root := t.TempDir()
	for rel, content := range map[string]string{
		"stat":            "cpu  100 5 20 300 7 0 1 2\n",
		"meminfo":         "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemFree: 128 kB\n",
		"self/status":     "Name:\tproxy\nVmRSS:\t123 kB\n",
		"diskstats":       "   8       0 sda 1 2 6 4 5 6 10 8 0 0 0 0\n",
		"net/dev":         "Inter-|   Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 101 1 0 0 0 0 0 0 202 2 0 0 0 0 0 0\n",
		"pressure/cpu":    "some avg10=1.00 avg60=2.00 avg300=3.00 total=10\nfull avg10=4.00 avg60=5.00 avg300=6.00 total=20\n",
		"pressure/memory": "some avg10=0.50 avg60=1.00 avg300=1.50 total=10\nfull avg10=2.00 avg60=2.50 avg300=3.00 total=20\n",
		"pressure/io":     "some avg10=0.25 avg60=0.50 avg300=0.75 total=10\nfull avg10=1.00 avg60=1.25 avg300=1.50 total=20\n",
		"self/fd/0":       "",
		"self/fd/1":       "",
	} {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	prev := ProcRoot()
	SetProcRoot(root)
	t.Cleanup(func() { SetProcRoot(prev) })

	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, NewMetrics())
	names := metricNamesFromMaps(pusher.systemMetrics(time.Now().UnixNano()))
	for _, required := range []string{
		"process_memory_total_bytes",
		"process_memory_available_bytes",
		"process_memory_free_bytes",
		"process_memory_usage_ratio",
		"process_cpu_usage_ratio",
		"process_pressure_cpu_some_ratio",
		"process_pressure_memory_full_ratio",
		"process_pressure_io_some_ratio",
		"process_resident_memory_bytes",
		"process_open_fds",
	} {
		if !names[required] {
			t.Fatalf("expected synthetic proc export to include %s, names=%v", required, names)
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

func flattenAnyAttrs(t *testing.T, raw interface{}) map[string]string {
	t.Helper()
	switch attrs := raw.(type) {
	case []interface{}:
		return flattenAttrs(t, attrs)
	case []map[string]interface{}:
		items := make([]interface{}, 0, len(attrs))
		for _, attr := range attrs {
			items = append(items, attr)
		}
		return flattenAttrs(t, items)
	default:
		t.Fatalf("unsupported attribute shape: %#v", raw)
		return nil
	}
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

func metricNamesFromMaps(metrics []map[string]interface{}) map[string]bool {
	names := make(map[string]bool, len(metrics))
	for _, metric := range metrics {
		name, _ := metric["name"].(string)
		if strings.TrimSpace(name) != "" {
			names[name] = true
		}
	}
	return names
}

func metricPointsByType(t *testing.T, metric map[string]interface{}, typ string) []interface{} {
	t.Helper()
	body, ok := metric[typ].(map[string]interface{})
	if !ok {
		t.Fatalf("metric %q missing %s body: %#v", metric["name"], typ, metric)
	}
	switch points := body["dataPoints"].(type) {
	case []interface{}:
		return points
	case []map[string]interface{}:
		out := make([]interface{}, 0, len(points))
		for _, point := range points {
			out = append(out, point)
		}
		return out
	default:
		t.Fatalf("metric %q missing datapoints: %#v", metric["name"], body)
		return nil
	}
}
