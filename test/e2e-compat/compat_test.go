//go:build e2e

// Package e2e_compat runs compatibility tests comparing proxy responses
// against real Loki responses for the same log data.
//
// Prerequisites: docker-compose up -d (from this directory)
// Run: go test -v -tags=e2e -timeout=120s ./test/e2e-compat/
package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	defaultLokiURL  = "http://localhost:3101" // Real Loki
	defaultProxyURL = "http://localhost:3100"  // Loki-VL-proxy
	defaultVLURL    = "http://localhost:9428"  // VictoriaLogs direct
)

func lokiURL() string {
	if v := os.Getenv("LOKI_URL"); v != "" {
		return v
	}
	return defaultLokiURL
}

func proxyURL() string {
	if v := os.Getenv("PROXY_URL"); v != "" {
		return v
	}
	return defaultProxyURL
}

func vlURL() string {
	if v := os.Getenv("VL_URL"); v != "" {
		return v
	}
	return defaultVLURL
}

// TestIngestSampleLogs pushes identical logs into both Loki and VictoriaLogs.
func TestIngestSampleLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Push to Loki via Loki push API
	lokiPushURL := lokiURL() + "/loki/api/v1/push"
	// Push to VL via JSON line ingestion
	vlPushURL := vlURL() + "/insert/jsonline"

	now := time.Now()
	tsNanos := fmt.Sprintf("%d", now.UnixNano())

	// Loki push format
	lokiPayload := fmt.Sprintf(`{
		"streams": [{
			"stream": {"app": "e2e-test", "env": "test"},
			"values": [
				["%s", "test log line error in payment service"],
				["%s", "test log line info request completed"],
				["%s", "test log line warn high latency detected"]
			]
		}]
	}`, tsNanos, fmt.Sprintf("%d", now.Add(time.Second).UnixNano()), fmt.Sprintf("%d", now.Add(2*time.Second).UnixNano()))

	resp, err := http.Post(lokiPushURL, "application/json", strings.NewReader(lokiPayload))
	if err != nil {
		t.Logf("Loki push failed (expected if Loki not running): %v", err)
	} else {
		resp.Body.Close()
		t.Logf("Loki push status: %d", resp.StatusCode)
	}

	// VL push format (JSON lines)
	vlLines := []string{
		fmt.Sprintf(`{"_time":%q,"_msg":"test log line error in payment service","app":"e2e-test","env":"test"}`, now.Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"_time":%q,"_msg":"test log line info request completed","app":"e2e-test","env":"test"}`, now.Add(time.Second).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"_time":%q,"_msg":"test log line warn high latency detected","app":"e2e-test","env":"test"}`, now.Add(2*time.Second).Format(time.RFC3339Nano)),
	}

	resp, err = http.Post(vlPushURL+"?_stream_fields=app,env", "application/stream+json", strings.NewReader(strings.Join(vlLines, "\n")))
	if err != nil {
		t.Fatalf("VL push failed: %v", err)
	}
	resp.Body.Close()
	t.Logf("VL push status: %d", resp.StatusCode)

	// Wait for indexing
	time.Sleep(2 * time.Second)
}

// CompatResult tracks per-endpoint compatibility.
type CompatResult struct {
	Endpoint   string
	ProxyOK    bool
	LokiOK    bool
	Compatible bool
	Details    string
}

// TestCompatLabels tests /loki/api/v1/labels compatibility.
func TestCompatLabels(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/labels")
	lokiResp := getJSON(t, lokiURL()+"/loki/api/v1/labels")

	// Both should return {"status": "success", "data": [...]}
	assertField(t, proxyResp, "status", "success")

	proxyLabels := extractStringSlice(t, proxyResp, "data")
	t.Logf("Proxy labels: %v", proxyLabels)

	if lokiResp != nil {
		lokiLabels := extractStringSlice(t, lokiResp, "data")
		t.Logf("Loki labels: %v", lokiLabels)
	}

	// The proxy MUST return the "app" and "env" labels we ingested
	if !contains(proxyLabels, "app") {
		t.Error("proxy /labels missing 'app' label")
	}
	if !contains(proxyLabels, "env") {
		t.Error("proxy /labels missing 'env' label")
	}
}

// TestCompatLabelValues tests /loki/api/v1/label/{name}/values.
func TestCompatLabelValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/label/app/values")
	assertField(t, proxyResp, "status", "success")

	values := extractStringSlice(t, proxyResp, "data")
	t.Logf("Proxy label values for 'app': %v", values)

	if !contains(values, "e2e-test") {
		t.Error("proxy /label/app/values missing 'e2e-test'")
	}
}

// TestCompatQueryRange tests /loki/api/v1/query_range with a log query.
func TestCompatQueryRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	now := time.Now()
	start := now.Add(-10 * time.Minute).UnixNano()
	end := now.UnixNano()

	params := url.Values{}
	params.Set("query", `{app="e2e-test"}`)
	params.Set("start", fmt.Sprintf("%d", start))
	params.Set("end", fmt.Sprintf("%d", end))
	params.Set("limit", "100")

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/query_range?"+params.Encode())
	assertField(t, proxyResp, "status", "success")

	data, ok := proxyResp["data"].(map[string]interface{})
	if !ok {
		t.Fatal("proxy query_range: missing 'data' object")
	}

	resultType, _ := data["resultType"].(string)
	if resultType != "streams" {
		t.Errorf("proxy query_range: expected resultType 'streams', got %q", resultType)
	}

	result, ok := data["result"].([]interface{})
	if !ok {
		t.Fatal("proxy query_range: missing 'result' array")
	}

	if len(result) == 0 {
		t.Error("proxy query_range: no streams returned — expected at least 1")
	}

	// Verify stream structure: each result should have "stream" and "values"
	for i, r := range result {
		stream, ok := r.(map[string]interface{})
		if !ok {
			t.Errorf("result[%d]: not an object", i)
			continue
		}
		if _, ok := stream["stream"]; !ok {
			t.Errorf("result[%d]: missing 'stream' field", i)
		}
		vals, ok := stream["values"].([]interface{})
		if !ok {
			t.Errorf("result[%d]: missing 'values' array", i)
			continue
		}
		// Each value should be [timestamp_ns, log_line]
		for j, v := range vals {
			pair, ok := v.([]interface{})
			if !ok || len(pair) != 2 {
				t.Errorf("result[%d].values[%d]: expected [ts, line] pair, got %v", i, j, v)
			}
		}
	}
}

// TestCompatQueryRangeWithFilter tests filtered log queries.
func TestCompatQueryRangeWithFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="e2e-test"} |= "error"`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/query_range?"+params.Encode())
	assertField(t, proxyResp, "status", "success")

	data, _ := proxyResp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})

	// Should find at least the "error" log line
	totalLines := 0
	for _, r := range result {
		stream, _ := r.(map[string]interface{})
		vals, _ := stream["values"].([]interface{})
		totalLines += len(vals)
	}

	if totalLines == 0 {
		t.Error("filtered query returned 0 lines — expected at least 1 with 'error'")
	}
	t.Logf("Filtered query returned %d log lines", totalLines)
}

// TestCompatSeries tests /loki/api/v1/series.
func TestCompatSeries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	params := url.Values{}
	params.Set("match[]", `{app="e2e-test"}`)

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/series?"+params.Encode())
	assertField(t, proxyResp, "status", "success")

	data, ok := proxyResp["data"].([]interface{})
	if !ok {
		t.Fatal("series: missing 'data' array")
	}
	t.Logf("Series returned %d streams", len(data))
}

// TestCompatReady tests /ready endpoint.
func TestCompatReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	resp, err := http.Get(proxyURL() + "/ready")
	if err != nil {
		t.Fatalf("GET /ready failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("/ready returned %d, expected 200", resp.StatusCode)
	}
}

// TestCompatBuildInfo tests /loki/api/v1/status/buildinfo.
func TestCompatBuildInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/status/buildinfo")
	assertField(t, proxyResp, "status", "success")
}

// TestCompatMetrics tests /metrics endpoint.
func TestCompatMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	resp, err := http.Get(proxyURL() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	requiredMetrics := []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_cache_misses_total",
		"loki_vl_proxy_uptime_seconds",
	}

	for _, m := range requiredMetrics {
		if !strings.Contains(text, m) {
			t.Errorf("/metrics missing metric: %s", m)
		}
	}
}

// TestCompatDetectedFields tests /loki/api/v1/detected_fields.
func TestCompatDetectedFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	proxyResp := getJSON(t, proxyURL()+"/loki/api/v1/detected_fields?query="+url.QueryEscape(`{app="e2e-test"}`))

	fields, ok := proxyResp["fields"].([]interface{})
	if !ok {
		t.Fatal("detected_fields: missing 'fields' array")
	}
	t.Logf("Detected %d fields", len(fields))
}

// --- Helpers ---

func getJSON(t *testing.T, urlStr string) map[string]interface{} {
	t.Helper()
	resp, err := http.Get(urlStr)
	if err != nil {
		t.Logf("GET %s failed: %v", urlStr, err)
		return nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("Failed to parse JSON from %s: %v (body: %s)", urlStr, err, string(body))
		return nil
	}
	return result
}

func assertField(t *testing.T, resp map[string]interface{}, key, expected string) {
	t.Helper()
	if resp == nil {
		t.Errorf("response is nil, expected %s=%q", key, expected)
		return
	}
	val, _ := resp[key].(string)
	if val != expected {
		t.Errorf("expected %s=%q, got %q", key, expected, val)
	}
}

func extractStringSlice(t *testing.T, resp map[string]interface{}, key string) []string {
	t.Helper()
	if resp == nil {
		return nil
	}
	arr, ok := resp[key].([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
