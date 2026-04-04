//go:build e2e

// Package e2e_compat runs side-by-side compatibility tests comparing
// Loki-VL-proxy responses against real Loki responses for identical log data.
//
// Prerequisites:
//   docker-compose up -d --build  (from this directory)
//   wait ~15s for services to start
//
// Run:
//   go test -v -tags=e2e -timeout=120s ./test/e2e-compat/
//
// The test suite:
// 1. Ingests identical logs into both Loki and VictoriaLogs
// 2. Calls every Loki API endpoint on both real Loki and the proxy
// 3. Compares response structures and content
// 4. Calculates a compatibility score
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

var (
	lokiURL  = envOr("LOKI_URL", "http://localhost:3101")
	proxyURL = envOr("PROXY_URL", "http://localhost:3100")
	vlURL    = envOr("VL_URL", "http://localhost:9428")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// CompatScore tracks pass/fail per endpoint for final scoring.
type CompatScore struct {
	total       int
	passed      int
	failed      int
	secureDiffs int
	details     []string
}

func (cs *CompatScore) pass(endpoint, detail string) {
	cs.total++
	cs.passed++
	cs.details = append(cs.details, fmt.Sprintf("PASS %s: %s", endpoint, detail))
}

func (cs *CompatScore) fail(endpoint, detail string) {
	cs.total++
	cs.failed++
	cs.details = append(cs.details, fmt.Sprintf("FAIL %s: %s", endpoint, detail))
}

func (cs *CompatScore) secure(endpoint, detail string) {
	cs.secureDiffs++
	cs.details = append(cs.details, fmt.Sprintf("SECURE %s: %s", endpoint, detail))
}

func (cs *CompatScore) report(t *testing.T) {
	t.Helper()
	t.Logf("\n=== COMPATIBILITY SCORE ===")
	for _, d := range cs.details {
		t.Log(d)
	}
	if cs.total == 0 {
		t.Logf("\nScore: 0/0 (100.0%%)")
		if cs.secureDiffs > 0 {
			t.Logf("Secure differences: %d", cs.secureDiffs)
		}
		t.Logf("===========================")
		return
	}
	pct := float64(cs.passed) / float64(cs.total) * 100
	t.Logf("\nScore: %d/%d (%.1f%%)", cs.passed, cs.total, pct)
	if cs.secureDiffs > 0 {
		t.Logf("Secure differences: %d", cs.secureDiffs)
	}
	t.Logf("===========================")
}

// --- Test Setup: Ingest identical logs into both Loki and VL ---

func TestSetup_IngestLogs(t *testing.T) {
	now := time.Now()

	// Wait for services
	waitForReady(t, lokiURL+"/ready", 30*time.Second)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	// Push to Loki
	pushToLoki(t, now, []logLine{
		{Msg: "GET /api/v1/users 200 15ms", Level: "info"},
		{Msg: "POST /api/v1/orders 201 42ms", Level: "info"},
		{Msg: "error: connection refused to payment-service", Level: "error"},
		{Msg: "warn: high latency detected on database query", Level: "warn"},
		{Msg: "GET /health 200 1ms", Level: "info"},
		{Msg: `{"method":"GET","path":"/api","status":200,"duration":15}`, Level: "info"},
	})

	// Push to VictoriaLogs
	pushToVL(t, now, []logLine{
		{Msg: "GET /api/v1/users 200 15ms", Level: "info"},
		{Msg: "POST /api/v1/orders 201 42ms", Level: "info"},
		{Msg: "error: connection refused to payment-service", Level: "error"},
		{Msg: "warn: high latency detected on database query", Level: "warn"},
		{Msg: "GET /health 200 1ms", Level: "info"},
		{Msg: `{"method":"GET","path":"/api","status":200,"duration":15}`, Level: "info"},
	})

	// Wait for indexing
	time.Sleep(3 * time.Second)
}

// --- Side-by-side comparison tests ---

func TestCompat_Labels(t *testing.T) {
	score := &CompatScore{}

	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/labels")
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/labels")

	// Both must return status=success
	if checkStatus(lokiResp) && checkStatus(proxyResp) {
		score.pass("labels", "both return status=success")
	} else {
		score.fail("labels", "status mismatch")
	}

	// Both must return data as string array
	lokiLabels := extractStrings(lokiResp, "data")
	proxyLabels := extractStrings(proxyResp, "data")

	if lokiLabels != nil && proxyLabels != nil {
		score.pass("labels", "both return string array")
	} else {
		score.fail("labels", "data format mismatch")
	}

	// Proxy must include at least the labels we ingested
	for _, label := range []string{"app", "level"} {
		if contains(proxyLabels, label) {
			score.pass("labels", fmt.Sprintf("proxy returns label %q", label))
		} else {
			score.fail("labels", fmt.Sprintf("proxy missing label %q", label))
		}
	}

	score.report(t)
}

func TestCompat_LabelValues(t *testing.T) {
	score := &CompatScore{}

	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/label/app/values")
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/app/values")

	if checkStatus(lokiResp) && checkStatus(proxyResp) {
		score.pass("label_values", "both return status=success")
	} else {
		score.fail("label_values", "status mismatch")
	}

	lokiValues := extractStrings(lokiResp, "data")
	proxyValues := extractStrings(proxyResp, "data")

	// Check for a commonly ingested app (e2e-test or api-gateway)
	found := contains(proxyValues, "e2e-test") || contains(proxyValues, "api-gateway")
	if found {
		score.pass("label_values", "proxy returns ingested app value")
	} else {
		score.fail("label_values", fmt.Sprintf("proxy missing app values, got: %v", proxyValues))
	}

	t.Logf("Loki values: %v", lokiValues)
	t.Logf("Proxy values: %v", proxyValues)
	score.report(t)
}

func TestCompat_QueryRange_LogQuery(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="e2e-test"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode())
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	// Status check
	if checkStatus(lokiResp) && checkStatus(proxyResp) {
		score.pass("query_range", "both return status=success")
	} else {
		score.fail("query_range", "status mismatch")
	}

	// resultType check
	lokiData := extractMap(lokiResp, "data")
	proxyData := extractMap(proxyResp, "data")

	if proxyData != nil && proxyData["resultType"] == "streams" {
		score.pass("query_range", "proxy returns resultType=streams")
	} else {
		score.fail("query_range", fmt.Sprintf("expected resultType=streams, got %v", proxyData["resultType"]))
	}

	// Result structure check
	lokiResult := extractArray(lokiData, "result")
	proxyResult := extractArray(proxyData, "result")

	if lokiResult != nil && proxyResult != nil && len(proxyResult) > 0 {
		score.pass("query_range", "both return non-empty results")
	} else {
		score.fail("query_range", fmt.Sprintf("loki=%d results, proxy=%d results", len(lokiResult), len(proxyResult)))
	}

	// Verify stream entry structure
	if len(proxyResult) > 0 {
		entry, ok := proxyResult[0].(map[string]interface{})
		if ok {
			if _, hasStream := entry["stream"]; hasStream {
				score.pass("query_range", "proxy result has 'stream' field")
			} else {
				score.fail("query_range", "proxy result missing 'stream'")
			}
			if _, hasValues := entry["values"]; hasValues {
				score.pass("query_range", "proxy result has 'values' field")
			} else {
				score.fail("query_range", "proxy result missing 'values'")
			}
		}
	}

	// Stats field
	if proxyData != nil {
		if _, hasStats := proxyData["stats"]; hasStats {
			score.pass("query_range", "proxy includes 'stats' object")
		} else {
			score.fail("query_range", "proxy missing 'stats' object")
		}
	}

	score.report(t)
}

func TestCompat_QueryRange_WithFilter(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="e2e-test"} |= "error"`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode())
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	lokiLines := countLogLines(lokiResp)
	proxyLines := countLogLines(proxyResp)

	t.Logf("Filter |= 'error': Loki=%d lines, Proxy=%d lines", lokiLines, proxyLines)

	if proxyLines > 0 {
		score.pass("query_range_filter", "proxy returns filtered results")
	} else {
		score.fail("query_range_filter", "proxy returned 0 lines for error filter")
	}

	if lokiLines > 0 && proxyLines > 0 {
		score.pass("query_range_filter", "both return matching lines")
	}

	score.report(t)
}

func TestCompat_Series(t *testing.T) {
	score := &CompatScore{}
	params := url.Values{}
	params.Set("match[]", `{app="e2e-test"}`)

	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/series?"+params.Encode())
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/series?"+params.Encode())

	if checkStatus(lokiResp) && checkStatus(proxyResp) {
		score.pass("series", "both return status=success")
	} else {
		score.fail("series", "status mismatch")
	}

	lokiData := extractArray(lokiResp, "data")
	proxyData := extractArray(proxyResp, "data")

	t.Logf("Series: Loki=%d, Proxy=%d", len(lokiData), len(proxyData))

	if len(proxyData) > 0 {
		score.pass("series", "proxy returns series data")
		// Check that series entries are label maps
		entry, ok := proxyData[0].(map[string]interface{})
		if ok && len(entry) > 0 {
			score.pass("series", "proxy series entries are label maps")
		}
	} else {
		score.fail("series", "proxy returned no series")
	}

	score.report(t)
}

func TestCompat_BuildInfo(t *testing.T) {
	score := &CompatScore{}

	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/status/buildinfo")
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/status/buildinfo")

	if checkStatus(proxyResp) {
		score.pass("buildinfo", "proxy returns status=success")
	} else {
		score.fail("buildinfo", "proxy status not success")
	}

	proxyData := extractMap(proxyResp, "data")
	if proxyData != nil && proxyData["version"] != nil {
		score.pass("buildinfo", "proxy returns version field")
	} else {
		score.fail("buildinfo", "proxy missing version")
	}

	_ = lokiResp
	score.report(t)
}

func TestCompat_Ready(t *testing.T) {
	score := &CompatScore{}

	lokiCode := getStatusCode(t, lokiURL+"/ready")
	proxyCode := getStatusCode(t, proxyURL+"/ready")

	if lokiCode == 200 {
		score.pass("ready", "loki returns 200")
	}
	if proxyCode == 200 {
		score.pass("ready", "proxy returns 200")
	} else {
		score.fail("ready", fmt.Sprintf("proxy returned %d", proxyCode))
	}

	score.report(t)
}

func TestCompat_DetectedFields(t *testing.T) {
	score := &CompatScore{}

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/detected_fields?query=%7Bapp%3D%22e2e-test%22%7D")

	if proxyResp != nil {
		if fields, ok := proxyResp["fields"].([]interface{}); ok {
			score.pass("detected_fields", fmt.Sprintf("proxy returns %d fields", len(fields)))
		} else {
			score.fail("detected_fields", "proxy missing fields array")
		}
	} else {
		score.fail("detected_fields", "proxy returned nil")
	}

	score.report(t)
}

func TestCompat_Metrics(t *testing.T) {
	score := &CompatScore{}

	resp, err := http.Get(proxyURL + "/metrics")
	if err != nil {
		score.fail("metrics", err.Error())
		score.report(t)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	metrics := []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_uptime_seconds",
	}
	for _, m := range metrics {
		if strings.Contains(text, m) {
			score.pass("metrics", fmt.Sprintf("has %s", m))
		} else {
			score.fail("metrics", fmt.Sprintf("missing %s", m))
		}
	}

	score.report(t)
}

// TestCompat_FinalScore aggregates all results into a summary.
func TestCompat_FinalScore(t *testing.T) {
	t.Log("\n" + strings.Repeat("=", 60))
	t.Log("E2E COMPATIBILITY TEST COMPLETE")
	t.Log("Compare Grafana side-by-side at http://localhost:3000")
	t.Log("  - 'Loki (direct)' = real Loki reference")
	t.Log("  - 'Loki (via VL proxy)' = VictoriaLogs via proxy")
	t.Log("  - 'VictoriaLogs (direct)' = native VL datasource")
	t.Log(strings.Repeat("=", 60))
}

// =============================================================================
// Helpers
// =============================================================================

type logLine struct {
	Msg   string
	Level string
}

func pushToLoki(t *testing.T, baseTime time.Time, lines []logLine) {
	t.Helper()
	values := make([][]string, len(lines))
	for i, l := range lines {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		values[i] = []string{fmt.Sprintf("%d", ts.UnixNano()), l.Msg}
	}

	payload := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": map[string]string{"app": "e2e-test", "env": "test", "level": lines[0].Level},
				"values": values,
			},
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Logf("Loki push failed: %v", err)
		return
	}
	resp.Body.Close()
	t.Logf("Loki push: %d", resp.StatusCode)
}

func pushToVL(t *testing.T, baseTime time.Time, lines []logLine) {
	t.Helper()
	var vlLines []string
	for i, l := range lines {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		entry := map[string]string{
			"_time": ts.Format(time.RFC3339Nano),
			"_msg":  l.Msg,
			"app":   "e2e-test",
			"env":   "test",
			"level": l.Level,
		}
		line, _ := json.Marshal(entry)
		vlLines = append(vlLines, string(line))
	}

	resp, err := http.Post(
		vlURL+"/insert/jsonline?_stream_fields=app,env,level",
		"application/stream+json",
		strings.NewReader(strings.Join(vlLines, "\n")),
	)
	if err != nil {
		t.Fatalf("VL push failed: %v", err)
	}
	resp.Body.Close()
	t.Logf("VL push: %d", resp.StatusCode)
}

func waitForReady(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timeout waiting for %s", url)
}

func getJSON(t *testing.T, url string) map[string]interface{} {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Logf("GET %s failed: %v", url, err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result
}

func getStatusCode(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func checkStatus(resp map[string]interface{}) bool {
	if resp == nil {
		return false
	}
	return resp["status"] == "success"
}

func extractStrings(resp map[string]interface{}, key string) []string {
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

func extractMap(resp map[string]interface{}, key string) map[string]interface{} {
	if resp == nil {
		return nil
	}
	m, _ := resp[key].(map[string]interface{})
	return m
}

func extractArray(resp map[string]interface{}, key string) []interface{} {
	if resp == nil {
		return nil
	}
	arr, _ := resp[key].([]interface{})
	return arr
}

func countLogLines(resp map[string]interface{}) int {
	data := extractMap(resp, "data")
	result := extractArray(data, "result")
	total := 0
	for _, r := range result {
		stream, _ := r.(map[string]interface{})
		values, _ := stream["values"].([]interface{})
		total += len(values)
	}
	return total
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
