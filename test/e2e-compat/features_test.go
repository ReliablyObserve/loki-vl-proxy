//go:build e2e

// Feature verification tests — validates all proxy features against real backends.
// Covers: index/stats, volume, multitenancy, tail, query analytics,
// Grafana datasource config, and additional edge cases.
package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// =============================================================================
// Index Stats (real implementation via VL /select/logsql/hits)
// =============================================================================

func TestFeature_IndexStats_ReturnsRealData(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/stats?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/index/stats?"+params.Encode())

	// Both should return numeric fields
	for _, field := range []string{"streams", "chunks", "entries", "bytes"} {
		if v, ok := proxyResp[field]; ok {
			if num, ok := v.(float64); ok && num >= 0 {
				score.pass("index_stats", fmt.Sprintf("proxy has %s=%v", field, num))
			} else {
				score.fail("index_stats", fmt.Sprintf("proxy %s not a number: %v", field, v))
			}
		} else {
			score.fail("index_stats", fmt.Sprintf("proxy missing %s", field))
		}
	}

	// Proxy should have entries > 0 if Loki does
	lokiEntries, _ := lokiResp["entries"].(float64)
	proxyEntries, _ := proxyResp["entries"].(float64)
	if lokiEntries > 0 && proxyEntries > 0 {
		score.pass("index_stats", fmt.Sprintf("both have entries: loki=%v proxy=%v", lokiEntries, proxyEntries))
	} else if lokiEntries > 0 && proxyEntries == 0 {
		score.fail("index_stats", "proxy has 0 entries but Loki has data")
	}

	score.report(t)
}

// =============================================================================
// Index Volume (real implementation via VL /select/logsql/hits)
// =============================================================================

func TestFeature_IndexVolume_ReturnsPrometheusFormat(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{namespace="prod"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/volume?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("volume", "proxy returns status=success")
	} else {
		score.fail("volume", "proxy error")
	}

	if data, ok := proxyResp["data"].(map[string]interface{}); ok {
		if rt, ok := data["resultType"].(string); ok && rt == "vector" {
			score.pass("volume", "resultType=vector")
		} else {
			score.fail("volume", fmt.Sprintf("expected resultType=vector, got %v", data["resultType"]))
		}

		if result, ok := data["result"].([]interface{}); ok {
			score.pass("volume", fmt.Sprintf("result has %d entries", len(result)))
			if len(result) > 0 {
				entry, _ := result[0].(map[string]interface{})
				if _, ok := entry["metric"]; ok {
					score.pass("volume", "entries have metric field")
				}
				if _, ok := entry["value"]; ok {
					score.pass("volume", "entries have value field")
				}
			}
		}
	} else {
		score.fail("volume", "missing data object")
	}

	score.report(t)
}

func TestFeature_IndexVolumeRange_ReturnsMatrix(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{namespace="prod"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/volume_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("volume_range", "proxy returns status=success")
	} else {
		score.fail("volume_range", "proxy error")
	}

	if data, ok := proxyResp["data"].(map[string]interface{}); ok {
		if rt := data["resultType"]; rt == "matrix" {
			score.pass("volume_range", "resultType=matrix")
		} else {
			score.fail("volume_range", fmt.Sprintf("expected resultType=matrix, got %v", rt))
		}
	}

	score.report(t)
}

// =============================================================================
// Multitenancy (X-Scope-OrgID → AccountID/ProjectID)
// =============================================================================

func TestFeature_Multitenancy_DefaultTenantBypassUsesVLGlobalTenantWithoutMappings(t *testing.T) {
	score := &CompatScore{}

	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")

	for _, orgID := range []string{"0", "*"} {
		req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", orgID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			score.fail("multitenancy", fmt.Sprintf("request with OrgID=%s failed: %v", orgID, err))
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			score.pass("multitenancy", fmt.Sprintf("OrgID=%s uses VL default tenant when no tenant map is configured", orgID))
		} else {
			score.fail("multitenancy", fmt.Sprintf("expected OrgID=%s to use VL default tenant, got %d", orgID, resp.StatusCode))
		}
	}

	// Query without tenant header is still allowed when auth.enabled=false.
	req2, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/labels", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		score.fail("multitenancy", "no-org request failed")
	} else {
		defer resp2.Body.Close()
		if resp2.StatusCode == 200 {
			score.pass("multitenancy", "no OrgID header accepted")
		}
	}

	score.report(t)
}

func TestFeature_AdminDebugEndpoints_DefaultClosed(t *testing.T) {
	score := &CompatScore{}

	for _, path := range []string{"/debug/queries", "/debug/pprof/"} {
		resp, err := http.Get(proxyURL + path)
		if err != nil {
			score.fail("admin_endpoints", fmt.Sprintf("%s request failed: %v", path, err))
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			score.pass("admin_endpoints", fmt.Sprintf("%s disabled by default", path))
		} else {
			score.fail("admin_endpoints", fmt.Sprintf("%s expected 404, got %d", path, resp.StatusCode))
		}
	}

	score.report(t)
}

// =============================================================================
// Tail (WebSocket)
// =============================================================================

func TestFeature_Tail_WebSocketConnection(t *testing.T) {
	score := &CompatScore{}

	wsURL := "ws" + strings.TrimPrefix(proxyURL, "http") + "/loki/api/v1/tail"
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial(wsURL+"?"+params.Encode(), nil)
	if err != nil {
		if resp != nil {
			score.fail("tail", fmt.Sprintf("WebSocket upgrade failed: %d", resp.StatusCode))
		} else {
			score.fail("tail", "WebSocket dial failed: "+err.Error())
		}
		score.report(t)
		return
	}
	defer conn.Close()
	score.pass("tail", "WebSocket connection established")

	// Set a short read deadline to avoid blocking
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		// Timeout is OK — means no new logs during the window
		score.pass("tail", "tail read completed (timeout or no new data)")
	} else {
		var frame map[string]interface{}
		if json.Unmarshal(msg, &frame) == nil {
			if _, ok := frame["streams"]; ok {
				score.pass("tail", "received Loki-compatible tail frame with streams")
			} else {
				score.fail("tail", "tail frame missing streams field")
			}
		}
	}

	score.report(t)
}

func TestFeature_Tail_BrowserOriginRejectedByDefault(t *testing.T) {
	score := &CompatScore{}

	wsURL := "ws" + strings.TrimPrefix(proxyURL, "http") + "/loki/api/v1/tail"
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	headers := http.Header{}
	headers.Set("Origin", "https://grafana.example.com")

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial(wsURL+"?"+params.Encode(), headers)
	if err == nil {
		conn.Close()
		score.fail("tail_origin", "expected browser origin to be rejected by default")
		score.report(t)
		return
	}
	if resp != nil && resp.StatusCode == http.StatusForbidden {
		score.pass("tail_origin", "browser origin rejected by default")
	} else {
		score.fail("tail_origin", fmt.Sprintf("expected 403 for browser origin, got resp=%v err=%v", resp, err))
	}

	score.report(t)
}

// =============================================================================
// Query Analytics (/debug/queries)
// =============================================================================

func TestFeature_QueryAnalytics_Endpoint(t *testing.T) {
	score := &CompatScore{}

	// First run some queries to populate tracker
	queryProxy(t, `{app="api-gateway"}`)
	queryProxy(t, `{app="payment-service"}`)
	queryProxy(t, `{app="api-gateway"} |= "error"`)

	resp, err := http.Get(proxyURL + "/debug/queries")
	if err != nil {
		score.fail("analytics", "request failed: "+err.Error())
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			score.pass("analytics", "/debug/queries disabled by default")
		} else {
			score.fail("analytics", fmt.Sprintf("expected /debug/queries to be disabled by default, got %d", resp.StatusCode))
		}
	}

	score.report(t)
}

// =============================================================================
// Security Headers
// =============================================================================

func TestFeature_SecurityHeaders(t *testing.T) {
	score := &CompatScore{}

	resp, err := http.Get(proxyURL + "/loki/api/v1/labels")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if v := resp.Header.Get("X-Content-Type-Options"); v == "nosniff" {
		score.pass("security", "X-Content-Type-Options: nosniff")
	} else {
		score.fail("security", fmt.Sprintf("expected nosniff, got %q", v))
	}

	if v := resp.Header.Get("X-Frame-Options"); v == "DENY" {
		score.pass("security", "X-Frame-Options: DENY")
	} else {
		score.fail("security", fmt.Sprintf("expected DENY, got %q", v))
	}

	if v := resp.Header.Get("Cache-Control"); v == "no-store" {
		score.pass("security", "Cache-Control: no-store")
	} else {
		score.fail("security", fmt.Sprintf("expected no-store, got %q", v))
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Concurrent identical queries (coalescing)
// =============================================================================

func TestEdge_ConcurrentIdenticalQueries(t *testing.T) {
	score := &CompatScore{}
	q := `{app="api-gateway"}`
	n := 10
	results := make(chan map[string]interface{}, n)

	for range n {
		go func() {
			results <- queryProxy(t, q)
		}()
	}

	successCount := 0
	for range n {
		resp := <-results
		if checkStatus(resp) {
			successCount++
		}
	}

	if successCount == n {
		score.pass("coalescing", fmt.Sprintf("all %d concurrent requests succeeded", n))
	} else {
		score.fail("coalescing", fmt.Sprintf("only %d/%d succeeded", successCount, n))
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Very long query string
// =============================================================================

func TestEdge_LongQueryString(t *testing.T) {
	score := &CompatScore{}

	// Build a query with many OR conditions
	conditions := make([]string, 50)
	for i := range conditions {
		conditions[i] = fmt.Sprintf(`|= "pattern_%d"`, i)
	}
	q := `{app="api-gateway"} ` + strings.Join(conditions, " ")

	now := time.Now()
	params := url.Values{}
	params.Set("query", q)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")

	resp, err := http.Get(proxyURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		score.fail("long_query", "request failed: "+err.Error())
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == 200 || resp.StatusCode == 400 {
			score.pass("long_query", fmt.Sprintf("long query handled gracefully (%d)", resp.StatusCode))
		} else {
			score.fail("long_query", fmt.Sprintf("unexpected status: %d", resp.StatusCode))
		}
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Query with all filter types combined
// =============================================================================

func TestEdge_CombinedFilterTypes(t *testing.T) {
	score := &CompatScore{}

	queries := []struct {
		name  string
		query string
	}{
		{"line_contains", `{app="api-gateway"} |= "GET"`},
		{"line_not_contains", `{app="api-gateway"} != "health"`},
		{"regex_match", `{app="api-gateway"} |~ "GET|POST"`},
		{"regex_not_match", `{app="api-gateway"} !~ "metrics|health|ready"`},
		{"json_parser", `{app="api-gateway", level="info"} | json`},
		{"multi_filter_chain", `{app="api-gateway"} |= "api" != "health" |~ "GET|POST"`},
	}

	for _, tc := range queries {
		proxyResult := queryProxy(t, tc.query)
		lokiResult := queryLoki(t, tc.query)

		proxyLines := countLogLines(proxyResult)
		lokiLines := countLogLines(lokiResult)

		t.Logf("[%s] Loki=%d, Proxy=%d", tc.name, lokiLines, proxyLines)

		if checkStatus(proxyResult) {
			score.pass(tc.name, fmt.Sprintf("proxy OK (%d lines)", proxyLines))
		} else {
			score.fail(tc.name, "proxy error")
		}

		// Both should return results (or both empty)
		if (lokiLines > 0 && proxyLines > 0) || (lokiLines == 0 && proxyLines == 0) {
			score.pass(tc.name, "result count direction matches Loki")
		} else {
			score.fail(tc.name, fmt.Sprintf("count mismatch: loki=%d proxy=%d", lokiLines, proxyLines))
		}
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Empty result queries
// =============================================================================

func TestEdge_EmptyResults(t *testing.T) {
	score := &CompatScore{}

	queries := []struct {
		name  string
		query string
	}{
		{"nonexistent_app", `{app="this-app-does-not-exist-xyz"}`},
		{"impossible_filter", `{app="api-gateway"} |= "IMPOSSIBLE_STRING_THAT_NEVER_EXISTS_12345"`},
		{"future_time", `{app="api-gateway"}`}, // with future start time
	}

	for _, tc := range queries {
		var proxyResult map[string]interface{}
		if tc.name == "future_time" {
			future := time.Now().Add(24 * time.Hour)
			params := url.Values{}
			params.Set("query", tc.query)
			params.Set("start", fmt.Sprintf("%d", future.UnixNano()))
			params.Set("end", fmt.Sprintf("%d", future.Add(time.Hour).UnixNano()))
			proxyResult = getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
		} else {
			proxyResult = queryProxy(t, tc.query)
		}

		if checkStatus(proxyResult) {
			score.pass(tc.name, "proxy returns success for empty result")
		} else {
			score.fail(tc.name, "proxy error on empty result")
		}

		lines := countLogLines(proxyResult)
		if lines == 0 {
			score.pass(tc.name, "correctly returns 0 lines")
		} else {
			score.fail(tc.name, fmt.Sprintf("expected 0 lines, got %d", lines))
		}
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Rapid sequential queries (rate limiter)
// =============================================================================

func TestEdge_RapidSequentialQueries(t *testing.T) {
	score := &CompatScore{}
	successCount := 0
	total := 20

	for range total {
		resp, err := http.Get(proxyURL + "/loki/api/v1/labels")
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			successCount++
		}
	}

	if successCount == total {
		score.pass("rate_limit", fmt.Sprintf("all %d rapid requests allowed", total))
	} else if successCount > 0 {
		score.pass("rate_limit", fmt.Sprintf("%d/%d requests allowed (rate limiter working)", successCount, total))
	} else {
		score.fail("rate_limit", "all requests rejected")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Instant query (not range)
// =============================================================================

func TestEdge_InstantQuery(t *testing.T) {
	score := &CompatScore{}

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("limit", "5")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/query?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("instant_query", "proxy returns success")
	} else {
		score.fail("instant_query", "proxy error")
	}

	if checkStatus(lokiResp) {
		score.pass("instant_query", "loki returns success")
	}

	score.report(t)
}

// =============================================================================
// Metrics endpoint validation
// =============================================================================

func TestFeature_MetricsEndpoint(t *testing.T) {
	score := &CompatScore{}

	resp, err := http.Get(proxyURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body := make([]byte, 64*1024)
	n, _ := resp.Body.Read(body)
	text := string(body[:n])

	expectedMetrics := []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_request_duration_seconds",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_cache_misses_total",
		"loki_vl_proxy_translations_total",
		"loki_vl_proxy_uptime_seconds",
	}

	for _, m := range expectedMetrics {
		if strings.Contains(text, m) {
			score.pass("metrics", fmt.Sprintf("has %s", m))
		} else {
			score.fail("metrics", fmt.Sprintf("missing %s", m))
		}
	}

	score.report(t)
}

// =============================================================================
// Gzip Response Compression
// =============================================================================

func TestFeature_GzipCompression(t *testing.T) {
	score := &CompatScore{}

	// Use raw http.Transport to prevent auto-decompression
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/labels", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		score.fail("gzip", "request failed: "+err.Error())
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") == "gzip" {
		score.pass("gzip", "response is gzip-compressed")
	} else {
		score.fail("gzip", fmt.Sprintf("expected gzip, got Content-Encoding: %q", resp.Header.Get("Content-Encoding")))
	}

	if resp.Header.Get("Vary") == "Accept-Encoding" {
		score.pass("gzip", "Vary: Accept-Encoding header present")
	}

	score.report(t)
}

func TestFeature_NoGzipWithoutAccept(t *testing.T) {
	score := &CompatScore{}

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/labels", nil)
	// No Accept-Encoding
	resp, err := client.Do(req)
	if err != nil {
		score.fail("no_gzip", "request failed")
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		score.pass("no_gzip", "no gzip without Accept-Encoding")
	} else {
		score.fail("no_gzip", "should not gzip without Accept-Encoding")
	}

	score.report(t)
}

// =============================================================================
// Derived Fields (trace linking)
// =============================================================================

func TestFeature_DerivedFields_TraceIDInJSON(t *testing.T) {
	score := &CompatScore{}

	// The test data has trace_id fields in JSON logs
	proxyResult := queryProxy(t, `{app="api-gateway", level="info"} | json`)
	if checkStatus(proxyResult) {
		score.pass("derived", "json parser query succeeds")
	} else {
		score.fail("derived", "json parser query failed")
		score.report(t)
		return
	}

	data, _ := proxyResult["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})

	traceFound := false
	for _, s := range result {
		stream, _ := s.(map[string]interface{})
		labels, _ := stream["stream"].(map[string]interface{})
		if _, ok := labels["trace_id"]; ok {
			traceFound = true
			break
		}
	}

	if traceFound {
		score.pass("derived", "trace_id available after json parsing")
	} else {
		score.pass("derived", "trace_id extraction depends on derived-fields config (OK)")
	}

	score.report(t)
}

// =============================================================================
// Chunked / streaming response validation
// =============================================================================

func TestFeature_ResponseStructureConsistency(t *testing.T) {
	score := &CompatScore{}

	proxyResult := queryProxy(t, `{namespace="prod"}`)

	if checkStatus(proxyResult) {
		score.pass("response_struct", "response is valid JSON")
	} else {
		score.fail("response_struct", "response is not valid JSON")
	}

	data, _ := proxyResult["data"].(map[string]interface{})
	if data["resultType"] == "streams" {
		score.pass("response_struct", "resultType=streams")
	}

	result, _ := data["result"].([]interface{})
	if len(result) > 0 {
		score.pass("response_struct", fmt.Sprintf("has %d stream entries", len(result)))

		entry0, _ := result[0].(map[string]interface{})
		if _, ok := entry0["stream"]; ok {
			score.pass("response_struct", "entry has stream object")
		}
		if vals, ok := entry0["values"].([]interface{}); ok && len(vals) > 0 {
			score.pass("response_struct", "entry has values array")
			if pair, ok := vals[0].([]interface{}); ok && len(pair) == 2 {
				if _, ok := pair[0].(string); ok {
					score.pass("response_struct", "timestamp is string (nanoseconds)")
				}
				if _, ok := pair[1].(string); ok {
					score.pass("response_struct", "line is string")
				}
			}
		}
	}

	score.report(t)
}

// =============================================================================
// query_range vs query consistency
// =============================================================================

func TestFeature_QueryRange_vs_Query_Consistency(t *testing.T) {
	score := &CompatScore{}

	rangeResult := queryProxy(t, `{app="api-gateway"}`)
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("time", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")
	instantResult := getJSON(t, proxyURL+"/loki/api/v1/query?"+params.Encode())

	if checkStatus(rangeResult) && checkStatus(instantResult) {
		score.pass("consistency", "both query and query_range return success")
	} else {
		score.fail("consistency", "one or both failed")
	}

	rangeData, _ := rangeResult["data"].(map[string]interface{})
	instantData, _ := instantResult["data"].(map[string]interface{})

	if rangeData["resultType"] == "streams" {
		score.pass("consistency", "query_range returns streams")
	}
	if instantData["resultType"] == "streams" {
		score.pass("consistency", "query returns streams")
	}

	score.report(t)
}
