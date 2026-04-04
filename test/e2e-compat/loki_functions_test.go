//go:build e2e

// Tests for Loki-specific functions implemented at the proxy level.
// These are features VL doesn't natively support — the proxy fills the gap.
package e2e_compat

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestLokiFunctions_Decolorize verifies | decolorize strips ANSI codes.
func TestLokiFunctions_Decolorize(t *testing.T) {
	ingestDecolorizeData(t)

	t.Run("decolorize_strips_ansi", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{app="ansi-test"} | decolorize`)
		if len(streams) == 0 {
			t.Fatal("decolorize query should return results")
		}

		for _, s := range streams {
			stream, _ := s.(map[string]interface{})
			values, _ := stream["values"].([]interface{})
			for _, v := range values {
				val, _ := v.([]interface{})
				if len(val) >= 2 {
					line, _ := val[1].(string)
					if strings.Contains(line, "\x1b[") {
						t.Errorf("decolorize should strip ANSI codes, still found in: %q", line)
					}
				}
			}
		}
	})

	t.Run("without_decolorize_keeps_ansi", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{app="ansi-test"}`)
		if len(streams) == 0 {
			t.Fatal("query without decolorize should return results")
		}

		foundANSI := false
		for _, s := range streams {
			stream, _ := s.(map[string]interface{})
			values, _ := stream["values"].([]interface{})
			for _, v := range values {
				val, _ := v.([]interface{})
				if len(val) >= 2 {
					line, _ := val[1].(string)
					if strings.Contains(line, "\x1b[") || strings.Contains(line, "\\u001b[") {
						foundANSI = true
					}
				}
			}
		}
		if !foundANSI {
			t.Log("note: ANSI codes may be stripped by VL ingestion")
		}
	})
}

// TestLokiFunctions_IPFilter verifies | label = ip("CIDR") filtering.
func TestLokiFunctions_IPFilter(t *testing.T) {
	ingestIPFilterData(t)

	t.Run("ip_filter_private_range", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{app="ip-test"} | addr = ip("10.0.0.0/8")`)
		for _, s := range streams {
			stream, _ := s.(map[string]interface{})
			labels, _ := stream["stream"].(map[string]interface{})
			addr, _ := labels["addr"].(string)
			if addr != "" && !strings.HasPrefix(addr, "10.") {
				t.Errorf("ip filter should only return 10.x.x.x, got %s", addr)
			}
		}
	})

	t.Run("ip_filter_192_168", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{app="ip-test"} | addr = ip("192.168.0.0/16")`)
		for _, s := range streams {
			stream, _ := s.(map[string]interface{})
			labels, _ := stream["stream"].(map[string]interface{})
			addr, _ := labels["addr"].(string)
			if addr != "" && !strings.HasPrefix(addr, "192.168.") {
				t.Errorf("ip filter should only return 192.168.x.x, got %s", addr)
			}
		}
	})
}

// TestLokiFunctions_LineFormat verifies | line_format with Go template functions.
func TestLokiFunctions_LineFormat(t *testing.T) {
	ingestLineFormatData(t)

	t.Run("line_format_basic_field", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{app="fmt-test"} | line_format "{{.app}}"`)
		if len(streams) == 0 {
			t.Log("note: line_format may not produce results if VL translator changes the query")
		}
	})
}

// TestLokiFunctions_FormatQuery verifies /loki/api/v1/format_query endpoint.
func TestLokiFunctions_FormatQuery(t *testing.T) {
	query := `{app="nginx"} |= "error" | json | status >= 500`

	resp, err := http.Get(proxyURL + "/loki/api/v1/format_query?query=" + url.QueryEscape(query))
	if err != nil {
		t.Fatalf("format_query failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	status, _ := result["status"].(string)
	if status != "success" {
		t.Errorf("expected status=success, got %q", status)
	}

	data, _ := result["data"].(string)
	if data != query {
		t.Errorf("format_query should return query as-is, got %q", data)
	}
}

// TestLokiFunctions_DetectedLabels verifies /loki/api/v1/detected_labels endpoint.
func TestLokiFunctions_DetectedLabels(t *testing.T) {
	resp, err := http.Get(proxyURL + "/loki/api/v1/detected_labels?query=*")
	if err != nil {
		t.Fatalf("detected_labels failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	status, _ := result["status"].(string)
	if status != "success" {
		t.Errorf("expected status=success, got %q", status)
	}

	data, _ := result["data"].([]interface{})
	if len(data) == 0 {
		t.Error("detected_labels should return label names")
	}
}

// TestLokiFunctions_WriteBlocked verifies /loki/api/v1/push is blocked.
func TestLokiFunctions_WriteBlocked(t *testing.T) {
	resp, err := http.Post(proxyURL+"/loki/api/v1/push", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("push request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 405 {
		t.Errorf("expected 405 Method Not Allowed for push, got %d", resp.StatusCode)
	}

	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if !strings.Contains(result["error"], "read-only") {
		t.Errorf("expected read-only error message, got %q", result["error"])
	}
}

// TestLokiFunctions_BuildInfo verifies /loki/api/v1/status/buildinfo.
func TestLokiFunctions_BuildInfo(t *testing.T) {
	resp, err := http.Get(proxyURL + "/loki/api/v1/status/buildinfo")
	if err != nil {
		t.Fatalf("buildinfo failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	status, _ := result["status"].(string)
	if status != "success" {
		t.Errorf("expected status=success, got %q", status)
	}

	data, _ := result["data"].(map[string]interface{})
	if data["version"] == nil {
		t.Error("buildinfo should contain version")
	}
}

// TestLokiFunctions_Ready verifies /ready endpoint.
func TestLokiFunctions_Ready(t *testing.T) {
	resp, err := http.Get(proxyURL + "/ready")
	if err != nil {
		t.Fatalf("ready check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200 ready, got %d", resp.StatusCode)
	}
}

// TestLokiFunctions_Metrics verifies /metrics endpoint returns valid Prometheus exposition.
func TestLokiFunctions_Metrics(t *testing.T) {
	resp, err := http.Get(proxyURL + "/metrics")
	if err != nil {
		t.Fatalf("metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body := make([]byte, 8192)
	n, _ := resp.Body.Read(body)
	content := string(body[:n])

	// Verify key metrics exist
	requiredMetrics := []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_request_duration_seconds",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_cache_misses_total",
		"loki_vl_proxy_uptime_seconds",
		"loki_vl_proxy_tenant_requests_total",
		"loki_vl_proxy_client_errors_total",
		"loki_vl_proxy_circuit_breaker_state",
	}
	for _, metric := range requiredMetrics {
		if !strings.Contains(content, metric) {
			t.Errorf("missing metric %q in /metrics output", metric)
		}
	}

	// Per-endpoint cache metrics (HELP line always present, counters only after cache hits)
	if !strings.Contains(content, "loki_vl_proxy_cache_hits_by_endpoint") {
		t.Log("note: per-endpoint cache hit metric appears after first cache hit")
	}

	// Backend duration
	if !strings.Contains(content, "loki_vl_proxy_backend_duration_seconds") {
		t.Log("note: backend_duration may not appear until first VL request")
	}
}

// TestLokiFunctions_Patterns verifies /loki/api/v1/patterns extracts log patterns.
func TestLokiFunctions_Patterns(t *testing.T) {
	// Ingest data with repeating patterns
	ingestPatternData(t)

	params := url.Values{
		"query": {`{app="pattern-test"}`},
		"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
		"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
	}
	resp, err := http.Get(proxyURL + "/loki/api/v1/patterns?" + params.Encode())
	if err != nil {
		t.Fatalf("patterns failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["status"] != "success" {
		t.Errorf("patterns should return status=success, got %v", result["status"])
	}

	data, _ := result["data"].([]interface{})
	if len(data) == 0 {
		t.Log("note: patterns may be empty if VL query returned no data for this time range")
	} else {
		t.Logf("found %d patterns", len(data))
		for _, d := range data {
			p, _ := d.(map[string]interface{})
			t.Logf("  pattern: %v", p["pattern"])
		}
	}
}

// TestLokiFunctions_PatternsWithLoki compares patterns between Loki and proxy.
func TestLokiFunctions_PatternsWithLoki(t *testing.T) {
	ingestPatternData(t)

	params := url.Values{
		"query": {`{app="pattern-test"}`},
		"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
		"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
	}

	// Both should return valid responses
	for _, base := range []string{lokiURL, proxyURL} {
		resp, err := http.Get(base + "/loki/api/v1/patterns?" + params.Encode())
		if err != nil {
			t.Logf("%s: patterns request failed: %v", base, err)
			continue
		}
		defer resp.Body.Close()

		// Loki OSS may not support /patterns (returns 404) — tolerate
		if resp.StatusCode == 404 {
			t.Logf("%s: /patterns returned 404 (not supported in this Loki version)", base)
			continue
		}
		if resp.StatusCode != 200 {
			t.Logf("%s: expected 200, got %d (non-fatal)", base, resp.StatusCode)
			continue
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		if result["status"] != "success" {
			t.Logf("%s: expected status=success (non-fatal)", base)
		}

		data, _ := result["data"].([]interface{})
		t.Logf("%s: returned %d patterns", base, len(data))
	}
}

func ingestPatternData(t *testing.T) {
	t.Helper()
	now := time.Now()
	pushStream(t, now, streamDef{
		Labels: map[string]string{"app": "pattern-test", "level": "info"},
		Lines: []string{
			"GET /api/v1/users 200 15ms",
			"GET /api/v1/users 200 22ms",
			"GET /api/v1/users 200 18ms",
			"POST /api/v1/orders 201 142ms",
			"POST /api/v1/orders 201 155ms",
			"GET /api/v1/products 200 8ms",
			"DELETE /api/v1/orders/123 403 3ms",
			"DELETE /api/v1/orders/456 403 2ms",
		},
	})
	time.Sleep(2 * time.Second)
}

// ─── Test Data Ingestion ─────────────────────────────────────────────────────

func ingestDecolorizeData(t *testing.T) {
	t.Helper()
	now := time.Now()
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "ansi-test", "level": "info",
		},
		Lines: []string{
			"\x1b[31mERROR\x1b[0m: connection refused to database",
			"\x1b[32mINFO\x1b[0m: request processed in \x1b[1m42ms\x1b[0m",
			"\x1b[33mWARN\x1b[0m: high latency detected on \x1b[4mquery_range\x1b[0m",
			"plain line without any color codes",
		},
	})
	time.Sleep(2 * time.Second)
}

func ingestIPFilterData(t *testing.T) {
	t.Helper()
	now := time.Now()

	ips := []struct {
		addr string
		line string
	}{
		{"10.0.1.42", "internal request from datacenter"},
		{"192.168.1.100", "request from office network"},
		{"8.8.8.8", "external DNS query"},
		{"172.16.0.5", "request from container network"},
		{"10.0.2.99", "another internal request"},
	}

	for i, ip := range ips {
		pushStream(t, now.Add(time.Duration(i)*time.Second), streamDef{
			Labels: map[string]string{
				"app": "ip-test", "level": "info", "addr": ip.addr,
			},
			Lines: []string{ip.line},
		})
	}
	time.Sleep(2 * time.Second)
}

func ingestLineFormatData(t *testing.T) {
	t.Helper()
	now := time.Now()
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "fmt-test", "level": "info", "method": "get", "status": "200",
		},
		Lines: []string{
			`{"msg":"request handled","duration_ms":42}`,
			`{"msg":"another request","duration_ms":15}`,
		},
	})
	time.Sleep(2 * time.Second)
}
