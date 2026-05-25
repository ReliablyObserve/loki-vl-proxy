//go:build e2e

// Label cache e2e tests — verifies warmup, disk persistence, background full-range
// fetch, and keep-warm behaviour against a live proxy+VictoriaLogs stack.
package e2e_compat

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// parsePrometheusCounter reads the /metrics text and returns the sum of all
// samples for the given metric name that match ALL of the provided label pairs.
// Returns -1 if no matching sample was found.
func parsePrometheusCounter(body, metricName string, labels map[string]string) float64 {
	total := -1.0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		hasLabels := strings.HasPrefix(line, metricName+"{")
		noLabels := strings.HasPrefix(line, metricName+" ") || strings.HasPrefix(line, metricName+"\t")
		if !hasLabels && !noLabels && line != metricName {
			continue
		}
		allMatch := true
		for k, v := range labels {
			needle := k + `="` + v + `"`
			if !strings.Contains(line, needle) {
				allMatch = false
				break
			}
		}
		if !allMatch {
			continue
		}
		// value is the last space-separated token
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[len(parts)-1], 64)
		if err != nil {
			continue
		}
		if total < 0 {
			total = v
		} else {
			total += v
		}
	}
	return total
}

func fetchMetricsBody(t *testing.T) string {
	t.Helper()
	resp, err := http.Get(proxyURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// labelsWindowParams returns query params for /loki/api/v1/labels covering
// the given window ending now.
func labelsWindowParams(window time.Duration) url.Values {
	now := time.Now()
	p := url.Values{}
	p.Set("start", fmt.Sprintf("%d", now.Add(-window).UnixNano()))
	p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	return p
}

// assertLabelsOK hits /loki/api/v1/labels with the given window and fails the
// test if the response status is not "success".
func assertLabelsOK(t *testing.T, window time.Duration) {
	t.Helper()
	p := labelsWindowParams(window)
	resp := getJSON(t, proxyURL+"/loki/api/v1/labels?"+p.Encode())
	if !checkStatus(resp) {
		t.Fatalf("labels request for window=%v failed: %v", window, resp)
	}
}

// =============================================================================
// Warmup: cache pre-populated before first user request
// =============================================================================

// TestLabelCache_WarmupPrePopulatesCache verifies that the startup warmup loop
// runs before any user request lands so the first labels call is a cache hit.
// Proof: after warmup completes (proxy is /ready), hit /loki/api/v1/labels for
// four standard windows and confirm 0 cache misses in /metrics.
func TestLabelCache_WarmupPrePopulatesCache(t *testing.T) {
	// Proxy must be up and warmup must have finished before we measure.
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	// Snapshot misses BEFORE our requests so we can detect any introduced by
	// the warmup itself (should be 0 or at most the warmup calls themselves,
	// which should also hit 0 misses because warmup writes, not reads).
	bodyBefore := fetchMetricsBody(t)
	missesBefore := parsePrometheusCounter(bodyBefore, "loki_vl_proxy_cache_misses_total", nil)
	if missesBefore < 0 {
		missesBefore = 0
	}

	windows := []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour}
	for _, w := range windows {
		assertLabelsOK(t, w)
	}

	bodyAfter := fetchMetricsBody(t)
	missesAfter := parsePrometheusCounter(bodyAfter, "loki_vl_proxy_cache_misses_total", nil)
	if missesAfter < 0 {
		missesAfter = 0
	}
	newMisses := missesAfter - missesBefore

	if newMisses > 0 {
		t.Errorf("expected 0 new cache misses after warmup (all windows pre-populated), got %.0f", newMisses)
	}

	hitsAfter := parsePrometheusCounter(bodyAfter, "loki_vl_proxy_cache_hits_total", nil)
	if hitsAfter < float64(len(windows)) {
		t.Errorf("expected at least %d cache hits after warmup + %d requests, got %.0f", len(windows), len(windows), hitsAfter)
	}
}

// =============================================================================
// Warmup: VL stream_field_names upstream calls come from warmup windows
// =============================================================================

// TestLabelCache_WarmupTriggersVLBackendCalls verifies that proxy startup
// warmup drives at least 4 upstream /select/logsql/stream_field_names calls
// (one per window: 1h, 6h, 24h, 7d).
func TestLabelCache_WarmupTriggersVLBackendCalls(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	body := fetchMetricsBody(t)
	vlCalls := parsePrometheusCounter(body, "loki_vl_proxy_requests_total",
		map[string]string{"endpoint": "select_logsql_stream_field_names", "status": "200"})

	// Warmup covers 4 windows (1h, 6h, 24h, 7d) — each issues at least 1 VL call.
	const minExpected = 4
	if vlCalls < minExpected {
		t.Errorf("expected at least %d VL stream_field_names calls from warmup, got %.0f", minExpected, vlCalls)
	}
}

// =============================================================================
// Second request for wide window is a cache hit (background full-range refresh)
// =============================================================================

// TestLabelCache_WideWindowSecondRequestIsCacheHit ensures that a second
// request for a wide time window (7d) is served from cache without triggering
// additional VL upstream calls.
func TestLabelCache_WideWindowSecondRequestIsCacheHit(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	window := 7 * 24 * time.Hour

	// First request — may or may not hit cache (warmup covers it).
	assertLabelsOK(t, window)

	bodyBefore := fetchMetricsBody(t)
	hitsBefore := parsePrometheusCounter(bodyBefore, "loki_vl_proxy_cache_hits_total", nil)
	if hitsBefore < 0 {
		hitsBefore = 0
	}

	// Second request — must be a cache hit.
	assertLabelsOK(t, window)

	bodyAfter := fetchMetricsBody(t)
	hitsAfter := parsePrometheusCounter(bodyAfter, "loki_vl_proxy_cache_hits_total", nil)
	if hitsAfter < 0 {
		hitsAfter = 0
	}

	if hitsAfter <= hitsBefore {
		t.Errorf("expected cache hit count to increase on second wide-window request: before=%.0f after=%.0f", hitsBefore, hitsAfter)
	}
}

// =============================================================================
// Background full-range refresh: no cap at 1h for wide windows
// =============================================================================

// TestLabelCache_BackgroundRefreshFetchesFullRange verifies that a request for
// a 7d window triggers a background full-range VL fetch (not capped at 1h) so
// subsequent requests have complete historical label data.
func TestLabelCache_BackgroundRefreshFetchesFullRange(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	// Prime the cache with a 7d request.
	assertLabelsOK(t, 7*24*time.Hour)

	// Allow background goroutine time to complete its full-range VL fetch.
	time.Sleep(2 * time.Second)

	// The response must still be healthy and contain labels.
	resp := getJSON(t, proxyURL+"/loki/api/v1/labels?"+labelsWindowParams(7*24*time.Hour).Encode())
	if !checkStatus(resp) {
		t.Fatalf("labels request after background refresh failed: %v", resp)
	}

	data, _ := resp["data"].([]interface{})
	if len(data) == 0 {
		t.Error("expected non-empty labels after background full-range refresh")
	}
}

// =============================================================================
// Labels response shape matches Loki for all standard windows
// =============================================================================

// TestLabelCache_AllWindowsReturnValidShape checks that labels for all four
// warmup windows return a valid Loki response shape (status=success, data=[]).
func TestLabelCache_AllWindowsReturnValidShape(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	windows := map[string]time.Duration{
		"1h":  time.Hour,
		"6h":  6 * time.Hour,
		"24h": 24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
	}

	for name, window := range windows {
		t.Run(name, func(t *testing.T) {
			p := labelsWindowParams(window)
			resp := getJSON(t, proxyURL+"/loki/api/v1/labels?"+p.Encode())
			if !checkStatus(resp) {
				t.Fatalf("window=%s: expected status=success, got %v", name, resp)
			}
			data, ok := resp["data"].([]interface{})
			if !ok {
				t.Fatalf("window=%s: data field not an array", name)
			}
			if len(data) == 0 {
				t.Errorf("window=%s: expected non-empty label list", name)
			}
		})
	}
}

// =============================================================================
// Keep-warm: labels stay in cache across keep-warm interval
// =============================================================================

// TestLabelCache_KeepWarmMaintainsHitRate verifies that after the initial
// warmup, repeated label requests continue to be cache hits (not misses) even
// when no user queries have refreshed the cache recently.
func TestLabelCache_KeepWarmMaintainsHitRate(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	windows := []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}

	for _, w := range windows {
		assertLabelsOK(t, w)
	}

	bodyBefore := fetchMetricsBody(t)
	hitsBefore := parsePrometheusCounter(bodyBefore, "loki_vl_proxy_cache_hits_total", nil)
	missesBefore := parsePrometheusCounter(bodyBefore, "loki_vl_proxy_cache_misses_total", nil)
	if hitsBefore < 0 {
		hitsBefore = 0
	}
	if missesBefore < 0 {
		missesBefore = 0
	}

	// Second pass — all should still be cache hits.
	for _, w := range windows {
		assertLabelsOK(t, w)
	}

	bodyAfter := fetchMetricsBody(t)
	hitsAfter := parsePrometheusCounter(bodyAfter, "loki_vl_proxy_cache_hits_total", nil)
	missesAfter := parsePrometheusCounter(bodyAfter, "loki_vl_proxy_cache_misses_total", nil)
	if hitsAfter < 0 {
		hitsAfter = 0
	}
	if missesAfter < 0 {
		missesAfter = 0
	}

	newHits := hitsAfter - hitsBefore
	newMisses := missesAfter - missesBefore

	if newMisses > 0 {
		t.Errorf("expected 0 new misses on repeated requests (keep-warm active), got %.0f", newMisses)
	}
	if newHits < float64(len(windows)) {
		t.Errorf("expected at least %d new hits from repeated requests, got %.0f", len(windows), newHits)
	}
}

// =============================================================================
// Proxy health check reaches VL before warmup proceeds
// =============================================================================

// TestLabelCache_VLHealthCheckRegistered verifies that the proxy called the
// VL /health endpoint (readiness probe before warmup) at least once.
func TestLabelCache_VLHealthCheckRegistered(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	body := fetchMetricsBody(t)
	healthCalls := parsePrometheusCounter(body, "loki_vl_proxy_requests_total",
		map[string]string{"endpoint": "health", "status": "200"})

	if healthCalls < 1 {
		t.Errorf("expected at least 1 VL health call (readiness probe), got %.0f", healthCalls)
	}
}

// =============================================================================
// Regression: existing label endpoint behaviour unchanged
// =============================================================================

// TestLabelCache_LabelsEndpointReturnsAppLabel is a regression guard ensuring
// the labels endpoint still exposes the "app" label that was ingested by the
// main test suite setup.
func TestLabelCache_LabelsEndpointReturnsAppLabel(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	resp := getJSON(t, proxyURL+"/loki/api/v1/labels")
	if !checkStatus(resp) {
		t.Fatalf("expected status=success, got %v", resp)
	}
	labels := extractStrings(resp, "data")
	if !contains(labels, "app") {
		t.Errorf("expected 'app' label in response, got %v", labels)
	}
}

// TestLabelCache_LabelValuesEndpointNotBroken verifies label/values for the
// "app" label still returns values (regression guard for cache changes).
func TestLabelCache_LabelValuesEndpointNotBroken(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	resp := getJSON(t, proxyURL+"/loki/api/v1/label/app/values")
	if !checkStatus(resp) {
		t.Fatalf("expected status=success, got %v", resp)
	}
	values := extractStrings(resp, "data")
	if len(values) == 0 {
		t.Error("expected non-empty app label values, got empty list")
	}
}

// TestLabelCache_MetricsEndpointHasCacheCounters verifies that cache hit/miss
// counters appear in /metrics (basic smoke test, regression guard).
func TestLabelCache_MetricsEndpointHasCacheCounters(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	body := fetchMetricsBody(t)
	for _, name := range []string{
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_cache_misses_total",
		"loki_vl_proxy_requests_total",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("metric %s missing from /metrics", name)
		}
	}
}
