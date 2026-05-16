//go:build e2e

// Performance comparison tests: Loki direct vs Loki-VL-proxy.
// Measures latency for each API endpoint on both backends.
package e2e_compat

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type perfResult struct {
	Endpoint    string
	LokiP50     time.Duration
	ProxyP50    time.Duration
	Overhead    time.Duration
	OverheadPct float64
}

func measure(t *testing.T, urlStr string, iterations int) time.Duration {
	t.Helper()
	var total time.Duration
	for range iterations {
		start := time.Now()
		resp, err := http.Get(urlStr)
		if err != nil {
			continue
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
		total += time.Since(start)
	}
	return total / time.Duration(iterations)
}

func measurePost(t *testing.T, urlStr string, body string, iterations int) time.Duration {
	t.Helper()
	var total time.Duration
	for range iterations {
		start := time.Now()
		resp, err := http.Post(urlStr, "application/x-www-form-urlencoded", strings.NewReader(body))
		if err != nil {
			continue
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
		total += time.Since(start)
	}
	return total / time.Duration(iterations)
}

func TestPerf_Labels(t *testing.T) {
	iterations := 10
	lokiAvg := measure(t, lokiURL+"/loki/api/v1/labels", iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/labels", iterations)
	reportPerf(t, "labels", lokiAvg, proxyAvg)
}

func TestPerf_LabelValues(t *testing.T) {
	iterations := 10
	lokiAvg := measure(t, lokiURL+"/loki/api/v1/label/app/values", iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/label/app/values", iterations)
	reportPerf(t, "label_values", lokiAvg, proxyAvg)
}

func TestPerf_QueryRange_Simple(t *testing.T) {
	iterations := 5
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")
	qs := params.Encode()

	lokiAvg := measure(t, lokiURL+"/loki/api/v1/query_range?"+qs, iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/query_range?"+qs, iterations)
	reportPerf(t, "query_range_simple", lokiAvg, proxyAvg)
}

func TestPerf_QueryRange_WithFilter(t *testing.T) {
	iterations := 5
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"} |= "error"`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")
	qs := params.Encode()

	lokiAvg := measure(t, lokiURL+"/loki/api/v1/query_range?"+qs, iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/query_range?"+qs, iterations)
	reportPerf(t, "query_range_filter", lokiAvg, proxyAvg)
}

func TestPerf_QueryRange_JSONParse(t *testing.T) {
	iterations := 5
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway",level="info"} | json | status >= 400`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")
	qs := params.Encode()

	lokiAvg := measure(t, lokiURL+"/loki/api/v1/query_range?"+qs, iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/query_range?"+qs, iterations)
	reportPerf(t, "query_range_json_parse", lokiAvg, proxyAvg)
}

func TestPerf_Series(t *testing.T) {
	iterations := 5
	params := url.Values{}
	params.Set("match[]", `{namespace="prod"}`)
	qs := params.Encode()

	lokiAvg := measure(t, lokiURL+"/loki/api/v1/series?"+qs, iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/series?"+qs, iterations)
	reportPerf(t, "series", lokiAvg, proxyAvg)
}

func TestPerf_Ready(t *testing.T) {
	iterations := 20
	lokiAvg := measure(t, lokiURL+"/ready", iterations)
	proxyAvg := measure(t, proxyURL+"/ready", iterations)
	reportPerf(t, "ready", lokiAvg, proxyAvg)
}

func TestPerf_BuildInfo(t *testing.T) {
	iterations := 20
	lokiAvg := measure(t, lokiURL+"/loki/api/v1/status/buildinfo", iterations)
	proxyAvg := measure(t, proxyURL+"/loki/api/v1/status/buildinfo", iterations)
	reportPerf(t, "buildinfo", lokiAvg, proxyAvg)
}

func TestPerf_CacheEffect(t *testing.T) {
	// First call: cache miss (cold)
	start := time.Now()
	resp, _ := http.Get(proxyURL + "/loki/api/v1/labels?start=1&end=999999999999")
	if resp != nil {
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	cold := time.Since(start)

	// Second call: cache hit (warm)
	start = time.Now()
	resp, _ = http.Get(proxyURL + "/loki/api/v1/labels?start=1&end=999999999999")
	if resp != nil {
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	warm := time.Since(start)

	t.Logf("\n=== CACHE EFFECT ===")
	t.Logf("Cold (miss):  %v", cold)
	t.Logf("Warm (hit):   %v", warm)
	t.Logf("Speedup:      %.1fx", float64(cold)/float64(warm))
	t.Logf("====================")
}

func TestPerf_Summary(t *testing.T) {
	t.Log("\n" + strings.Repeat("=", 70))
	t.Log("PERFORMANCE TEST COMPLETE")
	t.Log("Run with: go test -tags=e2e -v -run TestPerf ./test/e2e-compat/")
	t.Log("Note: proxy overhead includes translation + VL query + response conversion")
	t.Log(strings.Repeat("=", 70))
}

func reportPerf(t *testing.T, endpoint string, lokiAvg, proxyAvg time.Duration) {
	t.Helper()
	overhead := proxyAvg - lokiAvg
	overheadPct := float64(0)
	if lokiAvg > 0 {
		overheadPct = float64(overhead) / float64(lokiAvg) * 100
	}

	t.Logf("\n=== PERF: %s ===", endpoint)
	t.Logf("Loki direct:  %v", lokiAvg)
	t.Logf("Via proxy:    %v", proxyAvg)
	t.Logf("Overhead:     %v (%.1f%%)", overhead, overheadPct)

	// Proxy should not be more than 5x slower than Loki for any endpoint
	if proxyAvg > lokiAvg*5 && lokiAvg > time.Millisecond {
		t.Errorf("proxy is >5x slower than Loki: %v vs %v", proxyAvg, lokiAvg)
	}
}
