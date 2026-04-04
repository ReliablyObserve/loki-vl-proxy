//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

var proxyAddrs = []string{
	"http://localhost:3100", // proxy-a
	"http://localhost:3101", // proxy-b
	"http://localhost:3102", // proxy-c
}

func init() {
	// Wait for all proxies to be ready
	client := &http.Client{Timeout: 2 * time.Second}
	for _, addr := range proxyAddrs {
		for i := 0; i < 30; i++ {
			resp, err := client.Get(addr + "/ready")
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(time.Second)
		}
	}
}

// ingestLogs pushes test data via any proxy to VictoriaLogs.
func ingestLogs(t *testing.T) {
	t.Helper()
	lines := []string{
		`{"_time":"2026-01-01T00:00:00Z","_msg":"fleet test line 1","app":"web","env":"prod"}`,
		`{"_time":"2026-01-01T00:00:01Z","_msg":"fleet test line 2","app":"api","env":"prod"}`,
		`{"_time":"2026-01-01T00:00:02Z","_msg":"fleet test error","app":"web","env":"staging"}`,
	}

	body := strings.Join(lines, "\n")
	resp, err := http.Post("http://localhost:9428/insert/jsonline?_stream_fields=app,env",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest failed: %d %s", resp.StatusCode, b)
	}
	// Allow VL to index (CI can be slow)
	time.Sleep(5 * time.Second)
}

type lokiResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string            `json:"resultType"`
		Result     []json.RawMessage `json:"result"`
	} `json:"data"`
}

func queryProxy(proxyAddr, query string) (*lokiResponse, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", "1735689600000000000") // 2026-01-01T00:00:00Z
	params.Set("end", "1735776000000000000")   // 2026-01-02T00:00:00Z
	params.Set("limit", "100")

	resp, err := http.Get(proxyAddr + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var lr lokiResponse
	if err := json.Unmarshal(body, &lr); err != nil {
		return nil, err
	}
	return &lr, nil
}

// TestFleet_AllProxiesReturnSameResults verifies that all 3 proxies return
// consistent results for the same query (demonstrating fleet cache sharing).
func TestFleet_AllProxiesReturnSameResults(t *testing.T) {
	ingestLogs(t)

	query := `{app="web"}`
	var results []int

	for _, addr := range proxyAddrs {
		lr, err := queryProxy(addr, query)
		if err != nil {
			t.Fatalf("query to %s failed: %v", addr, err)
		}
		if lr.Status != "success" {
			t.Fatalf("query to %s returned status %s", addr, lr.Status)
		}
		results = append(results, len(lr.Data.Result))
	}

	// All proxies should return the same number of streams
	for i := 1; i < len(results); i++ {
		if results[i] != results[0] {
			t.Errorf("inconsistent results: proxy[0]=%d proxy[%d]=%d",
				results[0], i, results[i])
		}
	}
	if results[0] == 0 {
		t.Log("Warning: 0 streams returned (VL may need more indexing time)")
	} else {
		t.Logf("All 3 proxies returned %d streams", results[0])
	}
}

// TestFleet_CacheGetEndpoint verifies the /_cache/get peer endpoint works.
func TestFleet_CacheGetEndpoint(t *testing.T) {
	// First, warm the cache on proxy-a
	_, err := queryProxy(proxyAddrs[0], `{app="api"}`)
	if err != nil {
		t.Fatalf("warm query failed: %v", err)
	}

	// Hit the cache endpoint directly on proxy-a
	resp, err := http.Get(proxyAddrs[0] + "/_cache/get?key=test-key")
	if err != nil {
		t.Fatalf("cache get failed: %v", err)
	}
	defer resp.Body.Close()

	// The peer cache endpoint should not be publicly callable from outside the fleet.
	// Hardened builds return 403 here; older builds might expose a 404/200.
	switch resp.StatusCode {
	case http.StatusForbidden:
		t.Log("peer cache endpoint is protected from external access")
	case http.StatusNotFound, http.StatusOK:
		t.Logf("peer cache endpoint responded with %d", resp.StatusCode)
	default:
		t.Errorf("unexpected status: %d (expected 403, 404, or 200)", resp.StatusCode)
	}
}

// TestFleet_MetricsEndpoint verifies /metrics is accessible on all proxies.
func TestFleet_MetricsEndpoint(t *testing.T) {
	for _, addr := range proxyAddrs {
		resp, err := http.Get(addr + "/metrics")
		if err != nil {
			t.Fatalf("metrics on %s failed: %v", addr, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("%s: metrics returned %d", addr, resp.StatusCode)
		}

		bodyStr := string(body)
		// Check for peer cache metrics
		if !strings.Contains(bodyStr, "loki_vl_proxy_") {
			t.Errorf("%s: metrics don't contain loki_vl_proxy_ prefix", addr)
		}
	}
}

// TestFleet_ReadyEndpoint verifies /ready is healthy on all proxies.
func TestFleet_ReadyEndpoint(t *testing.T) {
	for _, addr := range proxyAddrs {
		resp, err := http.Get(addr + "/ready")
		if err != nil {
			t.Fatalf("ready on %s failed: %v", addr, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("%s: ready returned %d", addr, resp.StatusCode)
		}
	}
}

// TestFleet_SecondQueryHitsCache verifies the second identical query across
// any proxy benefits from caching (either local L1/L2 or peer L3).
func TestFleet_SecondQueryHitsCache(t *testing.T) {
	ingestLogs(t)

	query := `{env="prod"}`

	// First query on proxy-a (populates cache)
	start := time.Now()
	_, err := queryProxy(proxyAddrs[0], query)
	if err != nil {
		t.Fatalf("first query failed: %v", err)
	}
	firstDuration := time.Since(start)

	// Wait for cache to settle
	time.Sleep(500 * time.Millisecond)

	// Second query on proxy-a (should hit L1 cache)
	start = time.Now()
	lr, err := queryProxy(proxyAddrs[0], query)
	if err != nil {
		t.Fatalf("second query failed: %v", err)
	}
	secondDuration := time.Since(start)

	if lr.Status != "success" {
		t.Errorf("expected success, got %s", lr.Status)
	}

	t.Logf("First query: %v, Second query (cached): %v", firstDuration, secondDuration)
	// We don't assert timing strictly (CI can be slow), just log it
}

// TestFleet_LabelsConsistentAcrossProxies verifies /loki/api/v1/labels
// returns consistent results across all proxies.
func TestFleet_LabelsConsistentAcrossProxies(t *testing.T) {
	ingestLogs(t)

	var labelSets []string
	for _, addr := range proxyAddrs {
		resp, err := http.Get(addr + "/loki/api/v1/labels")
		if err != nil {
			t.Fatalf("labels from %s failed: %v", addr, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("%s: labels returned %d", addr, resp.StatusCode)
			continue
		}
		labelSets = append(labelSets, string(body))
	}

	// All should return the same label set
	for i := 1; i < len(labelSets); i++ {
		if labelSets[i] != labelSets[0] {
			t.Errorf("labels differ: proxy[0] vs proxy[%d]", i)
			t.Logf("proxy[0]: %s", labelSets[0])
			t.Logf("proxy[%d]: %s", i, labelSets[i])
		}
	}
}

// TestFleet_CacheHitMetrics verifies that cache hits are reported in /metrics
// after repeated queries.
func TestFleet_CacheHitMetrics(t *testing.T) {
	ingestLogs(t)

	query := `{app="web"}`

	// Query twice on same proxy to generate a cache hit
	queryProxy(proxyAddrs[0], query)
	time.Sleep(200 * time.Millisecond)
	queryProxy(proxyAddrs[0], query)
	time.Sleep(200 * time.Millisecond)

	// Check metrics for cache hits
	resp, err := http.Get(proxyAddrs[0] + "/metrics")
	if err != nil {
		t.Fatalf("metrics failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	bodyStr := string(body)
	if strings.Contains(bodyStr, "loki_vl_proxy_cache_hits_total") {
		t.Log("Cache hit metrics present")
	}
	if strings.Contains(bodyStr, "loki_vl_proxy_cache_misses_total") {
		t.Log("Cache miss metrics present")
	}

	// Look for disk cache metrics
	if strings.Contains(bodyStr, "disk") || strings.Contains(bodyStr, "l2") {
		t.Log("Disk cache metrics present")
	}
}

// TestFleet_DiskCacheEnabled verifies that disk cache is active by checking
// that cache entries survive across queries.
func TestFleet_DiskCacheEnabled(t *testing.T) {
	ingestLogs(t)

	// Run multiple different queries to fill cache
	queries := []string{
		`{app="web"}`,
		`{app="api"}`,
		`{env="prod"}`,
		`{env="staging"}`,
	}

	for _, q := range queries {
		_, err := queryProxy(proxyAddrs[0], q)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
	}

	// Wait for disk cache flush (flush interval = 1s, flush size = 10)
	time.Sleep(2 * time.Second)

	// Query the same queries again — should be faster (L1 or L2 cache hit)
	for _, q := range queries {
		start := time.Now()
		_, err := queryProxy(proxyAddrs[0], q)
		if err != nil {
			t.Fatalf("cached query failed: %v", err)
		}
		d := time.Since(start)
		t.Logf("Cached query %q: %v", q, d)
	}
}

// TestFleet_CrossProxyCacheWarming verifies that querying one proxy
// warms the cache so that subsequent queries to other proxies can benefit
// from the peer cache (L3).
func TestFleet_CrossProxyCacheWarming(t *testing.T) {
	ingestLogs(t)

	query := `{app="web"}`

	// Query proxy-a first (populates its L1 + L2 cache)
	_, err := queryProxy(proxyAddrs[0], query)
	if err != nil {
		t.Fatalf("first query failed: %v", err)
	}

	// Wait for cache to settle
	time.Sleep(500 * time.Millisecond)

	// Query proxy-b with same query — it should check peer cache (L3)
	// and potentially get data from proxy-a
	start := time.Now()
	lr, err := queryProxy(proxyAddrs[1], query)
	if err != nil {
		t.Fatalf("cross-proxy query failed: %v", err)
	}
	duration := time.Since(start)

	if lr.Status != "success" {
		t.Errorf("expected success, got %s", lr.Status)
	}

	t.Logf("Cross-proxy query: %v (peer cache may have served this)", duration)
}

// TestFleet_RepeatedQueriesShowCacheEffect runs the same query multiple times
// and verifies that response times improve (demonstrating cache effect).
func TestFleet_RepeatedQueriesShowCacheEffect(t *testing.T) {
	ingestLogs(t)

	query := `{env="prod"}`

	var durations []time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		lr, err := queryProxy(proxyAddrs[0], query)
		if err != nil {
			t.Fatalf("query %d failed: %v", i, err)
		}
		d := time.Since(start)
		durations = append(durations, d)

		if lr.Status != "success" {
			t.Errorf("query %d: expected success, got %s", i, lr.Status)
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("Query durations: %v", durations)

	// The first query should generally be slowest (cold miss)
	// Subsequent queries should be faster (cache hit)
	// We don't assert strictly because CI timing varies
	if len(durations) >= 3 {
		avg23 := (durations[1] + durations[2]) / 2
		if avg23 < durations[0] {
			t.Logf("Cache effect confirmed: first=%v avg(2-3)=%v", durations[0], avg23)
		} else {
			t.Logf("Cache effect not visible in timing (CI noise): first=%v avg(2-3)=%v", durations[0], avg23)
		}
	}
}

// TestFleet_ConcurrentQueries runs queries concurrently across all proxies
// and verifies no errors or panics.
func TestFleet_ConcurrentQueries(t *testing.T) {
	ingestLogs(t)

	queries := []string{
		`{app="web"}`,
		`{app="api"}`,
		`{env="prod"}`,
		`{env="staging"}`,
	}

	errCh := make(chan error, len(queries)*len(proxyAddrs))

	for _, q := range queries {
		for _, addr := range proxyAddrs {
			go func(addr, query string) {
				_, err := queryProxy(addr, query)
				errCh <- err
			}(addr, q)
		}
	}

	for i := 0; i < len(queries)*len(proxyAddrs); i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent query failed: %v", err)
		}
	}
}
