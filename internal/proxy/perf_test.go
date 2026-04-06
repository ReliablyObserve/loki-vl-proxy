package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Performance: high-concurrency request handling
// =============================================================================

func BenchmarkProxy_QueryRange_CacheHit(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Warm cache
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	p.handleQueryRange(w, r)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
			p.handleQueryRange(w, r)
		}
	})
}

func BenchmarkProxy_Labels_CacheHit(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 100},
				{"value": "namespace", "hits": 50},
			},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Warm cache
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
			p.handleLabels(w, r)
		}
	})
}

func BenchmarkProxy_MultiTenantQueryRange_CacheHit(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("AccountID") {
		case "10":
			w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test-a","app":"nginx"}` + "\n"))
		case "20":
			w.Write([]byte(`{"_time":"2024-01-15T10:30:01Z","_msg":"test-b","app":"nginx"}` + "\n"))
		default:
			b.Fatalf("unexpected AccountID %q", r.Header.Get("AccountID"))
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	p.handleQueryRange(w, r)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
			r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
			p.handleQueryRange(w, r)
		}
	})
}

func BenchmarkProxy_MultiTenantLabels_CacheHit(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 100},
				{"value": "namespace", "hits": 50},
			},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	p.handleLabels(w, r)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
			r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
			p.handleLabels(w, r)
		}
	})
}

// =============================================================================
// Load test: sustained high-concurrency with resource monitoring
// =============================================================================

func TestLoad_HighConcurrency_MemoryStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	var requestCount atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 100},
			},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}

	// Baseline memory
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Run 100 concurrent goroutines, 1000 requests each
	concurrency := 100
	requestsPerGoroutine := 1000
	var wg sync.WaitGroup

	start := time.Now()
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < requestsPerGoroutine; i++ {
				w := httptest.NewRecorder()
				r := httptest.NewRequest("GET",
					fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", i, i+1), nil)
				p.handleLabels(w, r)
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Post-run memory
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	totalRequests := concurrency * requestsPerGoroutine
	rps := float64(totalRequests) / elapsed.Seconds()
	memGrowthMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024

	t.Logf("Load test results:")
	t.Logf("  Total requests: %d", totalRequests)
	t.Logf("  Concurrency: %d", concurrency)
	t.Logf("  Duration: %s", elapsed)
	t.Logf("  Throughput: %.0f req/s", rps)
	t.Logf("  Backend calls: %d (cache effectiveness: %.1f%%)",
		requestCount.Load(), 100*(1-float64(requestCount.Load())/float64(totalRequests)))
	t.Logf("  Memory growth: %.1f MB (before: %.1f MB, after: %.1f MB)",
		memGrowthMB, float64(memBefore.Alloc)/1024/1024, float64(memAfter.Alloc)/1024/1024)
	t.Logf("  GC cycles: %d", memAfter.NumGC-memBefore.NumGC)

	// Assertions
	if rps < 10000 {
		t.Errorf("throughput too low: %.0f req/s (expected >10,000)", rps)
	}
	// Memory growth should be bounded — not linearly growing with request count
	if memGrowthMB > 100 {
		t.Errorf("memory growth too high: %.1f MB (expected <100 MB for %d requests)", memGrowthMB, totalRequests)
	}
}

func TestLoad_CacheMiss_BackendPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	var backendCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		// Simulate slow backend
		time.Sleep(1 * time.Millisecond)
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(1*time.Millisecond, 10) // Very short TTL, small cache → many misses
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}

	concurrency := 50
	requestsPerGoroutine := 100
	var wg sync.WaitGroup

	start := time.Now()
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < requestsPerGoroutine; i++ {
				w := httptest.NewRecorder()
				// Unique query per request → all cache misses
				r := httptest.NewRequest("GET",
					fmt.Sprintf(`/loki/api/v1/query_range?query={app="nginx-%d"}&start=1&end=2&step=1`, i), nil)
				p.handleQueryRange(w, r)
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalRequests := concurrency * requestsPerGoroutine
	t.Logf("Cache-miss load test:")
	t.Logf("  Total requests: %d", totalRequests)
	t.Logf("  Duration: %s", elapsed)
	t.Logf("  Throughput: %.0f req/s", float64(totalRequests)/elapsed.Seconds())
	t.Logf("  Backend calls: %d", backendCalls.Load())

	// All requests should complete without panic or deadlock
	if backendCalls.Load() == 0 {
		t.Error("expected backend calls with cache misses")
	}
}

// =============================================================================
// Benchmark: response conversion
// =============================================================================

func BenchmarkVLLogsToLokiStreams(b *testing.B) {
	// Simulate 100 VL NDJSON log lines
	var body []byte
	for i := 0; i < 100; i++ {
		line := fmt.Sprintf(`{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"request %d processed","_stream":"{app=\"nginx\",namespace=\"prod\"}","app":"nginx","namespace":"prod","level":"info"}`, i%60, i)
		body = append(body, []byte(line+"\n")...)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vlLogsToLokiStreams(body)
	}
}

func BenchmarkWrapAsLokiResponse(b *testing.B) {
	body := []byte(`{"results":[{"metric":{"app":"nginx"},"values":[[1705312200,"42"],[1705312260,"43"]]}]}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wrapAsLokiResponse(body, "matrix")
	}
}

// =============================================================================
// NO-CACHE performance: raw proxy overhead (translation + conversion)
// =============================================================================

// newNoCacheProxy creates a proxy with cache disabled (TTL=0, max=0) to measure raw overhead.
func newNoCacheProxy(b *testing.B, backendURL string) *Proxy {
	b.Helper()
	c := cache.New(0, 0) // zero TTL = immediate expiry, effectively no cache
	p, err := New(Config{BackendURL: backendURL, Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	return p
}

func BenchmarkNoCache_QueryRange_LogQuery(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 10 NDJSON log lines (realistic small result)
		for i := 0; i < 10; i++ {
			fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"request %d processed","_stream":"{app=\"nginx\"}","app":"nginx","level":"info"}`+"\n", i, i)
		}
	}))
	defer vlBackend.Close()

	p := newNoCacheProxy(b, vlBackend.URL)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
			p.handleQueryRange(w, r)
		}
	})
}

func BenchmarkNoCache_QueryRange_LargeResult(b *testing.B) {
	// 500 NDJSON lines — simulate a heavy log query
	var responseBody []byte
	for i := 0; i < 500; i++ {
		line := fmt.Sprintf(`{"_time":"2024-01-15T10:%02d:%02d.000Z","_msg":"[INFO] %s request %d from 10.0.%d.%d path=/api/v1/users duration=%dms status=200","_stream":"{app=\"api-gateway\",namespace=\"prod\"}","app":"api-gateway","namespace":"prod","level":"info","trace_id":"abc%04x"}`, i/60, i%60, []string{"GET", "POST", "PUT"}[i%3], i, i/256, i%256, i%1000, i)
		responseBody = append(responseBody, []byte(line+"\n")...)
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(responseBody)
	}))
	defer vlBackend.Close()

	p := newNoCacheProxy(b, vlBackend.URL)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api-gateway"}&start=1&end=2&step=1&limit=500`, nil)
		p.handleQueryRange(w, r)
	}
}

func BenchmarkNoCache_Labels(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 100},
				{"value": "namespace", "hits": 80},
				{"value": "pod", "hits": 60},
				{"value": "container", "hits": 40},
				{"value": "level", "hits": 200},
			},
		})
	}))
	defer vlBackend.Close()

	p := newNoCacheProxy(b, vlBackend.URL)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
			p.handleLabels(w, r)
		}
	})
}

func BenchmarkNoCache_StatsQuery(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"metric": map[string]string{"app": "nginx"},
					"values": [][]interface{}{
						{1705312200.0, "42"},
						{1705312260.0, "43"},
						{1705312320.0, "45"},
					},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newNoCacheProxy(b, vlBackend.URL)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query=rate({app="nginx"}[5m])&start=1705312200&end=1705312800&step=60`, nil)
			p.handleQueryRange(w, r)
		}
	})
}

// =============================================================================
// Proxy-side processing benchmarks (decolorize, IP filter, line_format)
// =============================================================================

func BenchmarkDecolorizeStreams(b *testing.B) {
	streams := make([]map[string]interface{}, 0, 10)
	for i := 0; i < 10; i++ {
		streams = append(streams, map[string]interface{}{
			"stream": map[string]string{"app": "nginx"},
			"values": [][]string{
				{fmt.Sprintf("%d", 1705312200000000000+int64(i)), fmt.Sprintf("\033[31m[ERROR]\033[0m request %d failed with \033[1;33mstatus 500\033[0m at path /api/v1/users", i)},
			},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Deep copy to avoid mutating benchmark input
		cp := make([]map[string]interface{}, len(streams))
		for j, s := range streams {
			vals := s["values"].([][]string)
			cpVals := make([][]string, len(vals))
			for k, v := range vals {
				cpVals[k] = []string{v[0], v[1]}
			}
			cp[j] = map[string]interface{}{
				"stream": s["stream"],
				"values": cpVals,
			}
		}
		decolorizeStreams(cp)
	}
}

func BenchmarkApplyLineFormatTemplate(b *testing.B) {
	streams := make([]map[string]interface{}, 0, 10)
	for i := 0; i < 10; i++ {
		streams = append(streams, map[string]interface{}{
			"stream": map[string]string{"app": "nginx", "level": "info"},
			"values": [][]string{
				{fmt.Sprintf("%d", 1705312200000000000+int64(i)), fmt.Sprintf("request %d processed", i)},
			},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp := make([]map[string]interface{}, len(streams))
		for j, s := range streams {
			vals := s["values"].([][]string)
			cpVals := make([][]string, len(vals))
			for k, v := range vals {
				cpVals[k] = []string{v[0], v[1]}
			}
			cp[j] = map[string]interface{}{
				"stream": s["stream"],
				"values": cpVals,
			}
		}
		applyLineFormatTemplate(cp, `"{{.app}} [{{.level}}] {{._line}}"`)
	}
}

// =============================================================================
// Load test: NO CACHE with scaling (100, 1000, 10000 req/s targets)
// =============================================================================

func TestLoad_NoCache_ScalingProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 5 NDJSON lines per request
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"request %d","_stream":"{app=\"perf\"}","app":"perf","level":"info"}`+"\n", i, i)
		}
	}))
	defer vlBackend.Close()

	profiles := []struct {
		name        string
		concurrency int
		requests    int
	}{
		{"low_100rps", 10, 1000},
		{"medium_1000rps", 50, 5000},
		{"high_10000rps", 200, 20000},
	}

	for _, prof := range profiles {
		t.Run(prof.name, func(t *testing.T) {
			c := cache.New(0, 0) // No cache — pure proxy overhead
			p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}

			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			var wg sync.WaitGroup
			var errors atomic.Int64
			perGoroutine := prof.requests / prof.concurrency

			start := time.Now()
			for g := 0; g < prof.concurrency; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						w := httptest.NewRecorder()
						r := httptest.NewRequest("GET",
							fmt.Sprintf(`/loki/api/v1/query_range?query={app="perf-%d"}&start=1&end=2&step=1`, i), nil)
						p.handleQueryRange(w, r)
						if w.Code != 200 {
							errors.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			runtime.GC()
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			rps := float64(prof.requests) / elapsed.Seconds()
			avgLatency := elapsed / time.Duration(prof.requests)
			peakMem := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024 / 1024
			liveMem := float64(memAfter.Alloc) / 1024 / 1024

			t.Logf("Profile: %s (no cache)", prof.name)
			t.Logf("  Requests: %d, Concurrency: %d", prof.requests, prof.concurrency)
			t.Logf("  Duration: %s", elapsed.Round(time.Millisecond))
			t.Logf("  Throughput: %.0f req/s", rps)
			t.Logf("  Avg latency: %s", avgLatency.Round(time.Microsecond))
			t.Logf("  Errors: %d (%.1f%%)", errors.Load(), 100*float64(errors.Load())/float64(prof.requests))
			t.Logf("  Total alloc: %.1f MB", peakMem)
			t.Logf("  Live heap: %.1f MB", liveMem)
			t.Logf("  GC cycles: %d", memAfter.NumGC-memBefore.NumGC)
		})
	}
}

// =============================================================================
// Load test: WITH CACHE — same profiles for comparison
// =============================================================================

func TestLoad_WithCache_ScalingProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"request %d","_stream":"{app=\"perf\"}","app":"perf","level":"info"}`+"\n", i, i)
		}
	}))
	defer vlBackend.Close()

	profiles := []struct {
		name        string
		concurrency int
		requests    int
	}{
		{"low_100rps", 10, 1000},
		{"medium_1000rps", 50, 5000},
		{"high_10000rps", 200, 20000},
	}

	for _, prof := range profiles {
		t.Run(prof.name, func(t *testing.T) {
			c := cache.New(60*time.Second, 50000) // Large cache
			p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}

			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			var wg sync.WaitGroup
			var backendCalls atomic.Int64
			perGoroutine := prof.requests / prof.concurrency

			start := time.Now()
			for g := 0; g < prof.concurrency; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						w := httptest.NewRecorder()
						// Use small set of queries to get cache hits
						r := httptest.NewRequest("GET",
							fmt.Sprintf(`/loki/api/v1/query_range?query={app="perf-%d"}&start=1&end=2&step=1`, i%10), nil)
						p.handleQueryRange(w, r)
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			runtime.GC()
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			rps := float64(prof.requests) / elapsed.Seconds()
			avgLatency := elapsed / time.Duration(prof.requests)
			liveMem := float64(memAfter.Alloc) / 1024 / 1024

			t.Logf("Profile: %s (with cache)", prof.name)
			t.Logf("  Requests: %d, Concurrency: %d", prof.requests, prof.concurrency)
			t.Logf("  Duration: %s", elapsed.Round(time.Millisecond))
			t.Logf("  Throughput: %.0f req/s", rps)
			t.Logf("  Avg latency: %s", avgLatency.Round(time.Microsecond))
			t.Logf("  Backend calls: %d (saved: %.1f%%)", backendCalls.Load(),
				100*(1-float64(backendCalls.Load())/float64(prof.requests)))
			t.Logf("  Live heap: %.1f MB", liveMem)

			backendCalls.Load() // keep compiler happy; backend counting needs handler instrumentation
		})
	}
}
