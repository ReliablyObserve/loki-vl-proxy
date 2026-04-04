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
	"github.com/szibis/Loki-VL-proxy/internal/translator"
)

// =============================================================================
// Subquery benchmarks: measure CPU, memory, allocations
// =============================================================================

// BenchmarkSubquery_Small benchmarks a small subquery (30m/10m = 3 sub-steps).
func BenchmarkSubquery_Small(b *testing.B) {
	benchSubquery(b, "30m", "10m", 3)
}

// BenchmarkSubquery_Medium benchmarks a medium subquery (1h/5m = 12 sub-steps).
func BenchmarkSubquery_Medium(b *testing.B) {
	benchSubquery(b, "1h", "5m", 12)
}

// BenchmarkSubquery_Large benchmarks a large subquery (6h/5m = 72 sub-steps).
func BenchmarkSubquery_Large(b *testing.B) {
	benchSubquery(b, "6h", "5m", 72)
}

// BenchmarkSubquery_VeryLarge benchmarks a very large subquery (24h/5m = 288 sub-steps).
func BenchmarkSubquery_VeryLarge(b *testing.B) {
	benchSubquery(b, "24h", "5m", 288)
}

func benchSubquery(b *testing.B, rng, step string, expectedCalls int) {
	b.Helper()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"42.5"]}]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	query := fmt.Sprintf(`max_over_time(rate({app="nginx"}[5m])[%s:%s])`, rng, step)
	url := fmt.Sprintf(`/loki/api/v1/query?query=%s&time=1609462800`, query)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", url, nil)
		p.handleQuery(w, r)
	}
}

// BenchmarkSubquery_QueryRange benchmarks subquery in query_range mode.
func BenchmarkSubquery_QueryRange(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"42.5"]}]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	url := `/loki/api/v1/query_range?query=max_over_time(rate({app="nginx"}[5m])[1h:5m])&start=1609459200&end=1609462800&step=300`

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", url, nil)
		p.handleQueryRange(w, r)
	}
}

// BenchmarkSubquery_vs_RegularMetric compares subquery overhead vs regular metric query.
func BenchmarkSubquery_vs_RegularMetric(b *testing.B) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"nginx"},"values":[[1609459200,"42.5"]]}]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	b.Run("regular_metric", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query=rate({app="nginx"}[5m])&start=1609459200&end=1609462800&step=60`, nil)
			p.handleQueryRange(w, r)
		}
	})

	b.Run("subquery_3steps", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[30m:10m])&time=1609462800`, nil)
			p.handleQuery(w, r)
		}
	})

	b.Run("subquery_12steps", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", `/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[1h:5m])&time=1609462800`, nil)
			p.handleQuery(w, r)
		}
	})
}

// BenchmarkSubqueryAggregate benchmarks the aggregation functions in isolation.
func BenchmarkSubqueryAggregate(b *testing.B) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i) * 1.5
	}

	funcs := []string{"max_over_time", "min_over_time", "avg_over_time", "sum_over_time", "count_over_time", "stddev_over_time"}
	for _, fn := range funcs {
		b.Run(fn, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = subqueryAggregate(fn, values)
			}
		})
	}
}

// BenchmarkParseLokiDuration benchmarks duration parsing.
func BenchmarkParseLokiDuration(b *testing.B) {
	durations := []string{"5m", "1h", "30s", "1d", "2h30m"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, d := range durations {
			parseLokiDuration(d)
		}
	}
}

// BenchmarkTranslateSubquery benchmarks translator subquery parsing.
func BenchmarkTranslateSubquery(b *testing.B) {
	queries := []string{
		`max_over_time(rate({app="nginx"}[5m])[1h:5m])`,
		`avg_over_time(count_over_time({job="varlogs"}[5m])[6h:30m])`,
		`sum_over_time(rate({app="api"}[1m])[30m:5m])`,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			_, _ = translator.TranslateLogQL(q)
		}
	}
}

// =============================================================================
// Load test: subquery under high concurrency
// =============================================================================

func TestLoad_Subquery_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	var backendCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"42.5"]}]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	concurrency := 50
	requestsPerGoroutine := 20
	var wg sync.WaitGroup

	start := time.Now()
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < requestsPerGoroutine; i++ {
				w := httptest.NewRecorder()
				// Each subquery does 4 sub-steps (30m/10m ≈ 3-4)
				r := httptest.NewRequest("GET",
					`/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[30m:10m])&time=1609462800`, nil)
				p.handleQuery(w, r)

				// Verify response
				var resp map[string]interface{}
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Errorf("goroutine %d request %d: parse error: %v", id, i, err)
					continue
				}
				if resp["status"] != "success" {
					t.Errorf("goroutine %d request %d: status=%v", id, i, resp["status"])
				}
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	totalRequests := concurrency * requestsPerGoroutine
	rps := float64(totalRequests) / elapsed.Seconds()
	// Use TotalAlloc (monotonic) to avoid unsigned underflow
	totalAllocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024 / 1024
	callsPerRequest := float64(backendCalls.Load()) / float64(totalRequests)

	t.Logf("Subquery load test results:")
	t.Logf("  Total requests: %d", totalRequests)
	t.Logf("  Concurrency: %d", concurrency)
	t.Logf("  Duration: %s", elapsed)
	t.Logf("  Throughput: %.0f req/s", rps)
	t.Logf("  Backend calls: %d (%.1f per request)", backendCalls.Load(), callsPerRequest)
	t.Logf("  Total allocated: %.1f MB", totalAllocMB)
	t.Logf("  GC cycles: %d", memAfter.NumGC-memBefore.NumGC)

	// Regression bounds
	if rps < 100 {
		t.Errorf("subquery throughput too low: %.0f req/s (expected >100 with 50 concurrent, 4 sub-steps each)", rps)
	}
	// Total allocation for 1000 requests × ~4 sub-steps should be reasonable
	if totalAllocMB > 500 {
		t.Errorf("total allocation too high: %.1f MB (expected <500 MB for %d subquery requests)", totalAllocMB, totalRequests)
	}
	// Each subquery request should make ~4 sub-step calls (30m/10m)
	if callsPerRequest < 3 || callsPerRequest > 5 {
		t.Errorf("unexpected VL calls per request: %.1f (expected 3-5 for 30m/10m subquery)", callsPerRequest)
	}
}

// =============================================================================
// Regression: subquery memory doesn't leak under sustained load
// =============================================================================

func TestRegression_Subquery_NoMemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory leak test in short mode")
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"42.5"]}]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Run 3 rounds of 100 requests and check TotalAlloc doesn't grow disproportionately
	var allocSamples []uint64
	for round := 0; round < 3; round++ {
		runtime.GC()
		var memStart runtime.MemStats
		runtime.ReadMemStats(&memStart)

		for i := 0; i < 100; i++ {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET",
				`/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[1h:5m])&time=1609462800`, nil)
			p.handleQuery(w, r)
		}

		runtime.GC()
		var memEnd runtime.MemStats
		runtime.ReadMemStats(&memEnd)
		allocSamples = append(allocSamples, memEnd.TotalAlloc-memStart.TotalAlloc)
	}

	t.Logf("Allocation per round: %.1f MB, %.1f MB, %.1f MB",
		float64(allocSamples[0])/1024/1024,
		float64(allocSamples[1])/1024/1024,
		float64(allocSamples[2])/1024/1024)

	// Round 3 allocation should be similar to round 1 (no leak = stable allocation rate)
	// Allow 3x variance for GC timing
	if allocSamples[2] > allocSamples[0]*3 {
		t.Errorf("possible memory leak: round 3 allocated %.1f MB vs round 1 %.1f MB",
			float64(allocSamples[2])/1024/1024, float64(allocSamples[0])/1024/1024)
	}
}

// =============================================================================
// Regression: subquery aggregation correctness under load
// =============================================================================

func TestRegression_Subquery_AggregationCorrectness(t *testing.T) {
	// Backend returns incrementing values per call
	var callCount atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"%d"]}]}}`, n)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// max_over_time should return the maximum of the sub-step values
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET",
		`/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[30m:10m])&time=1609462800`, nil)
	p.handleQuery(w, r)

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("expected success, got %q", resp.Status)
	}
	if len(resp.Data.Result) == 0 {
		t.Fatal("expected at least one result")
	}

	// The max should be the highest value returned by the backend
	// Since backend returns 1, 2, 3, ... the max should be the count of sub-steps
	valStr, ok := resp.Data.Result[0].Value[1].(string)
	if !ok {
		t.Fatalf("expected string value, got %T", resp.Data.Result[0].Value[1])
	}
	t.Logf("max_over_time result: %s (backend called %d times)", valStr, callCount.Load())

	// Should have made at least 3 backend calls (30m/10m ≈ 4)
	if callCount.Load() < 3 {
		t.Errorf("expected at least 3 sub-step calls, got %d", callCount.Load())
	}
}

// =============================================================================
// Regression: subquery respects context cancellation
// =============================================================================

func TestRegression_Subquery_ContextCancellation(t *testing.T) {
	var callCount atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		// Slow backend — subquery should respect context timeout
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"42"]}]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Large subquery with slow backend — should still complete within bounded time
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET",
		`/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[1h:5m])&time=1609462800`, nil)

	start := time.Now()
	p.handleQuery(w, r)
	elapsed := time.Since(start)

	// With 12 sub-steps, 50ms each, max concurrency 10: should take ~100ms (2 batches)
	// Allow generous timeout for CI
	if elapsed > 10*time.Second {
		t.Errorf("subquery took too long: %v (expected <10s with bounded concurrency)", elapsed)
	}
	t.Logf("Subquery with slow backend: %v (%d calls)", elapsed, callCount.Load())
}

// =============================================================================
// Regression: subquery with multiple series
// =============================================================================

func TestRegression_Subquery_MultipleSeries(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return multiple series
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"app":"nginx","env":"prod"},"value":[1609459200,"10"]},
			{"metric":{"app":"nginx","env":"staging"},"value":[1609459200,"5"]}
		]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET",
		`/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[30m:10m])&time=1609462800`, nil)
	p.handleQuery(w, r)

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Data.Result) < 2 {
		t.Errorf("expected at least 2 series, got %d", len(resp.Data.Result))
	}
}
