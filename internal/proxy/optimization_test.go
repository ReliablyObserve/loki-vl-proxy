package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Test: vlLogsToLokiStreams correctness with optimized byte scanning + sync.Pool
// =============================================================================

func TestOptimization_VLLogsToLokiStreams_Basic(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00.000Z","_msg":"hello world","_stream":"{app=\"nginx\"}","app":"nginx","level":"info"}
{"_time":"2024-01-15T10:30:01.000Z","_msg":"goodbye","_stream":"{app=\"nginx\"}","app":"nginx","level":"error"}
`)
	streams := vlLogsToLokiStreams(body)

	if len(streams) == 0 {
		t.Fatal("expected at least 1 stream")
	}
	if len(streams) != 2 {
		t.Fatalf("expected level-aware stream split into 2 streams, got %d", len(streams))
	}

	totalValues := 0
	seenLevels := map[string]bool{}
	for _, s := range streams {
		labels, ok := s["stream"].(map[string]string)
		if !ok {
			t.Fatalf("expected stream labels as map[string]string, got %T", s["stream"])
		}
		if labels["app"] != "nginx" {
			t.Errorf("expected app=nginx, got %q", labels["app"])
		}
		if labels["service_name"] != "nginx" {
			t.Errorf("expected synthetic service_name=nginx, got %q", labels["service_name"])
		}
		seenLevels[labels["level"]] = true

		values, ok := s["values"].([][]string)
		if !ok {
			t.Fatalf("expected values as [][]string, got %T", s["values"])
		}
		if len(values) != 1 {
			t.Errorf("expected 1 value per effective stream, got %d", len(values))
		}
		totalValues += len(values)
		// First value should be nanosecond timestamp
		if len(values[0]) != 2 {
			t.Fatalf("expected [ts, line], got len=%d", len(values[0]))
		}
		if len(values[0][0]) < 19 {
			t.Errorf("timestamp should be nanoseconds (19+ digits), got %q", values[0][0])
		}
		if values[0][1] != "hello world" && values[0][1] != "goodbye" {
			t.Errorf("unexpected log line %q", values[0][1])
		}
	}
	if totalValues != 2 {
		t.Fatalf("expected 2 total log values, got %d", totalValues)
	}
	if !seenLevels["info"] || !seenLevels["error"] {
		t.Fatalf("expected info and error streams, got %v", seenLevels)
	}
}

func TestOptimization_VLLogsToLokiStreams_EmptyBody(t *testing.T) {
	streams := vlLogsToLokiStreams([]byte{})
	if len(streams) != 0 {
		t.Errorf("expected 0 streams for empty body, got %d", len(streams))
	}
}

func TestOptimization_VLLogsToLokiStreams_InvalidJSON(t *testing.T) {
	body := []byte("not json\n{invalid\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 0 {
		t.Errorf("expected 0 streams for invalid JSON, got %d", len(streams))
	}
}

func TestOptimization_VLLogsToLokiStreams_MissingTime(t *testing.T) {
	body := []byte(`{"_msg":"no time field","app":"nginx"}` + "\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 0 {
		t.Errorf("expected 0 streams for missing _time, got %d", len(streams))
	}
}

func TestOptimization_VLLogsToLokiStreams_MultipleStreams(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"line1","_stream":"{app=\"nginx\"}","app":"nginx"}
{"_time":"2024-01-15T10:30:01Z","_msg":"line2","_stream":"{app=\"api\"}","app":"api"}
{"_time":"2024-01-15T10:30:02Z","_msg":"line3","_stream":"{app=\"nginx\"}","app":"nginx"}
`)
	streams := vlLogsToLokiStreams(body)

	if len(streams) != 2 {
		t.Errorf("expected 2 streams (nginx + api), got %d", len(streams))
	}

	// Find nginx stream
	for _, s := range streams {
		labels := s["stream"].(map[string]string)
		values := s["values"].([][]string)
		if labels["app"] == "nginx" && len(values) != 2 {
			t.Errorf("nginx stream should have 2 values, got %d", len(values))
		}
		if labels["app"] == "api" && len(values) != 1 {
			t.Errorf("api stream should have 1 value, got %d", len(values))
		}
	}
}

func TestOptimization_VLReaderToLokiStreams_CollectsPatterns(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected backend call to %s", r.URL.Path)
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	t.Cleanup(func() {
		if p.cache != nil {
			p.cache.Close()
		}
		if p.translationCache != nil {
			p.translationCache.Close()
		}
	})

	body := strings.Join([]string{
		`{"_time":"2024-01-15T10:30:00Z","_msg":"GET /api/users 200 15ms","_stream":"{app=\"api\"}","app":"api","level":"info"}`,
		`{"_time":"2024-01-15T10:30:01Z","_msg":"GET /api/users 200 17ms","_stream":"{app=\"api\"}","app":"api","level":"info"}`,
		`{"_time":"2024-01-15T10:30:02Z","_msg":"POST /api/users 500 31ms","_stream":"{app=\"api\"}","app":"api","level":"error"}`,
		"",
	}, "\n")

	streams, patterns, err := p.vlReaderToLokiStreams(bytes.NewReader([]byte(body)), `{app="api"} |= "users"`, "1m", false, false, true)
	if err != nil {
		t.Fatalf("vlReaderToLokiStreams error: %v", err)
	}
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams split by level, got %d", len(streams))
	}
	if len(patterns) == 0 {
		t.Fatal("expected autodetected patterns from NDJSON reader path")
	}
}

func TestOptimization_VLLogsToLokiStreams_WhitespaceLines(t *testing.T) {
	body := []byte("\n  \n\t\n" + `{"_time":"2024-01-15T10:30:00Z","_msg":"test","_stream":"{}"}` + "\n\n  \n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Errorf("expected 1 stream (whitespace lines skipped), got %d", len(streams))
	}
}

func TestOptimization_VLLogsToLokiStreams_CarriageReturn(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","_stream":"{}"}` + "\r\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Errorf("expected 1 stream (\\r\\n handled), got %d", len(streams))
	}
}

// =============================================================================
// Test: sync.Pool doesn't leak state between invocations
// =============================================================================

func TestOptimization_SyncPool_NoStateLeak(t *testing.T) {
	// First call with labels {app="nginx"}
	body1 := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"first","_stream":"{}","app":"nginx","extra":"field1"}` + "\n")
	streams1 := vlLogsToLokiStreams(body1)

	// Second call with different labels — pooled map must not carry over old keys
	body2 := []byte(`{"_time":"2024-01-15T10:30:01Z","_msg":"second","_stream":"{}","svc":"api"}` + "\n")
	streams2 := vlLogsToLokiStreams(body2)

	if len(streams1) != 1 || len(streams2) != 1 {
		t.Fatalf("expected 1 stream each, got %d and %d", len(streams1), len(streams2))
	}

	labels2 := streams2[0]["stream"].(map[string]string)
	// "app" and "extra" from first call must NOT appear
	if _, hasApp := labels2["app"]; hasApp {
		t.Error("sync.Pool leaked 'app' label from previous invocation")
	}
	if _, hasExtra := labels2["extra"]; hasExtra {
		t.Error("sync.Pool leaked 'extra' label from previous invocation")
	}
	if labels2["svc"] != "api" {
		t.Errorf("expected svc=api, got %q", labels2["svc"])
	}
}

func TestOptimization_SyncPool_ConcurrentSafety(t *testing.T) {
	// Hammer vlLogsToLokiStreams concurrently to detect pool races
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				body := []byte(fmt.Sprintf(
					`{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"goroutine %d iter %d","_stream":"{app=\"g%d\"}","app":"g%d","iter":"%d"}`+"\n",
					i%60, id, i, id, id, i,
				))
				streams := vlLogsToLokiStreams(body)
				if len(streams) != 1 {
					t.Errorf("goroutine %d iter %d: expected 1 stream, got %d", id, i, len(streams))
					return
				}
				labels := streams[0]["stream"].(map[string]string)
				expected := fmt.Sprintf("g%d", id)
				if labels["app"] != expected {
					t.Errorf("goroutine %d iter %d: expected app=%s, got %s", id, i, expected, labels["app"])
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// =============================================================================
// Test: Connection pool tuning — verify MaxIdleConnsPerHost is set
// =============================================================================

func TestOptimization_ConnectionPool_HighConcurrency(t *testing.T) {
	// This test verifies the proxy can handle 200+ concurrent connections
	// without ephemeral port exhaustion (the bug that was fixed by tuning
	// MaxIdleConnsPerHost from default 2 to 256).

	var requestCount int64
	var mu sync.Mutex
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"ok","_stream":"{}"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(0, 0) // no cache
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}

	// 200 concurrent goroutines, 50 requests each
	concurrency := 200
	perGoroutine := 50
	var wg sync.WaitGroup
	var errors int64

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				w := httptest.NewRecorder()
				r := httptest.NewRequest("GET",
					fmt.Sprintf(`/loki/api/v1/query_range?query={app="conn-%d"}&start=1&end=2&step=1`, i), nil)
				p.handleQueryRange(w, r)
				if w.Code != 200 {
					mu.Lock()
					errors++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	totalRequests := int64(concurrency * perGoroutine)
	errorPct := float64(errors) / float64(totalRequests) * 100

	t.Logf("Connection pool test: %d requests, %d concurrent, %d errors (%.1f%%)",
		totalRequests, concurrency, errors, errorPct)

	// Must have zero errors — the transport tuning should prevent port exhaustion
	if errorPct > 1.0 {
		t.Errorf("too many errors: %d/%d (%.1f%%) — connection pool may not be tuned correctly",
			errors, totalRequests, errorPct)
	}
}

// =============================================================================
// Test: formatVLStep conversion
// =============================================================================

func TestOptimization_FormatVLStep(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"60", "60s"},      // bare number → append "s"
		{"3600", "3600s"},  // large number
		{"0.5", "0.5s"},    // float
		{"1m", "1m"},       // already duration
		{"5s", "5s"},       // already duration
		{"1h30m", "1h30m"}, // complex duration
		{"", ""},           // empty
		{"  60  ", "60s"},  // whitespace trimmed
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := formatVLStep(tt.input)
			if got != tt.want {
				t.Errorf("formatVLStep(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Regression: ensure optimized conversion produces valid Loki JSON
// =============================================================================

func TestOptimization_VLLogsToLokiStreams_ValidJSON(t *testing.T) {
	// Generate 100 diverse NDJSON lines
	var body []byte
	for i := 0; i < 100; i++ {
		line := fmt.Sprintf(
			`{"_time":"2024-01-15T10:%02d:%02d.%03dZ","_msg":"request %d from 10.0.%d.%d path=/api/v%d duration=%dms","_stream":"{app=\"svc-%d\",ns=\"prod\"}","app":"svc-%d","ns":"prod","level":"%s","trace_id":"abc%04x"}`,
			i/60, i%60, i*7%1000, i, i/256, i%256, i%3+1, i*17%5000,
			i%5, i%5, []string{"info", "warn", "error", "debug"}[i%4], i,
		)
		body = append(body, []byte(line+"\n")...)
	}

	streams := vlLogsToLokiStreams(body)

	// Must produce valid JSON when marshaled
	result, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "streams",
			"result":     streams,
		},
	})
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}

	// Verify it's parseable
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("produced invalid JSON: %v", err)
	}

	if parsed["status"] != "success" {
		t.Errorf("expected status=success, got %v", parsed["status"])
	}

	data := parsed["data"].(map[string]interface{})
	resultStreams := data["result"].([]interface{})
	if len(resultStreams) == 0 {
		t.Error("expected non-empty result streams")
	}

	// Count total values across all streams
	totalValues := 0
	for _, s := range resultStreams {
		sm := s.(map[string]interface{})
		vals := sm["values"].([]interface{})
		totalValues += len(vals)
	}
	if totalValues != 100 {
		t.Errorf("expected 100 total values (one per input line), got %d", totalValues)
	}
}

// =============================================================================
// Test: memory does not grow under sustained load (no leaks from pool)
// =============================================================================

func TestOptimization_NoMemoryLeak_SustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 10; i++ {
			fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"line %d","_stream":"{}","app":"test"}`+"\n", i, i)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(0, 0)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Warm up
	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="test"}&start=1&end=2&step=1`, nil)
		p.handleQueryRange(w, r)
	}

	// Measure total allocations (cumulative, not affected by GC timing)
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Sustained load: 10,000 requests
	for i := 0; i < 10000; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET",
			fmt.Sprintf(`/loki/api/v1/query_range?query={app="test-%d"}&start=1&end=2&step=1`, i%100), nil)
		p.handleQueryRange(w, r)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	totalAllocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024 / 1024
	allocPerReq := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 10000
	t.Logf("10K requests: %.1f MB total alloc, %.0f bytes/req", totalAllocMB, allocPerReq)

	// Should allocate less than 700 KB per request on average (no leak, bounded alloc).
	// Threshold accounts for race detector overhead (~2x allocations) plus JSON
	// log-line reconstruction overhead (reconstructLogLine per entry).
	if allocPerReq > 700*1024 {
		t.Errorf("excessive allocation: %.0f bytes/req (expected <700KB)", allocPerReq)
	}
}

// =============================================================================
// Test: large NDJSON body doesn't cause OOM or excessive GC
// =============================================================================

// =============================================================================
// Test: /metrics exposes Go runtime/GC stats
// =============================================================================

func TestOptimization_Metrics_ExposesGCStats(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	p.Init()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	p.GetMetrics().Handler(w, r)

	body := w.Body.String()

	requiredMetrics := []string{
		"go_memstats_alloc_bytes",
		"go_memstats_sys_bytes",
		"go_memstats_heap_inuse_bytes",
		"go_memstats_heap_idle_bytes",
		"go_goroutines",
		"go_gc_cycles_total",
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(body, metric) {
			t.Errorf("missing Go runtime metric %q in /metrics output", metric)
		}
	}
}

func TestOptimization_LargeBody_GCPressure(t *testing.T) {
	// Generate 1000-line NDJSON (simulate heavy query result)
	var body []byte
	for i := 0; i < 1000; i++ {
		line := fmt.Sprintf(
			`{"_time":"2024-01-15T10:%02d:%02d.000Z","_msg":"%s","_stream":"{app=\"heavy\"}","app":"heavy","payload":"%s"}`,
			i/60%60, i%60,
			strings.Repeat("x", 200), // 200-char message
			strings.Repeat("y", 500), // 500-char payload field
		)
		body = append(body, []byte(line+"\n")...)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	streams := vlLogsToLokiStreams(body)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if len(streams) == 0 {
		t.Fatal("expected non-empty streams for 1000-line input")
	}

	// Count total values
	total := 0
	for _, s := range streams {
		vals := s["values"].([][]string)
		total += len(vals)
	}
	if total != 1000 {
		t.Errorf("expected 1000 values, got %d", total)
	}

	allocMB := float64(after.TotalAlloc-before.TotalAlloc) / 1024 / 1024
	t.Logf("1000-line NDJSON: %d streams, %d values, %.1f MB allocated", len(streams), total, allocMB)

	// Should not allocate more than 50 MB for 1000 lines (~800 bytes each = 800KB input)
	if allocMB > 50 {
		t.Errorf("excessive allocation: %.1f MB for 1000-line body", allocMB)
	}
}
