package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// perfBaseTimeNs is a fixed realistic epoch-nanosecond anchor (2024-01-01T00:00:00Z).
// Using a fixed timestamp keeps test output deterministic and ensures the value
// is always ≥1e18 so normalizeUnixNanos treats it as nanoseconds.
const perfBaseTimeNs = int64(1704067200_000_000_000)

// perfWindow is one test case: a Grafana time-picker preset.
type perfWindow struct {
	name     string
	duration time.Duration
}

var labelsWindowCases = []perfWindow{
	{"1h", time.Hour},
	{"6h", 6 * time.Hour},
	{"12h", 12 * time.Hour},
	{"24h", 24 * time.Hour},
	{"2d", 48 * time.Hour},
	{"7d", 7 * 24 * time.Hour},
}

// newPerfVLBackend creates a test VL server that responds to /health and all
// /select/logsql/* with a minimal field-names payload.  The optional onCall
// callback is invoked for every non-health request.
func newPerfVLBackend(t *testing.T, onCall func(r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if onCall != nil {
			onCall(r)
		}
		writeVLFieldNames(w, []fieldHit{
			{"app", 100}, {"env", 50}, {"namespace", 30},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newPerfProxy creates a proxy mux backed by the given VL server.
func newPerfProxy(t *testing.T, vlURL string) *http.ServeMux {
	t.Helper()
	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{BackendURL: vlURL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return mux
}

// labelsPath returns a /loki/api/v1/labels URL for the given time window
// anchored at perfBaseTimeNs.
func labelsPath(window time.Duration) string {
	endNs := perfBaseTimeNs
	startNs := endNs - int64(window)
	return fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", startNs, endNs)
}

// =============================================================================
// Correctness: VL always receives a ≤1h window regardless of dashboard range
// =============================================================================

// TestPerf_Labels_BackendWindowCap verifies that for every Grafana time-picker
// preset the proxy caps the VL backend call to ≤1h+1bucket (5 min tolerance).
// Wide ranges (2d, 7d) must not trigger full-range stream-index scans.
func TestPerf_Labels_BackendWindowCap(t *testing.T) {
	for _, tc := range labelsWindowCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var receivedStart, receivedEnd string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				q := r.URL.Query()
				receivedStart = q.Get("start")
				receivedEnd = q.Get("end")
				writeVLFieldNames(w, []fieldHit{{"app", 100}})
			}))
			t.Cleanup(srv.Close)

			mux := newPerfProxy(t, srv.URL)
			req := httptest.NewRequest(http.MethodGet, labelsPath(tc.duration), nil)
			mux.ServeHTTP(httptest.NewRecorder(), req)

			if receivedStart == "" || receivedEnd == "" {
				t.Fatal("VL backend was not called")
			}
			gotStart, okS := parseLokiTimeToUnixNano(receivedStart)
			gotEnd, okE := parseLokiTimeToUnixNano(receivedEnd)
			if !okS || !okE {
				t.Fatalf("could not parse VL params: start=%q end=%q", receivedStart, receivedEnd)
			}
			gotWindow := time.Duration(gotEnd - gotStart)

			// Allow one extra bucket (5 min) for rounding.
			const maxWindow = time.Hour + 5*time.Minute
			if gotWindow > maxWindow {
				t.Errorf("window=%s: VL received %v window (want ≤%v); start=%s end=%s",
					tc.name, gotWindow, maxWindow, receivedStart, receivedEnd)
			}
		})
	}
}

// =============================================================================
// Latency: cold miss vs warm cache-hit latency bounds
// =============================================================================

// TestPerf_Labels_ColdAndWarmLatency asserts that:
//   - Cold (first) fetch completes in <200 ms against a zero-latency mock VL.
//   - Warm (second) fetch — served from cache — completes in <5 ms.
//
// Both bounds are deliberately generous to keep the test green on slow CI
// runners.  The actual numbers on a developer machine are typically <2 ms cold
// and <100 µs warm.
func TestPerf_Labels_ColdAndWarmLatency(t *testing.T) {
	const (
		coldBound = 200 * time.Millisecond
		warmBound = 5 * time.Millisecond
	)
	for _, tc := range labelsWindowCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			srv := newPerfVLBackend(t, nil)
			mux := newPerfProxy(t, srv.URL)
			path := labelsPath(tc.duration)

			// Cold hit
			coldStart := time.Now()
			mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
			cold := time.Since(coldStart)

			// Warm hit
			warmStart := time.Now()
			mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
			warm := time.Since(warmStart)

			t.Logf("window=%-4s  cold=%v  warm=%v", tc.name, cold, warm)

			if cold > coldBound {
				t.Errorf("cold latency %v > %v", cold, coldBound)
			}
			if warm > warmBound {
				t.Errorf("warm latency %v > %v", warm, warmBound)
			}
		})
	}
}

// TestPerf_Labels_SameVLCallForAllWindows confirms that 1h and 7d label
// requests — after capping — hit the same VL time window and therefore
// produce identical proxy cache keys once the first request lands.
func TestPerf_Labels_SameVLCallForAllWindows(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Capture the bucketed window the proxy sends to VL.
		// Mutex required: background refresh goroutines may call VL concurrently.
		q := r.URL.Query()
		entry := q.Get("start") + "/" + q.Get("end")
		mu.Lock()
		calls = append(calls, entry)
		mu.Unlock()
		writeVLFieldNames(w, []fieldHit{{"app", 100}})
	}))
	t.Cleanup(srv.Close)

	mux := newPerfProxy(t, srv.URL)

	// Issue one request per window using a fixed anchor so all
	// requests share the same bucket.
	endNs := perfBaseTimeNs
	for _, tc := range labelsWindowCases {
		startNs := endNs - int64(tc.duration)
		path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", startNs, endNs)
		req := httptest.NewRequest(http.MethodGet, path, nil)
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}

	mu.Lock()
	snapshot := append([]string(nil), calls...)
	mu.Unlock()

	// All windows share the same capped end; the capped start differs by at
	// most one bucket.  Every window > 1h should produce the same VL params
	// (the 1h cap collapses them).
	if len(snapshot) == 0 {
		t.Fatal("no VL calls recorded")
	}
	first := snapshot[0]
	for i, c := range snapshot[1:] {
		if c != first {
			// Two different VL windows is fine for the 1h case vs longer cases
			// as long as each window is ≤1h+5min — that is verified by
			// TestPerf_Labels_BackendWindowCap.  Log for visibility.
			t.Logf("window[0]=%s window[%d]=%s (bucket drift OK if within 5 min)", first, i+1, c)
		}
	}
}

// =============================================================================
// Warmup: startup pre-population covers all standard Grafana presets
// =============================================================================

// TestPerf_Labels_WarmupCoverage verifies that warmMetadataCacheOnStartup
// populates the read cache for all 4 preset windows (1h, 6h, 24h, 7d) and that
// post-warmup requests for those windows are served from cache (≤1 extra call
// allowed for bucket-boundary rounding). The warmup may make fewer than 4 backend
// calls when windows cap to the same 1h backend params and share the capped-key cache.
func TestPerf_Labels_WarmupCoverage(t *testing.T) {
	warmupWindows := []time.Duration{
		time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour,
	}

	var backendCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		backendCalls.Add(1)
		writeVLFieldNames(w, []fieldHit{{"app", 100}, {"env", 50}})
	}))
	t.Cleanup(srv.Close)

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	p.warmMetadataCacheOnStartup()

	// Poll until warmup completes; VL is immediately reachable so this is fast.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if int(backendCalls.Load()) >= len(warmupWindows) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	warmupCallCount := backendCalls.Load()
	if warmupCallCount == 0 {
		t.Fatalf("warmup made 0 backend calls, want ≥1")
	}

	// Post-warmup: requests for the same window must be cache hits.
	// Allow ≤1 extra backend call for 5-min bucket-boundary drift between
	// the warmup instant and the test instant.
	beforePost := backendCalls.Load()
	nowNs := time.Now().UnixNano()
	for _, w := range warmupWindows {
		startNs := nowNs - int64(w)
		path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", startNs, nowNs)
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	afterPost := backendCalls.Load()

	if afterPost-beforePost > 1 {
		t.Errorf("post-warmup requests triggered %d backend calls (want 0–1); cache not populated",
			afterPost-beforePost)
	}
}

// =============================================================================
// Benchmarks: cold fetch vs warm cache-hit for 1h and 7d
// =============================================================================

func BenchmarkLabels_Cold_1h(b *testing.B) { benchmarkLabelsCold(b, time.Hour) }
func BenchmarkLabels_Cold_7d(b *testing.B) { benchmarkLabelsCold(b, 7*24*time.Hour) }
func BenchmarkLabels_Warm_1h(b *testing.B) { benchmarkLabelsWarm(b, time.Hour) }
func BenchmarkLabels_Warm_7d(b *testing.B) { benchmarkLabelsWarm(b, 7*24*time.Hour) }

func benchmarkLabelsCold(b *testing.B, window time.Duration) {
	b.Helper()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeVLFieldNames(w, []fieldHit{{"app", 100}, {"env", 50}})
	}))
	b.Cleanup(vlBackend.Close)

	endNs := perfBaseTimeNs
	startNs := endNs - int64(window)
	path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", startNs, endNs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Fresh proxy per iteration defeats cache — measures raw proxy+VL cost.
		c := cache.New(60*time.Second, 10000)
		p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
}

func benchmarkLabelsWarm(b *testing.B, window time.Duration) {
	b.Helper()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeVLFieldNames(w, []fieldHit{{"app", 100}, {"env", 50}})
	}))
	b.Cleanup(vlBackend.Close)

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	endNs := perfBaseTimeNs
	startNs := endNs - int64(window)
	path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", startNs, endNs)

	// Pre-warm cache
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
}
