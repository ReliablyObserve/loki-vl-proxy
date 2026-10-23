package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Lifecycle leak guards: building a proxy, starting it, serving traffic and
// shutting it down must hand every goroutine back. A process or a test that
// builds proxies repeatedly leaks per instance otherwise.

const (
	leakFlagPatternsPersist uint8 = 1 << iota
	leakFlagLabelIndexPersist
	leakFlagColdBackend
	leakFlagLabelsWarm
	leakFlagRateLimit
	leakFlagAll = leakFlagPatternsPersist | leakFlagLabelIndexPersist | leakFlagColdBackend | leakFlagLabelsWarm | leakFlagRateLimit
)

const leakSettleDeadline = 5 * time.Second

// goroutineCreators counts live goroutines by the line that started them
// ("created by X", without the goroutine id).
func goroutineCreators() map[string]int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]int{}
	for _, block := range strings.Split(string(buf), "\n\n") {
		key := ""
		lines := strings.Split(strings.TrimSpace(block), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "created by ") {
				key = line
				if i := strings.Index(key, " in goroutine "); i >= 0 {
					key = key[:i]
				}
			}
		}
		if key == "" && len(lines) > 1 {
			key = "(no creator) " + lines[1]
		}
		if key != "" {
			out[key]++
		}
	}
	return out
}

// settleGoroutines waits for the goroutine count to fall back to baseline and
// returns "" on success, or a report naming the constructors that kept
// goroutines alive.
func settleGoroutines(baseline int, before map[string]int) string {
	deadline := time.Now().Add(leakSettleDeadline)
	for {
		runtime.GC()
		if runtime.NumGoroutine() <= baseline {
			return ""
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	after := goroutineCreators()
	var lines []string
	for key, n := range after {
		if grown := n - before[key]; grown > 0 {
			lines = append(lines, fmt.Sprintf("  +%d %s", grown, key))
		}
	}
	sort.Strings(lines)
	return fmt.Sprintf("%d goroutines alive, baseline %d; grown creators:\n%s",
		runtime.NumGoroutine(), baseline, strings.Join(lines, "\n"))
}

// leakStubBackend answers every VictoriaLogs endpoint immediately with an
// empty, well-formed body.
func leakStubBackend() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/health":
			_, _ = w.Write([]byte("OK"))
		case strings.Contains(r.URL.Path, "/stream_field_names"),
			strings.Contains(r.URL.Path, "/field_names"),
			strings.Contains(r.URL.Path, "/stream_field_values"),
			strings.Contains(r.URL.Path, "/field_values"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}
	}))
}

func leakConfig(flags uint8, backendURL string, dir string, c *cache.Cache) Config {
	cfg := Config{
		BackendURL:             backendURL,
		Cache:                  c,
		LogLevel:               "error",
		DisableLabelsCacheWarm: flags&leakFlagLabelsWarm == 0,
	}
	if flags&leakFlagPatternsPersist != 0 {
		cfg.PatternsPersistPath = filepath.Join(dir, "patterns.json")
		cfg.PatternsPersistInterval = 20 * time.Millisecond
	}
	if flags&leakFlagLabelIndexPersist != 0 {
		cfg.LabelValuesIndexedCache = true
		cfg.LabelValuesIndexPersistPath = filepath.Join(dir, "label-values-index.json")
		cfg.LabelValuesIndexPersistInterval = 20 * time.Millisecond
	}
	if flags&leakFlagColdBackend != 0 {
		cfg.ColdBackend = ColdBackendConfig{
			Enabled:         true,
			URL:             backendURL,
			ManifestRefresh: 50 * time.Millisecond,
			Timeout:         time.Second,
		}
	}
	if flags&leakFlagLabelsWarm != 0 {
		cfg.LabelCacheTTL = 400 * time.Millisecond
	}
	if flags&leakFlagRateLimit != 0 {
		cfg.RatePerSecond = 1000
		cfg.RateBurst = 100
		cfg.MaxConcurrent = 10
	}
	return cfg
}

// runProxyLifecycle runs New -> Init -> one real request -> Shutdown for the
// feature set in flags and returns a leak report, or "" when every goroutine
// came back.
func runProxyLifecycle(t testing.TB, flags uint8) string {
	t.Helper()
	runtime.GC()
	baseline := runtime.NumGoroutine()
	before := goroutineCreators()

	stub := leakStubBackend()
	callerCache := cache.New(time.Minute, 1000)
	p, err := New(leakConfig(flags, stub.URL, t.TempDir(), callerCache))
	if err != nil {
		stub.Close()
		callerCache.Close()
		t.Fatalf("New(flags=%#x): %v", flags, err)
	}
	p.Init()

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	now := time.Now()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d",
		now.Add(-time.Hour).UnixNano(), now.UnixNano()), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("flags=%#x: /labels returned %d: %s", flags, rec.Code, rec.Body.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("flags=%#x: Shutdown: %v", flags, err)
	}
	// The caches handed in by the caller belong to the caller, and closing the
	// stub ends the idle upstream connections of the proxy's HTTP client.
	callerCache.Close()
	stub.Close()

	return settleGoroutines(baseline, before)
}

// conformance: shutdown-releases-background-work
func TestProxyLifecycle_ReturnsEveryGoroutine(t *testing.T) {
	cases := []struct {
		name  string
		flags uint8
	}{
		{"minimal", 0},
		{"patterns_persist", leakFlagPatternsPersist},
		{"label_index_persist", leakFlagLabelIndexPersist},
		{"cold_backend", leakFlagColdBackend},
		{"labels_warm", leakFlagLabelsWarm},
		{"rate_limit", leakFlagRateLimit},
		{"all", leakFlagAll},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if report := runProxyLifecycle(t, tc.flags); report != "" {
				t.Fatalf("proxy lifecycle leaked: %s", report)
			}
		})
	}
}

// The startup label warm-up waits for VictoriaLogs' /health with backoffs of up
// to 10 s and a 60 s deadline. A proxy shut down while VictoriaLogs is not ready
// yet must stop that wait instead of leaving it running for up to a minute.
// conformance: shutdown-releases-background-work
func TestProxyLifecycle_ShutdownStopsStartupWarmWaitingForBackend(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()
	before := goroutineCreators()

	notReady := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	callerCache := cache.New(time.Minute, 100)
	p, err := New(leakConfig(leakFlagLabelsWarm, notReady.URL, t.TempDir(), callerCache))
	if err != nil {
		notReady.Close()
		callerCache.Close()
		t.Fatalf("New: %v", err)
	}
	p.Init()
	// Let the warm-up fail its first /health probe and enter its backoff.
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	callerCache.Close()
	notReady.Close()

	if report := settleGoroutines(baseline, before); report != "" {
		t.Fatalf("startup warm-up outlived Shutdown: %s", report)
	}
}

// conformance: shutdown-releases-background-work
func TestProxyLifecycle_RepeatedNewShutdownDoesNotAccumulate(t *testing.T) {
	const proxies = 50
	runtime.GC()
	baseline := runtime.NumGoroutine()
	before := goroutineCreators()

	for i := 0; i < proxies; i++ {
		callerCache := cache.New(time.Minute, 100)
		p, err := New(Config{
			BackendURL:             "http://127.0.0.1:1",
			Cache:                  callerCache,
			LogLevel:               "error",
			DisableLabelsCacheWarm: true,
		})
		if err != nil {
			callerCache.Close()
			t.Fatalf("New #%d: %v", i, err)
		}
		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown #%d: %v", i, err)
		}
		callerCache.Close()
	}

	if report := settleGoroutines(baseline, before); report != "" {
		t.Fatalf("%d New+Shutdown cycles leaked: %s", proxies, report)
	}
}

// conformance: shutdown-releases-background-work
func FuzzProxyLifecycleNoGoroutineLeak(f *testing.F) {
	f.Add(uint8(0))
	f.Add(leakFlagPatternsPersist)
	f.Add(leakFlagLabelIndexPersist)
	f.Add(leakFlagColdBackend)
	f.Add(leakFlagLabelsWarm)
	f.Add(leakFlagRateLimit)
	f.Add(leakFlagAll)
	f.Fuzz(func(t *testing.T, flags uint8) {
		flags &= leakFlagAll
		if report := runProxyLifecycle(t, flags); report != "" {
			t.Fatalf("proxy lifecycle leaked (flags=%#x): %s", flags, report)
		}
	})
}
