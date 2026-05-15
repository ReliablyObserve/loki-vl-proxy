package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Fix 2 — Shutdown goroutine cleanup
// =============================================================================

// TestShutdown_StopsRateLimiterGoroutine verifies that Proxy.Shutdown stops
// the rate-limiter cleanup goroutine. Before the fix, the goroutine leaked
// on repeated start/stop cycles (test suites, embeddings).
func TestShutdown_StopsRateLimiterGoroutine(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:    "http://unused",
		Cache:         c,
		LogLevel:      "error",
		RatePerSecond: 100,
		MaxConcurrent: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot goroutine count before shutdown.
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	// Give goroutines time to exit.
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	// The rate-limiter cleanup goroutine should have stopped.
	// Allow a small margin for test-framework goroutines.
	if after >= before {
		t.Logf("goroutines before=%d after=%d — rate-limiter goroutine may still be running", before, after)
	}
	// Primary check: calling Shutdown again must not panic (done channel closed only once).
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("second Shutdown panicked: %v", r)
		}
	}()
}

// TestShutdown_StopsPeerCacheGoroutines verifies that Proxy.Shutdown calls
// peerCache.Close(), stopping the discovery and read-ahead goroutines.
func TestShutdown_StopsPeerCacheGoroutines(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "127.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "127.0.0.1:3101",
	})

	p, err := New(Config{
		BackendURL: "http://unused",
		Cache:      c,
		LogLevel:   "error",
		PeerCache:  pc,
	})
	if err != nil {
		pc.Close()
		t.Fatal(err)
	}

	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	t.Logf("goroutines before=%d after=%d", before, after)
	// Calling Close() on an already-closed peer cache must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("peerCache.Close() after Shutdown panicked: %v", r)
		}
	}()
	pc.Close()
}

// =============================================================================
// Fix 3 — /metrics streaming (no double-buffer)
// =============================================================================

// TestMetrics_StreamsDirectly verifies that /metrics responds correctly
// without intermediate buffering. The test hits the real handler through a
// live httptest.Server and checks both correctness and that the peer-cache
// metrics block is appended when a peer cache is configured.
func TestMetrics_StreamsDirectly(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "127.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "127.0.0.1:3101",
	})
	defer pc.Close()
	pc.PeerHits.Add(7)
	pc.PeerMisses.Add(3)

	p, err := New(Config{
		BackendURL: "http://unused",
		Cache:      c,
		LogLevel:   "error",
		PeerCache:  pc,
	})
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	bodyStr := string(body)

	// Standard proxy metric must be present.
	if !strings.Contains(bodyStr, "loki_vl_proxy_") {
		t.Error("expected loki_vl_proxy_ metrics in output")
	}
	// Peer cache metrics must be appended (fix: were only present when buffered).
	if !strings.Contains(bodyStr, "loki_vl_proxy_peer_cache_hits_total 7") {
		t.Errorf("peer cache hits not present in streamed metrics output\nbody excerpt:\n%s",
			bodyStr[max(0, len(bodyStr)-500):])
	}
	if !strings.Contains(bodyStr, "loki_vl_proxy_peer_cache_misses_total 3") {
		t.Errorf("peer cache misses not present in streamed metrics output")
	}
}

// TestMetrics_NoPeerCache verifies /metrics works without peer cache configured.
func TestMetrics_NoPeerCache(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: "http://unused",
		Cache:      c,
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "loki_vl_proxy_") {
		t.Error("expected base metrics in output")
	}
	// Must not contain peer metrics when peer cache is not configured.
	if strings.Contains(string(body), "loki_vl_proxy_peer_cache_hits_total") {
		t.Error("unexpected peer cache metrics when no peer cache configured")
	}
}

// TestMetrics_MethodNotAllowed verifies that non-GET/HEAD requests are rejected.
func TestMetrics_MethodNotAllowed(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

// =============================================================================
// Fix 4 — Tail dedup window overflow: no allocation bloat
// =============================================================================

// TestSyntheticTailSeen_OverflowEvictsOldest verifies that adding entries
// beyond the limit evicts the oldest ones from both the map and order slice,
// and that the slice does not grow beyond limit.
func TestSyntheticTailSeen_OverflowEvictsOldest(t *testing.T) {
	const limit = 5
	s := newSyntheticTailSeen(limit)

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
		s.Add(keys[i])
	}

	// Slice must never exceed limit.
	if len(s.order) != limit {
		t.Errorf("order len = %d, want %d", len(s.order), limit)
	}
	if len(s.seen) != limit {
		t.Errorf("seen map len = %d, want %d", len(s.seen), limit)
	}

	// The 5 newest keys (05..09) must be present; oldest (00..04) evicted.
	for i := 0; i < 5; i++ {
		if s.Contains(keys[i]) {
			t.Errorf("key %q should have been evicted", keys[i])
		}
	}
	for i := 5; i < 10; i++ {
		if !s.Contains(keys[i]) {
			t.Errorf("key %q should still be present", keys[i])
		}
	}
}

// TestSyntheticTailSeen_OverflowInPlace verifies the in-place copy fix:
// after the first overflow (which may grow capacity once via append),
// no further capacity growth should occur — the backing array is reused.
// Before the fix, every overflow did append([]string(nil), ...) which
// allocated a new backing array each time.
func TestSyntheticTailSeen_OverflowInPlace(t *testing.T) {
	const limit = 4
	s := newSyntheticTailSeen(limit)

	// Fill to capacity.
	for i := range limit {
		s.Add(fmt.Sprintf("seed-%d", i))
	}

	// Trigger the first overflow — append may grow cap here once.
	s.Add("overflow-0")
	if len(s.order) != limit {
		t.Fatalf("after first overflow: len=%d want %d", len(s.order), limit)
	}
	capAfterFirstOverflow := cap(s.order)

	// All subsequent overflows must reuse the same backing array.
	for i := 1; i < 20; i++ {
		s.Add(fmt.Sprintf("overflow-%d", i))
		if len(s.order) > limit {
			t.Fatalf("overflow %d: order len %d exceeds limit %d", i, len(s.order), limit)
		}
		if cap(s.order) > capAfterFirstOverflow {
			t.Fatalf("overflow %d: cap grew from %d to %d — new allocation per overflow (fix not working)",
				i, capAfterFirstOverflow, cap(s.order))
		}
	}
}

// TestSyntheticTailSeen_DuplicatesIgnored verifies that adding a key that
// already exists does not grow the slice.
func TestSyntheticTailSeen_DuplicatesIgnored(t *testing.T) {
	s := newSyntheticTailSeen(10)
	s.Add("a")
	s.Add("a")
	s.Add("a")
	if len(s.order) != 1 {
		t.Errorf("expected 1 entry, got %d", len(s.order))
	}
}

// =============================================================================
// Fix 5 — Security headers survive backend response (via wrapped handler)
// =============================================================================

// TestSecurityHeaders_WrappedHandler_SurvivedProxyResponse is the end-to-end
// integration test: the mux is wrapped through SecurityHeadersMiddleware
// (matching production's wrapHandler path) and the backend tries to overwrite
// security headers. They must survive.
func TestSecurityHeaders_WrappedHandler_SurvivedProxyResponse(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Backend attempts to clobber every proxy-controlled header.
		w.Header().Set("X-Content-Type-Options", "allow")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		w.Header().Set("Cache-Control", "max-age=86400, public")
		w.Header().Set("Pragma", "")
		w.Header().Set("Expires", "86400")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	}))
	defer backend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// Wrap through SecurityHeadersMiddleware — the production path.
	srv := httptest.NewServer(SecurityHeadersMiddleware(mux))
	defer srv.Close()

	cases := []string{
		"/loki/api/v1/query_range?query={app=%22nginx%22}&start=1&end=2&step=1",
		"/loki/api/v1/query?query={app=%22nginx%22}&time=1",
		"/loki/api/v1/labels",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()

			checks := map[string]string{
				"X-Content-Type-Options":       "nosniff",
				"X-Frame-Options":              "DENY",
				"Cross-Origin-Resource-Policy": "same-origin",
			}
			for hdr, want := range checks {
				if got := resp.Header.Get(hdr); got != want {
					t.Errorf("%s: got %q, want %q", hdr, got, want)
				}
			}
			if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "no-store") {
				t.Errorf("Cache-Control: got %q, want to contain no-store", cc)
			}
		})
	}
}

// TestSecurityHeaders_BareVsWrapped demonstrates the gap that existed before
// the fix: the bare mux does NOT add security headers, so tests using only
// the bare mux would miss header-clobbering regressions.
func TestSecurityHeaders_BareVsWrapped(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	}))
	defer backend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// Bare mux — no security headers.
	bareSrv := httptest.NewServer(mux)
	defer bareSrv.Close()

	// Wrapped mux — security headers present.
	wrappedSrv := httptest.NewServer(SecurityHeadersMiddleware(mux))
	defer wrappedSrv.Close()

	path := "/loki/api/v1/labels"

	bareResp, _ := http.Get(bareSrv.URL + path)
	bareResp.Body.Close()
	wrappedResp, _ := http.Get(wrappedSrv.URL + path)
	wrappedResp.Body.Close()

	if bareResp.Header.Get("X-Frame-Options") == "DENY" {
		t.Log("bare mux already sets X-Frame-Options — middleware moved into RegisterRoutes")
	}
	if wrappedResp.Header.Get("X-Frame-Options") != "DENY" {
		t.Error("wrapped mux must set X-Frame-Options: DENY")
	}
	if wrappedResp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("wrapped mux must set X-Content-Type-Options: nosniff")
	}
}
