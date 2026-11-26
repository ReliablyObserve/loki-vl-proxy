package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Restart hand-over through the proxy's lifecycle: Shutdown writes it after
// every background writer stopped, Init warms from it while /ready answers
// 503, the warm-up never holds /ready past its timeout, Shutdown stops a
// running warm-up, and with the defaults nothing of it runs.

type handoverProxyFixture struct {
	path string
	c    *cache.Cache
	dc   *cache.DiskCache
	p    *Proxy
}

func openHandoverProxy(t *testing.T, path, backendURL string, cfg Config) *handoverProxyFixture {
	t.Helper()
	dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: path, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	c := cache.NewWithMaxBytes(time.Minute, 1000, 1<<20)
	c.SetL2(dc)
	cfg.BackendURL = backendURL
	cfg.Cache = c
	cfg.LogLevel = "error"
	cfg.DisableLabelsCacheWarm = true
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &handoverProxyFixture{path: path, c: c, dc: dc, p: p}
}

func (f *handoverProxyFixture) shutdown(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	f.c.Close()
	if err := f.dc.Close(); err != nil {
		t.Fatalf("disk close: %v", err)
	}
}

func readyStatus(p *Proxy) (int, string) {
	rec := httptest.NewRecorder()
	p.handleReady(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	return rec.Code, rec.Body.String()
}

func healthyBackend(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitWarmDone(t *testing.T, p *Proxy) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for p.responseCacheWarming() {
		if time.Now().After(deadline) {
			t.Fatal("warm-up did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// conformance: cache-restart-handover
func TestCacheHandover_ShutdownFlushesAndInitWarms(t *testing.T) {
	backend := healthyBackend(t)
	path := filepath.Join(t.TempDir(), "cache.db")
	cfg := Config{CacheShutdownFlushTimeout: 5 * time.Second, CacheStartupWarmBytes: 1 << 20, CacheHandoverScope: func() string { return "build-a" }}
	first := openHandoverProxy(t, path, backend.URL, cfg)
	first.p.Init()
	first.c.SetWithTTL("query_range:t:answer", []byte("disk answer"), time.Minute)
	first.c.SetLocalOnlyRestorableWithTTL("compat:t:big:routing:x", []byte("oversized answer"), time.Minute)
	first.c.SetLocalOnlyWithTTL("query_range:t:helper:confirmed", []byte("1"), time.Minute)
	first.shutdown(t)

	second := openHandoverProxy(t, path, backend.URL, cfg)
	if !second.c.HasHandover() {
		t.Fatal("the graceful shutdown left no hand-over")
	}
	second.p.Init()
	waitWarmDone(t, second.p)
	if code, body := readyStatus(second.p); code != http.StatusOK {
		t.Fatalf("/ready after the warm-up: %d %s", code, body)
	}
	for key, want := range map[string]string{"query_range:t:answer": "disk answer", "compat:t:big:routing:x": "oversized answer"} {
		if v, ok := second.c.Peek(key); !ok || string(v) != want {
			t.Fatalf("%s not warmed into memory (got %q %v)", key, v, ok)
		}
	}
	if _, ok := second.c.Peek("query_range:t:helper:confirmed"); ok {
		t.Fatal("a memory-only helper entry was restored")
	}
	stats := second.c.Stats()
	if stats.L2.Hits != 0 {
		t.Fatalf("the warm-up counted %d disk hits: it is not a lookup", stats.L2.Hits)
	}
	if h := stats.Handover; !h.PreviousFlushComplete || h.WarmDiskEntries != 1 || h.WarmMemoryEntries != 1 {
		t.Fatalf("hand-over stats %+v", h)
	}
	second.shutdown(t)
}

// conformance: cache-restart-handover
func TestCacheHandover_OffByDefault(t *testing.T) {
	backend := healthyBackend(t)
	path := filepath.Join(t.TempDir(), "cache.db")
	first := openHandoverProxy(t, path, backend.URL, Config{})
	first.p.Init()
	first.c.SetWithTTL("query_range:t:answer", []byte("a"), time.Minute)
	first.shutdown(t)
	second := openHandoverProxy(t, path, backend.URL, Config{CacheStartupWarmBytes: 1 << 20})
	if second.c.HasHandover() {
		t.Fatal("a shutdown with the defaults wrote a hand-over")
	}
	second.p.Init()
	if second.p.responseCacheWarming() {
		t.Fatal("/ready waits for a warm-up that has nothing to load")
	}
	second.shutdown(t)
}

// conformance: cache-restart-handover
func TestCacheHandover_WarmOffDropsTheHandover(t *testing.T) {
	backend := healthyBackend(t)
	path := filepath.Join(t.TempDir(), "cache.db")
	first := openHandoverProxy(t, path, backend.URL, Config{CacheShutdownFlushTimeout: time.Second})
	first.p.Init()
	first.c.SetWithTTL("query_range:t:answer", []byte("a"), time.Minute)
	first.shutdown(t)
	second := openHandoverProxy(t, path, backend.URL, Config{})
	second.p.Init()
	if second.c.HasHandover() || second.p.responseCacheWarming() {
		t.Fatal("with the warm-up off the hand-over is dropped and /ready does not wait")
	}
	if _, ok := second.c.Peek("query_range:t:answer"); ok {
		t.Fatal("nothing is warmed with the warm-up off")
	}
	// The disk tier still answers it as before.
	if v, _, ok := second.c.GetWithTier("query_range:t:answer"); !ok || string(v) != "a" {
		t.Fatal("the disk record must stay readable")
	}
	second.shutdown(t)
}

// conformance: cache-restart-handover
func TestCacheHandover_ReadyNeverWaitsPastTheTimeout(t *testing.T) {
	backend := healthyBackend(t)
	f := openHandoverProxy(t, filepath.Join(t.TempDir(), "cache.db"), backend.URL, Config{CacheStartupWarmBytes: 1 << 20})
	h := f.p.cacheHandover
	// A warm-up still running inside its timeout holds /ready.
	h.ready.Store(false)
	h.warmDeadline.Store(time.Now().Add(time.Hour).UnixNano())
	if code, body := readyStatus(f.p); code != http.StatusServiceUnavailable || body != "response cache warming" {
		t.Fatalf("/ready during the warm-up: %d %q", code, body)
	}
	// Past its timeout /ready no longer waits, whatever the goroutine does.
	h.warmDeadline.Store(time.Now().Add(-time.Millisecond).UnixNano())
	if code, body := readyStatus(f.p); code != http.StatusOK {
		t.Fatalf("/ready past the warm-up timeout: %d %q", code, body)
	}
	h.ready.Store(true)
	f.shutdown(t)
}

// A proxy shut down while its warm-up runs stops it and hands back every
// goroutine; the flush then runs on a memory tier no warm-up writes to.
// conformance: shutdown-releases-background-work, cache-restart-handover
func TestCacheHandover_ShutdownStopsTheWarmUp(t *testing.T) {
	backend := healthyBackend(t)
	path := filepath.Join(t.TempDir(), "cache.db")
	cfg := Config{CacheShutdownFlushTimeout: 5 * time.Second, CacheStartupWarmBytes: 1 << 20, CacheHandoverScope: func() string { return "s" }}
	first := openHandoverProxy(t, path, backend.URL, cfg)
	first.p.Init()
	for i := 0; i < 200; i++ {
		first.c.SetWithTTL("query_range:t:"+string(rune('a'+i%26))+time.Duration(i).String(), []byte("v"), time.Minute)
	}
	first.shutdown(t)

	runtime.GC()
	baseline := runtime.NumGoroutine()
	before := goroutineCreators()
	second := openHandoverProxy(t, path, backend.URL, cfg)
	second.p.Init()
	second.shutdown(t)
	if second.p.responseCacheWarming() {
		t.Fatal("a stopped warm-up must not hold /ready")
	}
	if report := settleGoroutines(baseline, before); report != "" {
		t.Fatalf("the warm-up outlived Shutdown: %s", report)
	}
}
