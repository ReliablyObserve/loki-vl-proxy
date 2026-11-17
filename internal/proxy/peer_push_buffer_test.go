package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The push buffer is visible on /metrics: what was buffered (by reason),
// coalesced, replayed and dropped, and what waits now.
//
// conformance: peer-cache-push-replay
func TestMetricsRoute_ExportsPeerPushBufferMetrics(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	defer c.Close()
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr: "127.0.0.1:3100", DiscoveryType: "static", StaticPeers: "127.0.0.1:3100,127.0.0.1:1",
		Timeout: 50 * time.Millisecond, WriteThrough: true, ReplicationFactor: 2,
	})
	defer pc.Close()
	pc.PushBuffered[0].Add(4)
	pc.PushReplayed.Add(3)
	pc.PushCoalesced.Add(2)
	pc.PushBufferDropped[0].Add(1)
	p, err := New(Config{BackendURL: "http://unused", Cache: c, LogLevel: "error", PeerCache: pc})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, metric := range []string{
		`loki_vl_proxy_peer_cache_push_buffered_total{reason="peer_unavailable"} 4`,
		`loki_vl_proxy_peer_cache_push_buffered_total{reason="queue_full"} 0`,
		"loki_vl_proxy_peer_cache_push_buffer_coalesced_total 2",
		"loki_vl_proxy_peer_cache_push_replayed_total 3",
		`loki_vl_proxy_peer_cache_push_buffer_dropped_total{reason="full"} 1`,
		`loki_vl_proxy_peer_cache_push_buffer_dropped_total{reason="closed"} 0`,
		"# TYPE loki_vl_proxy_peer_cache_push_buffer_entries gauge",
		"loki_vl_proxy_peer_cache_push_buffer_bytes 0",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("expected %q in metrics output\nbody:\n%s", metric, body)
		}
	}
}

// Shutting the proxy down stops the peer pushes in flight to a peer that never
// answers and the push buffer behind them (internal/cache covers a replay in
// flight), and hands every goroutine back.
//
// conformance: peer-cache-push-replay, shutdown-releases-background-work
func TestProxyLifecycle_ShutdownStopsPeerPushReplay(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()
	before := goroutineCreators()

	var once sync.Once
	hung := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		once.Do(func() { close(hung) })
		<-r.Context().Done()
	}))
	stub := leakStubBackend()
	callerCache := cache.New(time.Minute, 1000)
	self := "127.0.0.1:1"
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr: self, DiscoveryType: "static", StaticPeers: self + "," + peer.Listener.Addr().String(),
		Timeout: 30 * time.Second, WriteThrough: true, WriteThroughMinTTL: time.Second, ReplicationFactor: 2,
	})
	cfg := leakConfig(0, stub.URL, t.TempDir(), callerCache)
	cfg.PeerCache = pc
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Init()
	for i := 0; i < 50; i++ {
		pc.Replicate(fmt.Sprintf("key-%d", i), []byte("value"), time.Minute)
	}
	select {
	case <-hung:
	case <-time.After(5 * time.Second):
		t.Fatal("no push reached the peer")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	callerCache.Close()
	stub.Close()
	peer.Close()
	if took := time.Since(started); took > 3*time.Second {
		t.Errorf("shutdown took %s with a push waiting on a hung peer (peer timeout 30s)", took)
	}
	// The peer cache closes its own connections on Close; an idle connection
	// a test client of the shared default transport left behind is not this
	// test's (race-stress runs every test in one process).
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	if report := settleGoroutines(baseline, before); report != "" {
		t.Fatalf("peer push replay outlived Shutdown: %s", report)
	}
}
