package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestMetricsRoute_ExportsPeerCacheMetrics(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "127.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "127.0.0.1:3100,127.0.0.1:3101",
	})
	defer pc.Close()
	pc.PeerHits.Add(3)
	pc.PeerMisses.Add(2)
	pc.PeerErrors.Add(1)
	pc.RecordPeerErrorReason("timeout")

	p, err := New(Config{
		BackendURL: "http://unused",
		Cache:      c,
		LogLevel:   "error",
		PeerCache:  pc,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	for _, metric := range []string{
		"loki_vl_proxy_peer_cache_peers 1",
		"loki_vl_proxy_peer_cache_cluster_members 2",
		"loki_vl_proxy_peer_cache_hits_total 3",
		"loki_vl_proxy_peer_cache_misses_total 2",
		"loki_vl_proxy_peer_cache_errors_total 1",
		"loki_vl_proxy_peer_cache_error_reason_total{reason=\"timeout\"} 1",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("expected %q in metrics output\nbody:\n%s", metric, body)
		}
	}
}
