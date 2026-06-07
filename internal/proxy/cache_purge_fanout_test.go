package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// newPeerProxy builds a Proxy whose peer cache is configured (so /_cache/* routes
// register) with the given shared token and optional static peer list. The cache
// is pre-seeded with one key so a purge is observable.
func newPeerProxy(t *testing.T, token, staticPeers string) (*Proxy, *cache.Cache, *http.ServeMux) {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	c.Set("seed-key", []byte("seed-value"))
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "127.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   staticPeers,
	})
	t.Cleanup(pc.Close)
	p, err := New(Config{
		BackendURL:    "http://unused",
		Cache:         c,
		LogLevel:      "error",
		PeerCache:     pc,
		PeerAuthToken: token,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, c, mux
}

// TestPeerCachePurge_EndpointRequiresToken locks the peer-side /_cache/purge auth.
func TestPeerCachePurge_EndpointRequiresToken(t *testing.T) {
	_, c, mux := newPeerProxy(t, "peer-secret", "")

	// No token → 401, cache untouched.
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/_cache/purge", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no-token purge: want 401, got %d", w.Code)
	}
	if _, ok := c.Get("seed-key"); !ok {
		t.Fatal("cache was purged despite 401")
	}

	// Wrong method → 405.
	w = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/_cache/purge", nil)
	req.Header.Set("X-Peer-Token", "peer-secret")
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET purge: want 405, got %d", w.Code)
	}

	// Valid token + POST → 200, cache purged.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/_cache/purge", nil)
	req.Header.Set("X-Peer-Token", "peer-secret")
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("authed purge: want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if _, ok := c.Get("seed-key"); ok {
		t.Fatal("cache not purged after authed /_cache/purge")
	}
}

// TestCacheFlush_FanoutPurgesPeers verifies POST /admin/cache/flush?peers=1 purges
// the local instance AND every peer (authenticated with the shared token), and
// that without ?peers only the local instance is purged.
func TestCacheFlush_FanoutPurgesPeers(t *testing.T) {
	const token = "shared-secret"

	// Peer B: real server so the local node can POST to its /_cache/purge.
	_, peerCacheStore, peerMux := newPeerProxy(t, token, "")
	peerSrv := httptest.NewServer(peerMux)
	t.Cleanup(peerSrv.Close)
	peerURL, _ := url.Parse(peerSrv.URL)

	// Local node A: peer list points at B.
	local, localCache, _ := newPeerProxy(t, token, peerURL.Host)

	// --- 1) Without ?peers: only local purged, peer untouched. ---
	localCache.Set("seed-key", []byte("v"))
	peerCacheStore.Set("seed-key", []byte("v"))
	w := httptest.NewRecorder()
	local.handleCacheFlush(w, httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("local-only flush: want 200, got %d", w.Code)
	}
	if _, ok := localCache.Get("seed-key"); ok {
		t.Fatal("local cache not purged")
	}
	if _, ok := peerCacheStore.Get("seed-key"); !ok {
		t.Fatal("peer cache was purged without ?peers")
	}

	// --- 2) With ?peers=1: local AND peer purged. ---
	localCache.Set("seed-key", []byte("v"))
	peerCacheStore.Set("seed-key", []byte("v"))
	w = httptest.NewRecorder()
	local.handleCacheFlush(w, httptest.NewRequest(http.MethodPost, "/admin/cache/flush?peers=1", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("fanout flush: want 200, got %d", w.Code)
	}
	if _, ok := localCache.Get("seed-key"); ok {
		t.Fatal("local cache not purged on fanout")
	}
	if _, ok := peerCacheStore.Get("seed-key"); ok {
		t.Fatal("peer cache not purged on fanout")
	}

	// Response reports the peer purge result.
	var resp struct {
		Status string            `json:"status"`
		Local  string            `json:"local"`
		Peers  map[string]string `json:"peers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v body=%s", err, w.Body.String())
	}
	if resp.Status != "ok" || resp.Local == "" {
		t.Errorf("unexpected local status: %+v", resp)
	}
	if got := resp.Peers[peerURL.Host]; got != "purged" {
		t.Errorf("peer %s status=%q, want purged (peers=%v)", peerURL.Host, got, resp.Peers)
	}
}

// TestCacheFlush_FanoutReportsUnreachablePeer verifies a down peer is reported,
// not fatal — the local purge still succeeds.
func TestCacheFlush_FanoutReportsUnreachablePeer(t *testing.T) {
	local, localCache, _ := newPeerProxy(t, "shared-secret", "127.0.0.1:1") // :1 = unreachable

	localCache.Set("seed-key", []byte("v"))
	w := httptest.NewRecorder()
	local.handleCacheFlush(w, httptest.NewRequest(http.MethodPost, "/admin/cache/flush?peers=true", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("want 200 despite down peer, got %d", w.Code)
	}
	if _, ok := localCache.Get("seed-key"); ok {
		t.Fatal("local cache not purged when peer unreachable")
	}
	body := w.Body.String()
	if !strings.Contains(body, "127.0.0.1:1") || !strings.Contains(body, "error") {
		t.Errorf("expected down peer reported with error, got %s", body)
	}
}

// TestCacheFlush_PeerFanoutDisabledWhenNoPeerCache: a node without peer cache just
// purges local and notes peers are disabled.
func TestCacheFlush_PeerFanoutNoPeerCache(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	c.Set("seed-key", []byte("v"))
	p, err := New(Config{BackendURL: "http://unused", Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	w := httptest.NewRecorder()
	p.handleCacheFlush(w, httptest.NewRequest(http.MethodPost, "/admin/cache/flush?peers=1", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	if _, ok := c.Get("seed-key"); ok {
		t.Fatal("local cache not purged")
	}
	b, _ := io.ReadAll(w.Result().Body)
	if !strings.Contains(string(b), "disabled") {
		t.Errorf("expected peers note about disabled, got %s", string(b))
	}
}
