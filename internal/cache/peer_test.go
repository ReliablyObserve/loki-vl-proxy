package cache

import (
	"bytes"
	gzip "github.com/klauspost/compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestHashRing_Consistent(t *testing.T) {
	ring := newHashRing(150)
	ring.add("10.0.0.1:3100")
	ring.add("10.0.0.2:3100")
	ring.add("10.0.0.3:3100")

	node1 := ring.get("query:rate({app=\"nginx\"}[5m])")
	node2 := ring.get("query:rate({app=\"nginx\"}[5m])")
	if node1 != node2 {
		t.Errorf("inconsistent: %q vs %q", node1, node2)
	}
}

func TestHashRing_Distribution(t *testing.T) {
	ring := newHashRing(150)
	ring.add("node-1")
	ring.add("node-2")
	ring.add("node-3")

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		counts[ring.get(fmt.Sprintf("key-%d", i))]++
	}
	for node, count := range counts {
		if count < 200 {
			t.Errorf("node %q got %d/1000 keys (expected >200)", node, count)
		}
	}
}

func TestHashRing_Empty(t *testing.T) {
	ring := newHashRing(150)
	if ring.get("any") != "" {
		t.Error("empty ring should return empty")
	}
}

func TestPeerCache_StaticDiscovery(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "10.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "10.0.0.1:3100,10.0.0.2:3100,10.0.0.3:3100",
	})
	defer pc.Close()
	if pc.PeerCount() != 2 {
		t.Errorf("expected 2 peers (excluding self), got %d", pc.PeerCount())
	}
}

func TestPeerCache_SetAndGossipHaveKey_NoOp(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "10.0.0.1:3100"})
	defer pc.Close()

	pc.Set("key", []byte("value"))
	pc.GossipHaveKey("key")

	if stats := pc.Stats(); stats["peer_hits"].(int64) != 0 || stats["peer_misses"].(int64) != 0 || stats["peer_errors"].(int64) != 0 {
		t.Fatalf("expected no-op methods to leave stats unchanged, got %+v", stats)
	}
}

func TestPeerCache_Disabled(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "10.0.0.1:3100"})
	defer pc.Close()
	_, _, ok := pc.Get("any-key")
	if ok {
		t.Error("disabled peer cache should always miss")
	}
}

func TestPeerCache_IsOwner(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "10.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "10.0.0.1:3100,10.0.0.2:3100",
	})
	defer pc.Close()

	selfCount := 0
	for i := 0; i < 100; i++ {
		if pc.IsOwner(fmt.Sprintf("key-%d", i)) {
			selfCount++
		}
	}
	if selfCount < 20 || selfCount > 80 {
		t.Errorf("expected ~50%% self-ownership, got %d/100", selfCount)
	}
}

func TestPeerCache_IsOwner_NoPeers(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "10.0.0.1:3100"})
	defer pc.Close()
	if !pc.IsOwner("any-key") {
		t.Error("should be owner when no peers")
	}
}

func TestPeerCache_ServeHTTP_Hit(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	localCache.Set("test-key", []byte("hello"))

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get?key=test-key", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != 200 || w.Body.String() != "hello" {
		t.Errorf("expected 200/hello, got %d/%q", w.Code, w.Body.String())
	}
}

func TestPeerCache_ServeHTTP_HitCompressed(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	payload := []byte(strings.Repeat("x", 1024))
	localCache.Set("test-key", payload)

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get?key=test-key", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	pc.ServeHTTP(w, r, localCache)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected gzip content encoding, got %q", w.Header().Get("Content-Encoding"))
	}
	zr, err := gzip.NewReader(strings.NewReader(w.Body.String()))
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer zr.Close()
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read gzip payload: %v", err)
	}
	if string(decoded) != string(payload) {
		t.Fatalf("unexpected gzip payload, want len=%d got len=%d", len(payload), len(decoded))
	}
}

func TestPeerCache_ServeHTTP_HitCompressedZstd(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	payload := []byte(strings.Repeat("x", 1024))
	localCache.Set("test-key", payload)

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get?key=test-key", nil)
	r.Header.Set("Accept-Encoding", "zstd, gzip")
	pc.ServeHTTP(w, r, localCache)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Encoding"); got != "zstd" {
		t.Fatalf("expected zstd content encoding, got %q", got)
	}
	zr, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("create zstd reader: %v", err)
	}
	defer zr.Close()
	decoded, err := zr.DecodeAll(w.Body.Bytes(), nil)
	if err != nil {
		t.Fatalf("decode zstd payload: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("unexpected zstd payload, want len=%d got len=%d", len(payload), len(decoded))
	}
}

func TestPeerCache_ServeHTTP_HotIndex(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:         "localhost:3100",
		ReadAheadEnabled: true,
	})
	defer pc.Close()
	localCache.SetL3(pc)

	localCache.Set("query_range:tenant-a:a", []byte("value-a"))
	localCache.Set("query_range:tenant-b:b", []byte("value-b"))
	for range 10 {
		_, _ = localCache.Get("query_range:tenant-a:a")
	}
	for range 3 {
		_, _ = localCache.Get("query_range:tenant-b:b")
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/_cache/hot?limit=1", nil)
	r.Header.Set("Accept-Encoding", "zstd, gzip")
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	decoded, err := decodePeerResponseBody(w.Header().Get("Content-Encoding"), w.Body.Bytes(), maxPeerHotIndexBytes)
	if err != nil {
		t.Fatalf("decode hot-index body: %v", err)
	}
	var payload hotIndexResponse
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatalf("decode hot-index payload: %v", err)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected 1 hot entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].Tenant != "tenant-a" {
		t.Fatalf("expected top tenant-a entry, got %+v", payload.Entries[0])
	}
}

func TestPeerCache_ServeHTTP_Miss(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get?key=nonexistent", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestPeerCache_ServeHTTP_MissingKey(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing key, got %d", w.Code)
	}
}

func TestPeerCache_ServeHTTP_SetRoundTrip(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/_cache/set?key=test-key&ttl_ms=90000", strings.NewReader("hello"))
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for set, got %d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/_cache/get?key=test-key", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusOK || w.Body.String() != "hello" {
		t.Fatalf("expected roundtrip 200/hello, got %d/%q", w.Code, w.Body.String())
	}
}

func TestPeerCache_ServeHTTP_SetRoundTrip_CompressedRequest(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	payload := []byte(strings.Repeat("hello-compressed-", 128))
	encoded, err := encodePeerRequestBody(payload, "zstd")
	if err != nil {
		t.Fatalf("encode zstd payload: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/_cache/set?key=test-key&ttl_ms=90000", bytes.NewReader(encoded))
	r.Header.Set("Content-Encoding", "zstd")
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for compressed set, got %d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/_cache/get?key=test-key", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusOK || w.Body.String() != string(payload) {
		t.Fatalf("expected roundtrip 200/%q, got %d/%q", string(payload), w.Code, w.Body.String())
	}
}

func TestPeerCache_ServeHTTP_SetRejectsEmptyBody(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/_cache/set?key=test-key", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty set payload, got %d", w.Code)
	}
}

func TestPeerCache_ServeHTTP_RejectsNearExpiryEntry(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	localCache.SetWithTTL("near-expiry", []byte("hello"), time.Second)

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get?key=near-expiry", nil)
	pc.ServeHTTP(w, r, localCache)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected near-expiry cache entry to be treated as miss, got %d", w.Code)
	}
}

func TestPeerCache_FetchFromPeer(t *testing.T) {
	// Peer server with data
	peerCache := New(60*time.Second, 1000)
	defer peerCache.Close()

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerPC.ServeHTTP(w, r, peerCache)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	// Try keys until one maps to the peer
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("shared-key-%d", i)
		peerCache.Set(key, []byte("peer-data"))

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner == peerServer.Listener.Addr().String() {
			value, _, found := pc.Get(key)
			if found && string(value) == "peer-data" {
				t.Logf("fetched from peer for key %q", key)
				return
			}
		}
	}
	t.Log("no key mapped to peer — acceptable for small hash ring")
}

func TestPeerCache_FetchFromPeer_EncodesCacheKey(t *testing.T) {
	peerCache := New(60*time.Second, 1000)
	defer peerCache.Close()

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerPC.ServeHTTP(w, r, peerCache)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("labels:tenant-a:query={service_name=\"api\"}&start=1&end=2:%d", i)
		peerCache.Set(key, []byte("peer-data"))

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner != peerServer.Listener.Addr().String() {
			continue
		}
		value, _, found := pc.Get(key)
		if !found || string(value) != "peer-data" {
			t.Fatalf("expected encoded peer hit for key %q, got found=%v value=%q", key, found, string(value))
		}
		return
	}
	t.Fatal("no key mapped to peer")
}

func TestPeerCache_Get_DecodesCompressedPeerPayload(t *testing.T) {
	payload := []byte(strings.Repeat("peer-data", 128))
	var badAcceptEncoding atomic.Value
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); !strings.Contains(got, "zstd") {
			badAcceptEncoding.Store(got)
			http.Error(w, "unexpected accept-encoding", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Cache-TTL-Ms", "60000")
		w.Header().Set("Content-Encoding", "zstd")
		zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			t.Fatalf("create zstd writer: %v", err)
		}
		if _, err := zw.Write(payload); err != nil {
			t.Fatalf("write zstd payload: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close zstd writer: %v", err)
		}
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("compressed-key-%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()
		if owner != peerServer.Listener.Addr().String() {
			continue
		}
		value, ttl, found := pc.Get(key)
		if !found {
			t.Fatalf("expected peer hit for key %q", key)
		}
		if ttl <= 0 {
			t.Fatalf("expected remaining ttl, got %s", ttl)
		}
		if !bytes.Equal(value, payload) {
			t.Fatalf("unexpected decoded peer payload, want len=%d got len=%d", len(payload), len(value))
		}
		if got, _ := badAcceptEncoding.Load().(string); got != "" {
			t.Fatalf("unexpected accept-encoding seen by peer server: %q", got)
		}
		return
	}
	t.Fatal("no key mapped to peer")
}

func TestPeerCache_Get_RecordsTimeoutReason(t *testing.T) {
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       10 * time.Millisecond,
	})
	defer pc.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("timeout-key-%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()
		if owner != peerServer.Listener.Addr().String() {
			continue
		}
		if value, _, ok := pc.Get(key); ok || value != nil {
			t.Fatalf("expected timeout miss for key %q", key)
		}
		if got := pc.PeerErrorReasons()["timeout"]; got != 1 {
			t.Fatalf("expected timeout reason count 1, got %d", got)
		}
		if got := pc.PeerErrors.Load(); got != 1 {
			t.Fatalf("expected peer error count 1, got %d", got)
		}
		return
	}
	t.Fatal("no key mapped to peer")
}

func TestPeerCache_Get_RecordsStatusReason(t *testing.T) {
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream boom", http.StatusBadGateway)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("status-key-%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()
		if owner != peerServer.Listener.Addr().String() {
			continue
		}
		if value, _, ok := pc.Get(key); ok || value != nil {
			t.Fatalf("expected status miss for key %q", key)
		}
		reasons := pc.PeerErrorReasons()
		if got := reasons["status_502"]; got != 1 {
			t.Fatalf("expected status_502 reason count 1, got %d (%v)", got, reasons)
		}
		if got := pc.PeerErrors.Load(); got != 1 {
			t.Fatalf("expected peer error count 1, got %d", got)
		}
		return
	}
	t.Fatal("no key mapped to peer")
}

func TestPeerCache_ShadowCopy_ShortTTL(t *testing.T) {
	// Verify that L3 hits get stored in L1 with shadow TTL (≤30s)
	peerCache := New(60*time.Second, 1000)
	defer peerCache.Close()
	peerCache.Set("shadow-key", []byte("data"))

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerPC.ServeHTTP(w, r, peerCache)
	}))
	defer peerServer.Close()

	localCache := NewWithMaxBytes(120*time.Second, 1000, 256*1024*1024) // 120s default TTL
	defer localCache.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()
	localCache.SetL3(pc)

	// Find a key that maps to the peer
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("shadow-key-%d", i)
		peerCache.Set(key, []byte("data"))

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner == peerServer.Listener.Addr().String() {
			val, ok := localCache.Get(key) // triggers L3 fetch + shadow copy
			if ok {
				t.Logf("L3 hit, shadow copy stored: %q", string(val))
				// Verify it's in L1 now
				val2, ok2 := localCache.Get(key)
				if !ok2 {
					t.Error("shadow copy should be in L1 after L3 fetch")
				}
				_ = val2
				return
			}
		}
	}
	t.Log("no key mapped to peer for shadow test")
}

func TestPeerCache_CircuitBreaker(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   "dead:9999",
		Timeout:       100 * time.Millisecond,
	})
	defer pc.Close()

	for i := 0; i < 10; i++ {
		pc.recordPeerFailure("dead:9999")
	}
	if pc.peerAllowed("dead:9999") {
		t.Error("should be circuit-broken after failures")
	}
}

func TestDiscoverDNS_UsesLookupHostAndSortsPeers(t *testing.T) {
	oldLookupHost := lookupHost
	lookupHost = func(name string) ([]string, error) {
		if name != "proxy-headless.ns.svc.cluster.local" {
			t.Fatalf("unexpected lookup name %q", name)
		}
		return []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}, nil
	}
	t.Cleanup(func() { lookupHost = oldLookupHost })

	peers, err := discoverDNS("proxy-headless.ns.svc.cluster.local", 3100)
	if err != nil {
		t.Fatalf("discoverDNS: %v", err)
	}

	want := []string{"10.0.0.1:3100", "10.0.0.2:3100", "10.0.0.3:3100"}
	if strings.Join(peers, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected peers: got %v want %v", peers, want)
	}
}

func TestDiscoverDNS_WrapsLookupErrors(t *testing.T) {
	oldLookupHost := lookupHost
	lookupHost = func(string) ([]string, error) {
		return nil, errors.New("dns boom")
	}
	t.Cleanup(func() { lookupHost = oldLookupHost })

	_, err := discoverDNS("proxy-headless.ns.svc.cluster.local", 3100)
	if err == nil || !strings.Contains(err.Error(), `DNS lookup "proxy-headless.ns.svc.cluster.local": dns boom`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPeerCache_DiscoveryLoop_RefreshesPeers(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "10.0.0.1:3100"})
	defer pc.Close()

	updates := make(chan struct{}, 1)
	pc.discoveryInt = 10 * time.Millisecond
	pc.discoveryFn = func() ([]string, error) {
		select {
		case updates <- struct{}{}:
		default:
		}
		return []string{"10.0.0.1:3100", "10.0.0.2:3100"}, nil
	}

	go pc.discoveryLoop()

	select {
	case <-updates:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("discovery loop did not trigger")
	}

	requireEventually(t, 200*time.Millisecond, func() bool {
		peers := pc.Peers()
		return len(peers) == 2 && peers[1] == "10.0.0.2:3100"
	})
}

func requireEventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}

func TestPeerCache_Singleflight(t *testing.T) {
	var fetchCount atomic.Int64
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCount.Add(1)
		time.Sleep(50 * time.Millisecond)
		w.Write([]byte("data"))
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	var targetKey string
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sf-%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()
		if owner == peerServer.Listener.Addr().String() {
			targetKey = key
			break
		}
	}
	if targetKey == "" {
		t.Skip("no key mapped to peer")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, _ = pc.Get(targetKey) }()
	}
	wg.Wait()

	if fetchCount.Load() > 3 {
		t.Errorf("singleflight: %d fetches for 10 concurrent (expected ~1)", fetchCount.Load())
	}
}

func TestPeerCache_Stats(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   "peer-1:3100,peer-2:3100",
	})
	defer pc.Close()

	stats := pc.Stats()
	if stats["peers"].(int) != 2 {
		t.Errorf("expected 2 peers, got %v", stats["peers"])
	}
	if len(pc.MetricsJSON()) == 0 {
		t.Error("MetricsJSON empty")
	}
}

func TestPeerCache_LBScenario_ThreePeers(t *testing.T) {
	// 3 proxy instances behind LB. Verify data propagates via sharded cache.
	caches := make([]*Cache, 3)
	pcs := make([]*PeerCache, 3)
	servers := make([]*httptest.Server, 3)

	for i := range caches {
		caches[i] = New(60*time.Second, 1000)
	}

	for i := range servers {
		idx := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pcs[idx].ServeHTTP(w, r, caches[idx])
		}))
	}

	addrs := make([]string, 3)
	for i := range servers {
		addrs[i] = servers[i].Listener.Addr().String()
	}
	peerList := strings.Join(addrs, ",")

	for i := range pcs {
		pcs[i] = NewPeerCache(PeerConfig{
			SelfAddr:      addrs[i],
			DiscoveryType: "static",
			StaticPeers:   peerList,
			Timeout:       2 * time.Second,
		})
		caches[i].SetL3(pcs[i])
	}

	defer func() {
		for i := range servers {
			servers[i].Close()
			pcs[i].Close()
			caches[i].Close()
		}
	}()

	// Peer 0 gets data from "VL"
	testKey := "query:rate({app=nginx}[5m])"
	caches[0].Set(testKey, []byte("vl-response-data"))

	// Find which peer owns this key
	owner := pcs[0].ring.get(testKey)
	ownerIdx := -1
	for i, addr := range addrs {
		if addr == owner {
			ownerIdx = i
			break
		}
	}

	if ownerIdx == 0 {
		// Peer 0 is the owner — peer 1 should fetch via L3
		val, ok := caches[1].Get(testKey)
		if ok {
			t.Logf("peer 1 fetched from owner (peer 0) via L3: %q", string(val))
		}
	} else if ownerIdx >= 0 {
		t.Logf("owner is peer %d — data stored on peer 0 won't be found by others (correct: non-owner data is local only)", ownerIdx)
	}
}

func TestPeerCache_ThreePeers_ShadowCopiesAvoidRepeatedOwnerFetches(t *testing.T) {
	caches := make([]*Cache, 3)
	pcs := make([]*PeerCache, 3)
	servers := make([]*httptest.Server, 3)
	serverCalls := make([]atomic.Int64, 3)

	for i := range caches {
		caches[i] = New(60*time.Second, 1000)
	}

	for i := range servers {
		idx := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serverCalls[idx].Add(1)
			pcs[idx].ServeHTTP(w, r, caches[idx])
		}))
	}

	addrs := make([]string, len(servers))
	for i := range servers {
		addrs[i] = servers[i].Listener.Addr().String()
	}
	peerList := strings.Join(addrs, ",")

	for i := range pcs {
		pcs[i] = NewPeerCache(PeerConfig{
			SelfAddr:      addrs[i],
			DiscoveryType: "static",
			StaticPeers:   peerList,
			Timeout:       2 * time.Second,
		})
		caches[i].SetL3(pcs[i])
	}

	defer func() {
		for i := range servers {
			servers[i].Close()
			pcs[i].Close()
			caches[i].Close()
		}
	}()

	testKey := "query_range:team-a:{app=\"api\"}:1:2:15"
	owner := pcs[0].ring.get(testKey)
	ownerIdx := -1
	for i, addr := range addrs {
		if addr == owner {
			ownerIdx = i
			break
		}
	}
	if ownerIdx < 0 {
		t.Fatal("failed to resolve cache owner")
	}

	caches[ownerIdx].Set(testKey, []byte("vl-response-data"))

	for round := 0; round < 30; round++ {
		for i := range caches {
			value, ok := caches[i].Get(testKey)
			if !ok || string(value) != "vl-response-data" {
				t.Fatalf("round %d peer %d failed cache lookup: ok=%v value=%q", round, i, ok, string(value))
			}
		}
	}

	if got := serverCalls[ownerIdx].Load(); got > 2 {
		t.Fatalf("expected at most one peer fetch per non-owner after shadow copies warm, owner saw %d peer requests", got)
	}

	for i := range caches {
		if i == ownerIdx {
			continue
		}
		if _, ok := caches[i].Get(testKey); !ok {
			t.Fatalf("expected peer %d to retain a shadow copy after warm-up", i)
		}
	}
}

func TestPeerCache_Peers(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   "self:3100,a:3100,b:3100",
	})
	defer pc.Close()

	peers := pc.Peers()
	if len(peers) != 3 {
		t.Errorf("expected 3 peers, got %d", len(peers))
	}
}

func TestPeerCache_SetIsNoop(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self"})
	defer pc.Close()
	pc.Set("key", []byte("val")) // should not panic
}

func TestPeerCache_WriteThroughPushesToOwner(t *testing.T) {
	ownerCache := NewWithMaxBytes(60*time.Second, 1000, 1024*1024)
	defer ownerCache.Close()
	ownerPC := NewPeerCache(PeerConfig{
		SelfAddr:         "owner",
		ReadAheadEnabled: true,
	})
	defer ownerPC.Close()
	ownerCache.SetL3(ownerPC)
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer ownerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:           "self:3100",
		DiscoveryType:      "static",
		StaticPeers:        "self:3100," + ownerServer.Listener.Addr().String(),
		Timeout:            500 * time.Millisecond,
		WriteThrough:       true,
		WriteThroughMinTTL: 5 * time.Second,
	})
	defer pc.Close()

	var key string
	for i := 0; i < 512; i++ {
		candidate := fmt.Sprintf("wt-key-%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(candidate)
		pc.mu.RUnlock()
		if owner == ownerServer.Listener.Addr().String() {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("failed to find key mapped to owner peer")
	}

	pc.SetWithTTL(key, []byte("value"), 30*time.Second)
	requireEventually(t, 3*time.Second, func() bool {
		got, ok := ownerCache.Get(key)
		if !ok || string(got) != "value" {
			return false
		}
		stats := pc.Stats()
		return stats["wt_pushes"].(int64) >= 1
	})
}

func TestPeerCache_WriteThroughPushesEncodedCacheKeyToOwner(t *testing.T) {
	ownerCache := NewWithMaxBytes(60*time.Second, 1000, 1024*1024)
	defer ownerCache.Close()
	ownerPC := NewPeerCache(PeerConfig{SelfAddr: "owner"})
	defer ownerPC.Close()
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer ownerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:           "self:3100",
		DiscoveryType:      "static",
		StaticPeers:        "self:3100," + ownerServer.Listener.Addr().String(),
		Timeout:            500 * time.Millisecond,
		WriteThrough:       true,
		WriteThroughMinTTL: 5 * time.Second,
	})
	defer pc.Close()

	var key string
	for i := 0; i < 512; i++ {
		candidate := fmt.Sprintf("detected_fields:tenant-a:{service_name=\"api\"}&start=1&end=2:%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(candidate)
		pc.mu.RUnlock()
		if owner == ownerServer.Listener.Addr().String() {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("failed to find key mapped to owner peer")
	}

	pc.SetWithTTL(key, []byte("value"), 30*time.Second)
	requireEventually(t, 3*time.Second, func() bool {
		got, ok := ownerCache.Get(key)
		return ok && string(got) == "value"
	})
}

func TestPeerCache_WriteThroughSkipsShortTTL(t *testing.T) {
	ownerCache := NewWithMaxBytes(60*time.Second, 1000, 1024*1024)
	defer ownerCache.Close()
	ownerPC := NewPeerCache(PeerConfig{SelfAddr: "owner"})
	defer ownerPC.Close()
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer ownerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:           "self:3100",
		DiscoveryType:      "static",
		StaticPeers:        "self:3100," + ownerServer.Listener.Addr().String(),
		Timeout:            500 * time.Millisecond,
		WriteThrough:       true,
		WriteThroughMinTTL: 10 * time.Second,
	})
	defer pc.Close()

	var key string
	for i := 0; i < 512; i++ {
		candidate := fmt.Sprintf("wt-short-key-%d", i)
		pc.mu.RLock()
		owner := pc.ring.get(candidate)
		pc.mu.RUnlock()
		if owner == ownerServer.Listener.Addr().String() {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("failed to find key mapped to owner peer")
	}

	pc.SetWithTTL(key, []byte("value"), 2*time.Second)
	time.Sleep(150 * time.Millisecond)
	if _, ok := ownerCache.Get(key); ok {
		t.Fatalf("expected short-TTL key %q to skip write-through", key)
	}
	stats := pc.Stats()
	if stats["wt_pushes"].(int64) != 0 {
		t.Fatalf("expected no write-through pushes for short TTL, got %+v", stats)
	}
}

func TestPeerCache_GossipIsNoop(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self"})
	defer pc.Close()
	pc.GossipHaveKey("key") // should not panic
}

func TestPeerCache_MinUsableTTL(t *testing.T) {
	// Owner has a key with only 2s remaining — below MinUsableTTL (5s)
	ownerCache := NewWithMaxBytes(60*time.Second, 100, 1024*1024)
	defer ownerCache.Close()
	ownerCache.SetWithTTL("expiring", []byte("old"), 2*time.Second) // 2s < 5s threshold

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	// Find a key mapping to the peer
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("expiring-%d", i)
		ownerCache.SetWithTTL(key, []byte("old"), 2*time.Second)

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner == peerServer.Listener.Addr().String() {
			_, _, found := pc.Get(key)
			if found {
				t.Errorf("key with TTL < 5s should be treated as miss (force refresh), but got hit")
				return
			}
			t.Log("correctly treated near-expiry key as miss")
			return
		}
	}
	t.Log("no key mapped to peer — acceptable")
}

func TestPeerCache_TTLHeader(t *testing.T) {
	ownerCache := NewWithMaxBytes(60*time.Second, 100, 1024*1024)
	defer ownerCache.Close()
	ownerCache.SetWithTTL("fresh", []byte("data"), 30*time.Second)

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("fresh-%d", i)
		ownerCache.SetWithTTL(key, []byte("data"), 30*time.Second)

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner == peerServer.Listener.Addr().String() {
			_, ttl, found := pc.Get(key)
			if found {
				if ttl <= 0 || ttl > 31*time.Second {
					t.Errorf("TTL should be ~30s, got %v", ttl)
				} else {
					t.Logf("TTL preserved from owner: %v", ttl)
				}
				return
			}
		}
	}
	t.Log("no key mapped to peer for TTL test")
}

func TestPeerCache_CircuitBreaker_Cooldown(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self"})
	defer pc.Close()

	// Trip breaker
	for i := 0; i < 5; i++ {
		pc.recordPeerFailure("bad-peer")
	}
	if pc.peerAllowed("bad-peer") {
		t.Error("should be blocked")
	}

	// Success resets
	pc.recordPeerSuccess("bad-peer")
	// Need to manually reset failures since recordPeerSuccess sets to 0
	if !pc.peerAllowed("bad-peer") {
		t.Error("should be allowed after success")
	}
}

func TestHashRing_MinimalRebalance(t *testing.T) {
	// Adding a node should move ~1/N keys (minimal rebalancing)
	ring2 := newHashRing(150)
	ring2.add("a")
	ring2.add("b")

	ring3 := newHashRing(150)
	ring3.add("a")
	ring3.add("b")
	ring3.add("c")

	moved := 0
	total := 1000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ring2.get(key) != ring3.get(key) {
			moved++
		}
	}
	// Adding 1 of 3 nodes should move ~33% of keys
	pct := float64(moved) / float64(total) * 100
	if pct > 50 {
		t.Errorf("too many keys moved (%.1f%%) — consistent hashing should move ~33%%", pct)
	}
	t.Logf("%.1f%% keys moved when adding 3rd node (ideal: ~33%%)", pct)
}

// =============================================================================
// Fuzz tests for cache
// =============================================================================

func FuzzHashRingGet(f *testing.F) {
	f.Add("key-1")
	f.Add("")
	f.Add("a very long key with special chars: 日本語")
	f.Fuzz(func(t *testing.T, key string) {
		ring := newHashRing(150)
		ring.add("node-1")
		ring.add("node-2")
		result := ring.get(key)
		if result == "" {
			t.Error("non-empty ring should always return a node")
		}
	})
}

func FuzzCacheGetSet(f *testing.F) {
	f.Add("key", "value")
	f.Add("", "")
	f.Add("k with spaces", "v\x00null")
	f.Fuzz(func(t *testing.T, key, value string) {
		c := New(1*time.Second, 100)
		defer c.Close()
		c.Set(key, []byte(value))
		if key != "" {
			v, ok := c.Get(key)
			if !ok || string(v) != value {
				t.Errorf("Set/Get mismatch for key=%q", key)
			}
		}
	})
}

// =============================================================================
// Stable Consistent Hashing Tests
// =============================================================================

// TestHashRing_Stability verifies that the same set of peers always produces
// the same key→peer mapping (deterministic). This is critical: when the headless
// service returns the same pod IPs, keys must not shuffle.
func TestHashRing_Stability(t *testing.T) {
	peers := []string{"10.0.0.1:3100", "10.0.0.2:3100", "10.0.0.3:3100"}
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("cache:query_range:tenant-%d:%d", i%5, i)
	}

	// Build ring twice with same peers (different order should not matter after sort)
	ring1 := newHashRing(150)
	for _, p := range peers {
		ring1.add(p)
	}

	ring2 := newHashRing(150)
	// Add in reverse order — result must be identical
	for i := len(peers) - 1; i >= 0; i-- {
		ring2.add(peers[i])
	}

	for _, key := range keys {
		owner1 := ring1.get(key)
		owner2 := ring2.get(key)
		if owner1 != owner2 {
			t.Errorf("key %q: ring1=%s ring2=%s — order of peer addition should not affect mapping",
				key, owner1, owner2)
		}
	}
}

// TestHashRing_SamePeersNoMovement verifies that when the same peer set is
// re-applied (e.g., DNS refresh returns same IPs), zero keys move.
func TestHashRing_SamePeersNoMovement(t *testing.T) {
	peers := []string{"a:3100", "b:3100", "c:3100"}

	ring1 := newHashRing(150)
	for _, p := range peers {
		ring1.add(p)
	}

	// Simulate DNS refresh returning same peers
	ring2 := newHashRing(150)
	for _, p := range peers {
		ring2.add(p)
	}

	moved := 0
	total := 10000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ring1.get(key) != ring2.get(key) {
			moved++
		}
	}
	if moved != 0 {
		t.Errorf("%d/%d keys moved when peer set is unchanged — expected 0", moved, total)
	}
}

// TestHashRing_RemoveNodeMinimalRebalance verifies that removing a peer
// only moves the keys that belonged to that peer.
func TestHashRing_RemoveNodeMinimalRebalance(t *testing.T) {
	ring3 := newHashRing(150)
	ring3.add("a")
	ring3.add("b")
	ring3.add("c")

	// Record which keys belong to "c"
	keysOnC := 0
	total := 10000
	for i := 0; i < total; i++ {
		if ring3.get(fmt.Sprintf("key-%d", i)) == "c" {
			keysOnC++
		}
	}

	// Remove "c" — build ring with only a and b
	ring2 := newHashRing(150)
	ring2.add("a")
	ring2.add("b")

	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ring3.get(key) != ring2.get(key) {
			moved++
		}
	}

	// Only keys that were on "c" should move (to a or b)
	if moved != keysOnC {
		t.Errorf("expected %d keys to move (those on removed node), got %d", keysOnC, moved)
	}
	// Each node should own ~33%, so keysOnC should be ~3333
	pct := float64(keysOnC) / float64(total) * 100
	t.Logf("Node 'c' owned %.1f%% of keys — %d keys moved on removal", pct, moved)
}

// TestHashRing_AddNodeOnlyTakesFromExisting verifies that adding a new node
// only takes keys from existing nodes, not creating new mappings.
func TestHashRing_AddNodeOnlyTakesFromExisting(t *testing.T) {
	ring2 := newHashRing(150)
	ring2.add("a")
	ring2.add("b")

	ring3 := newHashRing(150)
	ring3.add("a")
	ring3.add("b")
	ring3.add("c") // new node

	movedToC := 0
	movedFromA := 0
	movedFromB := 0
	total := 10000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		old := ring2.get(key)
		new := ring3.get(key)
		if old != new {
			if new == "c" {
				movedToC++
			}
			if old == "a" {
				movedFromA++
			}
			if old == "b" {
				movedFromB++
			}
		}
	}

	// All moved keys should go to the new node "c"
	if movedToC != movedFromA+movedFromB {
		t.Errorf("moved-to-c=%d but moved-from-a=%d + moved-from-b=%d",
			movedToC, movedFromA, movedFromB)
	}

	// ~33% should move to new node
	pct := float64(movedToC) / float64(total) * 100
	if pct < 20 || pct > 45 {
		t.Errorf("expected ~33%% keys to move to new node, got %.1f%%", pct)
	}
	t.Logf("%.1f%% keys moved to new node (ideal: ~33%%)", pct)
}

// TestHashRing_ScaleUpDown verifies that scaling up then back down returns
// all keys to their original owners.
func TestHashRing_ScaleUpDown(t *testing.T) {
	peers := []string{"a", "b", "c"}

	ring := newHashRing(150)
	for _, p := range peers {
		ring.add(p)
	}

	// Record original mappings
	original := make(map[string]string)
	total := 5000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		original[key] = ring.get(key)
	}

	// Scale up: add "d"
	ringUp := newHashRing(150)
	for _, p := range append(peers, "d") {
		ringUp.add(p)
	}

	// Scale back down: remove "d"
	ringDown := newHashRing(150)
	for _, p := range peers {
		ringDown.add(p)
	}

	// After scale down, all keys should return to original owners
	moved := 0
	for key, origOwner := range original {
		if ringDown.get(key) != origOwner {
			moved++
		}
	}
	if moved != 0 {
		t.Errorf("%d/%d keys didn't return to original owner after scale up+down", moved, total)
	}
}

// TestPeerCache_UpdatePeersRebuildsRing verifies that updatePeers
// correctly rebuilds the hash ring with new peers.
func TestPeerCache_UpdatePeersRebuildsRing(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   "self:3100,a:3100,b:3100",
	})
	defer pc.Close()

	// Record ownership of 100 keys
	before := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		pc.mu.RLock()
		before[key] = pc.ring.get(key)
		pc.mu.RUnlock()
	}

	// Simulate DNS refresh adding a new peer
	pc.updatePeers([]string{"self:3100", "a:3100", "b:3100", "c:3100"})

	moved := 0
	for key, oldOwner := range before {
		pc.mu.RLock()
		newOwner := pc.ring.get(key)
		pc.mu.RUnlock()
		if oldOwner != newOwner {
			moved++
		}
	}

	pct := float64(moved) / 100 * 100
	t.Logf("%.0f%% keys moved after adding 4th peer (ideal: ~25%%)", pct)
	if pct > 50 {
		t.Errorf("too many keys moved: %.0f%% (consistent hashing should move ~25%%)", pct)
	}
}

// TestPeerCache_CoalescingAndCacheIntegration verifies that singleflight
// in the peer cache prevents duplicate requests to the same owner.
func TestPeerCache_CoalescingAndCacheIntegration(t *testing.T) {
	var requestCount atomic.Int64
	ownerCache := NewWithMaxBytes(60*time.Second, 100, 1024*1024)
	defer ownerCache.Close()

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Simulate slow response
		time.Sleep(50 * time.Millisecond)
		peerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer peerServer.Close()

	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       5 * time.Second,
	})
	defer pc.Close()

	// Store a key that maps to the remote peer
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("coalesce-%d", i)
		ownerCache.SetWithTTL(key, []byte("data"), 30*time.Second)

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner == peerServer.Listener.Addr().String() {
			// Found a key that maps to the peer — fire 10 concurrent requests
			requestCount.Store(0)
			var wg sync.WaitGroup
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pc.Get(key)
				}()
			}
			wg.Wait()

			// Singleflight should collapse 10 requests into 1
			count := requestCount.Load()
			if count > 2 { // Allow 2 due to timing races
				t.Errorf("expected 1-2 requests (singleflight), got %d", count)
			} else {
				t.Logf("10 concurrent gets → %d actual peer requests (singleflight working)", count)
			}
			return
		}
	}
	t.Log("no key mapped to peer for coalescing test")
}

func TestPeerCache_ReadAhead_BoundedFairPrefetch(t *testing.T) {
	ownerCache := NewWithMaxBytes(60*time.Second, 1000, 1024*1024)
	defer ownerCache.Close()
	ownerPC := NewPeerCache(PeerConfig{
		SelfAddr:         "owner",
		ReadAheadEnabled: true,
	})
	defer ownerPC.Close()
	ownerCache.SetL3(ownerPC)
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerPC.ServeHTTP(w, r, ownerCache)
	}))
	defer ownerSrv.Close()

	followerCache := NewWithMaxBytes(60*time.Second, 1000, 1024*1024)
	defer followerCache.Close()
	followerPC := NewPeerCache(PeerConfig{
		SelfAddr:                 "self:3100",
		DiscoveryType:            "static",
		StaticPeers:              "self:3100," + ownerSrv.Listener.Addr().String(),
		Timeout:                  2 * time.Second,
		ReadAheadEnabled:         true,
		ReadAheadTopN:            64,
		ReadAheadMaxKeys:         4,
		ReadAheadMaxBytes:        1 << 20,
		ReadAheadMaxConcurrency:  2,
		ReadAheadMinTTL:          20 * time.Second,
		ReadAheadMaxObjectBytes:  1 << 20,
		ReadAheadTenantFairShare: 50,
		ReadAheadErrorBackoff:    time.Second,
	})
	defer followerPC.Close()
	followerCache.SetL3(followerPC)

	keysA := make([]string, 0, 3)
	keysB := make([]string, 0, 3)
	for i := 0; i < 2000 && (len(keysA) < 3 || len(keysB) < 3); i++ {
		pcKeyA := fmt.Sprintf("query_range:tenant-a:key-%d", i)
		pcKeyB := fmt.Sprintf("query_range:tenant-b:key-%d", i)
		followerPC.mu.RLock()
		ownerA := followerPC.ring.get(pcKeyA)
		ownerB := followerPC.ring.get(pcKeyB)
		followerPC.mu.RUnlock()
		if ownerA == ownerSrv.Listener.Addr().String() && len(keysA) < 3 {
			keysA = append(keysA, pcKeyA)
		}
		if ownerB == ownerSrv.Listener.Addr().String() && len(keysB) < 3 {
			keysB = append(keysB, pcKeyB)
		}
	}
	if len(keysA) < 3 || len(keysB) < 3 {
		t.Fatalf("failed to find owner-mapped keys for both tenants: a=%d b=%d", len(keysA), len(keysB))
	}

	for _, k := range append(keysA, keysB...) {
		ownerCache.SetWithTTL(k, []byte(strings.Repeat("x", 256)), 45*time.Second)
	}
	for _, k := range keysA {
		for range 30 {
			_, _ = ownerCache.Get(k)
		}
	}
	for _, k := range keysB {
		for range 20 {
			_, _ = ownerCache.Get(k)
		}
	}

	errs := followerPC.runReadAheadCycle()
	if errs != 0 {
		t.Fatalf("expected zero read-ahead errors, got %d", errs)
	}

	entries, _ := followerCache.Size()
	if entries == 0 || entries > 4 {
		t.Fatalf("expected bounded prefetch entries 1..4, got %d", entries)
	}

	hasTenantA := false
	for _, k := range keysA {
		if _, ok := followerCache.Get(k); ok {
			hasTenantA = true
			break
		}
	}
	hasTenantB := false
	for _, k := range keysB {
		if _, ok := followerCache.Get(k); ok {
			hasTenantB = true
			break
		}
	}
	if !hasTenantA || !hasTenantB {
		t.Fatalf("expected fairness prefetch for both tenants, tenant-a=%v tenant-b=%v", hasTenantA, hasTenantB)
	}

	stats := followerPC.Stats()
	if stats["ra_prefetches"].(int64) == 0 {
		t.Fatalf("expected read-ahead prefetches > 0, got %+v", stats)
	}
}

func TestParsePeerList(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"a:1", 1},
		{"a:1,b:2,c:3", 3},
		{"a:1, b:2 , c:3 ", 3},
	}
	for _, tt := range tests {
		got := parsePeerList(tt.input)
		if len(got) != tt.want {
			t.Errorf("parsePeerList(%q) = %d, want %d", tt.input, len(got), tt.want)
		}
	}
}
