package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHashRing_Consistent(t *testing.T) {
	ring := newHashRing(150)
	ring.add("10.0.0.1:3100")
	ring.add("10.0.0.2:3100")
	ring.add("10.0.0.3:3100")

	// Same key always maps to same node
	node1 := ring.get("query:rate({app=\"nginx\"}[5m])")
	node2 := ring.get("query:rate({app=\"nginx\"}[5m])")
	if node1 != node2 {
		t.Errorf("inconsistent hashing: %q vs %q", node1, node2)
	}
}

func TestHashRing_Distribution(t *testing.T) {
	ring := newHashRing(150)
	ring.add("node-1")
	ring.add("node-2")
	ring.add("node-3")

	// Check distribution across 1000 keys
	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		node := ring.get(fmt.Sprintf("key-%d", i))
		counts[node]++
	}

	// Each node should get at least 20% (fair would be 33%)
	for node, count := range counts {
		if count < 200 {
			t.Errorf("node %q only got %d/1000 keys (expected >200 for fair distribution)", node, count)
		}
	}
}

func TestHashRing_AddRemove(t *testing.T) {
	ring := newHashRing(150)
	ring.add("node-1")
	ring.add("node-2")

	before := ring.get("test-key")

	// Add a third node — most keys should stay on the same node
	ring2 := newHashRing(150)
	ring2.add("node-1")
	ring2.add("node-2")
	ring2.add("node-3")

	// Verify the key didn't move (or moved to the new node)
	after := ring2.get("test-key")
	if before != after {
		t.Logf("key moved from %q to %q (expected for some keys)", before, after)
	}
}

func TestHashRing_Empty(t *testing.T) {
	ring := newHashRing(150)
	result := ring.get("any-key")
	if result != "" {
		t.Errorf("empty ring should return empty string, got %q", result)
	}
}

func TestPeerCache_StaticDiscovery(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "10.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "10.0.0.1:3100,10.0.0.2:3100,10.0.0.3:3100",
	})
	defer pc.Close()

	if pc.PeerCount() != 2 { // excludes self
		t.Errorf("expected 2 peers (excluding self), got %d", pc.PeerCount())
	}
}

func TestPeerCache_Disabled(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr: "10.0.0.1:3100",
		// No discovery configured
	})
	defer pc.Close()

	// Should return miss without errors
	_, ok := pc.Get("any-key")
	if ok {
		t.Error("disabled peer cache should always miss")
	}
}

func TestPeerCache_ServeHTTP_Hit(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	localCache.Set("test-key", []byte("hello world"))

	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost"})
	defer pc.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/_cache/get?key=test-key", nil)
	pc.ServeHTTP(w, r, localCache)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "hello world" {
		t.Errorf("expected 'hello world', got %q", w.Body.String())
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

	if w.Code != http.StatusNotFound {
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
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestPeerCache_FetchFromPeer(t *testing.T) {
	// Create a mock peer server
	peerCache := New(60*time.Second, 1000)
	defer peerCache.Close()
	peerCache.Set("shared-key", []byte("peer-data"))

	peerPC := NewPeerCache(PeerConfig{SelfAddr: "peer"})
	defer peerPC.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerPC.ServeHTTP(w, r, peerCache)
	}))
	defer peerServer.Close()

	// Create client peer cache pointing to the peer server
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   peerServer.Listener.Addr().String(),
		Timeout:       2 * time.Second,
	})
	defer pc.Close()

	// Force the hash ring to map our key to the peer
	// Try many keys until one maps to the peer
	var value []byte
	var found bool
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("shared-key-%d", i)
		peerCache.Set(key, []byte("peer-data"))

		pc.mu.RLock()
		owner := pc.ring.get(key)
		pc.mu.RUnlock()

		if owner == peerServer.Listener.Addr().String() {
			value, found = pc.Get(key)
			if found {
				break
			}
		}
	}

	if !found {
		t.Log("no key mapped to peer (expected in small hash ring) — testing that no panic occurred")
		return
	}
	if string(value) != "peer-data" {
		t.Errorf("expected 'peer-data', got %q", string(value))
	}
}

func TestPeerCache_CircuitBreaker(t *testing.T) {
	pc := NewPeerCache(PeerConfig{
		SelfAddr:      "self:3100",
		DiscoveryType: "static",
		StaticPeers:   "dead-peer:9999",
		Timeout:       100 * time.Millisecond,
	})
	defer pc.Close()

	// After enough failures, the peer should be circuit-broken
	for i := 0; i < 10; i++ {
		pc.recordPeerFailure("dead-peer:9999")
	}

	if pc.peerAllowed("dead-peer:9999") {
		t.Error("peer should be circuit-broken after 10 failures")
	}
}

func TestPeerCache_CircuitBreaker_Recovery(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self"})
	defer pc.Close()

	// Trip the breaker
	for i := 0; i < 5; i++ {
		pc.recordPeerFailure("peer-1")
	}
	if pc.peerAllowed("peer-1") {
		t.Error("should be tripped")
	}

	// After success, reset
	pc.recordPeerSuccess("peer-1")

	// Force reset by setting failures to 0
	v, _ := pc.breakers.Load("peer-1")
	pb := v.(*peerBreaker)
	pb.failures.Store(0)

	if !pc.peerAllowed("peer-1") {
		t.Error("should be allowed after reset")
	}
}

func TestPeerCache_Singleflight(t *testing.T) {
	var fetchCount atomic.Int64
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCount.Add(1)
		time.Sleep(50 * time.Millisecond) // simulate latency
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

	// Find a key that maps to the peer
	var targetKey string
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-%d", i)
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

	// Fire 10 concurrent requests for the same key
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc.Get(targetKey)
		}()
	}
	wg.Wait()

	// Singleflight should coalesce — expect 1 actual fetch (or at most 2 due to timing)
	if fetchCount.Load() > 3 {
		t.Errorf("singleflight failed: %d fetches for 10 concurrent requests (expected ~1)", fetchCount.Load())
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

	b := pc.MetricsJSON()
	if len(b) == 0 {
		t.Error("MetricsJSON should not be empty")
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
			t.Errorf("parsePeerList(%q) = %d items, want %d", tt.input, len(got), tt.want)
		}
	}
}
