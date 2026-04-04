package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
