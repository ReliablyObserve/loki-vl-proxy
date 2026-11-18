package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A value of the everywhere class reaches every peer, not only the key's ring replicas, and goes to the handler
// its owner registered instead of the cache tiers; a key outside the class is never broadcast, and a peer with no
// handler drops it.
//
// conformance: semantics/error-cache-vl-refusal-peer-sync
func TestPeerCacheBroadcastReachesEveryPeer(t *testing.T) {
	const n = 4
	caches := make([]*Cache, n)
	pcs := make([]*PeerCache, n)
	addrs := make([]string, n)
	for i := range caches {
		caches[i] = New(60*time.Second, 1000)
		t.Cleanup(caches[i].Close)
		idx := i
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { pcs[idx].ServeHTTP(w, r, caches[idx]) }))
		t.Cleanup(srv.Close)
		addrs[i] = srv.Listener.Addr().String()
	}
	var mu sync.Mutex
	got := map[int][]byte{}
	var calls atomic.Int64
	for i := range pcs {
		pcs[i] = NewPeerCache(PeerConfig{SelfAddr: addrs[i], DiscoveryType: "static", StaticPeers: strings.Join(addrs, ","),
			Timeout: 2 * time.Second, ReplicationFactor: 1})
		t.Cleanup(pcs[i].Close)
		caches[i].SetL3(pcs[i])
		if i == n-1 {
			continue // no handler: drops it
		}
		idx := i
		pcs[i].HandleEverywhere(func(key string, value []byte, ttl time.Duration) {
			calls.Add(1)
			if !strings.HasPrefix(key, EverywherePrefix) || ttl <= 0 {
				t.Errorf("handler got %q ttl %v", key, ttl)
			}
			mu.Lock()
			got[idx] = value
			mu.Unlock()
		})
	}
	if pcs[0].Broadcast("plain-key", []byte("v"), time.Minute) || pcs[0].Broadcast(EverywherePrefix+"k", nil, time.Minute) {
		t.Fatal("a key outside the class or an empty value must not be broadcast")
	}
	if !pcs[0].Broadcast(EverywherePrefix+"k", []byte("verdict"), time.Minute) {
		t.Fatal("not queued")
	}
	deadline := time.Now().Add(10 * time.Second)
	for calls.Load() < n-2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != n-2 || got[0] != nil {
		t.Fatalf("handlers reached %d peers (%v), want every peer with a handler but the sender", len(got), got)
	}
	for i := range caches {
		if _, ok := caches[i].Peek(EverywherePrefix + "k"); ok {
			t.Fatalf("peer %d stored the value in its cache tiers", i)
		}
	}
	pcs[1].HandleEverywhere(nil)
	var nilPC *PeerCache
	nilPC.HandleEverywhere(nil)
	if nilPC.Broadcast(EverywherePrefix+"k", []byte("v"), time.Minute) {
		t.Fatal("a nil peer cache broadcasts nothing")
	}
}
