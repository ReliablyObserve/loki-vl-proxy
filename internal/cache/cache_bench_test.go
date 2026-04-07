package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func BenchmarkCache_Set(b *testing.B) {
	c := New(60*time.Second, 100000)
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	for b.Loop() {
		c.Set(fmt.Sprintf("key-%d", b.N), value)
	}
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	c := New(60*time.Second, 100000)
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	c.Set("bench-key", value)
	for b.Loop() {
		c.Get("bench-key")
	}
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	c := New(60*time.Second, 100000)
	for b.Loop() {
		c.Get("nonexistent")
	}
}

func BenchmarkCache_SetWithTTL(b *testing.B) {
	c := New(60*time.Second, 100000)
	value := []byte(`{"data":"test"}`)
	for b.Loop() {
		c.SetWithTTL(fmt.Sprintf("key-%d", b.N), value, 30*time.Second)
	}
}

func BenchmarkPeerCache_ThreePeers_ShadowCopyHit(b *testing.B) {
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

	b.Cleanup(func() {
		for i := range servers {
			servers[i].Close()
			pcs[i].Close()
			caches[i].Close()
		}
	})

	testKey := "compat:series:team-a:app-api:window-1"
	owner := pcs[0].ring.get(testKey)
	ownerIdx := 0
	for i, addr := range addrs {
		if addr == owner {
			ownerIdx = i
			break
		}
	}
	nonOwnerIdx := (ownerIdx + 1) % len(caches)

	caches[ownerIdx].Set(testKey, []byte(`{"status":"success","data":[{"app":"api"}]}`))
	if _, ok := caches[nonOwnerIdx].Get(testKey); !ok {
		b.Fatal("expected first non-owner fetch to create a shadow copy")
	}

	b.ResetTimer()
	for b.Loop() {
		if _, ok := caches[nonOwnerIdx].Get(testKey); !ok {
			b.Fatal("expected warm shadow-copy hit")
		}
	}
}
