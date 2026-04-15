package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

func benchmarkDiskPayload() []byte {
	return []byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api","env":"prod","service_name":"checkout"},"values":[["1735689600000000000","{\"trace_id\":\"trace-0001\",\"custom.team\":\"payments\",\"custom.region\":\"eu-west-1\",\"msg\":\"bench\"}"]]}]}}`)
}

func BenchmarkDiskCache_SetFlush_Compressed(b *testing.B) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(b.TempDir(), "disk-cache.db"),
		Compression: true,
		FlushSize:   1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dc.Close() })

	value := benchmarkDiskPayload()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dc.Set(fmt.Sprintf("disk-key-%d", i), value, 30*time.Second)
	}
}

func BenchmarkDiskCache_Get_Hit_Compressed(b *testing.B) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(b.TempDir(), "disk-cache.db"),
		Compression: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dc.Close() })

	dc.Set("bench-key", benchmarkDiskPayload(), 30*time.Second)
	dc.Flush()

	b.ResetTimer()
	for b.Loop() {
		if _, ok := dc.Get("bench-key"); !ok {
			b.Fatal("expected disk-cache hit")
		}
	}
}

func BenchmarkDiskCache_Get_Hit_Uncompressed(b *testing.B) {
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(b.TempDir(), "disk-cache.db"),
		Compression: false,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dc.Close() })

	dc.Set("bench-key", benchmarkDiskPayload(), 30*time.Second)
	dc.Flush()

	b.ResetTimer()
	for b.Loop() {
		if _, ok := dc.Get("bench-key"); !ok {
			b.Fatal("expected disk-cache hit")
		}
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

func BenchmarkCache_TopHotKeys(b *testing.B) {
	c := New(10*time.Minute, 20000)
	defer c.Close()

	for i := range 10000 {
		k := fmt.Sprintf("query_range:tenant-a:key-%d", i)
		c.SetWithTTL(k, []byte(`{"result":"ok"}`), 5*time.Minute)
		hits := 1 + (i % 32)
		for range hits {
			_, _ = c.Get(k)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = c.TopHotKeys(256, 15*time.Second, 128*1024)
	}
}

func BenchmarkPeerCache_ReadAheadCycle_Bounded(b *testing.B) {
	caches := make([]*Cache, 2)
	pcs := make([]*PeerCache, 2)
	servers := make([]*httptest.Server, 2)

	for i := range caches {
		caches[i] = New(10*time.Minute, 50000)
	}

	for i := range servers {
		idx := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pcs[idx].ServeHTTP(w, r, caches[idx])
		}))
	}

	addrs := []string{servers[0].Listener.Addr().String(), servers[1].Listener.Addr().String()}
	peers := strings.Join(addrs, ",")

	for i := range pcs {
		pcs[i] = NewPeerCache(PeerConfig{
			SelfAddr:                 addrs[i],
			DiscoveryType:            "static",
			StaticPeers:              peers,
			Timeout:                  2 * time.Second,
			ReadAheadEnabled:         false,
			ReadAheadTopN:            256,
			ReadAheadMaxKeys:         64,
			ReadAheadMaxBytes:        8 * 1024 * 1024,
			ReadAheadMaxConcurrency:  4,
			ReadAheadMinTTL:          15 * time.Second,
			ReadAheadMaxObjectBytes:  128 * 1024,
			ReadAheadTenantFairShare: 50,
			ReadAheadErrorBackoff:    5 * time.Second,
			ReadAheadInterval:        30 * time.Second,
			ReadAheadJitter:          0,
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

	ownerIdx := 0
	followerIdx := 1
	for i := range 5000 {
		k := fmt.Sprintf("query_range:tenant-%d:key-%d", i%8, i)
		val := []byte(fmt.Sprintf(`{"stream":%d,"ok":true}`, i))
		caches[ownerIdx].SetWithTTL(k, val, 5*time.Minute)
		hits := 1 + (i % 64)
		for range hits {
			_, _ = caches[ownerIdx].Get(k)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = pcs[followerIdx].runReadAheadCycle()
	}
}
