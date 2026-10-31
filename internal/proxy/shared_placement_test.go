package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

type sharedPlacementNode struct {
	c       *cache.Cache
	disk    *cache.DiskCache
	peer    *cache.PeerCache
	srv     *httptest.Server
	handler atomic.Pointer[cache.PeerCache]
}

func sharedPlacementFleet(t *testing.T, rf int) []*sharedPlacementNode {
	t.Helper()
	var nodes []*sharedPlacementNode
	for range 3 {
		node := &sharedPlacementNode{c: cache.NewWithMaxBytes(time.Hour, 1000, 8<<20)}
		node.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if pc := node.handler.Load(); pc != nil {
				pc.ServeHTTP(w, r, node.c)
			} else {
				http.NotFound(w, r)
			}
		}))
		nodes = append(nodes, node)
	}
	var addresses []string
	for _, node := range nodes {
		addresses = append(addresses, strings.TrimPrefix(node.srv.URL, "http://"))
	}
	for i, node := range nodes {
		var err error
		node.disk, err = cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db", FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		node.peer = cache.NewPeerCache(cache.PeerConfig{SelfAddr: addresses[i], DiscoveryType: "static", StaticPeers: strings.Join(addresses, ","), ReplicationFactor: rf, WriteThrough: true, WriteThroughMinTTL: time.Second, Timeout: time.Second})
		node.peer.SetPushFilter(peerPushAllowed)
		node.c.SetL2(node.disk)
		node.c.SetL3(node.peer)
		node.handler.Store(node.peer)
		t.Cleanup(func() { node.srv.Close(); node.peer.Close(); node.c.Close(); _ = node.disk.Close() })
	}
	return nodes
}

func TestSharedMetadataPlacementAcrossThreeFrontends(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			nodes := sharedPlacementFleet(t, rf)
			for _, endpoint := range []string{"labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels"} {
				t.Run(endpoint, func(t *testing.T) {
					key := endpoint + ":tenant-a:placement-proof"
					for _, node := range nodes {
						p := &Proxy{cache: node.c}
						p.setEndpointReadCacheWithTTL(endpoint, key, []byte(`{"status":"success","data":["shared"]}`), time.Minute)
					}
					deadline := time.Now().Add(3 * time.Second)
					for {
						correct := true
						for _, node := range nodes {
							node.disk.Flush()
							_, present := node.disk.Get(key)
							correct = correct && present == node.peer.IsReplica(key)
						}
						if correct {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("shared metadata did not converge to exactly the configured replica set")
						}
						time.Sleep(time.Millisecond)
					}
					copies := 0
					for _, node := range nodes {
						if _, ok := node.disk.Get(key); ok {
							copies++
						}
						body, _, _, ok := (&Proxy{cache: node.c}).endpointReadCacheEntry(endpoint, key)
						if !ok || !strings.Contains(string(body), "shared") {
							t.Fatal("a frontend cannot reuse the shared answer")
						}
					}
					if copies != rf {
						t.Fatalf("persistent copies=%d, configured=%d", copies, rf)
					}
				})
			}
		})
	}
}
