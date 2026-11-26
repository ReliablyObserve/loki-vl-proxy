package cache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A key the push filter keeps on its replica is neither listed in the hot
// index served to peers nor prefetched from a peer that lists it.
//
// conformance: cache-key-credential-digest, peer-cache-replication
func TestPeerCache_PushFilterKeepsKeysOutOfHotIndexAndReadAhead(t *testing.T) {
	localCache := New(60*time.Second, 1000)
	defer localCache.Close()
	pc := NewPeerCache(PeerConfig{SelfAddr: "localhost:3100", ReadAheadEnabled: true})
	defer pc.Close()
	localCache.SetL3(pc)
	pc.SetPushFilter(func(key string) bool { return !strings.Contains(key, "local-only") })

	localCache.Set("query_range:tenant-a:shared", []byte("v"))
	localCache.Set("query_range:tenant-a:local-only", []byte("v"))
	for range 5 {
		_, _ = localCache.Get("query_range:tenant-a:shared")
		_, _ = localCache.Get("query_range:tenant-a:local-only")
	}
	localCache.drainPromotions()

	w := httptest.NewRecorder()
	pc.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/_cache/hot?limit=10", nil), localCache)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	decoded, err := decodePeerResponseBody(w.Header().Get("Content-Encoding"), w.Body.Bytes(), maxPeerHotIndexBytes)
	if err != nil {
		t.Fatal(err)
	}
	var payload hotIndexResponse
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range payload.Entries {
		keys = append(keys, e.Key)
	}
	if len(keys) != 1 || keys[0] != "query_range:tenant-a:shared" {
		t.Fatalf("hot index lists %v, want only the shareable key", keys)
	}

	// A peer (an older build) that lists the key anyway: nothing is fetched.
	other := New(60*time.Second, 1000)
	defer other.Close()
	if pc.prefetchHotKey(other, hotIndexEntry{Key: "query_range:tenant-a:local-only", RemainingTTLMS: 60000}) {
		t.Fatal("a key the push filter keeps local was prefetched")
	}
	if _, ok := other.Get("query_range:tenant-a:local-only"); ok {
		t.Fatal("the key reached the cache")
	}
}
