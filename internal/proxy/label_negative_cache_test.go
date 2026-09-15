package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// countingEmptyVL answers every metadata call with an empty list and counts calls.
func countingEmptyVL(t *testing.T, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		calls.Add(1)
		switch r.URL.Path {
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			writeVLFieldValues(w, nil)
		default:
			writeVLFieldNames(w, nil)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newDiskBackedCache(t *testing.T) (*cache.Cache, *cache.DiskCache) {
	t.Helper()
	// Production default -disk-cache-min-ttl.
	dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db", MinTTL: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dc.Close() })
	c := cache.New(60*time.Second, 1000)
	c.SetL2(dc)
	t.Cleanup(c.Close)
	return c, dc
}

// An empty backend answer goes through the normal write path with the 30s
// negative TTL, so it overwrites an earlier non-empty answer in memory and on
// disk (30s is not below the disk minimum TTL). After it expires the old list is
// never served from disk; the next read asks the backend again.
func TestLabelsNegativeCache_EmptyAnswerOverwritesPersistedNonEmptyCopy(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, label, path string
	}{
		{name: "labels", endpoint: "labels", path: "/loki/api/v1/labels"},
		{name: "label_values", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := new(atomic.Int64)
			srv := countingEmptyVL(t, calls)
			c, dc := newDiskBackedCache(t)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			path := tc.path + "?start=1703980800000000000&end=1704067200000000000"
			req := httptest.NewRequest(http.MethodGet, path, nil)
			var key string
			if tc.label != "" {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req, tc.label)
			} else {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req)
			}
			// 1. A non-empty answer cached in memory and on disk.
			p.setEndpointReadCacheWithTTL(tc.endpoint, key, lokiLabelsResponse([]string{"old-list"}), time.Hour)
			dc.Flush()
			if body, onDisk := dc.Get(key); !onDisk || !strings.Contains(string(body), "old-list") {
				t.Fatal("non-empty answer was not persisted to disk")
			}
			// 2. The backend then answers empty (the handler's cache write).
			p.setMetadataListCache(tc.endpoint, key, lokiLabelsResponse([]string{}), 0, time.Hour)
			dc.Flush()
			if body, onDisk := dc.Get(key); !onDisk || !metadataListPayloadEmpty(body) {
				t.Fatalf("disk copy not overwritten by the empty answer: ok=%v body=%s", onDisk, body)
			}
			if got := serveLokiStrings(t, mux, path); len(got) != 0 {
				t.Fatalf("within the negative TTL got %v, want the empty list", got)
			}
			// 3. The empty entry expires.
			restore := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + time.Second)
			defer restore()
			// 4. The next read asks the backend again instead of serving the old list.
			before := calls.Load()
			if got := serveLokiStrings(t, mux, path); contains(got, "old-list") {
				t.Fatalf("old non-empty list served after the empty answer expired: %v", got)
			}
			if calls.Load() == before {
				t.Fatal("read after the negative TTL did not reach the backend")
			}
		})
	}
}

// For merged multi-tenant responses the 30s empty answer also reaches the owner
// peer through write-through (30s is not below the write-through minimum) and
// overwrites the owner's non-empty copy in memory and on its disk. This uses a
// real owner Cache and DiskCache behind the peer cache HTTP handler.
func TestMultiTenantFanout_EmptyMergeOverwritesOwnerPeerCopy(t *testing.T) {
	calls := new(atomic.Int64)
	srv := countingEmptyVL(t, calls)

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values?start=1703980800000000000&end=1704067200000000000", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	old := lokiLabelsResponse([]string{"old-list"})

	ownerCache, ownerDisk := newDiskBackedCache(t)
	var ownerPeer atomic.Pointer[cache.PeerCache]
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pc := ownerPeer.Load(); pc != nil {
			pc.ServeHTTP(w, r, ownerCache)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ownerSrv.Close)
	ownerAddr := strings.TrimPrefix(ownerSrv.URL, "http://")

	c, _ := newDiskBackedCache(t)
	p, err := New(Config{
		BackendURL: srv.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	key, ok := p.multiTenantCacheKey(req, "label_values")
	if !ok {
		t.Fatal("request not multi-tenant cacheable")
	}

	// A two-member ring (production write-through minimum) in which the owner
	// server owns the key.
	peerConfig := func(self, members string) cache.PeerConfig {
		return cache.PeerConfig{
			SelfAddr:           self,
			DiscoveryType:      "static",
			StaticPeers:        members,
			Timeout:            time.Second,
			WriteThrough:       true,
			WriteThroughMinTTL: 30 * time.Second,
		}
	}
	var requester *cache.PeerCache
	var members string
	for i := 0; i < 1024 && requester == nil; i++ {
		self := fmt.Sprintf("127.0.0.1:%d", 20000+i)
		members = self + "," + ownerAddr
		candidate := cache.NewPeerCache(peerConfig(self, members))
		if candidate.IsOwner(key) {
			candidate.Close()
			continue
		}
		requester = candidate
	}
	if requester == nil {
		t.Fatal("no ring layout where the owner server owns the key")
	}
	t.Cleanup(requester.Close)
	owner := cache.NewPeerCache(peerConfig(ownerAddr, members))
	t.Cleanup(owner.Close)
	if !owner.IsOwner(key) {
		t.Fatal("owner peer does not own the key")
	}
	ownerPeer.Store(owner)
	ownerCache.SetL3(owner)
	c.SetL3(requester)

	// 1. The owner holds a non-empty merged answer in memory and on disk.
	ownerCache.SetWithTTL(key, old, 5*time.Minute)
	ownerDisk.Flush()
	if body, onDisk := ownerDisk.Get(key); !onDisk || !strings.Contains(string(body), "old-list") {
		t.Fatal("owner did not persist the non-empty answer")
	}
	// 2. The requester merges an empty list; write-through pushes it to the owner.
	p.setMultiTenantMergeCache("label_values", key, lokiLabelsResponse([]string{}))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if body, _, ok := ownerCache.GetWithTTL(key); ok && metadataListPayloadEmpty(body) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("empty merged answer did not reach the owner peer")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ownerDisk.Flush()
	if body, onDisk := ownerDisk.Get(key); !onDisk || !metadataListPayloadEmpty(body) {
		t.Fatalf("owner disk copy not overwritten by the empty answer: ok=%v body=%s", onDisk, body)
	}

	// 3. The empty entries expire everywhere.
	restore := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + time.Second)
	defer restore()

	// The owner itself never serves the old list again (memory, then disk).
	if body, ok := ownerCache.Get(key); ok && strings.Contains(string(body), "old-list") {
		t.Fatalf("owner served the old list after the empty answer expired: %s", body)
	}
	// 4. The requester's next read misses every tier and reaches the backend.
	before := calls.Load()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "old-list") {
		t.Fatalf("old merged list served after the empty answer expired: %d %s", rec.Code, rec.Body.String())
	}
	if calls.Load() == before {
		t.Fatal("read after the negative TTL did not reach the backend")
	}
}

// The TTL of empty label lists never falls below the disk or peer write-through
// minimum TTL: otherwise those tiers skip the empty write and keep an older
// non-empty copy that is served again once the empty entry expires.
func TestMetadataNegativeTTL_RisesWithDiskAndWriteThroughMinimums(t *testing.T) {
	newProxy := func(t *testing.T, diskMin, writeThroughMin time.Duration) *Proxy {
		t.Helper()
		c := cache.New(60*time.Second, 1000)
		t.Cleanup(c.Close)
		if diskMin > 0 {
			dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db", MinTTL: diskMin})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = dc.Close() })
			c.SetL2(dc)
		}
		cfg := Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"}
		if writeThroughMin > 0 {
			pc := cache.NewPeerCache(cache.PeerConfig{
				SelfAddr:           "self:3100",
				DiscoveryType:      "static",
				StaticPeers:        "self:3100,other:3100",
				WriteThrough:       true,
				WriteThroughMinTTL: writeThroughMin,
			})
			t.Cleanup(pc.Close)
			cfg.PeerCache = pc
		}
		p, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	for _, tc := range []struct {
		name                     string
		diskMin, writeThroughMin time.Duration
		want                     time.Duration
	}{
		{name: "defaults", diskMin: 30 * time.Second, writeThroughMin: 30 * time.Second, want: 30 * time.Second},
		{name: "no disk and no peers", want: 30 * time.Second},
		{name: "lower minimums keep the floor", diskMin: time.Second, writeThroughMin: 5 * time.Second, want: 30 * time.Second},
		{name: "raised disk minimum", diskMin: 2 * time.Minute, writeThroughMin: 30 * time.Second, want: 2 * time.Minute},
		{name: "raised write-through minimum", diskMin: 30 * time.Second, writeThroughMin: 90 * time.Second, want: 90 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newProxy(t, tc.diskMin, tc.writeThroughMin)
			if got := p.metadataNegativeTTL(); got != tc.want {
				t.Fatalf("negative TTL = %v, want %v", got, tc.want)
			}
			// The handler's empty-list write uses it.
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?start=1703980800000000000&end=1704067200000000000", nil)
			key := p.canonicalReadCacheKey("labels", "", req)
			p.setMetadataListCache("labels", key, lokiLabelsResponse([]string{}), 0, time.Hour)
			if _, remaining, _, ok := p.endpointReadCacheEntry("labels", key); !ok || remaining <= tc.want-time.Second || remaining > tc.want {
				t.Fatalf("empty labels entry remaining = %v (ok=%v), want about %v", remaining, ok, tc.want)
			}
		})
	}
	// A write-through minimum with write-through disabled does not apply.
	pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "self:3100", WriteThroughMinTTL: 5 * time.Minute})
	defer pc.Close()
	if got := effectiveMetadataNegativeTTL(0, pc.WriteThroughMinTTL()); got != metadataNegativeCacheTTL {
		t.Fatalf("disabled write-through raised the negative TTL to %v", got)
	}
}
