package proxy

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

const readsKey = "query_range:tenant:q"

// readsOf is the request's tier reads: memory, disk, peer.
func readsOf(ctx context.Context) [3]int { return snapshotTelemetry(ctx).cacheReads.counts() }

type readsStack struct {
	c       *cache.Cache
	disk    *cache.DiskCache
	owner   *cache.Cache
	asked   *atomic.Int64
	release chan struct{} // non-nil: the peer holds its answers until closed
}

// newReadsStack is a response cache with the given tiers behind memory; the peer tier is a second cache behind
// the real peer protocol, which owns every key.
func newReadsStack(t *testing.T, withDisk, withPeer bool, release chan struct{}) readsStack {
	t.Helper()
	s := readsStack{c: cache.New(time.Minute, 100), asked: new(atomic.Int64), release: release}
	t.Cleanup(s.c.Close)
	if withDisk {
		dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/l2.db", FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dc.Close() })
		s.c.SetL2(dc)
		s.disk = dc
	}
	if withPeer {
		s.owner = cache.New(time.Minute, 100)
		t.Cleanup(s.owner.Close)
		server := cache.NewPeerCache(cache.PeerConfig{})
		t.Cleanup(server.Close)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.asked.Add(1)
			if s.release != nil {
				<-s.release
			}
			server.ServeHTTP(w, r, s.owner)
		}))
		t.Cleanup(srv.Close)
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "requester:3100", DiscoveryType: "static", StaticPeers: strings.TrimPrefix(srv.URL, "http://"), Timeout: 5 * time.Second})
		t.Cleanup(pc.Close)
		s.c.SetL3(pc)
	}
	return s
}

type readsLookup struct {
	name string
	read func(p *Proxy, ctx context.Context, c *cache.Cache, key string)
}

var readsLookups = []readsLookup{
	{"Get", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cacheGet(ctx, c, k) }},
	{"Shared", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cacheGetShared(ctx, c, k) }},
	{"SharedWithAge", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cacheGetSharedWithAge(ctx, c, k) }},
	{"Peek", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cachePeek(ctx, c, k) }},
	{"WithTTL", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cacheGetWithTTL(ctx, c, k) }},
	{"WithAge", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cacheGetWithAge(ctx, c, k) }},
	{"Stale", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) { p.cacheGetStale(ctx, c, k) }},
	{"RecoverableStale", func(p *Proxy, ctx context.Context, c *cache.Cache, k string) {
		p.cacheGetRecoverableStale(ctx, c, k, nil)
	}},
}

// One read is one lookup reaching one attached tier, hit or miss: a full-stack lookup reads memory, then disk,
// then the peers until one answers; a memory-only method reads memory; a stale-recovery read reads memory, then
// disk. The tier counts of the full-stack methods are those of cache_tier_requests_total.
//
// conformance: observability-query-flow
func TestCacheReadsPerTier(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	for _, tiers := range []struct {
		name       string
		disk, peer bool
	}{{"memory", false, false}, {"memory+disk", true, false}, {"memory+peer", false, true}, {"all", true, true}} {
		for _, where := range []string{"memory", "disk", "peer", "nowhere"} {
			if where == "disk" && !tiers.disk || where == "peer" && !tiers.peer {
				continue
			}
			for _, l := range readsLookups {
				t.Run(fmt.Sprintf("%s/%s/%s", tiers.name, where, l.name), func(t *testing.T) {
					s := newReadsStack(t, tiers.disk, tiers.peer, nil)
					switch where {
					case "memory":
						s.c.SetLocalOnlyWithTTL(readsKey, []byte("v"), time.Minute)
					case "disk":
						s.disk.Set(readsKey, []byte("v"), time.Minute)
					case "peer":
						s.owner.SetLocalOnlyWithTTL(readsKey, []byte("v"), time.Minute)
					}
					full := l.name == "Get" || strings.HasPrefix(l.name, "Shared")
					want := [3]int{1, 0, 0}
					if where != "memory" {
						if tiers.disk && (full || l.name == "RecoverableStale") {
							want[1] = 1
						}
						if tiers.peer && full && where != "disk" {
							want[2] = 1
						}
					}
					before := s.c.Stats()
					ctx, _ := flowCtx()
					l.read(p, ctx, s.c, readsKey)
					if got := readsOf(ctx); got != want {
						t.Fatalf("reads (memory, disk, peer) = %v, want %v", got, want)
					}
					if full {
						after := s.c.Stats()
						counted := [3]int{int(after.L1.Requests - before.L1.Requests), int(after.L2.Requests - before.L2.Requests), int(after.L3.Requests - before.L3.Requests)}
						if counted != want {
							t.Fatalf("cache_tier_requests moved by %v, reads %v", counted, want)
						}
						if (where == "peer" || where == "nowhere" && tiers.peer) != (s.asked.Load() > 0) {
							t.Fatalf("peer asked %d times", s.asked.Load())
						}
					}
				})
			}
		}
	}
}

// A disk hit is promoted: the next lookup of the key reads memory only. A disabled or nil cache is not read. A
// stale-recovery read whose memory value is refused reads the disk too.
//
// conformance: observability-query-flow
func TestCacheReadsFollowPromotionAndDisabledCaches(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	s := newReadsStack(t, true, false, nil)
	s.disk.Set(readsKey, []byte("v"), time.Minute)
	ctx, _ := flowCtx()
	p.cacheGet(ctx, s.c, readsKey)
	p.cacheGet(ctx, s.c, readsKey)
	if got := readsOf(ctx); got != [3]int{2, 1, 0} {
		t.Fatalf("disk hit then memory hit: %v", got)
	}
	ctx, _ = flowCtx()
	p.cacheGetRecoverableStale(ctx, s.c, readsKey, func([]byte) bool { return false })
	if got := readsOf(ctx); got != [3]int{1, 1, 0} {
		t.Fatalf("refused memory value: %v", got)
	}
	ctx, _ = flowCtx()
	var nilCache *cache.Cache
	p.cacheGet(ctx, nilCache, readsKey)
	p.cacheGet(ctx, cache.NewDisabled(), readsKey)
	if reads, lookups := readsOf(ctx), flowOf(ctx); reads != [3]int{} || lookups.cacheLookups() != 2 {
		t.Fatalf("lookups of no cache: %v reads, %d lookups", reads, lookups.cacheLookups())
	}
}

// Requests that wait on one peer fetch (singleflight) each count one peer read: the read is the lookup reaching
// the tier, not the network call.
//
// conformance: observability-query-flow
func TestCacheReadsOfCoalescedPeerLookups(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	release := make(chan struct{})
	s := newReadsStack(t, false, true, release)
	s.owner.SetLocalOnlyWithTTL(readsKey, []byte("v"), time.Minute)
	const callers = 4
	ctxs := make([]context.Context, callers)
	var wg sync.WaitGroup
	for i := range ctxs {
		ctxs[i], _ = flowCtx()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.cacheGet(ctxs[i], s.c, readsKey)
		}()
	}
	// Every caller has counted its memory miss and started its peer lookup before the peer answers.
	deadline := time.Now().Add(5 * time.Second)
	for s.c.Stats().L3.Requests < callers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	for i, ctx := range ctxs {
		// A caller that arrived after the first one's promotion would read memory only; none can here, because
		// all reached the peer tier before the answer.
		if got := readsOf(ctx); got != [3]int{1, 0, 1} {
			t.Fatalf("caller %d: %v", i, got)
		}
	}
}

// The histogram takes each client request once, per route and tier, zero included; parallel reads of one request
// add up. Reads outside a client request (a background build, a peer's request, work that outlives its request)
// change nothing.
//
// conformance: observability-query-flow
func TestCacheReadsPerRequestHistogram(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	s := newReadsStack(t, true, false, nil)
	s.c.SetLocalOnlyWithTTL(readsKey, []byte("v"), time.Minute)
	var late context.Context
	serve := func(h http.HandlerFunc) {
		p.requestLogger("labels", "/loki/api/v1/labels", h).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	}
	serve(func(w http.ResponseWriter, r *http.Request) {
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.cacheGet(r.Context(), s.c, readsKey)
				p.cacheGet(r.Context(), s.c, fmt.Sprintf("query_range:tenant:missing%d", i))
			}()
		}
		wg.Wait()
		late = context.WithoutCancel(r.Context())
	})
	serve(func(http.ResponseWriter, *http.Request) {})
	// A route none of whose requests read a cache has no series.
	p.requestLogger("ready", "/ready", func(http.ResponseWriter, *http.Request) {}).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ready", nil))

	want := []string{
		`loki_vl_proxy_cache_reads_per_request_count{route="/loki/api/v1/labels",tier="l1_memory"} 2`,
		`loki_vl_proxy_cache_reads_per_request_sum{route="/loki/api/v1/labels",tier="l1_memory"} 16`,
		`loki_vl_proxy_cache_reads_per_request_sum{route="/loki/api/v1/labels",tier="l2_disk"} 8`,
		`loki_vl_proxy_cache_reads_per_request_sum{route="/loki/api/v1/labels",tier="l3_peer"} 0`,
		`loki_vl_proxy_cache_reads_per_request_bucket{route="/loki/api/v1/labels",tier="l2_disk",le="0"} 1`,
		`loki_vl_proxy_cache_reads_per_request_bucket{route="/loki/api/v1/labels",tier="l2_disk",le="8"} 2`,
	}
	check := func(when string) {
		t.Helper()
		body := metricsBody(p)
		for _, line := range want {
			if !strings.Contains(body, line+"\n") {
				t.Fatalf("%s: metrics lack %s\n%s", when, line, body)
			}
		}
	}
	check("after two requests")
	if body := metricsBody(p); strings.Contains(body, `loki_vl_proxy_cache_reads_per_request_count{route="/ready"`) || !strings.Contains(body, `loki_vl_proxy_request_fanout_count{route="/ready",kind="cache_lookup"} 1`) {
		t.Fatalf("a route that reads no cache:\n%s", body)
	}
	p.cacheGet(context.Background(), s.c, "query_range:tenant:background")
	p.cacheGet(late, s.c, "query_range:tenant:late")
	peer := cache.NewPeerCache(cache.PeerConfig{})
	defer peer.Close()
	peer.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/_cache/get?key="+readsKey, nil), s.c)
	check("after reads outside a request")
}

// A count stops at its maximum instead of wrapping around.
//
// conformance: observability-query-flow
func TestCacheReadCountsSaturate(t *testing.T) {
	r := flowReads{math.MaxUint16 - 1, 0, 0}
	r.add(flowTierL1)
	r.add(flowTierL1)
	if got := r.counts(); got != [3]int{math.MaxUint16, 0, 0} {
		t.Fatalf("counts %v", got)
	}
}
