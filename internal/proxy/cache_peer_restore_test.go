package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Peer bulk restore through the proxy's lifecycle: a replica that starts with
// an empty disk and the warm-up on pulls its entries from its peers in Init,
// /ready answers 503 until it ends, the route asks for the peer token, and
// with the flag off or the warm-up off nothing is pulled.

type peerRestoreNode struct {
	srv     *httptest.Server
	handler atomic.Pointer[http.Handler]
	c       *cache.Cache
	dc      *cache.DiskCache
	pc      *cache.PeerCache
	p       *Proxy
}

func ptrHandler(h http.Handler) *http.Handler { return &h }

func (n *peerRestoreNode) addr() string { return n.srv.Listener.Addr().String() }

func newPeerRestorePair(t *testing.T) (serving, restoring *peerRestoreNode) {
	t.Helper()
	serving, restoring = &peerRestoreNode{}, &peerRestoreNode{}
	for _, n := range []*peerRestoreNode{serving, restoring} {
		n.handler.Store(ptrHandler(http.NotFoundHandler()))
		n.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			(*n.handler.Load()).ServeHTTP(w, r)
		}))
		n.srv.Start()
		t.Cleanup(n.srv.Close)
	}
	return serving, restoring
}

func (n *peerRestoreNode) open(t *testing.T, peers []string, token string, cfg Config) {
	t.Helper()
	dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"), FlushInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	n.dc = dc
	n.c = cache.NewWithMaxBytes(time.Minute, 1000, 8<<20)
	n.c.SetL2(dc)
	n.pc = cache.NewPeerCache(cache.PeerConfig{
		SelfAddr: n.addr(), DiscoveryType: "static", StaticPeers: strings.Join(peers, ","),
		AuthToken: token, WriteThrough: true, ReplicationFactor: 2, Timeout: time.Second,
	})
	n.c.SetL3(n.pc)
	cfg.BackendURL = healthyBackend(t).URL
	cfg.Cache = n.c
	cfg.LogLevel = "error"
	cfg.DisableLabelsCacheWarm = true
	cfg.PeerCache = n.pc
	cfg.PeerAuthToken = token
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.p = p
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	n.handler.Store(ptrHandler(mux))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
		n.c.Close()
		_ = dc.Close()
	})
}

func seedServing(t *testing.T, n *peerRestoreNode, count int) []string {
	t.Helper()
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("query_range:t:answer-%d", i)
		n.c.SetLocalAndDiskWithTTL(keys[i], []byte(strings.Repeat("v", 512)), time.Minute)
	}
	n.dc.Flush()
	return keys
}

// conformance: cache-restart-handover
func TestCachePeerRestore_InitRestoresAnEmptyDiskFromPeers(t *testing.T) {
	serving, restoring := newPeerRestorePair(t)
	peers := []string{serving.addr(), restoring.addr()}
	serving.open(t, peers, "peer-secret", Config{})
	keys := seedServing(t, serving, 40)
	// The serving side answers slowly, so /ready is seen waiting.
	inner := *serving.handler.Load()
	serving.handler.Store(ptrHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cache/bulk" {
			time.Sleep(200 * time.Millisecond)
		}
		inner.ServeHTTP(w, r)
	})))
	restoring.open(t, peers, "peer-secret", Config{CacheStartupWarmBytes: 1 << 20, CacheStartupWarmTimeout: 10 * time.Second, CacheStartupPeerRestore: true})
	if !restoring.c.PeerRestoreWanted() {
		t.Fatal("an empty disk with a peer should want a peer restore")
	}
	restoring.p.Init()
	if code, body := readyStatus(restoring.p); code != http.StatusServiceUnavailable || body != "response cache warming" {
		t.Fatalf("/ready during the peer restore: %d %q", code, body)
	}
	waitWarmDone(t, restoring.p)
	for _, key := range keys {
		if _, ok := restoring.c.Peek(key); !ok {
			t.Fatalf("%s not restored from the peer", key)
		}
	}
	p := restoring.c.HandoverStats().PeerRestore
	if p.Outcome != cache.RestoreDone || p.RestoredEntries != int64(len(keys)) {
		t.Fatalf("peer restore %+v", p)
	}
	if d := restoring.c.HandoverStats().Restore; d.Outcome != "" {
		t.Fatalf("the disk warm-up ran too: %+v", d)
	}
}

// conformance: cache-restart-handover
func TestCachePeerRestore_OffWithoutTheFlagOrTheWarmUp(t *testing.T) {
	for _, cfg := range []Config{
		{CacheStartupWarmBytes: 1 << 20, CacheStartupPeerRestore: false},
		{CacheStartupWarmBytes: 0, CacheStartupPeerRestore: true},
	} {
		serving, restoring := newPeerRestorePair(t)
		peers := []string{serving.addr(), restoring.addr()}
		serving.open(t, peers, "peer-secret", Config{})
		seedServing(t, serving, 5)
		restoring.open(t, peers, "peer-secret", cfg)
		restoring.p.Init()
		if code, _ := readyStatus(restoring.p); code != http.StatusOK {
			t.Fatalf("%+v: /ready %d", cfg, code)
		}
		if p := restoring.c.HandoverStats().PeerRestore; p.Outcome != "" {
			t.Fatalf("%+v: a peer restore ran: %+v", cfg, p)
		}
	}
}

// The route asks for the peer token like every /_cache route; a restoring
// replica with the wrong token gets nothing and is ready at once.
//
// conformance: cache-restart-handover
func TestCachePeerRestore_RouteRequiresThePeerToken(t *testing.T) {
	serving, restoring := newPeerRestorePair(t)
	peers := []string{serving.addr(), restoring.addr()}
	serving.open(t, peers, "peer-secret", Config{})
	seedServing(t, serving, 5)
	resp, err := http.Post(serving.srv.URL+"/_cache/bulk", "application/json", strings.NewReader(`{"version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	restoring.open(t, peers, "wrong-token", Config{CacheStartupWarmBytes: 1 << 20, CacheStartupWarmTimeout: 10 * time.Second, CacheStartupPeerRestore: true})
	start := time.Now()
	restoring.p.Init()
	waitWarmDone(t, restoring.p)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v", took)
	}
	if p := restoring.c.HandoverStats().PeerRestore; p.Outcome != cache.RestoreNone || p.RestoredEntries != 0 {
		t.Fatalf("peer restore with a wrong token: %+v", p)
	}
}

// StopPeerBulkStreams (the HTTP server's shutdown hook) ends served streams:
// the next request is refused.
//
// conformance: cache-restart-handover
func TestCachePeerRestore_StopPeerBulkStreamsRefusesNewStreams(t *testing.T) {
	serving, restoring := newPeerRestorePair(t)
	peers := []string{serving.addr(), restoring.addr()}
	serving.open(t, peers, "peer-secret", Config{})
	seedServing(t, serving, 5)
	serving.p.StopPeerBulkStreams()
	restoring.open(t, peers, "peer-secret", Config{CacheStartupWarmBytes: 1 << 20, CacheStartupWarmTimeout: 10 * time.Second, CacheStartupPeerRestore: true})
	restoring.p.Init()
	waitWarmDone(t, restoring.p)
	if p := restoring.c.HandoverStats().PeerRestore; p.RestoredEntries != 0 || p.Outcome != cache.RestoreNone {
		t.Fatalf("restored from a stopped server: %+v", p)
	}
}

// SetPeerBulkLoad hands the client request counters to the bulk governor.
//
// conformance: cache-restart-handover
func TestCachePeerRestore_SetPeerBulkLoad(t *testing.T) {
	serving, restoring := newPeerRestorePair(t)
	serving.open(t, []string{serving.addr(), restoring.addr()}, "peer-secret", Config{})
	serving.p.SetPeerBulkLoad(func() (int64, int64) { return 3, 3e6 })
	serving.p.SetPeerBulkLoad(nil)
	var nilProxy *Proxy
	nilProxy.SetPeerBulkLoad(func() (int64, int64) { return 0, 0 })
}
