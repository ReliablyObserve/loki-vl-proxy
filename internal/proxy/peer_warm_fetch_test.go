package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// fakePeer is one peer of the fleet: it holds keys with a remaining TTL and answers the two warm-up endpoints.
type fakePeer struct {
	name   string
	keys   map[string]int64 // key -> remaining TTL in ms
	values map[string]string
	token  string

	mu    sync.Mutex
	gets  []string
	hases int

	hasStatus   int // non-zero: the status of /_cache/has
	hasBody     string
	gzipHas     bool
	getStatus   int // non-zero: the status of /_cache/get
	emptyValues bool
}

func (f *fakePeer) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.token != "" && r.Header.Get("X-Peer-Token") != f.token {
			t.Errorf("peer %s: token %q, want %q", f.name, r.Header.Get("X-Peer-Token"), f.token)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/_cache/has":
			f.mu.Lock()
			f.hases++
			f.mu.Unlock()
			if f.hasStatus != 0 {
				w.WriteHeader(f.hasStatus)
				return
			}
			if f.hasBody != "" {
				_, _ = w.Write([]byte(f.hasBody))
				return
			}
			presence := map[string]cache.PeerKeyPresence{}
			for _, key := range strings.Split(r.URL.Query().Get("keys"), ",") {
				ttl, ok := f.keys[key]
				presence[key] = cache.PeerKeyPresence{OK: ok, TTLMs: ttl}
			}
			body, _ := json.Marshal(presence)
			if f.gzipHas {
				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				_, _ = zw.Write(body)
				_ = zw.Close()
				w.Header().Set("Content-Encoding", "gzip")
				body = buf.Bytes()
			}
			_, _ = w.Write(body)
		case "/_cache/get":
			key := r.URL.Query().Get("key")
			f.mu.Lock()
			f.gets = append(f.gets, key)
			f.mu.Unlock()
			if f.getStatus != 0 {
				w.WriteHeader(f.getStatus)
				return
			}
			if f.emptyValues {
				return
			}
			_, _ = w.Write([]byte(f.values[key]))
		default:
			t.Errorf("peer %s: unexpected path %s", f.name, r.URL.Path)
			http.NotFound(w, r)
		}
	})
}

// fleetProxy is a proxy whose peer cache names the given peers.
func fleetProxy(t *testing.T, token string, peers ...*fakePeer) *Proxy {
	t.Helper()
	var hosts []string
	for _, f := range peers {
		f.token = token
		srv := httptest.NewServer(f.handler(t))
		t.Cleanup(srv.Close)
		u, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, u.Host)
	}
	p := newTestProxy(t, "http://unused")
	p.peerCache = cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "127.0.0.1:1",
		DiscoveryType: "static",
		StaticPeers:   strings.Join(hosts, ","),
		Timeout:       2 * time.Second,
	})
	t.Cleanup(p.peerCache.Close)
	p.peerAuthToken = token
	return p
}

// A restarting replica asks every peer which keys it has in one request each, then fetches each key from the peer
// holding the freshest copy; keys no peer has are left to the backend.
// conformance: peer-cache-fleet-recovery
func TestFetchCacheKeysFromPeersPicksTheFreshestCopy(t *testing.T) {
	a := &fakePeer{name: "a", keys: map[string]int64{"k1": 10_000, "k2": 5_000}, values: map[string]string{"k1": "k1-from-a", "k2": "k2-from-a"}}
	b := &fakePeer{name: "b", keys: map[string]int64{"k1": 30_000, "k3": 1_000}, values: map[string]string{"k1": "k1-from-b", "k3": "k3-from-b"}}
	p := fleetProxy(t, "fleet-secret", a, b)

	warmed := p.fetchCacheKeysFromPeers(context.Background(), "labels", []string{"k1", "k2", "k3", "k4"}, time.Minute)
	if want := map[string]bool{"k1": true, "k2": true, "k3": true}; !reflect.DeepEqual(warmed, want) {
		t.Fatalf("warmed = %v, want %v", warmed, want)
	}
	for key, want := range map[string]string{"k1": "k1-from-b", "k2": "k2-from-a", "k3": "k3-from-b"} {
		got, ttl, ok := p.cache.GetWithTTL(key)
		if !ok || string(got) != want {
			t.Errorf("%s: cached %q (found %v), want %q", key, got, ok, want)
		}
		if ttl <= 0 || ttl > time.Minute {
			t.Errorf("%s: ttl %v, want at most the warm-up ttl", key, ttl)
		}
	}
	if _, _, ok := p.cache.GetWithTTL("k4"); ok {
		t.Error("a key no peer holds must not be cached")
	}
	if a.hases != 1 || b.hases != 1 {
		t.Errorf("one presence request per peer: a=%d b=%d", a.hases, b.hases)
	}
	if !reflect.DeepEqual(a.gets, []string{"k2"}) || !reflect.DeepEqual(b.gets, []string{"k1", "k3"}) {
		t.Errorf("values come from the freshest peer only: a=%v b=%v", a.gets, b.gets)
	}
}

// A peer that fails, answers garbage or holds an empty value costs nothing but its own keys.
// conformance: peer-cache-fleet-recovery
func TestFetchCacheKeysFromPeersSurvivesBadPeers(t *testing.T) {
	good := &fakePeer{name: "good", keys: map[string]int64{"k1": 1_000, "k2": 1_000}, values: map[string]string{"k1": "v1", "k2": "v2"}}
	down := &fakePeer{name: "down", hasStatus: http.StatusInternalServerError}
	garbage := &fakePeer{name: "garbage", hasBody: "{not json"}
	notFound := &fakePeer{name: "gone", keys: map[string]int64{"k1": 99_000}, getStatus: http.StatusNotFound}
	p := fleetProxy(t, "", down, garbage, notFound, good)
	warmed := p.fetchCacheKeysFromPeers(context.Background(), "query_range", []string{"k1", "k2"}, time.Minute)
	// k1: the freshest copy is on the peer that then answers 404, so it is not warmed; k2 comes from the good peer.
	if want := map[string]bool{"k2": true}; !reflect.DeepEqual(warmed, want) {
		t.Errorf("warmed = %v, want %v", warmed, want)
	}
	if got, _, ok := p.cache.GetWithTTL("k2"); !ok || string(got) != "v2" {
		t.Errorf("k2 = %q, %v", got, ok)
	}
	if _, _, ok := p.cache.GetWithTTL("k1"); ok {
		t.Error("k1 must not be cached from a failed fetch")
	}

	empty := &fakePeer{name: "empty", keys: map[string]int64{"k1": 5_000}, emptyValues: true}
	p = fleetProxy(t, "", empty)
	if warmed := p.fetchCacheKeysFromPeers(context.Background(), "labels", []string{"k1"}, time.Minute); len(warmed) != 0 {
		t.Errorf("an empty value warms nothing: %v", warmed)
	}
}

// With no peer cache, no peer or no key there is nothing to ask.
// conformance: peer-cache-fleet-recovery
func TestFetchCacheKeysFromPeersWithoutPeers(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	if got := p.fetchCacheKeysFromPeers(context.Background(), "labels", []string{"k"}, time.Minute); got != nil {
		t.Errorf("no peer cache: %v", got)
	}
	peer := &fakePeer{name: "a", keys: map[string]int64{"k": 1}}
	p = fleetProxy(t, "", peer)
	if got := p.fetchCacheKeysFromPeers(context.Background(), "labels", nil, time.Minute); got != nil {
		t.Errorf("no keys: %v", got)
	}
	if peer.hases != 0 {
		t.Errorf("no keys: %d requests", peer.hases)
	}
	empty := newTestProxy(t, "http://unused")
	empty.peerCache = cache.NewPeerCache(cache.PeerConfig{SelfAddr: "127.0.0.1:1", DiscoveryType: "static", StaticPeers: ""})
	t.Cleanup(empty.peerCache.Close)
	if got := empty.fetchCacheKeysFromPeers(context.Background(), "labels", []string{"k"}, time.Minute); got != nil {
		t.Errorf("an empty peer list: %v", got)
	}
}

// The presence query reads a gzip answer, sends the peer token, and treats any failure as "the peer has nothing".
// conformance: peer-cache-fleet-recovery
func TestQueryPeerHas(t *testing.T) {
	peer := &fakePeer{name: "a", keys: map[string]int64{"k1": 7_000}, gzipHas: true}
	p := fleetProxy(t, "tok", peer)
	addr := p.peerCache.Peers()[0]
	got := p.queryPeerHas(context.Background(), addr, []string{"k1", "k2"})
	want := map[string]cache.PeerKeyPresence{"k1": {OK: true, TTLMs: 7_000}, "k2": {OK: false}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("presence = %v, want %v", got, want)
	}
	for name, c := range map[string]*fakePeer{
		"status":   {hasStatus: http.StatusServiceUnavailable},
		"garbage":  {hasBody: "[1,2"},
		"toolarge": {hasBody: `{"k":{"ok":true,"ttl_ms":1}}` + strings.Repeat(" ", 70<<10)},
	} {
		c.name = name
		q := fleetProxy(t, "", c)
		if got := q.queryPeerHas(context.Background(), q.peerCache.Peers()[0], []string{"k"}); got != nil {
			t.Errorf("%s: %v, want nil", name, got)
		}
	}
	// A peer that cannot be reached, and a canceled request, are nobody's presence either.
	if got := p.queryPeerHas(context.Background(), "127.0.0.1:1", []string{"k"}); got != nil {
		t.Errorf("unreachable peer: %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := p.queryPeerHas(ctx, addr, []string{"k1"}); got != nil {
		t.Errorf("canceled: %v", got)
	}
	if got := p.queryPeerHas(context.Background(), "bad host", []string{"k"}); got != nil {
		t.Errorf("an address that is no host: %v", got)
	}
}
