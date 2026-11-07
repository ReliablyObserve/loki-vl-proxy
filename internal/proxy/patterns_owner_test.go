package proxy

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// statePeers is proxies that are each other's peers, each serving its routes, with a pattern state on the same
// fake VictoriaLogs.
type statePeers struct {
	proxies []*Proxy
	addrs   []string
	servers []*http.Server
}

func newStatePeers(t *testing.T, vl *fakeStateVL, now *time.Time, n int) *statePeers {
	t.Helper()
	backend := vl.server(t)
	listeners := make([]net.Listener, n)
	sp := &statePeers{}
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		sp.addrs = append(sp.addrs, l.Addr().String())
	}
	for i, l := range listeners {
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: sp.addrs[i], DiscoveryType: "static", StaticPeers: strings.Join(sp.addrs, ","), AuthToken: "peer-secret", ReplicationFactor: 2})
		t.Cleanup(pc.Close)
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", PeerCache: pc, PeerAuthToken: "peer-secret", PatternsCounts: true})
		if err != nil {
			t.Fatal(err)
		}
		p.patternsExactMaxLines = 100_000
		p.patternState = newPatternStateStore(1<<30, DefaultPatternsStateStreamIdle)
		p.patternState.now = func() time.Time { return *now }
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		srv := &http.Server{Handler: mux}
		go func() { _ = srv.Serve(l) }()
		t.Cleanup(func() { _ = srv.Close() })
		sp.proxies = append(sp.proxies, p)
		sp.servers = append(sp.servers, srv)
	}
	return sp
}

// patternsVia asks one peer for a selector's patterns over HTTP, as a client does.
func (sp *statePeers) patternsVia(t *testing.T, i int, org, query string, start, end time.Time) string {
	t.Helper()
	q := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	req, _ := http.NewRequest(http.MethodGet, "http://"+sp.addrs[i]+"/loki/api/v1/patterns?"+q.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", org)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body patternsResponse
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&body) != nil {
		t.Fatalf("peer %d: status %d", i, resp.StatusCode)
	}
	encoded, _ := json.Marshal(body.Data)
	return string(encoded)
}

// Every peer answers a selector from the one peer that holds its pattern state, the key's owner: the same
// patterns and samples on every peer, the selector's lines read from VictoriaLogs once.
//
// conformance: peer-cache-patterns, patterns-loki-mining, loki_api_v1_patterns
func TestPatternStateIsHeldByOnePeer(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-40*time.Minute), 200)...)
	now := stateNow
	sp := newStatePeers(t, vl, &now, 3)
	var answers []string
	for i := range sp.proxies {
		answers = append(answers, sp.patternsVia(t, i, "0", `{app="api"}`, now.Add(-time.Hour), now))
	}
	for i, a := range answers[1:] {
		if a != answers[0] || !strings.Contains(a, "shipped to berlin") {
			t.Fatalf("peer %d answers\n %s\npeer 0\n %s", i+1, a, answers[0])
		}
	}
	if vl.counts != 1 || len(vl.lineRanges) != 1 {
		t.Fatalf("the selector's lines were read %d times (%d counts): want once, by its owner", len(vl.lineRanges), vl.counts)
	}
}

// A forward mark without the peers' token is a client's header: it does not make a peer answer as the owner.
//
// conformance: peer-cache-patterns, loki_api_v1_patterns
func TestPatternsForwardMarkNeedsThePeerToken(t *testing.T) {
	p := &Proxy{peerAuthToken: "peer-secret"}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns", nil)
	r.Header.Set(patternsForwardedHeader, "1")
	if p.patternsForwardedToUs(r) {
		t.Fatal("a forward mark without the token was honoured")
	}
	r.Header.Set("X-Peer-Token", "wrong")
	if p.patternsForwardedToUs(r) {
		t.Fatal("a forward mark with a wrong token was honoured")
	}
	r.Header.Set("X-Peer-Token", "peer-secret")
	if !p.patternsForwardedToUs(r) {
		t.Fatal("a forward with the peers' token was not honoured")
	}
}

// With the selector's owner and replica down, a peer answers from its own state rather than failing.
//
// conformance: peer-cache-patterns, loki_api_v1_patterns
func TestPatternsAnsweredLocallyWhenTheOwnersAreDown(t *testing.T) {
	vl := &fakeStateVL{}
	vl.add(stateLines("api/a", "info", "order %d shipped to berlin", stateNow.Add(-40*time.Minute), 200)...)
	now := stateNow
	sp := newStatePeers(t, vl, &now, 3)
	scope, _ := sp.proxies[0].patternScope(`{app="api"}`, "1", "2", "")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Scope-OrgID", "0")
	replicas := sp.proxies[0].peerCache.Replicas(sp.proxies[0].patternStateKey(req, scope))
	survivor := -1
	for i, addr := range sp.addrs {
		if addr == replicas[0] || addr == replicas[1] {
			_ = sp.servers[i].Close()
		} else {
			survivor = i
		}
	}
	if got := sp.patternsVia(t, survivor, "0", `{app="api"}`, now.Add(-time.Hour), now); !strings.Contains(got, "shipped to berlin") {
		t.Fatalf("survivor answered %s", got)
	}
}

// Tenants never share a pattern state key, so no peer answers one tenant from another's state.
//
// conformance: peer-cache-patterns, loki_api_v1_patterns
func TestPatternStateKeysKeepTenantsApart(t *testing.T) {
	vl := &fakeStateVL{}
	now := stateNow
	sp := newStatePeers(t, vl, &now, 1)
	p := sp.proxies[0]
	scope, _ := p.patternScope(`{app="api"}`, "1", "2", "")
	a := httptest.NewRequest(http.MethodGet, "/", nil)
	a.Header.Set("X-Scope-OrgID", "team-a")
	b := httptest.NewRequest(http.MethodGet, "/", nil)
	b.Header.Set("X-Scope-OrgID", "team-b")
	if p.patternStateKey(a, scope) == p.patternStateKey(b, scope) {
		t.Fatal("two tenants share a pattern state key")
	}
}
