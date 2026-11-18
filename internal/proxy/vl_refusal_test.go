package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// answerParseRefusal is VictoriaLogs' answer to a query it cannot parse (v1.53 wording).
func answerParseRefusal(_ int64, w http.ResponseWriter, r *http.Request) {
	http.Error(w, "cannot parse `query` arg ["+r.FormValue("query")+"]: unexpected token", http.StatusBadRequest)
}

// refusalWindow is flightLogTarget with its window moved by shift seconds (a refresh moving end=now).
func refusalWindow(shift int) string {
	return fmt.Sprintf("/loki/api/v1/query_range?query=%%7Bapp%%3D%%22api%%22%%7D&start=%d&end=%d&limit=100", 1700000000+shift, 1700003600+shift)
}

func refusalProxy(t *testing.T, backend string, release string, mutate func(*Config)) (*Proxy, http.Handler) {
	t.Helper()
	p, h := newFlightProxy(t, backend, func(cfg *Config) {
		cfg.ErrorCache.RefusalTTL = time.Minute
		if mutate != nil {
			mutate(cfg)
		}
	})
	if release != "" {
		p.storeBackendVersion("test", release)
	}
	return p, h
}

// A VictoriaLogs parse refusal is held for the refusal TTL, for every window of the query; the hold ends with the
// TTL, never answers another tenant, and a new VictoriaLogs release asks again.
//
// conformance: semantics/error-cache-vl-refusal-held
func TestVLRefusal_HeldLongForEveryWindow(t *testing.T) {
	stub := newFlightStub(t, answerParseRefusal)
	p, h := refusalProxy(t, stub.srv.URL, "v1.53.0", nil)
	first := flightGet(h, context.Background(), refusalWindow(0), "team-a")
	if first.Code != http.StatusInternalServerError || stub.calls.Load() != 1 {
		t.Fatalf("first answer %d after %d calls", first.Code, stub.calls.Load())
	}
	for _, shift := range []int{0, 15, 600} {
		got := flightGet(h, context.Background(), refusalWindow(shift), "team-a")
		if got.Code != first.Code || got.Body.String() != first.Body.String() || got.Header().Get(vlBackendCallsHeader) != "0" {
			t.Fatalf("window +%ds: %d %q calls=%s", shift, got.Code, got.Body.String(), got.Header().Get(vlBackendCallsHeader))
		}
	}
	if stub.calls.Load() != 1 {
		t.Fatalf("held refusal sent %d calls", stub.calls.Load())
	}
	if flightGet(h, context.Background(), refusalWindow(0), "team-b"); stub.tenantCalls("20") != 1 {
		t.Fatal("another tenant must ask VictoriaLogs itself")
	}
	restore := cache.AdvanceClockForTesting(61 * time.Second)
	flightGet(h, context.Background(), refusalWindow(30), "team-a")
	restore()
	if got := stub.tenantCalls("10"); got != 2 {
		t.Fatalf("after the TTL the refusal must be asked again: %d calls", got)
	}
	p.noteVLRelease("v1.54.0")
	flightGet(h, context.Background(), refusalWindow(0), "team-a")
	if got := stub.tenantCalls("10"); got != 3 {
		t.Fatalf("a new release must ask again: %d calls", got)
	}
	s := p.errorFlightStats()
	if s.Held[3] == 0 || s.Stored[3] == 0 {
		t.Fatalf("refusal counters %+v", s)
	}
}

// Before the release is known nothing is predicted (the call is sent and its answer classified), and a refusal
// stays local; a resource limit VictoriaLogs reaches while running is never a refusal.
//
// conformance: semantics/error-cache-vl-refusal-held
func TestVLRefusal_UnknownReleaseAndNonRefusals(t *testing.T) {
	stub := newFlightStub(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "the `query` arg length cannot exceed -search.maxQueryLen=10 bytes; the current query length is 99 bytes; query=x", http.StatusBadRequest)
	})
	p, h := refusalProxy(t, stub.srv.URL, "", nil)
	flightGet(h, context.Background(), refusalWindow(0), "team-a")
	flightGet(h, context.Background(), "/loki/api/v1/query_range?query=%7Bapp%3D%22web%22%7D&start=1700000000&end=1700003600", "team-a")
	if stub.calls.Load() != 2 || p.learnedMaxQueryLen(p.vlRelease()) != 0 {
		t.Fatalf("unknown release: %d calls, learned %d", stub.calls.Load(), p.learnedMaxQueryLen(p.vlRelease()))
	}
	for _, c := range []struct {
		status int
		raw    string
		want   bool
	}{
		{400, "cannot parse `query` arg [x]: unexpected token", true},
		{422, "missing `| stats ...` pipe in the query [x]", true},
		{422, "the pipe `| \"sort by (a)\"` cannot be put in front of `| \"stats count(*)\"`, since it may modify or delete `_time` field", true},
		{422, "the limit 1 pipe isn't allowed in range queries, since it cannot be applied individually for each step; step=1", true},
		{400, "the `extra_filters` arg length cannot exceed -search.maxQueryLen=16384 bytes; the current length is 16406 bytes", true},
		{422, "cannot execute query [x]: cannot calculate [stats], since it requires more than 100MB of memory", false},
		{400, "the query matches too many logs (1000000+); add more specific filters to the query", false},
		{500, "cannot parse `query` arg: x", false},
		{400, "backend says no", false},
	} {
		if got := isVLRefusalMessage(c.status, c.raw); got != c.want {
			t.Errorf("%d %q: %v, want %v", c.status, c.raw, got, c.want)
		}
	}
}

// A query over VictoriaLogs' length limit is sent once; the limit its refusal names is learned for the release,
// and the next query over it is refused without a call, with the answer VictoriaLogs' refusal gave.
//
// conformance: vl-query-too-long
func TestVLRefusal_LearnedLengthPredicted(t *testing.T) {
	stub := newFlightStub(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		q := r.FormValue("query")
		if len(q) > 20 {
			http.Error(w, fmt.Sprintf("the `query` arg length cannot exceed -search.maxQueryLen=20 bytes; the current query length is %d bytes; query=%s", len(q), q), http.StatusBadRequest)
			return
		}
		answerLogs(0, w, r)
	})
	p, h := refusalProxy(t, stub.srv.URL, "v1.53.0", nil)
	first := flightGet(h, context.Background(), refusalWindow(0), "team-a")
	if stub.calls.Load() != 1 || p.learnedMaxQueryLen(p.vlRelease()) != 20 {
		t.Fatalf("%d calls, learned %d", stub.calls.Load(), p.learnedMaxQueryLen(p.vlRelease()))
	}
	other := flightGet(h, context.Background(), "/loki/api/v1/query_range?query=%7Bapp%3D%22web%22%7D&start=1700000000&end=1700003600&limit=100", "team-a")
	if stub.calls.Load() != 1 || other.Code != first.Code || other.Body.String() != first.Body.String() {
		t.Fatalf("predicted: %d calls, %d %q vs %d %q", stub.calls.Load(), other.Code, other.Body.String(), first.Code, first.Body.String())
	}
	if s := p.errorFlightStats(); s.Predicted[0] != 1 || s.Rules[0] != logsql.RuleQueryTooLong {
		t.Fatalf("predicted counters %v %v", s.Rules, s.Predicted)
	}
	// Prediction off: every such query is sent.
	stub2 := newFlightStub(t, stub.answer)
	p2, h2 := refusalProxy(t, stub2.srv.URL, "v1.53.0", func(cfg *Config) { cfg.VLRefusalPredictionDisabled = true })
	flightGet(h2, context.Background(), refusalWindow(0), "team-a")
	flightGet(h2, context.Background(), "/loki/api/v1/query_range?query=%7Bapp%3D%22web%22%7D&start=1700000000&end=1700003600&limit=100", "team-a")
	if stub2.calls.Load() != 2 || p2.learnedMaxQueryLen(p2.vlRelease()) != 20 {
		t.Fatalf("prediction off: %d calls", stub2.calls.Load())
	}
	// Predictions name the limit too, but never extend how long it is trusted.
	restore := cache.AdvanceClockForTesting(50 * time.Second)
	flightGet(h, context.Background(), "/loki/api/v1/query_range?query=%7Bapp%3D%22db%22%7D&start=1700000000&end=1700003600&limit=100", "team-a")
	restore()
	if stub.calls.Load() != 1 {
		t.Fatalf("a predicted refusal sent a call: %d", stub.calls.Load())
	}
	restore = cache.AdvanceClockForTesting(2 * time.Minute)
	defer restore()
	if p.learnedMaxQueryLen(p.vlRelease()) != 0 {
		t.Fatal("a learned limit must expire with the refusal TTL, however many predictions name it")
	}
}

// A refusal of an argument other than the query text, or of the length, is held for the exact request only: a
// request with another window asks VictoriaLogs.
//
// conformance: semantics/error-cache-vl-refusal-held
func TestVLRefusal_WindowDependentRefusalsHeldExactly(t *testing.T) {
	stub := newFlightStub(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cannot parse start=xyz: cannot parse timestamp", http.StatusBadRequest)
	})
	_, h := refusalProxy(t, stub.srv.URL, "v1.53.0", nil)
	flightGet(h, context.Background(), refusalWindow(0), "team-a")
	flightGet(h, context.Background(), refusalWindow(0), "team-a")
	if stub.calls.Load() != 1 {
		t.Fatalf("exact repeat: %d calls", stub.calls.Load())
	}
	flightGet(h, context.Background(), refusalWindow(15), "team-a")
	if stub.calls.Load() != 2 {
		t.Fatalf("another window must ask again: %d calls", stub.calls.Load())
	}
	for raw, want := range map[string]bool{
		"cannot parse `query` arg [x]: unexpected token":                                   true,
		"the pipe `| \"sort by (a)\"` cannot be put in front of `| \"stats count(*)\"`, x": true,
		"cannot parse start=xyz": false,
		"the `query` arg length cannot exceed -search.maxQueryLen=10 bytes; the current query": false,
		"the `extra_filters` arg length cannot exceed -search.maxQueryLen=10 bytes":            false,
		"cannot parse duration from the arg step":                                              false,
	} {
		if got := isWindowFreeRefusalMessage(http.StatusBadRequest, raw); got != want {
			t.Errorf("%q: window-free %v, want %v", raw, got, want)
		}
	}
}

// A call VictoriaLogs' handler would panic on is not sent on a release without the fix: the client gets the
// answer a dropped connection gives, and VictoriaLogs gets no call.
//
// conformance: vl-math-quoted-constant-clone-crash
func TestVLRefusal_PredictedCrashAnswersAsADroppedConnection(t *testing.T) {
	var calls atomic.Int64
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
	}))
	defer drop.Close()
	params := url.Values{"query": {`* | math "2025-01-01T00:00:00Z" as x`}, "limit": {"5"}}
	answer := func(predict bool) (string, int64) {
		before := calls.Load()
		p, _ := refusalProxy(t, drop.URL, "v1.49.0", func(cfg *Config) {
			cfg.VLRefusalPredictionDisabled = !predict
			cfg.CBFailThreshold = 1000
		})
		_, err := p.vlGet(context.Background(), logsql.PathQuery, params)
		rec := httptest.NewRecorder()
		p.fail(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil), err)
		class, _ := failureClassOf(context.Background(), err, nil)
		return fmt.Sprintf("%d %s %q", rec.Code, class, rec.Body.String()), calls.Load() - before
	}
	off, offCalls := answer(false)
	on, onCalls := answer(true)
	if offCalls == 0 || onCalls != 0 || !strings.HasPrefix(on, off[:4]) || !strings.Contains(on, "vl_refusal") {
		t.Fatalf("off %d calls %s, on %d calls %s", offCalls, off, onCalls, on)
	}
	if strings.TrimPrefix(off, off[:4]) != strings.Replace(strings.TrimPrefix(on, on[:4]), "vl_refusal", "transient", 1) {
		t.Fatalf("client answers differ: %s / %s", off, on)
	}
}

// peerFleet is n proxies with a peer cache each, on their own listeners, in front of one VictoriaLogs stand-in
// (the routing configuration a peer must share names the backend).
type peerFleet struct {
	proxies []*Proxy
	handler []http.Handler
	stub    *flightStub
}

func newPeerFleet(t *testing.T, releases []string, answer func(int64, http.ResponseWriter, *http.Request)) *peerFleet {
	t.Helper()
	n := len(releases)
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i], addrs[i] = l, l.Addr().String()
	}
	f := &peerFleet{stub: newFlightStub(t, answer)}
	for i := range releases {
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: addrs[i], DiscoveryType: "static", StaticPeers: strings.Join(addrs, ","),
			Timeout: 2 * time.Second, AuthToken: "fleet-token"})
		t.Cleanup(pc.Close)
		p, h := refusalProxy(t, f.stub.srv.URL, releases[i], func(cfg *Config) {
			cfg.PeerCache, cfg.PeerAuthToken = pc, "fleet-token"
		})
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
		go func(l net.Listener) { _ = srv.Serve(l) }(listeners[i])
		t.Cleanup(func() { _ = srv.Close() })
		f.proxies, f.handler = append(f.proxies, p), append(f.handler, h)
	}
	return f
}

func (f *peerFleet) calls() int64 { return f.stub.calls.Load() }

// A refusal VictoriaLogs answered on one replica is pushed to every peer: a fleet of three replicas refreshing the
// same refused query sends one VictoriaLogs call; a replica of another VictoriaLogs release ignores the push and
// asks its own; another tenant is never answered from it.
//
// conformance: semantics/error-cache-vl-refusal-peer-sync
func TestVLRefusal_FleetSendsOneCallPerHold(t *testing.T) {
	f := newPeerFleet(t, []string{"v1.53.0", "v1.53.0", "v1.53.0", "v1.54.0"}, answerParseRefusal)
	first := flightGet(f.handler[0], context.Background(), refusalWindow(0), "team-a")
	if f.calls() != 1 {
		t.Fatalf("first refusal: %d calls", f.calls())
	}
	waitFor(t, func() bool {
		return f.proxies[1].peerSyncCounts[2].Load() == 1 && f.proxies[2].peerSyncCounts[2].Load() == 1 && f.proxies[3].peerSyncCounts[3].Load() == 1
	})
	for round := 0; round < 5; round++ {
		for i := 0; i < 3; i++ {
			got := flightGet(f.handler[i], context.Background(), refusalWindow(round*15), "team-a")
			if got.Code != first.Code || got.Body.String() != first.Body.String() {
				t.Fatalf("replica %d: %d %q", i, got.Code, got.Body.String())
			}
		}
	}
	if got := f.calls(); got != 1 {
		t.Fatalf("three replicas, five refreshes each: %d VictoriaLogs calls, want 1", got)
	}
	flightGet(f.handler[3], context.Background(), refusalWindow(0), "team-a")
	if f.calls() != 2 {
		t.Fatal("a replica of another VictoriaLogs release must ask its own")
	}
	flightGet(f.handler[1], context.Background(), refusalWindow(0), "team-b")
	if f.stub.tenantCalls("20") != 1 {
		t.Fatal("another tenant must never be answered from a peer's refusal")
	}
	if sent := f.proxies[0].peerSyncCounts[0].Load(); sent != 1 {
		t.Fatalf("pushes sent %d", sent)
	}
}

// A refusal held shorter than the sync bound, a predicted refusal and one with peer sync off stay on their replica.
//
// conformance: semantics/error-cache-vl-refusal-peer-sync
func TestVLRefusal_SyncOnlyWhatPeersWouldPayFor(t *testing.T) {
	f := newPeerFleet(t, []string{"v1.53.0", "v1.53.0"}, answerParseRefusal)
	f.proxies[0].errorCacheRefusalTTL = 10 * time.Second
	flightGet(f.handler[0], context.Background(), refusalWindow(0), "team-a")
	f.proxies[0].errorCacheRefusalTTL = time.Minute
	f.proxies[0].errorCachePeerSync = false
	flightGet(f.handler[0], context.Background(), refusalWindow(0)+"&direction=forward", "team-a")
	time.Sleep(200 * time.Millisecond)
	if got := f.proxies[1].peerSyncCounts[2].Load() + f.proxies[1].peerSyncCounts[3].Load(); got != 0 || f.proxies[0].peerSyncCounts[0].Load() != 0 {
		t.Fatalf("pushes received %d", got)
	}
	var b syncBudget
	now := time.Now()
	for i := 0; i < int(refusalSyncRate); i++ {
		if !b.take(now) {
			t.Fatal("budget spent early")
		}
	}
	if b.take(now) || !b.take(now.Add(time.Second)) {
		t.Fatal("budget refill")
	}
	// A push for another build, configuration or a malformed one is ignored.
	p := f.proxies[1]
	for _, value := range []string{"{", `{"b":"other","r":"v1.53.0","e":"query_range","k":"x","x":1,"c":500}`} {
		p.receiveRefusal(refusalSyncKeyPrefix+"x", []byte(value), time.Minute)
	}
	if p.peerSyncCounts[3].Load() != 2 {
		t.Fatalf("ignored %d", p.peerSyncCounts[3].Load())
	}
	// A peer's headers other than the error's content type never reach a client.
	h := peerAnswerHeader(http.Header{"Content-Type": {"text/plain; charset=utf-8"}, "Set-Cookie": {"a=b"}, "Location": {"/x"}, "Access-Control-Allow-Origin": {"*"}})
	if len(h) != 1 || h.Get("Content-Type") == "" {
		t.Fatalf("peer headers %v", h)
	}
}

// The prediction and peer sync counters are exposed with their rules and outcomes.
func TestVLRefusal_Metrics(t *testing.T) {
	p, _ := refusalProxy(t, "http://127.0.0.1:1", "v1.53.0", nil)
	p.countPredictedRefusal(logsql.RuleStatsPipeAfterStats)
	m := metrics.NewMetrics()
	m.SetErrorFlightStatsProvider(p.errorFlightStats)
	rec := httptest.NewRecorder()
	m.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`loki_vl_proxy_vl_refusals_predicted_total{rule="vl-stats-pipe-after-stats"} 1`,
		`loki_vl_proxy_error_cache_peer_sync_total{outcome="sent"} 0`,
		`loki_vl_proxy_error_cache_hits_total{class="vl_refusal"} 0`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing %s", want)
		}
	}
}

// The window-free query set stays within its bound (expired entries go first), the release is recorded once, and
// a refusal is not pushed without a known release or once the send budget is spent.
//
// conformance: semantics/error-cache-vl-refusal-peer-sync
func TestVLRefusal_Bounds(t *testing.T) {
	var s refusalQuerySet
	now := cache.Now().UnixNano()
	s.add("query_range", "a", now-1, 2)
	s.add("query_range", "b", now+int64(time.Minute), 2)
	s.add("query_range", "c", now+int64(time.Minute), 2) // the expired a makes room
	s.add("query_range", "d", now+int64(time.Minute), 2) // full: not added
	if s.has("query_range", "a", now) || !s.has("query_range", "c", now) || s.has("query_range", "d", now) {
		t.Fatalf("set %v", s.m)
	}
	p, _ := refusalProxy(t, "http://127.0.0.1:1", "", nil)
	if p.errorFlightsMaxEntries() != DefaultErrorCacheMaxEntries {
		t.Fatal("default bound")
	}
	before := p.flightPrefix()
	p.noteVLRelease("garbage")
	p.noteVLRelease("v1.53.0")
	after := p.flightPrefix()
	p.noteVLRelease("v1.53.0")
	if before == after || p.flightPrefix() != after || !strings.Contains(after, "v1.53.0") {
		t.Fatalf("prefix %q -> %q", before, after)
	}
	pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1,other:1"})
	t.Cleanup(pc.Close)
	p.peerCache, p.errorCachePeerSync = pc, true
	a := &cache.Answer{Status: http.StatusInternalServerError, Body: []byte("x")}
	until := cache.Now().Add(time.Minute)
	p.vlReleaseSeen.Store(nil)
	p.syncRefusal("query_range", "q", "k", a, until)
	if p.peerSyncCounts[0].Load() != 0 {
		t.Fatal("no push before the release is known")
	}
	p.noteVLRelease("v1.53.0")
	for i := 0; i < int(refusalSyncRate)+1; i++ {
		p.syncRefusal("query_range", "q", "k", a, until)
	}
	if p.peerSyncCounts[0].Load() != int64(refusalSyncRate) || p.peerSyncCounts[1].Load() != 1 {
		t.Fatalf("sent %d throttled %d", p.peerSyncCounts[0].Load(), p.peerSyncCounts[1].Load())
	}
}

// A refusal held for the exact request (a length refusal) reaches the peers under that request's key: the same
// request on another replica sends no call, another window does.
//
// conformance: semantics/error-cache-vl-refusal-peer-sync
func TestVLRefusal_FleetExactRefusal(t *testing.T) {
	f := newPeerFleet(t, []string{"v1.53.0", "v1.53.0"}, func(_ int64, w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("the `query` arg length cannot exceed -search.maxQueryLen=5 bytes; the current query length is %d bytes; query=x", len(r.FormValue("query"))), http.StatusBadRequest)
	})
	first := flightGet(f.handler[0], context.Background(), refusalWindow(0), "team-a")
	waitFor(t, func() bool { return f.proxies[1].peerSyncCounts[2].Load() == 1 })
	got := flightGet(f.handler[1], context.Background(), refusalWindow(0), "team-a")
	if f.calls() != 1 || got.Code != first.Code || got.Body.String() != first.Body.String() {
		t.Fatalf("exact repeat on a peer: %d calls, %d %q", f.calls(), got.Code, got.Body.String())
	}
	flightGet(f.handler[1], context.Background(), refusalWindow(15), "team-a")
	if f.calls() != 2 {
		t.Fatalf("another window must ask: %d calls", f.calls())
	}
}

// A predicted answer reached no VictoriaLogs: the circuit breaker records neither a success nor a failure.
//
// conformance: vl-math-quoted-constant-clone-crash
func TestVLRefusal_PredictionLeavesTheBreakerAlone(t *testing.T) {
	stub := newFlightStub(t, answerLogs)
	p, _ := refusalProxy(t, stub.srv.URL, "v1.49.0", func(cfg *Config) { cfg.CBFailThreshold = 1 })
	for i := 0; i < 3; i++ {
		if _, err := p.vlPost(context.Background(), logsql.PathQuery, url.Values{"query": {`* | math "2025-01-01T00:00:00Z" as x`}, "limit": {"5"}}); err == nil {
			t.Fatal("a predicted crash must fail")
		}
	}
	resp, err := p.vlPost(context.Background(), logsql.PathStatsQueryRange, url.Values{"query": {`* | sort by (a) | stats count()`}, "step": {"1m"}})
	if err != nil || resp.StatusCode != http.StatusUnprocessableEntity || !predictedVLAnswer(resp, nil) {
		t.Fatalf("predicted refusal: %v %v", resp, err)
	}
	_ = resp.Body.Close()
	if !p.breaker.Allow() || stub.calls.Load() != 0 {
		t.Fatalf("breaker open after predictions, or %d calls", stub.calls.Load())
	}
}
