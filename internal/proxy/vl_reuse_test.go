package proxy

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// metricSum is the sum of the samples of family whose labels contain every pair of want ("" matches any).
func metricSum(t *testing.T, body, family string, want map[string]string) float64 {
	t.Helper()
	sum := 0.0
	sc := bufio.NewScanner(strings.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, family+"{") && !strings.HasPrefix(line, family+" ") {
			continue
		}
		labels, value, _ := strings.Cut(line[len(family):], " ")
		ok := true
		for k, v := range want {
			if !strings.Contains(labels, k+`="`+v+`"`) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		sum += f
	}
	return sum
}

// seriesBackend is a VictoriaLogs answering /select/logsql/streams, counting the calls it saw.
func seriesBackend(t *testing.T) (*httptest.Server, *atomic.Int64, int) {
	t.Helper()
	const body = `{"values":[{"value":"{app=\"api\"}","hits":1}]}`
	calls := new(atomic.Int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/streams" {
			w.WriteHeader(http.StatusOK)
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, calls, len(body)
}

func seriesTarget(app string) string {
	return "/loki/api/v1/series?match[]=%7Bapp%3D%22" + app + "%22%7D"
}

// A repeat answered by the response cache reuses the first request's answer: the VictoriaLogs call, time and
// bytes measured while the first one was built count as avoided, under the response layer and the memory tier.
// The first request is served by VictoriaLogs, the repeat by the cache, and each is counted by how it was served.
//
// conformance: observability-vl-savings
func TestResponseCacheReuseCountsTheBuildWork(t *testing.T) {
	srv, calls, bodyLen := seriesBackend(t)
	p := newCompatTestProxy(t, srv.URL)
	headers := map[string]string{"X-Scope-OrgID": "team-a"}
	for i := 0; i < 3; i++ {
		if rec := doCompatProxyRequest(p, seriesTarget("api"), headers); rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("VictoriaLogs calls %d, want 1", calls.Load())
	}
	body := metricsBody(p)
	route := map[string]string{"route": "/loki/api/v1/series"}
	reuse := map[string]string{"route": "/loki/api/v1/series", "layer": "response", "tier": "l1_memory"}
	for _, c := range []struct {
		family string
		labels map[string]string
		want   float64
	}{
		{"loki_vl_proxy_served_request_duration_seconds_count", map[string]string{"served": "vl"}, 1},
		{"loki_vl_proxy_served_request_duration_seconds_count", map[string]string{"served": "cache"}, 2},
		{"loki_vl_proxy_served_vl_calls_total", map[string]string{"served": "vl"}, 1},
		{"loki_vl_proxy_served_vl_calls_total", map[string]string{"served": "cache"}, 0},
		{"loki_vl_proxy_served_vl_bytes_total", map[string]string{"served": "vl"}, float64(bodyLen)},
		{"loki_vl_proxy_vl_reuse_total", map[string]string{"cost": "recorded"}, 2},
		{"loki_vl_proxy_vl_reuse_total", map[string]string{"cost": "unknown"}, 0},
		{"loki_vl_proxy_vl_avoided_calls_total", reuse, 2},
		{"loki_vl_proxy_vl_avoided_bytes_total", reuse, float64(2 * bodyLen)},
	} {
		labels := map[string]string{}
		for k, v := range route {
			labels[k] = v
		}
		for k, v := range c.labels {
			labels[k] = v
		}
		if got := metricSum(t, body, c.family, labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.family, labels, got, c.want)
		}
	}
	if metricSum(t, body, "loki_vl_proxy_vl_avoided_seconds_total", reuse) <= 0 {
		t.Error("avoided VictoriaLogs time is zero")
	}
	if got := metricSum(t, body, "loki_vl_proxy_served_response_bytes_total", map[string]string{"served": "cache"}); got <= 0 {
		t.Errorf("bytes served from the cache %v", got)
	}
}

// Replaying the same requests with the caches and the coalescer on and with them off: the VictoriaLogs calls of
// the run with them off equal the calls of the run with them on plus the calls the reuse avoided. Nothing is
// counted twice and nothing reused goes missing.
//
// conformance: observability-vl-savings
func TestAvoidedCallsMatchTheCallsOfARunWithoutReuse(t *testing.T) {
	replay := []string{"api", "web", "api", "db", "web", "api", "api", "db"}
	run := func(t *testing.T, reuse bool) (float64, float64, int64) {
		srv, calls, _ := seriesBackend(t)
		var p *Proxy
		if reuse {
			p = newCompatTestProxy(t, srv.URL)
		} else {
			var err error
			p, err = New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", CoalescerDisabled: true,
				TenantMap: map[string]TenantMapping{"team-a": {AccountID: "10", ProjectID: "0"}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		}
		for _, app := range replay {
			if rec := doCompatProxyRequest(p, seriesTarget(app), map[string]string{"X-Scope-OrgID": "team-a"}); rec.Code != http.StatusOK {
				t.Fatalf("status %d", rec.Code)
			}
		}
		body := metricsBody(p)
		return metricSum(t, body, "loki_vl_proxy_served_vl_calls_total", nil), metricSum(t, body, "loki_vl_proxy_vl_avoided_calls_total", nil), calls.Load()
	}
	sentOn, avoidedOn, seenOn := run(t, true)
	sentOff, avoidedOff, seenOff := run(t, false)
	if sentOn != float64(seenOn) || sentOff != float64(seenOff) {
		t.Fatalf("calls counted %v/%v, VictoriaLogs saw %d/%d", sentOn, sentOff, seenOn, seenOff)
	}
	if avoidedOff != 0 || seenOff != int64(len(replay)) {
		t.Fatalf("without reuse: avoided %v, VictoriaLogs calls %d", avoidedOff, seenOff)
	}
	if sentOn+avoidedOn != float64(seenOff) {
		t.Fatalf("with reuse %v sent + %v avoided != %d without", sentOn, avoidedOn, seenOff)
	}
}

// Identical requests in flight share one VictoriaLogs call: the waiters count it as avoided (layer vl_call, in
// flight) with the call's measured work, the leader counts it as sent, and their sum is the calls the requests
// would have made apart. The waiters are served as shared.
//
// conformance: observability-vl-savings
func TestCoalescedCallsCountTheSharedCallsWork(t *testing.T) {
	repeatUntilShared(t, func(t *testing.T) float64 {
		release := make(chan struct{})
		var calls atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			<-release
			_, _ = w.Write([]byte(`{"values":[]}`))
		}))
		t.Cleanup(srv.Close)
		p := newTestProxy(t, srv.URL)
		const n = 4
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/series", nil)
				p.requestLogger("series", "/loki/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
					if _, err := p.vlGetCoalesced(r.Context(), "streams", "/select/logsql/streams", nil); err != nil {
						t.Error(err)
						return
					}
					w.WriteHeader(http.StatusOK)
				}).ServeHTTP(httptest.NewRecorder(), req)
			}()
		}
		for p.coalescer.ActiveShared.Load() == 0 && calls.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // the other requests join the call in flight
		close(release)
		wg.Wait()
		body := metricsBody(p)
		sent := metricSum(t, body, "loki_vl_proxy_served_vl_calls_total", nil)
		avoided := metricSum(t, body, "loki_vl_proxy_vl_avoided_calls_total", map[string]string{"layer": "vl_call", "tier": "in_flight"})
		shared := metricSum(t, body, "loki_vl_proxy_served_request_duration_seconds_count", map[string]string{"served": "shared"})
		if sent != float64(calls.Load()) || sent+avoided != n || shared != avoided {
			t.Fatalf("sent %v (VictoriaLogs saw %d) + avoided %v != %d, shared requests %v", sent, calls.Load(), avoided, n, shared)
		}
		if avoided > 0 && metricSum(t, body, "loki_vl_proxy_vl_avoided_bytes_total", nil) != avoided*float64(len(`{"values":[]}`)) {
			t.Fatalf("avoided bytes\n%s", body)
		}
		return avoided
	})
}

// A replica that reads an answer from a peer counts it as reused with an unknown cost (it never measured its
// build), adds no avoided work, and the replica that served the peer counts nothing: across the fleet one
// answer's work is counted once, by the replica that built it, when its own clients reuse it.
//
// conformance: observability-vl-savings
func TestPeerReadReuseHasUnknownCostAndTheOwnerCountsNothing(t *testing.T) {
	srv, calls, _ := seriesBackend(t)
	owner := cache.New(time.Minute, 100)
	t.Cleanup(owner.Close)
	server := cache.NewPeerCache(cache.PeerConfig{})
	t.Cleanup(server.Close)
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { server.ServeHTTP(w, r, owner) }))
	t.Cleanup(peerSrv.Close)
	newReplica := func(c *cache.Cache) *Proxy {
		p, err := New(Config{BackendURL: srv.URL, Cache: c, CompatCache: cache.New(time.Minute, 100), LogLevel: "error"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	a := newReplica(owner)
	bCache := cache.New(time.Minute, 100)
	t.Cleanup(bCache.Close)
	pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "b:3100", DiscoveryType: "static", StaticPeers: strings.TrimPrefix(peerSrv.URL, "http://"), Timeout: 5 * time.Second})
	t.Cleanup(pc.Close)
	bCache.SetL3(pc)
	b := newReplica(bCache)
	if rec := doCompatProxyRequest(a, seriesTarget("api"), nil); rec.Code != http.StatusOK {
		t.Fatalf("a: %d", rec.Code)
	}
	if rec := doCompatProxyRequest(b, seriesTarget("api"), nil); rec.Code != http.StatusOK {
		t.Fatalf("b: %d", rec.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("VictoriaLogs calls %d, want 1 (b reads a's answer)", calls.Load())
	}
	if got := metricSum(t, metricsBody(a), "loki_vl_proxy_vl_reuse_total", nil); got != 0 {
		t.Fatalf("the owner counted %v reuses", got)
	}
	bBody := metricsBody(b)
	if got := metricSum(t, bBody, "loki_vl_proxy_vl_reuse_total", map[string]string{"tier": "l3_peer", "cost": "unknown"}); got != 1 {
		t.Fatalf("b's peer reuse %v\n%s", got, bBody)
	}
	if got := metricSum(t, bBody, "loki_vl_proxy_vl_avoided_calls_total", nil); got != 0 {
		t.Fatalf("b counted %v avoided calls for an answer it never built", got)
	}
}

// The ledger keeps one entry per slot under the key's full hash: a key that lands in a slot another key took reads
// as unknown, never as the other key's work, and the table never grows.
//
// conformance: observability-vl-savings
func TestBuildCostLedgerNeverAnswersAnotherKey(t *testing.T) {
	l := newBuildCostLedger(10000)
	w := metrics.VLWork{Calls: 3, Duration: time.Second, Bytes: 10, Rows: 2}
	l.put("a", w)
	if got, ok := l.get("a"); !ok || got != w {
		t.Fatalf("get a = %v %v", got, ok)
	}
	if _, ok := l.get("b"); ok {
		t.Fatal("an absent key answered")
	}
	// Find a key that shares a's slot.
	ha, ia := l.slot("a")
	other := ""
	for i := 0; other == ""; i++ {
		k := fmt.Sprint("k", i)
		if h, idx := l.slot(k); idx == ia && h != ha {
			other = k
		}
	}
	l.put(other, metrics.VLWork{Calls: 7})
	if _, ok := l.get("a"); ok {
		t.Fatal("a overwritten slot still answers a")
	}
	if got, ok := l.get(other); !ok || got.Calls != 7 {
		t.Fatalf("get other = %v %v", got, ok)
	}
	if len(*l.slots.Load()) != 1<<16 {
		t.Fatalf("ledger of %d slots for 10000 entries, want 65536", len(*l.slots.Load()))
	}
	if small, big := newBuildCostLedger(0), newBuildCostLedger(1<<30); small.size != buildCostMinSlots || big.size != buildCostMaxSlots {
		t.Fatalf("ledger sizes %d and %d", small.size, big.size)
	}
	if _, ok := newBuildCostLedger(10).get("a"); ok {
		t.Fatal("an empty ledger answered")
	}
	var nilLedger *buildCostLedger
	nilLedger.put("a", w)
	if _, ok := nilLedger.get("a"); ok {
		t.Fatal("a nil ledger answered")
	}
}

// How a request was served follows from its status, its own VictoriaLogs calls, its cache hits and its reuse.
//
// conformance: observability-vl-savings
func TestServedClass(t *testing.T) {
	for _, c := range []struct {
		status, calls, hits int
		reuse               flowReuse
		want                metrics.ServedClass
	}{
		{200, 0, 0, flowReuse{}, metrics.ServedLocal},
		{200, 0, 1, flowReuse{}, metrics.ServedCache},
		{200, 0, 0, flowReuse{recorded: 1}, metrics.ServedCache},
		{200, 0, 0, flowReuse{shared: true}, metrics.ServedShared},
		{200, 2, 1, flowReuse{}, metrics.ServedVL},
		{200, 2, 0, flowReuse{unknown: 1}, metrics.ServedPartial},
		{200, 1, 0, flowReuse{shared: true}, metrics.ServedPartial},
		{200, 1, 0, flowReuse{}, metrics.ServedVL},
		{429, 0, 1, flowReuse{recorded: 1}, metrics.ServedError},
		{500, 3, 0, flowReuse{}, metrics.ServedError},
	} {
		if got := servedClass(c.status, c.calls, c.hits, c.reuse); got != c.want {
			t.Errorf("servedClass(%d, %d, %d, %+v) = %v, want %v", c.status, c.calls, c.hits, c.reuse, got, c.want)
		}
	}
	var r flowReuse
	for i := 0; i < 300; i++ {
		r.add(true)
		r.add(false)
	}
	if r.recorded != 255 || r.unknown != 255 {
		t.Fatalf("counts do not stop at 255: %+v", r)
	}
}

// A work scope collects the VictoriaLogs work of its context and of the scopes under it; work outside it is not
// its own.
//
// conformance: observability-vl-savings
func TestWorkScopesNest(t *testing.T) {
	ctx, outer := withWorkScope(context.Background())
	inner, piece := withWorkScope(ctx)
	recordFlowVL(inner, 10*time.Millisecond, 100)
	recordFlowVLRows(inner, 5)
	recordFlowVL(ctx, time.Millisecond, 1)
	recordFlowVL(context.Background(), time.Millisecond, 1000)
	if got := piece.work(); got != (metrics.VLWork{Calls: 1, Duration: 10 * time.Millisecond, Bytes: 100, Rows: 5}) {
		t.Fatalf("piece %+v", got)
	}
	if got := outer.work(); got != (metrics.VLWork{Calls: 2, Duration: 11 * time.Millisecond, Bytes: 101, Rows: 5}) {
		t.Fatalf("outer %+v", got)
	}
}

// repeatUntilShared runs a scenario of identical requests in flight until one of them shared another's work (the
// scheduler decides whether a request joins before the first one ends), checking its counts on every run.
func repeatUntilShared(t *testing.T, run func(t *testing.T) float64) {
	t.Helper()
	for attempt := 0; attempt < 10; attempt++ {
		if run(t) >= 1 || t.Failed() {
			return
		}
	}
	t.Fatal("no request shared another's work in 10 runs")
}

// reuseCtx is the context of one client request of route.
func reuseCtx(endpoint, route string) context.Context {
	ctx := context.WithValue(context.Background(), requestTelemetryKey, newRequestTelemetry())
	return context.WithValue(ctx, requestRouteMetaKey, requestRouteMeta{endpoint: endpoint, route: route})
}

// A query_range log window is a piece built in its own work scope: a later request that reads it from memory
// avoided the window's call, time, bytes and rows. After a restart the window read from disk was built by an
// earlier process, so it counts as reuse of unknown cost.
//
// conformance: observability-vl-savings
func TestWindowReuseCountsTheWindowsOwnWork(t *testing.T) {
	const line = `{"_time":"2026-10-01T00:01:00Z","_msg":"settled log","_stream":"{app=\"api\"}"}` + "\n"
	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		_, _ = w.Write([]byte(line))
	}))
	defer backend.Close()
	config := cache.DiskCacheConfig{Path: t.TempDir() + "/log-window.db", MaxBytes: 1 << 20, FlushInterval: time.Hour}
	request := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&limit=100", nil)
	if err := request.ParseForm(); err != nil {
		t.Fatal(err)
	}
	window := queryRangeWindow{startNs: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()}
	window.endNs = window.startNs + int64(time.Hour) - 1
	shape := newLogQueryShape(request.FormValue("query"))
	newProxy := func(disk *cache.DiskCache) (*Proxy, *cache.Cache) {
		memory := cache.New(time.Minute, 1000)
		memory.SetL2(disk)
		p, err := New(Config{BackendURL: backend.URL, Cache: memory, LogLevel: "error", QueryRangeHistoryCacheTTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		return p, memory
	}
	fetch := func(p *Proxy) {
		t.Helper()
		if _, err := p.fetchQueryRangeWindow(reuseCtx("query_range", "/loki/api/v1/query_range"), request, `{app="api"}`, "100", 100, window, shape, false, false); err != nil {
			t.Fatal(err)
		}
	}
	firstDisk, err := cache.NewDiskCache(config)
	if err != nil {
		t.Fatal(err)
	}
	first, firstMemory := newProxy(firstDisk)
	fetch(first)
	fetch(first)
	if backendCalls.Load() != 1 {
		t.Fatalf("VictoriaLogs calls %d", backendCalls.Load())
	}
	window1 := map[string]string{"layer": "window", "tier": "l1_memory"}
	body := metricsBody(first)
	for family, want := range map[string]float64{
		"loki_vl_proxy_vl_avoided_calls_total": 1,
		"loki_vl_proxy_vl_avoided_bytes_total": float64(len(line)),
		"loki_vl_proxy_vl_avoided_rows_total":  1,
	} {
		if got := metricSum(t, body, family, window1); got != want {
			t.Fatalf("%s = %v, want %v", family, got, want)
		}
	}
	firstDisk.Flush()
	_ = first.Shutdown(context.Background())
	firstMemory.Close()
	if err := firstDisk.Close(); err != nil {
		t.Fatal(err)
	}
	secondDisk, err := cache.NewDiskCache(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDisk.Close() })
	second, secondMemory := newProxy(secondDisk)
	t.Cleanup(func() { _ = second.Shutdown(context.Background()); secondMemory.Close() })
	fetch(second)
	body = metricsBody(second)
	if backendCalls.Load() != 1 || metricSum(t, body, "loki_vl_proxy_vl_reuse_total", map[string]string{"layer": "window", "tier": "l2_disk", "cost": "unknown"}) != 1 ||
		metricSum(t, body, "loki_vl_proxy_vl_avoided_calls_total", nil) != 0 {
		t.Fatalf("restored window: VictoriaLogs calls %d\n%s", backendCalls.Load(), body)
	}
}

// Identical client requests in flight get the leader's answer: each follower counts the leader's measured work as
// avoided (request_flight, in flight) and is served as shared; a failure held afterwards answers later requests as
// a held_failure reuse.
//
// conformance: observability-vl-savings
func TestRequestFlightFollowersCountTheLeadersWork(t *testing.T) {
	repeatUntilShared(t, func(t *testing.T) float64 {
		release := make(chan struct{})
		var calls atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/select/logsql/streams" {
				w.WriteHeader(http.StatusOK)
				return
			}
			calls.Add(1)
			<-release
			_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\"}","hits":1}]}`))
		}))
		t.Cleanup(srv.Close)
		// No response cache: identical requests still share one run (error_flights.go).
		p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		const n = 3
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if rec := doCompatProxyRequest(p, seriesTarget("api"), nil); rec.Code != http.StatusOK {
					t.Errorf("status %d", rec.Code)
				}
			}()
		}
		for calls.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond)
		close(release)
		wg.Wait()
		body := metricsBody(p)
		flight := map[string]string{"layer": "request_flight", "tier": "in_flight"}
		followers := metricSum(t, body, "loki_vl_proxy_vl_reuse_total", flight)
		if metricSum(t, body, "loki_vl_proxy_vl_avoided_calls_total", flight) != followers ||
			metricSum(t, body, "loki_vl_proxy_served_vl_calls_total", nil)+followers != n ||
			metricSum(t, body, "loki_vl_proxy_served_request_duration_seconds_count", map[string]string{"served": "shared"}) != followers {
			t.Fatalf("followers %v, VictoriaLogs calls %d\n%s", followers, calls.Load(), body)
		}
		return followers
	})
}

// Stored stats buckets an answer reads instead of asking VictoriaLogs count as reuse of unknown cost, one per
// stored split the answer used, at the tier it came from: their share of the reads that stored them was not
// measured, so they add no avoided work. A moved window that fetches its new buckets is served as partial.
//
// conformance: observability-vl-savings
func TestStatsExtentReuseIsCountedWithUnknownCost(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 2000; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 10 * time.Second).UnixNano(), app: []string{"a", "b"}[i%2], msg: "n=1"})
	}
	fake, _ := newSlidingFakeVL(t, lines)
	srv, _ := newExtentShim(t, fake.Config.Handler)
	p := newExtentTestProxy(t, srv.URL, true, 256<<20)
	query := `sum by (app) (count_over_time({app=~".+"}[1m]))`
	logged := p.requestLogger("query_range", "/loki/api/v1/query_range", p.handleQueryRange)
	for _, shift := range []time.Duration{0, time.Hour} {
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(s0.Add(shift).UnixNano(), 10)},
			"end": {strconv.FormatInt(s0.Add(shift+3*time.Hour).UnixNano(), 10)}, "step": {"60"}}
		rec := httptest.NewRecorder()
		logged.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		p.statsExtentStores.wait()
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
	}
	body := metricsBody(p)
	extent := map[string]string{"layer": "stats_extent"}
	if metricSum(t, body, "loki_vl_proxy_vl_reuse_total", map[string]string{"layer": "stats_extent", "cost": "unknown"}) < 1 ||
		metricSum(t, body, "loki_vl_proxy_vl_reuse_total", map[string]string{"layer": "stats_extent", "cost": "recorded"}) != 0 ||
		metricSum(t, body, "loki_vl_proxy_vl_avoided_calls_total", extent) != 0 {
		t.Fatalf("stats extent reuse\n%s", body)
	}
	if metricSum(t, body, "loki_vl_proxy_served_request_duration_seconds_count", map[string]string{"route": "/loki/api/v1/query_range", "served": "partial"}) != 1 {
		t.Fatalf("the moved window is not partial\n%s", body)
	}
}

// A Logs Drilldown field count read by the fused call another request started counts as a burst reuse of unknown
// cost: its own scan was never sent, and the fused call's cost is not the field's.
//
// conformance: observability-vl-savings
func TestBurstJoinerCountsAnUnknownCostReuse(t *testing.T) {
	c := newDrilldownBurstCoalescer(500, 10)
	var joined atomic.Int64
	c.onJoined = func(context.Context) { joined.Add(1) }
	fire := func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
		out := map[string]fieldResult{}
		for _, f := range fields {
			out[f] = fieldResult{}
		}
		return out, nil
	}
	var wg sync.WaitGroup
	for i, f := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Submit(context.Background(), burstKey{base: "q"}, f, fire); err != nil {
				t.Error(err)
			}
		}()
		for i == 0 && func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.pending) == 0 }() {
			time.Sleep(time.Millisecond) // the first field opens the group
		}
	}
	wg.Wait()
	if joined.Load() != 2 {
		t.Fatalf("joiners %d of 3 fields read by one fused call", joined.Load())
	}
}
