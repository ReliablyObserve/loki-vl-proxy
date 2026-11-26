package proxy

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// routeVL is a VictoriaLogs stand-in for the service_name route: it records every data read (path, whether it was
// the proof, the stream filter it carried in extra_filters), answers the proof with a row when found is set (or a
// 500 when proofFails is), refuses a narrowed read with refuseNarrowed, and answers every log read with one row.
type routeVL struct {
	mu             sync.Mutex
	reads          []routeRead
	found          bool
	proofFails     bool
	refuseNarrowed bool
	// throttleNarrowed answers a narrowed read 429, as vmauth or an overloaded VictoriaLogs may.
	throttleNarrowed bool
}

type routeRead struct {
	path     string
	proof    bool
	narrowed bool
	tenant   string
}

func (v *routeVL) handler(t *testing.T, value string) http.Handler {
	proof := logsql.Query{Filter: translate.ServiceNameStreamFilterProof(value), Pipes: []logsql.Pipe{logsql.PipeLimit{N: 1}}}.String()
	stream := translate.ServiceNameStreamFilter(value).String()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = io.Copy(io.Discard, r.Body)
		switch r.URL.Path {
		case "/select/logsql/query", "/select/logsql/hits", "/select/logsql/stats_query", "/select/logsql/stats_query_range":
		default:
			return
		}
		read := routeRead{path: r.URL.Path, proof: r.Form.Get("query") == proof, tenant: r.Header.Get("AccountID")}
		for _, f := range r.Form["extra_filters"] {
			if f == stream {
				read.narrowed = true
			} else if strings.Contains(f, "service_name") {
				t.Errorf("unexpected extra_filters %q", f)
			}
		}
		v.mu.Lock()
		v.reads = append(v.reads, read)
		found, fails, refuse, throttle := v.found, v.proofFails, v.refuseNarrowed, v.throttleNarrowed
		v.mu.Unlock()
		switch {
		case read.proof && fails:
			http.Error(w, "boom", http.StatusInternalServerError)
		case read.proof && found:
			_, _ = w.Write([]byte(`{"_time":"2026-11-19T23:30:00Z","_msg":"x","service_name":"` + value + `"}` + "\n"))
		case read.proof:
		case read.narrowed && throttle:
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		case read.narrowed && refuse:
			http.Error(w, "cannot parse extra_filters", http.StatusBadRequest)
		case r.URL.Path == "/select/logsql/query":
			_, _ = w.Write([]byte(`{"_time":"2026-11-19T23:30:00Z","_msg":"hello","_stream":"{service.name=\"` + value + `\"}","service.name":"` + value + `"}` + "\n"))
		case r.URL.Path == "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
		case strings.Contains(r.URL.Path, "stats_query_range"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		}
	})
}

func (v *routeVL) take() []routeRead {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := v.reads
	v.reads = nil
	return out
}

func (v *routeVL) set(fn func(*routeVL)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fn(v)
}

func serviceNameRouteProxy(t *testing.T, backend string, cfg func(*Config)) *Proxy {
	t.Helper()
	c := Config{
		BackendURL:                         backend,
		Cache:                              cache.New(60*time.Second, 1000),
		LogLevel:                           "error",
		LabelStyle:                         LabelStyleUnderscores,
		MetadataFieldMode:                  MetadataFieldModeTranslated,
		FieldMappings:                      []FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}},
		ServiceNameStreamRoute:             true,
		ServiceNameStreamRouteMinWindow:    30 * time.Minute,
		ServiceNameStreamRouteViolationTTL: time.Hour,
	}
	if cfg != nil {
		cfg(&c)
	}
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	return p
}

var snRouteEnd = time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)

// routeQuery sends a query_range of query over [snRouteEnd-shift-window, snRouteEnd-shift] as tenant.
func routeQuery(t *testing.T, p *Proxy, query string, window, shift time.Duration, tenant string, step string) *httptest.ResponseRecorder {
	t.Helper()
	end := snRouteEnd.Add(-shift)
	v := url.Values{"query": {query}, "start": {strconv.FormatInt(end.Add(-window).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"100"}}
	if step != "" {
		v.Set("step", step)
	}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+v.Encode(), map[string]string{"X-Scope-OrgID": tenant})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body.String())
	}
	return res
}

// sameJSONAnswer compares two answers as JSON values (object keys in any order).
func sameJSONAnswer(t *testing.T, name, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("%s: %v: %s", name, err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: %v: %s", name, err, want)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s: answer with the route\n%s\nwithout\n%s", name, got, want)
	}
}

func countReads(reads []routeRead) (data, narrowed, proofs int) {
	for _, r := range reads {
		switch {
		case r.proof:
			proofs++
		case r.narrowed:
			narrowed++
			data++
		default:
			data++
		}
	}
	return
}

// TestServiceNameRoute_CleanWindowIsNarrowed: a one-service log query over a window the proof finds clean is read
// once, narrowed by the stream filter, beside one proof read, and answers what the proxy without the route answers;
// the exact repeat is served from the response cache with no VictoriaLogs read.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_CleanWindowIsNarrowed(t *testing.T) {
	vl := &routeVL{}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, nil)
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.ServiceNameStreamRoute = false })
	for _, q := range []string{`{service_name="svc-a"}`, `{service_name="svc-a"} | json | logfmt | drop __error__, __error_details__`, `{service_name="svc-a", level="info"} |= "x"`} {
		want := routeQuery(t, off, q, time.Hour, 0, "0", "").Body.String()
		base, n0, pr0 := countReads(vl.take())
		if base == 0 || n0 != 0 || pr0 != 0 {
			t.Fatalf("%s: route off: %d reads, %d narrowed, %d proofs", q, base, n0, pr0)
		}
		got := routeQuery(t, on, q, time.Hour, 0, "0", "").Body.String()
		sameJSONAnswer(t, q, got, want)
		// A read the handler makes before it reaches the log read (the parser probe of `| json | logfmt`) is
		// made as without the route.
		if d, n, pr := countReads(vl.take()); d != base || n != 1 || pr != 1 {
			t.Errorf("%s: route on, clean: %d reads, %d narrowed, %d proofs; want %d, 1, 1", q, d, n, pr, base)
		}
		routeQuery(t, on, q, time.Hour, 0, "0", "")
		if reads := vl.take(); len(reads) != 0 {
			t.Errorf("%s: the exact repeat made %d VictoriaLogs reads", q, len(reads))
		}
	}
}

// TestServiceNameRoute_ViolationFallsBackAndIsRemembered: when the proof finds a row the narrowing leaves out, the
// read is sent again without the stream filter (the answer is the derived filter's), the finding is remembered for
// the tenant and service, and the next log read of that service is sent once, unnarrowed, with no proof; another
// tenant and another service are narrowed still.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_ViolationFallsBackAndIsRemembered(t *testing.T) {
	vl := &routeVL{found: true}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, nil)
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.ServiceNameStreamRoute = false })
	q := `{service_name="svc-a"}`
	want := routeQuery(t, off, q, time.Hour, 0, "0", "").Body.String()
	vl.take()
	sameJSONAnswer(t, "after a violation", routeQuery(t, on, q, time.Hour, 0, "0", "").Body.String(), want)
	reads := vl.take()
	if d, n, pr := countReads(reads); d != 2 || n != 1 || pr != 1 || reads[len(reads)-1].narrowed {
		t.Errorf("violation: %d reads, %d narrowed, %d proofs (%+v); want the narrowed read, the proof and the read without the stream filter last", d, n, pr, reads)
	}
	routeQuery(t, on, q, time.Hour, time.Minute, "0", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
		t.Errorf("after a remembered violation: %d reads, %d narrowed, %d proofs; want 1, 0, 0", d, n, pr)
	}
	vl.set(func(v *routeVL) { v.found = false })
	routeQuery(t, on, q, time.Hour, 0, "2", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 1 || pr != 1 {
		t.Errorf("another tenant: %d reads, %d narrowed, %d proofs; want 1, 1, 1", d, n, pr)
	}
}

// TestServiceNameRoute_DoubtKeepsTheDerivedFilter: a failed proof, a throttled narrowed read and a narrowed read
// VictoriaLogs refuses send the read again without the stream filter; a failed proof and a throttle are not
// remembered (the next read is narrowed and proven again), a refused narrowing (400) is.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_DoubtKeepsTheDerivedFilter(t *testing.T) {
	vl := &routeVL{proofFails: true}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, nil)
	q := `{service_name="svc-a"}`
	routeQuery(t, on, q, time.Hour, 0, "0", "")
	if reads := vl.take(); len(reads) != 3 || reads[2].narrowed || reads[2].proof {
		t.Fatalf("failed proof: %+v; want narrowed, proof, unnarrowed", reads)
	}
	vl.set(func(v *routeVL) { v.proofFails, v.throttleNarrowed = false, true })
	routeQuery(t, on, q, time.Hour, 30*time.Second, "0", "")
	if reads := vl.take(); len(reads) != 3 || reads[2].narrowed {
		t.Fatalf("a throttled narrowed read falls back: %+v", reads)
	}
	vl.set(func(v *routeVL) { v.throttleNarrowed = false })
	routeQuery(t, on, q, time.Hour, 45*time.Second, "0", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 1 || pr != 1 {
		t.Fatalf("a throttle is not remembered: %d reads, %d narrowed, %d proofs; want 1, 1, 1", d, n, pr)
	}
	vl.set(func(v *routeVL) { v.refuseNarrowed = true })
	routeQuery(t, on, q, time.Hour, time.Minute, "0", "")
	if reads := vl.take(); len(reads) != 3 || reads[2].narrowed {
		t.Fatalf("a failed proof is not remembered, a refused narrowing falls back: %+v", reads)
	}
	routeQuery(t, on, q, time.Hour, 2*time.Minute, "0", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
		t.Errorf("after a refused narrowing: %d reads, %d narrowed, %d proofs; want 1, 0, 0", d, n, pr)
	}
}

// TestServiceNameRoute_KeepsTheDerivedFilter: the classes the route leaves alone send what the proxy sends without
// it: a read shorter than the minimum window, metric queries, negated, regexp and empty matchers, a selector with a
// declared stream field, no service_name matcher, the route off, and -service-name-stream-label-only.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_KeepsTheDerivedFilter(t *testing.T) {
	vl := &routeVL{}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.StreamFields = []string{"app"} })
	for _, c := range []struct {
		query  string
		window time.Duration
		step   string
	}{
		{`{service_name="svc-a"}`, 15 * time.Minute, ""},
		{`sum(count_over_time({service_name="svc-a"}[1m]))`, time.Hour, "60"},
		{`sum by (level) (count_over_time({service_name="svc-a", level!=""}[1m]))`, time.Hour, "60"},
		{`{service_name!="svc-a", job="x"}`, time.Hour, ""},
		{`{service_name=~"svc-a|svc-b"}`, time.Hour, ""},
		{`{service_name!~"svc-a", job="x"}`, time.Hour, ""},
		{`{service_name="", job="x"}`, time.Hour, ""},
		{`{app="x", service_name="svc-a"}`, time.Hour, ""},
		{`{job="x"}`, time.Hour, ""},
	} {
		routeQuery(t, on, c.query, c.window, 0, "0", c.step)
		if d, n, pr := countReads(vl.take()); n != 0 || pr != 0 || d == 0 {
			t.Errorf("%s over %s: %d reads, %d narrowed, %d proofs; want the reads without the route", c.query, c.window, d, n, pr)
		}
	}
	for name, cfg := range map[string]func(*Config){
		"route off":  func(c *Config) { c.ServiceNameStreamRoute = false },
		"label only": func(c *Config) { c.ServiceNameStreamLabelOnly = true },
	} {
		p := serviceNameRouteProxy(t, srv.URL, cfg)
		routeQuery(t, p, `{service_name="svc-a"}`, time.Hour, 0, "0", "")
		if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
			t.Errorf("%s: %d reads, %d narrowed, %d proofs; want 1, 0, 0", name, d, n, pr)
		}
	}
}

// TestServiceNameRoute_SplitWindowsShareTheirProofs: a log query the proxy reads in windows (-query-range-windowing)
// proves the first window and the rest, which every later window read of the request joins: at most two proof
// reads, whatever the number of windows.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_SplitWindowsShareTheirProofs(t *testing.T) {
	vl := &routeVL{}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, func(c *Config) {
		c.QueryRangeWindowingEnabled, c.QueryRangeSplitInterval = true, time.Hour
		c.ServiceNameStreamRouteMinWindow = 0
	})
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) {
		c.QueryRangeWindowingEnabled, c.QueryRangeSplitInterval = true, time.Hour
		c.ServiceNameStreamRoute = false
	})
	q := `{service_name="svc-a"}`
	want := routeQuery(t, off, q, 6*time.Hour, 0, "0", "").Body.String()
	base := len(vl.take())
	sameJSONAnswer(t, "split", routeQuery(t, on, q, 6*time.Hour, 0, "0", "").Body.String(), want)
	d, n, pr := countReads(vl.take())
	if d != base || n != d || pr == 0 || pr > 2 {
		t.Errorf("split windows: %d reads (%d without the route), %d narrowed, %d proofs; want every read narrowed and 1 or 2 proofs", d, base, n, pr)
	}
}

// BenchmarkWithServiceNameRoute is the work the route adds to a log query request before its reads: arming it for
// a one-service selector, and the check that leaves any other selector alone.
func BenchmarkWithServiceNameRoute(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 1000), LogLevel: "error", ServiceNameStreamRoute: true, ServiceNameStreamRouteViolationTTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(b.Context()) })
	for _, c := range []struct{ name, query string }{
		{"one service", `{service_name="svc-a"} | json`},
		{"other selector", `{app="x"} | json`},
	} {
		parsed, err := logqlpkg.Parse(c.query)
		if err != nil {
			b.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?start=1&end=3600000000000", nil)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = p.withServiceNameRoute(r, parsed)
			}
		})
	}
}

// snRouteMetric reads p's count of a service_name route outcome from its /metrics.
func snRouteMetric(p *Proxy, outcome string) string {
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, `loki_vl_proxy_internal_operation_total{operation="service_name_stream_route",outcome="`+outcome+`"}`) {
			return line[strings.LastIndexByte(line, ' ')+1:]
		}
	}
	return "0"
}

// TestServiceNameRoute_ViolationReachesPeers: a violation one replica finds is pushed to its peers (the everywhere
// class), so another replica's first log read of that tenant's service goes to VictoriaLogs once, unnarrowed, with
// no proof read; a pushed key that is not a violation key is rejected.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_ViolationReachesPeers(t *testing.T) {
	vl := &routeVL{found: true}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	listeners := make([]net.Listener, 2)
	for i := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
	}
	static := listeners[0].Addr().String() + "," + listeners[1].Addr().String()
	replicas := make([]*Proxy, 2)
	for i, ln := range listeners {
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: ln.Addr().String(), DiscoveryType: "static", StaticPeers: static, AuthToken: "peer-token", Timeout: 2 * time.Second})
		t.Cleanup(pc.Close)
		c := cache.New(60*time.Second, 1000)
		c.SetL3(pc)
		replicas[i] = serviceNameRouteProxy(t, srv.URL, func(cfg *Config) { cfg.Cache, cfg.PeerCache, cfg.PeerAuthToken = c, pc, "peer-token" })
		mux := http.NewServeMux()
		replicas[i].RegisterRoutes(mux)
		hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = hs.Serve(ln) }()
		t.Cleanup(func() { _ = hs.Close() })
	}
	a, b := replicas[0], replicas[1]
	q := `{service_name="svc-a"}`
	routeQuery(t, a, q, time.Hour, 0, "0", "")
	if d, n, pr := countReads(vl.take()); d != 2 || n != 1 || pr != 1 {
		t.Fatalf("replica a: %d reads, %d narrowed, %d proofs; want the narrowed read, the proof and the read without the filter", d, n, pr)
	}
	deadline := time.Now().Add(10 * time.Second)
	for snRouteMetric(b, "peer_violation_received") == "0" {
		if time.Now().After(deadline) {
			t.Fatal("the violation did not reach replica b")
		}
		time.Sleep(20 * time.Millisecond)
	}
	routeQuery(t, b, q, time.Hour, time.Minute, "0", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
		t.Errorf("replica b after a's violation: %d reads, %d narrowed, %d proofs; want 1, 0, 0", d, n, pr)
	}
	b.receiveEverywhere(serviceNameRouteSyncKeyPrefix+"x", []byte(`{"k":"response:anything","x":9999999999999}`), time.Hour)
	if snRouteMetric(b, "peer_violation_rejected") == "0" {
		t.Error("a pushed key that is no violation key was not rejected")
	}
	if _, _, ok := b.cache.GetWithTTL("response:anything"); ok {
		t.Error("a peer push wrote a key that is no violation key")
	}
	// A violation of a scope of a user's forwarded credentials is never pushed, nor taken from a peer.
	credKey := serviceNameRouteOffKeyPrefix + "x:scope:t0:auth:" + identityFingerprintPrefix + "abc"
	rejected := snRouteMetric(b, "peer_violation_rejected")
	b.receiveEverywhere(serviceNameRouteSyncKeyPrefix+"y", []byte(`{"k":"`+credKey+`","x":9999999999999}`), time.Hour)
	if snRouteMetric(b, "peer_violation_rejected") == rejected {
		t.Error("a pushed violation of a credential scope was not rejected")
	}
	if _, _, ok := b.cache.GetWithTTL(credKey); ok {
		t.Error("a peer push wrote a violation of a credential scope")
	}
	// The send side keeps such a key local through the same filter (rememberServiceNameRouteOff).
	if peerPushAllowed(credKey) {
		t.Fatal("the credential scope key is not one the push filter keeps local")
	}
}

// formVL records every data read's whole form (path and every argument, sorted).
type formVL struct {
	mu    sync.Mutex
	reads []string
}

func (v *formVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	switch r.URL.Path {
	case "/select/logsql/query", "/select/logsql/hits", "/select/logsql/stats_query", "/select/logsql/stats_query_range":
		v.mu.Lock()
		v.reads = append(v.reads, r.URL.Path+"?"+r.Form.Encode())
		v.mu.Unlock()
	}
	switch {
	case strings.Contains(r.URL.Path, "stats_query_range"):
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	case strings.Contains(r.URL.Path, "stats_query"):
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	case r.URL.Path == "/select/logsql/hits":
		_, _ = w.Write([]byte(`{"hits":[]}`))
	}
}

func (v *formVL) take() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := v.reads
	v.reads = nil
	return out
}

// TestServiceNameRoute_OffSendsWhatTheProxyWithoutItSends: with the route off (the default) every VictoriaLogs
// read of the Drilldown shapes, log and metric, over short and long windows, carries exactly the path and
// arguments of the proxy built without the route's configuration at all: no extra_filters, no proof read.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_OffSendsWhatTheProxyWithoutItSends(t *testing.T) {
	vl := &formVL{}
	srv := httptest.NewServer(vl)
	defer srv.Close()
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.ServiceNameStreamRoute = false })
	plain := serviceNameRouteProxy(t, srv.URL, func(c *Config) {
		c.ServiceNameStreamRoute, c.ServiceNameStreamRouteMinWindow, c.ServiceNameStreamRouteViolationTTL = false, 0, 0
	})
	for _, c := range serviceNameDrilldownShapes {
		for _, window := range []time.Duration{15 * time.Minute, time.Hour, 6 * time.Hour} {
			routeQuery(t, plain, c.query, window, 0, "0", c.step)
			want := vl.take()
			routeQuery(t, off, c.query, window, 0, "0", c.step)
			got := vl.take()
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("%s over %s: the route off sends\n%s\nthe proxy without it\n%s", c.query, window, strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
			for _, read := range got {
				if strings.Contains(read, "extra_filters=%7Bservice_name") {
					t.Errorf("%s: the route off sent the stream filter: %s", c.query, read)
				}
			}
		}
	}
}
