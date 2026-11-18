package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientprofile"
)

const (
	buildV153 = "victoria-logs-20260901-120000-tags-v1.53.0-0-gabcdef0123"
	buildV154 = "victoria-logs-20261113-102924-tags-v1.54.0-0-g6433dae586"
	buildV149 = "victoria-logs-20260101-120000-tags-v1.49.0-0-g0123456789"
	buildV152 = "victoria-logs-20261101-000000-tags-v1.52.2-enterprise-0-gabcdef0123"
	// the stack's vmauth: VictoriaMetrics v1.155.0
	buildVMAuth = "vmauth-20260929-120000-tags-v1.155.0-0-gabcdef0123"
)

// capBackend is a VictoriaLogs stand-in for the capability tests: it serves /metrics for a build string and answers
// the proxy's feature probes as a release does, or as the test overrides, or not at all (an outage).
type capBackend struct {
	srv  *httptest.Server
	mu   sync.Mutex
	rel  string // the release it behaves as ("" answers the probes with a 503)
	text string // the /metrics payload
	down bool   // every request answers 503
	// override answers a probe path with this status and body (a probe the test wants refused or accepted)
	override map[string][2]any
	hits     map[string]int
}

func newCapBackend(t *testing.T, build string) *capBackend {
	t.Helper()
	b := &capBackend{hits: map[string]int{}}
	b.setBuild(build)
	b.srv = httptest.NewServer(http.HandlerFunc(b.serve))
	t.Cleanup(b.srv.Close)
	return b
}

func (b *capBackend) setBuild(build string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.text = fmt.Sprintf("vm_app_version{version=%q,short_version=\"x\"} 1\n", build)
	if r, ok := capability.ParseBuild(build, capability.SourceMetrics); ok {
		b.rel = r.String()
	} else {
		b.rel = ""
	}
}

func (b *capBackend) hit(path string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hits[path]
}

func (b *capBackend) probeHits() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for path, c := range b.hits {
		if path != "/metrics" && path != "/health" {
			n += c
		}
	}
	return n
}

func (b *capBackend) serve(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.hits[r.URL.Path]++
	down, text, rel, ov := b.down, b.text, b.rel, b.override[r.URL.Path]
	b.mu.Unlock()
	switch {
	case down:
		w.WriteHeader(http.StatusServiceUnavailable)
	case r.URL.Path == "/health":
		_, _ = w.Write([]byte("ok"))
	case r.URL.Path == "/metrics":
		_, _ = w.Write([]byte(text))
	case ov[0] != nil:
		w.WriteHeader(ov[0].(int))
		_, _ = w.Write([]byte(ov[1].(string)))
	default:
		for _, g := range capability.Gates() {
			pr := g.Probe
			if pr == nil || r.URL.Path != pr.Path || r.URL.Query().Get("query") != pr.Query {
				continue
			}
			rl := capability.ParseRelease(rel)
			if !rl.Known {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			verdict := capability.VerdictOff
			if g.In(rl) {
				verdict = capability.VerdictOn
			}
			if verdict == pr.Accepted {
				_, _ = w.Write([]byte(`{"status":"success"}`))
			} else {
				w.WriteHeader(pr.RefusalStatus[0])
				_, _ = w.Write([]byte(pr.RefusalText[0]))
			}
			return
		}
		_, _ = w.Write([]byte(`{"values":[]}`))
	}
}

func newCapProxy(t *testing.T, b *capBackend, mutate func(*Config)) *Proxy {
	t.Helper()
	p := newCapProxyUnmanaged(t, b, mutate)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// newCapProxyUnmanaged builds a proxy the test shuts down itself.
func newCapProxyUnmanaged(t *testing.T, b *capBackend, mutate func(*Config)) *Proxy {
	t.Helper()
	cfg := Config{BackendURL: b.srv.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error", BackendVersionCheckTimeout: time.Second, DebugVLCallsHeader: true}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func captureLog(p *Proxy) *bytes.Buffer {
	buf := &bytes.Buffer{}
	p.log = slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return buf
}

func gateOn(p *Proxy, id capability.GateID) bool { return p.caps.Backend().On(id) }

// The startup check reads the release from the backend, runs the gates' probes and agrees with them; one info line
// says the set changed.
//
// conformance: vl-capability-layer
func TestCapabilityStartupReadsReleaseAndProbes(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, nil)
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	set := p.caps.Backend()
	if set.Release().String() != "v1.54.0" || !set.On(capability.PipeCoalesce) || !set.On(capability.ExtraFiltersLen) || !set.On(capability.MathQuotedConstantClone) || set.Epoch() == 0 {
		t.Fatalf("gates: %s", set.HeaderValue())
	}
	for _, g := range capability.Gates() {
		if g.Probe != nil && set.Why(g.ID) != capability.ReasonBoth {
			t.Errorf("%s: the probe must agree with the release, got %s", g.Name, set.Why(g.ID))
		}
	}
	if b.hit("/select/logsql/query") != 1 || b.hit("/select/logsql/stats_query") != 1 || b.hit("/select/logsql/stats_query_range") != 1 || b.hit("/select/logsql/stream_field_names") != 1 {
		t.Fatalf("one probe per gate: %v", b.hits)
	}
	// One info line per change of the set, naming the releases, the gates and the epoch.
	changes := 0
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) != nil || rec["msg"] != "capability set changed" {
			continue
		}
		changes++
		if rec["level"] != "INFO" || rec["capability.release.old"] != "unknown" || rec["capability.release.new"] != "v1.54.0" || rec["capability.digest"] != set.Digest() || rec["capability.epoch"] == nil {
			t.Fatalf("log line %s", line)
		}
		if on, _ := rec["capability.gates_on"].([]any); len(on) == 0 {
			t.Fatalf("flipped gates missing: %s", line)
		}
	}
	if changes != 1 {
		t.Fatalf("%d change lines, want 1:\n%s", changes, logs.String())
	}
	// The drilldown-limits answer keeps its fields.
	raw, semver, profile := p.backendVersionState()
	if raw != buildV154 || semver != "v1.54.0" || profile != "vl-v1.50-plus" {
		t.Fatalf("%q %q %q", raw, semver, profile)
	}
}

// vmauth in front of VictoriaLogs answers /metrics with its own version (VictoriaMetrics v1.155.0): not a release
// of VictoriaLogs, so nothing is learned from it and the gates hold the floor.
//
// conformance: vl-capability-layer
func TestCapabilityIgnoresOtherProductVersions(t *testing.T) {
	b := newCapBackend(t, buildVMAuth)
	p := newCapProxy(t, b, func(c *Config) { c.BackendFeatureProbesDisabled = true })
	p.probeBackendVersionFromMetrics(context.Background())
	p.observeBackendVersionFromHeaders(http.Header{"Server": {"vmauth/v1.155.0"}, "X-App-Version": {"v1.155.0"}, "X-Victoriametrics-Build": {buildVMAuth}})
	for _, raw := range []string{"vlagent-20261113-102924-tags-v1.54.0-0-g6433dae586", "victoria-metrics-20260929-tags-v1.155.0-0-gabc", "v1.54.0"} {
		p.observeBackendVersionFromHeaders(http.Header{"Server": {raw}})
	}
	if p.caps.Backend().Release().Known || gateOn(p, capability.PipeCoalesce) || p.caps.Backend().Epoch() != 0 {
		t.Fatalf("a version of another product counted: %s", p.caps.Backend().HeaderValue())
	}
	// The real VictoriaLogs behind it, read from its own metrics.
	b.setBuild(buildV154)
	p.probeBackendVersionFromMetrics(context.Background())
	if p.caps.Backend().Release().String() != "v1.54.0" {
		t.Fatal("VictoriaLogs build not read")
	}
}

// No release readable (no /metrics route behind vmauth): the probes decide the gates they cover, the others hold the
// floor, and an undecided gate predicts nothing.
//
// conformance: vl-capability-layer
func TestCapabilityUnknownReleaseProbesDecide(t *testing.T) {
	b := newCapBackend(t, buildVMAuth) // /metrics names no VictoriaLogs
	b.mu.Lock()
	b.rel = "v1.52.0" // behaves as v1.52.0: coalesce and limit-after-stats present, range _time writers accepted
	b.mu.Unlock()
	p := newCapProxy(t, b, nil)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	set := p.caps.Backend()
	if set.Release().Known || !set.On(capability.PipeCoalesce) || !set.Decided(capability.PipeCoalesce) || set.Why(capability.PipeCoalesce) != capability.ReasonProbe ||
		!set.On(capability.StatsLimitAfterStats) || set.On(capability.RangeTimeWritersRefused) || !set.Decided(capability.RangeTimeWritersRefused) {
		t.Fatalf("probes decide: %s", set.HeaderValue())
	}
	// A backend that answers coalesce is v1.51 or later: the v1.50 capabilities follow (the stack's vmauth variant
	// keeps dense pattern windowing and column-indexed field values), the guard and the v1.54 behaviour do not.
	if !set.On(capability.DensePatternWindowing) || !set.On(capability.ColumnFieldValues) || set.Why(capability.DensePatternWindowing) != capability.ReasonImplied ||
		set.Decided(capability.ExtraFiltersLen) || set.On(capability.MathQuotedConstantClone) || set.On(capability.PipeJSONArrayConcat) {
		t.Fatalf("implied and undecided gates: %s", set.HeaderValue())
	}
	if p.logsqlCapabilities().PipeCoalesce != true {
		t.Fatal("the translator reads the probed gate")
	}
}

// A release that says the gate is on while the probe observes it refused: the behaviour wins and the mismatch is
// logged once and counted.
//
// conformance: vl-capability-layer
func TestCapabilityMismatchReported(t *testing.T) {
	b := newCapBackend(t, buildV154)
	b.mu.Lock()
	b.override = map[string][2]any{"/select/logsql/query": {400, `cannot parse query: unexpected pipe "coalesce"`}}
	b.mu.Unlock()
	p := newCapProxy(t, b, nil)
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	set := p.caps.Backend()
	if set.On(capability.PipeCoalesce) || set.Why(capability.PipeCoalesce) != capability.ReasonMismatch || p.logsqlCapabilities().PipeCoalesce {
		t.Fatalf("the observed behaviour must win: %s", set.HeaderValue())
	}
	if n := strings.Count(logs.String(), `"msg":"capability mismatch"`); n != 1 {
		t.Fatalf("%d mismatch lines:\n%s", n, logs.String())
	}
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`loki_vl_proxy_capability_mismatch_total{gate="vl.pipe_coalesce"} 1`,
		`loki_vl_proxy_capability_gate{gate="vl.pipe_coalesce",side="victorialogs",source="mismatch"} 0`,
		`loki_vl_proxy_backend_feature_probe_total{gate="vl.pipe_coalesce",verdict="off"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metric missing: %s", want)
		}
	}
}

// With -backend-feature-probes=false no probe leaves the proxy: the gates follow the release, and an unreadable one
// holds the floor.
//
// conformance: vl-capability-layer
func TestCapabilityProbesDisabledHoldTheFloor(t *testing.T) {
	b := newCapBackend(t, buildVMAuth)
	p := newCapProxy(t, b, func(c *Config) { c.BackendFeatureProbesDisabled = true })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.runCapabilityProbes(context.Background(), true)
	if b.probeHits() != 0 {
		t.Fatalf("probes sent while disabled: %v", b.hits)
	}
	set := p.caps.Backend()
	if set.Release().Known || gateOn(p, capability.PipeCoalesce) || gateOn(p, capability.DensePatternWindowing) || !gateOn(p, capability.StreamMetadataEndpoints) || set.On(capability.MathQuotedConstantClone) {
		t.Fatalf("floor expected: %s", set.HeaderValue())
	}
}

// An outage during a re-probe changes no gate, no digest and no epoch, however long it lasts, and the probes bypass
// the circuit breaker and are never counted as upstream requests.
//
// conformance: vl-capability-layer
func TestCapabilityOutageDuringReprobeChangesNothing(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, func(c *Config) { c.CBFailThreshold = 2 })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := p.caps.Backend()
	upstream := upstreamRequests(p)
	b.mu.Lock()
	b.down = true
	b.mu.Unlock()
	for i := 0; i < 20; i++ {
		p.runCapabilityProbes(context.Background(), true)
	}
	b.mu.Lock()
	b.down = false
	b.mu.Unlock()
	b.srv.CloseClientConnections()
	after := p.caps.Backend()
	if after.Digest() != before.Digest() || after.Epoch() != before.Epoch() || after.Release().String() != "v1.54.0" || !after.On(capability.PipeCoalesce) {
		t.Fatalf("an outage moved the set: %s -> %s", before.HeaderValue(), after.HeaderValue())
	}
	if p.breaker.State() != "closed" {
		t.Fatalf("probe failures reached the circuit breaker: %s", p.breaker.State())
	}
	if got := upstreamRequests(p); got != upstream {
		t.Fatalf("probes counted as upstream requests: %d -> %d", upstream, got)
	}
	// A transport failure (the backend is gone) is no verdict either.
	b.srv.Close()
	p.runCapabilityProbes(context.Background(), true)
	if p.caps.Backend().Digest() != before.Digest() {
		t.Fatal("a refused connection moved the digest")
	}
}

// An upgrade and a rollback of VictoriaLogs flip the gates and the digest without a restart; a flight key or a peer's
// held refusal built under the old digest is not read under the new one.
//
// conformance: vl-capability-layer
func TestCapabilityUpgradeRollbackMoveKeysAndPeerEntries(t *testing.T) {
	b := newCapBackend(t, buildV153)
	p := newCapProxy(t, b, nil)
	clock := time.Now()
	p.caps = capability.New(capability.Options{Now: func() time.Time { return clock }})
	p.caps.SetWindow(10 * time.Minute)
	p.caps.Subscribe(p.onCapabilityChange)
	p.runCapabilityProbes(context.Background(), true)
	old := p.caps.Backend()
	oldPrefix := p.flightPrefix()
	if old.Release().String() != "v1.53.0" || old.On(capability.ExtraFiltersLen) || !strings.Contains(oldPrefix, old.Digest()) {
		t.Fatalf("v1.53.0: %s %q", old.HeaderValue(), oldPrefix)
	}

	// Upgrade: the 1.53 member is gone, the backend now reads v1.54.0 and the old release ages out of the window.
	b.setBuild(buildV154)
	clock = clock.Add(11 * time.Minute)
	p.runCapabilityProbes(context.Background(), true)
	up := p.caps.Backend()
	if up.Release().String() != "v1.54.0" || !up.On(capability.ExtraFiltersLen) || up.Digest() == old.Digest() || up.Epoch() <= old.Epoch() {
		t.Fatalf("upgrade: %s", up.HeaderValue())
	}
	if p.flightPrefix() == oldPrefix || !strings.Contains(p.flightPrefix(), up.Digest()) {
		t.Fatalf("the flight key prefix must follow the digest: %q -> %q", oldPrefix, p.flightPrefix())
	}
	// A peer's held refusal built under the old digest is ignored; one under the new digest is held.
	pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "self:1", DiscoveryType: "static", StaticPeers: "self:1,other:1"})
	t.Cleanup(pc.Close)
	p.peerCache, p.errorCachePeerSync, p.errorCacheRefusalTTL = pc, true, time.Minute
	p.errorFlights = cache.NewErrorFlights(16, 1<<20, 1<<16, false)
	send := func(digest string) int64 {
		before := p.peerSyncCounts[2].Load()
		msg, _ := json.Marshal(refusalSync{Build: p.buildVersion, Release: digest, Scope: p.routingScope(), Endpoint: "query_range", Key: "k",
			Expires: time.Now().Add(time.Minute).UnixMilli(), Status: http.StatusInternalServerError, Body: []byte("x")})
		p.receiveRefusal(refusalSyncKeyPrefix+"x", msg, time.Minute)
		return p.peerSyncCounts[2].Load() - before
	}
	if send(old.Digest()) != 0 {
		t.Fatal("a refusal held under the old digest was taken")
	}
	if send(up.Digest()) != 1 {
		t.Fatal("a refusal under the current digest must be held")
	}

	// Rollback: the 1.53 release answers again and is the lowest at once.
	b.setBuild(buildV153)
	p.runCapabilityProbes(context.Background(), true)
	down := p.caps.Backend()
	if down.Release().String() != "v1.53.0" || down.On(capability.ExtraFiltersLen) || down.Digest() != old.Digest() || down.Epoch() <= up.Epoch() {
		t.Fatalf("rollback: %s", down.HeaderValue())
	}
}

// Two backends of a pool on different releases: the lowest decides, and the digest does not flap while both answer.
//
// conformance: vl-capability-layer
func TestCapabilityMixedPoolDoesNotFlap(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, func(c *Config) { c.BackendFeatureProbesDisabled = true })
	for i := 0; i < 100; i++ {
		p.caps.ObserveRaw("pool-a", buildV154, capability.SourceHeader)
		p.caps.ObserveRaw("pool-b", buildV152, capability.SourceHeader)
	}
	set := p.caps.Backend()
	if set.Release().String() != "v1.52.2-enterprise" || set.On(capability.ExtraFiltersLen) || set.On(capability.MathQuotedConstantClone) || set.Epoch() != 2 {
		t.Fatalf("lowest in the window: %s", set.HeaderValue())
	}
}

// GET /admin/capabilities is protected like the other admin routes and shows the releases and the gates.
//
// conformance: vl-capability-layer, instrumentation-reveals-no-secrets
func TestAdminCapabilitiesRouteAuthAndShape(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, func(c *Config) { c.AdminAuthToken = "s3cret" })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	get := func(method string, mutate func(*http.Request)) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/admin/capabilities", nil)
		if mutate != nil {
			mutate(req)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	for name, mutate := range map[string]func(*http.Request){
		"no token":    nil,
		"wrong token": func(r *http.Request) { r.Header.Set("X-Admin-Token", "nope") },
		"wrong bearer": func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer nope")
		},
	} {
		if rec := get(http.MethodGet, mutate); rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), "v1.54.0") {
			t.Errorf("%s: %d %q", name, rec.Code, rec.Body.String())
		}
	}
	for name, mutate := range map[string]func(*http.Request){
		"token":  func(r *http.Request) { r.Header.Set("X-Admin-Token", "s3cret") },
		"bearer": func(r *http.Request) { r.Header.Set("Authorization", "Bearer s3cret") },
	} {
		rec := get(http.MethodGet, mutate)
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("X-Content-Type-Options") == "" {
			t.Fatalf("%s: %d %v", name, rec.Code, rec.Header())
		}
		var st capability.Status
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
			t.Fatal(err)
		}
		if st.Effective != "v1.54.0" || !st.Known || st.Epoch == 0 || len(st.Gates) != len(capability.Gates()) || len(st.Releases) != 1 || !st.Probing || len(st.Mismatches) != 0 ||
			st.Releases[0].Raw != buildV154 || st.Releases[0].Source != "metrics" {
			t.Fatalf("status %+v", st)
		}
		if strings.Contains(rec.Body.String(), "s3cret") || strings.Contains(rec.Body.String(), b.srv.URL) {
			t.Fatal("the status shows a secret or the backend URL")
		}
	}
	if rec := get(http.MethodPost, func(r *http.Request) { r.Header.Set("X-Admin-Token", "s3cret") }); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %d", rec.Code)
	}
	if rec := get(http.MethodHead, func(r *http.Request) { r.Header.Set("X-Admin-Token", "s3cret") }); rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("HEAD: %d %q", rec.Code, rec.Body.String())
	}
	// Not registered with instrumentation off, like /admin/cache/flush.
	off := false
	p2 := newCapProxy(t, b, func(c *Config) { c.RegisterInstrumentation = &off })
	mux2 := http.NewServeMux()
	p2.RegisterRoutes(mux2)
	rec := httptest.NewRecorder()
	mux2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/capabilities", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("instrumentation off: %d", rec.Code)
	}
}

// The debug header carries the epoch, digest and the gates that are on, only with -debug-vl-calls-header.
//
// conformance: vl-capability-layer
func TestCapabilityDebugHeader(t *testing.T) {
	b := newCapBackend(t, buildV154)
	for _, debug := range []bool{true, false} {
		p := newCapProxy(t, b, func(c *Config) { c.DebugVLCallsHeader = debug })
		p.probeBackendVersionFromMetrics(context.Background())
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
		got := rec.Header().Get(capabilitiesHeader)
		set := p.caps.Backend()
		if !debug {
			if got != "" {
				t.Fatalf("header without the flag: %q", got)
			}
			continue
		}
		if !strings.Contains(got, "digest="+set.Digest()) || !strings.Contains(got, "vl.pipe_coalesce") || !strings.Contains(got, "release=v1.54.0") {
			t.Fatalf("header %q", got)
		}
	}
}

// The background loop re-probes on its interval and stops when the proxy shuts down: no goroutine outlives it.
//
// conformance: vl-capability-layer
func TestCapabilityLoopStopsWithTheProxy(t *testing.T) {
	old := minCapabilityLoopInterval
	minCapabilityLoopInterval = 5 * time.Millisecond
	defer func() { minCapabilityLoopInterval = old }()
	b := newCapBackend(t, buildV154)
	p := newCapProxyUnmanaged(t, b, func(c *Config) { c.BackendVersionReprobeInterval = 10 * time.Millisecond })
	p.StartBackendCapabilityLoop()
	p.StartBackendCapabilityLoop() // one loop
	deadline := time.Now().Add(5 * time.Second)
	for b.hit("/metrics") < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if b.hit("/metrics") < 3 || p.caps.Backend().Release().String() != "v1.54.0" {
		t.Fatalf("the loop did not re-probe: %v", b.hits)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	hits := b.hit("/metrics")
	time.Sleep(100 * time.Millisecond)
	if after := b.hit("/metrics"); after != hits {
		t.Fatalf("the loop kept probing after shutdown: %d -> %d", hits, after)
	}
	// Nothing starts after shutdown either.
	p.StartBackendCapabilityLoop()
	p.startVersionProbe()
	time.Sleep(50 * time.Millisecond)
	if after := b.hit("/metrics"); after != hits {
		t.Fatalf("a probe started after shutdown: %d -> %d", hits, after)
	}
	// A zero interval starts nothing.
	p0 := newCapProxy(t, b, nil)
	p0.StartBackendCapabilityLoop()
	if p0.capLoopStarted.Load() {
		t.Fatal("interval 0 starts no loop")
	}
}

// A request that finds the release unknown triggers one background /metrics read per five minutes, never feature
// probes; it stops with the proxy.
func TestCapabilityUnknownReleaseTriggersOneVersionProbe(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, nil)
	p.backendVersionProbedAt.Store(time.Now().Add(-backendVersionReprobeInterval).UnixNano())
	for i := 0; i < 20; i++ {
		p.observeBackendVersionFromHeaders(http.Header{"X-Other": {"1"}})
	}
	deadline := time.Now().Add(3 * time.Second)
	for p.caps.Backend().Release().String() != "v1.54.0" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if p.caps.Backend().Release().String() != "v1.54.0" || b.hit("/metrics") != 1 || b.probeHits() != 0 {
		t.Fatalf("hits %v", b.hits)
	}
}

// The client request metric is bounded: distinct versions beyond the cap are counted as other.
//
// conformance: vl-capability-layer
func TestCapabilityClientMetricCardinalityCap(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, nil)
	for i := 0; i < 3000; i++ {
		p.caps.ObserveClient(clientprofile.Profile{Surface: clientprofile.SurfaceExplore, Grafana: clientprofile.Version{Major: uint16(i), Known: true}, Datasource: clientprofile.Version{Minor: uint16(i), Known: true}})
	}
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	series := 0
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "loki_vl_proxy_client_profile_requests_total{") {
			series++
		}
	}
	if series == 0 || series > capability.MaxClientSeries || !strings.Contains(rec.Body.String(), `grafana_version="other"`) {
		t.Fatalf("%d client series", series)
	}
	// Every other capability family has a fixed, small label space.
	count := func(prefix string) (n int) {
		for _, line := range strings.Split(rec.Body.String(), "\n") {
			if strings.HasPrefix(line, prefix+"{") || strings.HasPrefix(line, prefix+" ") {
				n++
			}
		}
		return
	}
	if count("loki_vl_proxy_capability_gate") != len(capability.Gates()) || count("loki_vl_proxy_backend_feature_probe_total") != 3*len(capability.Gates()) || count("loki_vl_proxy_capability_epoch") != 1 {
		t.Fatal("capability metric families")
	}
}

// upstreamRequests is the sum of the proxy's upstream request counters (/metrics).
func upstreamRequests(p *Proxy) (n int) {
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "loki_vl_proxy_requests_total{") && strings.Contains(line, `direction="upstream"`) {
			var v int
			fmt.Sscanf(line[strings.LastIndexByte(line, ' ')+1:], "%d", &v)
			n += v
		}
	}
	return n
}

// A request counts under its typed client profile (surface, Grafana, Loki datasource and Drilldown versions) in the
// capability layer's client metric, apart from the client-identity metric of the same family prefix.
//
// conformance: vl-capability-layer
func TestCapabilityClientProfileMetricFromRequests(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
		req.Header.Set("User-Agent", "Grafana/13.3.1 (linux; amd64) loki/13.2.5")
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`loki_vl_proxy_client_profile_requests_total{surface="drilldown",grafana_version="13.3.1",datasource_version="13.2.5",drilldown_version="unknown"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s in\n%s", want, body)
		}
	}
	if !strings.Contains(body, `loki_vl_proxy_client_profile_requests_total{surface="api"`) && !strings.Contains(body, `loki_vl_proxy_client_profile_requests_total{surface="unknown"`) {
		t.Error("a request without a Grafana User-Agent has a profile too")
	}
}

// Starting the loop and the version probe while the proxy shuts down never joins a wait group that is being waited
// on (run with -race), and nothing starts once Shutdown has begun.
//
// conformance: vl-capability-layer
func TestCapabilityStartVersusShutdownRace(t *testing.T) {
	old := minCapabilityLoopInterval
	minCapabilityLoopInterval = time.Millisecond
	defer func() { minCapabilityLoopInterval = old }()
	b := newCapBackend(t, buildV154)
	for round := 0; round < 20; round++ {
		p := newCapProxyUnmanaged(t, b, func(c *Config) { c.BackendVersionReprobeInterval = time.Millisecond })
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.StartBackendCapabilityLoop()
				p.startVersionProbe()
			}()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		wg.Wait()
		p.startVersionProbe()
		p.StartBackendCapabilityLoop()
	}
}

// The probe of the stream metadata endpoints turns the gate off only on VictoriaLogs' own refusal of an unknown path:
// a 404 of an ingress or vmauth in front of it is no verdict.
//
// conformance: vl-capability-layer
func TestCapabilityStreamProbeNeedsVictoriaLogsOwnRefusal(t *testing.T) {
	const path = "/select/logsql/stream_field_names"
	for _, c := range []struct {
		name   string
		status int
		body   string
		on     bool
		decide bool
	}{
		{"ingress 404", 404, "404 page not found", true, false},
		{"vmauth 400", 400, "missing route for \"/select/logsql/stream_field_names\"", true, false},
		{"vmauth 401", 401, "unauthorized", true, false},
		{"VictoriaLogs without the endpoint", 400, `unsupported path requested: "/select/logsql/stream_field_names"`, false, true},
		{"VictoriaLogs with it", 200, `{"values":[]}`, true, true},
	} {
		b := newCapBackend(t, buildVMAuth)
		b.mu.Lock()
		b.override = map[string][2]any{path: {c.status, c.body}}
		b.mu.Unlock()
		p := newCapProxy(t, b, nil)
		p.probeBackendFeatures(context.Background())
		set := p.caps.Backend()
		if set.On(capability.StreamMetadataEndpoints) != c.on || set.Decided(capability.StreamMetadataEndpoints) != c.decide {
			t.Errorf("%s: on=%v decided=%v", c.name, set.On(capability.StreamMetadataEndpoints), set.Decided(capability.StreamMetadataEndpoints))
		}
	}
}

// A pattern answer, its window cache entry and the pattern state are keyed by the dense windowing gate, which can
// flip while the proxy runs: one built under one gate set is not read under another.
//
// conformance: vl-capability-layer
func TestPatternKeysCarryTheDenseWindowingGate(t *testing.T) {
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, nil)
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?query=%7Bapp%3D%22a%22%7D&start=1&end=2", nil)
	keys := func() [4]string {
		return [4]string{
			p.patternsAutodetectCacheKey("t", "", `{app="a"}`, "1", "2", "15"),
			p.patternWindowCacheKey(req, `{app="a"}`, "15", 10, 100, 1, 2),
			p.patternStateTenantKey(req),
			p.patternsKeyPrefix() + "t:" + p.detectedLevelCacheKey() + ":" + p.patternGateKey(),
		}
	}
	p.setReleaseForTest("v1.49.0")
	old := keys()
	p.setReleaseForTest("v1.54.0")
	up := keys()
	p.setReleaseForTest("v1.49.0")
	back := keys()
	for i := range old {
		if old[i] == up[i] {
			t.Errorf("key %d is the same under v1.49.0 and v1.54.0: %q", i, old[i])
		}
		if old[i] != back[i] {
			t.Errorf("key %d moved without a gate change: %q vs %q", i, old[i], back[i])
		}
	}
}

// A backend that goes to v1.49.0 and back to v1.54.0, three times, with the window the proxy derives from a 10s
// re-probe interval: the digest follows the lowest release down at once and returns to the first value once the
// lower release has not been seen for the window, each change is one `capability set changed` line, and the
// version metric lists only the releases that count.
//
// conformance: vl-capability-layer
func TestCapabilityUpgradeRollbackCyclesThroughTheProxy(t *testing.T) {
	const interval = 10 * time.Second
	b := newCapBackend(t, buildV154)
	p := newCapProxy(t, b, func(c *Config) { c.BackendVersionReprobeInterval = interval })
	logs := captureLog(p)
	clock := time.Now()
	p.caps = capability.New(capability.Options{Now: func() time.Time { return clock }})
	p.caps.SetWindow(capabilityWindowIntervals*p.backendReprobeInterval + p.probeRoundTimeout())
	var digestChanges int
	p.caps.Subscribe(func(capability.Set, capability.Set) { digestChanges++ })
	p.caps.Subscribe(p.onCapabilityChange)
	round := func(build string) {
		b.setBuild(build)
		clock = clock.Add(interval)
		p.runCapabilityProbes(context.Background(), true)
	}
	versions := func() []string {
		rec := httptest.NewRecorder()
		p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		var out []string
		for _, line := range strings.Split(rec.Body.String(), "\n") {
			if strings.HasPrefix(line, "loki_vl_proxy_backend_version_info{") {
				out = append(out, line[strings.Index(line, `version="`)+9:strings.Index(line, `",flavour`)])
			}
		}
		return out
	}
	for i := 0; i < 3; i++ {
		round(buildV154)
	}
	d54 := p.caps.Backend().Digest()
	for cycle := 0; cycle < 3; cycle++ {
		before := digestChanges
		round(buildV149)
		if p.caps.Backend().Release().String() != "v1.49.0" || p.caps.Backend().Digest() == d54 || digestChanges != before+1 {
			t.Fatalf("cycle %d: down: %s, %d changes", cycle, p.caps.Backend().Release(), digestChanges-before)
		}
		rounds := 0
		for p.caps.Backend().Digest() != d54 {
			round(buildV154)
			if rounds++; rounds > 13 {
				t.Fatalf("cycle %d: the digest did not return after %d rounds (%v counted)", cycle, rounds, versions())
			}
		}
		if got := versions(); len(got) != 1 || got[0] != "v1.54.0" {
			t.Fatalf("cycle %d: the version metric must list the releases that count only, got %v", cycle, got)
		}
		if digestChanges != before+2 {
			t.Fatalf("cycle %d: %d changes, want 2 (down, up)", cycle, digestChanges-before)
		}
	}
	if n := strings.Count(logs.String(), `"msg":"capability set changed"`); n != digestChanges {
		t.Fatalf("%d change lines for %d changes:\n%s", n, digestChanges, logs.String())
	}
}

// A startup round that moves several gates is one change, one line: the probes of a round are applied together.
//
// conformance: vl-capability-layer
func TestCapabilityProbeRoundIsOneChange(t *testing.T) {
	b := newCapBackend(t, buildVMAuth)
	b.mu.Lock()
	b.rel = "v1.52.0"
	b.mu.Unlock()
	p := newCapProxy(t, b, nil)
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logs.String(), `"msg":"capability set changed"`); n != 1 || p.caps.Backend().Epoch() != 1 {
		t.Fatalf("%d lines, epoch %d, want one change:\n%s", n, p.caps.Backend().Epoch(), logs.String())
	}
}
