package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// flightStub is a VictoriaLogs stand-in: every select call waits for the gate
// (when set), then answers with answer(call number, request).
type flightStub struct {
	calls  atomic.Int64
	gate   chan struct{}
	answer func(n int64, w http.ResponseWriter, r *http.Request)
	srv    *httptest.Server
	tenant sync.Map // AccountID -> *atomic.Int64
}

func newFlightStub(t *testing.T, answer func(n int64, w http.ResponseWriter, r *http.Request)) *flightStub {
	t.Helper()
	s := &flightStub{answer: answer}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/select/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		n := s.calls.Add(1)
		c, _ := s.tenant.LoadOrStore(r.Header.Get("AccountID"), new(atomic.Int64))
		c.(*atomic.Int64).Add(1)
		if g := s.gate; g != nil {
			select {
			case <-g:
			case <-r.Context().Done():
				return
			}
		}
		s.answer(n, w, r)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *flightStub) tenantCalls(account string) int64 {
	c, ok := s.tenant.Load(account)
	if !ok {
		return 0
	}
	return c.(*atomic.Int64).Load()
}

func answer500(_ int64, w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "storage node unavailable", http.StatusInternalServerError)
}

func answerLogs(_ int64, w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
}

var flightTestErrorCache = ErrorCacheConfig{TransientTTL: time.Second, DataTTL: 5 * time.Second}

func newFlightProxy(t *testing.T, backend string, mutate func(*Config)) (*Proxy, http.Handler) {
	t.Helper()
	cfg := Config{
		BackendURL:         backend,
		Cache:              cache.New(60*time.Second, 1000),
		CompatCache:        cache.New(60*time.Second, 1000),
		LogLevel:           "error",
		ErrorCache:         flightTestErrorCache,
		DebugVLCallsHeader: true,
		TenantMap: map[string]TenantMapping{
			"team-a": {AccountID: "10", ProjectID: "0"},
			"team-b": {AccountID: "20", ProjectID: "0"},
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

const flightLogTarget = "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1700000000&end=1700003600&limit=100"

func flightGet(h http.Handler, ctx context.Context, target, tenant string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	req.Header.Set("X-Scope-OrgID", tenant)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// burst sends n identical requests at once and returns their answers once the
// stub saw the first call and n-1 requests wait on it.
func burst(t *testing.T, p *Proxy, stub *flightStub, n int, send func() *httptest.ResponseRecorder) []*httptest.ResponseRecorder {
	t.Helper()
	stub.gate = make(chan struct{})
	out := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i] = send()
		}(i)
	}
	waitFor(t, func() bool { return stub.calls.Load() >= 1 && p.errorFlights.Waiting() >= int64(n-1) })
	close(stub.gate)
	wg.Wait()
	stub.gate = nil
	return out
}

// answerData is a success body without its stats, which every request gets
// for itself.
func answerData(a *httptest.ResponseRecorder) string {
	body, _, _ := strings.Cut(a.Body.String(), `,"stats":`)
	return body
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

// N identical requests whose VictoriaLogs call fails send one call; every one
// gets the leader's answer byte for byte, the same answer a fresh request
// gets from a proxy without flights, and the followers report no backend call.
//
// conformance: semantics/error-coalescing-request-flights, semantics/error-cache-byte-equal-answers
func TestErrorFlights_ConcurrentFailuresShareOneCall(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	const n = 20
	answers := burst(t, p, stub, n, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), flightLogTarget, "team-a")
	})
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("VictoriaLogs calls = %d, want 1 for %d identical failing requests", got, n)
	}
	plainStub := newFlightStub(t, answer500)
	_, plain := newFlightProxy(t, plainStub.srv.URL, func(c *Config) {
		c.CoalescerDisabled, c.ErrorCache = true, ErrorCacheConfig{Disabled: true}
	})
	fresh := flightGet(plain, context.Background(), flightLogTarget, "team-a")
	if fresh.Code != http.StatusInternalServerError {
		t.Fatalf("fresh answer %d", fresh.Code)
	}
	followers := 0
	for _, a := range answers {
		if a.Code != fresh.Code || !bytes.Equal(a.Body.Bytes(), fresh.Body.Bytes()) ||
			a.Header().Get("Content-Type") != fresh.Header().Get("Content-Type") ||
			a.Header().Get("X-Content-Type-Options") != fresh.Header().Get("X-Content-Type-Options") {
			t.Fatalf("shared answer %d %q %v differs from a fresh one %d %q %v", a.Code, a.Body.String(), a.Header(), fresh.Code, fresh.Body.String(), fresh.Header())
		}
		if a.Header().Get(vlBackendCallsHeader) == "0" {
			followers++
		}
	}
	if followers != n-1 {
		t.Fatalf("%d answers report no backend call, want the %d followers", followers, n-1)
	}
	st := p.errorFlightStats()
	if st.Coalesced[0] != n-1 || st.Stored[0] != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// A transient failure is held for its TTL: a repeat sends no call; once the
// TTL passed, the first request after VictoriaLogs recovered answers 200.
//
// conformance: semantics/error-cache-classes-and-ttl
func TestErrorFlights_TransientHeldThenRecovers(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			answer500(n, w, r)
			return
		}
		answerLogs(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	if a := flightGet(h, context.Background(), flightLogTarget, "team-a"); a.Code != http.StatusInternalServerError {
		t.Fatalf("first answer %d", a.Code)
	}
	calls := stub.calls.Load()
	failing.Store(false)
	repeat := flightGet(h, context.Background(), flightLogTarget, "team-a")
	if repeat.Code != http.StatusInternalServerError || stub.calls.Load() != calls {
		t.Fatalf("repeat within the TTL: %d with %d new calls, want the held 500 and none", repeat.Code, stub.calls.Load()-calls)
	}
	if repeat.Header().Get(vlCacheResultHeader) != "hit" || repeat.Header().Get(vlBackendCallsHeader) != "0" {
		t.Fatalf("held answer headers %v", repeat.Header())
	}
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	after := flightGet(h, context.Background(), flightLogTarget, "team-a")
	if after.Code != http.StatusOK || stub.calls.Load() == calls {
		t.Fatalf("after the TTL: %d, %d new calls; want a fresh 200", after.Code, stub.calls.Load()-calls)
	}
	if p.errorFlightStats().Held[0] != 1 {
		t.Fatalf("held hits %+v", p.errorFlightStats())
	}
}

// A leader that its client cancels hands nothing to the followers: the first
// follower whose request is alive asks VictoriaLogs itself, and the others get
// its answer, never the leader's cancellation.
//
// conformance: semantics/error-coalescing-no-inherited-cancel
func TestErrorFlights_CancelledLeaderFollowersRetry(t *testing.T) {
	leaderGate := make(chan struct{})
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			select {
			case <-leaderGate:
			case <-r.Context().Done():
				return
			}
		}
		answer500(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan *httptest.ResponseRecorder)
	go func() { leaderDone <- flightGet(h, leaderCtx, flightLogTarget, "team-a") }()
	waitFor(t, func() bool { return stub.calls.Load() == 1 })
	const followers = 8
	answers := make([]*httptest.ResponseRecorder, followers)
	var wg sync.WaitGroup
	for i := 0; i < followers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			answers[i] = flightGet(h, context.Background(), flightLogTarget, "team-a")
		}(i)
	}
	waitFor(t, func() bool { return p.errorFlights.Waiting() >= followers })
	cancelLeader()
	if a := <-leaderDone; a.Code != 499 {
		t.Fatalf("cancelled leader answered %d, want 499", a.Code)
	}
	wg.Wait()
	close(leaderGate)
	for _, a := range answers {
		if a.Code != http.StatusInternalServerError {
			t.Fatalf("follower answered %d %q, want the new leader's 500", a.Code, a.Body.String())
		}
	}
	if got := stub.calls.Load(); got != 2 {
		t.Fatalf("VictoriaLogs calls = %d, want 2 (the cancelled call and one for all followers)", got)
	}
}

// A failure held for one tenant is never served to another, and a
// configuration reload drops held failures.
//
// conformance: semantics/error-cache-tenant-isolation
func TestErrorFlights_TenantIsolationAndReload(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	if a := stub.tenantCalls("10"); a != 1 {
		t.Fatalf("team-a calls %d, want 1 (repeat held)", a)
	}
	flightGet(h, context.Background(), flightLogTarget, "team-b")
	if b := stub.tenantCalls("20"); b != 1 {
		t.Fatalf("team-b calls %d, want its own call", b)
	}
	p.ReloadTenantMap(map[string]TenantMapping{
		"team-a": {AccountID: "10", ProjectID: "0"},
		"team-b": {AccountID: "20", ProjectID: "0"},
	})
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	if a := stub.tenantCalls("10"); a != 2 {
		t.Fatalf("team-a calls after reload %d, want a new call", a)
	}
	p.ReloadFieldMappings(nil)
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	if a := stub.tenantCalls("10"); a != 3 {
		t.Fatalf("team-a calls after field mapping reload %d, want a new call", a)
	}
}

// N identical successful requests send one call: the followers read the
// leader's answer from the response cache.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_ConcurrentSuccessesShareOneCall(t *testing.T) {
	stub := newFlightStub(t, answerLogs)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	const n = 20
	answers := burst(t, p, stub, n, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), flightLogTarget, "team-a")
	})
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("VictoriaLogs calls = %d, want 1", got)
	}
	for _, a := range answers {
		if a.Code != http.StatusOK || answerData(a) != answerData(answers[0]) {
			t.Fatalf("answer %d %q", a.Code, a.Body.String())
		}
	}
	if p.errorFlightStats().Succeeded != n-1 {
		t.Fatalf("stats %+v", p.errorFlightStats())
	}
}

// POST queries are joined too: failures are shared and held, a complete
// success is handed to the followers in memory.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_PostQueries(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			answer500(n, w, r)
			return
		}
		answerLogs(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	form := url.Values{"query": {`{app="api"}`}, "start": {"1700000000"}, "end": {"1700003600"}, "limit": {"100"}}.Encode()
	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Scope-OrgID", "team-a")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	for _, a := range burst(t, p, stub, 10, post) {
		if a.Code != http.StatusInternalServerError {
			t.Fatalf("POST answer %d", a.Code)
		}
	}
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("failing POST calls = %d, want 1", got)
	}
	post()
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("held POST failure made %d calls", got-1)
	}
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	failing.Store(false)
	stub.calls.Store(0)
	answers := burst(t, p, stub, 10, post)
	for _, a := range answers {
		if a.Code != http.StatusOK || answerData(a) != answerData(answers[0]) {
			t.Fatalf("POST success %d %q", a.Code, a.Body.String())
		}
	}
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("successful POST calls = %d, want 1", got)
	}
}

// With -stream-response a failure decided before the first byte is joined
// and held; a streamed success lets the followers run on their own.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_StreamResponse(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			answer500(n, w, r)
			return
		}
		answerLogs(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.StreamResponse = true })
	get := func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), flightLogTarget, "team-a")
	}
	for _, a := range burst(t, p, stub, 10, get) {
		if a.Code != http.StatusInternalServerError {
			t.Fatalf("streamed failure %d", a.Code)
		}
	}
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("failing streamed calls = %d, want 1", got)
	}
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	failing.Store(false)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a := get(); a.Code != http.StatusOK {
				t.Errorf("streamed success %d", a.Code)
			}
		}()
	}
	wg.Wait()
}

// -coalescer-disabled turns the join off (every request runs) while held
// failures still answer repeats; with the error cache disabled followers
// still share the leader's failure but nothing is held.
//
// conformance: semantics/error-cache-operator-switches
func TestErrorFlights_Switches(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.CoalescerDisabled = true })
	stub.gate = make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); flightGet(h, context.Background(), flightLogTarget, "team-a") }()
	}
	waitFor(t, func() bool { return stub.calls.Load() == 5 })
	close(stub.gate)
	wg.Wait()
	stub.gate = nil
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	if got := stub.calls.Load(); got != 5 {
		t.Fatalf("without the join: %d calls, want 5 and a held repeat", got)
	}
	_ = p

	stub2 := newFlightStub(t, answer500)
	p2, h2 := newFlightProxy(t, stub2.srv.URL, func(c *Config) { c.ErrorCache.Disabled = true })
	burst(t, p2, stub2, 5, func() *httptest.ResponseRecorder {
		return flightGet(h2, context.Background(), flightLogTarget, "team-a")
	})
	flightGet(h2, context.Background(), flightLogTarget, "team-a")
	if got := stub2.calls.Load(); got != 2 {
		t.Fatalf("error cache off: %d calls, want 1 shared + 1 repeat", got)
	}
	if e, _ := p2.errorFlights.Len(); e != 0 {
		t.Fatal("error cache off must hold nothing")
	}

	_, h3 := newFlightProxy(t, stub2.srv.URL, func(c *Config) { c.CoalescerDisabled, c.ErrorCache.Disabled = true, true })
	if a := flightGet(h3, context.Background(), flightLogTarget, "team-a"); a.Code != http.StatusInternalServerError {
		t.Fatalf("both off: %d", a.Code)
	}
}

// The class of a failure comes from its error's type; the plan says which
// classes the request can have.
//
// conformance: semantics/error-cache-plan-policy, semantics/error-cache-classes-and-ttl
func TestErrorFlights_FailureClassOf(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	pipeline := &orderedJSONPipelineError{message: "pipeline error"}
	cases := []struct {
		name  string
		ctx   context.Context
		err   error
		typed *lokierr.Error
		class cache.ErrorClass
		need  queryplan.Failures
	}{
		{"client cancel", cancelled, context.Canceled, nil, cache.ErrorNever, 0},
		{"backend failed", context.Background(), nil, lokierr.BackendFailed(nil), cache.ErrorTransient, queryplan.FailBackend},
		{"series limit", context.Background(), nil, lokierr.SeriesLimit(5), cache.ErrorData, queryplan.FailLimit},
		{"no org id", context.Background(), nil, lokierr.NoOrgID(nil), cache.ErrorNever, 0},
		{"parse", context.Background(), nil, lokierr.Parse(1, 2, "syntax error"), cache.ErrorRequest, 0},
		{"query length", context.Background(), nil, lokierr.QueryLength(2*time.Hour, time.Hour), cache.ErrorRequest, 0},
		{"queue full", context.Background(), nil, lokierr.QueueFull(nil), cache.ErrorNever, 0},
		{"backend deadline, request alive", context.Background(), nil, lokierr.Deadline(nil), cache.ErrorNever, 0},
		{"backend failed by a timeout", context.Background(), nil, lokierr.BackendFailed(&url.Error{Op: "Post", URL: "x", Err: timeoutErr{}}), cache.ErrorNever, 0},
		{"backend failed by a refused connection", context.Background(), nil, lokierr.BackendFailed(&url.Error{Op: "Post", URL: "x", Err: errors.New("connection refused")}), cache.ErrorTransient, queryplan.FailBackend},
		{"vl query deadline", context.Background(), &upstreamStatusError{status: 503, class: vlErrorDeadline}, nil, cache.ErrorNever, 0},
		{"vl 504", context.Background(), &upstreamStatusError{status: 504}, nil, cache.ErrorNever, 0},
		{"expired deadline", context.Background(), fmt.Errorf("read: %w", context.DeadlineExceeded), nil, cache.ErrorNever, 0},
		{"client deadline", cancelled, nil, lokierr.Deadline(nil), cache.ErrorNever, 0},
		{"pipeline", context.Background(), pipeline, nil, cache.ErrorData, queryplan.FailPipeline},
		{"vl 503", context.Background(), &upstreamStatusError{status: 503}, nil, cache.ErrorTransient, queryplan.FailBackend},
		{"vl rejected", context.Background(), &upstreamStatusError{status: 400, class: vlErrorQueryRejected}, nil, cache.ErrorNever, 0},
		{"plan defect", context.Background(), &planDefectError{cause: &upstreamStatusError{status: 400, class: vlErrorQueryRejected}}, nil, cache.ErrorNever, 0},
		{"aborted body", context.Background(), &vlResponseAbortedError{}, nil, cache.ErrorNever, 0},
		{"transport timeout", context.Background(), &url.Error{Op: "Get", URL: "x", Err: &timeoutErr{}}, nil, cache.ErrorNever, 0},
		{"refused", context.Background(), &url.Error{Op: "Get", URL: "x", Err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}}, nil, cache.ErrorTransient, queryplan.FailBackend},
		{"canceled call", context.Background(), context.Canceled, nil, cache.ErrorNever, 0},
		{"untyped", context.Background(), io.ErrUnexpectedEOF, nil, cache.ErrorNever, 0},
	}
	for _, c := range cases {
		class, need := failureClassOf(c.ctx, c.err, c.typed)
		if class != c.class || need != c.need {
			t.Errorf("%s: class %s need %b, want %s %b", c.name, class, need, c.class, c.need)
		}
	}
}

// A request that answered failures of two classes, or whose final status is
// not the failure it recorded, holds nothing; a class outside the plan's
// reach is not held either.
//
// conformance: semantics/error-cache-plan-policy
func TestErrorFlights_HeldFailureClass(t *testing.T) {
	ctx := context.WithValue(context.Background(), requestTelemetryKey, newRequestTelemetry())
	if classOnly(heldFailureClass(context.Background(), 500, queryplan.FailBackend)) != cache.ErrorNever {
		t.Fatal("no telemetry")
	}
	if classOnly(heldFailureClass(ctx, 500, queryplan.FailBackend)) != cache.ErrorNever {
		t.Fatal("nothing recorded")
	}
	recordFlightFailure(ctx, nil, 500, lokierr.BackendFailed(nil))
	if classOnly(heldFailureClass(ctx, 500, queryplan.FailBackend)) != cache.ErrorTransient {
		t.Fatal("recorded transient")
	}
	if classOnly(heldFailureClass(ctx, 400, queryplan.FailBackend)) != cache.ErrorNever {
		t.Fatal("final status differs from the recorded failure")
	}
	if classOnly(heldFailureClass(ctx, 500, 0)) != cache.ErrorNever {
		t.Fatal("a plan that reads no data cannot fail in the backend")
	}
	recordFlightFailure(ctx, nil, 400, lokierr.SeriesLimit(5))
	if classOnly(heldFailureClass(ctx, 400, queryplan.FailBackend|queryplan.FailLimit)) != cache.ErrorNever {
		t.Fatal("two failures of different classes hold nothing")
	}
	recordFlightFailure(context.Background(), nil, 500, nil) // no telemetry: no-op
}

// The error flight families are exported on /metrics.
//
// conformance: semantics/error-cache-operator-switches
func TestErrorFlights_Metrics(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	rec := httptest.NewRecorder()
	p.GetMetrics().Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`loki_vl_proxy_error_cache_hits_total{class="transient"} 1`,
		`loki_vl_proxy_error_cache_stores_total{class="transient"} 1`,
		`loki_vl_proxy_error_coalesced_total{class="data"} 0`,
		`loki_vl_proxy_request_flight_saved_total 1`,
		`loki_vl_proxy_error_cache_entries 3`, // the request, the same request for every window, and its probe marker
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics lack %q", want)
		}
	}
	var s metrics.ErrorFlightStats
	if s.Saved() != 0 {
		t.Fatal("zero stats")
	}
}

// A failure read from the window's data (Loki's series limit) is held for the
// data TTL, not the transient one, and answered again once it expires.
//
// conformance: semantics/error-cache-classes-and-ttl, limits/series-limit-error
func TestErrorFlights_DataFailureHeldForDataTTL(t *testing.T) {
	stub := newFlightStub(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"app":"a1"},"values":[[1700000060,"1"]]},` +
			`{"metric":{"app":"a2"},"values":[[1700000060,"1"]]},` +
			`{"metric":{"app":"a3"},"values":[[1700000060,"1"]]}]}}`))
	})
	p, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.MaxStatsQuerySeries = 1 })
	target := "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum by (app) (count_over_time({app=~"a.+"}[1m]))`) + "&start=1700000000&end=1700003600&step=60"
	first := flightGet(h, context.Background(), target, "team-a")
	if first.Code != http.StatusBadRequest || !strings.Contains(first.Body.String(), "maximum number of series (1)") {
		t.Fatalf("first answer %d %q, want Loki's series limit", first.Code, first.Body.String())
	}
	calls := stub.calls.Load()
	restore := cache.AdvanceClockForTesting(2 * time.Second) // past the transient TTL
	defer restore()
	held := flightGet(h, context.Background(), target, "team-a")
	if held.Code != first.Code || held.Body.String() != first.Body.String() || stub.calls.Load() != calls {
		t.Fatalf("within the data TTL: %d with %d calls", held.Code, stub.calls.Load()-calls)
	}
	if p.errorFlightStats().Stored[1] != 1 {
		t.Fatalf("stats %+v", p.errorFlightStats())
	}
	restore2 := cache.AdvanceClockForTesting(4 * time.Second)
	defer restore2()
	flightGet(h, context.Background(), target, "team-a")
	if stub.calls.Load() == calls {
		t.Fatal("after the data TTL the request must run again")
	}
	// A changed series policy must not reuse the held 400 or a cached 200
	// from the other policy. The endpoint and the query remain identical.
	p.maxStatsQuerySeries = 100
	complete := flightGet(h, context.Background(), target, "team-a")
	if complete.Code != http.StatusOK || !strings.Contains(complete.Body.String(), `"a3"`) {
		t.Fatalf("raised limit reused a held failure: %d %s", complete.Code, complete.Body)
	}
	calls = stub.calls.Load()
	if repeat := flightGet(h, context.Background(), target, "team-a"); repeat.Code != complete.Code || !bytes.Equal(stripQueryStats(repeat.Body.Bytes()), stripQueryStats(complete.Body.Bytes())) || stub.calls.Load() != calls {
		t.Fatal("the complete answer changed its data or read the backend again at the same limit")
	}
	p.maxStatsQuerySeries = 1
	limited := flightGet(h, context.Background(), target, "team-a")
	if limited.Code != first.Code || limited.Body.String() != first.Body.String() {
		t.Fatalf("lowered limit reused a complete answer: %d %s", limited.Code, limited.Body)
	}
}

// A success larger than the response cache keeps releases the waiting
// requests at its overflow: each runs on its own, none waits for a share that
// never comes.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_OversizedSuccessReleasesFollowers(t *testing.T) {
	line := `{"_time":"2023-11-14T22:13:20Z","_msg":"` + strings.Repeat("x", 4096) + `","_stream":"{app=\"api\"}","app":"api"}` + "\n"
	stub := newFlightStub(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(line))
	})
	p, h := newFlightProxy(t, stub.srv.URL, func(c *Config) {
		c.Cache = cache.NewWithMaxBytes(60*time.Second, 1000, 20000)
		c.CompatCache = cache.NewWithMaxBytes(60*time.Second, 1000, 20000)
	})
	const n = 6
	answers := burst(t, p, stub, n, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), flightLogTarget, "team-a")
	})
	for _, a := range answers {
		if a.Code != http.StatusOK {
			t.Fatalf("answer %d", a.Code)
		}
	}
	if got := stub.calls.Load(); got != n {
		t.Fatalf("calls %d, want %d: an answer no cache keeps runs per request", got, n)
	}
}

// failureReach reads the plan for queries, and claims a backend failure or a
// limit for every other read; the bypass key covers GET and form POST only.
//
// conformance: semantics/error-cache-plan-policy
func TestErrorFlights_ReachAndBypassKey(t *testing.T) {
	q := func(query string) *http.Request {
		return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query="+url.QueryEscape(query), nil)
	}
	if got := failureReach("query", q(`vector(1)`)); got != 0 {
		t.Fatalf("vector(1) reach %b", got)
	}
	if got := failureReach("query", q(`{app=`)); got != 0 {
		t.Fatalf("unparsable reach %b", got)
	}
	if got := failureReach("labels", q("")); got != queryplan.FailBackend|queryplan.FailLimit {
		t.Fatalf("labels reach %b", got)
	}
	p, _ := newFlightProxy(t, "http://127.0.0.1:1", nil)
	get := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	if p.uncachedFlightKey("tail", get).join != "" {
		t.Fatal("tail is not joined")
	}
	ws := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	ws.Header.Set("Upgrade", "websocket")
	if p.uncachedFlightKey("labels", ws).join != "" {
		t.Fatal("a websocket is not joined")
	}
	if p.uncachedFlightKey("labels", httptest.NewRequest(http.MethodDelete, "/loki/api/v1/labels", nil)).join != "" {
		t.Fatal("only reads are joined")
	}
	if p.uncachedFlightKey("labels", httptest.NewRequest(http.MethodPost, "/loki/api/v1/labels", nil)).join != "" {
		t.Fatal("a POST whose body was not read as a form is not joined")
	}
	post := httptest.NewRequest(http.MethodPost, "/loki/api/v1/labels?start=1", strings.NewReader("end=2"))
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_ = post.ParseForm()
	pk, gk := p.uncachedFlightKey("labels", post), p.uncachedFlightKey("labels", get)
	if pk.join == "" || gk.join == "" || pk.join == gk.join || !strings.HasPrefix(pk.join, p.flightPrefix()+"POST:") {
		t.Fatalf("pre-keys %q %q", pk.join, gk.join)
	}
	if pe, ge := pk.exactKey(), gk.exactKey(); pe == ge || !strings.HasPrefix(pe, p.flightPrefix()+"POST:compat:v2:labels:") || pe != pk.exactKey() {
		t.Fatalf("exact keys %q %q", pe, ge)
	}
	if exact := (&flightKey{join: "j"}); exact.exactKey() != "j" {
		t.Fatal("a join that is exact is its own exact key")
	}
	tap := (&flightLead{f: nil}).tap(httptest.NewRecorder(), 1)
	if u, ok := tap.(interface{ Unwrap() http.ResponseWriter }); !ok || u.Unwrap() == nil {
		t.Fatal("the tap unwraps to the writer below")
	}
	var none *flightLead
	none.release()
	none.markStored()
	none.observe(500, nil, false, false)
	if none.tap(nil, 0) != nil {
		t.Fatal("no lead, no tap")
	}
}

// A leader whose failure is never shared (the proxy's own queue answered it)
// lets its followers run at once: they wait about one leader's time, not one
// per round, and each answers on its own.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_UnsharedFailureReleasesFollowers(t *testing.T) {
	p, _ := newFlightProxy(t, "http://127.0.0.1:1", nil)
	const d = 200 * time.Millisecond
	started := make(chan struct{})
	var ran atomic.Int64
	serve := func(lead bool) time.Duration {
		begin := time.Now()
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
		req = req.WithContext(context.WithValue(req.Context(), requestTelemetryKey, newRequestTelemetry()))
		p.serveFlight("labels", "/loki/api/v1/labels", httptest.NewRecorder(), req, flightKey{join: "queue-full-key"}, begin, func(w http.ResponseWriter, r *http.Request, l *flightLead) {
			ran.Add(1)
			if l != nil {
				close(started)
				time.Sleep(d)
			}
			p.fail(l.tap(w, 0), r, lokierr.QueueFull(nil))
		})
		return time.Since(begin)
	}
	go serve(true)
	<-started
	const followers = 5
	var wg sync.WaitGroup
	took := make([]time.Duration, followers)
	for i := 0; i < followers; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); took[i] = serve(false) }(i)
	}
	wg.Wait()
	for _, d2 := range took {
		if d2 > 2*d {
			t.Fatalf("a follower took %v, want about one leader's time (%v)", d2, d)
		}
	}
	if got := ran.Load(); got != followers+1 {
		t.Fatalf("runs %d, want every follower to run on its own", got)
	}
}

// A held answer carries the same status, headers and body as a fresh one from
// a proxy without flights, for every class it holds (the debug counters and
// Date aside).
//
// conformance: semantics/error-cache-byte-equal-answers
func TestErrorFlights_HeldAnswersEqualFreshOnes(t *testing.T) {
	metric := "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum by (app) (count_over_time({app=~"a.+"}[1m]))`) + "&start=1700000000&end=1700003600&step=60"
	threeSeries := func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a1"},"values":[[1700000060,"1"]]},{"metric":{"app":"a2"},"values":[[1700000060,"1"]]}]}}`))
	}
	status := func(code int) func(int64, http.ResponseWriter, *http.Request) {
		return func(_ int64, w http.ResponseWriter, _ *http.Request) { http.Error(w, "backend says no", code) }
	}
	refusal := func(_ int64, w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cannot parse query arg: unexpected token", http.StatusBadRequest)
	}
	cases := []struct {
		name   string
		target string
		answer func(int64, http.ResponseWriter, *http.Request)
		held   bool
	}{
		{"VictoriaLogs 500", flightLogTarget, status(http.StatusInternalServerError), true},
		{"VictoriaLogs 429", flightLogTarget, status(http.StatusTooManyRequests), true},
		{"VictoriaLogs 503 after retries", flightLogTarget, status(http.StatusServiceUnavailable), true},
		{"VictoriaLogs 400 unclassified", flightLogTarget, status(http.StatusBadRequest), true},
		// A refusal of the text the proxy wrote is the same every time on the same release: held for the
		// refusal TTL (vl_refusal.go), with the answer a fresh request gets.
		{"VictoriaLogs parse refusal (plan defect)", flightLogTarget, refusal, true},
		{"series limit", metric, threeSeries, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			freshStub := newFlightStub(t, c.answer)
			_, plain := newFlightProxy(t, freshStub.srv.URL, func(cfg *Config) {
				cfg.MaxStatsQuerySeries, cfg.CoalescerDisabled, cfg.ErrorCache = 1, true, ErrorCacheConfig{Disabled: true}
			})
			fresh := flightGet(plain, context.Background(), c.target, "team-a")
			stub := newFlightStub(t, c.answer)
			_, h := newFlightProxy(t, stub.srv.URL, func(cfg *Config) { cfg.MaxStatsQuerySeries, cfg.ErrorCache.RefusalTTL = 1, time.Minute })
			first := flightGet(h, context.Background(), c.target, "team-a")
			calls := stub.calls.Load()
			held := flightGet(h, context.Background(), c.target, "team-a")
			if fresh.Code < 400 {
				t.Fatalf("fresh answer %d", fresh.Code)
			}
			for _, got := range []*httptest.ResponseRecorder{first, held} {
				if got.Code != fresh.Code || got.Body.String() != fresh.Body.String() {
					t.Fatalf("answer %d %q, fresh %d %q", got.Code, got.Body.String(), fresh.Code, fresh.Body.String())
				}
				if a, b := comparableHeader(got.Header()), comparableHeader(fresh.Header()); a != b {
					t.Fatalf("headers %s, fresh %s", a, b)
				}
			}
			if repeatCalls := stub.calls.Load() - calls; (repeatCalls == 0) != c.held {
				t.Fatalf("%s: repeat sent %d calls, held=%v expected", c.name, repeatCalls, c.held)
			}
		})
	}
}

func comparableHeader(h http.Header) string {
	c := h.Clone()
	for _, name := range []string{"Date", vlBackendCallsHeader, vlCacheResultHeader} {
		c.Del(name)
	}
	var b strings.Builder
	_ = c.WriteSubset(&b, nil)
	return b.String()
}

// When VictoriaLogs fails and the proxy answers a stale label list (200 with
// the stale header), identical requests in flight share that answer: one
// VictoriaLogs call for the burst.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_StaleAnswerSharedInFlight(t *testing.T) {
	var failing atomic.Bool
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			answer500(n, w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
	})
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	target := "/loki/api/v1/labels?start=1700000000&end=1700003600"
	if a := flightGet(h, context.Background(), target, "team-a"); a.Code != http.StatusOK {
		t.Fatalf("warm-up %d", a.Code)
	}
	restore := cache.AdvanceClockForTesting(48 * time.Hour)
	defer restore()
	failing.Store(true)
	stub.calls.Store(0)
	answers := burst(t, p, stub, 10, func() *httptest.ResponseRecorder { return flightGet(h, context.Background(), target, "team-a") })
	stale := 0
	for _, a := range answers {
		if a.Header().Get(staleResponseHeader) != "" {
			stale++
		}
	}
	t.Logf("burst of 10 during an outage: %d VictoriaLogs calls, %d stale answers, statuses %d", stub.calls.Load(), stale, answers[0].Code)
	if stale == len(answers) && stub.calls.Load() > 4 {
		t.Fatalf("stale answers not shared in flight: %d calls", stub.calls.Load())
	}
}

func classOnly(c cache.ErrorClass, _ bool) cache.ErrorClass { return c }

// A backend failure that does not depend on the window (a VictoriaLogs 5xx)
// is held for every window of the request: a refresh that moved end=now, or a
// POST with another window, answers it without a call. Another query, another
// tenant, and a failure that depends on the window (a timeout) are not.
//
// conformance: semantics/error-cache-classes-and-ttl, semantics/error-cache-tenant-isolation
func TestErrorFlights_WindowFreeFailureHeldForEveryWindow(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	at := func(query string, end int) string {
		return "/loki/api/v1/query_range?query=" + url.QueryEscape(query) + "&start=1700000000&end=" + strconv.Itoa(end) + "&limit=100"
	}
	flightGet(h, context.Background(), at(`{app="api"}`, 1700003600), "team-a")
	calls := stub.calls.Load()
	moved := flightGet(h, context.Background(), at(`{app="api"}`, 1700003615), "team-a")
	if moved.Code != http.StatusInternalServerError || stub.calls.Load() != calls {
		t.Fatalf("moved window: %d with %d calls, want the held 500 and none", moved.Code, stub.calls.Load()-calls)
	}
	flightGet(h, context.Background(), at(`{app="web"}`, 1700003615), "team-a")
	if stub.calls.Load() == calls {
		t.Fatal("another query must ask VictoriaLogs")
	}
	calls = stub.calls.Load()
	flightGet(h, context.Background(), at(`{app="api"}`, 1700003615), "team-b")
	if stub.calls.Load() == calls {
		t.Fatal("another tenant must ask VictoriaLogs")
	}
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	calls = stub.calls.Load()
	flightGet(h, context.Background(), at(`{app="api"}`, 1700003630), "team-a")
	if stub.calls.Load() == calls {
		t.Fatal("after the TTL the request must ask VictoriaLogs again")
	}
	_ = p

	for _, c := range []struct {
		name string
		err  error
		free bool
	}{
		{"vl 500", &upstreamStatusError{status: 500}, true},
		{"vl 429", &upstreamStatusError{status: 429}, true},
		{"vl query deadline", &upstreamStatusError{status: 503, class: vlErrorDeadline}, false},
		{"vl 504", &upstreamStatusError{status: 504}, false},
		{"refused", &url.Error{Op: "Post", URL: "x", Err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}}, true},
		{"no such host (a stopped container)", &url.Error{Op: "Post", URL: "x", Err: &net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "victorialogs", IsNotFound: true}}}, true},
		{"timeout", &url.Error{Op: "Post", URL: "x", Err: timeoutErr{}}, false},
		{"aborted", &vlResponseAbortedError{}, false},
	} {
		if got := windowFreeFailure(c.err, nil); got != c.free {
			t.Errorf("%s: windowFree %v, want %v", c.name, got, c.free)
		}
		if got := windowFreeFailure(nil, lokierr.BackendFailed(c.err)); got != c.free {
			t.Errorf("%s typed: windowFree %v, want %v", c.name, got, c.free)
		}
	}
	if windowFreeFailure(nil, lokierr.Deadline(nil)) || windowFreeFailure(nil, lokierr.BackendFailed(nil)) {
		t.Fatal("a deadline, or a backend failure of unknown cause, depends on the window")
	}
}

// The form digest tells apart bodies that differ in any name, value, value
// count or split between name and value, and ignores the order of names.
//
// conformance: semantics/error-cache-tenant-isolation
func TestErrorFlights_FormDigest(t *testing.T) {
	d := func(pairs ...string) uint64 {
		v := url.Values{}
		for i := 0; i+1 < len(pairs); i += 2 {
			v.Add(pairs[i], pairs[i+1])
		}
		return formDigest(v)
	}
	base := d("query", `{a="b"}`, "start", "1")
	if d("start", "1", "query", `{a="b"}`) != base {
		t.Fatal("name order must not matter")
	}
	for _, other := range []uint64{
		d("query", `{a="c"}`, "start", "1"),
		d("query", `{a="b"}`, "start", "2"),
		d("query", `{a="b"}`, "start", "1", "start", "1"),
		d("query", `{a="b"}`, "star", "t1"),
		d("query", `{a="b"}`),
	} {
		if other == base {
			t.Fatal("different bodies share a digest")
		}
	}
}

// Two POSTs with the same pre-key but different exact keys (another client
// profile) never share an answer: the follower runs on its own, and a held
// failure answers only the exact request that failed.
//
// conformance: semantics/error-cache-tenant-isolation
func TestErrorFlights_PreKeyNeverSharesAcrossExactKeys(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	form := url.Values{"query": {`{app="api"}`}, "start": {"1700000000"}, "end": {"1700003600"}, "limit": {"100"}}.Encode()
	post := func(ua string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Scope-OrgID", "team-a")
		req.Header.Set("User-Agent", ua)
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		if ua == "" {
			req.Header.Del("X-Query-Tags")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	keyOf := func(ua string) string {
		req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Scope-OrgID", "team-a")
		req.Header.Set("User-Agent", ua)
		if ua != "" {
			req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		}
		_ = req.ParseForm()
		k := p.uncachedFlightKey("query_range", req)
		return k.join + "|" + k.exactKey()
	}
	grafana, plain := keyOf("Grafana/12.1.0"), keyOf("")
	gj, ge, _ := strings.Cut(grafana, "|")
	pj, pe, _ := strings.Cut(plain, "|")
	if gj != pj || ge == pe {
		t.Skipf("the profiles do not differ in the exact key only: %q %q", grafana, plain)
	}
	var i atomic.Int64
	answers := burst(t, p, stub, 2, func() *httptest.ResponseRecorder {
		if i.Add(1) == 1 {
			return post("Grafana/12.1.0")
		}
		return post("")
	})
	if stub.calls.Load() != 2 || answers[0].Code != 500 || answers[1].Code != 500 {
		t.Fatalf("in flight: %d calls, want each profile its own", stub.calls.Load())
	}
	restore := cache.AdvanceClockForTesting(2 * time.Second)
	defer restore()
	post("Grafana/12.1.0")
	calls := stub.calls.Load()
	post("")
	if stub.calls.Load() == calls {
		t.Fatal("a failure held for one profile answered another")
	}
	calls = stub.calls.Load()
	post("Grafana/12.1.0")
	if stub.calls.Load() != calls {
		t.Fatal("the held failure must answer its own exact request")
	}
}

// Identical failing requests arriving over the leader's run and its end (a
// burst whose requests do not all arrive at once) make one VictoriaLogs call
// while the failure is held, GET (joined on the response key) and POST (joined
// on a pre-key) alike: a request arriving as the leader ends waits on it or
// reads the held failure, never leads a second call.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlights_StaggeredBurstOneCall(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
				time.Sleep(time.Millisecond)
				answer500(n, w, r)
			})
			_, h := newFlightProxy(t, stub.srv.URL, func(c *Config) {
				c.ErrorCache.TransientTTL = time.Minute
			})
			const trials, n = 20, 50
			for trial := 0; trial < trials; trial++ {
				// Each burst its own request (a failure of every window is held for
				// all windows of one request, so the bursts differ in limit).
				params := url.Values{"query": {`{app="api"}`}, "start": {"1700000000"}, "end": {"1700003600"}, "limit": {strconv.Itoa(100 + trial)}}
				send := func() *httptest.ResponseRecorder {
					var req *http.Request
					if method == http.MethodGet {
						req = httptest.NewRequest(method, "/loki/api/v1/query_range?"+params.Encode(), nil)
					} else {
						req = httptest.NewRequest(method, "/loki/api/v1/query_range", strings.NewReader(params.Encode()))
						req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					}
					req.Header.Set("X-Scope-OrgID", "team-a")
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					return rec
				}
				var wg sync.WaitGroup
				for i := 0; i < n; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						// Arrivals spread over about twice the backend's time.
						time.Sleep(time.Duration(i) * 40 * time.Microsecond)
						if a := send(); a.Code != http.StatusInternalServerError {
							t.Errorf("answer %d %q", a.Code, a.Body.String())
						}
					}(i)
				}
				wg.Wait()
			}
			if got := stub.calls.Load(); got != trials {
				t.Fatalf("%s: %d VictoriaLogs calls for %d bursts of %d identical failing requests, want one per burst", method, got, trials, n)
			}
		})
	}
}
