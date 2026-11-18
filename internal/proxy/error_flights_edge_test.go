package proxy

import (
	"context"
	"fmt"
	"io"
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
)

// A held failure answers until just before its TTL and not at it.
//
// conformance: semantics/error-cache-classes-and-ttl
func TestErrorFlightsEdge_TTLBoundary(t *testing.T) {
	stub := newFlightStub(t, answer500)
	_, h := newFlightProxy(t, stub.srv.URL, nil)
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	calls := stub.calls.Load()
	restore := cache.AdvanceClockForTesting(time.Second - 5*time.Millisecond)
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	restore()
	if stub.calls.Load() != calls {
		t.Fatal("a failure must be held until its TTL")
	}
	restore = cache.AdvanceClockForTesting(time.Second + 5*time.Millisecond)
	defer restore()
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	if stub.calls.Load() == calls {
		t.Fatal("a failure must not be held past its TTL")
	}
}

// A held failure answers only its own request: another query, step, limit,
// direction, route, method, tenant or set of forwarded credentials asks
// VictoriaLogs itself.
//
// conformance: semantics/error-cache-tenant-isolation
func TestErrorFlightsEdge_IsolationByRequest(t *testing.T) {
	stub := newFlightStub(t, answer500)
	_, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.ForwardHeaders = []string{"Authorization"} })
	send := func(method, target, tenant, auth string) {
		var req *http.Request
		if method == http.MethodPost {
			path, query, _ := strings.Cut(target, "?")
			req = httptest.NewRequest(method, path, strings.NewReader(query))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			req = httptest.NewRequest(method, target, nil)
		}
		req.Header.Set("X-Scope-OrgID", tenant)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	base := "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum(count_over_time({app="api"}[1m]))`) + "&start=1700000000&end=1700003600&step=60&limit=100&direction=backward"
	send(http.MethodGet, base, "team-a", "Bearer one")
	others := []struct {
		name, method, target, tenant, auth string
	}{
		{"query", http.MethodGet, strings.Replace(base, "%22api%22", "%22web%22", 1), "team-a", "Bearer one"},
		{"step", http.MethodGet, strings.Replace(base, "step=60", "step=30", 1), "team-a", "Bearer one"},
		{"limit", http.MethodGet, strings.Replace(base, "limit=100", "limit=50", 1), "team-a", "Bearer one"},
		{"direction", http.MethodGet, strings.Replace(base, "backward", "forward", 1), "team-a", "Bearer one"},
		{"route", http.MethodGet, strings.Replace(base, "query_range", "query", 1), "team-a", "Bearer one"},
		{"method", http.MethodPost, base, "team-a", "Bearer one"},
		{"tenant", http.MethodGet, base, "team-b", "Bearer one"},
		{"credentials", http.MethodGet, base, "team-a", "Bearer two"},
	}
	for _, o := range others {
		before := stub.calls.Load()
		send(o.method, o.target, o.tenant, o.auth)
		if stub.calls.Load() == before {
			t.Errorf("another %s was answered by the held failure", o.name)
		}
	}
	before := stub.calls.Load()
	send(http.MethodGet, base, "team-a", "Bearer one")
	if stub.calls.Load() != before {
		t.Fatal("the request itself must be answered by its held failure")
	}
}

// Only failures are held for every window: a success is never answered to
// another window, and after a held failure expires a new window reads
// VictoriaLogs and sees its data.
//
// conformance: semantics/error-cache-classes-and-ttl
func TestErrorFlightsEdge_WindowFreeHoldNeverServesSuccess(t *testing.T) {
	var failing atomic.Bool
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			answer500(n, w, r)
			return
		}
		answerLogs(n, w, r)
	})
	_, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.CompatCache = nil })
	at := func(end int) string {
		return "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1700000000&end=" + strconv.Itoa(end) + "&limit=100"
	}
	flightGet(h, context.Background(), at(1700003600), "team-a")
	before := stub.calls.Load()
	if a := flightGet(h, context.Background(), at(1700003615), "team-a"); a.Code != http.StatusOK || stub.calls.Load() == before {
		t.Fatalf("a success must never answer another window (%d)", a.Code)
	}
	failing.Store(true)
	flightGet(h, context.Background(), at(1700003630), "team-a")
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	failing.Store(false)
	if a := flightGet(h, context.Background(), at(1700003645), "team-a"); a.Code != http.StatusOK || !strings.Contains(a.Body.String(), "hello") {
		t.Fatalf("after the hold a new window must answer its data: %d %.120q", a.Code, a.Body.String())
	}
}

// A failure written after the first streamed byte is never held: the client
// already read a 200, and the repeat asks VictoriaLogs again.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsEdge_FailureAfterFirstByteNotHeld(t *testing.T) {
	stub := newFlightStub(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // the body breaks after the first line
	})
	_, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.StreamResponse = true })
	front := httptest.NewServer(h) // the proxy aborts the connection of a broken stream
	defer front.Close()
	get := func() {
		req, _ := http.NewRequest(http.MethodGet, front.URL+flightLogTarget, nil)
		req.Header.Set("X-Scope-OrgID", "team-a")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
	get()
	before := stub.calls.Load()
	get()
	if stub.calls.Load() == before {
		t.Fatal("a stream broken after its first byte must not be held")
	}
}

// compatHandler is the response cache middleware of a read route around a
// handler written for the test.
func compatHandler(p *Proxy, endpoint string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), requestTelemetryKey, newRequestTelemetry()))
		p.compatCacheMiddleware(endpoint, "/loki/api/v1/"+endpoint, next)(w, r)
	})
}

// A partial answer (200 with the partial-upstream header), an error body
// above the capture bound and an untyped failure are never held; a typed
// failure is, and no response cache ever stores a failure.
//
// conformance: semantics/error-coalescing-request-flights, semantics/error-cache-bounded-memory
func TestErrorFlightsEdge_NeverHeldAnswers(t *testing.T) {
	p, _ := newFlightProxy(t, "http://127.0.0.1:1", nil)
	cases := []struct {
		name string
		held bool
		run  func(w http.ResponseWriter, r *http.Request)
	}{
		{"partial answer", false, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set(drilldownPartialUpstreamStatusHeader, "500")
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		}},
		{"oversized failure", false, func(w http.ResponseWriter, r *http.Request) {
			p.fail(w, r, lokierr.BackendFailed(nil))
			_, _ = w.Write([]byte(strings.Repeat("x", errorFlightMaxAnswerBytes+1)))
		}},
		{"untyped failure", false, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "something", http.StatusInternalServerError)
		}},
		{"typed backend failure", true, func(w http.ResponseWriter, r *http.Request) { p.fail(w, r, lokierr.BackendFailed(nil)) }},
	}
	for i, c := range cases {
		var runs atomic.Int64
		h := compatHandler(p, "labels", func(w http.ResponseWriter, r *http.Request) { runs.Add(1); c.run(w, r) })
		target := "/loki/api/v1/labels?start=1700000000&end=1700003600&query=" + url.QueryEscape(fmt.Sprintf(`{case="%d"}`, i))
		for j := 0; j < 2; j++ {
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("X-Scope-OrgID", "team-a")
			h.ServeHTTP(httptest.NewRecorder(), req)
		}
		if held := runs.Load() == 1; held != c.held {
			t.Errorf("%s: ran %d times, held=%v expected", c.name, runs.Load(), c.held)
		}
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "team-a")
		if key, ok := p.compatCacheKey("labels", req); ok && c.name != "partial answer" {
			if _, found := p.compatCache.Peek(key); found {
				t.Errorf("%s: the response cache stored a failure", c.name)
			}
		}
	}
}

// A follower whose client leaves stops waiting with its own 499; the leader
// goes on and answers.
//
// conformance: semantics/error-coalescing-no-inherited-cancel
func TestErrorFlightsEdge_FollowerCancelLeavesLeader(t *testing.T) {
	gate := make(chan struct{})
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		<-gate
		answerLogs(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	leader := make(chan *httptest.ResponseRecorder)
	go func() { leader <- flightGet(h, context.Background(), flightLogTarget, "team-a") }()
	waitFor(t, func() bool { return stub.calls.Load() == 1 })
	ctx, cancel := context.WithCancel(context.Background())
	follower := make(chan *httptest.ResponseRecorder)
	go func() { follower <- flightGet(h, ctx, flightLogTarget, "team-a") }()
	waitFor(t, func() bool { return p.errorFlights.Waiting() == 1 })
	cancel()
	if f := <-follower; f.Code != 499 {
		t.Fatalf("the follower answered %d, want its own 499", f.Code)
	}
	close(gate)
	if l := <-leader; l.Code != http.StatusOK || stub.calls.Load() != 1 {
		t.Fatalf("the leader answered %d with %d calls", l.Code, stub.calls.Load())
	}
}

// A flood of distinct failing requests keeps the held answers within the
// caps, evicts the oldest, and leaves cached successes alone.
//
// conformance: semantics/error-cache-bounded-memory
func TestErrorFlightsEdge_DistinctFailuresStayBounded(t *testing.T) {
	var failing atomic.Bool
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			answer500(n, w, r)
			return
		}
		answerLogs(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, func(c *Config) {
		c.ErrorCache.MaxEntries, c.ErrorCache.MaxBytes = 50, 64<<10
	})
	ok := flightGet(h, context.Background(), flightLogTarget, "team-a")
	failing.Store(true)
	for i := 0; i < 2000; i++ {
		flightGet(h, context.Background(), "/loki/api/v1/query_range?query="+url.QueryEscape(fmt.Sprintf(`{app="flood-%d"}`, i))+"&start=1700000000&end=1700003600&limit=100", "team-a")
	}
	entries, bytes := p.errorFlights.Len()
	if entries > 50 || bytes > 64<<10 {
		t.Fatalf("held %d entries, %d bytes over the caps", entries, bytes)
	}
	before := stub.calls.Load()
	again := flightGet(h, context.Background(), flightLogTarget, "team-a")
	if again.Code != http.StatusOK || stub.calls.Load() != before || answerData(again) != answerData(ok) {
		t.Fatalf("the cached success must survive the flood: %d, %d calls", again.Code, stub.calls.Load()-before)
	}
}

// With -coalescer-disabled and the error cache off the proxy behaves as
// before: every request runs, nothing is held, nothing is joined.
//
// conformance: semantics/error-cache-operator-switches
func TestErrorFlightsEdge_BothOffBehaveLikeBefore(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, func(c *Config) { c.CoalescerDisabled, c.ErrorCache.Disabled = true, true })
	if p.errorFlights != nil {
		t.Fatal("no flights with both switches off")
	}
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
	if got := stub.calls.Load(); got != 6 {
		t.Fatalf("calls %d, want one per request", got)
	}
}

// Held failures never enter the response cache tiers that reach peers: the
// primary and edge caches hold the same entries before and after a failure.
//
// conformance: semantics/error-cache-tenant-isolation
func TestErrorFlightsEdge_HeldFailuresStayLocal(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	primary, _ := p.cache.Size()
	edge, _ := p.compatCache.Size()
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	flightGet(h, context.Background(), flightLogTarget, "team-a")
	if e, _ := p.errorFlights.Len(); e == 0 {
		t.Fatal("the failure must be held")
	}
	if a, _ := p.cache.Size(); a != primary {
		t.Fatalf("primary cache entries %d -> %d", primary, a)
	}
	if b, _ := p.compatCache.Size(); b != edge {
		t.Fatalf("edge cache entries %d -> %d", edge, b)
	}
}

// The leader reports its own backend calls and every follower 0, with the
// hit cache result.
//
// conformance: semantics/error-cache-byte-equal-answers
func TestErrorFlightsEdge_BackendCallsHeader(t *testing.T) {
	stub := newFlightStub(t, answer500)
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	answers := burst(t, p, stub, 6, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), flightLogTarget, "team-a")
	})
	leaders := 0
	for _, a := range answers {
		switch a.Header().Get(vlBackendCallsHeader) {
		case "0":
			if a.Header().Get(vlCacheResultHeader) != "hit" {
				t.Fatalf("a follower's cache result %q", a.Header().Get(vlCacheResultHeader))
			}
		case "1":
			leaders++
		default:
			t.Fatalf("calls header %q", a.Header().Get(vlBackendCallsHeader))
		}
	}
	if leaders != 1 {
		t.Fatalf("%d answers report the call, want the leader only", leaders)
	}
}

// A query holding markup is echoed in Loki's parse error only as text/plain
// with nosniff, fresh, joined and held alike.
//
// conformance: semantics/error-cache-byte-equal-answers
func TestErrorFlightsEdge_MarkupEchoedOnlyAsPlainText(t *testing.T) {
	p, h := newFlightProxy(t, "http://127.0.0.1:1", nil)
	target := "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="api"} |~ "<script>alert(1)(</script>"`) + "&start=1700000000&end=1700003600&step=60"
	answers := []*httptest.ResponseRecorder{flightGet(h, context.Background(), target, "team-a")}
	answers = append(answers, flightGet(h, context.Background(), target, "team-a"))
	_ = p
	for i, a := range answers {
		if a.Code != http.StatusBadRequest || !strings.Contains(a.Body.String(), "<script>") {
			t.Fatalf("answer %d: %d %q, want Loki's parse error echoing the query", i, a.Code, a.Body.String())
		}
		if ct, ns := a.Header().Get("Content-Type"), a.Header().Get("X-Content-Type-Options"); ct != "text/plain; charset=utf-8" || ns != "nosniff" {
			t.Fatalf("answer %d: Content-Type %q, nosniff %q", i, ct, ns)
		}
	}
	rec := httptest.NewRecorder()
	defaultAnswerHeaders(rec.Header(), 200)
	if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("a success without a Content-Type is JSON, never sniffed")
	}
	set := httptest.NewRecorder()
	set.Header().Set("Content-Type", "text/plain; charset=utf-8")
	defaultAnswerHeaders(set.Header(), 500)
	if set.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatal("a Content-Type already set is kept")
	}
}

// A request refused from its text (Loki's parse error) never reaches
// VictoriaLogs through the flight: its answer carries no exact key, whose
// stream label names would be read from VictoriaLogs.
//
// conformance: semantics/error-coalescing-request-flights, profiles/dotted-name-parse-error
func TestErrorFlightsEdge_RefusedRequestSendsNoBackendCall(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeTranslated, false)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for i := 0; i < 3; i++ {
		body := url.Values{"query": {"{env=\"production\"} | json | pipeline=`metrics/prometheus` | k8s.namespace.name=`monitoring`"}, "start": {"1700000000"}, "end": {"1700003600"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Scope-OrgID", "0")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
	}
	// The exact key of such a request, built when a second one is in flight
	// or a failure of it is held, reads no stream label names from
	// VictoriaLogs either.
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(url.Values{"query": {`{env="production"} | json`}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_ = req.ParseForm()
	req = req.WithContext(context.WithValue(req.Context(), origRequestKey, req))
	k := p.uncachedFlightKey("query_range", req)
	_ = k.exactKey()
	_ = p.windowFreeKey("query_range", req)
	time.Sleep(200 * time.Millisecond) // a refresh started in the background would land by now
	if calls.Load() != 0 {
		t.Fatalf("a refused request reached the backend %d times", calls.Load())
	}
}

// When a window-free hold expires during an outage, requests of every window
// join one probe: one VictoriaLogs call answers them all with the failure;
// once the backend answers, the probe's success lets each window run on its
// own.
//
// conformance: semantics/error-cache-classes-and-ttl
func TestErrorFlightsEdge_WindowFreeProbeAfterHoldExpires(t *testing.T) {
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
	at := func(end int) string {
		return "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1700000000&end=" + strconv.Itoa(end) + "&limit=100"
	}
	flightGet(h, context.Background(), at(1700003600), "team-a")
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond) // the hold expired, the outage is recent
	defer restore()
	stub.calls.Store(0)
	var n atomic.Int64
	answers := burst(t, p, stub, 10, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), at(1700003700+int(n.Add(1))), "team-a")
	})
	for _, a := range answers {
		if a.Code != http.StatusInternalServerError {
			t.Fatalf("answer %d", a.Code)
		}
	}
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("10 windows of one query during an outage sent %d calls, want the probe's 1", got)
	}
	restore2 := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore2()
	failing.Store(false)
	stub.calls.Store(0)
	n.Store(100)
	answers = burst(t, p, stub, 5, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), at(1700003700+int(n.Add(1))), "team-a")
	})
	for _, a := range answers {
		if a.Code != http.StatusOK {
			t.Fatalf("after recovery a window answered %d", a.Code)
		}
	}
}

// The per-request headers are matched in canonical form: a follower never
// carries the leader's debug counters.
//
// conformance: semantics/error-cache-byte-equal-answers
func TestErrorFlightsEdge_PerRequestHeadersCanonical(t *testing.T) {
	h := http.Header{}
	h.Set(vlBackendCallsHeader, "3")
	h.Set(vlCacheResultHeader, "miss")
	h.Set("Content-Type", "text/plain; charset=utf-8")
	out := flightHeader(h)
	if out.Get(vlBackendCallsHeader) != "" || out.Get(vlCacheResultHeader) != "" || out.Get("Content-Type") == "" {
		t.Fatalf("shared header %v", out)
	}
}

// Probing is per query: while one query probes after its window-free failure,
// identical successes of another query (GET and POST) still share one call
// and wait on no probe; once a probe of the failed query succeeds, that query
// stops probing.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsEdge_ProbeIsPerQuery(t *testing.T) {
	var failA atomic.Bool
	failA.Store(true)
	stub := newFlightStub(t, func(n int64, w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if failA.Load() && strings.Contains(r.Form.Get("query"), `"api"`) {
			answer500(n, w, r)
			return
		}
		answerLogs(n, w, r)
	})
	p, h := newFlightProxy(t, stub.srv.URL, nil)
	a := func(end int) string {
		return "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1700000000&end=" + strconv.Itoa(end) + "&limit=100"
	}
	flightGet(h, context.Background(), a(1700003600), "team-a")
	restore := cache.AdvanceClockForTesting(1100 * time.Millisecond)
	defer restore()
	stub.calls.Store(0)
	answers := burst(t, p, stub, 10, func() *httptest.ResponseRecorder {
		return flightGet(h, context.Background(), "/loki/api/v1/query_range?query=%7Bapp%3D%22web%22%7D&start=1700000000&end=1700003600&limit=100", "team-a")
	})
	if stub.calls.Load() != 1 || answers[0].Code != http.StatusOK {
		t.Fatalf("another query's successes: %d calls, status %d; want 1 shared call", stub.calls.Load(), answers[0].Code)
	}
	form := url.Values{"query": {`{app="web"}`}, "start": {"1700000000"}, "end": {"1700003600"}, "limit": {"100"}}.Encode()
	stub.calls.Store(0)
	burst(t, p, stub, 10, func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Scope-OrgID", "team-a")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	})
	if stub.calls.Load() != 1 {
		t.Fatalf("another query's POST successes sent %d calls, want 1", stub.calls.Load())
	}
	failA.Store(false)
	if ok := flightGet(h, context.Background(), a(1700003700), "team-a"); ok.Code != http.StatusOK {
		t.Fatalf("the failed query after recovery: %d", ok.Code)
	}
	wk := p.windowFreeKey("query_range", httptest.NewRequest(http.MethodGet, a(1700003800), nil))
	if p.errorFlights.Peek(probeMarkerKey(wk)) != nil {
		t.Fatal("a probe the backend answered must end the query's probing")
	}
}
