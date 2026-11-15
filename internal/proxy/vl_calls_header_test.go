package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// The per-request VictoriaLogs call headers are off by default: Loki sends
// neither, so a client sees exactly Loki's headers.
// conformance: quality/edge-cache-oversized-reply
func TestVLCallsHeaderOffByDefault(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	srv := httptest.NewServer((&vlemu.Emulator{T: t, Rows: oversizedReplyRows(now), Field: "duration_ms", Version: "v1.50.0"}).Handler())
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if _, counting := p.client.Transport.(*vlCallCounter); counting {
		t.Fatal("the backend client counts calls with the header off")
	}
	rec := doCompatProxyRequest(p, oversizedReplyRequests(now)[0].target, map[string]string{"X-Scope-OrgID": "0"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	for _, name := range []string{vlBackendCallsHeader, vlCacheResultHeader} {
		if got := rec.Header().Values(name); len(got) != 0 {
			t.Fatalf("%s sent with the flag off: %q", name, got)
		}
	}
}

// With the header on, a request reports the VictoriaLogs calls it started, and
// only its own: an error answer and a miss count theirs, and calls made for
// another request in flight at the same time are not added.
// conformance: quality/edge-cache-oversized-reply
func TestVLCallsHeaderCountsTheRequestsOwnCalls(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	traffic := &vlTraffic{inner: (&vlemu.Emulator{T: t, Rows: oversizedReplyRows(now), Field: "duration_ms", Version: "v1.50.0"}).Handler()}
	srv := httptest.NewServer(traffic)
	t.Cleanup(srv.Close)
	p := newOversizedReplyProxy(t, srv.URL, 0, 0) // no cache: every request asks VictoriaLogs
	headers := map[string]string{"X-Scope-OrgID": "0"}
	target := oversizedReplyRequests(now)[0].target
	traffic.take()
	rec := doCompatProxyRequest(p, target, headers)
	served, _ := traffic.take()
	calls, result := vlCallsOf(t, rec)
	if rec.Code != http.StatusOK || calls == 0 || calls != served {
		t.Fatalf("status %d: header says %d calls, VictoriaLogs served %d", rec.Code, calls, served)
	}
	if result != "bypass" {
		t.Fatalf("no response cache consulted, cache result %q", result)
	}
	// Several requests at once: each counts its own calls only. A request that
	// joins another's call in flight starts none itself, so each reports at
	// most the calls it makes alone, and together they report every call
	// VictoriaLogs served.
	const parallel = 4
	traffic.take()
	got := make(chan int, parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			r := doCompatProxyRequest(p, target, headers)
			n, _ := strconv.Atoi(r.Header().Get(vlBackendCallsHeader))
			got <- n
		}()
	}
	sum := 0
	for i := 0; i < parallel; i++ {
		n := <-got
		if n > calls {
			t.Fatalf("a concurrent request reported %d calls, alone it makes %d", n, calls)
		}
		sum += n
	}
	if served, _ := traffic.take(); sum != served {
		t.Fatalf("concurrent requests reported %d calls, VictoriaLogs served %d", sum, served)
	}
	// An answer refused before any VictoriaLogs call reports none.
	bad := "/loki/api/v1/query_range?" + url.Values{"query": {`sum(`}, "start": {"1"}, "end": {"2"}}.Encode()
	rec = doCompatProxyRequest(p, bad, headers)
	if n, _ := vlCallsOf(t, rec); rec.Code != http.StatusBadRequest || n != 0 {
		t.Fatalf("parse error: status %d, %d calls", rec.Code, n)
	}
}

// Calls through the backend client to another host (a peer's cache) are not
// VictoriaLogs calls; calls without a client request are nobody's.
// conformance: quality/edge-cache-oversized-reply
func TestVLCallCounterCountsOnlyTheBackendHost(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	vl, peer := httptest.NewServer(ok), httptest.NewServer(ok)
	t.Cleanup(vl.Close)
	t.Cleanup(peer.Close)
	vlURL, _ := url.Parse(vl.URL)
	counter := &vlCallCounter{inner: http.DefaultTransport, host: vlURL.Host}
	rt := newRequestTelemetry()
	ctx := context.WithValue(context.Background(), requestTelemetryKey, rt)
	for _, target := range []string{vl.URL + "/select/logsql/query", peer.URL + "/_cache/get", vl.URL + "/health"} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		resp, err := counter.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, vl.URL+"/select/logsql/query", nil)
	if resp, err := counter.RoundTrip(req); err == nil {
		_ = resp.Body.Close()
	}
	h := http.Header{}
	rt.setVLCallsHeaders(h)
	if h.Get(vlBackendCallsHeader) != "2" || h.Get(vlCacheResultHeader) != "bypass" {
		t.Fatalf("headers %v: want 2 VictoriaLogs calls and no cache consulted", h)
	}
	setCacheResult(ctx, "hit")
	rt.setVLCallsHeaders(h)
	if h.Get(vlCacheResultHeader) != "hit" {
		t.Fatalf("cache result %q", h.Get(vlCacheResultHeader))
	}
}

// The headers go out once, with whatever sends the response's headers first:
// an explicit status, the first body write, or a flush.
// conformance: quality/edge-cache-oversized-reply
func TestStatusCaptureAddsHeadersBeforeSending(t *testing.T) {
	for name, send := range map[string]func(*statusCapture){
		"WriteHeader": func(sc *statusCapture) { sc.WriteHeader(http.StatusTeapot) },
		"Write":       func(sc *statusCapture) { _, _ = sc.Write([]byte("x")) },
		"Flush":       func(sc *statusCapture) { sc.Flush() },
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			runs := 0
			sc := &statusCapture{ResponseWriter: rec, code: 200, beforeHeader: func(h http.Header) {
				runs++
				h.Set(vlBackendCallsHeader, strconv.Itoa(runs))
			}}
			send(sc)
			_, _ = sc.Write([]byte("y"))
			sc.Flush()
			if runs != 1 || rec.Header().Get(vlBackendCallsHeader) != "1" {
				t.Fatalf("hook ran %d times, header %q", runs, rec.Header().Get(vlBackendCallsHeader))
			}
		})
	}
}
