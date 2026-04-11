package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newBreakerTestProxy(t *testing.T, backendURL string, failThreshold int) *Proxy {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:      backendURL,
		Cache:           c,
		LogLevel:        "error",
		CBFailThreshold: failThreshold,
		CBOpenDuration:  time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func TestCircuitBreaker_DoesNotTripOnUpstreamHTTP5xx(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, `{"status":"error","error":"all backends unavailable"}`, http.StatusBadGateway)
	}))
	defer backend.Close()

	p := newBreakerTestProxy(t, backend.URL, 1)
	params := url.Values{"query": []string{"*"}}

	for i := 0; i < 3; i++ {
		resp, err := p.vlPost(context.Background(), "/select/logsql/query", params)
		if err != nil {
			t.Fatalf("vlPost attempt %d returned unexpected error: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("vlPost attempt %d status=%d want=%d", i+1, resp.StatusCode, http.StatusBadGateway)
		}
		_ = resp.Body.Close()
	}

	if got := calls.Load(); got != 3 {
		t.Fatalf("unexpected backend call count: got=%d want=3", got)
	}
	if state := p.breaker.State(); state != "closed" {
		t.Fatalf("breaker should stay closed on upstream HTTP errors, got state=%s", state)
	}
	if !p.breaker.Allow() {
		t.Fatal("breaker should still allow traffic after repeated upstream HTTP 502 responses")
	}
}

func TestCircuitBreaker_TripsOnTransportFailure(t *testing.T) {
	p := newBreakerTestProxy(t, "http://unused", 2)
	p.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp: connect: connection refused")
		}),
	}

	params := url.Values{"query": []string{"*"}}
	for i := 0; i < 2; i++ {
		if _, err := p.vlGet(context.Background(), "/select/logsql/query", params); err == nil {
			t.Fatalf("expected transport error on attempt %d", i+1)
		}
	}

	if state := p.breaker.State(); state != "open" {
		t.Fatalf("breaker should open after consecutive transport failures, got state=%s", state)
	}
	if p.breaker.Allow() {
		t.Fatal("breaker unexpectedly allowed request while open")
	}
}

func TestCircuitBreaker_IgnoresCanceledTransportError(t *testing.T) {
	p := newBreakerTestProxy(t, "http://unused", 1)
	p.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, context.Canceled
		}),
	}

	params := url.Values{"query": []string{"*"}}
	if _, err := p.vlGet(context.Background(), "/select/logsql/query", params); err == nil {
		t.Fatal("expected context canceled error")
	}

	if state := p.breaker.State(); state != "closed" {
		t.Fatalf("breaker should stay closed on canceled request errors, got state=%s", state)
	}
	if !p.breaker.Allow() {
		t.Fatal("breaker should continue allowing traffic after canceled errors")
	}
}
