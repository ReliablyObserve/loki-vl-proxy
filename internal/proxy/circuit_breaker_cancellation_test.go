package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

type errorRoundTripper struct {
	err error
}

func (rt errorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, rt.err
}

func newCircuitBreakerTestProxy(t *testing.T, transport http.RoundTripper) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:      "http://backend.example",
		Cache:           cache.New(30*time.Second, 10),
		LogLevel:        "error",
		CBFailThreshold: 2,
		CBOpenDuration:  time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	p.client.Transport = transport
	return p
}

func TestCircuitBreaker_IgnoresCanceledErrors(t *testing.T) {
	t.Run("context canceled", func(t *testing.T) {
		p := newCircuitBreakerTestProxy(t, errorRoundTripper{err: context.Canceled})
		for i := 0; i < 5; i++ {
			_, err := p.vlGet(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context canceled from transport, got %v", err)
			}
		}
		if got := p.breaker.State(); got != "closed" {
			t.Fatalf("canceled requests must not trip breaker, got state=%s", got)
		}
	})

	t.Run("wrapped canceled message", func(t *testing.T) {
		p := newCircuitBreakerTestProxy(t, errorRoundTripper{err: errors.New("upstream: context canceled")})
		for i := 0; i < 5; i++ {
			_, err := p.vlPost(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}})
			if err == nil {
				t.Fatal("expected transport error")
			}
		}
		if got := p.breaker.State(); got != "closed" {
			t.Fatalf("wrapped canceled errors must not trip breaker, got state=%s", got)
		}
	})
}

func TestCircuitBreaker_TripsOnRealTransportErrors(t *testing.T) {
	p := newCircuitBreakerTestProxy(t, errorRoundTripper{err: errors.New("connection refused")})

	for i := 0; i < 2; i++ {
		if _, err := p.vlGet(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}}); err == nil {
			t.Fatalf("expected transport error on failure %d", i+1)
		}
	}

	if got := p.breaker.State(); got != "open" {
		t.Fatalf("expected breaker open after threshold failures, got state=%s", got)
	}

	if _, err := p.vlGet(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}}); err == nil || err.Error() != "circuit breaker open — backend unavailable" {
		t.Fatalf("expected open-breaker error on next call, got %v", err)
	}
}
