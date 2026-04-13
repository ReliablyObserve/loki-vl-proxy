package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestVolumeEndpoint_UsesCacheOnRepeatQueries(t *testing.T) {
	var hitsCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		hitsCalls.Add(1)
		_, _ = w.Write([]byte(`{"hits":[{"fields":{"app":"api"},"timestamps":["2026-04-10T00:00:00Z"],"values":[3]}]}`))
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	q := url.QueryEscape(`{app="api"}`)
	req1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/index/volume?query=%s&start=now-1h&end=now&targetLabels=app", q), nil)
	w1 := httptest.NewRecorder()
	p.handleVolume(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("unexpected status on first volume call: %d body=%s", w1.Code, w1.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/index/volume?query=%s&start=now-1h&end=now&targetLabels=app", q), nil)
	w2 := httptest.NewRecorder()
	p.handleVolume(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("unexpected status on second volume call: %d body=%s", w2.Code, w2.Body.String())
	}

	if got := hitsCalls.Load(); got != 1 {
		t.Fatalf("expected backend to be called once for cached repeat volume query, got %d", got)
	}
}

func TestVolumeRangeEndpoint_UsesCacheOnRepeatQueries(t *testing.T) {
	var hitsCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		hitsCalls.Add(1)
		_, _ = w.Write([]byte(`{"hits":[{"fields":{"app":"api"},"timestamps":["2026-04-10T00:00:00Z","2026-04-10T00:01:00Z"],"values":[2,1]}]}`))
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	q := url.QueryEscape(`{app="api"}`)
	req1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/index/volume_range?query=%s&start=now-1h&end=now&step=60&targetLabels=app", q), nil)
	w1 := httptest.NewRecorder()
	p.handleVolumeRange(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("unexpected status on first volume_range call: %d body=%s", w1.Code, w1.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/index/volume_range?query=%s&start=now-1h&end=now&step=60&targetLabels=app", q), nil)
	w2 := httptest.NewRecorder()
	p.handleVolumeRange(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("unexpected status on second volume_range call: %d body=%s", w2.Code, w2.Body.String())
	}

	if got := hitsCalls.Load(); got != 1 {
		t.Fatalf("expected backend to be called once for cached repeat volume_range query, got %d", got)
	}
}
