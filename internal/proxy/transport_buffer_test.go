package proxy

import (
	"net/http"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestTransportBufferSize verifies that New() sets read/write buffer sizes on
// both the main and tail transports according to BackendReadBufferSize and
// BackendWriteBufferSize config fields.
func TestTransportBufferSize_DefaultIs64KB(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: "http://localhost:9428",
		Cache:      c,
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tr, ok := p.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", p.client.Transport)
	}

	const want = 64 * 1024
	if tr.ReadBufferSize != want {
		t.Errorf("ReadBufferSize: got %d, want %d", tr.ReadBufferSize, want)
	}
	if tr.WriteBufferSize != want {
		t.Errorf("WriteBufferSize: got %d, want %d", tr.WriteBufferSize, want)
	}

	// tail transport (clone) should carry the same buffer sizes
	tailTr, ok := p.tailClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport for tailClient, got %T", p.tailClient.Transport)
	}
	if tailTr.ReadBufferSize != want {
		t.Errorf("tailTransport ReadBufferSize: got %d, want %d", tailTr.ReadBufferSize, want)
	}
	if tailTr.WriteBufferSize != want {
		t.Errorf("tailTransport WriteBufferSize: got %d, want %d", tailTr.WriteBufferSize, want)
	}
}

func TestTransportBufferSize_CustomReadBuf(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:            "http://localhost:9428",
		Cache:                 c,
		LogLevel:              "error",
		BackendReadBufferSize: 32 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tr, ok := p.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", p.client.Transport)
	}

	if tr.ReadBufferSize != 32*1024 {
		t.Errorf("ReadBufferSize: got %d, want %d", tr.ReadBufferSize, 32*1024)
	}
	// WriteBufferSize unset → defaults to 64 KB
	if tr.WriteBufferSize != 64*1024 {
		t.Errorf("WriteBufferSize: got %d, want %d (default)", tr.WriteBufferSize, 64*1024)
	}
}

func TestTransportBufferSize_ZeroFallsBackToDefault(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:             "http://localhost:9428",
		Cache:                  c,
		LogLevel:               "error",
		BackendReadBufferSize:  0, // explicit zero → should use 64 KB default
		BackendWriteBufferSize: 0,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tr, ok := p.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", p.client.Transport)
	}

	const want = 64 * 1024
	if tr.ReadBufferSize != want {
		t.Errorf("ReadBufferSize with zero config: got %d, want %d", tr.ReadBufferSize, want)
	}
	if tr.WriteBufferSize != want {
		t.Errorf("WriteBufferSize with zero config: got %d, want %d", tr.WriteBufferSize, want)
	}
}
