package proxy

import (
	"bytes"
	gzip "github.com/klauspost/compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/klauspost/compress/zstd"
)

func newCompressionTestProxy(t *testing.T, backendURL string, backendCompression string) *Proxy {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:         backendURL,
		Cache:              c,
		LogLevel:           "error",
		BackendCompression: backendCompression,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func TestApplyBackendHeaders_DefaultsToAutoCompression(t *testing.T) {
	p := newCompressionTestProxy(t, "http://example.com", "")
	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)

	p.applyBackendHeaders(req)

	if got := req.Header.Get("Accept-Encoding"); got != "zstd, gzip" {
		t.Fatalf("expected auto backend compression, got %q", got)
	}
}

func TestApplyBackendHeaders_RespectsExplicitCompressionMode(t *testing.T) {
	p := newCompressionTestProxy(t, "http://example.com", "gzip")
	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)

	p.applyBackendHeaders(req)

	if got := req.Header.Get("Accept-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip backend compression, got %q", got)
	}
}

func TestVLGet_DecodesZstdResponse(t *testing.T) {
	payload := []byte(`{"status":"success"}`)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "zstd" {
			t.Fatalf("expected zstd accept-encoding, got %q", got)
		}
		w.Header().Set("Content-Encoding", "zstd")
		zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			t.Fatalf("create zstd writer: %v", err)
		}
		if _, err := zw.Write(payload); err != nil {
			t.Fatalf("write zstd payload: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close zstd writer: %v", err)
		}
	}))
	defer backend.Close()

	p := newCompressionTestProxy(t, backend.URL, "zstd")
	resp, err := p.vlGet(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}})
	if err != nil {
		t.Fatalf("vlGet returned error: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read decoded body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected decoded body %q", string(body))
	}
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Fatalf("expected content-encoding to be stripped, got %q", got)
	}
}

func TestAlertingBackendGet_DecodesGzipResponse(t *testing.T) {
	payload := []byte(`{"status":"success"}`)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
			t.Fatalf("expected gzip accept-encoding, got %q", got)
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		if _, err := gz.Write(payload); err != nil {
			t.Fatalf("write gzip payload: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("close gzip writer: %v", err)
		}
	}))
	defer backend.Close()

	p := newCompressionTestProxy(t, "http://example.com", "gzip")
	u, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "http://proxy.test/loki/api/v1/rules", nil)
	resp, err := p.alertingBackendGet(req, u, "/api/v1/rules")
	if err != nil {
		t.Fatalf("alertingBackendGet returned error: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read decoded body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected decoded body %q", string(body))
	}
}
