package middleware

import (
	"bufio"
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGzipHandler_CompressesWhenAccepted(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":["app","level","namespace"]}`))
	})

	handler := GzipHandler(inner)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Accept-Encoding", "gzip, deflate")

	handler.ServeHTTP(w, r)

	if w.Header().Get("Content-Encoding") != "gzip" {
		t.Error("expected Content-Encoding: gzip")
	}
	if w.Header().Get("Vary") != "Accept-Encoding" {
		t.Error("expected Vary: Accept-Encoding")
	}

	// Decompress and verify
	gr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("invalid gzip: %v", err)
	}
	defer gr.Close()
	body, _ := io.ReadAll(gr)
	if !strings.Contains(string(body), "success") {
		t.Errorf("expected success in body, got %q", body)
	}
}

func TestGzipHandler_NoCompressionWithoutAcceptHeader(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success"}`))
	})

	handler := GzipHandler(inner)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	// No Accept-Encoding header

	handler.ServeHTTP(w, r)

	if w.Header().Get("Content-Encoding") == "gzip" {
		t.Error("should not gzip without Accept-Encoding")
	}
	if w.Body.String() != `{"status":"success"}` {
		t.Errorf("unexpected body: %q", w.Body.String())
	}
}

func TestGzipHandler_LargeResponse(t *testing.T) {
	// Large response should compress well
	data := strings.Repeat(`{"stream":{"app":"nginx"},"values":[["1234","log line"]]},`, 1000)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(data))
	})

	handler := GzipHandler(inner)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Accept-Encoding", "gzip")

	handler.ServeHTTP(w, r)

	// Compressed size should be much smaller
	compressedSize := w.Body.Len()
	originalSize := len(data)
	ratio := float64(compressedSize) / float64(originalSize) * 100
	t.Logf("Compression ratio: %d → %d bytes (%.1f%%)", originalSize, compressedSize, ratio)

	if compressedSize >= originalSize {
		t.Error("expected compressed size to be smaller than original")
	}
}

func TestGzipHandler_FlushSupport(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("chunk1"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		w.Write([]byte("chunk2"))
	})

	handler := GzipHandler(inner)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Accept-Encoding", "gzip")

	handler.ServeHTTP(w, r)

	// Should still produce valid gzip
	gr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("invalid gzip after flush: %v", err)
	}
	body, _ := io.ReadAll(gr)
	gr.Close()
	if string(body) != "chunk1chunk2" {
		t.Errorf("expected chunk1chunk2, got %q", body)
	}
}

type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (r *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	r.hijacked = true
	return nil, bufio.NewReadWriter(bufio.NewReader(strings.NewReader("")), bufio.NewWriter(io.Discard)), nil
}

func TestGzipHandler_SkipsWebSocketUpgrades(t *testing.T) {
	recorder := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	var sawHijacker bool

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawHijacker = w.(http.Hijacker)
		w.WriteHeader(http.StatusSwitchingProtocols)
	})

	handler := GzipHandler(inner)
	req := httptest.NewRequest(http.MethodGet, "/tail", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")

	handler.ServeHTTP(recorder, req)

	if !sawHijacker {
		t.Fatal("expected websocket upgrade request to preserve Hijacker support")
	}
	if got := recorder.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected websocket upgrade to skip gzip, got %q", got)
	}
}

func TestGzipResponseWriter_WriteHeaderDeletesContentLength(t *testing.T) {
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Content-Length", "123")
	gz := gzip.NewWriter(io.Discard)
	defer gz.Close()

	writer := &gzipResponseWriter{
		ResponseWriter: recorder,
		writer:         gz,
	}
	writer.WriteHeader(http.StatusAccepted)

	if writer.statusCode != http.StatusAccepted {
		t.Fatalf("expected status code to be tracked, got %d", writer.statusCode)
	}
	if got := recorder.Header().Get("Content-Length"); got != "" {
		t.Fatalf("expected content length to be removed, got %q", got)
	}
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected recorder status 202, got %d", recorder.Code)
	}
}

func TestGzipResponseWriter_Hijack(t *testing.T) {
	gz := gzip.NewWriter(io.Discard)
	defer gz.Close()

	t.Run("supported", func(t *testing.T) {
		recorder := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
		writer := &gzipResponseWriter{
			ResponseWriter: recorder,
			writer:         gz,
		}

		_, _, err := writer.Hijack()
		if err != nil {
			t.Fatalf("expected hijack to succeed, got %v", err)
		}
		if !recorder.hijacked {
			t.Fatal("expected underlying hijacker to be used")
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		writer := &gzipResponseWriter{
			ResponseWriter: httptest.NewRecorder(),
			writer:         gz,
		}

		if _, _, err := writer.Hijack(); err == nil {
			t.Fatal("expected hijack to fail when unsupported")
		}
	})
}
