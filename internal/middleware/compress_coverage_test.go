package middleware

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type unwrapResponseWriter struct {
	http.ResponseWriter
	next http.ResponseWriter
}

func (u *unwrapResponseWriter) Unwrap() http.ResponseWriter {
	return u.next
}

type selfUnwrappingResponseWriter struct {
	http.ResponseWriter
}

func (s *selfUnwrappingResponseWriter) Unwrap() http.ResponseWriter {
	return s
}

func decodeGzipBody(t *testing.T, body []byte) string {
	t.Helper()

	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer gr.Close()
	decoded, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	return string(decoded)
}

func TestCompressedResponseWriter_DirectBranches(t *testing.T) {
	t.Run("started bypass write uses safe headers", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		writer := &compressedResponseWriter{
			ResponseWriter: recorder,
			started:        true,
			bypass:         true,
		}

		if _, err := writer.Write([]byte("plain")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := recorder.Body.String(); got != "plain" {
			t.Fatalf("unexpected body %q", got)
		}
		if got := recorder.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("unexpected content-type %q", got)
		}
		if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("unexpected nosniff header %q", got)
		}
	})

	t.Run("started compression write sends gzip payload", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		writer := &compressedResponseWriter{
			ResponseWriter: recorder,
			encoding:       "gzip",
			minBytes:       1,
		}

		if err := writer.startCompression(); err != nil {
			t.Fatalf("startCompression: %v", err)
		}
		if _, err := writer.Write([]byte("hello")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := writer.finish(); err != nil {
			t.Fatalf("finish: %v", err)
		}

		if got := recorder.Header().Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("unexpected content-encoding %q", got)
		}
		if got := decodeGzipBody(t, recorder.Body.Bytes()); got != "hello" {
			t.Fatalf("unexpected decoded body %q", got)
		}
	})

	t.Run("flush bypasses when status forbids body compression", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		writer := &compressedResponseWriter{
			ResponseWriter: recorder,
			encoding:       "gzip",
			statusCode:     http.StatusNoContent,
		}

		writer.Flush()

		if !writer.started || !writer.bypass {
			t.Fatalf("expected flush to start bypass path, got started=%v bypass=%v", writer.started, writer.bypass)
		}
		if got := recorder.Header().Get("Content-Encoding"); got != "" {
			t.Fatalf("expected no compression for 204, got %q", got)
		}
	})
}

func TestCompressionHelpers_AdditionalCoverage(t *testing.T) {
	t.Run("register encoded capture direct branches", func(t *testing.T) {
		writer := &compressedResponseWriter{encoding: "gzip"}
		if ok := writer.registerEncodedResponseCapture("gzip", nil); ok {
			t.Fatal("expected nil callback registration to fail")
		}

		writer.started = true
		if ok := writer.registerEncodedResponseCapture("gzip", func(string, []byte) {}); ok {
			t.Fatal("expected started writer registration to fail")
		}

		writer.started = false
		if ok := writer.registerEncodedResponseCapture("identity", func(string, []byte) {}); ok {
			t.Fatal("expected encoding mismatch registration to fail")
		}

		var capturedEncoding string
		var capturedBody []byte
		if ok := writer.registerEncodedResponseCapture("gzip", func(encoding string, body []byte) {
			capturedEncoding = encoding
			capturedBody = append([]byte(nil), body...)
		}); !ok {
			t.Fatal("expected matching registration to succeed")
		}

		writer.capture.buf.WriteString("payload")
		writer.finalizeEncodedCapture()
		if capturedEncoding != "gzip" || string(capturedBody) != "payload" {
			t.Fatalf("unexpected capture %q %q", capturedEncoding, string(capturedBody))
		}
		writer.finalizeEncodedCapture()
	})

	t.Run("register encoded capture unwraps and rejects loops", func(t *testing.T) {
		target := &compressedResponseWriter{ResponseWriter: httptest.NewRecorder(), encoding: "gzip"}
		wrapped := &unwrapResponseWriter{ResponseWriter: httptest.NewRecorder(), next: target}

		if ok := RegisterEncodedResponseCapture(wrapped, "gzip", func(string, []byte) {}); !ok {
			t.Fatal("expected unwrap chain to reach registrar")
		}

		loop := &selfUnwrappingResponseWriter{ResponseWriter: httptest.NewRecorder()}
		if ok := RegisterEncodedResponseCapture(loop, "gzip", func(string, []byte) {}); ok {
			t.Fatal("expected self-unwrapping writer to be rejected")
		}
	})

	t.Run("ensure safe headers handles nil and existing headers", func(t *testing.T) {
		ensureSafeResponseHeaders(nil, "text/plain; charset=utf-8")

		recorder := httptest.NewRecorder()
		recorder.Header().Set("Content-Type", "application/json")
		ensureSafeResponseHeaders(recorder, "")
		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("unexpected content-type overwrite %q", got)
		}
		if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("expected nosniff, got %q", got)
		}
	})

	t.Run("normalize and plan response compression", func(t *testing.T) {
		if got := normalizeCompressionMode("zstd"); got != ResponseCompressionGzip {
			t.Fatalf("expected zstd alias to gzip, got %q", got)
		}
		if got := normalizeCompressionMode("weird"); got != ResponseCompressionAuto {
			t.Fatalf("expected invalid mode to fall back to auto, got %q", got)
		}

		if encoding, minBytes := PlanResponseCompression(nil, CompressionOptions{Mode: "gzip", MinBytes: 16}); encoding != "" || minBytes != 32 {
			t.Fatalf("unexpected nil-request plan: encoding=%q minBytes=%d", encoding, minBytes)
		}

		headReq := httptest.NewRequest(http.MethodHead, "/loki/api/v1/query_range", nil)
		headReq.Header.Set("Accept-Encoding", "gzip")
		if encoding, minBytes := PlanResponseCompression(headReq, CompressionOptions{Mode: "gzip", MinBytes: 16}); encoding != "" || minBytes != 16 {
			t.Fatalf("unexpected HEAD plan: encoding=%q minBytes=%d", encoding, minBytes)
		}

		wsReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
		wsReq.Header.Set("Connection", "Upgrade")
		wsReq.Header.Set("Upgrade", "websocket")
		wsReq.Header.Set("Accept-Encoding", "gzip")
		if encoding, _ := PlanResponseCompression(wsReq, CompressionOptions{Mode: "gzip", MinBytes: 16}); encoding != "" {
			t.Fatalf("expected websocket upgrade to bypass compression, got %q", encoding)
		}

		controlReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		controlReq.Header.Set("Accept-Encoding", "gzip")
		if encoding, minBytes := PlanResponseCompression(controlReq, CompressionOptions{Mode: "gzip", MinBytes: 16}); encoding != "" || minBytes != 0 {
			t.Fatalf("unexpected control-plane plan: encoding=%q minBytes=%d", encoding, minBytes)
		}

		metaReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
		metaReq.Header.Set("Accept-Encoding", "gzip")
		if encoding, minBytes := PlanResponseCompression(metaReq, CompressionOptions{Mode: "gzip", MinBytes: 16}); encoding != "gzip" || minBytes != 64 {
			t.Fatalf("unexpected metadata plan: encoding=%q minBytes=%d", encoding, minBytes)
		}
	})

	t.Run("encode response body and helpers", func(t *testing.T) {
		identity, err := EncodeResponseBody("", []byte("hello"))
		if err != nil {
			t.Fatalf("identity encode: %v", err)
		}
		if string(identity) != "hello" {
			t.Fatalf("unexpected identity body %q", string(identity))
		}

		encoded, err := EncodeResponseBody("gzip", []byte("hello"))
		if err != nil {
			t.Fatalf("gzip encode: %v", err)
		}
		if got := decodeGzipBody(t, encoded); got != "hello" {
			t.Fatalf("unexpected gzip-decoded body %q", got)
		}

		if _, err := EncodeResponseBody("brotli", []byte("hello")); err == nil {
			t.Fatal("expected unsupported encoding error")
		}

		if !responseStatusAllowsBody(0) || responseStatusAllowsBody(101) || responseStatusAllowsBody(http.StatusNoContent) || responseStatusAllowsBody(http.StatusNotModified) == true || !responseStatusAllowsBody(http.StatusOK) {
			t.Fatal("unexpected responseStatusAllowsBody results")
		}

		header := http.Header{}
		addVaryHeader(header, "Accept-Encoding")
		addVaryHeader(header, "accept-encoding")
		if values := header.Values("Vary"); len(values) != 1 || values[0] != "Accept-Encoding" {
			t.Fatalf("unexpected vary header values %#v", values)
		}
	})
}
