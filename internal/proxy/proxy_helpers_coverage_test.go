package proxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushRecorder) Flush() {
	f.flushed = true
	f.ResponseRecorder.Flush()
}

func TestCompatCacheCaptureWriter_HelperBranches(t *testing.T) {
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	w := newCompatCacheCaptureWriter(rec, 4)

	w.WriteHeader(http.StatusCreated)
	w.capture([]byte("ab"))
	w.Flush()
	if !w.flushed || !rec.flushed {
		t.Fatalf("expected flush to be recorded on wrapper and inner recorder")
	}

	w.capture([]byte("cde"))
	if body := w.CapturedBody(); body != nil {
		t.Fatalf("expected overflowed capture body to be nil, got %q", string(body))
	}
	if !w.overflowed {
		t.Fatalf("expected capture writer to mark overflow")
	}

	w.Release()
	if w.body != nil || w.bufHolder != nil {
		t.Fatalf("expected capture buffers to be released")
	}

	buf, holder := acquireCompatCaptureBuf(defaultCompatCaptureBufSize * 2)
	if cap(buf) < defaultCompatCaptureBufSize {
		t.Fatalf("expected pooled buffer capacity to be initialized, got %d", cap(buf))
	}
	releaseCompatCaptureBuf(make([]byte, 0, maxPooledCompatCaptureBufSize+1), holder)
	releaseCompatCaptureBuf(nil, nil)
}

func TestCompatCacheCaptureAllowedAndPatternsPayloadEmpty(t *testing.T) {
	cookieHeader := http.Header{}
	cookieHeader.Add("Set-Cookie", "sid=abc")

	tests := []struct {
		name    string
		code    int
		flushed bool
		header  http.Header
		want    bool
	}{
		{name: "default ok json", code: 0, header: http.Header{}, want: true},
		{name: "non-200", code: http.StatusBadGateway, header: http.Header{}, want: false},
		{name: "flushed", code: http.StatusOK, flushed: true, header: http.Header{}, want: false},
		{name: "cookie", code: http.StatusOK, header: cookieHeader, want: false},
		{name: "plain text", code: http.StatusOK, header: http.Header{"Content-Type": []string{"text/plain"}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compatCacheCaptureAllowed(tt.code, tt.flushed, tt.header); got != tt.want {
				t.Fatalf("compatCacheCaptureAllowed() = %v, want %v", got, tt.want)
			}
		})
	}

	if !patternsPayloadEmpty(nil) {
		t.Fatalf("expected empty body to be treated as empty payload")
	}
	if !patternsPayloadEmpty([]byte(`{"data":[]}`)) {
		t.Fatalf("expected empty data array to be treated as empty payload")
	}
	if patternsPayloadEmpty([]byte(`{"data":[{}]}`)) {
		t.Fatalf("expected non-empty data array to be treated as non-empty payload")
	}
	if patternsPayloadEmpty([]byte(`{not-json`)) {
		t.Fatalf("expected invalid json payload to be treated as non-empty/unknown")
	}
}

func TestNormalizeBackendCompressionAndLimiter(t *testing.T) {
	if got := normalizeBackendCompression(" GZIP "); got != "gzip" {
		t.Fatalf("expected gzip normalization, got %q", got)
	}
	if got := normalizeBackendCompression("bogus"); got != "auto" {
		t.Fatalf("expected unknown backend compression to fall back to auto, got %q", got)
	}
	if limiter := buildConcurrencyLimiter(0); limiter != nil {
		t.Fatalf("expected zero limit to disable limiter")
	}
	if limiter := buildConcurrencyLimiter(3); cap(limiter) != 3 {
		t.Fatalf("expected limiter capacity 3, got %d", cap(limiter))
	}
}

func TestDecodeCompressedHTTPResponse_Variants(t *testing.T) {
	original := []byte(`{"status":"ok"}`)

	t.Run("identity", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{"identity"}},
			Body:   io.NopCloser(bytes.NewReader(original)),
		}
		if err := decodeCompressedHTTPResponse(resp); err != nil {
			t.Fatalf("identity decode failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != string(original) {
			t.Fatalf("unexpected identity body %q", string(body))
		}
	})

	t.Run("gzip", func(t *testing.T) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(original); err != nil {
			t.Fatalf("gzip write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}

		resp := &http.Response{
			Header:        http.Header{"Content-Encoding": []string{"x-gzip"}, "Content-Length": []string{"123"}},
			Body:          io.NopCloser(bytes.NewReader(buf.Bytes())),
			ContentLength: 123,
		}
		if err := decodeCompressedHTTPResponse(resp); err != nil {
			t.Fatalf("gzip decode failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != string(original) {
			t.Fatalf("unexpected gzip body %q", string(body))
		}
		if got := resp.Header.Get("Content-Encoding"); got != "" || resp.ContentLength != -1 || !resp.Uncompressed {
			t.Fatalf("expected decoded response metadata to be cleared, got encoding=%q len=%d uncompressed=%v", got, resp.ContentLength, resp.Uncompressed)
		}
	})

	t.Run("zstd", func(t *testing.T) {
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("zstd writer: %v", err)
		}
		if _, err := zw.Write(original); err != nil {
			t.Fatalf("zstd write: %v", err)
		}
		zw.Close()

		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{"zstd"}},
			Body:   io.NopCloser(bytes.NewReader(buf.Bytes())),
		}
		if err := decodeCompressedHTTPResponse(resp); err != nil {
			t.Fatalf("zstd decode failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != string(original) {
			t.Fatalf("unexpected zstd body %q", string(body))
		}
	})

	t.Run("invalid gzip", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{"gzip"}},
			Body:   io.NopCloser(bytes.NewReader([]byte("not-gzip"))),
		}
		if err := decodeCompressedHTTPResponse(resp); err == nil {
			t.Fatalf("expected invalid gzip stream to fail")
		}
	})
}
