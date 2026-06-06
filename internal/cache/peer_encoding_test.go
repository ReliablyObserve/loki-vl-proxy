package cache

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestAcceptsPeerEncoding(t *testing.T) {
	tests := []struct {
		header, want string
		ok           bool
	}{
		{"", "zstd", false},
		{"zstd", "zstd", true},
		{"gzip, deflate, br, zstd", "zstd", true},
		{"gzip;q=1.0, zstd;q=0.9", "zstd", true},
		{"*", "zstd", true},
		{"identity", "zstd", false},
		{"gzip", "zstd", false},
		{"GZIP, ZSTD", "zstd", true}, // case-insensitive
	}
	for _, tc := range tests {
		got := acceptsPeerEncoding(tc.header, tc.want)
		if got != tc.ok {
			t.Errorf("acceptsPeerEncoding(%q, %q) = %v, want %v", tc.header, tc.want, got, tc.ok)
		}
	}
}

func TestEncodePeerRequestBody(t *testing.T) {
	body := bytes.Repeat([]byte("abcdefghij"), 200) // 2 KB

	t.Run("identity", func(t *testing.T) {
		out, err := encodePeerRequestBody(body, "identity")
		if err != nil {
			t.Fatalf("identity: %v", err)
		}
		if !bytes.Equal(out, body) {
			t.Errorf("identity must pass body through unchanged")
		}
	})

	t.Run("empty_encoding_is_identity", func(t *testing.T) {
		out, err := encodePeerRequestBody(body, "")
		if err != nil {
			t.Fatalf("empty: %v", err)
		}
		if !bytes.Equal(out, body) {
			t.Errorf("empty encoding must behave as identity")
		}
	})

	t.Run("zstd_roundtrip", func(t *testing.T) {
		out, err := encodePeerRequestBody(body, "zstd")
		if err != nil {
			t.Fatalf("zstd: %v", err)
		}
		dec, err := zstd.NewReader(bytes.NewReader(out))
		if err != nil {
			t.Fatalf("zstd reader: %v", err)
		}
		defer dec.Close()
		decoded, _ := io.ReadAll(dec)
		if !bytes.Equal(decoded, body) {
			t.Errorf("zstd roundtrip mismatch")
		}
	})

	t.Run("gzip_roundtrip", func(t *testing.T) {
		out, err := encodePeerRequestBody(body, "gzip")
		if err != nil {
			t.Fatalf("gzip: %v", err)
		}
		dec, err := gzip.NewReader(bytes.NewReader(out))
		if err != nil {
			t.Fatalf("gzip reader: %v", err)
		}
		decoded, _ := io.ReadAll(dec)
		if !bytes.Equal(decoded, body) {
			t.Errorf("gzip roundtrip mismatch")
		}
	})

	t.Run("unsupported_returns_error", func(t *testing.T) {
		_, err := encodePeerRequestBody(body, "brotli")
		if err == nil {
			t.Error("expected error for unsupported encoding")
		}
		if !strings.Contains(err.Error(), "unsupported") {
			t.Errorf("expected 'unsupported' in error, got %v", err)
		}
	})

	t.Run("case_and_whitespace_tolerant", func(t *testing.T) {
		// Encoding name is trimmed and lower-cased.
		out, err := encodePeerRequestBody(body, "  ZSTD  ")
		if err != nil {
			t.Fatalf("ZSTD with whitespace: %v", err)
		}
		if len(out) == 0 || bytes.Equal(out, body) {
			t.Errorf("expected encoded output, got %d bytes (equal to input: %v)", len(out), bytes.Equal(out, body))
		}
	})
}

func TestWritePeerEncodedResponse_SmallBody_NoCompression(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	small := []byte("tiny")
	writePeerEncodedResponse(rec, req, small)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Errorf("small body should not be compressed, got Content-Encoding=%q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), small) {
		t.Errorf("small body should pass through unchanged")
	}
}

func TestWritePeerEncodedResponse_ZstdWhenAccepted(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "zstd")

	body := bytes.Repeat([]byte("payload"), 1000) // >> peerCompressionMinBytes
	writePeerEncodedResponse(rec, req, body)

	if got := rec.Header().Get("Content-Encoding"); got != "zstd" {
		t.Errorf("want Content-Encoding=zstd, got %q", got)
	}
	dec, _ := zstd.NewReader(bytes.NewReader(rec.Body.Bytes()))
	defer dec.Close()
	decoded, _ := io.ReadAll(dec)
	if !bytes.Equal(decoded, body) {
		t.Errorf("zstd-encoded body roundtrip mismatch")
	}
}

func TestWritePeerEncodedResponse_GzipFallback(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip") // no zstd

	body := bytes.Repeat([]byte("payload"), 1000)
	writePeerEncodedResponse(rec, req, body)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("want Content-Encoding=gzip, got %q", got)
	}
	dec, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decoded, _ := io.ReadAll(dec)
	if !bytes.Equal(decoded, body) {
		t.Errorf("gzip-encoded body roundtrip mismatch")
	}
}

func TestWritePeerEncodedResponse_NoAcceptedEncoding(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "deflate, br") // proxy supports neither

	body := bytes.Repeat([]byte("payload"), 1000)
	writePeerEncodedResponse(rec, req, body)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Errorf("want no Content-Encoding when none accepted, got %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Errorf("body should be sent raw when no supported encoding accepted")
	}
}

func TestPeerPreferredSetEncoding(t *testing.T) {
	pc := NewPeerCache(PeerConfig{SelfAddr: "self"})
	defer pc.Close()

	t.Run("unset_returns_false", func(t *testing.T) {
		v, ok := pc.peerPreferredSetEncoding("peer-x")
		if ok || v != "" {
			t.Errorf("unset peer: want (\"\", false), got (%q, %v)", v, ok)
		}
	})

	t.Run("empty_addr_returns_false", func(t *testing.T) {
		v, ok := pc.peerPreferredSetEncoding("")
		if ok || v != "" {
			t.Errorf("empty addr: want (\"\", false), got (%q, %v)", v, ok)
		}
	})

	t.Run("nil_receiver_safe", func(t *testing.T) {
		var nilPC *PeerCache
		v, ok := nilPC.peerPreferredSetEncoding("x")
		if ok || v != "" {
			t.Errorf("nil receiver: want (\"\", false), got (%q, %v)", v, ok)
		}
	})

	t.Run("recalls_stored", func(t *testing.T) {
		pc.peerSetEncodings.Store("peer-z", "zstd")
		v, ok := pc.peerPreferredSetEncoding("peer-z")
		if !ok || v != "zstd" {
			t.Errorf("stored encoding: want (\"zstd\", true), got (%q, %v)", v, ok)
		}
	})

	t.Run("blank_stored_returns_false", func(t *testing.T) {
		pc.peerSetEncodings.Store("peer-blank", "   ")
		v, ok := pc.peerPreferredSetEncoding("peer-blank")
		if ok || v != "" {
			t.Errorf("blank stored: want (\"\", false), got (%q, %v)", v, ok)
		}
	})
}
