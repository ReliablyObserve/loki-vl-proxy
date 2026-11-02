package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// shippedChain wraps a handler with the middleware cmd/proxy wraps around the
// mux (wrapHandler): the security headers and the response compression, both
// of which preset X-Content-Type-Options and a default Content-Type that a
// route's error writer has to override.
func shippedChain(next http.Handler) http.Handler {
	return SecurityHeadersMiddleware(mw.CompressionHandlerWithOptions(next, mw.CompressionOptions{Mode: "auto", MinBytes: 1024}))
}

// errBodyView is what a client reads of an error answer from a route where
// Loki's query frontend writes the error (pkg/util/server/error.go WriteError):
// the Content-Type as sent and the whole body, which is the message.
type errBodyView struct {
	ContentType string
	Error       string
}

// readErrorBody asserts a body is Loki's query frontend error shape: text/plain
// with nosniff, the message alone with no JSON and no trailing newline (that is
// http.Error's, a different shape). It returns the Content-Type and the text.
// A status is not part of the shape; assert it from the response.
func readErrorBody(t testing.TB, header http.Header, body []byte) errBodyView {
	t.Helper()
	contentType := header.Get("Content-Type")
	if contentType != "text/plain; charset=utf-8" {
		t.Fatalf("error Content-Type = %q, want Loki's text/plain; charset=utf-8; body=%q", contentType, body)
	}
	if got := header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("error X-Content-Type-Options = %q, want nosniff", got)
	}
	text := string(body)
	if strings.HasPrefix(text, "{") {
		t.Fatalf("error body starts with %q, want Loki's raw message (no JSON): %q", "{", text)
	}
	if strings.HasSuffix(text, "\n") {
		t.Fatalf("error body ends in a newline, which is http.Error's shape, not WriteError's: %q", text)
	}
	return errBodyView{ContentType: contentType, Error: text}
}

// readErrorRec is readErrorBody for a recorded response that must be an error.
func readErrorRec(t testing.TB, rec *httptest.ResponseRecorder) errBodyView {
	t.Helper()
	if rec.Code < http.StatusBadRequest {
		t.Fatalf("status %d is not an error; body=%q", rec.Code, rec.Body.String())
	}
	return readErrorBody(t, rec.Header(), rec.Body.Bytes())
}
