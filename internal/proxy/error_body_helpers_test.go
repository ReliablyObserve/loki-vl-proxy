package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// errBodyView is what a client reads of an error answer from a route where
// Loki writes text/plain (pkg/util/server/error.go WriteError): the status
// field and the errorType are what a Prometheus-style client would infer, the
// text is the whole body.
type errBodyView struct {
	Status    string
	ErrorType string
	Error     string
}

// readErrorBody asserts a body is Loki's query frontend error shape (text/plain,
// nosniff, the message alone, no JSON envelope) and returns it as a view.
func readErrorBody(t testing.TB, code int, header http.Header, body []byte) errBodyView {
	t.Helper()
	if got := header.Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Fatalf("error Content-Type = %q, want Loki's text/plain; charset=utf-8; body=%q", got, body)
	}
	if got := header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("error X-Content-Type-Options = %q, want nosniff", got)
	}
	text := string(body)
	if strings.HasPrefix(text, `{"status"`) || strings.HasPrefix(text, `{"error"`) {
		t.Fatalf("error body is the old JSON envelope, want Loki's raw text: %q", text)
	}
	return errBodyView{Status: "error", ErrorType: lokiErrorType(code), Error: text}
}

// readErrorRec is readErrorBody for a recorded response.
func readErrorRec(t testing.TB, rec *httptest.ResponseRecorder) errBodyView {
	t.Helper()
	return readErrorBody(t, rec.Code, rec.Header(), rec.Body.Bytes())
}
