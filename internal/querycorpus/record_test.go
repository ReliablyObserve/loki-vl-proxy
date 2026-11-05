//go:build !corpuscapture

package querycorpus

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Without the corpuscapture tag the route wrapper is the handler itself, so
// production requests take no extra step.
func TestHandlerIsTheHandlerWithoutCapture(t *testing.T) {
	called := false
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	got := Handler(h)
	got.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/?query=x", nil))
	if !called {
		t.Fatal("handler was not called")
	}
	if _, ok := got.(http.HandlerFunc); !ok {
		t.Fatalf("Handler wrapped the handler: %T", got)
	}
	Record(LogQL, `{app="a"}`) // must be a no-op
	RecordRequest(httptest.NewRequest(http.MethodGet, "/", nil), `{app="a"}`)
}
