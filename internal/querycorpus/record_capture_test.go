//go:build corpuscapture

package querycorpus

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Record writes each query once per kind and calling test, names the test
// it came from, and marks a query passed in by a test as direct. Handler
// records the raw query parameter of a request, and RecordRequest records
// a handler's query only when the request did not come through Handler.
func TestRecordAttributesAndDeduplicates(t *testing.T) {
	got := CaptureForTest(t, func() {
		Record(LogQL, `{app="a"}`)
		Record(LogQL, `{app="a"}`)
		Handler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			RecordRequest(r, `{app="rewritten"}`)
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, `/q?query=%7Bapp%3D%22b%22%7D`, nil))
		RecordRequest(httptest.NewRequest(http.MethodGet, "/q", nil), `{app="c"}`)
	})
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(got), got)
	}
	const src = "internal/querycorpus/record_capture_test.go:TestRecordAttributesAndDeduplicates"
	if got[0].Kind != LogQL || got[0].Query != `{app="a"}` || got[0].Source != src || !got[0].Direct {
		t.Fatalf("parser entry = %+v", got[0])
	}
	if got[1].Kind != Client || got[1].Query != `{app="b"}` || got[1].Source != src || !got[1].Direct {
		t.Fatalf("route entry = %+v", got[1])
	}
	if got[2].Kind != Client || got[2].Query != `{app="c"}` || got[2].Source != src || !got[2].Direct {
		t.Fatalf("direct handler entry = %+v", got[2])
	}
}
