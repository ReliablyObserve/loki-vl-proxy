//go:build corpuscapture

package querycorpus

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func readEntries(t *testing.T, d string) []entry {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(d, "*.jsonl"))
	var out []entry
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			var e entry
			if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
				t.Fatal(err)
			}
			out = append(out, e)
		}
		fh.Close()
	}
	return out
}

// Record writes each query once per kind and calling test, names the test
// it came from, and marks a query passed in by a test as direct. Handler
// records the raw query parameter of a request.
func TestRecordAttributesAndDeduplicates(t *testing.T) {
	d := t.TempDir()
	mu.Lock()
	oldDir, oldOut, oldSeen := dir, out, seen
	dir, out, seen = d, nil, map[entry]struct{}{}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		if out != nil {
			out.Close()
		}
		dir, out, seen = oldDir, oldOut, oldSeen
		mu.Unlock()
	})

	Record(LogQL, `{app="a"}`)
	Record(LogQL, `{app="a"}`)
	Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, `/q?query=%7Bapp%3D%22b%22%7D`, nil))

	got := readEntries(t, d)
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(got), got)
	}
	const src = "internal/querycorpus/record_capture_test.go:TestRecordAttributesAndDeduplicates"
	if got[0].Kind != LogQL || got[0].Query != `{app="a"}` || got[0].Source != src || !got[0].Direct {
		t.Fatalf("parser entry = %+v", got[0])
	}
	if got[1].Kind != Client || got[1].Query != `{app="b"}` || got[1].Source != src || !got[1].Direct {
		t.Fatalf("route entry = %+v", got[1])
	}
}
