package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A row of the cold tier (or of the hot half of a merged query) that is not JSON is a failed read: the client
// gets Loki's 500, never a 200 without the row. A body with no rows is an empty result.
//
// conformance: semantics/error-layer-silent-success
func TestColdTierLogQuery_RowThatIsNotJSONFailsTheRequest(t *testing.T) {
	const good = `{"_msg":"line","_time":"2026-04-01T00:00:01Z","_stream":"{}"}` + "\n"
	now := time.Now()
	for _, tc := range []struct {
		name      string
		direction string
		both      bool
		hot, cold string
		wantOK    bool
	}{
		{"cold only backward", "backward", false, "", good + "VLSENTINEL\n", false},
		{"cold only forward", "forward", false, "", good + "VLSENTINEL\n", false},
		{"merged backward, cold row", "backward", true, good, good + "VLSENTINEL\n", false},
		{"merged forward, cold row", "forward", true, good, good + "VLSENTINEL\n", false},
		{"merged backward, hot row", "backward", true, good + "VLSENTINEL\n", good, false},
		{"merged forward, hot row", "forward", true, good + "VLSENTINEL\n", good, false},
		{"cold only, no rows", "backward", false, "", "", true},
		{"merged, no rows", "forward", true, "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serve := func(body string) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/stream+json")
					_, _ = w.Write([]byte(body))
				}))
			}
			hot, cold := serve(tc.hot), serve(tc.cold)
			defer hot.Close()
			defer cold.Close()
			p, err := New(Config{BackendURL: hot.URL, ColdBackend: ColdBackendConfig{Enabled: true, URL: cold.URL, Boundary: 7 * 24 * time.Hour}})
			if err != nil {
				t.Fatal(err)
			}
			start, end := now.Add(-30*24*time.Hour), now.Add(-20*24*time.Hour)
			if tc.both {
				start, end = now.Add(-10*24*time.Hour), now.Add(-time.Hour)
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", fmt.Sprintf("/?query=*&direction=%s&start=%d&end=%d", tc.direction, start.UnixNano(), end.UnixNano()), nil)
			if tc.both {
				p.proxyLogQueryBoth(w, r, "*")
			} else {
				p.proxyLogQueryCold(w, r, "*")
			}
			if tc.wantOK {
				if w.Code != http.StatusOK {
					t.Fatalf("status %d, body %.200q", w.Code, w.Body.String())
				}
				return
			}
			if w.Code != http.StatusInternalServerError || w.Body.String() != lokiErrInternal {
				t.Fatalf("status %d, body %.200q, want Loki's 500 %q", w.Code, w.Body.String(), lokiErrInternal)
			}
			if strings.Contains(w.Body.String(), "VLSENTINEL") {
				t.Fatalf("body leaks the backend: %q", w.Body.String())
			}
		})
	}
}
