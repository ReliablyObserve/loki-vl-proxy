package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHardening_ColdMergeProgressWithOneBackendPermit(t *testing.T) {
	for _, direction := range []string{"forward", "backward"} {
		t.Run(direction, func(t *testing.T) {
			now := time.Now().UTC()
			var calls atomic.Int32
			backend := func(name string, stamp time.Time) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/select/logsql/query" {
						http.NotFound(w, r)
						return
					}
					calls.Add(1)
					fmt.Fprintf(w, "{\"_msg\":%q,\"_time\":%q,\"_stream\":\"{}\"}\n", name, stamp.Format(time.RFC3339Nano))
				}))
			}
			hot := backend("hot-budget-line", now.Add(-time.Minute))
			defer hot.Close()
			cold := backend("cold-budget-line", now.Add(-8*24*time.Hour))
			defer cold.Close()
			p, err := New(Config{BackendURL: hot.URL, MaxConcurrent: 1, ColdBackend: ColdBackendConfig{Enabled: true, URL: cold.URL, Boundary: 7 * 24 * time.Hour}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Shutdown(context.Background())
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/?query=*&direction=%s&limit=2&start=%d&end=%d", direction, now.Add(-8*24*time.Hour).UnixNano(), now.UnixNano()), nil).WithContext(ctx)
			rec := httptest.NewRecorder()
			p.proxyLogQueryBoth(rec, req, "*")
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "hot-budget-line") || !strings.Contains(rec.Body.String(), "cold-budget-line") {
				t.Fatalf("healthy cross-boundary query failed: status=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
			}
			if ctx.Err() != nil || len(p.backendBudget) != 0 {
				t.Fatalf("merge exhausted its deadline or retained a permit: context=%v permits=%d", ctx.Err(), len(p.backendBudget))
			}
		})
	}
}

type coldMergeTestBody struct {
	io.Reader
	closed bool
}

func (b *coldMergeTestBody) Close() error { b.closed = true; return nil }

type coldMergeFailReader struct{}

func (coldMergeFailReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestHardening_ColdMergeBufferClosesAndRejectsPartialSuccess(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reader  io.Reader
		status  int
		wantErr error
	}{
		{"complete", strings.NewReader("1234"), 200, nil},
		{"overflow", strings.NewReader("12345"), 200, errBodyTooLarge},
		{"read_failure", coldMergeFailReader{}, 200, io.ErrUnexpectedEOF},
		{"upstream_error", strings.NewReader("denied"), 403, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &coldMergeTestBody{Reader: tc.reader}
			resp := &http.Response{StatusCode: tc.status, Body: body}
			err := bufferMergeResponse(resp, 4)
			if !errors.Is(err, tc.wantErr) || !body.closed || resp.StatusCode != tc.status {
				t.Fatalf("err=%v want=%v closed=%v status=%d", err, tc.wantErr, body.closed, resp.StatusCode)
			}
			if err == nil {
				data, readErr := io.ReadAll(resp.Body)
				if readErr != nil || len(data) == 0 {
					t.Fatalf("buffered response unavailable: %q %v", data, readErr)
				}
			}
		})
	}
}
