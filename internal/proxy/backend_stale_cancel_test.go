package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// staleCancelTransport fails a call the way net/http does when a connection
// another call recycled is marked cancelled by the call that used it before:
// the request is written (its body read), then it fails with context.Canceled
// although its own context is live.
type staleCancelTransport struct {
	inner http.RoundTripper
	// fail decides, for the n-th call (from 1) to the path, whether it is
	// failed; it runs before the call reaches the backend.
	fail func(n int, req *http.Request, body string) bool
	// path limits the transport to one path (calls to others pass through).
	path string
	// err is what a failed call returns (context.Canceled by default).
	err error
	// cancelOwn makes a failed call cancel its own context first (a real
	// cancellation, which must not be repeated).
	cancelOwn context.CancelFunc

	mu     sync.Mutex
	calls  int
	bodies []string
}

func (s *staleCancelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.path != "" && req.URL.Path != s.path {
		return s.inner.RoundTrip(req)
	}
	var body string
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		body = string(raw)
		req.Body = io.NopCloser(strings.NewReader(body))
	}
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.bodies = append(s.bodies, body)
	s.mu.Unlock()
	if s.fail(n, req, body) {
		if s.cancelOwn != nil {
			s.cancelOwn()
		}
		if s.err != nil {
			return nil, s.err
		}
		return nil, context.Canceled
	}
	return s.inner.RoundTrip(req)
}

func (s *staleCancelTransport) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// A call whose own context is live is never answered with a cancellation: when
// the transport fails it with context.Canceled (a connection recycled from a
// sibling call that was cancelled at that moment, which net/http then marks
// cancelled), the call is sent again, on a connection of its own. A call that
// is really cancelled, or that fails for another reason, is not repeated.
// conformance: backend-deadlines-and-cancellation, semantics/live-request-not-cancelled-by-sibling
func TestBackendRequests_StaleConnectionCancelIsRetried(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = w.Write([]byte(`{"q":"` + r.Form.Get("query") + `"}` + "\n"))
	}))
	defer backend.Close()

	for _, tc := range []struct {
		name      string
		method    string // "POST" through vlPost, "GET" through vlGet
		path      string // default /select/logsql/query
		fail      func(n int, req *http.Request, body string) bool
		err       error
		cancelOwn bool
		wantCalls int
		wantErr   bool
	}{
		{"post, first attempt cancelled by a stale connection", "POST", "", func(n int, _ *http.Request, _ string) bool { return n == 1 }, nil, false, 2, false},
		{"get, first attempt cancelled by a stale connection", "GET", "", func(n int, _ *http.Request, _ string) bool { return n == 1 }, nil, false, 2, false},
		{"post, a second stale cancel is returned", "POST", "", func(int, *http.Request, string) bool { return true }, nil, false, 2, true},
		{"post, the caller cancelled its own context", "POST", "", func(int, *http.Request, string) bool { return true }, nil, true, 1, true},
		{"post to delete is not repeated", "POST", "/select/logsql/delete", func(int, *http.Request, string) bool { return true }, nil, false, 1, true},
		{"post, another failure is not repeated", "POST", "", func(int, *http.Request, string) bool { return true }, io.ErrUnexpectedEOF, false, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Millisecond, 10), LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			path := tc.path
			if path == "" {
				path = "/select/logsql/query"
			}
			rt := &staleCancelTransport{inner: p.client.Transport, path: path, fail: tc.fail, err: tc.err}
			if tc.cancelOwn {
				rt.cancelOwn = cancel
			}
			p.client.Transport = rt

			params := url.Values{"query": {`app:="api" | limit 7`}}
			var resp *http.Response
			if tc.method == "GET" {
				resp, err = p.vlGet(ctx, path, params)
			} else {
				resp, err = p.vlPost(ctx, path, params)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, want error %v", err, tc.wantErr)
			}
			if got := rt.count(); got != tc.wantCalls {
				t.Fatalf("%d calls reached the transport, want %d", got, tc.wantCalls)
			}
			if err != nil {
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(body), `app:=\"api\" | limit 7`) && !strings.Contains(string(body), `app:="api" | limit 7`) {
				t.Fatalf("the backend did not receive the query: %s", body)
			}
			if tc.method == "POST" && tc.wantCalls == 2 {
				// The second attempt carries the same arguments as the first.
				rt.mu.Lock()
				defer rt.mu.Unlock()
				if len(rt.bodies) != 2 || rt.bodies[0] == "" || rt.bodies[0] != rt.bodies[1] {
					t.Fatalf("the retry changed the request body: %q", rt.bodies)
				}
			}
		})
	}
}

// The detected_level volume of a stream severity (`| json | __error__=""`)
// asks VictoriaLogs for the presence of the level fields, a guard that finds
// the severity field (which sends the query to the raw rows) and a hazard
// probe, and cancels the calls still in flight when the guard answers. One of
// the raw-row calls that follows can meet a connection a cancelled sibling
// call poisoned; whatever the order, the client of a live request is answered.
// conformance: semantics/json-metric-detected-level-derivation, semantics/live-request-not-cancelled-by-sibling
func TestOrderedJSONDetectedLevelLiveRequestSurvivesStaleConnectionCancel(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(2*time.Minute)
	var probe detectedLevelProbe
	for _, candidate := range detectedLevelProbes {
		if candidate.name == "stream severity" {
			probe = candidate
		}
	}
	query := `sum by (detected_level) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	want := map[string]map[int64]string{
		canonicalLabelsKey(map[string]string{"detected_level": probe.want}): {end.Unix(): "1"},
	}
	srv, fake := newPushdownFakeVL(t, []pushdownRow{probe.row(s0.Add(100 * time.Second))}, nil)
	fake.presence = true
	p := newFilterPushdownProxy(t, srv.URL)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	fake.stored = p.labelTranslator.ToVL
	var failed atomic.Int32
	p.client.Transport = &staleCancelTransport{inner: p.client.Transport, path: "/select/logsql/query", fail: func(_ int, req *http.Request, body string) bool {
		form, _ := url.ParseQuery(body)
		raw := req.URL.Path == "/select/logsql/query" && strings.Contains(form.Get("query"), "| limit 1000001")
		return raw && failed.CompareAndSwap(0, 1)
	}}
	code, got, body := detectedLevelQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
	}
	if failed.Load() != 1 {
		t.Fatal("the stale cancellation was never injected: the test no longer reaches the raw rows")
	}
}
