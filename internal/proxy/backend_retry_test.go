package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// flakyBackend answers the calls of a test: step n (from 1) decides what the
// n-th call to a path gets, and every later call gets the last step.
type flakyBackend struct {
	inner http.RoundTripper
	// steps by call number; a nil step passes the call to the real backend.
	step func(n int, req *http.Request) (*http.Response, error)
	// path limits the faults to one path; other calls pass through.
	path string

	mu    sync.Mutex
	calls int
	paths []string
	forms []string
}

func (f *flakyBackend) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.path != "" && !strings.HasSuffix(req.URL.Path, f.path) {
		return f.inner.RoundTrip(req)
	}
	var body string
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		body = string(raw)
		req.Body = io.NopCloser(strings.NewReader(body))
	}
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.paths = append(f.paths, req.URL.Path)
	f.forms = append(f.forms, body)
	f.mu.Unlock()
	if f.step != nil {
		if resp, err := f.step(n, req); resp != nil || err != nil {
			return resp, err
		}
	}
	return f.inner.RoundTrip(req)
}

func (f *flakyBackend) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func statusAnswer(req *http.Request, code int, text string, header ...string) *http.Response {
	h := http.Header{"Content-Type": {"text/plain; charset=utf-8"}}
	for i := 0; i+1 < len(header); i += 2 {
		h.Set(header[i], header[i+1])
	}
	return &http.Response{StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)), Header: h, Body: io.NopCloser(strings.NewReader(text)), Request: req, ProtoMajor: 1, ProtoMinor: 1}
}

// retryTestProxy builds a proxy in front of a fake VictoriaLogs that answers
// every read with a row, with retries on and millisecond backoffs.
func retryTestProxy(t *testing.T, cfg Config) (*Proxy, *flakyBackend) {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = w.Write([]byte(`{"_time":"2026-11-13T10:00:00Z","_stream":"{app=\"api\"}","_msg":"hello"}` + "\n"))
	}))
	t.Cleanup(backend.Close)
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	cfg.BackendURL = backend.URL
	cfg.Cache = c
	cfg.LogLevel = "error"
	if cfg.BackendTimeout == 0 {
		cfg.BackendTimeout = 5 * time.Second
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if p.backendRetry != nil {
		p.backendRetry.initial = time.Millisecond
		p.backendRetry.max = 4 * time.Millisecond
	}
	fb := &flakyBackend{inner: p.client.Transport}
	p.client.Transport = fb
	return p, fb
}

func retryConfig() Config {
	return Config{BackendRetryAttempts: DefaultBackendRetryAttempts, BackendRetryMaxWait: DefaultBackendRetryMaxWait, BackendRetryBudgetRatio: DefaultBackendRetryBudgetRatio}
}

func operationCount(p *Proxy, outcome string) string {
	w := httptest.NewRecorder()
	p.metrics.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	want := fmt.Sprintf(`loki_vl_proxy_internal_operation_total{operation="backend_retry",outcome=%q} `, outcome)
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, want); ok {
			return rest
		}
	}
	return "0"
}

// transient failure kinds, each one a typed error or status a retry can fix.
var transientKinds = []struct {
	name string
	step func(req *http.Request) (*http.Response, error)
}{
	{"VictoriaLogs queue timeout 503", func(r *http.Request) (*http.Response, error) {
		return statusAnswer(r, 503, "couldn't start executing the request in 10.000 seconds, since -search.maxConcurrentRequests=16 concurrent requests are executed"), nil
	}},
	{"429 from a proxy in front", func(r *http.Request) (*http.Response, error) { return statusAnswer(r, 429, "too many requests"), nil }},
	{"502 from a proxy in front", func(r *http.Request) (*http.Response, error) { return statusAnswer(r, 502, "bad gateway"), nil }},
	{"504 from a proxy in front", func(r *http.Request) (*http.Response, error) { return statusAnswer(r, 504, "gateway timeout"), nil }},
	{"connection refused", func(r *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	}},
	{"connection reset", func(r *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}}
	}},
	{"closed before a response", func(r *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: io.EOF}
	}},
	{"dial failure", func(r *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("network is unreachable")}}
	}},
}

func callBackend(p *Proxy, ctx context.Context, method string) (*http.Response, error) {
	params := url.Values{"query": {`app:="api" | limit 7`}}
	if method == http.MethodGet {
		return p.vlGet(ctx, "/select/logsql/query", params)
	}
	return p.vlPost(ctx, "/select/logsql/query", params)
}

// A transient VictoriaLogs failure is repeated: the client gets the answer it
// would have got without the failure, and the repeat carries the same request.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_TransientFailureRecovers(t *testing.T) {
	for _, kind := range transientKinds {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			for _, failures := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/%s/%d failures", kind.name, method, failures), func(t *testing.T) {
					p, fb := retryTestProxy(t, retryConfig())
					fb.step = func(n int, req *http.Request) (*http.Response, error) {
						if n <= failures {
							return kind.step(req)
						}
						return nil, nil
					}
					resp, err := callBackend(p, context.Background(), method)
					if err != nil {
						t.Fatalf("a failure within the retries reached the caller: %v", err)
					}
					defer resp.Body.Close()
					body, _ := io.ReadAll(resp.Body)
					if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "hello") {
						t.Fatalf("status %d body %q, want the backend's answer", resp.StatusCode, body)
					}
					if got := fb.count(); got != failures+1 {
						t.Fatalf("%d calls reached the backend, want %d", got, failures+1)
					}
					if method == http.MethodPost {
						fb.mu.Lock()
						defer fb.mu.Unlock()
						for i, form := range fb.forms {
							if form == "" || form != fb.forms[0] {
								t.Fatalf("try %d carried another request body: %q vs %q", i+1, form, fb.forms[0])
							}
						}
					}
					if got, want := operationCount(p, backendRetryRetried), fmt.Sprint(failures); got != want {
						t.Fatalf("retried = %s, want %s", got, want)
					}
					if got := operationCount(p, backendRetryRecovered); got != "1" {
						t.Fatalf("recovered = %s, want 1", got)
					}
				})
			}
		}
	}
}

// A failure that lasts is returned after the last try unchanged: the response
// is the backend's, its body unread, so the error layer maps it as before.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_ExhaustedReturnsTheLastAnswerUnchanged(t *testing.T) {
	for _, kind := range transientKinds {
		t.Run(kind.name, func(t *testing.T) {
			p, fb := retryTestProxy(t, retryConfig())
			fb.step = func(_ int, req *http.Request) (*http.Response, error) { return kind.step(req) }
			resp, err := callBackend(p, context.Background(), http.MethodPost)
			if got := fb.count(); got != 1+DefaultBackendRetryAttempts {
				t.Fatalf("%d calls reached the backend, want %d", got, 1+DefaultBackendRetryAttempts)
			}
			if got := operationCount(p, backendRetryExhausted); got != "1" {
				t.Fatalf("exhausted = %s, want 1", got)
			}
			if got := operationCount(p, backendRetryRecovered); got != "0" {
				t.Fatalf("recovered = %s on a call that failed", got)
			}
			if err != nil {
				return // a transport failure: the error itself
			}
			defer resp.Body.Close()
			if resp.StatusCode < 429 {
				t.Fatalf("status %d: the failing answer was not returned", resp.StatusCode)
			}
			if body, _ := io.ReadAll(resp.Body); len(body) == 0 {
				t.Fatal("the last answer's body was consumed before it was returned")
			}
		})
	}
}

// What the client gets after the retries are spent is what it gets without
// retries: the same status and the same body.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_ClientAnswerAfterExhaustionMatchesNoRetry(t *testing.T) {
	for _, kind := range transientKinds {
		t.Run(kind.name, func(t *testing.T) {
			answers := map[bool][2]string{}
			for _, retries := range []bool{false, true} {
				cfg := Config{}
				if retries {
					cfg = retryConfig()
				}
				p, fb := retryTestProxy(t, cfg)
				fb.path = "/select/logsql/query"
				fb.step = func(_ int, req *http.Request) (*http.Response, error) { return kind.step(req) }
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				now := time.Now()
				q := url.Values{"query": {`{app="api"}`}, "start": {fmt.Sprint(now.Add(-time.Minute).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}, "limit": {"10"}}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil))
				answers[retries] = [2]string{fmt.Sprint(w.Code), w.Body.String()}
				if retries && fb.count() < 1+DefaultBackendRetryAttempts {
					t.Fatalf("only %d calls reached the backend: the failure was not retried", fb.count())
				}
			}
			if answers[true] != answers[false] {
				t.Fatalf("the client's answer changed: with retries %v, without %v", answers[true], answers[false])
			}
			for _, secret := range []string{"maxConcurrentRequests", "bad gateway", "refused", "dial tcp", "127.0.0.1"} {
				if strings.Contains(answers[true][1], secret) {
					t.Fatalf("VictoriaLogs' text reached the client: %q", answers[true][1])
				}
			}
		})
	}
}

// A single transient failure within the retries is no longer an error for the
// client: the query answers 200 with the lines.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_ClientGetsTheAnswerLokiWouldGive(t *testing.T) {
	for _, kind := range transientKinds {
		t.Run(kind.name, func(t *testing.T) {
			p, fb := retryTestProxy(t, retryConfig())
			fb.path = "/select/logsql/query"
			fb.step = func(n int, req *http.Request) (*http.Response, error) {
				if n == 1 {
					return kind.step(req)
				}
				return nil, nil
			}
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			now := time.Now()
			q := url.Values{"query": {`{app="api"}`}, "start": {fmt.Sprint(now.Add(-time.Minute).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}, "limit": {"10"}}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil))
			if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "hello") {
				t.Fatalf("status %d body %.300s, want 200 with the line", w.Code, w.Body.String())
			}
		})
	}
}

// A failure no retry can fix is returned at once, one call: a query
// VictoriaLogs rejects, a failed query, a timeout and a cancellation.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_PermanentFailuresAreNotRetried(t *testing.T) {
	cases := []struct {
		name string
		step func(req *http.Request) (*http.Response, error)
	}{
		{"400 parse error", func(r *http.Request) (*http.Response, error) {
			return statusAnswer(r, 400, "cannot parse query arg [x]: unexpected token"), nil
		}},
		{"422 stats error", func(r *http.Request) (*http.Response, error) { return statusAnswer(r, 422, `{"status":"error"}`), nil }},
		{"404", func(r *http.Request) (*http.Response, error) {
			return statusAnswer(r, 404, "unsupported path requested"), nil
		}},
		{"500 failed query", func(r *http.Request) (*http.Response, error) {
			return statusAnswer(r, 500, "cannot execute query [x]: out of memory"), nil
		}},
		{"response timeout", func(r *http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}}
		}},
		{"deadline", func(r *http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: context.DeadlineExceeded}
		}},
		{"a failure that is not a transport error", func(_ *http.Request) (*http.Response, error) { return nil, errors.New("tls: bad certificate") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, fb := retryTestProxy(t, retryConfig())
			fb.step = func(_ int, req *http.Request) (*http.Response, error) { return tc.step(req) }
			resp, err := callBackend(p, context.Background(), http.MethodPost)
			if err == nil {
				_ = resp.Body.Close()
			}
			if got := fb.count(); got != 1 {
				t.Fatalf("%d calls reached the backend, want 1", got)
			}
			for _, outcome := range []string{backendRetryRetried, backendRetryExhausted, backendRetryRecovered} {
				if got := operationCount(p, outcome); got != "0" {
					t.Fatalf("%s = %s for a failure no retry fixes", outcome, got)
				}
			}
		})
	}
}

// The delete call and anything that is not a VictoriaLogs read are sent once.
func TestBackendRetry_OnlyReadsAreRetried(t *testing.T) {
	for _, path := range []string{"/select/logsql/delete", "/insert/jsonline"} {
		t.Run(path, func(t *testing.T) {
			p, fb := retryTestProxy(t, retryConfig())
			fb.step = func(_ int, req *http.Request) (*http.Response, error) { return statusAnswer(req, 503, "busy"), nil }
			resp, err := p.vlPost(context.Background(), path, url.Values{"query": {"x"}})
			if err == nil {
				_ = resp.Body.Close()
			}
			if got := fb.count(); got != 1 {
				t.Fatalf("%d calls reached the backend, want 1", got)
			}
		})
	}
}

func TestBackendRetry_RequestsThatCannotBeRebuiltAreNotRetried(t *testing.T) {
	get := httptest.NewRequest(http.MethodGet, "http://vl/select/logsql/query", nil)
	post := httptest.NewRequest(http.MethodPost, "http://vl/select/logsql/query", strings.NewReader("a=b"))
	prefixed := httptest.NewRequest(http.MethodPost, "http://vl/logs/select/logsql/query", strings.NewReader("a=b"))
	metrics := httptest.NewRequest(http.MethodGet, "http://vl/metrics", nil)
	put := httptest.NewRequest(http.MethodPut, "http://vl/select/logsql/query", nil)
	del := httptest.NewRequest(http.MethodPost, "http://vl/select/logsql/delete", strings.NewReader("a=b"))
	for name, tc := range map[string]struct {
		req        *http.Request
		hasGetBody bool
		want       bool
	}{
		"get":                    {get, false, true},
		"post with a body":       {post, true, true},
		"post without GetBody":   {post, false, false},
		"post under a prefix":    {prefixed, true, true},
		"metrics get":            {metrics, false, true},
		"put":                    {put, false, false},
		"delete":                 {del, true, false},
		"post outside of select": {httptest.NewRequest(http.MethodPost, "http://vl/insert/x", strings.NewReader("a")), true, false},
	} {
		t.Run(name, func(t *testing.T) {
			r := tc.req.Clone(context.Background())
			if tc.hasGetBody {
				r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("a=b")), nil }
			} else {
				r.GetBody = nil
			}
			if got := backendRetryable(r); got != tc.want {
				t.Fatalf("backendRetryable = %v, want %v", got, tc.want)
			}
		})
	}
}

// The cold-tier call is a POST whose body must be rebuilt for the repeat.
func TestBackendRetry_ColdTierPostIsRetried(t *testing.T) {
	cold := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = w.Write([]byte(`{"_msg":"` + r.Form.Get("query") + `"}` + "\n"))
	}))
	defer cold.Close()
	p, _ := retryTestProxy(t, retryConfig())
	cr, err := NewColdRouter(ColdBackendConfig{URL: cold.URL, Boundary: time.Hour, Enabled: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Skipf("cold router: %v", err)
	}
	var calls atomic.Int32
	cr.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return statusAnswer(req, 503, "busy"), nil
		}
		return http.DefaultTransport.RoundTrip(req)
	})
	resp, err := cr.coldRequest(context.Background(), http.MethodPost, "/select/logsql/query", url.Values{"query": {"cold-marker"}}, nil, p.doBackendRead)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if calls.Load() != 2 || !strings.Contains(string(body), "cold-marker") {
		t.Fatalf("%d calls, body %q: the repeat did not carry the request", calls.Load(), body)
	}
}

// A client that goes away while a call waits to be repeated ends the wait: no
// further call reaches the backend and nothing keeps running.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_ClientCancelDuringBackoffStopsRetrying(t *testing.T) {
	p, fb := retryTestProxy(t, retryConfig())
	p.backendRetry.initial = 5 * time.Second
	p.backendRetry.max = 5 * time.Second
	fb.step = func(_ int, req *http.Request) (*http.Response, error) { return statusAnswer(req, 503, "busy"), nil }
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		resp, err := callBackend(p, ctx, http.MethodGet)
		if err == nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for fb.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want the cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the call kept waiting after the client left")
	}
	if got := fb.count(); got != 1 {
		t.Fatalf("%d calls reached the backend, want 1", got)
	}
	deadline = time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+1 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
}

// A call never waits past the request's deadline: when the wait does not fit,
// the answer comes at once.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_DeadlineBoundsTheWait(t *testing.T) {
	p, fb := retryTestProxy(t, retryConfig())
	p.backendRetry.initial = 2 * time.Second
	p.backendRetry.max = 2 * time.Second
	fb.step = func(_ int, req *http.Request) (*http.Response, error) { return statusAnswer(req, 503, "busy"), nil }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	resp, err := callBackend(p, ctx, http.MethodGet)
	if err == nil {
		_ = resp.Body.Close()
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the call waited %s past a 300 ms deadline", elapsed)
	}
	if got := fb.count(); got != 1 {
		t.Fatalf("%d calls reached the backend, want 1", got)
	}
	if got := operationCount(p, backendRetryExhausted); got != "1" {
		t.Fatalf("exhausted = %s, want 1", got)
	}
}

// Retry-After is honoured, and a value beyond the wait budget ends the retries.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_HonoursRetryAfter(t *testing.T) {
	t.Run("waits at least as long", func(t *testing.T) {
		p, fb := retryTestProxy(t, retryConfig())
		fb.step = func(n int, req *http.Request) (*http.Response, error) {
			if n == 1 {
				return statusAnswer(req, 503, "busy", "Retry-After", "1"), nil
			}
			return nil, nil
		}
		start := time.Now()
		resp, err := callBackend(p, context.Background(), http.MethodGet)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if elapsed := time.Since(start); elapsed < time.Second || fb.count() != 2 {
			t.Fatalf("retried after %s with %d calls, want a wait of at least the Retry-After", elapsed, fb.count())
		}
	})
	t.Run("longer than the wait budget gives up at once", func(t *testing.T) {
		p, fb := retryTestProxy(t, retryConfig())
		fb.step = func(_ int, req *http.Request) (*http.Response, error) {
			return statusAnswer(req, 503, "busy", "Retry-After", "3600"), nil
		}
		start := time.Now()
		resp, err := callBackend(p, context.Background(), http.MethodGet)
		if err == nil {
			defer resp.Body.Close()
		}
		if elapsed := time.Since(start); elapsed > time.Second || fb.count() != 1 {
			t.Fatalf("%d calls after %s, want one call and no wait", fb.count(), elapsed)
		}
	})
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Duration
	}{
		{"", 0}, {"0", 0}, {"-3", 0}, {"abc", 0}, {"2", 2 * time.Second}, {" 7 ", 7 * time.Second},
		{now.Add(5 * time.Second).Format(http.TimeFormat), 5 * time.Second},
		{now.Add(-5 * time.Second).Format(http.TimeFormat), 0},
		{"99999999999", 0}, {now.Add(48 * time.Hour).Format(http.TimeFormat), 0},
	} {
		if got := parseRetryAfter(tc.in, now); got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// The budget keeps retries a fraction of the calls: under a sustained failure
// the retries stop once the burst is spent, and the calls are answered as
// they would be without retries.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_BudgetDeniesRetriesUnderSustainedFailure(t *testing.T) {
	cfg := retryConfig()
	cfg.BackendRetryBudgetRatio = 0.1
	p, fb := retryTestProxy(t, cfg)
	p.backendRetry.budget = newRetryBudget(0.1, 4)
	fb.step = func(_ int, req *http.Request) (*http.Response, error) { return statusAnswer(req, 503, "busy"), nil }
	const calls = 30
	for range calls {
		resp, err := callBackend(p, context.Background(), http.MethodGet)
		if err == nil {
			_ = resp.Body.Close()
		}
	}
	// 4 burst retries plus 30 x 0.1 earned: no more than 7 retries in all.
	if got, max := fb.count(), calls+7; got > max {
		t.Fatalf("%d backend calls for %d requests: retries were not held to the budget (at most %d)", got, calls, max)
	}
	if got := fb.count(); got < calls+4 {
		t.Fatalf("%d backend calls: the burst was not used", got)
	}
	if got := operationCount(p, backendRetryBudgetDenied); got == "0" {
		t.Fatal("budget_denied was never counted")
	}
}

func TestRetryBudget(t *testing.T) {
	b := newRetryBudget(0.25, 3)
	for range 3 {
		if !b.spend() {
			t.Fatal("a fresh budget holds its burst")
		}
	}
	if b.spend() {
		t.Fatal("an empty budget spent")
	}
	for range 3 {
		b.earn()
		if b.spend() {
			t.Fatal("three quarters of a retry bought a retry")
		}
	}
	b.earn()
	if !b.spend() {
		t.Fatal("four calls at 0.25 earn a retry")
	}
	for range 100 {
		b.earn()
	}
	n := 0
	for b.spend() {
		n++
	}
	if n != 3 {
		t.Fatalf("earning is capped at the burst: spent %d, want 3", n)
	}
}

func TestRetryBudget_ConcurrentUseNeverOverspends(t *testing.T) {
	b := newRetryBudget(0, 50)
	var spent atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if b.spend() {
					spent.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if spent.Load() != 50 {
		t.Fatalf("spent %d of a budget of 50", spent.Load())
	}
}

// No retry while the circuit breaker is not closed.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_NoRetryWhileCircuitBreakerIsOpen(t *testing.T) {
	cfg := retryConfig()
	cfg.CBFailThreshold = 1
	cfg.CBOpenDuration = time.Minute
	p, fb := retryTestProxy(t, cfg)
	fb.step = func(n int, req *http.Request) (*http.Response, error) {
		if n == 1 {
			p.breaker.RecordFailure() // another call's failure opens the breaker
		}
		return statusAnswer(req, 503, "busy"), nil
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, p.backend.String()+"/select/logsql/query", nil)
	resp, err := p.doBackendRead(req, p.client)
	if err == nil {
		defer resp.Body.Close()
	}
	if p.breaker.State() != "open" {
		t.Fatalf("breaker is %s: the test did not open it", p.breaker.State())
	}
	if got := fb.count(); got != 1 {
		t.Fatalf("%d calls reached the backend, want 1", got)
	}
	if got := operationCount(p, backendRetryBreakerOpen); got != "1" {
		t.Fatalf("breaker_open = %s, want 1", got)
	}
}

// Retries off (the library default, and -backend-retry-attempts=0) send every
// read once, whatever the failure.
// conformance: semantics/transient-backend-failure-is-retried
func TestBackendRetry_DisabledSendsOnce(t *testing.T) {
	for name, cfg := range map[string]Config{
		"zero config":      {},
		"attempts 0":       {BackendRetryAttempts: 0, BackendRetryMaxWait: time.Second, BackendRetryBudgetRatio: 0.1},
		"max wait 0":       {BackendRetryAttempts: 2, BackendRetryMaxWait: 0, BackendRetryBudgetRatio: 0.1},
		"negative attempt": {BackendRetryAttempts: -1, BackendRetryMaxWait: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			p, fb := retryTestProxy(t, cfg)
			if p.backendRetry != nil {
				t.Fatal("a policy was built for settings that disable retries")
			}
			fb.step = func(_ int, req *http.Request) (*http.Response, error) { return statusAnswer(req, 503, "busy"), nil }
			resp, err := callBackend(p, context.Background(), http.MethodPost)
			if err == nil {
				_ = resp.Body.Close()
			}
			if got := fb.count(); got != 1 {
				t.Fatalf("%d calls reached the backend, want 1", got)
			}
		})
	}
}

// A repeated call gives its backend budget permit back before it waits, so a
// full permit pool does not stall the other calls while one backs off.
func TestBackendRetry_PermitIsReleasedDuringBackoff(t *testing.T) {
	cfg := retryConfig()
	p, fb := retryTestProxy(t, cfg)
	p.backendBudget = newBackendBudget(1)
	p.backendRetry.initial = 300 * time.Millisecond
	p.backendRetry.max = 300 * time.Millisecond
	fb.path = "/select/logsql/query"
	fb.step = func(n int, req *http.Request) (*http.Response, error) {
		if n == 1 {
			return statusAnswer(req, 503, "busy"), nil
		}
		return nil, nil
	}
	first := make(chan error, 1)
	go func() {
		resp, err := callBackend(p, context.Background(), http.MethodGet)
		if err == nil {
			_ = resp.Body.Close()
		}
		first <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for fb.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// While the first call waits to be repeated, another one gets the permit.
	start := time.Now()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, p.backend.String()+"/select/logsql/other", nil)
	other, err := p.doBackendRequest(req, p.client)
	if err != nil {
		t.Fatal(err)
	}
	_ = other.Body.Close()
	if waited := time.Since(start); waited > 140*time.Millisecond {
		t.Fatalf("another call waited %s for the permit of a call that was backing off", waited)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

// The flags reach the proxy and an out-of-range value is refused at startup.
func TestBackendRetry_PolicyFromConfig(t *testing.T) {
	p := newBackendRetryPolicy(2, 5*time.Second, 0.1)
	if p == nil || p.retries != 2 || p.maxWait != 5*time.Second || p.initial != backendRetryInitialBackoff || p.max != backendRetryMaxBackoff {
		t.Fatalf("policy = %+v", p)
	}
	if newBackendRetryPolicy(0, time.Second, 0.1) != nil || newBackendRetryPolicy(2, 0, 0.1) != nil {
		t.Fatal("a policy was built with retries off")
	}
	if got := newBackendRetryPolicy(1, time.Second, -1).budget.earnEach; got != 0 {
		t.Fatalf("a negative ratio earns %d", got)
	}
}

func TestTransientBackendFailureClassification(t *testing.T) {
	resp := func(code int) *http.Response { return &http.Response{StatusCode: code, Header: http.Header{}} }
	for _, tc := range []struct {
		name string
		resp *http.Response
		err  error
		want bool
	}{
		{"200", resp(200), nil, false},
		{"400", resp(400), nil, false},
		{"401", resp(401), nil, false},
		{"404", resp(404), nil, false},
		{"422", resp(422), nil, false},
		{"429", resp(429), nil, true},
		{"500", resp(500), nil, false},
		{"501", resp(501), nil, false},
		{"502", resp(502), nil, true},
		{"503", resp(503), nil, true},
		{"504", resp(504), nil, true},
		{"nothing", nil, nil, false},
		{"refused", nil, fmt.Errorf("wrapped: %w", syscall.ECONNREFUSED), true},
		{"reset", nil, &url.Error{Op: "Get", URL: "http://vl", Err: syscall.ECONNRESET}, true},
		{"broken pipe", nil, syscall.EPIPE, true},
		{"EOF", nil, &url.Error{Op: "Post", URL: "http://vl", Err: io.EOF}, true},
		{"unexpected EOF", nil, io.ErrUnexpectedEOF, true},
		{"dial", nil, &net.OpError{Op: "dial", Err: errors.New("no route")}, true},
		{"read timeout", nil, &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}, false},
		{"canceled", nil, &url.Error{Op: "Get", URL: "x", Err: context.Canceled}, false},
		{"deadline", nil, context.DeadlineExceeded, false},
		{"canceled while dialing", nil, &net.OpError{Op: "dial", Err: context.Canceled}, false},
		{"text that only says refused", nil, errors.New("connection refused"), false},
		{"plain error", nil, errors.New("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, got := transientBackendFailure(tc.resp, tc.err); got != tc.want {
				t.Fatalf("transient = %v, want %v", got, tc.want)
			}
		})
	}
}

// The failed tries are logged, VictoriaLogs' text included, and only there.
func TestBackendRetry_LogsTheFailedTries(t *testing.T) {
	var buf strings.Builder
	cfg := retryConfig()
	cfg.LogLevel = "warn"
	p, fb := retryTestProxy(t, cfg)
	p.log = slog.New(slog.NewTextHandler(&buf, nil))
	fb.step = func(n int, req *http.Request) (*http.Response, error) {
		if n == 1 {
			return statusAnswer(req, 503, "couldn't start executing the request in 10 seconds"), nil
		}
		return nil, nil
	}
	resp, err := callBackend(p, context.Background(), http.MethodGet)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	out := buf.String()
	for _, want := range []string{"VictoriaLogs call failed, retrying", "attempt=1", "503", "couldn't start executing"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q misses %q", out, want)
		}
	}
}

// FuzzBackendRetryDecision: whatever a backend sends as Retry-After or fails
// with, the wait read from it is zero or a positive span of at most a day and
// the classification never panics.
func FuzzBackendRetryDecision(f *testing.F) {
	for _, seed := range []string{"", "0", "1", "-1", "3600", "99999999999999999999", "Fri, 13 Nov 2026 10:00:05 GMT", "garbage", "1e9", " 5 "} {
		f.Add(seed, 503)
	}
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, retryAfter string, status int) {
		if d := parseRetryAfter(retryAfter, now); d < 0 || d > 24*time.Hour {
			t.Fatalf("parseRetryAfter(%q) = %s", retryAfter, d)
		}
		resp := &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {retryAfter}}}
		wait, transient := transientBackendFailure(resp, nil)
		if wait < 0 || wait > 24*time.Hour {
			t.Fatalf("wait %s", wait)
		}
		want := status == 429 || status == 502 || status == 503 || status == 504
		if transient != want {
			t.Fatalf("status %d: transient %v, want %v", status, transient, want)
		}
	})
}
