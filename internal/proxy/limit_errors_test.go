package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

const (
	grpcSizeText = "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (%d vs. %d)"
	limitText    = "limit reached while evaluating the query"
	queueText    = "too many outstanding requests"
)

// logProxy is a Proxy that logs every request error line to the returned buffer.
func logProxy() (*Proxy, *bytes.Buffer) {
	var buf bytes.Buffer
	return &Proxy{log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}, &buf
}

// TestRateLimiterAnswersQueueFull: the per-client rate limit and the
// -max-concurrent cap both answer Loki's 429 "too many outstanding requests"
// through the error layer, with the retry hints kept and the flag in the log.
// conformance: semantics/error-layer-proxy-limit-status, limits/admission-limits-answer-queue-full, status-429
func TestRateLimiterAnswersQueueFull(t *testing.T) {
	for _, tc := range []struct {
		name         string
		concurrent   int
		rate         float64
		burst        int
		flag, retry  string
		wantHeaders  map[string]string
		limitInvalue string
	}{
		{"rate", 0, 1, 1, "rate-limit-per-second", "1", map[string]string{"X-Ratelimit-Limit": "1", "X-Ratelimit-Remaining": "0"}, "1"},
		{"concurrent", 1, 0, 0, "max-concurrent", "5", nil, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, logs := logProxy()
			p.limiter = mw.NewRateLimiter(tc.concurrent, tc.rate, tc.burst)
			p.limiter.Reject = p.rejectAdmission
			defer p.limiter.Stop()
			release := make(chan struct{})
			h := p.limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
			defer close(release)
			go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
			time.Sleep(30 * time.Millisecond)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
			if rec.Code != http.StatusTooManyRequests || rec.Body.String() != queueText {
				t.Fatalf("answer %d %q, want 429 %q", rec.Code, rec.Body.String(), queueText)
			}
			if rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Errorf("headers %v", rec.Header())
			}
			if got := rec.Header().Get("Retry-After"); got != map[string]string{"rate": "1", "concurrent": "5"}[tc.name] {
				t.Errorf("Retry-After %q", got)
			}
			for k, v := range tc.wantHeaders {
				if got := rec.Header().Get(k); got != v {
					t.Errorf("%s = %q, want %q", k, got, v)
				}
			}
			if strings.Contains(rec.Body.String(), tc.flag) {
				t.Errorf("body names the flag: %q", rec.Body.String())
			}
			if line := logs.String(); !strings.Contains(line, "limit_flag=-"+tc.flag) || !strings.Contains(line, "limit_value="+tc.limitInvalue) {
				t.Errorf("log line %q", line)
			}
		})
	}
}

// TestHeavyQueueFullAnswers429OnEveryWriter: the full queue is 429 whichever
// writer a route reaches it through, with no message prefix deciding the status.
// conformance: semantics/error-layer-proxy-limit-status, status-429
func TestHeavyQueueFullAnswers429OnEveryWriter(t *testing.T) {
	queueFull := newHeavyQueueFull(&heavyQueryQueueFullError{maxConcurrent: 2, queueWait: time.Second})
	p, _ := logProxy()
	for name, write := range map[string]func(w http.ResponseWriter) int{
		"writeErr": func(w http.ResponseWriter) int { return p.writeErr(w, context.Background(), queueFull) },
		"fail": func(w http.ResponseWriter) int {
			return p.fail(w, requestOnRoute("series", "/x", context.Background()), queueFull)
		},
		"writeGrafanaStatsFailed": func(w http.ResponseWriter) int { p.writeGrafanaStatsFailure(w, queueFull); return 0 },
		"writeErrShaped": func(w http.ResponseWriter) int {
			return p.writeErrShaped(w, context.Background(), queueFull, writeLokiHTTPError)
		},
		"wrapped": func(w http.ResponseWriter) int {
			return p.writeErr(w, context.Background(), fmt.Errorf("labels: %w", queueFull))
		},
	} {
		rec := httptest.NewRecorder()
		write(rec)
		if rec.Code != http.StatusTooManyRequests || !strings.HasPrefix(rec.Body.String(), queueText) {
			t.Errorf("%s: %d %q", name, rec.Code, rec.Body.String())
		}
	}
	// A message that merely starts like the old queue prefix is no queue error.
	rec := httptest.NewRecorder()
	p.writeError(rec, http.StatusInternalServerError, "too many outstanding requests: heavy VictoriaLogs queries are busy")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("a text prefix chose status %d", rec.Code)
	}
	// A bare error with no type is a failed backend call, never a queue.
	rec = httptest.NewRecorder()
	p.writeErr(rec, context.Background(), errors.New("too many outstanding requests: heavy VictoriaLogs queries are limited"))
	if rec.Code != http.StatusInternalServerError || rec.Body.String() != lokiErrInternal {
		t.Errorf("untyped queue-like text answered %d %q", rec.Code, rec.Body.String())
	}
}

// TestClassificationByTypeNeverByText: cancellation, deadline and breaker
// decisions read the error's type (errors.Is, errors.As, net.Error.Timeout),
// never its text. A message that merely says "timeout", "deadline exceeded" or
// "context canceled" is a failed backend call.
// conformance: semantics/error-layer-cancel-deadline, semantics/error-layer-internal-go-text, semantics/error-layer-classify-by-type, status-500
func TestClassificationByTypeNeverByText(t *testing.T) {
	type outcome struct {
		status   int
		canceled bool
		breaker  bool // the failure counts against the backend's circuit breaker
	}
	cancelCause := errors.New("client went away")
	cctx, cancelWith := context.WithCancelCause(context.Background())
	cancelWith(cancelCause)
	tctx, tcancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer tcancel()
	<-tctx.Done()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want outcome
	}{
		{"context canceled", context.Background(), context.Canceled, outcome{499, true, false}},
		{"wrapped canceled", context.Background(), fmt.Errorf("vl call: %w", context.Canceled), outcome{499, true, false}},
		{"url error with canceled", context.Background(), &url.Error{Op: "Get", URL: "http://x", Err: context.Canceled}, outcome{499, true, false}},
		{"deadline exceeded", context.Background(), context.DeadlineExceeded, outcome{504, false, false}},
		{"wrapped deadline", context.Background(), fmt.Errorf("vl call: %w", context.DeadlineExceeded), outcome{504, false, false}},
		{"net timeout", context.Background(), &net.OpError{Op: "dial", Err: timeoutNetError{}}, outcome{504, false, false}},
		{"url error with a net timeout", context.Background(), &url.Error{Op: "Get", URL: "http://x", Err: timeoutNetError{}}, outcome{504, false, false}},
		{"aborted body that timed out", context.Background(), &vlResponseAbortedError{timedOut: true, cause: io.ErrUnexpectedEOF}, outcome{504, false, true}},
		{"cancelled request with a cause that is no context error", cctx, cancelCause, outcome{499, false, false}},
		{"expired request", tctx, errors.New("read tcp: use of closed network connection"), outcome{504, false, false}},
		{"text says timeout", context.Background(), errors.New("vl said: read timeout while scanning"), outcome{500, false, true}},
		{"text says deadline exceeded", context.Background(), errors.New("rpc: deadline exceeded"), outcome{500, false, true}},
		{"text says context canceled", context.Background(), errors.New("post: context canceled"), outcome{500, false, true}},
		{"VictoriaLogs message with timeout", context.Background(), &upstreamStatusError{status: 400, msg: "the request failed: timeout awaiting the storage nodes", class: vlErrorUnclassified}, outcome{500, false, false}},
		{"refused", context.Background(), &url.Error{Op: "Get", URL: "http://x", Err: errors.New("connection refused")}, outcome{500, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := upstreamErrorStatus(tc.ctx, tc.err); got != tc.want.status {
				t.Errorf("upstreamErrorStatus = %d, want %d", got, tc.want.status)
			}
			if got := isCanceledErr(tc.err); got != tc.want.canceled {
				t.Errorf("isCanceledErr = %v, want %v", got, tc.want.canceled)
			}
			if got := shouldRecordBreakerFailure(tc.ctx, tc.err); got != tc.want.breaker {
				t.Errorf("shouldRecordBreakerFailure = %v, want %v", got, tc.want.breaker)
			}
		})
	}
}

// TestTransportTimeoutsAreTypedTimeouts: the failures a real client raises for
// a slow backend are recognised by type: a client timeout before the headers, a
// client timeout while the body is read, a request context deadline and a
// cancellation. None needs its text read.
// conformance: semantics/error-layer-cancel-deadline, semantics/error-layer-classify-by-type, status-500
func TestTransportTimeoutsAreTypedTimeouts(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/body" {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("x"))
			w.(http.Flusher).Flush()
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	headerClient := &http.Client{Timeout: 100 * time.Millisecond}
	_, headerErr := headerClient.Get(srv.URL + "/headers")
	bodyClient := &http.Client{Timeout: 150 * time.Millisecond}
	var bodyErr error
	if resp, err := bodyClient.Get(srv.URL + "/body"); err != nil {
		bodyErr = err
	} else {
		_, bodyErr = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer dcancel()
	req, _ := http.NewRequestWithContext(dctx, http.MethodGet, srv.URL+"/headers", nil)
	_, ctxDeadlineErr := http.DefaultClient.Do(req)
	cctx, ccancel := context.WithCancel(context.Background())
	req, _ = http.NewRequestWithContext(cctx, http.MethodGet, srv.URL+"/headers", nil)
	time.AfterFunc(50*time.Millisecond, ccancel)
	_, ctxCancelErr := http.DefaultClient.Do(req)

	// A cancel while the body is read, as the windowed and tail retries meet it
	// with no request context at hand: the type alone says it was a cancel.
	bctx, bcancel := context.WithCancel(context.Background())
	req, _ = http.NewRequestWithContext(bctx, http.MethodGet, srv.URL+"/body", nil)
	var bodyCancelErr error
	if resp, err := http.DefaultClient.Do(req); err != nil {
		bodyCancelErr = err
	} else {
		time.AfterFunc(50*time.Millisecond, bcancel)
		_, bodyCancelErr = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	bcancel()
	if bodyCancelErr == nil || !isCanceledErr(bodyCancelErr) || isCanceledErr(headerErr) {
		t.Errorf("a cancel during the body read must be recognised by type: err=%v canceled=%v; a client timeout must not be one: %v", bodyCancelErr, isCanceledErr(bodyCancelErr), isCanceledErr(headerErr))
	}
	if tailRetryable(bodyCancelErr) {
		t.Errorf("a cancel during the body read must not be retried: %v", bodyCancelErr)
	}

	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"client timeout before headers", headerErr, 504},
		{"cancel during the body read", bodyCancelErr, 499},
		{"client timeout during the body read", bodyErr, 504},
		{"request deadline", ctxDeadlineErr, 504},
		{"request cancel", ctxCancelErr, 499},
	} {
		if tc.err == nil {
			t.Errorf("%s: no error", tc.name)
			continue
		}
		if got := statusFromUpstreamErr(tc.err); got != tc.want {
			t.Errorf("%s: statusFromUpstreamErr(%q) = %d, want %d", tc.name, tc.err, got, tc.want)
		}
		if shouldRecordBreakerFailure(context.Background(), tc.err) {
			t.Errorf("%s: %q counts against the backend's breaker", tc.name, tc.err)
		}
	}
}

// limitFlagOf is the flag behind a typed limit or queue error (without its
// dash), or "" for any other error. It is what a log line reports.
func limitFlagOf(err error) string {
	if typed, ok := lokierr.As(err); ok {
		flag, _, _ := typed.Limit()
		return flag
	}
	return ""
}

// heavyQueueCause is the queue error's operator-facing detail (which queue,
// at what limit, why the door was closed): the log line's upstream_error.
func heavyQueueCause(err error) error {
	var cause *heavyQueryQueueFullError
	if errors.As(err, &cause) {
		return cause
	}
	return err
}

// TestAdmissionRejectionsLogOncePerSecond: a flood of rejected requests gets
// the same answer each but one log line a second; the next second logs again.
// conformance: semantics/error-layer-proxy-limit-status, limits/admission-limits-answer-queue-full
func TestAdmissionRejectionsLogOncePerSecond(t *testing.T) {
	var g logGate
	t0 := time.Unix(1_700_000_000, 0)
	if !g.allow(t0) || g.allow(t0.Add(10*time.Millisecond)) || g.allow(t0.Add(999*time.Millisecond)) {
		t.Fatal("the gate must let one line through a second")
	}
	if !g.allow(t0.Add(time.Second)) || g.allow(t0.Add(time.Second+time.Millisecond)) {
		t.Fatal("the gate must open again after a second")
	}

	p, logs := logProxy()
	rejected := lokierr.QueueFull(errors.New("per-client rate limit exceeded")).WithLimit(flagRateLimitPerSecond, 1)
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		p.rejectAdmission(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil), rejected)
		if rec.Code != http.StatusTooManyRequests || rec.Body.String() != queueText || rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Fatalf("rejection %d answered %d %q %q", i, rec.Code, rec.Body.String(), rec.Header().Get("Content-Type"))
		}
	}
	if n := strings.Count(logs.String(), "request error"); n != 1 {
		t.Errorf("%d log lines for 5 rejections in one second, want 1: %s", n, logs.String())
	}
}

// TestAdmissionLimitsAnswer429OnEveryRoute: through the real route chain, a
// client over the rate limit, and a request over the -max-concurrent cap, get
// Loki's 429 text on the query routes, the ruler's views, the alerts views and
// tail: the limiter answers before a route's own error shape is known, so none
// of them may turn the status into another.
// conformance: semantics/error-layer-proxy-limit-status, limits/admission-limits-answer-queue-full, status-429
func TestAdmissionLimitsAnswer429OnEveryRoute(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	defer backend.Close()
	paths := []string{
		"/loki/api/v1/labels",
		"/loki/api/v1/query_range?query=%7Bapp%3D%22a%22%7D",
		"/loki/api/v1/rules",
		"/prometheus/api/v1/rules",
		"/api/prom/rules",
		"/loki/api/v1/alerts",
		"/prometheus/api/v1/alerts",
		"/api/prom/alerts",
		"/loki/api/v1/tail?query=%7Bapp%3D%22a%22%7D",
		"/loki/api/v1/index/stats?query=%7Bapp%3D%22a%22%7D",
	}
	for _, mode := range []string{"rate", "concurrent"} {
		for _, path := range paths {
			t.Run(mode+path, func(t *testing.T) {
				cfg := Config{BackendURL: backend.URL, AlertsBackendURL: backend.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error"}
				if mode == "rate" {
					cfg.RatePerSecond, cfg.RateBurst = 0.001, 1
				} else {
					cfg.MaxConcurrent = 1
				}
				p, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				if mode == "concurrent" {
					// Hold the only slot, as an in-flight request does.
					if !p.limiter.AcquireConcurrent() {
						t.Fatal("the slot was taken")
					}
					defer p.limiter.ReleaseConcurrent()
				} else {
					// The first request takes the one token.
					mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
				}
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("X-Scope-OrgID", "0")
				shippedChain(mux).ServeHTTP(rec, req)
				if rec.Code != http.StatusTooManyRequests || rec.Body.String() != queueText {
					t.Fatalf("answered %d %q, want 429 %q", rec.Code, rec.Body.String(), queueText)
				}
				if rec.Header().Get("Retry-After") == "" {
					t.Errorf("Retry-After is gone: %v", rec.Header())
				}
			})
		}
	}
}
