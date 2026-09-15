package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// errInternalBudget stands in for a proxy-internal abort (an evaluation budget,
// a failed sibling request) that cancels in-flight backend calls through a
// cancel-cause context such as the one errgroup.WithContext creates.
var errInternalBudget = errors.New("subquery exceeds evaluation, sample or decoded-byte limit")

// holdingBackend is a fake VictoriaLogs that parks requests until their
// connection is cancelled while hold is set, and answers immediately otherwise.
type holdingBackend struct {
	*httptest.Server
	hold    atomic.Bool
	arrived chan struct{}
}

func newHoldingBackend(t *testing.T, capacity int) *holdingBackend {
	t.Helper()
	b := &holdingBackend{arrived: make(chan struct{}, capacity)}
	b.hold.Store(true)
	stop := make(chan struct{})
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the body so the server notices the client dropping the connection.
		_, _ = io.Copy(io.Discard, r.Body)
		if b.hold.Load() {
			b.arrived <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-stop:
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	t.Cleanup(b.Close)
	t.Cleanup(func() { close(stop) }) // runs before Close
	return b
}

func (b *holdingBackend) waitArrived(t *testing.T, n int) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-b.arrived:
		case <-timeout:
			t.Fatalf("only %d of %d backend requests arrived", i, n)
		}
	}
}

func TestCircuitBreaker_IgnoresInternalCancellationCause(t *testing.T) {
	const threshold = 3
	const inFlight = 8 // well above the breaker threshold

	calls := map[string]func(p *Proxy, ctx context.Context, i int) error{
		"vlGet": func(p *Proxy, ctx context.Context, i int) error {
			resp, err := p.vlGet(ctx, "/select/logsql/stats_query", url.Values{"query": {"*"}, "i": {strconv.Itoa(i)}})
			if resp != nil {
				_ = resp.Body.Close()
			}
			return err
		},
		"vlPost": func(p *Proxy, ctx context.Context, i int) error {
			resp, err := p.vlPost(ctx, "/select/logsql/stats_query", url.Values{"query": {"*"}, "i": {strconv.Itoa(i)}})
			if resp != nil {
				_ = resp.Body.Close()
			}
			return err
		},
		"vlGetCoalescedWithStatus": func(p *Proxy, ctx context.Context, i int) error {
			_, _, err := p.vlGetCoalescedWithStatus(ctx, "k"+strconv.Itoa(i), "/select/logsql/field_names", url.Values{"i": {strconv.Itoa(i)}})
			return err
		},
		"vlPostCoalesced": func(p *Proxy, ctx context.Context, i int) error {
			_, _, err := p.vlPostCoalesced(ctx, "k"+strconv.Itoa(i), "/select/logsql/stats_query", url.Values{"i": {strconv.Itoa(i)}})
			return err
		},
		"fetchQueryRangeWindow": func(p *Proxy, ctx context.Context, i int) error {
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
			startNs := int64(i) * int64(time.Hour)
			window := queryRangeWindow{startNs: startNs, endNs: startNs + int64(time.Hour) - 1}
			_, err := p.fetchQueryRangeWindow(ctx, r, "*", "100", 100, window, logQueryShape{}, false, false)
			return err
		},
	}

	cancellers := map[string]func(t *testing.T, b *holdingBackend, run func(ctx context.Context, i int) error){
		"WithCancelCause": func(t *testing.T, b *holdingBackend, run func(ctx context.Context, i int) error) {
			ctx, cancel := context.WithCancelCause(context.Background())
			var wg sync.WaitGroup
			for i := 0; i < inFlight; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := run(ctx, i); err == nil {
						t.Errorf("call %d: expected cancellation error", i)
					}
				}()
			}
			b.waitArrived(t, inFlight)
			cancel(errInternalBudget)
			wg.Wait()
		},
		"errgroup sibling error": func(t *testing.T, b *holdingBackend, run func(ctx context.Context, i int) error) {
			g, gctx := errgroup.WithContext(context.Background())
			for i := 0; i < inFlight; i++ {
				g.Go(func() error { return run(gctx, i) })
			}
			b.waitArrived(t, inFlight)
			g.Go(func() error { return errInternalBudget })
			if err := g.Wait(); !errors.Is(err, errInternalBudget) {
				t.Fatalf("errgroup returned %v, want the sibling error", err)
			}
		},
	}

	for callName, call := range calls {
		for cancelName, cancelFn := range cancellers {
			t.Run(callName+"/"+cancelName, func(t *testing.T) {
				b := newHoldingBackend(t, inFlight)
				p := newBreakerTestProxy(t, b.URL, threshold)
				cancelFn(t, b, func(ctx context.Context, i int) error { return call(p, ctx, i) })

				if state := p.breaker.State(); state != "closed" {
					t.Fatalf("internal cancellation opened the circuit breaker: state=%s", state)
				}
				b.hold.Store(false)
				if err := call(p, context.Background(), inFlight+1); err != nil {
					t.Fatalf("request after internal cancellation failed: %v", err)
				}
			})
		}
	}
}

func TestCircuitBreaker_RealTransportFailureOnLiveCancelableContextTrips(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // connection refused from here on

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			p := newBreakerTestProxy(t, "http://"+addr, 2)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			for i := 0; i < 2; i++ {
				var callErr error
				if method == http.MethodGet {
					_, callErr = p.vlGet(ctx, "/select/logsql/query", url.Values{"query": {"*"}})
				} else {
					_, callErr = p.vlPost(ctx, "/select/logsql/query", url.Values{"query": {"*"}})
				}
				if callErr == nil {
					t.Fatalf("expected connection refused on attempt %d", i+1)
				}
			}
			if state := p.breaker.State(); state != "open" {
				t.Fatalf("connection refused on a live context must open the breaker, got state=%s", state)
			}
		})
	}
}

// A windowed query_range whose one window loses its connection cancels its
// sibling windows. Only that genuine failure may reach the breaker; the
// cancelled siblings must not open it and fail every other client.
func TestQueryRangeWindow_FailedSiblingWindowDoesNotOpenBreaker(t *testing.T) {
	const windows = 6
	start := time.Now().Add(-8 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(windows*time.Hour) - 1

	var queryCalls atomic.Int64
	arrived := make(chan struct{}, windows)
	stop := make(chan struct{})
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
			return
		}
		_ = r.ParseForm()
		reqStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		if queryCalls.Add(1) > windows {
			http.Error(w, "unexpected extra window fetch", http.StatusInternalServerError)
			return
		}
		if reqStart != start {
			arrived <- struct{}{}
			select { // sibling window: held until cancelled
			case <-r.Context().Done():
			case <-stop:
			}
			return
		}
		// First window: wait for every sibling, then drop the connection.
		for i := 1; i < windows; i++ {
			select {
			case <-arrived:
			case <-time.After(10 * time.Second):
				t.Errorf("sibling windows did not arrive")
			}
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("response writer cannot hijack")
			return
		}
		conn, _, err := hj.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer vlBackend.Close()
	defer close(stop) // runs before Close

	p, err := New(Config{
		BackendURL:                 vlBackend.URL,
		Cache:                      cache.New(60*time.Second, 10000),
		LogLevel:                   "error",
		CBFailThreshold:            3,
		CBOpenDuration:             time.Minute,
		QueryRangeWindowingEnabled: true,
		QueryRangeSplitInterval:    time.Hour,
		QueryRangeMaxParallel:      windows,
		QueryRangeAdaptiveParallel: false,
		QueryRangeFreshness:        10 * time.Minute,
		QueryRangeHistoryCacheTTL:  24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100",
		url.QueryEscape(`{app="nginx"}`), start, end,
	), nil)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)
	if resp.Code < http.StatusInternalServerError {
		t.Fatalf("expected the windowed query to fail, got %d body=%s", resp.Code, resp.Body.String())
	}

	if state := p.breaker.State(); state != "closed" {
		t.Fatalf("cancelled sibling windows opened the circuit breaker: state=%s", state)
	}
	labels := httptest.NewRecorder()
	p.handleLabels(labels, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	if labels.Code != http.StatusOK {
		t.Fatalf("unrelated /labels request failed after the windowed query: %d body=%s", labels.Code, labels.Body.String())
	}
}

func TestShouldRecordBreakerFailure_UsesCallContext(t *testing.T) {
	transportErr := &url.Error{Op: "Post", URL: "http://vl/select/logsql/stats_query", Err: errors.New("connection reset by peer")}

	live, cancelLive := context.WithCancelCause(context.Background())
	defer cancelLive(nil)
	if !shouldRecordBreakerFailure(live, transportErr) {
		t.Fatal("a transport failure on a live context must count")
	}
	var unknown context.Context
	if !shouldRecordBreakerFailure(unknown, transportErr) {
		t.Fatal("a transport failure without a known context must count")
	}
	if got := upstreamErrorStatus(live, transportErr); got != http.StatusBadGateway {
		t.Fatalf("upstreamErrorStatus on a live context = %d, want 502", got)
	}

	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(errInternalBudget)
	causeErr := &url.Error{Op: "Post", URL: "http://vl/select/logsql/stats_query", Err: context.Cause(canceled)}
	if errors.Is(causeErr, context.Canceled) {
		t.Fatal("test premise: a cancel-cause error does not wrap context.Canceled")
	}
	if shouldRecordBreakerFailure(canceled, causeErr) {
		t.Fatal("a failure on a cancelled context must not count, whatever the cause")
	}
	if shouldRecordBreakerFailure(canceled, transportErr) {
		t.Fatal("a transport error on an already cancelled context must not count")
	}
	if got := upstreamErrorStatus(canceled, causeErr); got != 499 {
		t.Fatalf("upstreamErrorStatus on a cancelled context = %d, want 499", got)
	}

	expired, cancelExpired := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelExpired()
	<-expired.Done()
	if got := upstreamErrorStatus(expired, transportErr); got != http.StatusGatewayTimeout {
		t.Fatalf("upstreamErrorStatus on an expired context = %d, want 504", got)
	}
}
