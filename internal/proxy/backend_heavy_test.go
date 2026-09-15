package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestHeavyQueryLimiter_SaturatedQueueRejectsWithLokiMessage(t *testing.T) {
	l := newHeavyQueryLimiter(1, 50*time.Millisecond)
	release, err := l.acquire(context.Background(), "t")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	started := time.Now()
	_, err = l.acquire(context.Background(), "t")
	if !isHeavyQueryQueueFull(err) {
		t.Fatalf("expected queue-full error, got %v", err)
	}
	if waited := time.Since(started); waited < 40*time.Millisecond {
		t.Fatalf("rejected after %s, expected to queue for the configured wait", waited)
	}
	for _, want := range []string{"too many outstanding requests", "-backend-max-concurrent-heavy-queries=1", "-backend-heavy-query-queue-wait=50ms"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
	if inUse, queued := l.stats(); inUse != 1 || queued != 0 {
		t.Fatalf("after rejection inUse=%d queued=%d, want 1/0", inUse, queued)
	}
	release()
	release() // idempotent
	if inUse, _ := l.stats(); inUse != 0 {
		t.Fatalf("after release inUse=%d, want 0", inUse)
	}
	if _, err := l.acquire(context.Background(), "t"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestHeavyQueryLimiter_ZeroWaitRejectsImmediately(t *testing.T) {
	l := newHeavyQueryLimiter(1, 0)
	if _, err := l.acquire(context.Background(), "t"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.acquire(context.Background(), "t"); !isHeavyQueryQueueFull(err) {
		t.Fatalf("expected immediate queue-full error, got %v", err)
	}
}

func TestHeavyQueryLimiter_QueuedCallGetsReleasedSlot(t *testing.T) {
	l := newHeavyQueryLimiter(1, 5*time.Second)
	release, _ := l.acquire(context.Background(), "t")
	got := make(chan error, 1)
	go func() {
		r, err := l.acquire(context.Background(), "t")
		if r != nil {
			r()
		}
		got <- err
	}()
	waitForQueued(t, l, 1)
	release()
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("queued acquire failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued acquire was not granted after release")
	}
}

func TestHeavyQueryLimiter_ReleasedSlotGoesToLeastServedTenant(t *testing.T) {
	l := newHeavyQueryLimiter(2, 5*time.Second)
	a1, _ := l.acquire(context.Background(), "a")
	a2, _ := l.acquire(context.Background(), "a")
	defer a2()
	order := make(chan string, 2)
	go func() {
		r, err := l.acquire(context.Background(), "a")
		if err == nil {
			order <- "a"
			r()
		}
	}()
	waitForQueued(t, l, 1)
	go func() {
		r, err := l.acquire(context.Background(), "b")
		if err == nil {
			order <- "b"
			defer r()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	waitForQueued(t, l, 2)
	a1()
	select {
	case first := <-order:
		if first != "b" {
			t.Fatalf("slot went to tenant %q, want the tenant holding no slots", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no waiter was granted")
	}
}

func TestHeavyQueryLimiter_CanceledWaiterLeavesQueue(t *testing.T) {
	l := newHeavyQueryLimiter(1, 5*time.Second)
	release, _ := l.acquire(context.Background(), "t")
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		_, err := l.acquire(ctx, "t")
		got <- err
	}()
	waitForQueued(t, l, 1)
	cancel()
	if err := <-got; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if _, queued := l.stats(); queued != 0 {
		t.Fatalf("canceled waiter still queued: %d", queued)
	}
}

func waitForQueued(t *testing.T, l *heavyQueryLimiter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, queued := l.stats(); queued >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected %d queued waiters", n)
}

func TestIsHeavyBackendRequest(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	ns := func(d time.Duration) string { return strconv.FormatInt(now.Add(-d).UnixNano(), 10) }
	end := strconv.FormatInt(now.UnixNano(), 10)
	cases := []struct {
		name   string
		path   string
		params url.Values
		heavy  bool
	}{
		{"raw metric fetch", "/select/logsql/query", url.Values{"query": {`{app="a"} | limit 1000001`}, "start": {ns(5 * time.Minute)}, "end": {end}}, true},
		{"log query limit arg", "/select/logsql/query", url.Values{"query": {`{app="a"}`}, "limit": {"1000"}, "start": {ns(7 * 24 * time.Hour)}, "end": {end}}, false},
		{"log query limit pipe", "/select/logsql/query", url.Values{"query": {`{app="a"} | sort by (_time desc) | limit 5000`}, "start": {ns(24 * time.Hour)}, "end": {end}}, false},
		{"unbounded short raw query", "/select/logsql/query", url.Values{"query": {`{app="a"}`}, "start": {ns(time.Hour)}, "end": {end}}, false},
		{"unbounded long raw query", "/select/logsql/query", url.Values{"query": {`{app="a"}`}, "start": {ns(24 * time.Hour)}, "end": {end}}, true},
		{"long stats range", "/select/logsql/stats_query_range", url.Values{"query": {`* | stats count()`}, "start": {now.Add(-24 * time.Hour).Format(time.RFC3339Nano)}, "end": {now.Format(time.RFC3339Nano)}, "step": {"60s"}}, true},
		{"short stats range", "/select/logsql/stats_query_range", url.Values{"query": {`* | stats count()`}, "start": {ns(time.Hour)}, "end": {end}, "step": {"5s"}}, false},
		{"fine bucket grid", "/select/logsql/stats_query_range", url.Values{"query": {`* | stats count()`}, "start": {ns(time.Hour)}, "end": {end}, "step": {"100ms"}}, true},
		{"long hits range", "/select/logsql/hits", url.Values{"query": {`*`}, "start": {ns(7 * 24 * time.Hour)}, "end": {end}, "step": {"1h"}}, true},
		{"instant stats at time", "/select/logsql/stats_query", url.Values{"query": {`_time:5m | stats count()`}, "time": {end}}, false},
		{"field values", "/select/logsql/field_values", url.Values{"query": {`*`}, "field": {"app"}, "start": {ns(7 * 24 * time.Hour)}, "end": {end}}, false},
		{"stream field names", "/select/logsql/stream_field_names", url.Values{"query": {`*`}, "start": {ns(7 * 24 * time.Hour)}, "end": {end}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isHeavyBackendRequest(tc.path, tc.params, 6*time.Hour); got != tc.heavy {
				t.Fatalf("isHeavyBackendRequest = %v, want %v", got, tc.heavy)
			}
		})
	}
}

// A heavy call that finds every slot busy past the queue wait answers the
// client with Loki's queue-full status and message, while metadata requests
// keep flowing.
func TestHeavyQueryAdmission_SaturatedReturns429AndLabelsStillServe(t *testing.T) {
	unblock := make(chan struct{})
	var statsCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsCalls.Add(1)
			select {
			case <-unblock:
			case <-r.Context().Done():
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		case "/select/logsql/stream_field_names", "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"values":[{"value":"app","hits":1}]}`)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"values":[]}`)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                       backend.URL,
		Cache:                            cache.New(time.Millisecond, 10),
		LogLevel:                         "error",
		BackendMaxConcurrentHeavyQueries: 1,
		BackendHeavyQueryQueueWait:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	end := time.Now().Truncate(time.Hour)
	start := end.Add(-24 * time.Hour)
	metricURL := func(app string) string {
		q := url.Values{
			"query": {`sum by (level) (count_over_time({app="` + app + `"}[1h]))`},
			"start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
			"step":  {"3600"},
		}
		return "/loki/api/v1/query_range?" + q.Encode()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, metricURL("holder"), nil))
	}()
	deadline := time.Now().Add(2 * time.Second)
	for statsCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if statsCalls.Load() == 0 {
		t.Fatal("first heavy query never reached the backend")
	}

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, metricURL("queued"), nil))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("saturated heavy query status = %d, want 429; body=%s", w.Code, w.Body.String())
	}
	if body := w.Body.String(); !strings.Contains(body, "too many outstanding requests") || !strings.Contains(body, "-backend-max-concurrent-heavy-queries") {
		t.Fatalf("429 body does not carry Loki's message and the flag name: %s", body)
	}
	if got := statsCalls.Load(); got != 1 {
		t.Fatalf("rejected query still reached VictoriaLogs: %d stats calls", got)
	}

	now := time.Now()
	labelsURL := "/loki/api/v1/labels?start=" + strconv.FormatInt(now.Add(-5*time.Minute).UnixNano(), 10) + "&end=" + strconv.FormatInt(now.UnixNano(), 10)
	lw := httptest.NewRecorder()
	mux.ServeHTTP(lw, httptest.NewRequest(http.MethodGet, labelsURL, nil))
	if lw.Code != http.StatusOK {
		t.Fatalf("labels while heavy queries saturate = %d, want 200; body=%s", lw.Code, lw.Body.String())
	}
	close(unblock)
	wg.Wait()
}

func TestBackendRequests_PassRemainingBudgetAsVLTimeout(t *testing.T) {
	var got atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got.Store(r.Form.Get("timeout"))
		_, _ = io.WriteString(w, `{"values":[]}`)
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Millisecond, 10), LogLevel: "error", BackendTimeout: 90 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	resp, err := p.vlPost(context.Background(), "/select/logsql/query", url.Values{"query": {"*"}, "limit": {"1"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if timeout, _ := got.Load().(string); timeout != "90000ms" {
		t.Fatalf("timeout arg without a request deadline = %q, want the -backend-timeout budget 90000ms", timeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err = p.vlGet(ctx, "/select/logsql/field_names", url.Values{"query": {"*"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	timeout, _ := got.Load().(string)
	ms, convErr := strconv.Atoi(strings.TrimSuffix(timeout, "ms"))
	if convErr != nil || !strings.HasSuffix(timeout, "ms") || ms <= 0 || ms > 5000 {
		t.Fatalf("timeout arg with a 5s request deadline = %q, want at most 5000ms", timeout)
	}
}

func TestBackendRequests_ClientCancellationCancelsVLRequest(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// VictoriaLogs reads its form arguments first; the server then watches
		// the connection and cancels the request context on disconnect.
		_ = r.ParseForm()
		close(entered)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-time.After(10 * time.Second):
		}
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Millisecond, 10), LogLevel: "error", BackendMaxConcurrentHeavyQueries: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := p.vlPost(ctx, "/select/logsql/query", url.Values{"query": {`* | limit 1000001`}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the backend")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("VictoriaLogs request kept running after the client canceled")
	}
	if err := <-done; err == nil {
		t.Fatal("canceled backend call returned no error")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if inUse, _ := p.heavyQueryLimiter.stats(); inUse == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("canceled heavy call did not release its admission slot")
}

// A long-range raw metric fetch whose selector already matches more lines than
// -manual-range-metric-row-limit is rejected from a one-row count, before any
// log line leaves VictoriaLogs.
func TestRawMetricRowPrecheck_RejectsBeforeFetchingRows(t *testing.T) {
	var rawFetches, counts atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		switch {
		case r.URL.Path == "/select/logsql/query" && strings.HasSuffix(query, "| stats count() as rows"):
			counts.Add(1)
			_, _ = io.WriteString(w, `{"rows":"5000"}`+"\n")
		case r.URL.Path == "/select/logsql/query" && strings.Contains(query, "| limit "):
			rawFetches.Add(1)
		default:
			_, _ = io.WriteString(w, `{"values":[]}`)
		}
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Millisecond, 10), LogLevel: "error", RangeMetricRowLimit: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	params := url.Values{
		"query": {`rate({app="api"}[5m])`},
		"start": {strconv.FormatInt(end.Add(-24*time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"60"},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code < 400 || !strings.Contains(rec.Body.String(), "manual range metric row limit exceeded (100)") || !strings.Contains(rec.Body.String(), "-manual-range-metric-row-limit") {
		t.Fatalf("status %d body %s, want the row limit error naming its flag", rec.Code, rec.Body.String())
	}
	if counts.Load() != 1 || rawFetches.Load() != 0 {
		t.Fatalf("count prechecks=%d raw fetches=%d, want 1 and 0", counts.Load(), rawFetches.Load())
	}
}
