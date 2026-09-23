package proxy

import (
	"context"
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

// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend, limits/metadata-scan-queue-429, limits/metadata-not-queued
func TestIsMetadataScanRequest(t *testing.T) {
	now := time.Now()
	params := func(rng time.Duration) url.Values {
		v := url.Values{"query": {"*"}}
		if rng > 0 {
			v.Set("start", strconv.FormatInt(now.Add(-rng).UnixNano(), 10))
			v.Set("end", strconv.FormatInt(now.UnixNano(), 10))
		}
		return v
	}
	for _, tc := range []struct {
		name   string
		path   string
		params url.Values
		want   bool
	}{
		{"stream field names over 7d", "/select/logsql/stream_field_names", params(7 * 24 * time.Hour), true},
		{"stream field names at the threshold", "/select/logsql/stream_field_names", params(6 * time.Hour), true},
		{"stream field names over 1h", "/select/logsql/stream_field_names", params(time.Hour), false},
		{"stream field names without a range", "/select/logsql/stream_field_names", params(0), true},
		{"stream field values over 24h", "/select/logsql/stream_field_values", params(24 * time.Hour), true},
		{"field values over 5m", "/select/logsql/field_values", params(5 * time.Minute), false},
		{"field names over 7d", "/select/logsql/field_names", params(7 * 24 * time.Hour), true},
		{"streams over 7d", "/select/logsql/streams", params(7 * 24 * time.Hour), true},
		{"stats is heavy, not a metadata scan", "/select/logsql/stats_query_range", params(7 * 24 * time.Hour), false},
		{"raw query is heavy, not a metadata scan", "/select/logsql/query", params(7 * 24 * time.Hour), false},
		{"hits is heavy, not a metadata scan", "/select/logsql/hits", params(7 * 24 * time.Hour), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMetadataScanRequest(tc.path, tc.params, 6*time.Hour); got != tc.want {
				t.Fatalf("isMetadataScanRequest(%s, %v) = %v, want %v", tc.path, tc.params, got, tc.want)
			}
			if isHeavyBackendRequest(tc.path, tc.params, 6*time.Hour) && tc.want {
				t.Fatalf("%s classified as both heavy and a metadata scan", tc.path)
			}
		})
	}
}

// A long-range metadata listing that finds every metadata-scan slot busy
// waits the shared queue wait and then fails with Loki's 429 naming
// -backend-max-concurrent-metadata-scans, while a short-range /labels and a
// heavy metric query (its own limiter) keep being served.
//
// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend, limits/metadata-scan-queue-429, limits/metadata-not-queued, loki_api_v1_labels
func TestMetadataScanAdmission_SaturatedReturns429NamingFlag(t *testing.T) {
	unblock := make(chan struct{})
	var longScans, shortScans, statsCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/stream_field_names", "/select/logsql/field_names":
			if rng, ok := backendParamsRange(r.Form); ok && rng >= 6*time.Hour {
				longScans.Add(1)
				select {
				case <-unblock:
				case <-r.Context().Done():
					return
				}
			} else {
				shortScans.Add(1)
			}
			_, _ = io.WriteString(w, `{"values":[{"value":"app","hits":1}]}`)
		case "/select/logsql/stats_query_range":
			statsCalls.Add(1)
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		default:
			_, _ = io.WriteString(w, `{"values":[]}`)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                        backend.URL,
		Cache:                             cache.New(time.Millisecond, 10),
		LogLevel:                          "error",
		BackendMaxConcurrentHeavyQueries:  1,
		BackendMaxConcurrentMetadataScans: 1,
		BackendHeavyQueryQueueWait:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	now := time.Now()
	labelsURL := func(rng time.Duration, query string) string {
		q := url.Values{
			"start": {strconv.FormatInt(now.Add(-rng).UnixNano(), 10)},
			"end":   {strconv.FormatInt(now.UnixNano(), 10)},
		}
		if query != "" {
			q.Set("query", query)
		}
		return "/loki/api/v1/labels?" + q.Encode()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, labelsURL(7*24*time.Hour, ""), nil))
	}()
	deadline := time.Now().Add(2 * time.Second)
	for longScans.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if longScans.Load() == 0 {
		t.Fatal("first long-range listing never reached the backend")
	}

	started := time.Now()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, labelsURL(7*24*time.Hour, `{env="production"}`), nil))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("saturated long-range labels status = %d, want 429; body=%s", w.Code, w.Body.String())
	}
	if waited := time.Since(started); waited < 100*time.Millisecond {
		t.Fatalf("rejected after %s, before the queue wait elapsed", waited)
	}
	body := w.Body.String()
	for _, want := range []string{"too many outstanding requests", "-backend-max-concurrent-metadata-scans=1", "-backend-heavy-query-queue-wait=100ms"} {
		if !strings.Contains(body, want) {
			t.Fatalf("429 body lacks %q: %s", want, body)
		}
	}
	if strings.Contains(body, "-backend-max-concurrent-heavy-queries") {
		t.Fatalf("metadata rejection names the heavy-query flag: %s", body)
	}
	if got := longScans.Load(); got != 1 {
		t.Fatalf("rejected listing still reached VictoriaLogs: %d long scans", got)
	}

	sw := httptest.NewRecorder()
	mux.ServeHTTP(sw, httptest.NewRequest(http.MethodGet, labelsURL(time.Hour, ""), nil))
	if sw.Code != http.StatusOK {
		t.Fatalf("short-range labels while scans saturate = %d, want 200; body=%s", sw.Code, sw.Body.String())
	}
	if shortScans.Load() == 0 {
		t.Fatal("short-range listing was not sent to the backend")
	}

	metricURL := "/loki/api/v1/query_range?" + url.Values{
		"query": {`sum by (level) (count_over_time({app="x"}[1h]))`},
		"start": {strconv.FormatInt(now.Add(-24*time.Hour).Truncate(time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.Truncate(time.Hour).UnixNano(), 10)},
		"step":  {"3600"},
	}.Encode()
	mw := httptest.NewRecorder()
	mux.ServeHTTP(mw, httptest.NewRequest(http.MethodGet, metricURL, nil))
	if mw.Code != http.StatusOK || statsCalls.Load() == 0 {
		t.Fatalf("heavy metric query while metadata scans saturate = %d (stats calls %d), want 200 on its own limiter; body=%s", mw.Code, statsCalls.Load(), mw.Body.String())
	}
	close(unblock)
	wg.Wait()
}

// Background inventory work (warm-ups, keep-warm, stale refreshes) never
// queues: with every slot busy it is rejected at once and retried later.
//
// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend
func TestMetadataScanAdmission_BackgroundInventorySkipsInsteadOfWaiting(t *testing.T) {
	p, err := New(Config{
		BackendURL:                        "http://127.0.0.1:1",
		Cache:                             cache.New(time.Millisecond, 10),
		LogLevel:                          "error",
		BackendMaxConcurrentMetadataScans: 1,
		BackendHeavyQueryQueueWait:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	release, err := p.metadataScanLimiter.acquire(context.Background(), "0")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	now := time.Now()
	params := url.Values{
		"query": {"*"},
		"start": {strconv.FormatInt(now.Add(-7*24*time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.UnixNano(), 10)},
	}
	started := time.Now()
	_, err = p.admitBackendRequest(withBackgroundInventory(context.Background()), "/select/logsql/stream_field_names", params)
	if !isHeavyQueryQueueFull(err) {
		t.Fatalf("background scan on a busy limiter: err = %v, want queue-full", err)
	}
	if waited := time.Since(started); waited > 500*time.Millisecond {
		t.Fatalf("background scan waited %s for a slot; it must skip immediately", waited)
	}
	if !strings.Contains(err.Error(), metadataScanLimitFlag) {
		t.Fatalf("rejection does not name %s: %v", metadataScanLimitFlag, err)
	}

	// A synchronous request on the same limiter does wait for the slot.
	started = time.Now()
	_, err = p.admitBackendRequest(context.Background(), "/select/logsql/stream_field_names", params)
	if !isHeavyQueryQueueFull(err) {
		t.Fatalf("synchronous scan on a busy limiter: err = %v, want queue-full", err)
	}
	if waited := time.Since(started); waited < 2*time.Second {
		t.Fatalf("synchronous scan waited only %s, want the 2s queue wait", waited)
	}
}

// A failed warm of a preset window is retried after one keep-warm interval,
// then two, doubling up to an hour; success clears the delay.
//
// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend
func TestLabelWarmBackoff_DoublesToAnHourAndResetsOnSuccess(t *testing.T) {
	b := newLabelWarmBackoff()
	now := time.Now()
	window := 7 * 24 * time.Hour
	if !b.ready(window, now) {
		t.Fatal("a window that never failed must be ready")
	}
	interval := 225 * time.Second
	want := []time.Duration{interval, 2 * interval, 4 * interval, 8 * interval, 16 * interval, time.Hour, time.Hour}
	for i, delay := range want {
		if got := b.failed(window, now, interval); got != delay {
			t.Fatalf("failure %d: delay = %s, want %s", i+1, got, delay)
		}
		if b.ready(window, now.Add(delay-time.Second)) {
			t.Fatalf("failure %d: window ready before its delay elapsed", i+1)
		}
		if !b.ready(window, now.Add(delay)) {
			t.Fatalf("failure %d: window not ready once its delay elapsed", i+1)
		}
	}
	if !b.ready(time.Hour, now) {
		t.Fatal("another window must not inherit the backoff")
	}
	b.succeeded(window)
	if !b.ready(window, now) {
		t.Fatal("a successful warm must clear the backoff")
	}
	if got := b.failed(window, now, interval); got != interval {
		t.Fatalf("delay after a success = %s, want the base interval %s", got, interval)
	}
	var nilBackoff *labelWarmBackoff
	if !nilBackoff.ready(window, now) || nilBackoff.failed(window, now, interval) != 0 {
		t.Fatal("a nil backoff must be a no-op")
	}
}

// conformance: backend-admission-and-heavy-query-queueing, limits/concurrent-full-retention-scans-exhaust-backend
func TestLabelKeepWarmDelay_JittersAroundTheInterval(t *testing.T) {
	interval := 225 * time.Second
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 200; i++ {
		d := labelKeepWarmDelay(interval)
		if d < interval-interval/4 || d >= interval+interval/4 {
			t.Fatalf("delay %s outside [%s, %s)", d, interval-interval/4, interval+interval/4)
		}
		seen[d] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("keep-warm delay is not jittered")
	}
	if got := labelKeepWarmDelay(0); got != 0 {
		t.Fatalf("delay for a zero interval = %s, want 0", got)
	}
}
