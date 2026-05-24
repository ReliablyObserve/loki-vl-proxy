//go:build stress

package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// TestCoalescerSameKeyStorm fires 50 goroutines with the same query key
// concurrently. The singleflight coalescer must deduplicate: upstream is
// called at most a handful of times (not 50), and all goroutines receive the
// same response body.
//
// Run with: go test -tags=stress -race -run TestCoalescer ./internal/proxy/
func TestCoalescerSameKeyStorm(t *testing.T) {
	const concurrency = 50
	wantBody := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api-gw"},"values":[[1700000000,"42"]]}]}}`

	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		// Simulate upstream latency so goroutines genuinely pile up.
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, wantBody)
	}))
	defer backend.Close()

	c := middleware.NewCoalescer()
	key := "same-key-storm"

	type result struct {
		body []byte
		err  error
	}

	results := make([]result, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, body, err := c.Do(key, func() (*http.Response, error) {
				return http.Get(backend.URL) //nolint:noctx
			})
			results[idx] = result{body: body, err: err}
		}(i)
	}
	wg.Wait()

	// All goroutines must succeed.
	for i, r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d: %v", i, r.err)
		} else if string(r.body) != wantBody {
			t.Errorf("goroutine %d: body mismatch\ngot:  %s\nwant: %s", i, r.body, wantBody)
		}
	}

	calls := backendCalls.Load()
	t.Logf("same-key storm: %d goroutines, %d upstream calls (dedup ratio %.1fx)",
		concurrency, calls, float64(concurrency)/float64(calls))

	// Singleflight must deduplicate heavily; allow a small number of calls in
	// case goroutines arrive after a previous call completes.
	if calls > 5 {
		t.Errorf("expected ≤5 upstream calls for %d concurrent same-key requests, got %d", concurrency, calls)
	}
}

// TestCoalescerMixedKeyLoad fires 50 goroutines each with a unique query key.
// Each goroutine must receive its own distinct response (no cross-contamination).
func TestCoalescerMixedKeyLoad(t *testing.T) {
	const concurrency = 50

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo the key from the query string back as the response body.
		key := r.URL.Query().Get("key")
		time.Sleep(5 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"key":%q}`, key)
	}))
	defer backend.Close()

	c := middleware.NewCoalescer()

	type result struct {
		key  string
		body []byte
		err  error
	}
	results := make([]result, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("unique-key-%04d", idx)
			u := backend.URL + "?key=" + url.QueryEscape(key)
			_, _, body, err := c.Do(key, func() (*http.Response, error) {
				return http.Get(u) //nolint:noctx
			})
			results[idx] = result{key: key, body: body, err: err}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d (key=%s): %v", i, r.key, r.err)
			continue
		}
		wantSnippet := fmt.Sprintf("%q", r.key)
		if !bytes.Contains(r.body, []byte(wantSnippet)) {
			t.Errorf("goroutine %d: response contaminated\nbody: %s\nwant key: %s", i, r.body, r.key)
		}
	}
	t.Logf("mixed-key load: %d goroutines, each received its own response", concurrency)
}

// TestCoalescerNoGoroutineLeaks verifies no goroutines are leaked after all
// calls complete. Uses a slow backend to force goroutines to queue up.
func TestCoalescerNoGoroutineLeaks(t *testing.T) {
	const concurrency = 30

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, `{"status":"success"}`)
	}))
	defer backend.Close()

	c := middleware.NewCoalescer()
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, _ = c.Do("leak-test", func() (*http.Response, error) {
				return http.Get(backend.URL) //nolint:noctx
			})
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		t.Logf("goroutine leak test: all %d goroutines completed cleanly", concurrency)
	case <-time.After(5 * time.Second):
		t.Fatal("goroutine leak: timed out waiting for goroutines to complete")
	}
}
