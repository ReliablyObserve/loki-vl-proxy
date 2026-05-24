//go:build stress

package proxy

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// TestTranslationConcurrency fires TranslateLogQL from 50 goroutines simultaneously.
// Run with: go test -tags=stress -race -run TestTranslation ./internal/proxy/
//
// The race detector is the primary value here — it catches data races in the
// translation pipeline that single-threaded unit tests cannot expose.
func TestTranslationConcurrency(t *testing.T) {
	queries := []string{
		`{namespace="prod"}`,
		`count_over_time({app="api-gw"}[5m])`,
		`rate({namespace="prod"}[1m])`,
		`sum by (app) (count_over_time({namespace="prod"}[5m]))`,
		`avg_over_time({app="worker"} | unwrap duration_ms [5m])`,
		`max_over_time({app="api-gw"} | unwrap duration_ms [5m])`,
		`sum(rate({namespace="prod"}[1m])) by (app)`,
		`label_replace(count_over_time({app="api-gw"}[5m]),"dest","$1","app","(.*)")`,
		`rate({app="api-gw"}[1m]) / rate({app="auth"}[1m])`,
		`sum_over_time({app="db"} | unwrap duration_ms [10m])`,
	}

	const goroutines = 50
	const itersPerGoroutine = 200

	// Pre-compute expected outputs (single-threaded, no race possible).
	expected := make(map[string]string, len(queries))
	for _, q := range queries {
		out, err := translator.TranslateLogQL(q)
		if err != nil {
			t.Fatalf("baseline translation of %q failed: %v", q, err)
		}
		expected[q] = out
	}

	var wg sync.WaitGroup
	errCh := make(chan string, goroutines*itersPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itersPerGoroutine; i++ {
				q := queries[(id+i)%len(queries)]
				got, err := translator.TranslateLogQL(q)
				if err != nil {
					errCh <- fmt.Sprintf("goroutine %d iter %d: translate(%q): %v", id, i, q, err)
					return
				}
				if got != expected[q] {
					errCh <- fmt.Sprintf("goroutine %d iter %d: translate(%q): got %q, want %q", id, i, q, got, expected[q])
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	var failures int
	for msg := range errCh {
		t.Error(msg)
		failures++
		if failures >= 10 {
			t.Log("(truncated after 10 failures)")
			break
		}
	}

	t.Logf("translation stress: %d goroutines × %d iters = %d calls completed",
		goroutines, itersPerGoroutine, goroutines*itersPerGoroutine)
}

// TestTranslationWithStreamFieldsConcurrency tests the stream-fields variant
// under concurrent load — the streamFields map is read-only once constructed
// but the label translation function exercises shared regex state.
func TestTranslationWithStreamFieldsConcurrency(t *testing.T) {
	streamFields := map[string]bool{
		"namespace": true,
		"app":       true,
		"env":       true,
	}

	queries := []string{
		`count_over_time({namespace="prod",app="api-gw"}[5m])`,
		`rate({namespace="prod"}[1m])`,
		`sum by (app) (rate({namespace="prod",env="production"}[1m]))`,
	}

	const goroutines = 30

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed int

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				q := queries[(id+i)%len(queries)]
				_, err := translator.TranslateLogQLWithStreamFields(q, nil, streamFields)
				if err != nil {
					mu.Lock()
					t.Errorf("goroutine %d: %v", id, err)
					failed++
					mu.Unlock()
					return
				}
			}
		}(g)
	}
	wg.Wait()
	t.Logf("stream-fields stress: %d goroutines, %d failures", goroutines, failed)
}
