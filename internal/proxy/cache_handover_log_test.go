package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// What a restoring replica logs: the plan, progress with an estimate while it
// runs, a warning once when the timeout will cut it, and how it ended.

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) lines(t *testing.T, message string) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if msg, _ := m["msg"].(string); strings.HasPrefix(msg, message) {
			out = append(out, m)
		}
	}
	return out
}

// stalledCtx blocks the warm-up before its second batch until released.
type stalledCtx struct {
	context.Context
	calls   int
	release chan struct{}
}

func (c *stalledCtx) Err() error {
	if c.calls++; c.calls == 2 {
		<-c.release
		return context.DeadlineExceeded
	}
	return nil
}

// conformance: cache-restart-handover
func TestCacheHandover_LogsPlanProgressWarningAndEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	backend := healthyBackend(t)
	first := openHandoverProxy(t, path, backend.URL, Config{CacheShutdownFlushTimeout: 5 * time.Second, CacheStartupWarmBytes: 1 << 19})
	for i := 0; i < 600; i++ {
		first.c.SetWithTTL(fmt.Sprintf("query:t:%03d", i), []byte("value"), time.Hour)
	}
	first.shutdown(t)

	// Not initialised: the test drives the warm-up itself.
	next := openHandoverProxy(t, path, backend.URL, Config{CacheStartupWarmBytes: 1 << 19})
	defer next.shutdown(t)
	logs := &logBuffer{}
	next.p.log = slog.New(slog.NewJSONHandler(logs, nil))

	deadline, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ctx := &stalledCtx{Context: deadline, release: make(chan struct{})}
	stop := next.p.logResponseCacheWarmProgress(deadline, time.Millisecond)
	result := make(chan cache.HandoverWarmReport, 1)
	go func() {
		result <- next.c.WarmFromHandover(ctx, cache.HandoverWarmOptions{Scope: next.p.cacheHandover.scope(), MaxBytes: 1 << 19, OnPlan: next.p.logResponseCacheWarmPlan})
	}()
	// The stalled warm-up has a third behind it and more time ahead of it than
	// the deadline leaves once enough of it has passed.
	for waited := time.Now(); len(logs.lines(t, "response cache warm-up will not finish")) == 0; time.Sleep(time.Millisecond) {
		if time.Since(waited) > 5*time.Second {
			t.Fatal("no warning before the deadline")
		}
	}
	stop()
	close(ctx.release)
	next.p.logResponseCacheWarmEnd(<-result)

	plan := logs.lines(t, "response cache warm-up started")
	if len(plan) != 1 || plan[0]["planned_entries"] != float64(600) || plan[0]["planned_disk_bytes"].(float64) <= 0 || plan[0]["budget_bytes"] != float64(1<<19) || plan[0]["estimate_assumes_disk_bytes_per_second"] == nil {
		t.Fatalf("plan lines %v", plan)
	}
	progress := logs.lines(t, "response cache warm-up progress")
	if len(progress) == 0 {
		t.Fatal("no progress line")
	}
	last := progress[len(progress)-1]
	for _, field := range []string{"source", "percent", "entries", "bytes", "bytes_per_second", "eta_ms", "time_left_ms", "elapsed_ms"} {
		if _, ok := last[field]; !ok {
			t.Fatalf("progress line misses %s: %v", field, last)
		}
	}
	warnings := logs.lines(t, "response cache warm-up will not finish")
	if len(warnings) != 1 || warnings[0]["level"] != "WARN" {
		t.Fatalf("warnings %v", warnings)
	}
	end := logs.lines(t, "response cache warmed from the restart hand-over")
	if len(end) != 1 || end[0]["level"] != "WARN" || end[0]["outcome"] != "deadline" || end[0]["disk_entries"] != float64(256) {
		t.Fatalf("end %v", end)
	}
	if reasons, _ := end[0]["skipped_by"].(map[string]any); reasons["deadline"] != float64(344) {
		t.Fatalf("skip reasons %v", end[0]["skipped_by"])
	}
}
