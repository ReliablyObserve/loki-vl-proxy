package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockingHandler counts handled records and optionally blocks on a release
// channel, simulating a slow downstream writer.
type blockingHandler struct {
	mu      sync.Mutex
	records []slog.Record
	block   chan struct{} // if non-nil, Handle waits on it before returning
}

func (b *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (b *blockingHandler) Handle(_ context.Context, r slog.Record) error {
	if b.block != nil {
		<-b.block
	}
	b.mu.Lock()
	b.records = append(b.records, r)
	b.mu.Unlock()
	return nil
}

func (b *blockingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &blockingHandler{block: b.block}
}

func (b *blockingHandler) WithGroup(name string) slog.Handler {
	return &blockingHandler{block: b.block}
}

func (b *blockingHandler) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

func waitForCount(t *testing.T, b *blockingHandler, want int, deadline time.Duration) {
	t.Helper()
	stop := time.Now().Add(deadline)
	for time.Now().Before(stop) {
		if b.count() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected at least %d records within %s, got %d", want, deadline, b.count())
}

func TestNewAsyncHandler_DefaultBufferSize(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 0)
	defer h.Stop()
	if cap(h.ch) != 4096 {
		t.Fatalf("expected default buffer size 4096, got %d", cap(h.ch))
	}
}

func TestNewAsyncHandler_CustomBufferSize(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 8)
	defer h.Stop()
	if cap(h.ch) != 8 {
		t.Fatalf("expected buffer size 8, got %d", cap(h.ch))
	}
}

func TestAsyncHandler_HandleEnqueuesAndDrains(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 16)
	defer h.Stop()

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "hello", 0)
	if err := h.Handle(context.Background(), r); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	waitForCount(t, inner, 1, time.Second)
}

func TestAsyncHandler_Enabled(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 4)
	defer h.Stop()
	if !h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("expected Enabled to pass through to inner handler")
	}
}

func TestAsyncHandler_FullBufferDropsAndIncrementsCounter(t *testing.T) {
	release := make(chan struct{})
	inner := &blockingHandler{block: release}
	h := NewAsyncHandler(inner, 1)
	defer func() {
		close(release)
		h.Stop()
	}()

	// First record is consumed by the drain goroutine and blocks in Handle.
	first := slog.NewRecord(time.Now(), slog.LevelInfo, "first", 0)
	if err := h.Handle(context.Background(), first); err != nil {
		t.Fatalf("first Handle returned error: %v", err)
	}
	// Give the drain goroutine time to read the first record into Handle().
	time.Sleep(20 * time.Millisecond)

	// Buffer capacity is 1 — fill it.
	if err := h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "buffered", 0)); err != nil {
		t.Fatalf("buffered Handle returned error: %v", err)
	}

	// Next 5 should be dropped because drain is blocked and buffer is full.
	for i := 0; i < 5; i++ {
		_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "dropped", 0))
	}
	if got := h.drops.Load(); got != 5 {
		t.Fatalf("expected 5 drops, got %d", got)
	}
}

func TestAsyncHandler_StopIsIdempotent(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 4)
	h.Stop()
	// Second Stop must not panic on close(done) or wg.Wait().
	h.Stop()
}

func TestAsyncHandler_StopDrainsPendingRecords(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 64)

	for i := 0; i < 5; i++ {
		_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0))
	}
	h.Stop()
	if got := inner.count(); got != 5 {
		t.Fatalf("expected Stop to drain 5 pending records, got %d", got)
	}
}

func TestAsyncHandler_WithAttrsSharesPlumbing(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 8)
	defer h.Stop()

	w := h.WithAttrs([]slog.Attr{slog.String("tenant", "a")}).(*AsyncHandler)
	if w.ch != h.ch || w.done != h.done || w.drops != h.drops {
		t.Fatal("WithAttrs must share channel/done/drops with parent")
	}
}

func TestAsyncHandler_WithGroupSharesPlumbing(t *testing.T) {
	inner := &blockingHandler{}
	h := NewAsyncHandler(inner, 8)
	defer h.Stop()

	w := h.WithGroup("req").(*AsyncHandler)
	if w.ch != h.ch || w.done != h.done || w.drops != h.drops {
		t.Fatal("WithGroup must share channel/done/drops with parent")
	}
}

func TestAsyncHandler_EndToEndThroughSlog(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	h := NewAsyncHandler(inner, 32)
	defer h.Stop()

	logger := slog.New(h)
	logger.Info("request handled", "endpoint", "query_range")

	// Stop drains synchronously so the JSON write definitely lands in buf.
	h.Stop()

	if !strings.Contains(buf.String(), `"endpoint":"query_range"`) {
		t.Fatalf("expected log to be written via inner handler, got: %s", buf.String())
	}
}
