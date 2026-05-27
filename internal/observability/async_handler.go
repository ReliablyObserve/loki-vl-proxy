package observability

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncHandler wraps an slog.Handler to decouple request goroutines from
// the serialized output write. Records are buffered in a channel and drained
// by a single writer goroutine, eliminating the slog output mutex from the
// request hot path.
//
// If the channel is full, records are dropped and a drop counter is
// incremented. The writer goroutine periodically logs the drop count.
type AsyncHandler struct {
	inner  slog.Handler
	ch     chan slog.Record
	done   chan struct{}
	wg     sync.WaitGroup
	drops  atomic.Int64
	closed atomic.Bool
}

// NewAsyncHandler creates an async slog handler with the given buffer size.
// A buffer of 4096 handles bursts of ~4000 concurrent log writes without drops.
func NewAsyncHandler(inner slog.Handler, bufSize int) *AsyncHandler {
	if bufSize <= 0 {
		bufSize = 4096
	}
	h := &AsyncHandler{
		inner: inner,
		ch:    make(chan slog.Record, bufSize),
		done:  make(chan struct{}),
	}
	h.wg.Add(1)
	go h.drain()
	return h
}

func (h *AsyncHandler) drain() {
	defer h.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case r, ok := <-h.ch:
			if !ok {
				return
			}
			_ = h.inner.Handle(context.Background(), r)
		case <-ticker.C:
			if dropped := h.drops.Swap(0); dropped > 0 {
				dr := slog.NewRecord(time.Now(), slog.LevelWarn, "async log buffer full, records dropped", 0)
				dr.AddAttrs(slog.Int64("dropped_count", dropped))
				_ = h.inner.Handle(context.Background(), dr)
			}
		case <-h.done:
			for {
				select {
				case r, ok := <-h.ch:
					if !ok {
						return
					}
					_ = h.inner.Handle(context.Background(), r)
				default:
					return
				}
			}
		}
	}
}

// Stop signals the writer goroutine to drain remaining records and exit.
func (h *AsyncHandler) Stop() {
	if h.closed.CompareAndSwap(false, true) {
		close(h.done)
		h.wg.Wait()
	}
}

func (h *AsyncHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *AsyncHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.ch <- r:
	default:
		h.drops.Add(1)
	}
	return nil
}

func (h *AsyncHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &AsyncHandler{
		inner: h.inner.WithAttrs(attrs),
		ch:    h.ch,
		done:  h.done,
		drops: h.drops,
	}
}

func (h *AsyncHandler) WithGroup(name string) slog.Handler {
	return &AsyncHandler{
		inner: h.inner.WithGroup(name),
		ch:    h.ch,
		done:  h.done,
		drops: h.drops,
	}
}
