package proxy

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// The read grid of a range request: what the window evaluator read for it, so the error layer's log line says which
// step and bucket grid made a read pass its bound (issue #1795). The client text stays Loki's; the operator sees that
// the step, not the range, sized the read.

type readGridNoteKey struct{}

// readGridNote is the bucket grid a request's window evaluator read: its route (the stats buckets of
// gcd(step, window), or the recurrence's step-sized window grids), the request's step and window, the bucket of the
// reads and how many of them one series spans.
type readGridNote struct {
	mu                   sync.Mutex
	set                  bool
	route                string
	step, window, bucket time.Duration
	buckets              int64
}

// withReadGridNote gives the request a note its window evaluator fills.
func withReadGridNote(ctx context.Context) context.Context {
	return context.WithValue(ctx, readGridNoteKey{}, &readGridNote{})
}

// noteReadGrid records the request's bucket grid; a later note (the route the evaluator went on to) replaces it.
func noteReadGrid(ctx context.Context, route string, step, window, bucket time.Duration, buckets int64) {
	n, _ := ctx.Value(readGridNoteKey{}).(*readGridNote)
	if n == nil {
		return
	}
	n.mu.Lock()
	n.set, n.route, n.step, n.window, n.bucket, n.buckets = true, route, step, window, bucket, buckets
	n.mu.Unlock()
}

// readGridBuckets is the buckets of one series over [start, end] and the window before it.
func readGridBuckets(start, end time.Time, window, bucket time.Duration) int64 {
	if bucket <= 0 {
		return 0
	}
	return int64((end.Sub(start) + window) / bucket)
}

// readGridAttrs appends the request's bucket grid to a log line's attributes, when its evaluator chose one.
func readGridAttrs(ctx context.Context, attrs []slog.Attr) []slog.Attr {
	if ctx == nil {
		return attrs
	}
	n, _ := ctx.Value(readGridNoteKey{}).(*readGridNote)
	if n == nil {
		return attrs
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.set {
		return attrs
	}
	return append(attrs, slog.String("read_route", n.route), slog.Duration("query_step", n.step), slog.Duration("range_window", n.window),
		slog.Duration("read_bucket", n.bucket), slog.Int64("read_buckets_per_series", n.buckets))
}
