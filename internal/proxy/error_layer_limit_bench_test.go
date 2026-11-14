package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"
)

// limitBenchCases build a limit or queue failure where it happens and hand it
// to the error layer: the whole path of a rejected request, the construction
// of the error included, with the request error log line on (a discarding
// handler at info level).
func limitBenchCases() []struct {
	name string
	make func() error
} {
	return []struct {
		name string
		make func() error
	}{
		{"series_limit", func() error { return newSeriesLimitError(context.Background(), 500) }},
		{"row_limit", func() error { return newRowLimitError(1000000, "", 0) }},
		{"binary_budget", func() error { return newBinaryBudgetError(flagBinaryMetricMaxArrays) }},
		{"buffer_limit", func() error { return newBackendBodyTooLarge(64 << 20) }},
		{"manual_metric_limit", func() error { return newManualMetricResponseTooLarge(64 << 20) }},
		{"label_values_limit", func() error { return newLabelValuesTooLarge(64<<20+1, 64<<20) }},
		{"heavy_queue", func() error {
			return newHeavyQueueFull(&heavyQueryQueueFullError{maxConcurrent: 2, queueWait: 20 * time.Second})
		}},
	}
}

// BenchmarkErrorLayerLimitAnswer is the answer to a request that hit a proxy
// limit or a full queue, from the failure site to the written body.
func BenchmarkErrorLayerLimitAnswer(b *testing.B) {
	p := &Proxy{log: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	for _, bc := range limitBenchCases() {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				p.writeErr(httptest.NewRecorder(), context.Background(), bc.make())
			}
		})
	}
}
