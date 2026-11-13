package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"testing"
)

// errorLayerBenchCases are failures as the handlers hand them to the error
// layer: a failed VictoriaLogs call, a refused connection, a rejected query,
// a series limit and a cancelled request.
func errorLayerBenchCases() []struct {
	name string
	ctx  context.Context
	err  error
} {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	return []struct {
		name string
		ctx  context.Context
		err  error
	}{
		{"vl500", context.Background(), &upstreamStatusError{status: 500, msg: "VL backend returned 500", class: vlErrorUnclassified}},
		{"refused", context.Background(), &url.Error{Op: "Post", URL: "http://10.0.0.9:9428/select/logsql/query", Err: errors.New("dial tcp 10.0.0.9:9428: connect: connection refused")}},
		{"vl400_rejected", context.Background(), &upstreamStatusError{status: 400, msg: "cannot parse query arg: x", class: vlErrorQueryRejected}},
		{"series_limit", context.Background(), newSeriesLimitError(context.Background(), 500)},
		{"canceled", canceled, context.Canceled},
	}
}

// BenchmarkErrorLayerWriteErr is the error layer's writer for the failures
// the handlers meet, with the request error log line on (a discarding
// handler at info level), as the base and the branch both run it.
func BenchmarkErrorLayerWriteErr(b *testing.B) {
	p := &Proxy{log: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	for _, bc := range errorLayerBenchCases() {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				p.writeErr(httptest.NewRecorder(), bc.ctx, bc.err)
			}
		})
	}
}
