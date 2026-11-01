package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

func (p *Proxy) observeBackendBody(ctx context.Context, body io.ReadCloser, path string, params url.Values, start time.Time) io.ReadCloser {
	if body == nil {
		return nil
	}
	g := &backendReadObservation{ReadCloser: body, p: p, ctx: ctx, path: path, start: start}
	from, okFrom := parseLokiTimeToUnixNano(params.Get("start"))
	to, okTo := parseLokiTimeToUnixNano(params.Get("end"))
	if okFrom && okTo && to >= from {
		g.hasSpan = true
		g.spanSeconds = (float64(to) - float64(from)) / float64(time.Second)
	}
	return g
}

// Count decoded bytes actually consumed, once per upstream body (before any
// coalesced replay or strip). Neither Content-Length nor EOF proves API parity.
type backendReadObservation struct {
	io.ReadCloser
	p           *Proxy
	ctx         context.Context
	path        string
	start       time.Time
	spanSeconds float64
	hasSpan     bool
	read        atomic.Int64
	eof, failed atomic.Bool
	once        sync.Once
	closeErr    error
}

func (g *backendReadObservation) BodyLimit() int64 {
	if bounded, ok := g.ReadCloser.(interface{ BodyLimit() int64 }); ok {
		return bounded.BodyLimit()
	}
	return 0
}

func (g *backendReadObservation) Read(b []byte) (int, error) {
	n, err := g.ReadCloser.Read(b)
	g.read.Add(int64(n))
	if err == io.EOF {
		g.eof.Store(true)
	} else if err != nil {
		g.failed.Store(true)
	}
	return n, err
}

func (g *backendReadObservation) Close() error {
	g.once.Do(func() {
		g.closeErr = g.ReadCloser.Close()
		outcome, level := "closed", slog.LevelDebug
		switch {
		case g.eof.Load() && !g.failed.Load() && g.closeErr == nil:
			outcome = "eof"
		case g.ctx.Err() == context.Canceled:
			outcome = "canceled"
		case g.ctx.Err() == context.DeadlineExceeded:
			outcome, level = "timeout", slog.LevelWarn
		case g.failed.Load() || g.closeErr != nil:
			outcome, level = "error", slog.LevelWarn
		}
		elapsed := time.Since(g.start)
		kind := "other"
		switch g.path {
		case "/select/logsql/query":
			kind = "raw"
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query", "/select/logsql/hits":
			kind = "stats"
		case "/select/logsql/field_names", "/select/logsql/field_values", "/select/logsql/stream_field_names", "/select/logsql/stream_field_values", "/select/logsql/streams":
			kind = "metadata"
		}
		g.p.metrics.RecordBackendReadBytes(kind, g.read.Load())
		g.p.observeInternalOperation(g.ctx, "backend_body_read", outcome, elapsed)
		if g.p.log.Enabled(g.ctx, level) {
			attrs := []any{"backend.route", g.path, "outcome", outcome, "decoded_bytes", g.read.Load(), "elapsed_seconds", elapsed.Seconds(), "eof_observed", g.eof.Load()}
			if g.hasSpan {
				attrs = append(attrs, "read_span_seconds", g.spanSeconds)
			}
			g.p.log.Log(g.ctx, level, "backend_body_read", attrs...)
		}
	})
	return g.closeErr
}
