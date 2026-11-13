package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
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

// BenchmarkQueryRefusalAnswer is the whole answer to a query the proxy accepts
// and the translation then refuses (the translator's refusal of `sum(1)`, and
// of a stage that is only a name), through the handler, with the request error
// log line on: base and branch both run it. The branch answers Loki's text, the
// base answered the translator's.
func BenchmarkQueryRefusalAnswer(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, LogLevel: "error", BackendTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	p.log = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	for _, bc := range []struct{ name, query string }{
		{"vector_aggregation_over_scalar", `sum(1)`},
		{"unknown_pipeline_stage", `{a="b"} | foo`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			target := "/loki/api/v1/query_range?" + url.Values{"query": {bc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}.Encode()
			b.ReportAllocs()
			for b.Loop() {
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, target, nil))
				if rec.Code < 400 {
					b.Fatalf("status %d, want an error", rec.Code)
				}
			}
		})
	}
}
