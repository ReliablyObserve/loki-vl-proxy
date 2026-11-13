package proxy

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// benchBackendProxy is a proxy whose VictoriaLogs call is answered in process,
// so a benchmark measures the proxy's cost of one call and nothing else.
func benchBackendProxy(b *testing.B, cfg Config) *Proxy {
	b.Helper()
	c := cache.New(time.Minute, 100)
	b.Cleanup(c.Close)
	cfg.BackendURL = "http://127.0.0.1:1"
	cfg.Cache = c
	cfg.LogLevel = "error"
	p, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.client.Transport = benchRoundTrip(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"_msg":"x"}` + "\n")), Request: req, ProtoMajor: 1, ProtoMinor: 1}, nil
	})
	return p
}

type benchRoundTrip func(*http.Request) (*http.Response, error)

func (f benchRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// BenchmarkBackendCall is the cost of one VictoriaLogs read on the happy path
// (the proxy's side of it: building the request, the admission, the retry
// wrapper, reading the answer).
func BenchmarkBackendCall(b *testing.B) {
	for _, method := range []string{"GET", "POST"} {
		b.Run(method, func(b *testing.B) {
			p := benchBackendProxy(b, Config{})
			params := url.Values{"query": {`{app="api"} | limit 7`}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var resp *http.Response
				var err error
				if method == "GET" {
					resp, err = p.vlGet(context.Background(), "/select/logsql/query", params)
				} else {
					resp, err = p.vlPost(context.Background(), "/select/logsql/query", params)
				}
				if err != nil {
					b.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
}
