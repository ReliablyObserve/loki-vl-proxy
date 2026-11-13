package proxy

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"
)

// BenchmarkBackendCallRetryOn is BenchmarkBackendCall with the shared retry on
// (the flag defaults): the happy path pays the wrapper's checks and one budget
// load, nothing else.
func BenchmarkBackendCallRetryOn(b *testing.B) {
	for _, method := range []string{"GET", "POST"} {
		b.Run(method, func(b *testing.B) {
			p := benchBackendProxy(b, retryConfig())
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
