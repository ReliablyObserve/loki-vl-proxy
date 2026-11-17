package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkListedIdentityWindowsDecision is the per-request cost of deciding the windows of a query_range at a step
// shorter than the range (dynamicIdentityWindows) and of its response key's fences (responseKeyFences), for an
// extraction list's identity, a parser of every key's and a query of neither, with the plan memo warm.
func BenchmarkListedIdentityWindowsDecision(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	for _, q := range []string{
		`sum(stdvar_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`sum by (ns) (rate({app="uw"} | logfmt [5m]))`,
	} {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {q}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}.Encode(), nil)
		p.dynamicIdentityWindows(r, q)
		responseKeyFences(r, q, responseKeyInnerQueryRange)
		b.Run(q, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				p.dynamicIdentityWindows(r, q)
				_ = responseKeyFences(r, q, responseKeyInnerQueryRange)
			}
		})
	}
}
