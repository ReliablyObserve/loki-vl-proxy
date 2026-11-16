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

func newBenchProxy(b *testing.B) *Proxy {
	b.Helper()
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// earlyAnswers runs the answers a query_range request may get before any other work, for a query that gets none.
func earlyAnswers(p *Proxy, w http.ResponseWriter, r *http.Request, query string) bool {
	return p.answerEmptyResult(w, r, "query_range", query, time.Time{})
}

// BenchmarkQueryRangeEarlyAnswers: the screens a query_range request passes before its route, for a query the
// validation cache holds that gets no early answer.
func BenchmarkQueryRangeEarlyAnswers(b *testing.B) {
	p := newBenchProxy(b)
	q := `sum by (pod) (rate({app="api"} | json | level="error" [5m]))`
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+url.Values{"query": {q}, "start": {"1700001000"}, "end": {"1700003400"}, "step": {"600"}}.Encode(), nil)
	w := httptest.NewRecorder()
	_ = cachedQueryValidation(q)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if earlyAnswers(p, w, r, q) {
			b.Fatal("answered early")
		}
	}
}
