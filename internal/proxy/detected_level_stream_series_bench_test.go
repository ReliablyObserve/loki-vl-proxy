package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkBuildMetricSeriesEntryStreamLevel times the series builder of a
// per-stream stats row that carries no level column (a without fold), once
// per distinct stream as the handler calls it.
func BenchmarkBuildMetricSeriesEntryStreamLevel(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	stream := `{app="payment-service",cluster="us-east-1",container="payment",env="production",level="error",namespace="prod",pod="payment-service-391a2-9208",service_name="payment-service"}`
	groupBy := []string{"_stream"}
	b.ReportAllocs()
	for b.Loop() {
		_ = p.buildMetricSeriesEntry(stream, "", nil, groupBy, false, nil)
	}
}

// BenchmarkResponseKeyFencesWithout times the fence chain of a without fold's
// query_range key.
func BenchmarkResponseKeyFencesWithout(b *testing.B) {
	const query = `sum without (pod) (rate({service_name="payment-service"} | logfmt amount | unwrap amount [5m]))`
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {query}, "start": {"1"}, "end": {"3600"}, "step": {"60"}}.Encode(), nil)
	b.ReportAllocs()
	for b.Loop() {
		_ = responseKeyFences(r, query, responseKeyInnerQueryRange)
	}
}

// BenchmarkResponseKeyFencesUnfenced times the fence chain of a query no
// fence fires on (the screen cost every query pays).
func BenchmarkResponseKeyFencesUnfenced(b *testing.B) {
	const query = `sum by (service_name) (count_over_time({service_name="payment-service"} |= "error" [5m]))`
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {query}, "start": {"1"}, "end": {"3600"}, "step": {"60"}}.Encode(), nil)
	b.ReportAllocs()
	for b.Loop() {
		_ = responseKeyFences(r, query, responseKeyInnerQueryRange)
	}
}

// BenchmarkResponseKeyFencesLogQuery times the fence chain of a log query.
func BenchmarkResponseKeyFencesLogQuery(b *testing.B) {
	const query = `{service_name="payment-service"} |= "error" | logfmt | status >= 500`
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {query}, "start": {"1"}, "end": {"3600"}, "limit": {"100"}}.Encode(), nil)
	b.ReportAllocs()
	for b.Loop() {
		_ = responseKeyFences(r, query, responseKeyInnerQueryRange)
	}
}
