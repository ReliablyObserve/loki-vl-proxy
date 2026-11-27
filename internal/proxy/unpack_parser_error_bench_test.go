package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// BenchmarkUnpackMetricHandler measures the query_range handler for metrics over an unpack, the ones the parser error
// check covers and a sum with no grouping it does not, in front of a VictoriaLogs answering 5000 JSON lines.
func BenchmarkUnpackMetricHandler(b *testing.B) {
	base := time.Unix(1700000000, 0).UTC()
	for _, query := range []string{
		`sum by (namespace) (count_over_time({namespace="prod"} | unpack [2m]))`,
		`sort(rate({namespace="prod"} | unpack [2m]))`,
		`sum(count_over_time({namespace="prod"} | unpack [2m]))`,
	} {
		b.Run(query, func(b *testing.B) {
			p, backend := makeBenchProxy(b, ndJSON5000)
			defer backend.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params := url.Values{}
				params.Set("query", query)
				params.Set("start", strconv.FormatInt(base.Unix(), 10))
				// A distinct end per iteration keeps the response cache out of the loop.
				params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix()+int64(i%1000), 10))
				params.Set("step", "30")
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}
