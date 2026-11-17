package proxy

import (
	"net/url"
	"testing"
)

// BenchmarkRequestChain_MetricQueryRange_Hit times a metric query_range cache hit through the whole route chain,
// the request rewrite (lowercaseQueryKeywords) included, in the spelling Grafana sends.
func BenchmarkRequestChain_MetricQueryRange_Hit(b *testing.B) {
	benchmarkRequestChain(b, `/loki/api/v1/query_range?query=`+url.QueryEscape(`sum by (level) (count_over_time({app="nginx"} |= "error" [1m]))`)+`&step=15`)
}

// BenchmarkRequestQueryText times the request rewrite's text (lowercaseQueryKeywords) for queries it keeps, in the
// spellings Grafana and Logs Drilldown send, and for one it respells.
func BenchmarkRequestQueryText(b *testing.B) {
	for _, c := range []struct{ name, q string }{
		{"metric", `sum by (level) (count_over_time({service_name="api"} | json | level=~"error|warn" [$__auto]))`},
		{"ranking", `topk(5, sum by (app) (rate({app=~".+"} |= "x" [5m]))) / on(app) group_left sum by (app) (rate({app=~".+"}[5m]))`},
		{"log", `{service_name="api"} | logfmt | line_format "{{.msg}}" |= "a (b)"`},
		{"respelled", `sum by (level) ( count_over_time ({service_name="api"} | json [5m]) )`},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = requestQueryText(c.q, 0)
			}
		})
	}
}
