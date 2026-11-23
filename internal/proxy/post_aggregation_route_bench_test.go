package proxy

import "testing"

// BenchmarkPostAggregationRoute times the post-aggregation route's decision (parseInstantMetricPostAggQuery), which
// both query handlers take for every metric query before they translate it: a query with no ranking, label_replace
// around an aggregation (no ranking word: no plan read), label_replace around a ranking (one plan read, memoised),
// and the rankings the route took before.
func BenchmarkPostAggregationRoute(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"rate_by", `sum by (level) (rate({app="a"} |= "error" [5m]))`},
		{"replace_aggregation", `label_replace(sum by (x) (rate({app="a"}[5m])), "p", "x-$1", "p", "(.*)")`},
		{"replace_ranking", `label_replace(topk(2, sum by (x) (rate({app="a"}[5m]))), "p", "x-$1", "x", "(.*)")`},
		{"topk", `topk(2, sum by (x) (rate({app="a"}[5m])))`},
		{"sum_over_topk", `sum(topk(2, sum by (x) (rate({app="a"}[5m]))))`},
		{"stddev_by", `stddev by (x) (count_over_time({app="a"}[5m]))`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = parseInstantMetricPostAggQuery(tc.query)
			}
		})
	}
}
