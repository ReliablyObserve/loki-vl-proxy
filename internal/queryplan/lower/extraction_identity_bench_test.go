package lower

import "testing"

// BenchmarkExtractionListIdentityLowering lowers aggregations over a range function after an extraction list (the
// shapes whose series identity the lowering writes), and one after a parser of every key, from their plans.
func BenchmarkExtractionListIdentityLowering(b *testing.B) {
	for _, q := range []string{
		`sum(max_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg(stddev_over_time({app="uj"} | json amount, route | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
	} {
		p := rangeAggPlan(q)
		b.Run(q, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := PlanQuery(q, p, Profile{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
