package lower

import "testing"

// BenchmarkExtractionListIdentityLowering lowers aggregations over a range function after an extraction list (the
// shapes whose series identity the lowering writes), two it turns away and one after a parser of every key, from their
// plans.
func BenchmarkExtractionListIdentityLowering(b *testing.B) {
	for _, q := range []string{
		`sum(max_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg(stddev_over_time({app="uj"} | json amount, route | unwrap amount [5m]))`,
		`avg by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`sum(stdvar_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		// Turned away: an alias in the list, a count over the series.
		`sum(max_over_time({app="uj"} | json a="amount", route | unwrap a [5m]))`,
		`count by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		// A parser of every key (the series hash), unchanged.
		`sum by (ns) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		// The maximum of maxima and the minimum of minima: one stats pipe over the rows.
		`max by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`min(min_over_time({app="uj"} | json amount, route | unwrap amount [5m]))`,
		`max by (ns) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
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
