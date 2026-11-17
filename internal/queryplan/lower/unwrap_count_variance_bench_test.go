package lower

import "testing"

// BenchmarkCountOverStdvarLowering lowers count by labels over stdvar_over_time (the count of the series) and its
// neighbours, from their plans.
func BenchmarkCountOverStdvarLowering(b *testing.B) {
	for _, q := range []string{
		`count by (ns) (stdvar_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`count by (ns) (stdvar_over_time({app="uw"} | unwrap amount [5m]))`,
		`count by (ns, pod) (stdvar_over_time({app="uw"} | json | unwrap amount [5m]))`,
		// Unchanged: count over stddev_over_time, sum by over stdvar_over_time.
		`count by (ns) (stddev_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (stdvar_over_time({app="uw"} | unwrap amount [5m]))`,
	} {
		p := rangeAggPlan(q)
		b.Run(q, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := PlanQuery(q, p, Profile{StreamIdentity: true}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
