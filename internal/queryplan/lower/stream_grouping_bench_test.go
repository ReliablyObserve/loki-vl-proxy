package lower

import "testing"

// BenchmarkStreamGroupedParsersAt is the shape check of the stream grouped sum rule over a plan in hand.
func BenchmarkStreamGroupedParsersAt(b *testing.B) {
	for _, q := range []string{
		`sum by (level) (count_over_time({app="x"} | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum by (level) (count_over_time({app="x"} | json | drop __error__, __error_details__ [5m]))`,
	} {
		p := planOfTB(b, q)
		b.Run(q[46:70], func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = StreamGroupedParsersAt(p)
			}
		})
	}
}
