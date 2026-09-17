package proxy

import "testing"

// Loki's batchRangeVectorIterator.load (pkg/logql/range_vector.go, v3.7.1)
// skips `sample.Timestamp <= start` for every range aggregation — the comment
// in that loop reads "the lower bound of the range is not inclusive", and the
// iterator is shared by unwrapped ranges too. An unwrapped `sum_over_time`,
// `avg_over_time`, `min_over_time`, `max_over_time` or `quantile_over_time`
// must therefore drop a sample sitting exactly on windowStart, the same way
// count_over_time does.
func TestLowerBoundIsExclusiveForEveryRangeFunction(t *testing.T) {
	samples := []rangeMetricSample{{ts: 0, value: 1}, {ts: 10, value: 2}, {ts: 20, value: 3}}
	for _, tc := range []struct {
		fn   string
		want float64
	}{
		{"sum", 5},   // 2+3, not 1+2+3
		{"avg", 2.5}, // (2+3)/2, not 2
		{"min", 2},   // not 1
		{"max", 3},   // unchanged, but must not read the excluded sample
		// q=0 discriminates: over {1,2,3} it is 1, over the correct {2,3} it is 2.
		{"quantile", 2},
	} {
		q := 0.0
		got, ok := aggregateManualWindow(tc.fn, q, samples, 0, 20, 20)
		if !ok {
			t.Fatalf("%s: aggregateManualWindow reported no data", tc.fn)
		}
		if got != tc.want {
			t.Errorf("%s over (0, 20] of {0:1, 10:2, 20:3} = %v, want %v (ts=0 is on the excluded lower bound)", tc.fn, got, tc.want)
		}
	}
}
