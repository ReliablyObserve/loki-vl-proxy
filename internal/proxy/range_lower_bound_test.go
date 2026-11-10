package proxy

import (
	"math"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// Loki's batchRangeVectorIterator.load (pkg/logql/range_vector.go:148-189, v3.7.7)
// skips `sample.Timestamp <= start` for every range aggregation — the comment
// in that loop reads "the lower bound of the range is not inclusive", and the
// iterator is shared by unwrapped ranges too. An unwrapped `sum_over_time`,
// `avg_over_time`, `min_over_time`, `max_over_time` or `quantile_over_time`
// must therefore drop a sample sitting exactly on windowStart, the same way
// count_over_time does.
//
// conformance: semantics/lower-bound-exclusive, window-bounds-and-step-alignment
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

// TestRateCounterReadsLokiWindow: rate_counter, which the raw-row evaluator
// answers, reads the window (t-range, t] through the handler on query_range and
// at an instant: the counter value on the lower edge (100) is out and the one
// at t (50) is in, so the increase over 10, 20, 50 is 40 per minute. With the
// lower edge in it would count a reset (50), with the line at t out only 10,
// with both (the old bounds) 20.
// The value is this evaluator's increase over the window; Loki extrapolates it
// to the window edges (pkg/logql/range_vector.go:275-346), which is not covered.
//
// conformance: semantics/lower-bound-exclusive, window-bounds-and-step-alignment
func TestRateCounterReadsLokiWindow(t *testing.T) {
	const at = int64(1_800_000_000) * 1e9
	rows := []vlemu.Row{
		{TS: at - 60e9, Stream: `{app="uw",pod="a"}`, Value: 100},
		{TS: at - 40e9, Stream: `{app="uw",pod="a"}`, Value: 10},
		{TS: at - 20e9, Stream: `{app="uw",pod="a"}`, Value: 20},
		{TS: at, Stream: `{app="uw",pod="a"}`, Value: 50},
	}
	query := `sum by (pod) (rate_counter({app="uw"} | unwrap amount [1m]))`
	for _, step := range []time.Duration{time.Minute, 0} {
		emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.53.0"}
		c := owCase{shape: owShape{query: func(string) string { return query }}, step: step, start: at, end: at}
		got := runOverTimeCase(t, emu, c, 0)
		if v, ok := got["pod=a"][at]; !ok || math.Abs(v-40.0/60) > 1e-12 {
			t.Fatalf("step %v: %v, want 0.666667 (the increase over (t-1m, t])", step, got)
		}
		if len(emu.EndpointCalls("/select/logsql/query")) == 0 {
			t.Fatalf("step %v: expected the raw-row evaluator", step)
		}
	}
}
