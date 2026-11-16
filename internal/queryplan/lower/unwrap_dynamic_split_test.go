package lower

import (
	"math"
	"math/rand"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// splitRangeOps and splitVectorOps are the functions taken per series and the aggregations over them that a
// parser's dynamic identity covers (dynamicIdentity).
var (
	splitRangeOps = []logql.RangeOp{logql.RangeAvgOverTime, logql.RangeMaxOverTime, logql.RangeMinOverTime,
		logql.RangeStddevOverTime, logql.RangeStdvarOverTime}
	splitVectorOps = []logql.VectorOp{logql.VectorSum, logql.VectorMax, logql.VectorMin, logql.VectorAvg}
)

// overTime is a range function over one series' samples, as Loki defines it (population stddev and stdvar).
func overTime(op logql.RangeOp, xs []float64) float64 {
	mean := 0.0
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	switch op {
	case logql.RangeAvgOverTime:
		return mean
	case logql.RangeMaxOverTime, logql.RangeMinOverTime:
		v := xs[0]
		for _, x := range xs[1:] {
			if op == logql.RangeMaxOverTime {
				v = math.Max(v, x)
			} else {
				v = math.Min(v, x)
			}
		}
		return v
	}
	variance := 0.0
	for _, x := range xs {
		variance += (x - mean) * (x - mean)
	}
	variance /= float64(len(xs))
	if op == logql.RangeStddevOverTime {
		return math.Sqrt(variance)
	}
	return variance
}

// overSeries is a vector aggregation over the series' values.
func overSeries(op logql.VectorOp, vs []float64) float64 {
	v := vs[0]
	sum := 0.0
	for _, x := range vs {
		sum += x
		switch op {
		case logql.VectorMax:
			v = math.Max(v, x)
		case logql.VectorMin:
			v = math.Min(v, x)
		}
	}
	switch op {
	case logql.VectorSum:
		return sum
	case logql.VectorAvg:
		return sum / float64(len(vs))
	}
	return v
}

// aggregateBySplit is op over rng taken per series, the samples split into series by series[i].
func aggregateBySplit(vec logql.VectorOp, rng logql.RangeOp, samples []float64, series []int) float64 {
	by := map[int][]float64{}
	var order []int
	for i, s := range series {
		if _, ok := by[s]; !ok {
			order = append(order, s)
		}
		by[s] = append(by[s], samples[i])
	}
	vs := make([]float64, 0, len(order))
	for _, s := range order {
		vs = append(vs, overTime(rng, by[s]))
	}
	return overSeries(vec, vs)
}

// The aggregations the lowering answers without the series hash (seriesSplitFree) are exactly the ones whose answer
// is the same for every split of a group's samples into series: over random samples (negative values, repeats,
// single-sample series), Loki's split (each line's parsed labels) and the stream's coarser split (series merged)
// agree on every trial for a maximum of maximums and a minimum of minimums, and differ on some trial for every other
// pair, which so keeps the dynamic identity.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesSplitFreeAggregations(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, vec := range splitVectorOps {
		for _, rng := range splitRangeOps {
			differs := 0
			for trial := 0; trial < 2000; trial++ {
				n := 1 + r.Intn(12)
				samples := make([]float64, n)
				fine, coarse := make([]int, n), make([]int, n)
				merge := 1 + r.Intn(3)
				for i := range samples {
					samples[i] = float64(r.Intn(21) - 10)
					fine[i] = r.Intn(5)
					coarse[i] = fine[i] / merge // a stream holds several series
				}
				a, b := aggregateBySplit(vec, rng, samples, fine), aggregateBySplit(vec, rng, samples, coarse)
				if math.Abs(a-b) > 1e-9*math.Max(1, math.Abs(a)) {
					differs++
				}
			}
			if free := seriesSplitFree(vec, rng); free != (differs == 0) {
				t.Errorf("%s over %s: seriesSplitFree %v, but %d of 2000 random splits differ", vec, rng, free, differs)
			}
		}
	}
}

// A maximum of maximums and a minimum of minimums are not written with the dynamic identity, with or without a label
// filter after the parser; the other aggregations over a function taken per series keep it.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentitySkipsSplitFreeAggregations(t *testing.T) {
	for q, want := range map[string]bool{
		`max by (app) (max_over_time({app="a"} | json | unwrap n [5m]))`:                                 false,
		`min(min_over_time({app="a"} | logfmt | unwrap n [5m]))`:                                         false,
		`max by (service_name) (max_over_time({a="b"} | json | unwrap duration_ms | __error__="" [1m]))`: false,
		`max by (app) (min_over_time({app="a"} | json | unwrap n [5m]))`:                                 true,
		`sum(max_over_time({app="a"} | json | unwrap n [5m]))`:                                           true,
		`min(avg_over_time({app="a"} | json | unwrap n [5m]))`:                                           true,
		`sum(stddev_over_time({a="b"} | json | __error__="" | unwrap duration_ms [5m]))`:                 true,
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		if got := DynamicUnwrapIdentity(p); got != want || WritesDynamicUnwrapIdentity(p) != want {
			t.Errorf("%s: dynamic identity %v, want %v", q, got, want)
		}
	}
}
