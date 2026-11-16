package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// quantileReference is the phi-quantile of values as quantile_over_time documents it, with linear interpolation
// between the two closest ranks: NaN for no value, -Inf for phi below 0, +Inf above 1, and otherwise, the values
// sorted ascending (NaN first, as sort.Float64s orders them), rank phi*(n-1) between the values at its floor and
// the next, weighted by its fraction: lower*(1-w) + upper*w. TestQuantileReferenceMatchesRecordedLoki holds it to
// Loki's answers, NaN and the infinities included.
func quantileReference(phi float64, values []float64) float64 {
	switch {
	case len(values) == 0 || math.IsNaN(phi):
		return math.NaN()
	case phi < 0:
		return math.Inf(-1)
	case phi > 1:
		return math.Inf(1)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := float64(len(sorted))
	rank := phi * (n - 1)
	lower := math.Max(0, math.Floor(rank))
	upper := math.Min(n-1, lower+1)
	w := rank - math.Floor(rank)
	return sorted[int(lower)]*(1-w) + sorted[int(upper)]*w
}

// quantileWindowReference is quantileReference over the samples in [start, end), each value repeated by its
// count: nonEmpty says a sample is in the window, computed that the values were few enough to expand (at most
// 1<<16).
func quantileWindowReference(phi float64, samples []quantileSample, start, end int64) (value float64, nonEmpty, computed bool) {
	var values []float64
	total := 0.0
	for _, s := range samples {
		if s.ts >= start && s.ts < end {
			nonEmpty = true
			total += s.count
		}
	}
	if !nonEmpty || total > 1<<16 {
		return 0, nonEmpty, false
	}
	for _, s := range samples {
		if s.ts >= start && s.ts < end {
			for k := 0; k < int(s.count); k++ {
				values = append(values, s.value)
			}
		}
	}
	return quantileReference(phi, values), true, true
}

// The quantile reference gives Loki's quantile_over_time over recorded value sets (to the last bits a fused
// multiply-add changes; NaN and the infinities exactly): one value, two,
// repeated values, negatives, NaN, +Inf and -Inf, phi at and between the bounds and above 1
// (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/unwrap-quantile-response-limit
func TestQuantileReferenceMatchesRecordedLoki(t *testing.T) {
	const at = int64(1700003600)
	pool := []float64{0, 0.25, 1, 1, 2.5, 3, -1.5, -7, 100.125, 7.75, math.NaN(), math.Inf(1), math.Inf(-1)}
	// A negative phi is no recordable request: Loki's parser rejects a negative first argument (400 "unexpected -,
	// expecting NUMBER"), so the phi < 0 rule rests on the documented bound alone.
	phis := []float64{0, 0.01, 0.25, 0.5, 0.9, 0.95, 0.99, 1, 1.1, 2}
	rng := rand.New(rand.NewSource(99))
	type tc struct {
		phi    float64
		values []float64
	}
	var cases []tc
	var sets []oraclerun.EngineDataSet
	for i := 0; i < 400; i++ {
		n := 1 + rng.Intn(9)
		if i%7 == 0 {
			n = 1 + rng.Intn(2)
		}
		var values []float64
		st := oraclerun.EngineStream{Labels: map[string]string{"k": "q"}}
		for j := 0; j < n; j++ {
			v := pool[rng.Intn(len(pool))]
			if i%3 != 0 && (math.IsNaN(v) || math.IsInf(v, 0)) {
				v = float64(rng.Intn(20)) / 4
			}
			values = append(values, v)
			st.Entries = append(st.Entries, oraclerun.EngineEntry{TS: (at - 1000 + int64(j)) * 1e9, Line: "v=" + strconv.FormatFloat(v, 'f', -1, 64)})
		}
		phi := phis[rng.Intn(len(phis))]
		if i%5 == 0 {
			phi = float64(rng.Intn(1000)) / 1000
		}
		query := fmt.Sprintf(`quantile_over_time(%s, {k="q"} | logfmt | unwrap v [1h])`, strconv.FormatFloat(phi, 'f', -1, 64))
		cases = append(cases, tc{phi, values})
		sets = append(sets, oraclerun.EngineDataSet{Streams: []oraclerun.EngineStream{st}, Requests: []oraclerun.EngineRequest{{ID: fmt.Sprint(i), Instant: true, Form: url.Values{"query": {query}, "time": {strconv.FormatInt(at, 10)}}.Encode()}}})
	}
	answers := lokiEngineAnswers(t, "quantile_reference", sets)
	for i, c := range cases {
		points, _ := lokiMetricPoints(t, fmt.Sprint(i), answers[fmt.Sprint(i)])
		if len(points) != 1 {
			t.Fatalf("#%d: %d series", i, len(points))
		}
		for _, series := range points {
			for _, v := range series {
				// The interpolation may be fused into one multiply-add on one architecture and not on another
				// (the recording and the run may differ), so finite values agree to the last bits; NaN and the
				// infinities exactly.
				if want := quantileReference(c.phi, c.values); !sameQuantile(v, want) {
					t.Fatalf("#%d quantile %v of %v: reference %v, Loki %v", i, c.phi, c.values, want, v)
				}
			}
		}
	}
}
