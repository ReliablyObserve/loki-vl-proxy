package proxy

import (
	"math"
	"strconv"
	"testing"
	"time"
)

// stddev and stdvar fold every sample at a step before label_replace's duplicates are filed under one series
// (pkg/logql evaluator VectorAggEvaluator.Next, then JoinSampleVector), so over label sets that label_replace
// made equal they are the population deviation and variance of all the apps' samples, not of one merged series
// (which would read 0). On the ranked route's stack, with the extent cache on and off.
//
// conformance: semantics/label-replace-duplicates-before-ranking, loki_api_v1_query_range
func TestFoldOverLabelSetsLabelReplaceMadeEqual(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(6, end, 3*time.Hour)
	start, step, window := end.Add(-time.Hour), 15*time.Second, 5*time.Minute
	all := lokiSlidingReference(lines, "count_over_time", true, 0, start, end, step, window)
	for _, extent := range []bool{true, false} {
		s := newRankedTopKStack(t, lines, extent, nil)
		for _, fn := range []string{"stddev", "stdvar"} {
			query := fn + `(label_replace(sum by (app) (count_over_time({env="production"}[5m])), "app", "all", "app", ".+"))`
			code, body := s.ask(t, query, start, end, step, nil)
			got := decodeRankedTopK(t, code, body)
			if len(got) != 1 || len(got[0].Metric) != 0 {
				t.Fatalf("%s extent=%v: %d series, want one without labels: %+v", fn, extent, len(got), got)
			}
			points := map[int64]float64{}
			for _, pair := range got[0].Values {
				ts, _ := pair[0].(float64)
				v, _ := strconv.ParseFloat(pair[1].(string), 64)
				points[int64(ts)] = v
			}
			checked, nonZero := 0, 0
			for ts := start; !ts.After(end); ts = ts.Add(step) {
				var samples []float64
				for _, byTS := range all {
					if sample, ok := byTS[ts.Unix()]; ok {
						v, _ := strconv.ParseFloat(sample, 64)
						samples = append(samples, v)
					}
				}
				if len(samples) == 0 {
					continue
				}
				mean := 0.0
				for _, v := range samples {
					mean += v
				}
				mean /= float64(len(samples))
				variance := 0.0
				for _, v := range samples {
					variance += (v - mean) * (v - mean)
				}
				variance /= float64(len(samples))
				want := variance
				if fn == "stddev" {
					want = math.Sqrt(variance)
				}
				got, ok := points[ts.Unix()]
				if !ok || math.Abs(got-want) > 1e-6*math.Max(1, math.Abs(want)) {
					t.Fatalf("%s extent=%v ts=%d: got %v (present %v), want %v over %v", fn, extent, ts.Unix(), got, ok, want, samples)
				}
				checked++
				if want != 0 {
					nonZero++
				}
			}
			if checked == 0 || nonZero == 0 {
				t.Fatalf("%s extent=%v: %d steps checked, %d non-zero: the fixture does not tell a fold from a merge", fn, extent, checked, nonZero)
			}
		}
	}
}
