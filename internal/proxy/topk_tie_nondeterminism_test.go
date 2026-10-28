package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Loki keeps the series it meets first when several tie across the k-th place
// (pkg/logql/evaluator.go VectorAggEvaluator.Next replaces the heap root only on
// a strictly better value), and the inner `sum by` hands the series over in
// Go map order. Which of the tied series ranks therefore changes from one
// evaluation to the next, while the ranked values never do. The proxy breaks the
// tie on the label key, so it answers the same series every time. These two
// steps are the ones the visual stack's seeded data holds at 5s resolution
// (pricing-svc: error 5, warn 5, info 20; batch-etl: warn 25, error 1, info 1).
//
// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, loki_api_v1_query_range
func TestTopKTieAtKthPlaceLokiPicksByInsertionOrderProxyByLabelKey(t *testing.T) {
	steps := []struct {
		service string
		levels  map[string]float64
	}{
		{"pricing-svc", map[string]float64{"info": 20, "error": 5, "warn": 5}},
		{"batch-etl", map[string]float64{"warn": 25, "error": 1, "info": 1}},
	}
	for _, st := range steps {
		t.Run(st.service, func(t *testing.T) {
			var vec []topKSeries
			for level, v := range st.levels {
				vec = append(vec, topKSeries{app: st.service, env: level, value: v})
			}
			sort.Slice(vec, func(i, j int) bool { return vec[i].env < vec[j].env })
			group := func(s topKSeries) string { return s.app }
			pick := func(in []topKSeries) (string, string) {
				var names []string
				var values []float64
				for _, s := range in {
					names = append(names, s.env)
					values = append(values, s.value)
				}
				sort.Strings(names)
				sort.Float64s(values)
				return fmt.Sprint(names), fmt.Sprint(values)
			}

			// Loki: every insertion order of the vector, as the map iteration yields them.
			winners, values := map[string]bool{}, map[string]bool{}
			rng := rand.New(rand.NewSource(1))
			for i := 0; i < 200; i++ {
				shuffled := append([]topKSeries(nil), vec...)
				rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
				w, v := pick(lokiTopK(shuffled, 2, true, group))
				winners[w], values[v] = true, true
			}
			if len(winners) < 2 {
				t.Fatalf("Loki's heap picked %v in every order, want the tie to change the winner", winners)
			}
			if len(values) != 1 {
				t.Fatalf("Loki's ranked values differ between orders: %v", values)
			}

			// The proxy: the same answer whatever order the series arrive in.
			var result []map[string]any
			for _, s := range vec {
				result = append(result, map[string]any{
					"metric": map[string]string{"service_name": s.app, "level": s.env},
					"values": [][]any{{100.0, fmt.Sprint(s.value)}},
				})
			}
			proxyWinners, proxyValues := map[string]bool{}, map[string]bool{}
			for i := 0; i < 50; i++ {
				rng.Shuffle(len(result), func(a, b int) { result[a], result[b] = result[b], result[a] })
				body, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
				if err != nil {
					t.Fatal(err)
				}
				var resp struct {
					Data struct {
						Result []struct {
							Metric map[string]string
							Values [][]any
						}
					}
				}
				if err := json.Unmarshal(applyTopKToMatrix(body, 2, true, &logqlpkg.Grouping{Labels: []string{"service_name"}}), &resp); err != nil {
					t.Fatal(err)
				}
				var got []topKSeries
				for _, s := range resp.Data.Result {
					v, err := strconv.ParseFloat(s.Values[0][1].(string), 64)
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, topKSeries{app: s.Metric["service_name"], env: s.Metric["level"], value: v})
				}
				w, v := pick(got)
				proxyWinners[w], proxyValues[v] = true, true
			}
			if len(proxyWinners) != 1 {
				t.Fatalf("the proxy's pick depends on arrival order: %v", proxyWinners)
			}
			for v := range values {
				if !proxyValues[v] || len(proxyValues) != 1 {
					t.Fatalf("ranked values: proxy %v, Loki %v", proxyValues, values)
				}
			}
			t.Logf("%s: Loki picked %v depending on order; the proxy always %v; ranked values %v", st.service, tieKeys(winners), tieKeys(proxyWinners), tieKeys(values))
		})
	}
}

func tieKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
