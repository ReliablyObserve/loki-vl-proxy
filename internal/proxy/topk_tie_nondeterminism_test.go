package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// When several series tie across the k-th place, which of them Loki keeps changes
// from one evaluation to the next, while the ranked values never do: Loki's
// engine, asked the same query over the same data many times (recorded with
// test/oracle/loki engine; LVP_RUN_ORACLES=1 records them again), keeps
// different tied series. The proxy breaks the tie on the label key, so it
// answers the same series every time. These two steps are the ones the visual
// stack's seeded data holds at 5s resolution (pricing-svc: error 5, warn 5,
// info 20; batch-etl: warn 25, error 1, info 1).
//
// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, loki_api_v1_query_range
func TestTopKTieAtKthPlaceLokiVariesProxyByLabelKey(t *testing.T) {
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

			// Loki: the same query evaluated again and again.
			winners, values := map[string]bool{}, map[string]bool{}
			for _, got := range topKTieLokiAnswers(t, st.service, vec) {
				w, v := pick(got)
				winners[w], values[v] = true, true
			}
			if len(winners) < 2 {
				t.Fatalf("Loki picked %v in every evaluation, want the tie to change the winner", winners)
			}
			if len(values) != 1 {
				t.Fatalf("Loki's ranked values differ between evaluations: %v", values)
			}

			// The proxy: the same answer whatever order the series arrive in.
			rng := rand.New(rand.NewSource(1))
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

// topKTieEvaluations is how many times Loki evaluates each tied vector.
const topKTieEvaluations = 40

// topKTieLokiAnswers are Loki's recorded answers to topk by (app) (2, ...) over vec, evaluated
// topKTieEvaluations times, each as the series it kept.
func topKTieLokiAnswers(t *testing.T, name string, vec []topKSeries) [][]topKSeries {
	t.Helper()
	const at = 1700000100
	set := oraclerun.EngineDataSet{}
	for _, s := range vec {
		set.Streams = append(set.Streams, oraclerun.EngineStream{Labels: map[string]string{"app": s.app, "env": s.env}, Entries: []oraclerun.EngineEntry{{TS: (at - 30) * 1e9, Line: "v=" + strconv.FormatFloat(s.value, 'f', -1, 64)}}})
	}
	query := `topk by (app) (2, sum by (app, env) (last_over_time({app=~".+"} | logfmt | unwrap v [1m])))`
	for i := 0; i < topKTieEvaluations; i++ {
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: name + "/" + strconv.Itoa(i), Instant: true, Form: url.Values{"query": {query}, "time": {strconv.Itoa(at)}}.Encode()})
	}
	answers := lokiEngineAnswers(t, "topk_tie_"+name, []oraclerun.EngineDataSet{set})
	var out [][]topKSeries
	for i := 0; i < topKTieEvaluations; i++ {
		a := answers[name+"/"+strconv.Itoa(i)]
		s, err := decodeLokiSeries(a.Body)
		if a.Status != 200 || err != nil {
			t.Fatalf("Loki answered %d %s %v", a.Status, a.Error, err)
		}
		var kept []topKSeries
		for key, points := range s.Points {
			for _, text := range points {
				v, _ := strconv.ParseFloat(text, 64)
				kept = append(kept, topKSeries{app: s.Labels[key]["app"], env: s.Labels[key]["env"], value: v})
			}
		}
		out = append(out, kept)
	}
	return out
}
