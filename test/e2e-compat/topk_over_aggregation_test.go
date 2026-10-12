//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// topKFoldTieSteps returns the steps at which the inner vector holds a tie
// among its best kMax+1 values, overall or inside one app. Loki keeps its
// window in a map, so which of the tied series a ranking keeps differs from
// one evaluation to the next, and the layers above it can then differ too;
// the comparison skips such steps.
func topKFoldTieSteps(inner map[string]map[string]string, kMax int, descending bool) map[string]bool {
	byStep := map[string]map[string][]float64{} // step -> app ("" overall) -> values
	for key, points := range inner {
		app := topKLabel(key, "app")
		for ts, raw := range points {
			v, _ := strconv.ParseFloat(raw, 64)
			if byStep[ts] == nil {
				byStep[ts] = map[string][]float64{}
			}
			byStep[ts][""] = append(byStep[ts][""], v)
			byStep[ts][app] = append(byStep[ts][app], v)
		}
	}
	tied := map[string]bool{}
	for ts, groups := range byStep {
		for _, values := range groups {
			sort.Float64s(values)
			if descending {
				sort.Sort(sort.Reverse(sort.Float64Slice(values)))
			}
			for i := 0; i < len(values)-1 && i < kMax; i++ {
				if values[i] == values[i+1] {
					tied[ts] = true
				}
			}
		}
	}
	return tied
}

// TestCompat_AggregationOverTopK: an aggregation written around a topk,
// bottomk or sort folds what the ranking kept at each step. `sum(topk(3, x))`
// is the sum of the three best samples of every step, `topk(2, topk(3, x))` the
// best two of the best three. The proxy took these for log queries (a `streams`
// answer on query_range, a 400 at an instant) or answered one empty series.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_AggregationOverTopK(t *testing.T) {
	id := time.Now().UnixNano()
	s0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	// Lines sit at :07 of each minute, so with a window edge at :30 every window
	// holds five whole minutes of every pod.
	const pods, minutes = 6, 125
	apps := []string{fmt.Sprintf("topk-fold-%d-a", id), fmt.Sprintf("topk-fold-%d-b", id)}
	start, end := s0.Add(10*time.Minute+30*time.Second), s0.Add(110*time.Minute+30*time.Second)
	for a, app := range apps {
		var rows strings.Builder
		streams := map[string][][]any{}
		total := 0
		for p := 0; p < pods; p++ {
			pod := fmt.Sprintf("pod-%d", p)
			for m := 0; m < minutes; m++ {
				// Every pod has a rate of its own, and bursts every third minute
				// at a phase of its own, so the leaders change between steps.
				n := 1 + 3*p + a + 20*b2i((m+p+a)%3 == 0)
				for i := 0; i < n; i++ {
					ts := s0.Add(time.Duration(m)*time.Minute + 7*time.Second + time.Duration(i)*time.Millisecond)
					msg := fmt.Sprintf("topk fold %s %d", pod, i)
					rows.WriteString(fmt.Sprintf(`{"_time":%q,"_msg":%q,"app":%q,"pod":%q}`+"\n", ts.UTC().Format(time.RFC3339Nano), msg, app, pod))
					streams[pod] = append(streams[pod], []any{strconv.FormatInt(ts.UnixNano(), 10), msg})
					total++
				}
			}
		}
		ingestHeavyRankFixture(t, app, rows.String(), streams, total, s0, s0.Add(minutes*time.Minute))
	}
	selector := fmt.Sprintf(`{app=~"topk-fold-%d-.+"}`, id)
	innerQuery := `sum by (app, pod) (count_over_time(` + selector + `[5m]))`
	step := time.Minute
	inner := slidingRangeSeries(t, lokiURL, innerQuery, start, end, step, nil)

	for _, tc := range []struct {
		name       string
		query      string
		kMax       int
		descending bool
	}{
		{"sum of topk", `sum(topk(3, ` + innerQuery + `))`, 3, true},
		{"sum by app of topk", `sum by (app) (topk(3, ` + innerQuery + `))`, 3, true},
		{"trailing by", `sum(topk(3, ` + innerQuery + `)) by (app)`, 3, true},
		{"avg of bottomk", `avg(bottomk(4, ` + innerQuery + `))`, 4, false},
		{"max of topk", `max(topk(2, ` + innerQuery + `))`, 2, true},
		{"min by app of topk", `min by (app) (topk(5, ` + innerQuery + `))`, 5, true},
		{"count by app of topk", `count by (app) (topk(4, ` + innerQuery + `))`, 4, true},
		{"sum without pod of bottomk", `sum without (pod) (bottomk(3, ` + innerQuery + `))`, 3, false},
		{"sum of topk by app", `sum(topk by (app) (2, ` + innerQuery + `))`, 2, true},
		{"topk of topk", `topk(2, topk(4, ` + innerQuery + `))`, 4, true},
		{"sum by app of topk of sort_desc", `sum by (app) (topk(3, sort_desc(` + innerQuery + `)))`, 3, true},
		{"sum of sort", `sum(sort(` + innerQuery + `))`, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, step, nil)
			proxy := slidingRangeSeries(t, proxyURL, tc.query, start, end, step, nil)
			tied := topKFoldTieSteps(inner, tc.kMax, tc.descending)
			compared := 0
			steps := map[string]bool{}
			for _, points := range loki {
				for ts := range points {
					steps[ts] = true
				}
			}
			for ts := range steps {
				if tied[ts] {
					continue
				}
				compared++
				for key, points := range loki {
					want, ok := points[ts]
					if !ok {
						continue
					}
					if got, have := proxy[key][ts]; !have || !sameSampleValue(got, want) {
						t.Errorf("%s {%s} t=%s: proxy %q (present=%v), Loki %q", tc.query, key, ts, got, have, want)
					}
				}
				for key, points := range proxy {
					if _, ok := points[ts]; ok {
						if _, want := loki[key][ts]; !want {
							t.Errorf("%s {%s} t=%s: proxy has a sample Loki does not", tc.query, key, ts)
						}
					}
				}
			}
			if compared < len(steps)/4 || len(steps) == 0 {
				t.Fatalf("%s: only %d of %d steps could be compared; the fixture ties too often", tc.query, compared, len(steps))
			}
		})
	}

	t.Run("instant", func(t *testing.T) {
		at := end
		for _, query := range []string{
			`sum(topk(3, ` + innerQuery + `))`,
			`sum by (app) (bottomk(2, ` + innerQuery + `))`,
			`count(topk(4, ` + innerQuery + `))`,
			`topk(1, topk(3, ` + innerQuery + `))`,
		} {
			fetch := func(base string) map[string]string {
				params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
				status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				if status != 200 || resp["status"] != "success" || extractMap(resp, "data")["resultType"] != "vector" {
					t.Fatalf("%s %s: %d %s", base, query, status, body)
				}
				out := map[string]string{}
				for _, item := range extractArray(extractMap(resp, "data"), "result") {
					series, _ := item.(map[string]interface{})
					metric, _ := series["metric"].(map[string]interface{})
					value, _ := series["value"].([]interface{})
					keys := make([]string, 0, len(metric))
					for k, v := range metric {
						keys = append(keys, fmt.Sprintf("%s=%v", k, v))
					}
					sort.Strings(keys)
					out[strings.Join(keys, ",")] = fmt.Sprint(value[1])
				}
				return out
			}
			loki, proxy := fetch(lokiURL), fetch(proxyURL)
			if len(loki) == 0 {
				t.Fatalf("%s: Loki returned no sample", query)
			}
			if strings.HasPrefix(query, "topk(1,") {
				// A tie for the first place is broken by Loki's map order: the
				// value must match, the series may not.
				if fmt.Sprint(valuesOf(loki)) != fmt.Sprint(valuesOf(proxy)) {
					t.Errorf("%s: proxy %v, Loki %v", query, proxy, loki)
				}
				continue
			}
			if len(loki) != len(proxy) {
				t.Errorf("%s: proxy %v, Loki %v", query, proxy, loki)
				continue
			}
			for key, want := range loki {
				if got, ok := proxy[key]; !ok || !sameSampleValue(got, want) {
					t.Errorf("%s {%s}: proxy %q, Loki %q", query, key, got, want)
				}
			}
		}
	})
}

func valuesOf(series map[string]string) []string {
	out := make([]string, 0, len(series))
	for _, v := range series {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// sameSampleValue compares two formatted sample values, allowing the last digit
// an average can differ in.
func sameSampleValue(a, b string) bool {
	x, errA := strconv.ParseFloat(a, 64)
	y, errB := strconv.ParseFloat(b, 64)
	if errA != nil || errB != nil {
		return a == b
	}
	diff := x - y
	if diff < 0 {
		diff = -diff
	}
	scale := y
	if scale < 0 {
		scale = -scale
	}
	if scale < 1 {
		scale = 1
	}
	return diff <= 1e-9*scale
}
