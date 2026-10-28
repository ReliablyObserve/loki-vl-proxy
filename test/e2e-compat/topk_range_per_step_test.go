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

// topKLabel reads one label out of the "k=v,k=v" series key slidingRangeSeries
// builds.
func topKLabel(key, name string) string {
	for _, pair := range strings.Split(key, ",") {
		if k, v, ok := strings.Cut(pair, "="); ok && k == name {
			return v
		}
	}
	return ""
}

// onlyTopKLabels keeps the named labels of each series key, so series that
// differ in other labels only (detected_level, service_name) compare equal.
func onlyTopKLabels(series map[string]map[string]string, names ...string) map[string]map[string]string {
	out := map[string]map[string]string{}
	for key, points := range series {
		parts := make([]string, 0, len(names))
		for _, name := range names {
			if v := topKLabel(key, name); v != "" {
				parts = append(parts, name+"="+v)
			}
		}
		kept := strings.Join(parts, ",")
		if out[kept] == nil {
			out[kept] = map[string]string{}
		}
		for ts, v := range points {
			out[kept][ts] = v
		}
	}
	return out
}

// assertTopKRangeParity compares the proxy with Loki sample by sample. Loki
// breaks a tie at the k-th place by the order of an unordered map, so where the
// inner vector of a group holds one the values selected must match and the
// series may differ; everywhere else the selected series match too.
func assertTopKRangeParity(t *testing.T, query string, inner, loki, proxy map[string]map[string]string, groupLabels []string, k int, descending bool) {
	t.Helper()
	group := func(key string) string {
		parts := make([]string, len(groupLabels))
		for i, name := range groupLabels {
			parts[i] = topKLabel(key, name)
		}
		return strings.Join(parts, "/")
	}
	// values[ts][group] holds the samples of one result at one step.
	collect := func(series map[string]map[string]string) map[string]map[string][]float64 {
		out := map[string]map[string][]float64{}
		for key, points := range series {
			for ts, raw := range points {
				v, _ := strconv.ParseFloat(raw, 64)
				if out[ts] == nil {
					out[ts] = map[string][]float64{}
				}
				out[ts][group(key)] = append(out[ts][group(key)], v)
			}
		}
		for _, groups := range out {
			for _, values := range groups {
				sort.Float64s(values)
			}
		}
		return out
	}
	innerValues, lokiValues, proxyValues := collect(inner), collect(loki), collect(proxy)
	if len(lokiValues) == 0 {
		t.Fatalf("%s: Loki returned no samples", query)
	}
	tied := map[string]bool{}
	for ts, groups := range innerValues {
		for g, values := range groups {
			sorted := append([]float64(nil), values...)
			if descending {
				sort.Sort(sort.Reverse(sort.Float64Slice(sorted)))
			}
			if len(sorted) > k && sorted[k-1] == sorted[k] {
				tied[ts+"|"+g] = true
			}
		}
	}
	for ts, groups := range lokiValues {
		for g, want := range groups {
			if got := proxyValues[ts][g]; fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s t=%s group %q: proxy kept %v, Loki %v", query, ts, g, got, want)
			}
		}
	}
	for ts, groups := range proxyValues {
		for g := range groups {
			if _, ok := lokiValues[ts][g]; !ok {
				t.Errorf("%s t=%s group %q: proxy has samples where Loki has none", query, ts, g)
			}
		}
	}
	for key, want := range loki {
		for ts := range want {
			if tied[ts+"|"+group(key)] {
				continue
			}
			if _, ok := proxy[key][ts]; !ok {
				t.Errorf("%s {%s} t=%s: Loki ranked the series, proxy dropped it", query, key, ts)
			}
		}
	}
	for key, got := range proxy {
		for ts := range got {
			if _, ok := loki[key][ts]; !ok && !tied[ts+"|"+group(key)] {
				t.Errorf("%s {%s} t=%s: proxy ranked a series Loki did not", query, key, ts)
			}
		}
	}
	groups := map[string]bool{}
	for key := range loki {
		groups[group(key)] = true
	}
	if len(loki) <= k*len(groups) {
		t.Fatalf("%s: Loki returned %d series for k=%d in %d groups; the fixture does not move the leaders between steps", query, len(loki), k, len(groups))
	}
}

// TestCompat_TopkRangePerStep: topk and bottomk on query_range rank every step
// on its own, inside each group of the by()/without() labels, in every spelling
// Loki's grammar has (`topk(k, e)`, `topk by (l) (k, e)`, `topk without (l)
// (k, e)`, `topk(k, e) by (l)`). The matrix holds the union of the series that
// ranked at some step, with their samples at those steps only. The leaders of
// the fixture change from minute to minute, so a ranking over the whole range,
// or a spelling the proxy took for a log query, fails here.
// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, limits/top-n-ranking-not-silent, loki_api_v1_query_range
func TestCompat_TopkRangePerStep(t *testing.T) {
	id := time.Now().UnixNano()
	s0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	// Lines sit at :07 of each minute, so with window edges on whole minutes every
	// window holds five whole minutes of every pod. Start and end are multiples of
	// the step: the compose Loki aligns queries to the step
	// (align_queries_with_step), which the proxy does not, like a default Loki.
	const pods, minutes = 6, 125
	apps := []string{fmt.Sprintf("topk-step-%d-a", id), fmt.Sprintf("topk-step-%d-b", id)}
	start, end := s0.Add(10*time.Minute), s0.Add(110*time.Minute)
	for a, app := range apps {
		var rows strings.Builder
		streams := map[string][][]any{}
		total := 0
		for p := 0; p < pods; p++ {
			pod := fmt.Sprintf("pod-%d", p)
			for m := 0; m < minutes; m++ {
				// Pods take turns at the front: each bursts every third minute, at a
				// phase of its own in each app.
				n := 1 + (a+p)%4 + 12*b2i((m+p+a)%3 == 0)
				for i := 0; i < n; i++ {
					ts := s0.Add(time.Duration(m)*time.Minute + 7*time.Second + time.Duration(i)*time.Millisecond)
					msg := fmt.Sprintf("topk %s %d", pod, i)
					rows.WriteString(fmt.Sprintf(`{"_time":%q,"_msg":%q,"app":%q,"pod":%q}`+"\n", ts.UTC().Format(time.RFC3339Nano), msg, app, pod))
					streams[pod] = append(streams[pod], []any{strconv.FormatInt(ts.UnixNano(), 10), msg})
					total++
				}
			}
		}
		ingestHeavyRankFixture(t, app, rows.String(), streams, total, s0, s0.Add(minutes*time.Minute))
	}
	selector := fmt.Sprintf(`{app=~"topk-step-%d-.+"}`, id)
	innerQuery := `sum by (app, pod) (count_over_time(` + selector + `[5m]))`
	step := time.Minute
	inner := slidingRangeSeries(t, lokiURL, innerQuery, start, end, step, nil)
	// A range function ranked as it is: a series per stream, with every label
	// of the stream (pod included) that topk/bottomk without must keep.
	// Only the stream labels are compared: the others (detected_level,
	// service_name) are not what this test is about.
	directQuery := `count_over_time(` + selector + `[5m])`
	direct := onlyTopKLabels(slidingRangeSeries(t, lokiURL, directQuery, start, end, step, nil), "app", "pod")

	for _, tc := range []struct {
		name        string
		query       string
		groupLabels []string
		k           int
		descending  bool
	}{
		{"topk", `topk(3, ` + innerQuery + `)`, nil, 3, true},
		{"bottomk", `bottomk(3, ` + innerQuery + `)`, nil, 3, false},
		{"spaced arguments", `topk (2, ` + innerQuery + `)`, nil, 2, true},
		{"topk by app", `topk by (app) (2, ` + innerQuery + `)`, []string{"app"}, 2, true},
		{"topk trailing by", `topk(2, ` + innerQuery + `) by (app)`, []string{"app"}, 2, true},
		{"bottomk without pod", `bottomk without (pod) (2, ` + innerQuery + `)`, []string{"app"}, 2, false},
		{"topk by pod, groups spanning both apps", `topk by (pod) (1, ` + innerQuery + `)`, []string{"pod"}, 1, true},
		{"topk by app,pod keeps every series", `topk by (app, pod) (1, ` + innerQuery + `)`, []string{"app", "pod"}, 1, true},
		{"k beyond the series count", `topk(50, ` + innerQuery + `)`, nil, 50, true},
		{"topk without pod over a range function", `topk without (pod) (2, ` + directQuery + `)`, []string{"app"}, 2, true},
		{"bottomk without pod over a range function", `bottomk without (pod) (1, ` + directQuery + `)`, []string{"app"}, 1, false},
		{"topk without pod, trailing clause", `topk(2, ` + directQuery + `) without (pod)`, []string{"app"}, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, step, nil)
			proxy := slidingRangeSeries(t, proxyURL, tc.query, start, end, step, nil)
			ranked := inner
			if strings.Contains(tc.query, directQuery) {
				ranked = direct
				for key := range proxy {
					if topKLabel(key, "pod") == "" {
						t.Fatalf("%s: proxy series {%s} lost its pod label", tc.query, key)
					}
				}
				loki, proxy = onlyTopKLabels(loki, "app", "pod"), onlyTopKLabels(proxy, "app", "pod")
			}
			if tc.k >= pods*len(apps) {
				// Nothing is dropped: the answer is the inner vector.
				assertSlidingParity(t, tc.query, loki, proxy)
				return
			}
			if len(tc.groupLabels) == 2 {
				// One series per group: also the inner vector.
				assertSlidingParity(t, tc.query, loki, proxy)
				return
			}
			assertTopKRangeParity(t, tc.query, ranked, loki, proxy, tc.groupLabels, tc.k, tc.descending)
		})
	}

	t.Run("instant topk by app", func(t *testing.T) {
		at := end
		query := `topk by (app) (2, ` + innerQuery + `)`
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
				out[fmt.Sprint(metric["app"])+"/"+fmt.Sprint(metric["pod"])] = fmt.Sprint(value[1])
			}
			return out
		}
		loki, proxy := fetch(lokiURL), fetch(proxyURL)
		perApp := func(series map[string]string) map[string][]string {
			out := map[string][]string{}
			for key, v := range series {
				app, _, _ := strings.Cut(key, "/")
				out[app] = append(out[app], v)
			}
			for _, values := range out {
				sort.Strings(values)
			}
			return out
		}
		if len(loki) != 4 || fmt.Sprint(perApp(loki)) != fmt.Sprint(perApp(proxy)) {
			t.Fatalf("instant %s: proxy %v, Loki %v", query, proxy, loki)
		}
	})
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
