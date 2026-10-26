package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file reproduces the shapes around `without` that the proxy still answers
// differently from Loki v3.7.7. Each case states the answer the proxy gives now
// and the answer Loki gives, computed from the rows with the evaluator of Loki's
// source (pkg/logql/evaluator.go: VectorAggEvaluator.Next for the aggregations,
// LabelReplaceEvaluator.Next for label_replace; not run against a Loki here).
// The test fails when the proxy's answer changes, in either direction: a fix
// moves the answer to Loki's and the case and its registry row are updated with it.
//
// The series are compared by their ns and pod labels (the labels the fixture
// varies); `-` stands for a series without them.

type openShapeAnswer struct {
	code   int
	series map[string]string // ns/pod labels -> values
	raw    string
}

// openShapeSeriesKey is the ns/pod labels of a series.
func openShapeSeriesKey(metric map[string]string) string {
	var parts []string
	for _, k := range []string{"ns", "pod"} {
		if v, ok := metric[k]; ok {
			parts = append(parts, k+"="+v)
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ",")
}

func openShapeFormat(values []float64) string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = strconv.FormatFloat(v, 'g', 6, 64)
	}
	return strings.Join(out, ",")
}

// openShapeRun sends query through the real handler against the emulated
// VictoriaLogs and returns the series by their ns/pod labels.
func openShapeRun(t *testing.T, query string, instant bool) openShapeAnswer {
	t.Helper()
	emu := &vlEmulator{t: t, rows: withoutScopeRows(), field: "amount", version: "v1.52.0"}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	return openShapeRunAgainst(t, backend.URL, query, instant)
}

func openShapeRunAgainst(t *testing.T, backendURL, query string, instant bool) openShapeAnswer {
	t.Helper()
	p, err := New(Config{BackendURL: backendURL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := int64(1_800_000_000)
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if instant {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(base, 10))
	} else {
		params.Set("start", strconv.FormatInt(base-120, 10))
		params.Set("end", strconv.FormatInt(base, 10))
		params.Set("step", "60")
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	out := openShapeAnswer{code: res.Code, series: map[string]string{}, raw: res.Body.String()}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v: %s", query, err, res.Body)
	}
	for _, s := range resp.Data.Result {
		points := s.Values
		if s.Value != nil {
			points = [][]any{s.Value}
		}
		var values []float64
		for _, pt := range points {
			v, _ := strconv.ParseFloat(pt[1].(string), 64)
			values = append(values, v)
		}
		out.series[openShapeSeriesKey(s.Metric)] = openShapeFormat(values)
	}
	return out
}

// openShapeStreams is the value of each stream of the fixture at the evaluation
// times: fn over the window ending at each time, as Loki evaluates it per series.
func openShapeStreams(fn string, window time.Duration, instant bool) map[string][]float64 {
	base := int64(1_800_000_000) * 1e9
	times := []int64{base - 120e9, base - 60e9, base}
	if instant {
		times = []int64{base}
	}
	type sample struct {
		ts    int64
		value float64
	}
	byStream := map[string][]sample{}
	for _, row := range withoutScopeRows() {
		var labels = map[string]string{}
		for _, m := range emuStreamLabelRE.FindAllStringSubmatch(row.stream, -1) {
			labels[m[1]] = m[2]
		}
		key := openShapeSeriesKey(labels)
		byStream[key] = append(byStream[key], sample{row.ts, row.value})
	}
	out := map[string][]float64{}
	for key, samples := range byStream {
		for _, ts := range times {
			var sum, count, maxV float64
			maxV = math.Inf(-1)
			for _, s := range samples {
				if s.ts > ts-int64(window) && s.ts <= ts {
					sum += s.value
					count++
					maxV = math.Max(maxV, s.value)
				}
			}
			if count == 0 {
				continue
			}
			switch fn {
			case "rate":
				out[key] = append(out[key], sum/window.Seconds())
			case "count_over_time":
				out[key] = append(out[key], count)
			case "max_over_time":
				out[key] = append(out[key], maxV)
			default:
				panic(fn)
			}
		}
	}
	return out
}

// openShapeGroup folds the streams that share the ns label with op over the
// evaluation times (dropping pod, as a `without (pod)` does), keeping only the
// streams keep accepts.
func openShapeGroup(streams map[string][]float64, op string, keep func(v float64) bool, byNS bool) map[string]string {
	groups := map[string]map[int]float64{}
	for key, values := range streams {
		group := "-"
		if byNS {
			group = strings.SplitN(key, ",", 2)[0]
		}
		for i, v := range values {
			if keep != nil && !keep(v) {
				continue
			}
			if groups[group] == nil {
				groups[group] = map[int]float64{}
			}
			cur, ok := groups[group][i]
			switch {
			case !ok:
				groups[group][i] = v
			case op == "sum":
				groups[group][i] = cur + v
			case op == "max":
				groups[group][i] = math.Max(cur, v)
			}
		}
	}
	out := map[string]string{}
	for group, byStep := range groups {
		steps := make([]int, 0, len(byStep))
		for i := range byStep {
			steps = append(steps, i)
		}
		sort.Ints(steps)
		values := make([]float64, len(steps))
		for n, i := range steps {
			values[n] = byStep[i]
		}
		out[group] = openShapeFormat(values)
	}
	return out
}

// openShapeTopK is topk or bottomk (1) per ns group: the series itself, with all
// its labels, that holds the group's top or bottom value at each time.
func openShapeTopK(streams map[string][]float64, top bool) map[string]string {
	best := map[string]map[int]struct {
		key string
		v   float64
	}{}
	for key, values := range streams {
		ns := strings.SplitN(key, ",", 2)[0]
		for i, v := range values {
			if best[ns] == nil {
				best[ns] = map[int]struct {
					key string
					v   float64
				}{}
			}
			cur, ok := best[ns][i]
			if !ok || (top && v > cur.v) || (!top && v < cur.v) {
				best[ns][i] = struct {
					key string
					v   float64
				}{key, v}
			}
		}
	}
	perSeries := map[string][]float64{}
	for _, byStep := range best {
		steps := make([]int, 0, len(byStep))
		for i := range byStep {
			steps = append(steps, i)
		}
		sort.Ints(steps)
		for _, i := range steps {
			perSeries[byStep[i].key] = append(perSeries[byStep[i].key], byStep[i].v)
		}
	}
	out := map[string]string{}
	for key, values := range perSeries {
		out[key] = openShapeFormat(values)
	}
	return out
}

func openShapeCanon(series map[string]string) string {
	keys := make([]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = "{" + k + "}=" + series[k]
	}
	return strings.Join(parts, " ")
}

func openShapeKeys(series map[string]string) map[string]string {
	out := map[string]string{}
	for k := range series {
		out[k] = ""
	}
	return out
}

// TestWithoutShapesStillDifferFromLoki: the registered shapes, with the answer
// the proxy gives and Loki's. See the rows semantics/without-* and
// semantics/label-replace-instant-vector. topk and bottomk without moved to
// TestWithoutTopKBottomKKeepsEveryLabelLikeLoki when
// semantics/topk-bottomk-without-strips-label was fixed.
//
// conformance: semantics/without-empty-grouping-range-collapses, metric-series-identity
func TestWithoutShapesStillDifferFromLoki(t *testing.T) {
	const unwrapRate = `rate({app="uw"} | unwrap amount [5m])`
	rate := func(instant bool) map[string][]float64 { return openShapeStreams("rate", 5*time.Minute, instant) }
	for _, tc := range []struct {
		row     string
		query   string
		instant bool
		// onlySeries compares the label sets only: the values of a range query
		// over an over-time function are an open row of their own
		// (semantics/unwrap-over-time-range-windows).
		onlySeries bool
		got        string // the proxy's answer now
		loki       func(instant bool) map[string]string
	}{
		// sum without () (max_over_time ...) on query_range: `without ()` drops no
		// label (Grouping.Noop: pkg/logql/syntax/ast.go), so every series stays
		// (the proxy folds them into one); at an instant the proxy keeps them.
		{"without-empty-grouping-range-collapses", `sum without () (max_over_time({app="uw"} | unwrap amount [5m]))`, false, true,
			`{-}=`, func(bool) map[string]string {
				return openShapeKeys(toSeries(openShapeStreams("max_over_time", 5*time.Minute, false)))
			}},
	} {
		name := fmt.Sprintf("%s/%s/instant=%v", tc.row, tc.query, tc.instant)
		t.Run(name, func(t *testing.T) {
			got := openShapeRun(t, tc.query, tc.instant)
			if got.code != http.StatusOK {
				t.Fatalf("%d %s", got.code, got.raw)
			}
			want := tc.loki(tc.instant)
			gotCanon, wantCanon := openShapeCanon(got.series), openShapeCanon(want)
			if tc.onlySeries {
				gotCanon, wantCanon = openShapeCanon(openShapeKeys(got.series)), openShapeCanon(want)
			}
			t.Logf("proxy %s | Loki %s", gotCanon, wantCanon)
			if gotCanon == wantCanon {
				t.Fatalf("the proxy now answers Loki's %s: update the case and the registry row %s", wantCanon, tc.row)
			}
			if gotCanon != tc.got {
				t.Fatalf("the proxy answered %s, the registry row %s states %s\nLoki: %s", gotCanon, tc.row, tc.got, wantCanon)
			}
		})
	}
}

// TestWithoutTopKBottomKKeepsEveryLabelLikeLoki: topk and bottomk without keep,
// per group of the labels other than the excluded ones, the series with the top
// (bottom) value at each step, with all its labels, pod included
// (VectorAggEvaluator.Next keeps the sample's metric: pkg/logql/evaluator.go).
// The proxy stripped pod and folded the rows by sum before
// (semantics/topk-bottomk-without-strips-label). Instant and query_range, topk
// and bottomk, in the leading and the trailing spelling of the clause.
//
// conformance: semantics/topk-bottomk-without-strips-label, semantics/topk-grouped-spellings, metric-series-identity
func TestWithoutTopKBottomKKeepsEveryLabelLikeLoki(t *testing.T) {
	const unwrapRate = `rate({app="uw"} | unwrap amount [5m])`
	rate := func(instant bool) map[string][]float64 { return openShapeStreams("rate", 5*time.Minute, instant) }
	for _, tc := range []struct {
		op    string
		top   bool
		query string
	}{
		{"topk", true, `topk without (pod) (1, ` + unwrapRate + `)`},
		{"topk", true, `topk(1, ` + unwrapRate + `) without (pod)`},
		{"bottomk", false, `bottomk without (pod) (1, ` + unwrapRate + `)`},
		{"bottomk", false, `bottomk(1, ` + unwrapRate + `) without (pod)`},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.query, instant), func(t *testing.T) {
				got := openShapeRun(t, tc.query, instant)
				if got.code != http.StatusOK {
					t.Fatalf("%d %s", got.code, got.raw)
				}
				want := openShapeTopK(rate(instant), tc.top)
				if len(want) == 0 {
					t.Fatal("the reference holds no series")
				}
				if gotCanon, wantCanon := openShapeCanon(got.series), openShapeCanon(want); gotCanon != wantCanon {
					t.Fatalf("proxy %s, Loki %s", gotCanon, wantCanon)
				}
			})
		}
	}
}

// TestWithoutNestedAggregationMatchesLoki: a `without` over an inner aggregation,
// and a `without` under an outer one, are folded level by level, innermost first,
// as Loki's VectorAggEvaluator does (pkg/logql/evaluator.go:435-529), on an
// instant query and on a sliding range. The answers were one series {} (the
// whole inner vector summed) before
// (semantics/without-over-inner-aggregation-collapses,
// semantics/without-nested-under-aggregation).
//
// conformance: semantics/without-over-inner-aggregation-collapses, semantics/without-nested-under-aggregation, semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
func TestWithoutNestedAggregationMatchesLoki(t *testing.T) {
	const unwrapRate = `rate({app="uw"} | unwrap amount [5m])`
	rate := func(instant bool) map[string][]float64 { return openShapeStreams("rate", 5*time.Minute, instant) }
	count := func(instant bool) map[string][]float64 {
		return openShapeStreams("count_over_time", time.Minute, instant)
	}
	for _, tc := range []struct {
		query string
		loki  func(instant bool) map[string]string
	}{
		{`sum without (pod) (sum by (pod, ns) (` + unwrapRate + `))`, func(i bool) map[string]string { return openShapeGroup(rate(i), "sum", nil, true) }},
		{`sum without (pod) (sum by (pod, ns) (count_over_time({app="uw"}[1m])))`, func(i bool) map[string]string { return openShapeGroup(count(i), "sum", nil, true) }},
		{`max(sum without (pod) (` + unwrapRate + `))`, func(i bool) map[string]string {
			return openShapeGroup(toStreams(openShapeGroup(rate(i), "sum", nil, true)), "max", nil, false)
		}},
		{`sum by (ns) (sum without (pod) (` + unwrapRate + `))`, func(i bool) map[string]string { return openShapeGroup(rate(i), "sum", nil, true) }},
		{`max(sum without (pod) (count_over_time({app="uw"}[5m])))`, func(i bool) map[string]string {
			return openShapeGroup(toStreams(openShapeGroup(openShapeStreams("count_over_time", 5*time.Minute, i), "sum", nil, true)), "max", nil, false)
		}},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.query, instant), func(t *testing.T) {
				got := openShapeRun(t, tc.query, instant)
				if got.code != http.StatusOK {
					t.Fatalf("%d %s", got.code, got.raw)
				}
				if gotCanon, wantCanon := openShapeCanon(got.series), openShapeCanon(tc.loki(instant)); gotCanon != wantCanon {
					t.Fatalf("proxy %s, Loki %s", gotCanon, wantCanon)
				}
			})
		}
	}
}

// TestWithoutComparisonInsideMatchesLoki: a comparison inside a `sum without`
// filters the series of the inner vector before the sum, as Loki evaluates the
// comparison (vectorBinop) and then the aggregation (VectorAggEvaluator.Next,
// pkg/logql/evaluator.go:451-466): the aggregation over a binary sends it to the
// binary evaluator and folds the filtered series. Range and instant for the
// rate; instant for max_over_time (its range values are another row:
// semantics/unwrap-over-time-range-windows).
//
// conformance: semantics/without-comparison-inside-ignored, metric-series-identity
func TestWithoutComparisonInsideMatchesLoki(t *testing.T) {
	const unwrapRate = `rate({app="uw"} | unwrap amount [5m])`
	for _, tc := range []struct {
		query   string
		instant bool
		loki    func(instant bool) map[string]string
	}{
		{`sum without (pod) (` + unwrapRate + ` > 0.33)`, false, func(i bool) map[string]string {
			return openShapeGroup(openShapeStreams("rate", 5*time.Minute, i), "sum", func(v float64) bool { return v > 0.33 }, true)
		}},
		{`sum without (pod) (` + unwrapRate + ` > 0.33)`, true, func(i bool) map[string]string {
			return openShapeGroup(openShapeStreams("rate", 5*time.Minute, i), "sum", func(v float64) bool { return v > 0.33 }, true)
		}},
		{`sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m]) > 35)`, true, func(i bool) map[string]string {
			return openShapeGroup(openShapeStreams("max_over_time", 5*time.Minute, i), "sum", func(v float64) bool { return v > 35 }, true)
		}},
	} {
		t.Run(fmt.Sprintf("%s/instant=%v", tc.query, tc.instant), func(t *testing.T) {
			got := openShapeRun(t, tc.query, tc.instant)
			if got.code != http.StatusOK {
				t.Fatalf("%d %s", got.code, got.raw)
			}
			gotCanon, wantCanon := openShapeCanon(got.series), openShapeCanon(tc.loki(tc.instant))
			if gotCanon != wantCanon {
				t.Fatalf("proxy %s, Loki %s", gotCanon, wantCanon)
			}
		})
	}
}

// TestWithoutOverInnerAggregationRateRangeMatchesLoki: on query_range the rate
// of an unwrapped field under `sum without` over an inner `sum by` is Loki's
// answer, one series per namespace (the nested aggregation is read down to the
// rate: semantics/non-sum-vector-aggregation-unreduced). The instant query and the
// count_over_time cases of the row are Loki's too (TestWithoutNestedAggregationMatchesLoki).
//
// conformance: semantics/without-over-inner-aggregation-collapses, metric-series-identity
func TestWithoutOverInnerAggregationRateRangeMatchesLoki(t *testing.T) {
	got := openShapeRun(t, `sum without (pod) (sum by (pod, ns) (rate({app="uw"} | unwrap amount [5m])))`, false)
	if got.code != http.StatusOK {
		t.Fatalf("%d %s", got.code, got.raw)
	}
	want := openShapeGroup(openShapeStreams("rate", 5*time.Minute, false), "sum", nil, true)
	if gotCanon, wantCanon := openShapeCanon(got.series), openShapeCanon(want); gotCanon != wantCanon {
		t.Fatalf("proxy %s, Loki %s", gotCanon, wantCanon)
	}
}

// toSeries lists the series of a stream map as a label-set -> values map.
func toSeries(streams map[string][]float64) map[string]string {
	out := map[string]string{}
	for k, v := range streams {
		out[k] = openShapeFormat(v)
	}
	return out
}

// toStreams is the reverse of openShapeFormat for a grouped answer: the folded
// series of each ns as a stream, to aggregate again.
func toStreams(series map[string]string) map[string][]float64 {
	out := map[string][]float64{}
	for k, text := range series {
		for _, f := range strings.Split(text, ",") {
			v, _ := strconv.ParseFloat(f, 64)
			out[k] = append(out[k], v)
		}
	}
	return out
}

// TestLabelReplaceOnAVectorAddsTheLabelAtAnInstant: label_replace around a
// vector aggregation sets the destination label on every series whose source
// label matches, on an instant query as on query_range. Loki evaluates the
// call per step on the vector its operand answers
// (pkg/logql/evaluator.go:387, LabelReplaceEvaluator.Next at :1279-1309).
//
// conformance: semantics/label-replace-instant-vector
func TestLabelReplaceOnAVectorAddsTheLabelAtAnInstant(t *testing.T) {
	const query = `label_replace(sum by (ns) (max_over_time({app="uw"} | unwrap amount [5m])), "dst", "$1", "ns", "(.*)")`
	for _, instant := range []bool{false, true} {
		t.Run(fmt.Sprintf("instant=%v", instant), func(t *testing.T) {
			emu := &vlEmulator{t: t, rows: withoutScopeRows(), field: "amount", version: "v1.52.0"}
			backend := httptest.NewServer(emu.handler())
			defer backend.Close()
			got := openShapeRunAgainst(t, backend.URL, query, instant)
			if got.code != http.StatusOK {
				t.Fatalf("%d %s", got.code, got.raw)
			}
			if !strings.Contains(got.raw, `"dst":"x"`) || !strings.Contains(got.raw, `"dst":"y"`) {
				t.Fatalf("instant=%v: want dst=x and dst=y on the series, as Loki answers\n%s", instant, got.raw)
			}
		})
	}
}

// TestLabelReplaceOfABinaryExpressionRoutesByTheBinary: a label_replace around
// a binary expression evaluates the binary, then replaces the label, on
// query_range as on an instant query (pkg/logql/evaluator.go:385-388; the
// transform is LabelReplaceEvaluator.Next at :1279-1309 over the binary's
// vector). Each ns divided by itself is 1 at every step, with dst equal to ns.
// The translator's binary marker never reaches VictoriaLogs as a query.
//
// conformance: semantics/label-replace-range-binary-marker
func TestLabelReplaceOfABinaryExpressionRoutesByTheBinary(t *testing.T) {
	const query = `label_replace(sum by (ns) (rate({app="uw"}[5m])) / sum by (ns) (rate({app="uw"}[5m])), "dst", "$1", "ns", "(.*)")`
	for _, instant := range []bool{false, true} {
		t.Run(fmt.Sprintf("instant=%v", instant), func(t *testing.T) {
			emu := &vlEmulator{t: t, rows: withoutScopeRows(), field: "amount", version: "v1.52.0"}
			backend := httptest.NewServer(emu.handler())
			defer backend.Close()
			got := openShapeRunAgainst(t, backend.URL, query, instant)
			if got.code != http.StatusOK {
				t.Fatalf("%d %s", got.code, got.raw)
			}
			emu.mu.Lock()
			for _, c := range emu.calls {
				if strings.Contains(c.query, "__binary__:") {
					t.Errorf("the binary marker reached VictoriaLogs: %s %s", c.path, c.query)
				}
			}
			emu.mu.Unlock()
			if len(got.series) != 2 || got.series["ns=x"] == "" || got.series["ns=y"] == "" {
				t.Fatalf("want the series ns=x and ns=y, as Loki answers: %v\n%s", got.series, got.raw)
			}
			for key, values := range got.series {
				for _, v := range strings.Split(values, ",") {
					if v != "1" {
						t.Fatalf("%s: values %s, want 1 at every point (each ns divided by itself)\n%s", key, values, got.raw)
					}
				}
			}
			if !strings.Contains(got.raw, `"dst":"x"`) || !strings.Contains(got.raw, `"dst":"y"`) {
				t.Fatalf("want dst=x and dst=y on the series, as Loki answers\n%s", got.raw)
			}
		})
	}
}

// TestWithoutComparisonAboveEveryValueDropsEverySeries: a comparison around a
// folded without drops the series that fail it, on query_range too, whatever
// the range function. The threshold is above every value the fixture holds, so
// the answer is empty on Loki and on the proxy whatever window the function
// reads (the over-time functions of a query_range read step-sized buckets:
// semantics/unwrap-over-time-range-windows).
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutComparisonAboveEveryValueDropsEverySeries(t *testing.T) {
	for _, fn := range []string{"max_over_time", "min_over_time", "sum_over_time", "rate"} {
		for _, op := range []string{"sum", "max", "min"} {
			for _, instant := range []bool{true, false} {
				query := fmt.Sprintf(`%s without (pod) (%s({app="uw"} | unwrap amount [5m])) > 1000000`, op, fn)
				t.Run(fmt.Sprintf("%s/instant=%v", query, instant), func(t *testing.T) {
					got := openShapeRun(t, query, instant)
					if got.code != http.StatusOK || len(got.series) != 0 {
						t.Fatalf("%d with series %s, want 200 with none: %s", got.code, openShapeCanon(got.series), got.raw)
					}
				})
			}
		}
	}
}
