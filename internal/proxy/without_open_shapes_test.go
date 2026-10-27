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
	"sync"
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
// semantics/label-replace-instant-vector.
//
// conformance: semantics/without-empty-grouping-range-collapses, semantics/without-comparison-inside-ignored, semantics/topk-bottomk-without-strips-label, metric-series-identity
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
		// a comparison inside the without: the filter is not applied before the sum.
		{"without-comparison-inside-ignored", `sum without (pod) (` + unwrapRate + ` > 0.33)`, false, false,
			`{ns=x}=0.813333,1.06,1.23333 {ns=y}=0.92,1.41333,1.5`, func(i bool) map[string]string {
				return openShapeGroup(rate(i), "sum", func(v float64) bool { return v > 0.33 }, true)
			}},
		{"without-comparison-inside-ignored", `sum without (pod) (` + unwrapRate + ` > 0.33)`, true, false,
			`{ns=x}=1.23333 {ns=y}=1.5`, func(i bool) map[string]string {
				return openShapeGroup(rate(i), "sum", func(v float64) bool { return v > 0.33 }, true)
			}},
		{"without-comparison-inside-ignored", `sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m]) > 35)`, true, false,
			`{ns=x}=138 {ns=y}=154`, func(i bool) map[string]string {
				return openShapeGroup(openShapeStreams("max_over_time", 5*time.Minute, i), "sum", func(v float64) bool { return v > 35 }, true)
			}},
		// topk and bottomk without: Loki keeps the series with all their labels.
		{"topk-bottomk-without-strips-label", `topk without (pod) (1, ` + unwrapRate + `)`, false, false,
			`{ns=x}=0.0533333 {ns=y}=0.133333`, func(i bool) map[string]string { return openShapeTopK(rate(i), true) }},
		{"topk-bottomk-without-strips-label", `topk without (pod) (1, ` + unwrapRate + `)`, true, false,
			`{ns=y}=0.4`, func(i bool) map[string]string { return openShapeTopK(rate(i), true) }},
		{"topk-bottomk-without-strips-label", `bottomk without (pod) (1, ` + unwrapRate + `)`, false, false,
			`{ns=x}=0.0566667 {ns=y}=0.0166667`, func(i bool) map[string]string { return openShapeTopK(rate(i), false) }},
		{"topk-bottomk-without-strips-label", `bottomk without (pod) (1, ` + unwrapRate + `)`, true, false,
			`{ns=x}=0.283333`, func(i bool) map[string]string { return openShapeTopK(rate(i), false) }},
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

// TestInstantLabelReplaceOnAVectorAddsNoLabel: label_replace around a vector
// aggregation at an instant keeps the series as they are, where Loki adds the
// destination label (LabelReplaceEvaluator.Next sets dst on every sample whose
// source label matches). query_range adds it.
//
// conformance: semantics/label-replace-instant-vector
func TestInstantLabelReplaceOnAVectorAddsNoLabel(t *testing.T) {
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
			hasDst := strings.Contains(got.raw, `"dst":"x"`) && strings.Contains(got.raw, `"dst":"y"`)
			if hasDst == instant {
				t.Fatalf("instant=%v: dst on the series is %v, want %v (instant adds none until the row semantics/label-replace-instant-vector is fixed)\n%s", instant, hasDst, !instant, got.raw)
			}
		})
	}
}

// TestRangeLabelReplaceOfABinaryExpressionSendsTheMarker: query_range does not
// strip the label_replace around a binary expression the way an instant query
// does, so the binary marker of the translation reaches VictoriaLogs as a query
// it cannot parse. Loki evaluates the binary expression and replaces the label.
//
// conformance: semantics/label-replace-range-binary-marker
func TestRangeLabelReplaceOfABinaryExpressionSendsTheMarker(t *testing.T) {
	const query = `label_replace(sum by (ns) (rate({app="uw"}[5m])) / sum by (ns) (rate({app="uw"}[5m])), "dst", "$1", "ns", "(.*)")`
	var (
		mu      sync.Mutex
		queries []string
	)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		queries = append(queries, r.Form.Get("query"))
		mu.Unlock()
		if strings.HasPrefix(r.Form.Get("query"), "__binary__:") {
			// VictoriaLogs' answer to a query it cannot parse.
			http.Error(w, `{"status":"error","error":"cannot parse query"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer backend.Close()
	for _, instant := range []bool{false, true} {
		mu.Lock()
		queries = nil
		mu.Unlock()
		got := openShapeRunAgainst(t, backend.URL, query, instant)
		mu.Lock()
		sent := strings.Join(queries, "\n")
		mu.Unlock()
		marker := strings.Contains(sent, "__binary__:")
		if instant && (got.code != http.StatusOK || marker) {
			t.Fatalf("instant: %d, binary marker sent=%v: %s", got.code, marker, got.raw)
		}
		if !instant && (got.code == http.StatusOK || !marker) {
			t.Fatalf("query_range: %d, binary marker sent=%v, want the marker sent to VictoriaLogs and an error (open row semantics/label-replace-range-binary-marker): %s", got.code, marker, got.raw)
		}
		if !instant {
			t.Logf("query_range answers %d %s", got.code, strings.TrimSpace(got.raw))
		}
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
