package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Which queries the proxy folds itself: a count, min, max or avg anywhere in a
// chain of vector aggregations over a range aggregation of no grouping of its
// own. Everything else is answered the way it was.
//
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity
func TestSeriesAggregationChain(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  string // the chain, innermost first; "" when the query is not folded
	}{
		{`max(count_over_time({app="a"}[5m]))`, "max"},
		{`min by (pod) (rate({app="a"}[5m]))`, "min by (pod)"},
		{`avg by () (count_over_time({app="a"}[5m]))`, "avg"},
		{`count(bytes_rate({app="a"}[5m]))`, "count"},
		{`sum(max(rate({app="a"}[5m])))`, "max,sum"},
		{`sum by (zone) (max by (pod, zone) (rate({app="a"}[5m])))`, "max by (pod, zone),sum by (zone)"},
		{`max(sum by (pod) (count_over_time({app="a"}[5m])))`, "sum by (pod),max"},
		{`label_replace(max(rate({app="a"}[5m])), "x", "$1", "y", "(.*)")`, "max"},
		{`max(avg_over_time({app="a"} | logfmt | unwrap n [5m]))`, "max"},
		// sum alone is one stats pipe, a without is folded by its marker, topk,
		// stddev and stdvar are not series-level, and a range aggregation with a
		// grouping of its own names the series itself.
		{`sum(count_over_time({app="a"}[5m]))`, ""},
		{`sum by (pod) (rate({app="a"}[5m]))`, ""},
		{`max without (pod) (rate({app="a"}[5m]))`, ""},
		{`sum(max without (pod) (rate({app="a"}[5m])))`, ""},
		{`topk(2, rate({app="a"}[5m]))`, ""},
		{`max(topk(2, rate({app="a"}[5m])))`, ""},
		{`stddev(count_over_time({app="a"}[5m]))`, ""},
		{`max(sum_over_time({app="a"} | logfmt | unwrap n [5m]) by (pod))`, ""},
		{`max(rate({app="a"}[5m])) / max(rate({app="b"}[5m]))`, ""},
		{`rate({app="a"}[5m])`, ""},
		{`max(`, ""},
	} {
		chain, ok := seriesAggregationChain(tc.query)
		got := ""
		if ok {
			var parts []string
			for _, agg := range chain {
				part := string(agg.Op)
				if agg.Grouping != nil && len(agg.Grouping.Labels) > 0 {
					part += " by (" + strings.Join(agg.Grouping.Labels, ", ") + ")"
				}
				parts = append(parts, part)
			}
			got = strings.Join(parts, ",")
		}
		if got != tc.want {
			t.Errorf("%s: chain %q, want %q", tc.query, got, tc.want)
		}
	}
}

// nan builds one series per value, all sampled at 60.
func nan(values []float64) []foldSeries {
	var out []foldSeries
	for i, v := range values {
		out = append(out, foldSeries{map[string]string{"pod": fmt.Sprint("p", i)}, [][2]float64{{60, v}}})
	}
	return out
}

// foldCase is a matrix (or vector) answer of the evaluator and the fold of one
// vector aggregation over it.
type foldCase struct {
	name   string
	op     string
	by     []string // nil: no grouping
	series []foldSeries
	want   []foldSeries
}

type foldSeries struct {
	metric map[string]string
	points [][2]float64
}

func foldBody(series []foldSeries, instant bool) []byte {
	result := make([]map[string]any, 0, len(series))
	for _, s := range series {
		entry := map[string]any{"metric": s.metric}
		var values [][]any
		for _, p := range s.points {
			values = append(values, []any{p[0], strconv.FormatFloat(p[1], 'f', -1, 64)})
		}
		if instant {
			entry["value"] = values[0]
		} else {
			entry["values"] = values
		}
		result = append(result, entry)
	}
	resultType := "matrix"
	if instant {
		resultType = "vector"
	}
	body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
	return body
}

func foldResult(t *testing.T, body []byte) []foldSeries {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	var out []foldSeries
	for _, r := range resp.Data.Result {
		s := foldSeries{metric: r.Metric}
		values := r.Values
		if r.Value != nil {
			values = [][]any{r.Value}
		}
		for _, v := range values {
			f, _ := strconv.ParseFloat(v[1].(string), 64)
			s.points = append(s.points, [2]float64{v[0].(float64), f})
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i].metric) < labelsKey(out[j].metric) })
	return out
}

// foldByGrouping is Loki's VectorAggEvaluator over the series the manual
// evaluator returns: per step, the series that share the labels the grouping
// keeps become one; a series with no sample at a step is not counted at it.
//
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity
func TestFoldByGroupingEdgeCases(t *testing.T) {
	ab := func(pod, zone string) map[string]string {
		return map[string]string{"app": "a", "pod": pod, "zone": zone, "service_name": "a", "detected_level": "unknown"}
	}
	pt := func(ts, v float64) [2]float64 { return [2]float64{ts, v} }
	three := []foldSeries{
		{ab("p1", "x"), [][2]float64{pt(60, 1), pt(120, 4), pt(180, 4)}},
		{ab("p2", "x"), [][2]float64{pt(60, 3), pt(180, 8)}},
		{ab("p3", "y"), [][2]float64{pt(120, 10)}},
	}
	for _, tc := range []foldCase{
		{"max ungrouped", "max", nil, three, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 3), pt(120, 10), pt(180, 8)}}}},
		{"min ungrouped", "min", nil, three, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 1), pt(120, 4), pt(180, 4)}}}},
		{"count ungrouped counts the series with a sample", "count", nil, three, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 2), pt(120, 2), pt(180, 2)}}}},
		{"avg ungrouped", "avg", nil, three, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 2), pt(120, 7), pt(180, 6)}}}},
		{"by () keeps no label", "max", []string{}, three, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 3), pt(120, 10), pt(180, 8)}}}},
		{"by a label no series has", "max", []string{"baz"}, three, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 3), pt(120, 10), pt(180, 8)}}}},
		{"max by zone", "max", []string{"zone"}, three, []foldSeries{
			{map[string]string{"zone": "x"}, [][2]float64{pt(60, 3), pt(120, 4), pt(180, 8)}},
			{map[string]string{"zone": "y"}, [][2]float64{pt(120, 10)}},
		}},
		{"count by zone", "count", []string{"zone"}, three, []foldSeries{
			{map[string]string{"zone": "x"}, [][2]float64{pt(60, 2), pt(120, 1), pt(180, 2)}},
			{map[string]string{"zone": "y"}, [][2]float64{pt(120, 1)}},
		}},
		{"avg by pod and zone is the series themselves", "avg", []string{"pod", "zone"}, three, []foldSeries{
			{map[string]string{"pod": "p1", "zone": "x"}, [][2]float64{pt(60, 1), pt(120, 4), pt(180, 4)}},
			{map[string]string{"pod": "p2", "zone": "x"}, [][2]float64{pt(60, 3), pt(180, 8)}},
			{map[string]string{"pod": "p3", "zone": "y"}, [][2]float64{pt(120, 10)}},
		}},
		{"count of a single series is one", "count", nil, three[2:], []foldSeries{{map[string]string{}, [][2]float64{pt(120, 1)}}}},
		{"zero values count and compare", "min", nil, []foldSeries{
			{ab("p1", "x"), [][2]float64{pt(60, 0)}},
			{ab("p2", "x"), [][2]float64{pt(60, 5)}},
		}, []foldSeries{{map[string]string{}, [][2]float64{pt(60, 0)}}}},
		{"sum keeps adding", "sum", []string{"zone"}, three, []foldSeries{
			{map[string]string{"zone": "x"}, [][2]float64{pt(60, 4), pt(120, 4), pt(180, 12)}},
			{map[string]string{"zone": "y"}, [][2]float64{pt(120, 10)}},
		}},
		{"max skips a NaN first", "max", nil, nan([]float64{math.NaN(), 3, math.NaN()}), []foldSeries{{map[string]string{}, [][2]float64{pt(60, 3)}}}},
		{"min skips a NaN in the middle", "min", nil, nan([]float64{5, math.NaN(), 3}), []foldSeries{{map[string]string{}, [][2]float64{pt(60, 3)}}}},
		{"max of NaN only is NaN", "max", nil, nan([]float64{math.NaN(), math.NaN()}), []foldSeries{{map[string]string{}, [][2]float64{pt(60, math.NaN())}}}},
		{"min of NaN only is NaN", "min", nil, nan([]float64{math.NaN(), math.NaN()}), []foldSeries{{map[string]string{}, [][2]float64{pt(60, math.NaN())}}}},
		{"no series", "max", nil, nil, nil},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.name, instant), func(t *testing.T) {
				series, want := tc.series, tc.want
				if instant {
					series, want = instantOf(series, 180), instantOf(want, 180)
				}
				agg := &logqlpkg.VectorAggregation{Op: logqlpkg.VectorOp(tc.op)}
				if tc.by != nil {
					agg.Grouping = &logqlpkg.Grouping{Labels: tc.by}
				}
				got := foldResult(t, foldByGrouping(foldBody(series, instant), agg))
				if fmt.Sprint(got) != fmt.Sprint(foldResult(t, foldBody(want, instant))) {
					t.Fatalf("got %v, want %v", got, foldResult(t, foldBody(want, instant)))
				}
			})
		}
	}
}

// instantOf keeps the samples of the series at ts, as a vector holds them.
func instantOf(series []foldSeries, ts float64) []foldSeries {
	var out []foldSeries
	for _, s := range series {
		for _, p := range s.points {
			if p[0] == ts {
				out = append(out, foldSeries{s.metric, [][2]float64{p}})
			}
		}
	}
	return out
}

// parseOriginalRangeMetricSpec reads the range function through every
// aggregation above it, so a nested sum(max(rate(...))) evaluates a rate (it
// evaluated a count of lines when only the outermost operator was stripped).
//
// conformance: semantics/non-sum-vector-aggregation-unreduced
func TestParseOriginalRangeMetricSpecNestedAggregations(t *testing.T) {
	for _, tc := range []struct {
		query, fn string
	}{
		{`rate({app="a"}[5m])`, "rate"},
		{`max(rate({app="a"}[5m]))`, "rate"},
		{`sum(max(rate({app="a"}[5m])))`, "rate"},
		{`sum by (zone) (max by (pod, zone) (rate({app="a"}[5m])))`, "rate"},
		{`count(avg without (pod) (bytes_rate({app="a"}[5m])))`, "bytes_rate"},
		{`max(count_over_time({app="a"}[5m]))`, "count_over_time"},
		{`sum(max(sum_over_time({app="a"} | logfmt | unwrap n [5m])))`, "sum_over_time"},
	} {
		spec, ok := parseOriginalRangeMetricSpec(tc.query)
		if !ok || spec.Func != tc.fn || spec.Window != 5*time.Minute {
			t.Errorf("%s: func %q window %v ok=%v, want %q 5m", tc.query, spec.Func, spec.Window, ok, tc.fn)
		}
	}
}

// The evaluator of the manual path folds a count, min, max and avg too when
// the range function is not one VictoriaLogs evaluates itself (rate_counter,
// whose counter resets the proxy walks), at an instant and per step, whatever
// the number of streams behind a group.
//
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity
func TestVectorAggregationOverRateCounterFolds(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	// A counter per stream, increasing by 2, 5, 3 and 2 over the window.
	first, last := []float64{1, 1, 1, 1}, []float64{3, 6, 4, 3}
	var rows []emuRow
	for i, stream := range vaStreams {
		rows = append(rows,
			emuRow{ts: base + int64(i+1)*10e9, stream: stream, value: first[i], msg: fmt.Sprintf("amount=%v", first[i])},
			emuRow{ts: base + int64(i+1)*10e9 + 5e9, stream: stream, value: last[i], msg: fmt.Sprintf("amount=%v", last[i])})
	}
	perSecond := func(m map[string]float64) map[string]float64 {
		for k, v := range m {
			m[k] = v / 300
		}
		return m
	}
	for _, tc := range []struct {
		query string
		want  map[string]float64 // zone -> value; "" for one series
	}{
		{`max by (zone) (rate_counter({app="uw"} | logfmt | unwrap amount [5m]))`, perSecond(map[string]float64{"x": 5, "y": 3})},
		{`min by (zone) (rate_counter({app="uw"} | logfmt | unwrap amount [5m]))`, perSecond(map[string]float64{"x": 2, "y": 2})},
		{`count by (zone) (rate_counter({app="uw"} | logfmt | unwrap amount [5m]))`, map[string]float64{"x": 2, "y": 2}},
		{`avg by (zone) (rate_counter({app="uw"} | logfmt | unwrap amount [5m]))`, perSecond(map[string]float64{"x": 3.5, "y": 2.5})},
		{`max(rate_counter({app="uw"} | logfmt | unwrap amount [5m]))`, perSecond(map[string]float64{"": 5})},
		{`count(rate_counter({app="uw"} | logfmt | unwrap amount [5m]))`, map[string]float64{"": 4}},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.query, instant), func(t *testing.T) {
				emu := &vlEmulator{t: t, rows: rows, field: "amount"}
				p, closeBackend := newVectorAggProxy(t, emu)
				defer closeBackend()
				params := url.Values{"query": {tc.query}}
				target := "/loki/api/v1/query_range?"
				if instant {
					target = "/loki/api/v1/query?"
					params.Set("time", strconv.FormatInt(base/1e9+60, 10))
				} else {
					params.Set("start", strconv.FormatInt(base/1e9+60, 10))
					params.Set("end", strconv.FormatInt(base/1e9+60, 10))
					params.Set("step", "60")
				}
				res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				if res.Code != http.StatusOK {
					t.Fatalf("%d %s", res.Code, res.Body)
				}
				got := map[string]float64{}
				series := foldResult(t, res.Body.Bytes())
				for _, s := range series {
					if len(s.metric) > 1 || (len(s.metric) == 1 && s.metric["zone"] == "") {
						t.Fatalf("series labels %v", s.metric)
					}
					if _, dup := got[s.metric["zone"]]; dup {
						t.Fatalf("two series for zone %q: %v", s.metric["zone"], series)
					}
					got[s.metric["zone"]] = s.points[0][1]
				}
				if len(got) != len(tc.want) {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
				for zone, want := range tc.want {
					if g, ok := got[zone]; !ok || g-want > 1e-9 || want-g > 1e-9 {
						t.Fatalf("got %v, want %v", got, tc.want)
					}
				}
			})
		}
	}
}

// The without fold combines values the same way: a NaN never wins a min or max.
//
// conformance: semantics/non-sum-vector-aggregation-unreduced
func TestApplyWithoutGroupingNaNMinMax(t *testing.T) {
	for _, tc := range []struct {
		op     string
		values []float64
		want   string
	}{
		{"max", []float64{math.NaN(), 3, math.NaN()}, "3"},
		{"min", []float64{4, math.NaN(), 3}, "3"},
		{"min", []float64{math.NaN(), 2, 3}, "2"},
		{"max", []float64{math.NaN(), math.NaN()}, "NaN"},
	} {
		for _, instant := range []bool{false, true} {
			series := nan(tc.values)
			for i := range series {
				series[i].metric = map[string]string{"app": "a", "pod": series[i].metric["pod"]}
			}
			body := applyWithoutGrouping(foldBody(series, instant), []string{"pod"}, tc.op)
			got := foldResult(t, body)
			if len(got) != 1 || fmt.Sprint(got[0].points[0][1]) != tc.want {
				t.Errorf("%s %v instant=%v: got %v, want %s", tc.op, tc.values, instant, got, tc.want)
			}
		}
	}
}
