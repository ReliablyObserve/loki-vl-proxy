package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// binaryAggregationAnswer sends query through the real handler against the
// emulated VictoriaLogs and returns label set -> timestamp (s) -> value.
func binaryAggregationAnswer(t *testing.T, emu *vlEmulator, query string, start, end int64, step time.Duration) (map[string]map[int64]float64, string) {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if step == 0 {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(start/1e9, 10))
		params.Set("end", strconv.FormatInt(end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v %s", query, err, res.Body)
	}
	got := map[string]map[int64]float64{}
	for i, series := range resp.Data.Result {
		if i > 0 && !testLokiLabelsLess(resp.Data.Result[i-1].Metric, series.Metric) {
			t.Fatalf("%s: series out of Loki's label order: %s", query, res.Body)
		}
		key := binaryLabelKey(series.Metric)
		if got[key] != nil {
			t.Fatalf("%s: series %s twice in %s", query, key, res.Body)
		}
		got[key] = map[int64]float64{}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, err := strconv.ParseFloat(point[1].(string), 64)
			if err != nil {
				t.Fatalf("%s: value %v", query, point[1])
			}
			got[key][int64(point[0].(float64))] = v
		}
	}
	return got, resp.Data.ResultType
}

// binaryAggregationRows: three streams in two jobs, inside the minute before
// binaryAggregationAt. count_over_time per (job, pod) is p1=3, p2=1, p3=2 and
// sum_over_time of amount is p1=6, p2=4, p3=12.
const binaryAggregationAt = int64(1_800_000_060) * 1e9

func binaryAggregationRows() []emuRow {
	row := func(offset int64, stream string, value float64) emuRow {
		return emuRow{ts: binaryAggregationAt - offset*1e9, stream: stream, value: value}
	}
	p1, p2, p3 := `{app="uw",job="a",pod="p1"}`, `{app="uw",job="a",pod="p2"}`, `{app="uw",job="b",pod="p3"}`
	return []emuRow{
		row(50, p1, 1), row(30, p1, 2), row(10, p1, 3),
		row(40, p2, 4),
		row(45, p3, 5), row(5, p3, 7),
	}
}

// TestAggregationOverBinaryEdgeCases: a vector aggregation over a binary
// expression aggregates the binary's answer, per step, like Loki
// (pkg/logql/evaluator.go newBinOpStepEvaluator then VectorAggEvaluator.Next):
// both operands, vector matching on the full label set or on()/ignoring() with
// group_left, a scalar on either side, nested binaries, a comparison filtering
// series or answering 0/1 with bool, then sum, avg, min, max and count grouped
// by labels, without labels or not at all, and an aggregation over that.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryEdgeCases(t *testing.T) {
	const (
		count  = `sum by (job, pod) (count_over_time({app="uw"}[1m]))`
		amount = `sum by (job, pod) (sum_over_time({app="uw"} | unwrap amount [1m]))`
		perJob = `sum by (job) (count_over_time({app="uw"}[1m]))`
	)
	ja, jb, none := `{"job":"a"}`, `{"job":"b"}`, `{}`
	for _, tc := range []struct {
		name, query string
		want        map[string]float64
	}{
		{"sum by over metric/metric", `sum by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 6, jb: 6}},
		{"avg by", `avg by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 3, jb: 6}},
		{"min by", `min by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 2, jb: 6}},
		{"max by", `max by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 4, jb: 6}},
		{"count by", `count by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 2, jb: 1}},
		{"ungrouped sum", `sum(` + amount + ` / ` + count + `)`, map[string]float64{none: 12}},
		{"sum by ()", `sum by () (` + amount + ` / ` + count + `)`, map[string]float64{none: 12}},
		{"sum without", `sum without (pod) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 6, jb: 6}},
		{"count without folds once", `count without (pod) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 2, jb: 1}},
		{"avg without", `avg without (pod) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 3, jb: 6}},
		{"metric op scalar", `sum by (job) (` + count + ` * 2)`, map[string]float64{ja: 8, jb: 4}},
		{"scalar op metric", `sum(2 * ` + count + `)`, map[string]float64{none: 12}},
		{"metric divided by scalar", `sum(` + count + ` / 2)`, map[string]float64{none: 3}},
		{"comparison filters", `count(` + count + ` > 1)`, map[string]float64{none: 2}},
		{"comparison keeps the vector value", `sum(` + count + ` > 1)`, map[string]float64{none: 5}},
		{"scalar on the left of a comparison", `sum(1 < ` + count + `)`, map[string]float64{none: 5}},
		{"comparison with bool", `sum(` + count + ` > bool 1)`, map[string]float64{none: 2}},
		{"count over bool counts every series", `count(` + count + ` > bool 1)`, map[string]float64{none: 3}},
		{"vector comparison keeps the left value", `sum by (job) (` + amount + ` > ` + count + `)`, map[string]float64{ja: 10, jb: 12}},
		{"vector comparison with bool", `max by (job) (` + count + ` >= bool ` + amount + `)`, map[string]float64{ja: 0, jb: 0}},
		{"nested binary", `sum by (job) ((` + count + ` + ` + count + `) / 2)`, map[string]float64{ja: 4, jb: 2}},
		{"on group_left", `sum by (job) (` + count + ` / on (job) group_left ` + perJob + `)`, map[string]float64{ja: 1, jb: 1}},
		{"ignoring group_left", `max(` + count + ` / ignoring (pod) group_left ` + perJob + `)`, map[string]float64{none: 1}},
		{"aggregation over the aggregation", `max(sum by (job) (` + amount + ` / ` + count + `))`, map[string]float64{none: 6}},
		{"empty after the filter", `sum(` + count + ` > 100)`, map[string]float64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, version := range []string{"v1.52.0", "v1.40.0"} {
				emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: version}
				got, resultType := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt, binaryAggregationAt, 0)
				if resultType != "vector" {
					t.Fatalf("%s %s: resultType %q, want vector", version, tc.query, resultType)
				}
				flat := map[string]float64{}
				for key, points := range got {
					for ts, v := range points {
						if ts != binaryAggregationAt/1e9 {
							t.Fatalf("%s %s: sample at %d, want %d", version, tc.query, ts, binaryAggregationAt/1e9)
						}
						flat[key] = v
					}
				}
				if !reflect.DeepEqual(flat, tc.want) {
					t.Fatalf("%s %s:\n got %v\nwant %v", version, tc.query, flat, tc.want)
				}
			}
		})
	}
}

// The same on query_range: every step aggregates the binary's samples at that
// step, and a step where the binary has no sample has no point.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query_range
func TestAggregationOverBinaryRange(t *testing.T) {
	rows := binaryAggregationRows()
	// A second minute: p1 once (amount 10), p3 once (amount 1); p2 is silent.
	rows = append(rows,
		emuRow{ts: binaryAggregationAt + 20e9, stream: `{app="uw",job="a",pod="p1"}`, value: 10},
		emuRow{ts: binaryAggregationAt + 30e9, stream: `{app="uw",job="b",pod="p3"}`, value: 1},
	)
	// Rates: the ratio of the two is amount per line, as above. A sum_over_time
	// range query is registered apart (semantics/unwrap-over-time-range-windows).
	const (
		count  = `sum by (job, pod) (rate({app="uw"}[1m]))`
		amount = `sum by (job, pod) (rate({app="uw"} | unwrap amount [1m]))`
	)
	t0, t1 := binaryAggregationAt/1e9, binaryAggregationAt/1e9+60
	for _, tc := range []struct {
		query string
		want  map[string]map[int64]float64
	}{
		{`sum by (job) (` + amount + ` / ` + count + `)`, map[string]map[int64]float64{`{"job":"a"}`: {t0: 6, t1: 10}, `{"job":"b"}`: {t0: 6, t1: 1}}},
		{`count(` + count + ` * 60 > bool 1)`, map[string]map[int64]float64{`{}`: {t0: 3, t1: 2}}},
		{`sum(60 * ` + count + ` > 1)`, map[string]map[int64]float64{`{}`: {t0: 5}}},
		{`avg without (pod) (` + count + ` * 180)`, map[string]map[int64]float64{`{"job":"a"}`: {t0: 6, t1: 3}, `{"job":"b"}`: {t0: 6, t1: 3}}},
	} {
		for _, version := range []string{"v1.52.0", "v1.40.0"} {
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: version}
			got, resultType := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt, binaryAggregationAt+60e9, time.Minute)
			if resultType != "matrix" {
				t.Fatalf("%s %s: resultType %q, want matrix", version, tc.query, resultType)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s %s:\n got %v\nwant %v", version, tc.query, got, tc.want)
			}
		}
	}
}

// The registry's discovered shape (cluster f2066991), with and without its label_replace:
// one operand parses JSON, the other logfmt. Each operand reaches VictoriaLogs
// with its own parser, though the whole query names both (the handler drops
// one of two parsers it finds in a query), and the answer is a sample type on
// both endpoints, not the 400 for a log query or a stream.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryKeepsEachOperandsParser(t *testing.T) {
	const inner = `sum by (job) ( sum_over_time( {app="uw"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] ) / count_over_time({app="uw"} | logfmt | label_format foo=bar[5m]) )`
	for _, step := range []time.Duration{0, time.Minute} {
		for _, query := range []string{inner, `label_replace( ` + inner + `, "foo", "$1", "service", "(.*):.*" )`} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
			_, resultType := binaryAggregationAnswer(t, emu, query, binaryAggregationAt-300e9, binaryAggregationAt, step)
			if want := map[bool]string{true: "vector", false: "matrix"}[step == 0]; resultType != want {
				t.Fatalf("step=%v: resultType %q, want %q", step, resultType, want)
			}
			var jsonOperand, logfmtOperand bool
			for _, call := range emu.calls {
				if call.path == "/select/logsql/query" && strings.Contains(call.query, " / ") {
					continue // the parser probe of the whole query string
				}
				jsonOperand = jsonOperand || (strings.Contains(call.query, "unpack_json") && strings.Contains(call.query, "level=error"))
				logfmtOperand = logfmtOperand || (strings.Contains(call.query, "unpack_logfmt") && !strings.Contains(call.query, "level=error"))
			}
			if !jsonOperand || !logfmtOperand {
				t.Fatalf("step=%v: an operand lost its parser (json %v, logfmt %v): %v", step, jsonOperand, logfmtOperand, emu.calls)
			}
		}
	}
}

// Loki takes an offset on one side of the binary only (each side is its own
// step evaluator); so does the proxy under an aggregation, as for a bare binary.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query
func TestAggregationOverBinaryMixedOffset(t *testing.T) {
	rows := append(binaryAggregationRows(),
		emuRow{ts: binaryAggregationAt + 20e9, stream: `{app="uw",job="a",pod="p1"}`, value: 10},
		emuRow{ts: binaryAggregationAt + 30e9, stream: `{app="uw",job="b",pod="p3"}`, value: 1},
	)
	// A minute earlier: p1=3, p2=1, p3=2; now: p1=1, p3=1.
	query := `sum(sum by (job, pod) (count_over_time({app="uw"}[1m] offset 1m)) / sum by (job, pod) (count_over_time({app="uw"}[1m])))`
	emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
	got, _ := binaryAggregationAnswer(t, emu, query, 0, binaryAggregationAt+60e9, 0)
	if want := map[string]map[int64]float64{`{}`: {binaryAggregationAt/1e9 + 60: 5}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// A top-level binary whose first operand is a label_replace stays on the binary
// evaluator, and the operand applies its own transform: only a transform around
// the whole expression is applied to the binary's answer.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query
func TestBinaryWithLabelReplaceOperandStaysBinary(t *testing.T) {
	const count = `sum by (job, pod) (count_over_time({app="uw"}[1m]))`
	for query, want := range map[string]map[string]float64{
		`label_replace(` + count + `, "dst", "$1", "job", "(.*)") / on (job, pod) ` + count: {
			`{"job":"a","pod":"p1"}`: 1, `{"job":"a","pod":"p2"}`: 1, `{"job":"b","pod":"p3"}`: 1},
		`label_replace(` + count + `, "dst", "$1", "job", "(.*)") * on (job, pod) ` + count: {
			`{"job":"a","pod":"p1"}`: 9, `{"job":"a","pod":"p2"}`: 1, `{"job":"b","pod":"p3"}`: 4},
		`label_replace(` + count + `, "dst", "$1", "job", "(.*)") * 2`: {
			`{"dst":"a","job":"a","pod":"p1"}`: 6, `{"dst":"a","job":"a","pod":"p2"}`: 2, `{"dst":"b","job":"b","pod":"p3"}`: 4},
		`sum by (dst) (label_replace(` + count + `, "dst", "$1", "job", "(.*)") / ` + count + `)`: {},
		`sum by (dst) (label_replace(` + count + `, "dst", "$1", "job", "(.*)") / ignoring (dst) group_left ` + count + `)`: {
			`{"dst":"a"}`: 2, `{"dst":"b"}`: 1},
	} {
		for _, step := range []time.Duration{0, time.Minute} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
			got, _ := binaryAggregationAnswer(t, emu, query, binaryAggregationAt, binaryAggregationAt, step)
			flat := map[string]float64{}
			for key, points := range got {
				for _, v := range points {
					flat[key] = v
				}
			}
			if !reflect.DeepEqual(flat, want) {
				t.Fatalf("step=%v %s:\n got %v\nwant %v", step, query, flat, want)
			}
		}
	}
}

// The same offset on both sides (the discovered shape's second form) shifts the
// windows and stamps the samples at the requested times, on both endpoints.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryUniformOffset(t *testing.T) {
	const (
		count  = `sum by (job, pod) (count_over_time({app="uw"}[1m] offset 1h))`
		amount = `sum by (job, pod) (sum_over_time({app="uw"} | unwrap amount [1m] offset 1h))`
		rate   = `sum by (job, pod) (rate({app="uw"} | unwrap amount [1m] offset 1h))`
	)
	at := binaryAggregationAt + 3600e9
	emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
	got, _ := binaryAggregationAnswer(t, emu, `sum by (job) (`+amount+` / `+count+`)`, 0, at, 0)
	if want := map[string]map[int64]float64{`{"job":"a"}`: {at / 1e9: 6}, `{"job":"b"}`: {at / 1e9: 6}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("instant: got %v, want %v", got, want)
	}
	emu = &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
	got, _ = binaryAggregationAnswer(t, emu, `max(`+rate+` / `+count+`)`, at-60e9, at, time.Minute)
	if want := map[string]map[int64]float64{`{}`: {at / 1e9: 6.0 / 60}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("range: got %v, want %v", got, want)
	}
}

// A label_replace or label_join around the aggregation (the discovered shape,
// cluster f2066991) applies to the folded answer, on both endpoints: the
// aggregation is not handed to the translator, which kept its first operand.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryUnderLabelTransforms(t *testing.T) {
	const (
		count  = `sum by (job, pod) (rate({app="uw"}[1m]))`
		amount = `sum by (job, pod) (rate({app="uw"} | unwrap amount [1m]))`
	)
	inner := `sum by (job) (` + amount + ` / ` + count + `)`
	at := binaryAggregationAt / 1e9
	for _, tc := range []struct {
		query string
		want  map[string]float64
	}{
		{`label_replace(` + inner + `, "dst", "$1", "job", "(.*)")`, map[string]float64{`{"dst":"a","job":"a"}`: 6, `{"dst":"b","job":"b"}`: 6}},
		{`label_join(` + inner + `, "dst", "-", "job", "job")`, map[string]float64{`{"dst":"a-a","job":"a"}`: 6, `{"dst":"b-b","job":"b"}`: 6}},
		{`label_replace(label_replace(` + inner + `, "dst", "$1", "job", "(.*)"), "job", "x", "", "")`, map[string]float64{`{"dst":"a","job":"x"}`: 6, `{"dst":"b","job":"x"}`: 6}},
	} {
		for _, step := range []time.Duration{0, time.Minute} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
			got, _ := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt, binaryAggregationAt, step)
			want := map[string]map[int64]float64{}
			for key, v := range tc.want {
				want[key] = map[int64]float64{at: v}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("step=%v %s:\n got %v\nwant %v", step, tc.query, got, want)
			}
		}
	}
}

// testLokiLabelsLess is labels.Compare(a, b) < 0 (Prometheus model/labels),
// written apart from the proxy's: the sorted (name, value) pairs compared in
// turn, a shorter list first when one is a prefix of the other.
func testLokiLabelsLess(a, b map[string]string) bool {
	pairs := func(m map[string]string) [][2]string {
		out := make([][2]string, 0, len(m))
		for k, v := range m {
			out = append(out, [2]string{k, v})
		}
		sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
		return out
	}
	pa, pb := pairs(a), pairs(b)
	for i := range pa {
		if i == len(pb) {
			return false
		}
		if pa[i] != pb[i] {
			if pa[i][0] != pb[i][0] {
				return pa[i][0] < pb[i][0]
			}
			return pa[i][1] < pb[i][1]
		}
	}
	return len(pa) < len(pb)
}

// A fold keeps every label set apart, a label value with a comma and an equals
// sign included, and answers the series in Loki's label order, which is not the
// order of their `name=value,...` text.
//
// conformance: semantics/aggregation-over-binary-dropped
func TestWithoutFoldLabelOrderAndKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		metric []map[string]string
		want   []map[string]string
	}{
		{"a name that extends another", []map[string]string{{"a0": "y"}, {"a": "x"}}, []map[string]string{{"a": "x"}, {"a0": "y"}}},
		{"a value that extends another", []map[string]string{{"route": "/a b"}, {"route": "/a", "status": "200"}}, []map[string]string{{"route": "/a", "status": "200"}, {"route": "/a b"}}},
		{"a value with a comma and an equals sign", []map[string]string{{"a": "b", "c": "d"}, {"a": "b,c=d"}}, []map[string]string{{"a": "b", "c": "d"}, {"a": "b,c=d"}}},
		{"a prefix first", []map[string]string{{"a": "x", "b": "y"}, {"a": "x"}}, []map[string]string{{"a": "x"}, {"a": "x", "b": "y"}}},
	} {
		if tc.name == "a value with a comma and an equals sign" && metricKeyStr(tc.metric[0]) != metricKeyStr(tc.metric[1]) {
			t.Fatal("the two sets should share their name=value text, the key the fold no longer uses")
		}
		for _, resultType := range []string{"vector", "matrix"} {
			result := make([]map[string]any, 0, len(tc.metric))
			for _, metric := range tc.metric {
				entry := map[string]any{"metric": metric, "value": []any{1.0, "1"}}
				if resultType == "matrix" {
					entry = map[string]any{"metric": metric, "values": [][]any{{1.0, "1"}}}
				}
				result = append(result, entry)
			}
			body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Value  []any             `json:"value"`
						Values [][]any           `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(applyWithoutGrouping(body, []string{"absent"}, "count"), &resp); err != nil {
				t.Fatal(err)
			}
			var got []map[string]string
			for _, series := range resp.Data.Result {
				got = append(got, series.Metric)
				point := series.Value
				if resultType == "matrix" {
					point = series.Values[0]
				}
				if point[1] != "1" {
					t.Fatalf("%s %s: %v counts %v series, want 1 (folded with another)", tc.name, resultType, series.Metric, point[1])
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s %s: got %v, want %v", tc.name, resultType, got, tc.want)
			}
			for i := 1; i < len(tc.want); i++ {
				if !testLokiLabelsLess(tc.want[i-1], tc.want[i]) {
					t.Fatalf("%s: the expectation is not in Loki's order", tc.name)
				}
			}
		}
	}
}

// A label transform rewrites the series of a vector or a matrix and keeps the
// answer's warnings and statistics.
//
// conformance: semantics/aggregation-over-binary-dropped
func TestLabelTransformsKeepVectorAndWarnings(t *testing.T) {
	body := []byte(`{"status":"success","warnings":["partial"],"data":{"resultType":"vector","stats":{"summary":{"bytesProcessedPerSecond":1}},"result":[{"metric":{"job":"a"},"value":[1,"2"]}]}}`)
	got := applyLabelJoin(applyLabelReplace(body, translator.LabelReplaceSpec{DstLabel: "dst", Replacement: "$1", SrcLabel: "job", Regex: "(.*)"}), translator.LabelJoinSpec{DstLabel: "both", Separator: "-", SrcLabels: []string{"job", "dst"}})
	want := `{"status":"success","warnings":["partial"],"data":{"resultType":"vector","stats":{"summary":{"bytesProcessedPerSecond":1}},"result":[{"metric":{"both":"a-a","dst":"a","job":"a"},"value":[1,"2"]}]}}`
	var g, w any
	if json.Unmarshal(got, &g) != nil || json.Unmarshal([]byte(want), &w) != nil || !reflect.DeepEqual(g, w) {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
