package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// FuzzFoldBinaryAggregation: the fold of an aggregation over a binary's
// answer (foldBinaryAggregation) against a reference of Loki's
// VectorAggEvaluator.Next (pkg/logql/evaluator.go:435-630): series are grouped
// by the labels `by` keeps or `without` leaves (none without a grouping), and
// each group is valued by sum, avg, min, max or count of its samples. Label
// values hold `,`, `=` and quotes, so a group key that joins them as text
// would merge two label sets. The answer is in Loki's series order
// (labels.Compare). Values are finite: NaN under min and max is registered
// apart (semantics/binary-division-by-zero-nan).
//
// conformance: semantics/aggregation-over-binary-dropped
func FuzzFoldBinaryAggregation(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, uint8(0), uint8(1))
	f.Add([]byte{3, 3, 3, 9, 9, 9, 1, 2}, uint8(3), uint8(2))
	f.Add([]byte{17, 33, 65, 129, 7, 200}, uint8(4), uint8(0))
	f.Add([]byte{}, uint8(1), uint8(5))
	f.Fuzz(func(t *testing.T, data []byte, opByte, groupByte uint8) {
		ops := []logqlpkg.VectorOp{logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount}
		op := ops[int(opByte)%len(ops)]
		names := []string{"job", "pod", "x"}
		values := []string{"", "a", "b", "a,b=c", `q"1`, "b,a"}
		groupings := []*logqlpkg.Grouping{
			nil,
			{Labels: []string{"job"}},
			{Labels: []string{"job", "x"}},
			{Without: true, Labels: []string{"pod"}},
			{Without: true, Labels: []string{"job", "pod", "x"}},
			{Labels: []string{"absent"}},
		}
		grouping := groupings[int(groupByte)%len(groupings)]

		type sample struct {
			labels map[string]string
			value  float64
		}
		var samples []sample
		for i := 0; i+1 < len(data) && len(samples) < 12; i += 2 {
			labels := map[string]string{}
			for j, name := range names {
				if v := values[(int(data[i])>>(2*j))%len(values)]; v != "" {
					labels[name] = v
				}
			}
			samples = append(samples, sample{labels: labels, value: float64(int(data[i+1])%41 - 20)})
		}

		result := make([]map[string]any, 0, len(samples))
		for _, s := range samples {
			result = append(result, map[string]any{"metric": s.labels, "value": []any{1800000060, strconv.FormatFloat(s.value, 'f', -1, 64)}})
		}
		body, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
		if err != nil {
			t.Fatal(err)
		}

		// Loki's fold.
		groupLabels := func(in map[string]string) map[string]string {
			out := map[string]string{}
			for name, value := range in {
				switch {
				case grouping == nil:
				case grouping.Without:
					if !slices.Contains(grouping.Labels, name) {
						out[name] = value
					}
				default:
					if slices.Contains(grouping.Labels, name) {
						out[name] = value
					}
				}
			}
			return out
		}
		type group struct {
			labels map[string]string
			vals   []float64
		}
		want := map[string]*group{}
		for _, s := range samples {
			labels := groupLabels(s.labels)
			key := binaryLabelKey(labels)
			if want[key] == nil {
				want[key] = &group{labels: labels}
			}
			want[key].vals = append(want[key].vals, s.value)
		}
		wantValue := func(vals []float64) float64 {
			switch op {
			case logqlpkg.VectorCount:
				return float64(len(vals))
			case logqlpkg.VectorMin, logqlpkg.VectorMax:
				out := vals[0]
				for _, v := range vals[1:] {
					if (op == logqlpkg.VectorMin) == (v < out) {
						out = v
					}
				}
				return out
			}
			sum := 0.0
			for _, v := range vals {
				sum += v
			}
			if op == logqlpkg.VectorAvg {
				return sum / float64(len(vals))
			}
			return sum
		}

		agg := &logqlpkg.VectorAggregation{Op: op, Grouping: grouping}
		desc := string(op)
		if grouping != nil {
			desc += fmt.Sprintf(" without=%v %v", grouping.Without, grouping.Labels)
		}
		var got struct {
			Status string `json:"status"`
			Data   struct {
				ResultType string `json:"resultType"`
				Result     []struct {
					Metric map[string]string `json:"metric"`
					Value  []any             `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		folded := foldBinaryAggregation(body, agg)
		if err := json.Unmarshal(folded, &got); err != nil {
			t.Fatalf("%s: %v %s", desc, err, folded)
		}
		if got.Status != "success" || got.Data.ResultType != "vector" || len(got.Data.Result) != len(want) {
			t.Fatalf("%s over %s: %s, want %d series", desc, body, folded, len(want))
		}
		for i, series := range got.Data.Result {
			if i > 0 && compareLabelSets(got.Data.Result[i-1].Metric, series.Metric) >= 0 {
				t.Fatalf("%s: series out of Loki's order: %s", desc, folded)
			}
			w := want[binaryLabelKey(series.Metric)]
			if w == nil {
				t.Fatalf("%s over %s: unexpected series %v in %s", desc, body, series.Metric, folded)
			}
			v, err := strconv.ParseFloat(series.Value[1].(string), 64)
			if expected := wantValue(w.vals); err != nil || math.Abs(v-expected) > 1e-9*math.Max(1, math.Abs(expected)) {
				t.Fatalf("%s over %s: %v = %v, want %v", desc, body, series.Metric, series.Value[1], expected)
			}
		}
	})
}

var parserParamsRE = regexp.MustCompile(`^(\s+[A-Za-z_]\w*(\s*=\s*"[^"\\]*")?(\s*,\s*[A-Za-z_]\w*(\s*=\s*"[^"\\]*")?)*)?$`)

// FuzzParserProbeLeavesBinaries: the parser probe (preferWorkingParser) drops
// one of the json and logfmt stages from a query naming both, which is right
// for one operand and wrong for a binary, whose operands each keep their own
// parser (pkg/logql/evaluator.go:385). With fuzzed stage parameters, every
// binary and aggregation over one that parses (bare, aggregated, nested, under
// label_replace) is left to its operands, and one operand naming both parsers
// is still probed. Any text at all is read without a panic.
//
// conformance: semantics/aggregation-over-binary-dropped
func FuzzParserProbeLeavesBinaries(f *testing.F) {
	f.Add(``, ``)
	f.Add(` route="route"`, ` x, y="z"`)
	f.Add(` first="servers[0]", second="f(1) | x"`, ` a="b[1]"`)
	f.Fuzz(func(t *testing.T, jsonParams, logfmtParams string) {
		_ = operandsKeepTheirParsers(jsonParams + logfmtParams)
		// Stage parameters as Loki's grammar has them (names, `name="value"`,
		// commas); a quoted value may hold anything but a quote or a backslash.
		if !parserParamsRE.MatchString(jsonParams) || !parserParamsRE.MatchString(logfmtParams) {
			return
		}
		amount := `sum by (route) (sum_over_time({app="x"} | json` + jsonParams + ` | unwrap amount [5m]))`
		lines := `sum by (route) (count_over_time({app="y"} | logfmt` + logfmtParams + ` [5m]))`
		for _, tc := range []struct {
			query string
			keep  bool
		}{
			{amount + ` / ` + lines, true},
			{`sum by (route) (` + amount + ` / ` + lines + `)`, true},
			{`max(avg without (route) (` + amount + ` > bool ` + lines + `))`, true},
			{`label_replace(sum(` + amount + ` / ` + lines + `), "dst", "$1", "route", "(.*)")`, true},
			{`sum by (route) (count_over_time({app="y"} | json` + jsonParams + ` | logfmt` + logfmtParams + ` [5m]))`, false},
		} {
			// The expression under the label_replace must parse as LogQL: the
			// parser takes what it cannot read (the wrapper itself, `| json]`)
			// as an opaque metric.
			if expr, err := logqlpkg.Parse(stripLabelTransforms(tc.query)); err != nil {
				continue
			} else if _, opaque := expr.(*logqlpkg.OpaqueMetricExpr); opaque {
				continue
			}
			if got := operandsKeepTheirParsers(tc.query); got != tc.keep {
				t.Fatalf("%q: operandsKeepTheirParsers %v, want %v", tc.query, got, tc.keep)
			}
		}
	})
}
