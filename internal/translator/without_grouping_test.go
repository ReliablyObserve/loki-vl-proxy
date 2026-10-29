package translator

import (
	"strings"
	"testing"
)

// lastStatsGrouping returns the by() list of the last stats pipe of a
// translated query, "" when it has none.
func lastStatsGrouping(query string) string {
	query, _, _ = ParseWithoutMarker(query)
	idx := strings.LastIndex(query, "| stats ")
	if idx < 0 {
		return "<no stats>"
	}
	rest := query[idx+len("| stats "):]
	if !strings.HasPrefix(rest, "by (") {
		return ""
	}
	return rest[len("by ("):strings.Index(rest, ")")]
}

// An `<op> without (...)` keeps a row per series in the last stats pipe, so the
// proxy can drop the excluded labels and fold the rows that share the labels
// left, as Loki does (pkg/logql/evaluator.go:451-466). Only `by` and an
// ungrouped aggregation fold in VictoriaLogs.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutKeepsSeriesInLastStatsPipe(t *testing.T) {
	for _, tc := range []struct {
		logql, want string
	}{
		// The registered shape: a parser's per-line identity stays in the
		// inner pipe, the outer pipe keeps the stream.
		{`sum without (pod) (max_over_time({app="x"} | logfmt | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod) (max_over_time({app="x"} | json | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod) (max_over_time({app="x"} | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod, ns) (sum_over_time({app="x"} | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod) (avg_over_time({app="x"} | logfmt | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod) (stdvar_over_time({app="x"} | logfmt | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod) (rate({app="x"} | logfmt | unwrap amount [5m]))`, "_stream"},
		{`sum without (pod) (rate({app="x"} | unwrap amount [5m]))`, "_stream, level"},
		{`sum without (pod) (rate({app="x"}[5m]))`, "_stream, level"},
		{`sum without (pod) (bytes_rate({app="x"}[5m]))`, "_stream, level"},
		{`max without (pod) (max_over_time({app="x"} | logfmt | unwrap amount [5m]))`, "_stream"},
		{`min without (pod) (min_over_time({app="x"} | unwrap amount [5m]))`, "_stream"},
		// count and avg read one value per series: kept when the stream is the
		// series.
		{`count without (pod) (max_over_time({app="x"} | unwrap amount [5m]))`, "_stream"},
		{`avg without (pod) (max_over_time({app="x"} | unwrap amount [5m]))`, "_stream"},
		// A range aggregation grouping is the series.
		{`sum without (pod) (max_over_time({app="x"} | unwrap amount [5m]) by (ns, pod))`, "ns, pod"},
		// Not `without`: the fold stays in VictoriaLogs.
		{`sum(max_over_time({app="x"} | logfmt | unwrap amount [5m]))`, ""},
		{`sum(rate({app="x"}[5m]))`, ""},
		{`sum by (ns) (rate({app="x"}[5m]))`, "ns"},
		// Not translated series by series: one row, as before (stddev and stdvar
		// without keep their single pipe; their rows must never be added up).
		{`stddev without (pod) (max_over_time({app="x"} | unwrap amount [1m]))`, ""},
		{`stdvar without (pod) (rate({app="x"}[1m]))`, ""},
		// Already a row per series before this change.
		{`count without (pod) (count_over_time({app="x"}[1m]))`, "_stream, level"},
	} {
		got, err := TranslateLogQL(tc.logql)
		if err != nil {
			t.Fatalf("%s: %v", tc.logql, err)
		}
		if g := lastStatsGrouping(withoutDetectedLevelShape(got)); g != tc.want {
			t.Errorf("%s: last stats pipe grouped by (%s), want (%s)\n%s", tc.logql, g, tc.want, got)
		}
		excluded := strings.ReplaceAll(tc.logql[strings.Index(tc.logql, "(")+1:strings.Index(tc.logql, ")")], " ", "")
		// stddev and stdvar without are not folded: their marker names no operator.
		op := ""
		if i := strings.Index(tc.logql, " "); i > 0 && !strings.HasPrefix(tc.logql, "std") {
			op = tc.logql[:i] + ":"
		}
		if strings.Contains(tc.logql, "without") && !strings.HasSuffix(got, WithoutMarkerSuffix+op+excluded) {
			t.Errorf("%s: the without marker is missing: %s", tc.logql, got)
		}
	}
}

func TestKeepWithoutSeries(t *testing.T) {
	const base = `* | stats by (_stream, _msg) max(v) as __lvp_inner | stats sum(__lvp_inner)`
	for _, tc := range []struct {
		innerBy, op string
		without     bool
		want        string
	}{
		{"_stream, _msg", "sum", true, `* | stats by (_stream, _msg) max(v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)`},
		{"_stream, _msg", "max", true, `* | stats by (_stream, _msg) max(v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)`},
		{"_stream, _msg", "min", true, `* | stats by (_stream, _msg) max(v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)`},
		{"_stream", "count", true, `* | stats by (_stream, _msg) max(v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)`},
		{"_stream, level", "avg", true, `* | stats by (_stream, _msg) max(v) as __lvp_inner | stats by (_stream, level) sum(__lvp_inner)`},
		// A row per stream would merge the per-line values of count and avg.
		{"_stream, _msg", "count", true, base},
		{"_stream, _msg", "avg", true, base},
		// stddev and stdvar are not merged by the proxy's without fold.
		{"_stream", "stddev", true, base},
		{"_stream", "stdvar", true, base},
		{"_stream", "sum", false, base},
		{emptyByGrouping, "sum", true, base},
		{"", "sum", true, base},
		{"_msg", "sum", true, base},
	} {
		if got := keepWithoutSeries(base, tc.innerBy, tc.op, tc.without); got != tc.want {
			t.Errorf("keepWithoutSeries(%q, %s, %v):\n got %s\nwant %s", tc.innerBy, tc.op, tc.without, got, tc.want)
		}
	}
}

// TestWithoutMarkerCarriesTheAggregation: the operator of the `without`
// aggregation travels in the marker, so every consumer folds the series the
// same way whatever wraps it (a label_replace, a comparison; topk, bottomk and
// sort translate the aggregation they wrap on its own).
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutMarkerCarriesTheAggregation(t *testing.T) {
	const inner = `{app="x"} | logfmt | unwrap amount [5m]`
	for _, tc := range []struct{ logql, op string }{
		{`sum without (pod) (max_over_time(` + inner + `))`, "sum"},
		{`max without (pod) (max_over_time(` + inner + `))`, "max"},
		{`min without (pod) (min_over_time(` + inner + `))`, "min"},
		{`label_replace(max without (pod) (max_over_time(` + inner + `)), "d", "$1", "app", "(.*)")`, "max"},
		// A binary expression is folded operand by operand, not as a whole.
		{`count without (pod) (max_over_time(` + inner + `)) > 0`, ""},
		{`avg(max_over_time(` + inner + `)) without (pod)`, "avg"},
		{`max(max_over_time({app="a(b"} | logfmt | unwrap amount [5m])) without (pod)`, "max"},
		{`sum without (pod) (rate({app="x"}[5m]))`, "sum"},
	} {
		query, err := TranslateLogQL(tc.logql)
		if err != nil {
			t.Fatalf("%s: %v", tc.logql, err)
		}
		_, op, labels := ParseWithoutMarker(query)
		if op != tc.op || len(labels) != 1 || labels[0] != "pod" {
			t.Fatalf("%s: marker op=%q labels=%v, want %q [pod]\n%s", tc.logql, op, labels, tc.op, query)
		}
	}
}

// TestFoldingWithoutOp: the proxy folds a `without` from a row per stream only
// for a sum, min, max, avg or count that is the outermost aggregation (a ranking,
// sorting or label transform may wrap it: the proxy applies it to the folded
// series). Every other query that holds a `without` clause carries a marker
// without an operator and is answered as before the fold existed.
//
// conformance: semantics/unwrap-sum-without-collapses
func TestFoldingWithoutOp(t *testing.T) {
	const m = `max_over_time({app="x"} | unwrap amount [5m])`
	for _, tc := range []struct{ logql, want string }{
		{`sum without (pod) (` + m + `)`, "sum"},
		{`min without (pod, ns) (` + m + `)`, "min"},
		{`max   without(pod)(` + m + `)`, "max"},
		{`avg(` + m + `) without (pod)`, "avg"},
		{`count (` + m + `) without (pod)`, "count"},
		{`topk(3, sum without (pod) (` + m + `))`, "sum"},
		{`bottomk(3, max without (pod) (` + m + `))`, "max"},
		{`sort(sum without (pod) (` + m + `))`, "sum"},
		{`sort_desc(sum without (pod) (` + m + `))`, "sum"},
		{`label_replace(sum without (pod) (` + m + `), "d", "$1", "app", "(.*)")`, "sum"},
		{`topk(3, label_replace(sum without (pod) (` + m + `), "d", "$1", "app", "(.*)"))`, "sum"},
		{`stddev(sum without (pod) (` + m + `))`, "sum"},
		// Operators the fold does not prove.
		{`topk without (pod) (3, ` + m + `)`, ""},
		{`bottomk without (pod) (3, ` + m + `)`, ""},
		{`stddev without (pod) (` + m + `)`, ""},
		{`stdvar without (pod) (` + m + `)`, ""},
		// A without under another aggregation, or not the outermost aggregation.
		{`max(sum without (pod) (` + m + `))`, ""},
		{`sum by (ns) (sum without (pod) (` + m + `))`, ""},
		{`sum(sum without (pod) (` + m + `)) by (ns)`, ""},
		{`topk(2, max(sum without (pod) (` + m + `)))`, ""},
		{`sum without (pod) (` + m + `) > 1`, ""},
		// Text after the call a wrapper peels: the wrapper is not the whole query.
		{`topk(5, sum without (pod) (` + m + `)) > 10`, ""},
		{`stddev(sum without (pod) (` + m + `)) by (level)`, ""},
		{`sum without (pod) (` + m + `) / sum without (pod) (` + m + `)`, ""},
		// More than one without clause, none with labels, no aggregation.
		{`sum without (pod) (sum without (ns) (` + m + `))`, ""},
		{`sum without () (` + m + `)`, ""},
		{`sum(` + m + `)`, ""},
		{m, ""},
	} {
		if got := foldingWithoutOp(tc.logql); got != tc.want {
			t.Errorf("foldingWithoutOp(%s) = %q, want %q", tc.logql, got, tc.want)
		}
	}
}
