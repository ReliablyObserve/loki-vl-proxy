package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The metric screen names the construct at the top of a metric query the plan may serve, and turns away, before any
// parse, every class of query the plan leaves to the string translator or evaluates in the proxy. Each row also
// parses the query and, where the screen names a construct, checks it is the plan's root.
//
// conformance: architecture-syntax-package-layering
func TestScreenMetric(t *testing.T) {
	for _, tc := range []struct {
		query, want string
	}{
		// Served constructs.
		{`sum by (app) (rate({app="api"} |= "x" [5m]))`, "VectorAgg"},
		{`sum(rate({app="uw"} [30s])) without (zone, rack)`, "VectorAgg"},
		{`max(sum(rate({app="uw"} | logfmt [30s])) by (pod, zone)) by (zone)`, "VectorAgg"},
		{`sum by (app) (quantile_over_time(0.9, {app="api"} |= "x" | logfmt | unwrap d [5m]))`, "VectorAgg"},
		{`sum by (app) (count_over_time({app="api"} | json | status >= 500 [5m]))`, "VectorAgg"},
		{`sum(count_over_time({app="a"} | logfmt | addr=ip("1.2.3.0/24") [10m]))`, "VectorAgg"},
		{`count_over_time({app="api"} |= "error" [5m])`, "RangeAgg"},
		{`max_over_time({app="api"} | json | unwrap d [5m]) by (app)`, "RangeAgg"},
		{`rate({app="a"} | json | status>=200 and status<300 [5m])`, "RangeAgg"},
		{`rate_counter({app="api"} | unwrap latency [5m])`, "RangeAgg"},
		{`sum_over_time({a="b"} | logfmt | label_format y="{{.x}}" | unwrap y [5m])`, "RangeAgg"},
		{`count_over_time({app="a"} !> "<_> x" [1m])`, "RangeAgg"},
		{`count_over_time({app="a"} |~ "\\d+" [1m] offset 5m)`, "RangeAgg"},
		{`sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`, "BinOp"},
		{`sum(rate({app="a"} | json | unwrap bytes [5m])) + 2`, "BinOp"},
		{`sum by (app) (rate({app="a"}[5m])) / on (app) group_left sum by (app) (rate({app="b"}[5m]))`, "BinOp"},
		{`label_replace(sum by (app) (rate({app="a"}[5m])), "x", "$1", "app", "(.*)")`, "LabelReplace"},
		{`label_replace(sum by (a) (rate({app="a"}[5m])) / sum by (a) (rate({app="a"}[5m])), "d", "$1", "a", "(.*)")`, "LabelReplace"},
		{`label_replace(label_replace(rate({app="a"}[5m]), "a", "$1", "b", "(.*)"), "c", "x", "d", "(.*)")`, "LabelReplace"},
		{`sum(count_over_time({path="separator ",app="tail"}[5m]))`, "VectorAgg"},
		// What the proxy evaluates or the plan does not write.
		{`topk(3, sum by (app) (rate({app="api"}[5m])))`, ""},
		{`sort_desc(sum(rate({app="api"}[5m])))`, ""},
		{`stddev by (app) (count_over_time({app="api"}[5m]))`, ""},
		{`vector(1)`, ""},
		{`vector(1) + sum(rate({app="api"}[5m]))`, ""},
		{`sum(rate({app="a"}[5m])) > bool 2`, ""},
		{`sum(rate({app="a"}[5m])) or sum(rate({app="b"}[5m]))`, ""},
		{`1 > 2`, ""},
		{`absent(rate({app="a"}[5m]))`, ""},
		{`rate_counter({app="api"}[5m])`, ""},
		{`quantile_over_time(0.95, {app="nginx"}[5m])`, ""},
		// Stages and clauses the lowering leaves to the translator.
		{`count_over_time({app="api"} |= "x" | unpack | x="y" [5m])`, ""},
		{`count_over_time({app="a"} | line_format "{{.x}}" [5m])`, ""},
		{`count_over_time({app="a"} | decolorize [5m])`, ""},
		// A regexp or pattern parser written with no blank before its quoted expression: the plan writes the spaced
		// spelling's text (semantics/translator-copies-unspaced-parser). An empty pattern is rejected by the request's
		// validation before any translation.
		{`count_over_time({}|pattern""[0y])`, "RangeAgg"},
		{`count_over_time({app="a"} |pattern"<_> <m>" [5m])`, "RangeAgg"},
		{`rate({app="a"} | regexp"(?P<a>x)" | a="x" [5m])`, "RangeAgg"},
		{"count_over_time({app=\"a\"} | regexp`(?P<a>x)` [5m])", "RangeAgg"},
		{`count_over_time({app="a"} | pattern="x" [5m])`, ""},
		{`count_over_time({app="a"} | pattern "<_> <m>" [5m])`, "RangeAgg"},
		{`sum by (app) (quantile_over_time(0.9, {app="a"} | keep app | json | unwrap d [5m]))`, ""},
		{`sum by (lvl) (count_over_time({app="a"} | logfmt | detected_level="error" [5m]))`, ""},
		{`sum without () (count_over_time({app="api"}[5m]))`, ""},
		{`sum without ( ) (count_over_time({app="api"}[5m]))`, ""},
		{`max(stdvar_over_time({app="a"} | json | unwrap d [5m]) by (pod))`, ""},
		{`absent_over_time({app="a"} | json [1m]) without (app)`, ""},
		{`max(sum without (pod) (rate({app="a"} | logfmt | unwrap amount [5m])))`, ""},
		{`sum by (cluster) (sum(rate({a="b"}[5m])) + sum(rate({a="b"}[5m])))`, ""},
		{`rate({a="b"}[5m]) * 2 + rate({a="b"}[5m])`, ""},
		{`(rate({a="b"}[5m]) + rate({a="b"}[5m])) * 2`, ""},
		{`sum without (pod) (rate({app="a"}[5m])) / 2`, ""},
		{`label_replace(rate({a="b"}[5m]), "d", "$1", "app", "x") + rate({a="b"}[5m])`, ""},
		{`sum by (s) (label_replace(sum by (a) (rate({e="p"}[5m])), "s", "$1", "a", "(.*)"))`, ""},
		{`label_replace(sum by (app) (count_over_time({app="a"}[5m])) "d", "$1", "app", "x")`, ""},
		{`label_replace(sum by (app) (count_over_time({app="a"}[5m])), "d", "$1", "app", 5)`, ""},
		{`label_replace(rate({app="a"}[5m]),,,,)`, ""},
		{`label_replace(rate({app="a"}[5m]), "d" "e", "$1", "app", "x")`, ""},
		{`label_replace(sum by (service_name) (rate({e="p"}[5m])), "s", "$1", "service_name", "(.*)")`, ""},
		{`label_replace(rate({a=~"x\\(y"}[5m]) + rate({b="z"}[5m]), "d", "$1", "app", "(.*)")`, ""},
		{`max by (service_name) (max_over_time({e="p"} | logfmt | unwrap latency_ms | __error__="" [1m]))`, "VectorAgg"},
		{`first_over_time({app="a"} | json duration_ms | drop pod | unwrap duration_ms [5m])`, "RangeAgg"},
		// An existence check after the parser under a resolved label: left to the translator under a rate or before
		// another stage, served as the Drilldown field breakdown.
		{`sum by (a_b) (rate({app="a"} | json | a_b != "" [5m]))`, ""},
		{`sum by (a_b) (bytes_rate({app="a"} | logfmt | a_b!=` + "``" + ` [5m]))`, ""},
		{`sum by (export_ms) (count_over_time({e="p"} | json | export_ms!="" | pipeline="x" [2m]))`, ""},
		{`sum by (a_b) (count_over_time({app="a"} | json | a_b != "" [5m]))`, "VectorAgg"},
		{`sum by (app) (rate({app="a"} | json | b != "" [5m]))`, "VectorAgg"},
		{`sum by (a_b) (rate({app="a"} | json | a_b != "x" [5m]))`, "VectorAgg"},
		// A filter on an _extracted label after the parser, and a json extraction expression the lowering does not
		// write.
		{`sum by (level_extracted) (count_over_time({app="a"} | json | level_extracted="error" [1m]))`, ""},
		{`sum by (a_b) (count_over_time({app="a"} | json a_b="[\"a.b\"]" [1m]))`, ""},
		{`sum by (a_b) (count_over_time({app="a"} | json a_b="a,b" [1m]))`, ""},
		{`sum by (a_b) (count_over_time({app="a"} | json a_b="a.b" [1m]))`, "VectorAgg"},
		{`sum by (app) (count_over_time({app="a"} | json | level_extracted="error" [1m]))`, "VectorAgg"},
		// Label filters, values and names the translator reads unlike Loki.
		{`sum(count_over_time({app="api"} |= "x" | json | status == 500 [5m]))`, ""},
		{`sum(count_over_time({app="api"} | logfmt | status = 500 [5m]))`, ""},
		{`sum(count_over_time({app="a"} | n > - .5 [5m]))`, ""},
		{`sum(count_over_time({app="a"} | n > +2 [5m]))`, ""},
		{`count_over_time({app="a"} | _time="x" [5m])`, ""},
		{`count_over_time({_STREAM="x"}[5m])`, ""},
		{`sum by (s) (rate({app="a"} | json | unwrap duration(_Stream) [5m]))`, ""},
		{"count_over_time({app=\"x\"} | s=`a detected_level b` [5m])", ""},
		{`count_over_time({app="x"} | s="service_name" [5m])`, ""},
		{`sum(count_over_time({app="x"} | o="\x02" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="\u0001" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="\001" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="\q" [5m]))`, ""},
		{"sum(count_over_time({app=\"x\"} | o=\"\x01\" [5m]))", ""},
		// Escapes that decode to another byte are read, and so are the plain ones.
		{`sum(count_over_time({app="x"} | o="\x41\U00000042\101\n\t\\\"" [5m]))`, "VectorAgg"},
		{`sum(count_over_time({app="x"} | o="\xZ1" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="\x4" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="\18" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="a\" [5m]))`, ""},
		{`sum(count_over_time({app="x"} | o="\u000A" [5m]))`, "VectorAgg"},
		// Spellings ScreenText leaves to the translator.
		{`(rate({app="x"}[5m]))`, ""},
		{`rate ({app="x"}[5m])`, ""},
		{`sum( rate({app="x"}[5m]))`, ""},
		{`sum by (zone) (max (rate({app="uw"}[30s])))`, ""},
		{`quantile_over_time(2.0, {app="a"} | json | unwrap d [5m])`, ""},
		{`quantile_over_time(.5, {app="a"} | json | unwrap d [5m])`, ""},
		{`quantile_over_time(0.50, {app="a"} | json | unwrap d [5m])`, ""},
		{`quantile_over_time(5e-1, {app="a"} | json | unwrap d [5m])`, ""},
		{`quantile_over_time(-0.1, {app="a"} | unwrap lat [5m])`, ""},
		// What the screen does not read.
		{`sum(count_over_time({app="a"}[1m])) # comment`, ""},
		{`sum(count_over_time({app="a"}[1m])`, ""},
		{`sum(count_over_time({app="a"}[1m])))`, ""},
		{`sum(count_over_time({app="a" [1m]))`, ""},
		{`sum(count_over_time({app="a"} |= "x [1m]))`, ""},
		{`count_over_time({app="a"}[1m]]`, ""},
		{`count_over_time({app="a"}}[1m])`, ""},
		{`sum(count_over_time({app="a"}[1m])) @ 5`, ""},
		{`-sum(count_over_time({app="a"}[1m]))`, ""},
		{``, ""},
		{`{app="a"}`, ""},
	} {
		got := ScreenMetric(tc.query)
		if got != tc.want {
			t.Errorf("ScreenMetric(%q) = %q, want %q", tc.query, got, tc.want)
			continue
		}
		if got == "" {
			continue
		}
		expr, err := logql.Parse(tc.query)
		if err != nil {
			t.Errorf("%q: %v", tc.query, err)
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Errorf("%q: %v", tc.query, err)
			continue
		}
		if c := Construct(p.Root); c != got {
			t.Errorf("%q: the screen names %s, the plan's root is %s", tc.query, got, c)
		}
	}
}

// The screen makes no allocation, whatever it reads.
//
// conformance: architecture-syntax-package-layering
func TestScreenMetricAllocatesNothing(t *testing.T) {
	for _, q := range []string{
		`sum by (app) (rate({app="api"} |= "x" [5m]))`,
		`label_replace(sum by (a) (rate({app="a"}[5m])) / sum by (a) (rate({app="a"}[5m])), "d", "$1", "a", "(.*)")`,
		`sum(count_over_time({app="x"} | o="\u0001" [5m]))`,
		`topk(3, sum by (app) (rate({app="api"}[5m])))`,
	} {
		if n := testing.AllocsPerRun(100, func() { _ = ScreenMetric(q) }); n != 0 {
			t.Errorf("ScreenMetric(%q): %v allocations", q, n)
		}
	}
}

// FuzzScreenMetric holds the screen, on any text, to an answer from its set, with no allocation; and where it names
// a construct for a query that parses and builds into a construct, to the plan's root.
func FuzzScreenMetric(f *testing.F) {
	for _, q := range []string{
		`sum by (app) (rate({app="api"} |= "x" [5m]))`, `count_over_time({app="a"} | json | n == 5 [5m])`,
		`label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`, `sum(rate({a="b"}[5m])) / 2`,
		`max(sum(rate({app="uw"} | logfmt [30s])) by (pod, zone)) by (zone)`, `quantile_over_time(0.9, {a="b"} | unwrap d [5m])`,
		`sum(count_over_time({app="x"} | o="\u0001" [5m]))`, `topk(2, rate({a="b"}[5m]))`, "rate({a=`b`}[5m])",
		`sum by (a_b) (rate({app="a"} | json | a_b != "" [5m]))`, `sum by (x_extracted) (count_over_time({a="b"} | logfmt | x_extracted="y" [1m]))`,
		`sum by (a_b) (count_over_time({app="a"} | json a_b="[\"a\"]" | a_b != "" [1m]))`,
		`max by (service_name) (max_over_time({e="p"} | json | unwrap duration_ms | __error__="" [1m]))`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 1024 {
			return
		}
		got := ScreenMetric(q)
		switch got {
		case "", "VectorAgg", "RangeAgg", "BinOp", "LabelReplace":
		default:
			t.Fatalf("ScreenMetric(%q) = %q", q, got)
		}
		if n := testing.AllocsPerRun(1, func() { _ = ScreenMetric(q) }); n != 0 {
			t.Fatalf("ScreenMetric(%q): %v allocations", q, n)
		}
		if got == "" {
			return
		}
		expr, err := logql.Parse(q)
		if err != nil {
			return
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			return
		}
		// A text the parser keeps whole (Opaque) or reads as a number is translated by the string translator; a plan of
		// another construct is a query the screen names wrongly.
		if c := Construct(p.Root); c != got && c != "Opaque" && c != "Scalar" {
			t.Fatalf("%q: the screen names %s, the plan's root is %s", q, got, c)
		}
	})
}

// ScreenMetricIn answers only a construct of the set it is given, and turns away, by the first word alone, a query
// whose construct cannot be one of them.
//
// conformance: architecture-syntax-package-layering
func TestScreenMetricIn(t *testing.T) {
	const (
		vec   = `sum by (app) (rate({app="a"}[5m]))`
		rng   = `rate({app="a"}[5m])`
		bin   = `sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`
		binN  = `2 * rate({app="a"}[5m])`
		lr    = `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`
		ranks = `topk(2, rate({app="a"}[5m]))`
	)
	all := MetricSetOf([]string{"LogSelect", "VectorAgg", "RangeAgg", "BinOp", "LabelReplace", "Rank"})
	if all != MetricVectorAgg|MetricRangeAgg|MetricBinOp|MetricLabelReplace || MetricSetOf([]string{"LogSelect"}) != 0 {
		t.Fatalf("MetricSetOf = %b", all)
	}
	for _, tc := range []struct {
		query string
		set   MetricSet
		want  string
	}{
		{vec, all, "VectorAgg"}, {vec, MetricVectorAgg, "VectorAgg"}, {vec, MetricRangeAgg | MetricBinOp, ""},
		{rng, all, "RangeAgg"}, {rng, MetricVectorAgg, ""}, {rng, MetricRangeAgg, "RangeAgg"},
		{bin, all, "BinOp"}, {bin, MetricVectorAgg, ""}, {bin, MetricBinOp, "BinOp"},
		{binN, MetricBinOp, "BinOp"}, {binN, MetricRangeAgg, ""},
		{lr, all, "LabelReplace"}, {lr, MetricVectorAgg | MetricRangeAgg | MetricBinOp, ""}, {lr, MetricLabelReplace, "LabelReplace"},
		{ranks, all, ""}, {vec, 0, ""}, {"  " + rng, MetricRangeAgg, "RangeAgg"},
	} {
		if got := ScreenMetricIn(tc.query, tc.set); got != tc.want {
			t.Errorf("ScreenMetricIn(%q, %b) = %q, want %q", tc.query, tc.set, got, tc.want)
		}
	}
}
