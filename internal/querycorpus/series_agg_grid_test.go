package querycorpus_test

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// seriesAggGrid is the generated grid of stddev and stdvar: both operators in every spelling of the by and without
// clause (both positions), over every kind of operand (range aggregations with parsers, label filters and unwrapped
// values, vector aggregations and nests of them, label_replace around either, rankings, stddev), with the layers
// sum, avg, min, max and count above them, rankings around them, and the spellings the proxy's route reads (case,
// blanks, newlines, comments, parentheses, quoted text). Deterministic. Every row the lowering answers is held to
// the proxy's route (proxy.PostAggregationPhysical) where the proxy routes it as one evaluation, and recorded as a
// known divergence where it sends the translator's text, which drops a level.
func seriesAggGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	// fold writes a stddev or stdvar of operand in the spelling of clause: before the arguments, after them, or none.
	fold := func(op, clause, operand string) {
		if clause == "" {
			add(fmt.Sprintf(`%s(%s)`, op, operand))
			return
		}
		add(fmt.Sprintf(`%s %s (%s)`, op, clause, operand))
		add(fmt.Sprintf(`%s(%s) %s`, op, operand, clause))
	}
	ops := []string{"stddev", "stdvar"}
	sel := `{app="a"}`
	ranges, vectors := gridRanges(sel), gridVectors()
	lr := func(in string) string { return `label_replace(` + in + `, "d", "$1", "app", "(.*)")` }
	operands := append([]string(nil), ranges...)
	for _, v := range vectors {
		for _, r := range ranges[:6] {
			operands = append(operands, fmt.Sprintf(v, r))
		}
		for _, r := range []string{ranges[6], ranges[9], ranges[16], ranges[17]} {
			operands = append(operands, fmt.Sprintf(v, r))
		}
	}
	for _, base := range []string{ranges[0], ranges[3], `sum by (app) (` + ranges[0] + `)`, `max without (pod) (` + ranges[3] + `)`, `sum by (service_name) (` + ranges[0] + `)`} {
		operands = append(operands, lr(base), lr(lr(base)))
	}
	clauses := []string{``, `by ()`, `by (app)`, `by (app, env)`, `by (app, app)`, `by (service_name)`, `by (k8s.pod.name)`, `by (route)`, `without ()`, `without (pod)`,
		`without (a, b)`, `without (detected_level)`, `by ( app )`, `by (bool)`, `by (detected_level)`}
	// every operator x every operand, without a clause.
	for _, op := range ops {
		for _, o := range operands {
			fold(op, ``, o)
		}
	}
	// every clause on each operator over a spread of operands, the parsed ones among them.
	for _, c := range clauses {
		for _, op := range ops {
			for _, o := range []string{ranges[0], ranges[3], ranges[6], ranges[7], ranges[16], ranges[17], ranges[18], fmt.Sprintf(vectors[1], ranges[0]), fmt.Sprintf(vectors[2], ranges[3]),
				fmt.Sprintf(vectors[1], ranges[6]), lr(ranges[0]), `sum by (app, route) (` + ranges[7] + `)`} {
				fold(op, c, o)
			}
		}
	}
	// the layers above (the proxy does not take a stddev under them), the rankings around, and a stddev under a stddev.
	inner := []string{ranges[0], ranges[3], fmt.Sprintf(vectors[1], ranges[0]), ranges[6], ranges[16]}
	for _, in := range inner {
		for _, op := range ops {
			for _, c := range []string{``, `by (app)`, `without (pod)`, `by ()`} {
				var s string
				if c == `` {
					s = fmt.Sprintf(`%s(%s)`, op, in)
				} else {
					s = fmt.Sprintf(`%s %s (%s)`, op, c, in)
				}
				for _, layer := range []string{
					`sum(%s)`, `sum by (app) (%s)`, `avg without (pod) (%s)`, `max(%s) by (app)`, `min by () (%s)`, `count(%s)`, `count by (app) (%s)`,
					`stddev(%s)`, `stdvar by (app) (%s)`, `topk(2, %s)`, `bottomk by (app) (1, %s)`, `topk(2, %s) by (app)`, `sort(%s)`, `sort_desc(%s)`,
					`label_replace(%s, "d", "$1", "app", "(.*)")`, `sum by (app) (max(%s))`, `topk(1, sum(%s))`, `sum(topk(1, %s))`, `(%s)`, `sum((%s))`,
					`%s / 2`, `sum(%s) / 2`, `sum(%s / 2)`, `2 * %s`, `%s > 1`,
				} {
					add(fmt.Sprintf(layer, s))
				}
			}
		}
	}
	// a ranking under a stddev, and the layers between: the operand text the route translates drops the ranking.
	for _, rk := range []string{`topk(3, ` + ranges[3] + `)`, `bottomk by (app) (2, ` + fmt.Sprintf(vectors[1], ranges[0]) + `)`, `sort(` + ranges[0] + `)`, `sort_desc(` + ranges[6] + `)`,
		`sum(topk(2, ` + fmt.Sprintf(vectors[1], ranges[0]) + `))`, `topk(2, sort_desc(` + ranges[3] + `))`} {
		for _, op := range ops {
			fold(op, ``, rk)
			fold(op, `by (app)`, rk)
			fold(op, `without (pod)`, rk)
			add(fmt.Sprintf(`sum(%s(%s))`, op, rk))
			add(fmt.Sprintf(`topk(2, %s by (app) (%s))`, op, rk))
			add(fmt.Sprintf(`%s(sum by (app) (%s))`, op, rk))
		}
	}
	// the spellings the proxy's route reads: case, blanks, newlines, comments, parser flags, parentheses.
	op := ranges[3]
	for _, q := range []string{
		`STDDEV(` + op + `)`, `Stdvar by (app) (` + op + `)`, `stddev (` + op + `)`, `stddev( ` + op + ` )`, `stddev by(app)(` + op + `)`, `stddev by (app)(` + op + `)by(app)`,
		"stddev(\n" + op + "\n)", "stddev\t(" + op + ")", ` stddev(` + op + `) `, `STDVAR BY (app) (` + op + `)`, `stddev(` + op + `)by(app)`, `stddev(` + op + `) BY (app)`,
		`(stddev(` + op + `))`, `( stddev by (app) (` + op + `) )`, `((stdvar(` + op + `)))`, `stddev((` + op + `))`, `stddev( (` + op + `) )`, `stddev(((` + op + `)))`,
		`stddev(RATE({app="a"}[5m]))`, `stddev(rate ({app="a"}[5m]))`, `stddev(rate({app="a"} [5m]))`, `stddev(sum( ` + op + `))`, `stddev(sum (` + op + `))`,
		`stddev(sum by (app) ( ` + op + `))`, `stddev(sum by (app) ((` + op + `)))`, `stddev((sum by (app) (` + op + `)))`, `stddev by (app) ( sum by (app, pod) (` + op + `) )`,
		`# a comment` + "\n" + `stddev(` + op + `)`, `stddev(` + op + `) # trailing`, `stddev(count_over_time({app="a"} | logfmt --strict [5m]))`,
		`stddev by (app) (count_over_time({app="a"} | logfmt --keep-empty | json [5m]))`, `stddev(sum(stddev(` + op + `)))`, `stddev(sum(topk(2, ` + op + `)))`,
		`sum(stddev(` + op + `))`, `sum( stddev(` + op + `))`, `sum (stddev(` + op + `))`, `sum((stddev(` + op + `)))`, `(sum(stddev(` + op + `)))`,
		`topk(2, STDDEV by (app) (` + op + `))`, `topk(2, (stddev by (app) (` + op + `)))`, `(topk(2, stddev(` + op + `)))`, `sort_desc( stddev(` + op + `) )`,
	} {
		add(q)
	}
	// quoted text the translator's and the route's scans read as syntax, in the operand and in the selector. A `|
	// json` inside a line filter is read as a parser by the route's rewrite of a stddev by a label.
	for _, line := range []string{
		` |= "without (x)"`, ` |= "by (x)"`, ` |= "| stats by (x)"`, ` |= "service_name"`, ` |= "detected_level"`, ` |= "a bool b"`, ` |~ "|||"`,
		` |= "stddev(rate(x))"`, ` |= "sum by (x) (rate(y))"`, ` |= "__without__:sum:app"`, ` |= "__lvp_lr:x__"`, ` |= "@@@on:x"`, ` |= "__binary__:+:"`,
		` |= "("`, ` |= ")"`, ` |= ","`, " |= `without (z)`", ` |= "stddev("`, ` |= "label_replace("`, ` |= "\\""`, ` |= "a\\"b, c"`,
		` |= "| json"`, ` |= "|logfmt"`, ` != "| pattern"`, ` |= "| regexp x"`, ` |= "| unpack"`, ` |= "x | jsonx"`, ` |= "| line_format"`,
	} {
		for _, o := range ops {
			for _, f := range []string{"count_over_time", "rate"} {
				body := f + `({app="a"}` + line + `[5m])`
				for _, c := range []string{``, `by (app)`, `without (pod)`} {
					fold(o, c, body)
					fold(o, c, `sum by (app) (`+body+`)`)
				}
			}
		}
	}
	for _, s := range []string{`{app="stddev(1, x)"}`, `{app="a, b"}`, `{app="(("}`, `{app=~"a|b"}`, `{app="without (x)"}`, `{a="stdvar(x)"}`, `{a=")"}`, `{a="| json"}`} {
		for _, o := range ops {
			fold(o, ``, `rate(`+s+`[5m])`)
			fold(o, `by (app)`, `sum(rate(`+s+`[5m]))`)
		}
	}
	// operands that stay with the string translator: a binary operation, vector(), a number, a log query.
	for _, operand := range []string{
		`sum(` + ranges[3] + `) / sum(` + ranges[0] + `)`, ranges[3] + ` > 1`, ranges[3] + ` * 2`, `vector(1)`, `1`, `2 + 3`,
		`label_replace(` + ranges[3] + ` > 1, "d", "$1", "app", "(.*)")`, `sum(` + ranges[3] + `) > bool 1`, `approx_topk(3, ` + ranges[3] + `)`, `{app="a"}`, `{app="a"} |= "x"`,
	} {
		for _, o := range ops {
			fold(o, ``, operand)
			fold(o, `by (app)`, operand)
		}
	}
	// label_replace around a stddev, a stddev inside a label_replace, and a stddev inside a binary operation.
	arg := `"d", "$1", "app", "(.*)"`
	for _, o := range ops {
		for _, in := range []string{ranges[3], fmt.Sprintf(vectors[1], ranges[0]), ranges[16], ranges[6]} {
			head := o + "("
			add(fmt.Sprintf(`label_replace(%s%s), %s)`, head, in, arg))
			add(fmt.Sprintf(`label_replace(%s by (app) (%s), %s)`, o, in, arg))
			add(fmt.Sprintf(`label_replace(label_replace(%s%s), %s), "e", "$1", "d", "(.*)")`, head, in, arg))
			add(fmt.Sprintf(`%slabel_replace(%s, %s))`, head, in, arg))
			add(fmt.Sprintf(`%s%s) + 1`, head, in))
			add(fmt.Sprintf(`%s%s) / %s%s)`, head, in, head, in))
			add(fmt.Sprintf(`sum(%s%s))`, head, in))
			add(fmt.Sprintf(`sum(%s%s)) / 2`, head, in))
			add(fmt.Sprintf(`%s%s%s)))`, head, head, in))
		}
	}
	for _, q := range parsedLabelFoldGrid() {
		add(q)
	}
	return out
}

// parsedLabelFoldGrid is the part of the grid where the route rewrites the operand of a stddev or stdvar by labels
// of a query with a parser (the series-level max by form, cut): every range function over json, logfmt, regexp,
// pattern and unpack (with flags, extraction lists, label filters, drops, keeps and formats after them), nests of
// sum, avg, min, max and count over them, groupings by parsed, stream, derived and dotted labels and by names that
// differ from _time, _stream or _stream_id only by case, both clause positions; and the shapes the plan leaves to
// the translator (an unwrapped value, a label_replace, a ranking, a quoted parser, a fold under another level).
func parsedLabelFoldGrid() []string {
	var out []string
	sel := `{app="a"}`
	parsers := []string{
		`| json`, `| logfmt`, `| regexp "(?P<route>[a-z]+) (?P<code>\\d+)"`, `| pattern "<route> <code> <_>"`, `| unpack`,
		`| json route="r.path", code`, `| logfmt --strict`, `| logfmt route, code`, `| json | status >= 500`,
		`| logfmt | level="error"`, `| json | drop pod`, `| logfmt | keep route, code`, `| json | line_format "{{.route}}"`,
		`| logfmt | route=~"/a.*"`, `| json | label_format r="{{.route}}"`, `| logfmt | json`, `|= "x" | logfmt`, `| decolorize | logfmt`,
	}
	funcs := []string{`count_over_time(%s [5m])`, `rate(%s [1m])`, `bytes_over_time(%s [30s])`, `bytes_rate(%s [5m] offset 1h)`, `absent_over_time(%s [5m])`}
	clauses := []string{`by (route)`, `by (app, route)`, `by (route, code)`, `by (level)`, `by (detected_level)`, `by (service_name)`, `by (k8s.pod.name)`,
		`by (route, route)`, `by (_Time)`, `by (_STREAM)`, `by (_stream_ID)`, `by (pod)`}
	for _, op := range []string{"stddev", "stdvar"} {
		for _, pr := range parsers {
			for _, f := range funcs {
				in := fmt.Sprintf(f, sel+" "+pr)
				for _, c := range clauses[:4] {
					out = append(out, fmt.Sprintf(`%s %s (%s)`, op, c, in), fmt.Sprintf(`%s(%s) %s`, op, in, c))
				}
			}
		}
		for _, pr := range parsers[:5] {
			for _, c := range clauses {
				for _, f := range funcs[:2] {
					out = append(out, fmt.Sprintf(`%s %s (%s)`, op, c, fmt.Sprintf(f, sel+" "+pr)))
				}
				for _, v := range []string{`sum by (route, pod) (%s)`, `sum(%s)`, `sum by () (%s)`, `avg by (route) (%s)`, `max by (app, route) (%s)`, `count by (route) (%s)`,
					`min without (pod) (%s)`, `sum by (route) (sum by (route, pod) (%s))`, `sum(sum by (route) (%s))`} {
					for _, f := range []string{funcs[0], funcs[1], funcs[3]} {
						out = append(out, fmt.Sprintf(`%s %s (%s)`, op, c, fmt.Sprintf(v, fmt.Sprintf(f, sel+" "+pr))))
					}
				}
			}
		}
		// Left to the translator: an unwrapped value, a label_replace, a ranking, a binary operation, a quoted parser, a
		// fold under another level, and a without clause (which the route does not rewrite).
		for _, in := range []string{
			`last_over_time({app="p"} | logfmt | unwrap amount [5m])`, `sum_over_time(` + sel + ` | json | unwrap n [5m])`,
			`label_replace(count_over_time(` + sel + ` | json [5m]), "d", "$1", "route", "(.*)")`, `topk(2, count_over_time(` + sel + ` | json [5m]))`,
			`count_over_time(` + sel + ` | json [5m]) / 2`, `count_over_time(` + sel + ` |= "| json" [5m])`, `rate(` + sel + ` != "| logfmt" [5m])`,
			`count_over_time(` + sel + ` |= "| json" | logfmt [5m])`, `sum by (route) (rate(` + sel + ` | logfmt | unwrap n [5m]))`,
		} {
			out = append(out, fmt.Sprintf(`%s by (route) (%s)`, op, in), fmt.Sprintf(`%s without (pod) (%s)`, op, in))
		}
		for _, layer := range []string{`sum(%s)`, `topk(2, %s)`, `%s / 2`, `label_replace(%s, "d", "$1", "route", "(.*)")`, `stddev(%s)`} {
			out = append(out, fmt.Sprintf(layer, op+` by (route) (count_over_time(`+sel+` | json [5m]))`))
		}
	}
	return out
}

// TestSeriesAggGridShadow holds the lowering of stddev and stdvar (and the layers around them) to the proxy's route
// on the generated grid under every profile: the operators the route builds (a SeriesAgg per level over the
// operand's operators) and the operand's request byte for byte, with 0 mismatches. The shapes the proxy sends as the
// translator's text, which drops a level, are counted as known divergences, and the clause by a parsed label as
// delegations; the rows the lowering leaves to the string translator are counted by construct.
// conformance: architecture-syntax-package-layering
func TestSeriesAggGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := seriesAggGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the proxy's route:\n%s", len(problems), joinFirst(problems, 20))
	}
	if native < rows/4 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
	for _, want := range []string{"known divergence " + knownNestedEvaluation, "VectorAgg stddev parsed label", "SeriesAgg operand BinOp", "SeriesAgg spelling"} {
		if delegated[want] == 0 {
			t.Errorf("no row delegated as %q: the grid does not reach it", want)
		}
	}
}

// The registry case semantics/stddev-stdvar-nested-or-binary lists the shapes of its difference in
// corpus.dimensions.shapes, which become corpus rows: the shadows and the route-parity digests see them. Ten are a
// level the proxy sends as the translator's text (lowered to the chain Loki evaluates, a known divergence of the
// shadow), two are the controls the proxy answers right (a stddev routed as one evaluation, a ranking), and three
// have a binary operation at the top (a stddev under it is a request of its own; the plan leaves it to the
// translator).
// conformance: semantics/stddev-stdvar-nested-or-binary
func TestNestedEvaluationCaseRowsAreInTheCorpus(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	raw, err := os.ReadFile("../../conformance/registry/cases/semantics/stddev-stdvar-nested-or-binary.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Corpus struct {
			Dimensions struct {
				Shapes []struct {
					Query string `yaml:"query"`
				} `yaml:"shapes"`
			} `yaml:"dimensions"`
		} `yaml:"corpus"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	inCorpus := map[string]bool{}
	for _, q := range planCorpus(t) {
		inCorpus[q] = true
	}
	prof := planProfiles()[2].lowerProfile()
	nested, routedRows, other := 0, 0, 0
	for _, s := range doc.Corpus.Dimensions.Shapes {
		q := s.Query
		if !inCorpus[q] {
			t.Errorf("%q is not a plan corpus row", q)
			continue
		}
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		queryplan.Resolve(plan)
		routed, divergence := evaluatedRoot(plan)
		_, lerr := lower.PlanQuery(q, plan, prof)
		switch {
		case divergence == knownNestedEvaluation:
			nested++
			if lerr != nil {
				t.Errorf("%q: a known divergence must lower to the chain Loki evaluates: %v", q, lerr)
			}
		case routed:
			routedRows++
			if lerr != nil {
				t.Errorf("%q: the control is routed as one evaluation and must lower: %v", q, lerr)
			}
		default:
			other++
		}
	}
	if nested != 10 || routedRows != 2 || other != 3 {
		t.Fatalf("the case lists %d nested, %d routed and %d other rows, want 10, 2 and 3", nested, routedRows, other)
	}
}
