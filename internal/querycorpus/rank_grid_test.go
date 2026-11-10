package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// rankGrid is the generated grid of rankings: topk and bottomk with k in several spellings, sort and sort_desc, in
// every spelling of the name and of the by and without clause (both positions), over every range aggregation the
// lowering writes (with line filters, selectors, ranges, a parser, an unwrapped value with and without one), plus
// the operands that stay delegated (a vector aggregation, a ranking, a binary operation, label_replace, vector()),
// the clauses that stay delegated, the spellings the string translator reads unlike the plan (blanks, parentheses),
// quoted text its scans read as syntax, and label_replace around a ranking. Deterministic; the lowering and the
// string translator are held to each other on every row under every profile (TestRankGridShadow).
func rankGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	// rank wraps an operand in every ranking and clause the grid combines with it.
	rank := func(name, k, clause, operand string) {
		args := operand
		if k != "" {
			args = k + ", " + operand
		}
		if clause == "" {
			add(fmt.Sprintf(`%s(%s)`, name, args))
			return
		}
		add(fmt.Sprintf(`%s %s (%s)`, name, clause, args))
		add(fmt.Sprintf(`%s(%s) %s`, name, args, clause))
	}
	names := []struct{ name, k string }{{"topk", "3"}, {"bottomk", "2"}, {"sort", ""}, {"sort_desc", ""}}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	selectors := []string{`{app="a"}`, `{app=~".*"}`, `{service_name="s"}`, `{detected_level="error"}`, `{app!="a", env=~"p.*"}`, `{k8s.pod.name="p"}`, `{app="a",env="b",pod=~"x.+"}`}
	inner := func(f, sel, line, rng string) string { return fmt.Sprintf(`%s(%s%s%s)`, f, sel, line, rng) }

	// every ranking x every function on the plain selector, no clause.
	for _, n := range names {
		for _, f := range funcs {
			rank(n.name, n.k, "", inner(f, `{app="a"}`, ``, `[5m]`))
		}
	}
	// every selector, line filter and range, on each ranking.
	for _, sel := range selectors {
		for _, line := range []string{``, ` |= "x"`, ` != "x" |~ "y.+"`, ` |> "<_> x"`, ` | status >= 500`, ` | status="200" |= "k"`, ` |= "a, b"`} {
			for _, n := range names {
				for _, f := range funcs {
					rank(n.name, n.k, "", inner(f, sel, line, `[5m]`))
				}
			}
		}
	}
	for _, rng := range []string{`[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[1d]`, `[1ms]`, `[1h30m]`, `[2w]`, `[0s]`, `[1.5h]`, `[1y]`} {
		for _, n := range names {
			for _, f := range []string{"rate", "bytes_rate", "count_over_time"} {
				rank(n.name, n.k, "", inner(f, `{app="a"}`, ``, rng))
			}
		}
	}
	// k in several spellings (Loki reads a whole number; the others are parse errors on both sides).
	for _, k := range []string{"1", "3", "03", "10", "100", "1000000", "99999999999", "0", "3.0", "+3", "-1", "1.5", "0x3", "1e2", "k", `"3"`, ""} {
		for _, name := range []string{"topk", "bottomk"} {
			add(fmt.Sprintf(`%s(%s, rate({app="a"}[5m]))`, name, k))
			add(fmt.Sprintf(`%s (%s, rate({app="a"}[5m]))`, name, k))
		}
	}
	// every clause, in both positions, on each ranking, over each function.
	clauses := []string{
		`by ()`, `by (app)`, `by (app, env)`, `by (app, app)`, `by (service_name)`, `by (detected_level)`, `by (level)`, `by (k8s.pod.name)`, `by ( app )`,
		`without ()`, `without (app)`, `without (a, b)`, `without (service_name)`, `without (detected_level)`, `without ( app )`,
	}
	for _, c := range clauses {
		for _, n := range names {
			for _, f := range funcs {
				rank(n.name, n.k, c, inner(f, `{app="a"}`, ``, `[5m]`))
			}
			rank(n.name, n.k, c, inner("rate", `{app="a"}`, ` |= "x"`, `[5m]`))
		}
	}
	// name spellings, case and blanks.
	op := `rate({app="a"}[5m])`
	for _, q := range []string{
		`topk(3, ` + op + `)`, `TOPK(3, ` + op + `)`, `Topk(3, ` + op + `)`, `topk (3, ` + op + `)`, `topk( 3, ` + op + ` )`, `topk(3 , ` + op + `)`, `topk(3,` + op + `)`,
		"topk(3,\n" + op + ")", "topk(\t3, " + op + ")", `topk(3,   ` + op + `)`, ` topk(3, ` + op + `) `, `bottomk (2, ` + op + `)`, `BOTTOMK(2, ` + op + `)`,
		`sort(` + op + `)`, `SORT(` + op + `)`, `Sort(` + op + `)`, `sort (` + op + `)`, `sort( ` + op + ` )`, `sort(` + op + ` )`, `sort( ` + op + `)`, "sort(\n" + op + "\n)",
		`sort_desc(` + op + `)`, `SORT_DESC(` + op + `)`, `sort_desc (` + op + `)`, `sort_desc( ` + op + ` )`, " sort_desc(" + op + ") ",
		`TOPK BY (app) (3, ` + op + `)`, `topk BY (app) (3, ` + op + `)`, `topk(3, ` + op + `) BY (app)`, `topk by(app)(3, ` + op + `)`, `topk(3, ` + op + `)by(app)`,
		`topk(3, RATE({app="a"}[5m]))`, `topk(3, rate ({app="a"}[5m]))`, `topk(3, rate({app="a"} [5m]))`, `topk(3, rate({app="a"}[ 5m ]))`,
		`topk(3, (` + op + `))`, `topk(3, ( ` + op + ` ))`, `topk(3, ((` + op + `)))`, `(topk(3, ` + op + `))`, `( topk(3, ` + op + `) )`, `((sort(` + op + `)))`,
		`sort((` + op + `))`, `sort_desc((` + op + `))`, `(sort_desc(` + op + `))`,
	} {
		add(q)
	}
	// parsers and unwrapped values: with a parser the ranking writes the operand's text, without one it groups by the
	// stream (delegated); both sides of the condition, every unwrap function, the conversions and a range-level by.
	unwrapFuncs := []string{"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time", "rate_counter", "rate", "bytes_rate"}
	stagesets := []string{
		``, ` | json`, ` | logfmt`, ` | json | level="error"`, ` | logfmt | x="y"`, ` | drop a`, ` | keep a`, ` | json | drop a`, ` | line_format "{{.x}}"`,
		` | regexp "(?P<a>x)"`, ` | pattern "<a> <b>"`, ` | unpack`, ` |= "| json"`, ` |= "| logfmt"`, ` |= "| unwrap x"`, ` |~ "| regexp"`,
		` | label_format a=b`, ` | json | label_format a=b`, ` | status >= 500`,
	}
	for _, n := range names {
		for _, f := range unwrapFuncs {
			for _, st := range stagesets {
				rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"}%s | unwrap amount [5m])`, f, st))
			}
			for _, u := range []string{`duration(t)`, `duration_seconds(t)`, `bytes(t)`, `x`} {
				rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"} | json | unwrap %s [5m])`, f, u))
				rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"} | unwrap %s [5m])`, f, u))
			}
			rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"} | json | unwrap amount [5m]) by (pod)`, f))
			rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"} | unwrap amount [5m]) by (pod)`, f))
			rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"} | json | unwrap amount [5m]) by ()`, f))
			rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"} | unwrap amount [5m]) without (pod)`, f))
		}
		for _, p := range []string{"0.5", "0.95", "1", "0", "0.50", "5e-1"} {
			rank(n.name, n.k, "", fmt.Sprintf(`quantile_over_time(%s, {app="a"} | json | unwrap amount [5m])`, p))
			rank(n.name, n.k, "", fmt.Sprintf(`quantile_over_time(%s, {app="a"} | unwrap amount [5m])`, p))
		}
		for _, st := range stagesets {
			for _, f := range funcs {
				rank(n.name, n.k, "", fmt.Sprintf(`%s({app="a"}%s[5m])`, f, st))
			}
		}
		// the stages the operand lowering leaves out, and the unwrap spelling the translator reads out of the text.
		for _, spell := range []string{`|unwrap amount`, `| unwrap  amount`, `|  unwrap amount`, `| unwrap amount `, `| unwrap amount|json`} {
			rank(n.name, n.k, "", fmt.Sprintf(`sum_over_time({app="a"} | json %s [5m])`, spell))
		}
	}
	// operands that stay delegated: a vector aggregation, a ranking, a binary operation, label_replace, vector(), a
	// number, a log query, stddev.
	for _, operand := range []string{
		`sum by (app) (rate({app="a"}[5m]))`, `sum(rate({app="a"}[5m])) by (app)`, `sum without (app) (rate({app="a"}[5m]))`, `count by (app) (count_over_time({app="a"}[5m]))`,
		`stddev by (app) (rate({app="a"}[5m]))`, `topk(2, rate({app="a"}[5m]))`, `sort(rate({app="a"}[5m]))`, `bottomk by (app) (2, sum by (app, env) (rate({app="a"}[5m])))`,
		`sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`, `rate({app="a"}[5m]) > 1`, `rate({app="a"}[5m]) * 2`,
		`label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)")`, `vector(1)`, `1`, `sum by (app) (sum_over_time({app="a"} | unwrap n [5m]))`,
		`sum by (service_name) (rate({app="a"}[5m]))`,
	} {
		for _, n := range names {
			rank(n.name, n.k, "", operand)
			rank(n.name, n.k, `by (app)`, operand)
		}
	}
	// the stages the log query lowering leaves out.
	for _, st := range []string{` | detected_level="error"`, ` | detected_level=~"error|warn"`, ` | service_name="s"`, ` | label_format a=b`, ` | line_format "{{.x}}"`,
		` | json | status=5`, ` | decolorize`, ` | logfmt --strict`, ` | json a="b"`} {
		for _, n := range names {
			rank(n.name, n.k, "", fmt.Sprintf(`rate({app="a"}%s[5m])`, st))
			rank(n.name, n.k, "", fmt.Sprintf(`count_over_time({app="a"}%s[5m])`, st))
		}
	}
	// text the translator's scans read as syntax, in the operand and in the selector.
	for _, line := range []string{
		` |= "without (x)"`, ` |= "by (x)"`, ` |= "| stats by (x)"`, ` |= "| stats "`, ` |= "service_name"`, ` |= "detected_level"`, ` |= "a bool b"`, ` |~ "|||"`, ` |= "a|||b"`,
		` |= "topk(3, rate(x))"`, ` |= "sum by (x) (rate(y))"`, ` |= "__without__:sum:app"`, ` |= "__lvp_lr:x__"`, ` |= "@@@on:x"`, ` |= "__binary__:+:"`,
		` |= "("`, ` |= ")"`, ` |= "[5m]"`, ` |= "["`, ` |= "]"`, ` |= ","`, ` |= "3, "`, " |= `without (z)`", ` |= "sort("`, ` |= "label_replace("`, ` |~ "without\\s*\\((a|b)\\)"`,
	} {
		for _, n := range names {
			for _, f := range []string{"count_over_time", "rate", "absent_over_time"} {
				rank(n.name, n.k, "", inner(f, `{app="a"}`, line, `[5m]`))
				rank(n.name, n.k, `by (app)`, inner(f, `{app="a"}`, line, `[5m]`))
				rank(n.name, n.k, `without (app)`, inner(f, `{app="a"}`, line, `[5m]`))
			}
		}
	}
	for _, sel := range []string{`{app="topk(1, x)"}`, `{app="a, b"}`, `{app="(("}`, `{app=~"a|b"}`, `{app="without (x)"}`, `{a="sort(x)"}`} {
		for _, n := range names {
			rank(n.name, n.k, "", inner("rate", sel, ``, `[5m]`))
			rank(n.name, n.k, "", inner("count_over_time", sel, ``, `[5m]`))
		}
	}
	// label_replace around a ranking (stays delegated: the proxy does not route it as a ranking), a ranking around
	// label_replace, and a ranking inside a binary operation.
	arg := `"d", "$1", "app", "(.*)"`
	for _, n := range names {
		for _, f := range funcs {
			rankOp := inner(f, `{app="a"}`, ``, `[5m]`)
			head := n.name + "("
			if n.k != "" {
				head += n.k + ", "
			}
			add(fmt.Sprintf(`label_replace(%s%s), %s)`, head, rankOp, arg))
			add(fmt.Sprintf(`label_replace(label_replace(%s%s), %s), "e", "$1", "d", "(.*)")`, head, rankOp, arg))
			add(fmt.Sprintf(`%slabel_replace(%s, %s))`, head, rankOp, arg))
			add(fmt.Sprintf(`%s%s) + 1`, head, rankOp))
			add(fmt.Sprintf(`1 / %s%s)`, head, rankOp))
			add(fmt.Sprintf(`%s%s) / %s%s)`, head, rankOp, head, rankOp))
			add(fmt.Sprintf(`sum(%s%s))`, head, rankOp))
			add(fmt.Sprintf(`sum by (app) (%s%s))`, head, rankOp))
			add(fmt.Sprintf(`%s%s%s)))`, head, head, rankOp))
		}
	}
	return out
}

// TestRankGridShadow holds the lowering of rankings to the string translator on the generated grid under every
// profile: 0 mismatches, the text byte for byte, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestRankGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := rankGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 3000))
	}
	if native < rows/8 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
