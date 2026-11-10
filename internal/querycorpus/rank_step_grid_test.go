package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// rankStepGrid is the generated grid of rankings the proxy evaluates: topk and bottomk in every spelling of k and of
// the by and without clause (both positions), sort and sort_desc, over every kind of operand (range aggregations with
// parsers, label filters and unwrapped values, vector aggregations and nests of them, label_replace around either,
// rankings, and the layers sum, avg, min, max and count between and above rankings), in the spellings the proxy's
// route reads (blanks, case, parentheses), and the operands and layers that stay with the string translator.
// Deterministic. Every row is held to the proxy's route (proxy.PostAggregationPhysical) when the lowering answers it.
func rankStepGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	type name struct{ name, k string }
	names := []name{{"topk", "3"}, {"bottomk", "1"}, {"sort", ""}, {"sort_desc", ""}}
	// rank writes a ranking of operand in the spelling of clause: before the arguments, after them, or none.
	rank := func(n name, clause, operand string) {
		args := operand
		if n.k != "" {
			args = n.k + ", " + operand
		}
		if clause == "" {
			add(fmt.Sprintf(`%s(%s)`, n.name, args))
			return
		}
		add(fmt.Sprintf(`%s %s (%s)`, n.name, clause, args))
		add(fmt.Sprintf(`%s(%s) %s`, n.name, args, clause))
	}
	sel := `{app="a"}`
	ranges := []string{
		`count_over_time(` + sel + `[5m])`, `bytes_over_time(` + sel + ` |= "x" [1h])`, `absent_over_time(` + sel + `[5m])`,
		`rate(` + sel + ` != "y" [1m])`, `bytes_rate({app=~"a.*", env!="p"}[30s] offset 1h)`, `rate({service_name="s"}[5m])`,
		`count_over_time(` + sel + ` | json [5m])`, `rate(` + sel + ` | logfmt [5m])`, `count_over_time(` + sel + ` | json | status >= 500 [5m])`,
		`count_over_time(` + sel + ` | logfmt | level="error" [5m])`, `rate(` + sel + ` | json | drop a [5m])`,
		`count_over_time(` + sel + ` | regexp "(?P<a>x)" [5m])`, `count_over_time(` + sel + ` | pattern "<a> <b>" [5m])`, `count_over_time(` + sel + ` | keep a [5m])`,
		`count_over_time(` + sel + ` | detected_level="error" [5m])`, `rate({app="a"}[0s])`,
		`sum_over_time(` + sel + ` | unwrap amount [5m])`, `last_over_time(` + sel + ` | json | unwrap amount [5m])`,
		`avg_over_time(` + sel + ` | logfmt | unwrap duration(t) [5m]) by (pod)`, `quantile_over_time(0.9, ` + sel + ` | json | unwrap amount [5m])`,
		`rate_counter(` + sel + ` | unwrap amount [5m])`, `max_over_time(` + sel + ` | unwrap bytes(b) [5m]) without (pod)`,
		`first_over_time(` + sel + ` | json | unwrap x [5m] offset 5m)`,
	}
	vectors := []string{
		`sum(%s)`, `sum by (app) (%s)`, `sum without (pod) (%s)`, `sum(%s) by (app, env)`, `avg by (app) (%s)`, `min without (pod, env) (%s)`,
		`max by (app) (%s)`, `count by (app) (%s)`, `count(%s)`, `sum by () (%s)`, `max by (service_name) (%s)`, `sum by (detected_level) (%s)`,
		`sum by (app) (sum by (app, pod) (%s))`, `max by (app) (min without (pod) (%s))`, `avg(sum by (app) (%s))`, `count without (pod) (sum by (app, pod) (%s))`,
	}
	operands := append([]string(nil), ranges...)
	for _, v := range vectors {
		for _, r := range ranges[:6] {
			operands = append(operands, fmt.Sprintf(v, r))
		}
		for _, r := range []string{ranges[6], ranges[9], ranges[16], ranges[17]} {
			operands = append(operands, fmt.Sprintf(v, r))
		}
	}
	lr := func(in string) string { return `label_replace(` + in + `, "d", "$1", "app", "(.*)")` }
	for _, base := range []string{ranges[0], ranges[3], `sum by (app) (` + ranges[0] + `)`, `max without (pod) (` + ranges[3] + `)`, `sum by (service_name) (` + ranges[0] + `)`} {
		operands = append(operands, lr(base), lr(lr(base)))
	}
	clauses := []string{``, `by ()`, `by (app)`, `by (app, env)`, `by (app, app)`, `by (service_name)`, `by (k8s.pod.name)`, `without ()`, `without (pod)`, `without (a, b)`, `without (detected_level)`, `by ( app )`}
	// every ranking x every operand, without a clause.
	for _, n := range names {
		for _, o := range operands {
			rank(n, ``, o)
		}
	}
	// every clause on each ranking over a spread of operands.
	for _, c := range clauses {
		for _, n := range names {
			for _, o := range []string{ranges[0], ranges[3], ranges[6], ranges[16], vectors[1][:0] + fmt.Sprintf(vectors[1], ranges[0]), fmt.Sprintf(vectors[2], ranges[3]), lr(ranges[0])} {
				rank(n, c, o)
			}
		}
	}
	// k in several spellings: Loki reads a whole number, the others are parse errors on both sides.
	for _, k := range []string{"1", "3", "03", "10", "100", "1000000", "99999999999", "0", "3.0", "+3", "-1", "1.5", "0x3", "1e2", "k", `"3"`, ""} {
		for _, nm := range []string{"topk", "bottomk"} {
			for _, o := range []string{ranges[0], fmt.Sprintf(vectors[1], ranges[0])} {
				add(fmt.Sprintf(`%s(%s, %s)`, nm, k, o))
				add(fmt.Sprintf(`%s (%s, %s)`, nm, k, o))
				add(fmt.Sprintf(`%s by (app) (%s, %s)`, nm, k, o))
			}
		}
	}
	// rankings over rankings, and the layers between and above them.
	inner := []string{fmt.Sprintf(vectors[1], ranges[0]), ranges[3], ranges[16], ranges[6]}
	for _, o := range inner {
		for _, a := range names {
			for _, b := range names {
				for _, c := range []string{``, `by (app)`, `without (pod)`} {
					var in string
					rank2 := func(x name, clause, operand string) string {
						args := operand
						if x.k != "" {
							args = x.k + ", " + operand
						}
						if clause == "" || x.name == "sort" || x.name == "sort_desc" {
							return fmt.Sprintf(`%s(%s)`, x.name, args)
						}
						return fmt.Sprintf(`%s %s (%s)`, x.name, clause, args)
					}
					in = rank2(b, c, o)
					add(rank2(a, c, in))
					add(rank2(a, ``, in))
				}
			}
		}
	}
	rk := fmt.Sprintf(`topk(2, %s)`, fmt.Sprintf(vectors[1], ranges[0]))
	for _, layer := range []string{
		`sum(%s)`, `sum by (app) (%s)`, `avg without (pod) (%s)`, `max(%s) by (app)`, `min by () (%s)`, `count(%s)`, `count by (app) (%s)`,
		`stddev(%s)`, `stdvar by (app) (%s)`, `topk(2, %s)`, `bottomk by (app) (1, %s)`, `sort(%s)`, `sort_desc(%s)`, `label_replace(%s, "d", "$1", "app", "(.*)")`,
		`sum by (app) (max(%s))`, `topk(1, sum(%s))`, `sum(topk(1, %s))`, `(%s)`, `sum((%s))`, `sum( %s)`, `sum (%s)`,
	} {
		for _, in := range []string{rk, `sort_desc(` + fmt.Sprintf(vectors[1], ranges[0]) + `)`, `bottomk(3, ` + ranges[3] + `)`} {
			add(fmt.Sprintf(layer, in))
			add(fmt.Sprintf(layer, fmt.Sprintf(`sum by (app) (%s)`, in)))
		}
	}
	// a stddev or stdvar between rankings, or above one, is not routed as a ranking.
	for _, q := range []string{
		`topk(2, stddev by (app) (topk(3, ` + ranges[3] + `)))`, `sort(stdvar(sort_desc(` + ranges[3] + `)))`, `bottomk(1, stddev(sum(topk(2, ` + ranges[3] + `))))`,
		`topk(2, stddev(` + ranges[3] + `))`, `sort_desc(stdvar by (app) (` + fmt.Sprintf(vectors[1], ranges[0]) + `))`,
	} {
		add(q)
	}
	// the spellings the proxy's route reads: case, blanks, newlines, parentheses.
	op := ranges[3]
	for _, q := range []string{
		`TOPK(3, ` + op + `)`, `Topk(3, ` + op + `)`, `topk (3, ` + op + `)`, `topk( 3, ` + op + ` )`, `topk(3 , ` + op + `)`, `topk(3,` + op + `)`, "topk(3,\n" + op + ")",
		"topk(\t3, " + op + ")", ` topk(3, ` + op + `) `, `BOTTOMK(2, ` + op + `)`, `sort(` + op + `)`, `SORT(` + op + `)`, `sort (` + op + `)`, `sort( ` + op + ` )`,
		"sort(\n" + op + "\n)", `sort_desc(` + op + `)`, `SORT_DESC(` + op + `)`, `sort_desc (` + op + `)`, ` sort_desc(` + op + `) `, `TOPK BY (app) (3, ` + op + `)`,
		`topk by(app)(3, ` + op + `)`, `topk(3, ` + op + `)by(app)`, `topk(3, RATE({app="a"}[5m]))`, `topk(3, rate ({app="a"}[5m]))`, `topk(3, rate({app="a"} [5m]))`,
		`topk(3, (` + op + `))`, `topk(3, ( ` + op + ` ))`, `topk(3, ((` + op + `)))`, `(topk(3, ` + op + `))`, `( topk(3, ` + op + `) )`, `((sort(` + op + `)))`,
		`sort((` + op + `))`, `sort_desc((` + op + `))`, `topk(3, sum( ` + op + `))`, `topk(3, sum (` + op + `))`, `topk(3, sum by (app) ( ` + op + `))`,
		`topk(3, sum by (app) ((` + op + `)))`, `topk(3, (sum by (app) (` + op + `)))`, `topk(2, (topk(3, ` + op + `)))`, `(topk(2, topk(3, ` + op + `)))`,
		`topk(2, topk (3, ` + op + `))`, `topk(2, TOPK(3, ` + op + `))`, `sort(sort_desc(` + op + `))`, `sort(sort_desc (` + op + `))`, `sort (sort_desc(` + op + `))`,
		`topk(2, sum(topk(3, ` + op + `)))`, `topk(2, sum( topk(3, ` + op + `)))`, `topk(2,sum(topk(3,` + op + `)))`, "topk(2, sum(\ntopk(3, " + op + ")))",
		`# a comment` + "\n" + `topk(3, ` + op + `)`, `topk(3, ` + op + `) # trailing`, `topk(3, count_over_time({app="a"} | logfmt --strict [5m]))`,
		`topk(3, count_over_time({app="a"} | logfmt --keep-empty | json [5m]))`,
	} {
		add(q)
	}
	// quoted text the translator's and the route's scans read as syntax, in the operand and in the selector.
	for _, line := range []string{
		` |= "without (x)"`, ` |= "by (x)"`, ` |= "| stats by (x)"`, ` |= "service_name"`, ` |= "detected_level"`, ` |= "a bool b"`, ` |~ "|||"`,
		` |= "topk(3, rate(x))"`, ` |= "sum by (x) (rate(y))"`, ` |= "__without__:sum:app"`, ` |= "__lvp_lr:x__"`, ` |= "@@@on:x"`, ` |= "__binary__:+:"`,
		` |= "("`, ` |= ")"`, ` |= ","`, ` |= "3, "`, " |= `without (z)`", ` |= "sort("`, ` |= "label_replace("`, ` |= "\\""`, ` |= "a\\"b, c"`,
	} {
		for _, n := range names {
			for _, f := range []string{"count_over_time", "rate"} {
				o := f + `({app="a"}` + line + `[5m])`
				rank(n, ``, o)
				rank(n, `by (app)`, o)
				rank(n, ``, `sum by (app) (`+o+`)`)
				rank(n, ``, `sort(`+o+`)`)
			}
		}
	}
	for _, s := range []string{`{app="topk(1, x)"}`, `{app="a, b"}`, `{app="(("}`, `{app=~"a|b"}`, `{app="without (x)"}`, `{a="sort(x)"}`, `{a=")"}`} {
		for _, n := range names {
			rank(n, ``, `rate(`+s+`[5m])`)
			rank(n, `by (app)`, `sum(rate(`+s+`[5m]))`)
		}
	}
	// operands that stay with the translator: a binary operation, vector(), a number, a log query, and the layers the
	// route does not take.
	for _, operand := range []string{
		`sum(` + ranges[3] + `) / sum(` + ranges[0] + `)`, ranges[3] + ` > 1`, ranges[3] + ` * 2`, `vector(1)`, `1`, `2 + 3`,
		`label_replace(` + ranges[3] + ` > 1, "d", "$1", "app", "(.*)")`, `sum(` + ranges[3] + `) > bool 1`, `stddev by (app) (` + ranges[3] + `)`, `stdvar(` + ranges[3] + `)`,
		`approx_topk(3, ` + ranges[3] + `)`,
	} {
		for _, n := range names {
			rank(n, ``, operand)
			rank(n, `by (app)`, operand)
		}
	}
	// label_replace around a ranking, a ranking inside a label_replace, and a ranking inside a binary operation.
	arg := `"d", "$1", "app", "(.*)"`
	for _, n := range names {
		for _, o := range []string{ranges[3], fmt.Sprintf(vectors[1], ranges[0]), ranges[16]} {
			head := n.name + "("
			if n.k != "" {
				head += n.k + ", "
			}
			add(fmt.Sprintf(`label_replace(%s%s), %s)`, head, o, arg))
			add(fmt.Sprintf(`label_replace(label_replace(%s%s), %s), "e", "$1", "d", "(.*)")`, head, o, arg))
			add(fmt.Sprintf(`%slabel_replace(%s, %s))`, head, o, arg))
			add(fmt.Sprintf(`%slabel_replace(%slabel_replace(%s, %s)), %s))`, head, head, o, arg, arg))
			add(fmt.Sprintf(`%s%s) + 1`, head, o))
			add(fmt.Sprintf(`%s%s) / %s%s)`, head, o, head, o))
			add(fmt.Sprintf(`sum(%s%s))`, head, o))
			add(fmt.Sprintf(`%s%s%s)))`, head, head, o))
		}
	}
	return out
}

// TestRankStepGridShadow holds the lowering of rankings to the proxy's route on the generated grid under every
// profile: the operators the route builds (a RankStep or SeriesAgg per level over the operand's operators) and the
// operand's request byte for byte, with 0 mismatches. The rows the lowering leaves to the string translator are
// counted by construct, and every row it answers must be routed by the proxy as a ranking.
// conformance: architecture-syntax-package-layering
func TestRankStepGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := rankStepGrid()
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
}

// The operand the lowering screens is the text the proxy slices and translates: for every ranking of the corpus and
// the grids that the plan reads as a ranking, the lowering's slice equals the route's operand when the proxy routes
// the query, and the lowering does not answer a query the proxy does not route as a ranking.
// conformance: architecture-syntax-package-layering
func TestRankOperandSpanIsTheProxysOperand(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	prof := planProfiles()[2].lowerProfile()
	compared, routedRows, lowered := 0, 0, 0
	for _, q := range append(append(planCorpus(t), rankGrid()...), rankStepGrid()...) {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(plan)
		if _, ok := plan.Root.(*queryplan.Rank); !ok {
			continue
		}
		compared++
		inner, routed := proxy.PostAggregationRoute(routedQuery(q))
		span, sliced := lower.RankOperand(q, plan)
		if routed {
			routedRows++
		}
		// Where the lowering slices the query, the proxy routes it and slices the same text; where the proxy does
		// not route it, nothing is lowered.
		if sliced && (!routed || span != inner) {
			// The route also reads a chain of layers the lowering does not lower; the slice is only held where it
			// is the deepest ranking's own operand.
			if routed && !strings.Contains(strings.ToLower(q), "stddev") && !strings.Contains(strings.ToLower(q), "stdvar") {
				t.Errorf("%q: the lowering slices %q, the proxy %q (routed %v)", q, span, inner, routed)
			} else if !routed {
				if op, err := lower.PlanQuery(q, plan, prof); err == nil {
					t.Errorf("%q: lowered (%T), the proxy does not route it as a ranking", q, op)
				}
			}
		}
		if _, err := lower.PlanQuery(q, plan, prof); err == nil {
			lowered++
			if !routed {
				t.Errorf("%q: lowered, the proxy does not route it as a ranking", q)
			}
		}
	}
	if compared < 5000 || routedRows == 0 || lowered < 1000 {
		t.Fatalf("compared %d rankings, %d routed, %d lowered", compared, routedRows, lowered)
	}
	t.Logf("%d rankings: %d routed by the proxy, %d lowered", compared, routedRows, lowered)
}

// FuzzProxyEvaluatedLowering holds the lowering of rankings to the proxy's route on any selector value, line
// filter, label name, ranking name, k, function, operand kind, nesting, clause and blank placement: the lowering
// either leaves the query to the string translator, or answers the operators the route builds and the operand's
// request byte for byte, and only for a query the proxy routes as a ranking. The text a metric query's scans read as
// syntax inside a quoted value (" bool ", "without") stays in: the lowering delegates it. Inputs are bounded (64
// bytes per value, k from a list), so a run does not stall on minimization.
// conformance: architecture-syntax-package-layering
func FuzzProxyEvaluatedLowering(f *testing.F) {
	f.Add("a", "x", "level", uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", uint8(7), uint8(3), uint8(5), uint8(9), uint8(41))
	f.Add("k8s.pod", "topk(1, x)", "a-b", uint8(200), uint8(9), uint8(11), uint8(70), uint8(250))
	f.Add("app", "without (x)", "pod", uint8(3), uint8(81), uint8(2), uint8(133), uint8(77))
	f.Fuzz(func(t *testing.T, sel, line, name string, fn, shape, rng, rank, operandKind uint8) {
		for _, s := range []*string{&sel, &line, &name} {
			if len(*s) > 64 {
				*s = (*s)[:64]
			}
		}
		q := rankFuzzQuery(sel, line, name, fn, shape, rng, rank, operandKind)
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			return
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			return
		}
		queryplan.Resolve(plan)
		profiles := planProfiles()
		for _, prof := range []planProfile{profiles[0], profiles[3], profiles[9]} {
			got, lerr := lower.PlanQuery(q, plan, prof.lowerProfile())
			if lerr != nil {
				continue
			}
			if rank, _ := evaluatedRoot(plan); !rank {
				continue
			}
			if _, ok := prof.translate(q); !ok {
				continue
			}
			if known := translatorMisreadsQuotedText(q) || translatorMisreadsParenthesizedMetric(q); known {
				continue
			}
			if msg := checkProxyEvaluated(q, prof, got); msg != "" {
				t.Fatal(msg)
			}
		}
	})
}

// rankFuzzQuery writes a ranking from the fuzzer's values: the name, k and clause spelling, the operand (a range
// aggregation with a stage, an unwrapped value, a vector aggregation or a nest of them, label_replace, a ranking),
// blanks, and wrappers around the whole.
func rankFuzzQuery(sel, line, name string, fn, shape, rng, rank, operandKind uint8) string {
	ident := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			if r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
				b.WriteRune(r)
			}
		}
		if b.Len() == 0 {
			return "x"
		}
		return b.String()
	}
	quote := func(s string) string { return fmt.Sprintf("%q", s) }
	selector := `{` + ident(sel) + []string{`="`, `!="`, `=~"`, `!~"`}[shape%4] + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(sel) + `"}`
	stage := []string{
		``, ` |= ` + quote(line), ` != ` + quote(line), ` | json`, ` | logfmt | ` + ident(name) + `=` + quote(line),
		` | json | unwrap ` + ident(name), ` | unwrap ` + ident(name), ` | json | ` + ident(name) + ` >= 5`,
	}[shape>>4%8]
	function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate", "sum_over_time", "last_over_time", "avg_over_time"}[fn%8]
	span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[0s]`}[rng%6]
	pad := []string{"", " ", "\t", "\n"}
	operand := function + pad[rng>>4%4] + `(` + selector + stage + span + `)`
	clauseOf := func(bits uint8) string {
		return []string{``, ` by ()`, ` by (` + ident(name) + `)`, ` without (` + ident(name) + `)`}[bits%4]
	}
	switch operandKind % 8 {
	case 1:
		operand = `sum` + clauseOf(operandKind>>3) + ` (` + operand + `)`
	case 2:
		operand = `max by (` + ident(name) + `) (sum by (` + ident(name) + `, pod) (` + operand + `))`
	case 3:
		operand = `label_replace(` + operand + `, "d", "$1", ` + quote(ident(name)) + `, "(.*)")`
	case 4:
		operand = `avg without (pod) (` + operand + `)`
	case 5:
		operand = `topk(2, ` + operand + `)`
	case 6:
		operand = `sum(` + operand + `) / 2`
	}
	kk := []string{"3", "03", "1", "100", "99999999999"}[rank>>4%5]
	name2 := []string{"topk", "bottomk", "TOPK", "Bottomk"}[rank>>6%4]
	var q string
	switch rank % 4 {
	case 0:
		q = name2 + clauseOf(rank>>2) + `(` + kk + `,` + pad[rng>>6%4] + operand + `)`
	case 1:
		q = name2 + `(` + kk + `, ` + operand + `)` + clauseOf(rank>>2)
	case 2:
		q = []string{"sort", "sort_desc", "SORT", "Sort_Desc"}[rank>>6%4] + pad[rng>>6%4] + `(` + pad[rng>>4%4] + operand + pad[rng>>2%4] + `)`
	default:
		q = `(` + name2 + `(` + kk + `, ` + operand + `))`
		if rank>>6%2 == 1 {
			q = `(sort(` + operand + `))`
		}
	}
	switch shape >> 7 {
	case 1:
		q = `sum(` + q + `)`
	}
	return q
}
