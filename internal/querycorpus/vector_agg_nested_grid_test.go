package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// vectorAggNestedGrid is the generated grid of vector aggregations over other vector aggregations: every outer
// and inner operator of sum, avg, min, max and count with the groupings each level can take (none, by (), by and
// without, derived labels, renamed labels, labels the inner level lacks), both spellings of every clause, over the
// five range functions, three and four levels, selectors, line filters and ranges, parser operands, binary
// operations and label_replace calls around a nest, quoted text a translator regexp could read as syntax, and the
// shapes that stay delegated (stddev and stdvar at any level, a ranking or binary under the nest, unwrap operands,
// a without with no label). Deterministic; the lowering and the string translator are held to each other on every
// row under every profile (TestVectorAggNestedGridShadow).
func vectorAggNestedGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	// level writes one vector aggregation over in, with its clause in front (prefix) or behind.
	level := func(op, g, in string, prefix bool) string {
		switch {
		case g == "":
			return fmt.Sprintf(`%s(%s)`, op, in)
		case prefix:
			return fmt.Sprintf(`%s %s (%s)`, op, g, in)
		}
		return fmt.Sprintf(`%s(%s) %s`, op, in, g)
	}
	ops := []string{"sum", "avg", "min", "max", "count"}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	rng := func(f, sel string) string { return fmt.Sprintf(`%s(%s[5m])`, f, sel) }
	outerG := []string{``, `by ()`, `by (app)`, `by (app, env)`, `by (env)`, `without (env)`, `without (app)`, `by (service_name)`, `by (detected_level)`, `by (_stream, level)`}
	innerG := []string{`by (app, env)`, `by (app)`, `by (env, app)`, `by (service_name, app)`, `by (app, detected_level)`, `by ()`, ``, `without (env)`, `without (app, env)`, `by (k8s.pod.name, app)`, `by (_stream, level, app)`}

	// every outer x inner operator x outer clause x inner clause; the spelling and the function rotate.
	n := 0
	for _, o := range ops {
		for _, i := range ops {
			for _, og := range outerG {
				for _, ig := range innerG {
					f := funcs[n%len(funcs)]
					add(level(o, og, level(i, ig, rng(f, `{app="a"}`), n%2 == 0), n%3 != 0))
					n++
				}
			}
		}
	}
	// both spellings of both clauses, and every function, on a few operator pairs.
	for _, pair := range [][2]string{{"sum", "sum"}, {"max", "count"}, {"avg", "min"}, {"count", "sum"}, {"min", "avg"}} {
		for _, og := range []string{`by (app)`, `without (env)`, ``} {
			for _, ig := range []string{`by (app, env)`, `without (env)`, `by ()`, ``} {
				for _, f := range funcs {
					for _, sp := range [][2]bool{{true, true}, {true, false}, {false, true}, {false, false}} {
						add(level(pair[0], og, level(pair[1], ig, rng(f, `{app="a"}`), sp[1]), sp[0]))
					}
				}
			}
		}
	}
	// three and four levels, every clause kind at every position.
	clauses := []string{``, `by ()`, `by (app)`, `by (app, env)`, `by (env)`, `without (env)`, `without (app)`, `by (service_name, app)`}
	for a, o1 := range ops {
		for b, o2 := range ops {
			for c, o3 := range ops {
				for ci := 0; ci < len(clauses); ci++ {
					g1, g2, g3 := clauses[ci], clauses[(ci+a+1)%len(clauses)], clauses[(ci+b+c+2)%len(clauses)]
					f := funcs[(a+b+c+ci)%len(funcs)]
					add(level(o1, g1, level(o2, g2, level(o3, g3, rng(f, `{app="a"}`), true), a%2 == 0), b%2 == 0))
					o4 := ops[(a+b+c)%len(ops)]
					g4 := clauses[(ci+a+b)%len(clauses)]
					add(level(o4, g4, level(o1, g1, level(o2, g2, level(o3, g3, rng(f, `{app="a"}`), c%2 == 0), true), false), true))
				}
			}
		}
	}
	// selectors, line filters and ranges.
	selectors := []string{`{app="a"}`, `{app=~".*"}`, `{service_name="s"}`, `{detected_level="error"}`, `{app!="a", env=~"p.*"}`, `{k8s.pod.name="p"}`}
	for _, sel := range selectors {
		for _, line := range []string{``, ` |= "x"`, ` != "x" |~ "y.+"`, ` |> "<_> x"`, ` | status >= 500`, ` | status="200" |= "k"`} {
			for _, f := range funcs {
				in := fmt.Sprintf(`%s(%s%s[5m])`, f, sel, line)
				add(level("sum", `by (app)`, level("max", `by (app, env)`, in, true), true))
				add(level("avg", ``, level("count", `by (service_name)`, in, true), true))
				add(level("min", `by (detected_level)`, level("sum", `without (env)`, in, true), false))
			}
		}
	}
	for _, r := range []string{`[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[1d]`, `[1ms]`, `[1h30m]`, `[2w]`, `[0s]`, `[1.5h]`, `[1y]`} {
		for _, f := range []string{"rate", "bytes_rate", "count_over_time"} {
			in := fmt.Sprintf(`%s({app="a"}%s)`, f, r)
			add(level("sum", `by (app)`, level("count", `by (app, env)`, in, true), true))
			add(level("max", ``, level("avg", `without (env)`, in, true), true))
		}
	}
	// parser operands: json and logfmt, with labels the parsers extract or a translator resolves.
	for _, st := range []string{` | json`, ` | logfmt`, ` | json | level="error"`, ` | logfmt | status >= 500`, ` | logfmt | drop a`, ` | json | keep a`, ` | line_format "{{.x}}"`,
		` | unpack`, ` | regexp "(?P<a>x)"`, ` | pattern "<a> <b>"`, ` | label_format a=b`, ` | json | unwrap x`, ` | unwrap x`} {
		for _, f := range []string{"rate", "count_over_time", "bytes_over_time", "bytes_rate"} {
			in := fmt.Sprintf(`%s({app="a"}%s[5m])`, f, st)
			add(level("max", `by (zone)`, level("min", `by (pod, zone)`, in, true), true))
			add(level("count", ``, level("sum", `by (pod_name)`, in, true), true))
			add(level("avg", `by (app)`, level("sum", `without (pod)`, in, true), true))
			add(level("sum", `without (a)`, level("max", `by (pod, a)`, in, true), true))
			add(level("sum", `by (k8s.pod.name)`, level("max", `by (k8s.pod.name, k8s_pod_name)`, in, true), true))
		}
	}
	// labels renamed by the label function: dotted and underscored spellings across the levels.
	for _, pair := range [][2]string{{`by (k8s.pod.name)`, `by (k8s.pod.name)`}, {`by (k8s_pod_name)`, `by (k8s.pod.name)`}, {`by (k8s.pod.name)`, `by (k8s_pod_name)`},
		{`by (k8s_pod_name)`, `by (k8s_pod_name)`}, {`by (k8s.pod.name, k8s_pod_name)`, `by (k8s_pod_name, k8s.pod.name)`}, {`by (app, app)`, `by (app, app, env)`},
		{`by (app)`, `by (app, app)`}, {`without (k8s.pod.name)`, `by (k8s.pod.name, app)`}, {`by (service_name, service.name)`, `by (service_name, service_name)`},
		{`by (a,b,c)`, `by ( a , b , c )`}, {`by (_stream)`, `by (_stream, level)`}, {`by (level)`, `by (_stream, level)`}, {`by (__error__)`, `by (__error__, app)`}} {
		for _, f := range []string{"rate", "count_over_time"} {
			add(level("sum", pair[0], level("count", pair[1], rng(f, `{app="a"}`), true), true))
			add(level("max", pair[0], level("sum", pair[1], rng(f, `{app="a"}`), false), false))
		}
	}
	// text a translator regexp reads as syntax: a line filter that spells a grouping, a stats pipe, a derived name,
	// a paren; a quoted clause under each level.
	for _, line := range []string{
		` |= "without (x)"`, ` |= "by (x)"`, ` |= "| stats by (x)"`, ` |= "| stats "`, ` |= "service_name"`, ` |= "detected_level"`, ` |= "a bool b"`, ` |~ "|||"`, ` |= "a|||b"`,
		` |= "sum by (x) (rate(y))"`, ` |= "__without__:sum:app"`, ` |~ "without\\s*\\((a|b)\\)"`, " |= `without (z)`", ` |= ")"`, ` |= "("`, ` |= "))"`, ` |= "sum("`,
		` |= "  "`, ` |= "a  b"`,
	} {
		for _, f := range []string{"count_over_time", "rate"} {
			in := rng(f, `{app="a"}`+line)
			add(level("sum", `by (app)`, level("max", `by (app, env)`, in, true), true))
			add(level("avg", ``, level("count", `without (env)`, in, true), true))
			add(level("sum", `without (env)`, level("max", `by (app, env)`, in, true), true))
			add(level("min", `by (app)`, level("sum", ``, in, true), false))
		}
	}
	for _, q := range []string{
		`sum(sum by (x) (rate({x!="  "}[5m])))`, `sum without (x) (sum by (x, y) (rate({x!="  "}[5m])))`, `sum by (x) (sum without (y) (rate({x!="  "}[5m])))`,
		`sum by ( bool ) (sum by (bool, a) (rate({app="a"}[5m])))`, `sum(sum by ( bool ) (rate({app="a"}[5m])))`, `sum by (bool) (count by (bool, a) (count_over_time({app="a"}[5m])))`,
		`sum(sum without (x) (rate({app="a"} |= "without(0)\xa2"[5m])))`, `sum(sum by (app) (rate({app="a"} |= "x\xa2"[5m])))`, `max by (a) (sum(rate({app="a"} |= "\xff"[5m])) by (a, b))`,
		`sum by (a) (sum by (a) (rate({app="a"}[5m] offset 1h)))`, `sum(sum by (a) (rate({app="a"}[5m]))) offset 1h`,
		`sum(sum by (app) (rate({app="a"}[5m]))) by (app) by (env)`, `sum(sum by (app) (rate({app="a"}[5m])) by (app))`, `sum(sum(rate({app="a"}[5m])) by (app)) by ()`,
		`sum by (app) ( sum by (app) (rate({app="a"}[5m])))`, `sum by (app) (sum by (app) ( rate({app="a"}[5m])))`, `sum by (app) (sum by (app) (rate ({app="a"}[5m])))`,
		"sum by (app) (\nsum by (app) (rate({app=\"a\"}[5m])))", `sum by (app) (sum  by (app) (rate({app="a"}[5m])))`, `sum  by (app) (sum by (app) (rate({app="a"}[5m])))`,
		`sum by(app)(sum by(app,env)(rate({app="a"}[5m])))`, `sum(sum(rate({app="a"}[5m])))`, `SUM by (app) (sum by (app) (rate({app="a"}[5m])))`,
		`(sum by (app) (sum by (app) (rate({app="a"}[5m]))))`, `sum((sum by (app) (rate({app="a"}[5m]))))`, `sum by (app) ((sum by (app) (rate({app="a"}[5m]))))`,
		`sum by (app) (sum by (app) ((rate({app="a"}[5m]))))`, `((sum by (app) (sum by (app) (rate({app="a"}[5m])))))`,
	} {
		add(q)
	}
	// binary operations and label_replace around a nest, and nests as operands of both.
	nest := level("sum", `by (app)`, level("max", `by (app, env)`, rng("rate", `{app="a"}`), true), true)
	nestW := level("sum", `by (app)`, level("max", `without (env)`, rng("rate", `{app="a"}`), true), true)
	nest3 := level("avg", ``, level("min", `by (app)`, level("sum", `by (app, env)`, rng("count_over_time", `{app="a"}`), true), true), true)
	for _, op := range []string{"/", "*", "+", "-", ">", "==", "and", "or"} {
		for _, other := range []string{nest, nestW, nest3, `sum by (app) (rate({app="b"}[5m]))`, `rate({app="b"}[5m])`, `2`, `0.5`} {
			add(fmt.Sprintf(`%s %s %s`, nest, op, other))
			add(fmt.Sprintf(`%s %s %s`, other, op, nest3))
			add(fmt.Sprintf(`%s %s %s`, nestW, op, other))
		}
	}
	for _, x := range []string{nest, nestW, nest3} {
		add(fmt.Sprintf(`label_replace(%s, "d", "$1", "app", "(.*)")`, x))
		add(fmt.Sprintf(`label_replace(label_replace(%s, "d", "$1", "app", "(.*)"), "e", "$1", "d", "(.*)")`, x))
		add(fmt.Sprintf(`label_replace(%s / 2, "d", "$1", "app", "(.*)")`, x))
	}
	// what stays delegated: stddev and stdvar at every position, a ranking or a binary under the nest, label_replace
	// under it, unwrap operands, a without with no label at any level, an operand that is no range aggregation.
	r := rng("rate", `{app="a"}`)
	for _, sd := range []string{"stddev", "stdvar"} {
		for _, g := range []string{``, `by (app)`, `by (app, env)`, `without (env)`, `by ()`} {
			add(level(sd, g, r, true))
			add(level(sd, g, level("sum", `by (app, env)`, r, true), true))
			add(level("sum", `by (app)`, level(sd, g, r, true), true))
			add(level("sum", `by (app)`, level("max", `by (app, env)`, level(sd, g, r, true), true), true))
			add(level(sd, g, level("max", `by (app, env)`, level("sum", `by (app, env)`, r, true), true), false))
			add(level("count", ``, level(sd, g, level("sum", `by (app, env)`, r, true), true), true))
		}
	}
	for _, q := range []string{
		`sum(topk(3, sum by (app) (rate({app="a"}[5m]))))`, `sum(sum(topk(3, rate({app="a"}[5m]))))`, `sum by (app) (bottomk(2, sum by (app, env) (rate({app="a"}[5m]))))`,
		`sum(sort(sum by (app) (rate({app="a"}[5m]))))`, `sum(sum(sort_desc(rate({app="a"}[5m]))))`, `sum(sum by (app) (rate({app="a"}[5m]) / 2))`,
		`sum(sum by (app) (rate({app="a"}[5m]) > 1))`, `sum by (app) (sum by (app) (rate({app="a"}[5m])) + sum by (app) (rate({app="b"}[5m])))`,
		`sum(sum(label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)")))`, `sum(sum(label_replace(sum by (app) (rate({app="a"}[5m])), "d", "$1", "app", "(.*)")))`,
		`sum(sum by (app) (sum_over_time({app="a"} | unwrap n [5m])))`, `sum by (app) (max by (app, pod) (avg_over_time({app="a"} | json | unwrap n [5m])))`,
		`sum(sum by (app) (rate({app="a"} | unwrap n [5m])))`, `sum(max by (app) (absent_over_time({app="a"} | unwrap n [5m])))`,
		`sum(sum by (app) (quantile_over_time(0.9, {app="a"} | unwrap n [5m])))`, `sum(sum by (app) (sum_over_time({app="a"} | unwrap n [5m]) by (pod)))`,
		`sum(sum by (app) (count_over_time({app="a"}[5m]) by (x)))`, `sum without () (sum by (app) (rate({app="a"}[5m])))`, `sum by (app) (sum without () (rate({app="a"}[5m])))`,
		`sum(sum(count_over_time({app="a"}[5m])) without ())`, `sum by (app) (sum by (app) (vector(1)))`, `sum(sum(1))`, `sum(sum by (app) ({app="a"}))`,
		`sum by (app) (group by (app) (rate({app="a"}[5m])))`, `count_values("v", sum by (app) (count_over_time({app="a"}[5m])))`,
		`sum(sum(first_over_time({app="a"} | unwrap n [5m])))`, `sum(sum(rate_counter({app="a"} | unwrap n [5m])))`,
		`sum(sum by (app) (rate({app="a"}[5m]) by (x)))`, `sum(sum by (app) (sum_over_time({app="a"} | json | unwrap n [5m]) without (x)))`,
	} {
		add(q)
	}
	return out
}

// TestVectorAggNestedGridShadow holds the lowering of a vector aggregation over another one to the string
// translator on the generated grid under every profile: 0 mismatches, the text byte for byte, and the rows each
// side answers are counted. The grid must lower a good share natively and delegate the shapes it names.
// conformance: architecture-syntax-package-layering
func TestVectorAggNestedGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := vectorAggNestedGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 40))
	}
	if native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
