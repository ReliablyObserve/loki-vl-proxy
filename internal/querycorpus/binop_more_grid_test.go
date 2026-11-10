package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// binOpMoreGrid is the generated grid of the two binary-operation spellings the lowering reads as the string
// translator does (step 21): a bool modifier, which the translator drops, and a without clause on an operand,
// which it removes from the text and writes once after the whole binary. It covers a without clause on each side and
// on both, in the head and the tail form, with every aggregation, label lists in several spellings (dotted names,
// blanks, the derived labels, none), every operator, bool on each comparison with vector matching, nested
// operations, set operators, vector() and numbers, label_replace around each, and the words in places that are not
// the modifier or the clause (labels, quoted values). Deterministic; the lowering and the string translator are
// held to each other on every row under every profile (TestBinOpMoreGridShadow).
func binOpMoreGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	ranges := []string{
		`rate({app="a"}[5m])`, `count_over_time({app="b"} |= "x"[1m])`, `bytes_over_time({app=~"a.*"}[5m])`, `bytes_rate({app="a", env!="p"}[5m] offset 1h)`,
		`absent_over_time({app="a"}[5m])`, `rate({app="uw"} | unwrap amount [150s])`, `max_over_time({app="uw"} | json | unwrap amount [5m])`,
		`sum_over_time({app="uw"} | logfmt | unwrap amount [5m])`, `count_over_time({app="a"} | json [5m])`,
	}
	aggs := []string{"sum", "count", "min", "max", "avg"}
	labelLists := []string{`pod`, `pod, node`, `pod,node`, ` pod `, `k8s.pod.name`, `service_name`, `detected_level`, `app`, `pod ,node`, `pod,`, `,pod`}
	// the clause in its head and tail forms, and its spellings.
	withouts := func(agg, labels, in string) []string {
		return []string{
			fmt.Sprintf(`%s without (%s) (%s)`, agg, labels, in),
			fmt.Sprintf(`%s(%s) without (%s)`, agg, in, labels),
			fmt.Sprintf(`%s without(%s)(%s)`, agg, labels, in),
			fmt.Sprintf(`%s(%s)without(%s)`, agg, in, labels),
			fmt.Sprintf(`%s(%s) without(%s)`, agg, in, labels),
			fmt.Sprintf(`%s WITHOUT (%s) (%s)`, strings.ToUpper(agg), labels, in),
		}
	}
	// a clause on the left, on the right and on both sides, with each operator.
	ops := []string{"/", "*", "+", "-", "%", "^", "==", "!=", ">=", "<=", ">", "<"}
	for i, in := range ranges {
		for j, agg := range aggs {
			// every aggregation over the first range, and sum and avg over the others.
			if i > 0 && j%2 == 1 {
				continue
			}
			for k, w := range withouts(agg, `pod`, in) {
				if i > 0 && k > 1 {
					continue
				}
				for _, op := range ops {
					if i > 0 && op != "/" && op != ">" {
						continue
					}
					add(fmt.Sprintf(`%s %s 2`, w, op))
					add(fmt.Sprintf(`%s %s %s`, w, op, ranges[0]))
					add(fmt.Sprintf(`100 %s %s`, op, w))
				}
				add(fmt.Sprintf(`sum by (app) (%s) / %s`, ranges[1], w))
				add(fmt.Sprintf(`%s > bool %s`, w, ranges[0]))
			}
		}
	}
	for _, labels := range labelLists {
		for _, agg := range aggs[:3] {
			for _, w := range withouts(agg, labels, ranges[0]) {
				add(fmt.Sprintf(`%s / 2`, w))
				add(fmt.Sprintf(`2 * %s`, w))
			}
		}
	}
	// on both sides: the first clause's labels are written, the second's are lost.
	for _, a := range withouts("sum", `pod`, ranges[0]) {
		for _, b := range withouts("max", `node`, ranges[1]) {
			add(fmt.Sprintf(`%s / %s`, a, b))
			add(fmt.Sprintf(`%s > bool %s`, a, b))
			add(fmt.Sprintf(`%s / on(app) group_left(env) %s`, a, b))
		}
	}
	for _, ll := range [][2]string{{`pod`, `pod`}, {`a, b`, `c`}, {`k8s.x`, `y.z`}, {`service_name`, `detected_level`}} {
		add(fmt.Sprintf(`sum without (%s) (%s) / count without (%s) (%s)`, ll[0], ranges[0], ll[1], ranges[1]))
		add(fmt.Sprintf(`sum(%s) without (%s) + avg(%s) without (%s)`, ranges[0], ll[0], ranges[2], ll[1]))
	}
	// vector matching around a clause.
	for _, m := range []string{`on(app)`, `on()`, `ignoring(pod)`, `on(app) group_left(pod)`, `ignoring(pod) group_right`, `on (app)`, `ON(app)`} {
		add(fmt.Sprintf(`sum without (pod) (%s) / %s sum by (app) (%s)`, ranges[0], m, ranges[1]))
		add(fmt.Sprintf(`sum by (app) (%s) %s %s sum without (pod) (%s)`, ranges[1], ops[0], m, ranges[0]))
		add(fmt.Sprintf(`sum without (pod) (%s) > bool %s sum without (node) (%s)`, ranges[0], m, ranges[1]))
		add(fmt.Sprintf(`sum without (pod) (%s) %s %s sum without (node) (%s)`, ranges[0], "and", m, ranges[1]))
	}
	// a clause that is not one the lowering writes: no label, another aggregation, a nest, an operand that stays
	// delegated, a word that is not the clause.
	for _, q := range []string{
		`sum without () (rate({app="a"}[5m])) / 2`, `sum without ( ) (rate({app="a"}[5m])) / 2`, `sum(rate({app="a"}[5m])) without () / 2`,
		`sum without (,) (rate({app="a"}[5m])) / 2`, `sum without ( , ) (rate({app="a"}[5m])) > bool 1`,
		`stddev without (pod) (rate({app="a"}[5m])) / 2`, `stdvar without (pod) (rate({app="a"}[5m])) / 2`,
		`sum without (pod) (sum by (app, pod) (rate({app="a"}[5m]))) / 2`, `sum without (pod) (sum without (node) (rate({app="a"}[5m]))) / 2`,
		`sum by (app) (sum without (pod) (rate({app="a"}[5m]))) / 2`, `sum without (pod) (rate({app="a"}[5m])) / sum by (app) (sum without (pod) (rate({app="a"}[5m])))`,
		`topk without (pod) (2, rate({app="a"}[5m])) / 2`, `topk(2, sum without (pod) (rate({app="a"}[5m]))) / 2`,
		`sum without (pod) (rate({app="a"}[5m])) + on() vector(1)`, `vector(1) / sum without (pod) (rate({app="a"}[5m]))`,
		`sum without (pod) (rate({app="a"}[5m])) and sum without (pod) (rate({app="b"}[5m]))`,
		`sum without (pod) (rate({app="a"}[5m])) or on(app) rate({app="b"}[5m])`, `rate({app="b"}[5m]) unless ignoring(pod) sum without (pod) (rate({app="a"}[5m]))`,
		`sum without (pod) (rate({app="a"} | json [5m])) / 2`, `sum without (pod) (rate({app="a"} | json | keep x [5m])) / 2`,
		`sum without (pod) (rate({app="a"} | drop x [5m])) / 2`, `sum without (pod) (rate({app="a"} | unwrap x [5m]) by (z)) / 2`,
		`sum without (pod) (rate({app="a"} |= "without (x)" [5m])) / 2`, `sum without (pod) (rate({app="a"} |= "without" [5m])) / 2`,
		`sum without (pod) (rate({app="a"} |= "a  b" [5m])) / 2`, `sum  without (pod) (rate({app="a"}[5m])) / 2`,
		`sum without  (pod) (rate({app="a"}[5m])) / 2`, `sum without (pod)  (rate({app="a"}[5m])) / 2`, `sum without (pod, node) (rate({app="a"}[5m]))  / 2`,
		"sum without (pod)\n(rate({app=\"a\"}[5m])) / 2", "sum without\t(pod) (rate({app=\"a\"}[5m])) / 2", "sum without (pod) (rate({app=\"a\"}[5m]))\n/ 2",
		"sum(rate({app=\"a\"}[5m])) without\n(pod) / 2", "sum(rate({app=\"a\"}[5m])) \n without (pod) / 2",
		`sum by (without_x) (rate({app="a"}[5m])) / 2`, `sum by (xwithout) (rate({app="a"}[5m])) / 2`, `sum by (app) (rate({app="a", without="x"}[5m])) / 2`,
		`sum by (app) (rate({app="a"} | without="x" [5m])) / sum without (pod) (rate({app="a"}[5m]))`,
		`sum without (pod) (rate({app="a"}[5m])) / (sum without (pod) (rate({app="a"}[5m])) + 1)`, `(sum without (pod) (rate({app="a"}[5m]))) / 2`,
		`sum without (pod) (rate({app="a"}[5m])) / 2 / 2`, `1 + sum without (pod) (rate({app="a"}[5m])) * 2`,
		`sum without (pod) (rate({app="a"}[5m])) + sum without (pod) (rate({app="a"}[5m])) / sum without (pod) (rate({app="a"}[5m]))`,
		`sum without (pod) (rate({app="a"}[5m])) * (rate({app="a"}[5m]) + rate({app="b"}[5m])) * sum without (node) (rate({app="b"}[5m]))`,
	} {
		add(q)
	}
	// bool: on each comparison and operand pair, with matching, and the word in places that are not the modifier.
	pool := []string{ranges[0], `sum by (app) (` + ranges[1] + `)`, `sum(` + ranges[0] + `)`, `count by (app, env) (` + ranges[3] + `)`, ranges[5], `sum by (service_name) (` + ranges[0] + `)`}
	for _, op := range []string{"==", "!=", ">=", "<=", ">", "<"} {
		for i, l := range pool {
			for j, r := range append(append([]string{}, pool...), "0", "1.5", "-2", "1000000") {
				if (i+j)%2 == 0 || i == 0 {
					add(fmt.Sprintf(`%s %s bool %s`, l, op, r))
				}
			}
			add(fmt.Sprintf(`0 %s bool %s`, op, l))
		}
	}
	for _, op := range []string{"/", "*", "+", "-", "%", "^"} {
		add(fmt.Sprintf(`%s %s bool 2`, ranges[0], op))
		add(fmt.Sprintf(`%s %s bool %s`, ranges[0], op, ranges[1]))
	}
	for _, m := range []string{`on(app)`, `on()`, `ignoring(pod)`, `on(app, env) group_left(pod)`, `ignoring() group_right`, `on (app)`, `ON(app)`, `on(app)group_left`, `on(app) GROUP_RIGHT(x)`} {
		for _, op := range []string{">", "==", "<="} {
			add(fmt.Sprintf(`%s %s bool %s %s`, pool[1], op, m, pool[3]))
			add(fmt.Sprintf(`%s %s bool %s %s`, pool[0], op, m, pool[2]))
			add(fmt.Sprintf(`%s %s %s bool %s`, pool[1], op, m, pool[3]))
		}
	}
	for _, q := range []string{
		`rate({app="a"}[5m]) > BOOL 1`, `rate({app="a"}[5m]) > Bool 1`, `rate({app="a"}[5m]) > bool 1`, `rate({app="a"}[5m]) >  bool 1`, `rate({app="a"}[5m]) > bool  1`,
		"rate({app=\"a\"}[5m]) > bool\n1", "rate({app=\"a\"}[5m]) >\tbool\t1", "rate({app=\"a\"}[5m]) >\nbool 1", "rate({app=\"a\"}[5m]) > \n bool \n 1",
		`rate({app="a"}[5m]) > bool bool`, `rate({app="a"}[5m]) >bool 1`, `rate({app="a"}[5m]) > bool1`, `rate({app="a"}[5m]) bool > 1`,
		`rate({app="a"}[5m])>bool 1`, `rate({app="a"}[5m]) >bool(1)`, `rate({app="a"}[5m]) > bool (1)`, `rate({app="a"}[5m]) > bool rate({app="a"}[5m])`,
		`sum by (bool) (rate({app="a"}[5m])) > bool 1`, `sum by (a, bool) (rate({app="a"}[5m])) > bool 1`, `sum by (bool, a) (rate({app="a"}[5m])) > bool 1`,
		`sum by ( bool ) (rate({app="a"}[5m])) > bool 1`, `sum by (a, bool , b) (rate({app="a"}[5m])) > bool 1`, `sum by (a) (rate({app="a"}[5m])) > bool on(bool) sum by (a) (rate({app="b"}[5m]))`,
		`rate({app="a"}[5m]) > bool on( bool ) rate({app="b"}[5m])`, `rate({app="a"}[5m]) > bool ignoring(bool) rate({app="b"}[5m])`,
		`sum by (boolean) (rate({app="a"}[5m])) > bool 1`, `sum by (xbool) (rate({app="a"}[5m])) > bool 1`, `sum by (a) (rate({app="a"}[5m])) > on(boolean) sum by (a) (rate({app="b"}[5m]))`,
		`rate({app="a", bool="x"}[5m]) > bool 1`, `rate({bool="x"} |= "a" [5m]) > bool 1`, `rate({app="a"} | bool="x" [5m]) > bool 1`, `rate({app="a"} | bool = "x" [5m]) > 1`,
		`rate({app="a"} |= "a bool b" [5m]) > bool 1`, `rate({app="a"} |= " bool " [5m]) > bool 1`, `rate({app="a"} |= "bool" [5m]) > bool 1`, `rate({app="a"} |= "bool" [5m]) > 1`,
		`rate({app="a"} != "x" [5m]) > bool 1`, `sum(rate({app="a"}[5m])) > bool 1 > bool 2`, `(sum(rate({app="a"}[5m])) > bool 1) > bool 2`,
		`sum(sum by (job) (rate({app="a"}[5m])) > bool 1) / 2`, `sum(rate({app="a"}[5m]) > bool 1)`, `rate({app="a"}[5m]) > bool 1 + 2`, `1 < bool 2`, `vector(1) > bool 0`,
		`rate({app="a"}[5m]) > bool vector(1)`, `sum(rate({app="a"}[5m])) and bool sum(rate({app="b"}[5m]))`, `rate({app="a"}[5m]) == bool 0 and rate({app="b"}[5m])`,
		`topk(2, rate({app="a"}[5m])) > bool 1`, `rate({app="a"}[5m]) > bool topk(2, rate({app="b"}[5m]))`, `{app="a"} > bool 1`,
		`label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)") > bool 1`, `rate({app="a"}[5m]) > bool label_replace(rate({app="b"}[5m]), "d", "$1", "app", "(.*)")`,
	} {
		add(q)
	}
	// operands whose stages the lowering leaves to the translator under bool and a clause (the allocation rule).
	for _, st := range []string{
		` | json | status >= 500`, ` | logfmt | level="error"`, ` | json | drop a`, ` | logfmt | keep a`, ` | status >= 500`, ` | regexp "(?P<a>x)"`, ` | pattern "<a> <b>"`,
		` | line_format "{{.a}}"`, ` | label_format b=a`, ` | decolorize`, ` | json a="b"`, ` | logfmt --strict`, ` | logfmt --keep-empty`, ` | unpack`, ` | json | unwrap amount`,
		` | json`, ` | logfmt`, ` |= "x" | json`, ` != "x"`, ` |~ "x.*"`, ` | json | level="x" | unwrap amount`,
	} {
		for _, f := range []string{"rate", "count_over_time", "bytes_rate", "sum_over_time", "absent_over_time"} {
			in := fmt.Sprintf(`%s({app="a"}%s [5m])`, f, st)
			add(fmt.Sprintf(`%s > bool 1`, in))
			add(fmt.Sprintf(`%s > bool %s`, in, ranges[0]))
			add(fmt.Sprintf(`sum without (pod) (%s) / 2`, in))
			add(fmt.Sprintf(`%s / on(app) max(%s) without (pod)`, ranges[0], in))
		}
	}
	// set operators, vector() and numbers around the clause and the modifier, with and without matching.
	for _, op := range []string{"and", "or", "unless"} {
		for _, m := range []string{``, `on(app) `, `ignoring(pod) `, `on() `, `on(app) group_left(pod) `} {
			add(fmt.Sprintf(`%s %s %s%s`, pool[1], op, m, pool[3]))
			add(fmt.Sprintf(`sum without (pod) (%s) %s %s%s`, ranges[0], op, m, pool[3]))
			add(fmt.Sprintf(`%s %s %ssum without (pod) (%s)`, pool[3], op, m, ranges[0]))
			add(fmt.Sprintf(`%s > bool 1 %s %s%s`, pool[0], op, m, pool[1]))
		}
	}
	for _, q := range []string{
		`vector(1) + vector(2)`, `vector(1) > bool vector(2)`, `1 + 2`, `1 > bool 2`, `-1 / 0`, `1 + 2 + 3`, `2 * 3 * rate({app="a"}[5m])`,
		`rate({app="a"}[5m]) * vector(2)`, `vector(2) * rate({app="a"}[5m])`, `sum by (app) (rate({app="a"}[5m])) + on() vector(0)`,
	} {
		add(q)
	}
	// nesting of depth 2 and 3, with a clause or a modifier at each level.
	a, b, c := `sum(rate({app="a"}[5m]))`, `sum(rate({app="b"}[5m]))`, `sum(rate({app="c"}[5m]))`
	aw, bw := `sum without (pod) (rate({app="a"}[5m]))`, `max without (node) (rate({app="b"}[5m]))`
	for _, o1 := range []string{"+", ">", "/"} {
		for _, o2 := range []string{"-", "<", "^"} {
			add(fmt.Sprintf(`%s %s %s %s %s`, a, o1, b, o2, c))
			add(fmt.Sprintf(`(%s %s %s) %s %s`, a, o1, b, o2, c))
			add(fmt.Sprintf(`%s %s (%s %s %s)`, a, o1, b, o2, c))
			add(fmt.Sprintf(`%s %s %s %s %s`, aw, o1, b, o2, c))
			add(fmt.Sprintf(`%s %s %s %s %s`, a, o1, bw, o2, aw))
			add(fmt.Sprintf(`(%s %s %s) %s 2`, aw, o1, bw, o2))
			add(fmt.Sprintf(`%s %s bool %s %s %s`, a, o1, b, o2, c))
			add(fmt.Sprintf(`((%s %s %s) %s %s) %s 1`, a, o1, bw, o2, c, o1))
			add(fmt.Sprintf(`%s %s %s %s %s %s %s`, a, o1, b, o2, c, o1, aw))
		}
	}
	// label_replace around the shapes.
	lr := `"d", "$1", "app", "(.*)"`
	for _, in := range []string{
		aw + ` / 2`, `2 * ` + aw, aw + ` / ` + bw, `sum(rate({app="a"}[5m])) without (pod) > 1`, a + ` > bool 1`, a + ` > bool ` + b, a + ` == bool on(app) ` + b,
		aw + ` > bool 1`, aw + ` > bool ` + bw, aw + ` / on(app) group_left ` + b, `count without (pod, node) (rate({app="a"}[5m])) / 2`,
		`sum without (service_name) (rate({app="a"}[5m])) / 2`, `sum by (service_name) (rate({app="a"}[5m])) > bool 1`, `sum without (pod) (` + ranges[5] + `) > 1`,
		aw + ` and ` + b, a + ` > bool 1 and ` + b, `stddev without (pod) (rate({app="a"}[5m])) / 2`, `sum without () (rate({app="a"}[5m])) / 2`,
		`sum without (pod) (rate({app="a"}[5m])) > bool 1 > bool 2`,
	} {
		add(fmt.Sprintf(`label_replace(%s, %s)`, in, lr))
		add(fmt.Sprintf(`label_replace(label_replace(%s, %s), "e", "$1", "d", "(.*)")`, in, lr))
		add(fmt.Sprintf(`label_replace(%s,%s)`, in, strings.ReplaceAll(lr, ", ", ",")))
	}
	return out
}

// TestBinOpMoreGridShadow holds the lowering of binary operations with a bool modifier or a without clause on an
// operand to the string translator on the generated grid under every profile: 0 mismatches, the text byte for byte,
// and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestBinOpMoreGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := binOpMoreGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 60))
	}
	// Each kind of row the grid is for must be there, lowered: a screen that turned everything away would pass.
	if native < rows/4 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
	for _, c := range []string{"BinOp bool", "BinOp operand without"} {
		if n := delegated[c]; n > rows/20 {
			t.Errorf("%d rows are delegated as %q: the lowering writes that spelling now", n, c)
		}
	}
}
