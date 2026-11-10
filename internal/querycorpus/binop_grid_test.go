package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// binOpGrid is the generated grid of binary operations and label_replace calls: every operator (the arithmetic and
// comparison ones the lowering writes, and and, or, unless, which stay delegated) between operands of every kind
// (the range and vector aggregations the lowering writes, numbers in several spellings, and the operands that stay
// delegated: a without clause, stddev, a parser, vector(), a ranking, a nested binary operation, label_replace),
// with and without bool, every vector matching clause in several spellings, operator and blank spellings, quoted
// text the translator's scans read as syntax, and label_replace around each lowered expression, nested, with
// argument spellings, and around the shapes that stay delegated. Deterministic; the lowering and the string
// translator are held to each other on every row under every profile (TestBinOpGridShadow).
func binOpGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	ops := []string{"/", "*", "+", "-", "%", "^", "==", "!=", ">=", "<=", ">", "<", "and", "or", "unless"}
	vectors := []string{
		`rate({app="a"}[5m])`, `count_over_time({app="b"} |= "x"[1m])`, `bytes_rate({app="a", env!="p"}[5m] offset 1h)`,
		`bytes_over_time({app=~"a.*"}[5m])`, `absent_over_time({app="a"}[5m])`,
		`sum(rate({app="a"}[5m]))`, `sum by (app) (rate({app="a"}[5m]))`, `count by (app) (count_over_time({app="a"}[5m]))`,
		`max(bytes_rate({app="a"}[5m])) by (env)`, `avg by (app, env) (count_over_time({app="b"}[5m]))`,
		`min by (detected_level) (rate({app="a"}[5m]))`, `sum by (service_name) (count_over_time({app="a"}[5m]))`,
		`sum by (k8s.pod.name) (rate({app="a"}[5m]))`,
	}
	delegatedOperands := []string{
		`sum without (pod) (rate({app="a"}[5m]))`, `stddev by (app) (rate({app="a"}[5m]))`,
		`sum by (app) (count_over_time({app="a"} | json [5m]))`, `sum_over_time({app="a"} | unwrap n [5m])`,
		`vector(1)`, `topk(2, sum by (app) (rate({app="a"}[5m])))`, `(rate({app="a"}[5m]) * 2)`,
		`label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)")`, `{app="a"}`, `sum(sum by (app) (rate({app="a"}[5m])))`,
	}
	numbers := []string{"2", "100", "0.5", "-1", "1e3", "1.50", "0", "+5", "1000000", "0.001"}
	// every operator between every pair of a few operands of each kind.
	pool := []string{vectors[0], vectors[1], vectors[5], vectors[6], vectors[7], vectors[11], "2", "0.5"}
	for _, op := range ops {
		for _, l := range pool {
			for _, r := range pool {
				add(fmt.Sprintf(`%s %s %s`, l, op, r))
			}
		}
	}
	// every vector operand against a number and against a vector, both sides.
	for _, v := range vectors {
		for _, op := range []string{"/", "*", "-", ">", "=="} {
			add(fmt.Sprintf(`%s %s 100`, v, op))
			add(fmt.Sprintf(`100 %s %s`, op, v))
			add(fmt.Sprintf(`%s %s %s`, v, op, vectors[6]))
		}
	}
	// number spellings.
	for _, n := range numbers {
		add(fmt.Sprintf(`%s * %s`, vectors[6], n))
		add(fmt.Sprintf(`%s / %s`, n, vectors[0]))
		add(fmt.Sprintf(`%s -%s`, vectors[0], n))
	}
	// bool on every comparison.
	for _, op := range []string{"==", "!=", ">=", "<=", ">", "<"} {
		add(fmt.Sprintf(`%s %s bool 0`, vectors[6], op))
		add(fmt.Sprintf(`%s %s bool %s`, vectors[5], op, vectors[6]))
	}
	// vector matching clauses and their spellings.
	for _, m := range []string{
		`on(app)`, `on (app)`, `on()`, `on ()`, `on(app, env)`, `on(app,env)`, `on( app )`, `ignoring(pod)`, `ignoring()`, `ignoring (pod, node)`,
		`on(app) group_left(pod)`, `on(app) group_left`, `on(app) group_left()`, `on(app) group_left (pod)`, `on(app) group_right(x, y)`,
		`ignoring(pod) group_left(level)`, `ignoring(pod) group_right`, `on(app)group_left(pod)`, `ON(app)`, `on(app) GROUP_LEFT(pod)`,
	} {
		for _, op := range []string{"/", "+", ">"} {
			add(fmt.Sprintf(`%s %s %s %s`, vectors[6], op, m, vectors[7]))
			add(fmt.Sprintf(`%s %s %s %s`, vectors[0], op, m, vectors[1]))
		}
	}
	// operands that stay delegated, both sides.
	for _, d := range delegatedOperands {
		add(fmt.Sprintf(`%s / %s`, d, vectors[6]))
		add(fmt.Sprintf(`%s * %s`, vectors[0], d))
		add(fmt.Sprintf(`%s > 2`, d))
	}
	// nested binary operations.
	for _, q := range []string{
		`rate({app="a"}[5m]) + rate({app="b"}[5m]) + rate({app="c"}[5m])`, `rate({app="a"}[5m]) / rate({app="b"}[5m]) * 100`,
		`sum(rate({app="a"}[5m])) / (sum(rate({app="b"}[5m])) + 1)`, `(rate({app="a"}[5m]) + 1) * 2`, `2 * 3 * rate({app="a"}[5m])`,
		`vector(1) + vector(2)`, `1 + 2`, `rate({app="a"}[5m]) ^ 2 ^ 3`, `-rate({app="a"}[5m])`, `rate({app="a"}[5m]) * -rate({app="b"}[5m])`,
	} {
		add(q)
	}
	// operator and blank spellings.
	for _, q := range []string{
		`sum(rate({app="a"}[5m]))/2`, `sum(rate({app="a"}[5m])) /2`, `sum(rate({app="a"}[5m]))/ 2`, `sum(rate({app="a"}[5m]))  / 2`,
		`sum(rate({app="a"}[5m])) /  2`, "sum(rate({app=\"a\"}[5m]))\t/ 2", "sum(rate({app=\"a\"}[5m]))\n/ 2", "sum(rate({app=\"a\"}[5m])) /\n2",
		` sum(rate({app="a"}[5m])) / 2 `, `(sum(rate({app="a"}[5m]))) / 2`, `sum(rate({app="a"}[5m])) / (2)`, `((sum(rate({app="a"}[5m])))) * 2`,
		`sum( rate({app="a"}[5m])) / 2`, `sum (rate({app="a"}[5m])) / 2`, `sum(rate ({app="a"}[5m])) / 2`, `SUM BY (app) (RATE({app="a"}[5m])) / 2`,
		`sum by (app) (rate({app="a"}[5m])) > BOOL 2`, `sum by (app)  (rate({app="a"}[5m])) / 2`, `rate({app="a"}  [5m]) / 2`,
		`sum by (bool) (rate({app="a"}[5m])) / 2`, `rate({app="a"}[5m]) / on(bool) rate({app="b"}[5m])`,
		`sum by ( bool ) (rate({app="a"}[5m])) / 2`, `rate({app="a"}[5m]) / on( bool ) rate({app="b"}[5m])`,
		`sum by (a, bool , b) (rate({app="a"}[5m])) > 1`, `rate({app="a"}[5m]) / ignoring( without ) rate({app="b"}[5m])`,
		`sum by ( app ) (rate({app="a"}[5m])) / on( app ) sum by ( app ) (rate({app="b"}[5m]))`,
	} {
		add(q)
	}
	// quoted text the translator's scans read as syntax.
	for _, line := range []string{
		` |= "("`, ` |= ")"`, ` |~ "(?i)err"`, ` |= " on(x)"`, ` |= "a bool b"`, ` |= "a  b"`, ` |= "|||"`, ` |= "@@@on:x"`,
		` |= "__binary__:/:"`, ` |= "__lvp_lr:x__"`, ` |= "without"`, ` |= " / "`, ` |= "a > b"`, " |= `(x)`", ` |= "group_left"`,
		` |= "on"`, ` |= "x"`,
	} {
		add(fmt.Sprintf(`sum by (app) (rate({app="a"}%s[5m])) / 2`, line))
		add(fmt.Sprintf(`rate({app="a"}%s[5m]) * rate({app="b"}[5m])`, line))
		add(fmt.Sprintf(`label_replace(sum by (app) (rate({app="a"}%s[5m])), "d", "$1", "app", "(.*)")`, line))
		add(fmt.Sprintf(`label_replace(rate({app="a"}%s[5m]) / 2, "d", "$1", "app", "(.*)")`, line))
	}
	// label_replace around each lowered expression, nested, with argument spellings, and around delegated shapes.
	args := []string{
		`"d", "$1", "app", "(.*)"`, `"d","$1","app","(.*)"`, "`d`, `$1`, `app`, `(.*)`", `"", "", "", ""`, `"d", "${1}-x", "app", "a(.*)b"`,
		`"d", "\\1", "app", "\\d+"`, `"d", "$1", "app", "*bad"`, `"d", "é", "app", "(é)"`, `"d", "a\"b", "app", ".*"`,
	}
	for _, in := range append(append([]string{}, vectors...),
		`sum without (pod) (rate({app="a"}[5m]))`, `count without (a, b) (count_over_time({app="a"}[5m]))`,
		`sum by (app) (rate({app="a"}[5m])) * 2`, `rate({app="a"}[5m]) / on(app) rate({app="b"}[5m])`, `2 * sum(rate({app="a"}[5m]))`,
		`sum by (detected_level) (count_over_time({app="a"}[5m]))`, `sum by (app, service_name) (rate({app="a"}[5m]))`,
		`stddev by (app) (rate({app="a"}[5m]))`, `topk(2, sum by (app) (rate({app="a"}[5m])))`, `vector(1)`, `sum by (app) (count_over_time({app="a"} | json [5m]))`,
		`sum(rate({app="a"}[5m])) and sum(rate({app="b"}[5m]))`, `rate({app="a"}[5m]) > bool 1`, `(sum(rate({app="a"}[5m])))`, `sum( rate({app="a"}[5m]))`,
	) {
		add(fmt.Sprintf(`label_replace(%s, %s)`, in, args[0]))
		add(fmt.Sprintf(`label_replace(label_replace(%s, %s), "e", "$1", "d", "(.*)")`, in, args[0]))
	}
	for _, a := range args {
		add(fmt.Sprintf(`label_replace(sum by (app) (rate({app="a"}[5m])), %s)`, a))
		add(fmt.Sprintf(`label_replace(rate({app="a"}[5m]) * 2, %s)`, a))
	}
	for _, q := range []string{
		`label_replace (sum by (app) (rate({app="a"}[5m])), "d", "$1", "app", "(.*)")`, `LABEL_REPLACE(sum by (app) (rate({app="a"}[5m])), "d", "$1", "app", "(.*)")`,
		`  label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)")  `, `label_replace( rate({app="a"}[5m]) , "d", "$1", "app", "(.*)")`,
		`label_replace(rate({app="a"}[5m]),"d","$1","app","(.*)")`, `label_replace(label_replace(label_replace(rate({app="a"}[5m]), "a", "$1", "app", "(.*)"), "b", "$1", "a", "(.*)"), "c", "$1", "b", "(.*)")`,
		`label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)") / 2`, `sum by (d) (label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)"))`,
		`label_replace(sum by (app) (rate({app="a"}[5m])) / label_replace(sum by (app) (rate({app="b"}[5m])), "d", "$1", "app", "(.*)"), "e", "$1", "app", "(.*)")`,
	} {
		add(q)
	}
	return out
}

// TestBinOpGridShadow holds the lowering of binary operations and label_replace calls to the string translator on
// the generated grid under every profile: 0 mismatches, the text byte for byte, and the rows each side answers are
// counted.
// conformance: architecture-syntax-package-layering
func TestBinOpGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := binOpGrid()
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
	if native < rows/4 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
