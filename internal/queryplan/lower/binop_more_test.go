package lower

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The text a binary operation is split at, as the translator leaves it: the clauses removed with the blanks folded,
// the labels read as the translator reads them; a clause the plan does not hold, or holds with other labels, is a
// spelling the plan does not hold.
//
// conformance: architecture-syntax-package-layering
func TestDropWithoutClauses(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	plan := func(q string) *queryplan.BinOp {
		t.Helper()
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		return p.Root.(*queryplan.BinOp)
	}
	for _, c := range []struct {
		plan, text, want string
		err              bool
	}{
		{`sum without (pod) (` + rate + `) / 2`, `sum without (pod) (` + rate + `) / 2`, `sum (` + rate + `) / 2`, false},
		{`sum(` + rate + `) without (pod) / 2`, `sum(` + rate + `) without (pod) / 2`, `sum(` + rate + `) / 2`, false},
		{`sum without (pod,node) (` + rate + `) / 2`, `sum without( pod ,node ) (` + rate + `) / 2`, `sum (` + rate + `) / 2`, false},
		{`sum without (a) (` + rate + `) / min without (b) (` + rate + `)`, `sum without (a) (` + rate + `) / min without (b) (` + rate + `)`, `sum (` + rate + `) / min (` + rate + `)`, false},
		{`2 / sum(` + rate + `) without (pod)`, `2 / sum(` + rate + `) without (pod)`, `2 / sum(` + rate + `) `, false},
		{rate + ` / 2`, rate + ` / 2`, rate + ` / 2`, false},
		// a clause the plan does not hold, other labels, the regexp's misses.
		{`sum without (pod) (` + rate + `) / 2`, `sum without (node) (` + rate + `) / 2`, "", true},
		{`sum without (pod, node) (` + rate + `) / 2`, `sum without (pod) (` + rate + `) / 2`, "", true},
		{`sum without (pod) (` + rate + `) / 2`, `sum xwithout (pod) (` + rate + `) / 2`, "", true},
		{`sum without (pod) (` + rate + `) / 2`, `sum without () (` + rate + `) / 2`, "", true},
		{`sum without (pod) (` + rate + `) / 2`, `sum without (pod) (` + rate + `) / 2 without (pod)`, "", true},
		{rate + ` / 2`, `sum without (pod) (` + rate + `) / 2`, "", true},
	} {
		got, err := dropWithoutClauses(c.text, plan(c.plan))
		if (err != nil) != c.err || got != c.want {
			t.Errorf("%s over %s: got %q, %v, want %q (error %v)", c.text, c.plan, got, err, c.want, c.err)
		}
	}
}

// The bool modifier is one `bool` with blanks on each side; the blank the translator leaves is where it stood.
//
// conformance: architecture-syntax-package-layering
func TestDropBool(t *testing.T) {
	for _, c := range []struct {
		text, want string
		at         int
		bool, err  bool
	}{
		{`a > bool 1`, `a > 1`, 3, true, false},
		{"a >\n bool\t1", `a > 1`, 3, true, false},
		{`a > bool on(x) b`, `a > on(x) b`, 3, true, false},
		{`a > 1`, `a > 1`, -1, false, false},
		// the word anywhere else, or twice, or without blanks, or with no modifier in the plan.
		{`sum by (bool) (a) > bool 1`, "", 0, true, true},
		{`a > bool 1`, "", 0, false, true},
		{`a > bool1`, "", 0, true, true},
		{`a >bool 1`, "", 0, true, true},
		{`a > 1`, "", 0, true, true},
	} {
		got, at, err := dropBool(c.text, c.bool)
		if (err != nil) != c.err || (err == nil && (got != c.want || at != c.at)) {
			t.Errorf("%q (bool %v): got %q at %d, %v; want %q at %d (error %v)", c.text, c.bool, got, at, err, c.want, c.at, c.err)
		}
	}
}

// The screen leaves to the translator a bool that is not right after the operator, and clauses or labels it reads
// unlike the plan.
//
// conformance: architecture-syntax-package-layering
func TestScreenBinOpMore(t *testing.T) {
	a, b := `rate({app="a"}[5m])`, `rate({app="b"}[5m])`
	plan := func(q string) *queryplan.BinOp {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		return p.Root.(*queryplan.BinOp)
	}
	for _, c := range []struct {
		plan, text string
		want       error
	}{
		{a + ` > bool ` + b, a + ` > bool ` + b, nil},
		{a + ` > bool ` + b, a + ` bool > ` + b, errBinSpelling},
		{a + ` > bool on(app) ` + b, a + ` > bool on(app) ` + b, nil},
		{a + ` > ` + b, a + ` > bool ` + b, errBinSpelling},
		{`sum without (pod) (` + a + `) / 2`, `sum without (pod) (` + a + `) / 2`, nil},
		{`sum without (pod) (` + a + `) / 2`, `sum without (node) (` + a + `) / 2`, errBinSpelling},
		{`sum without (pod) (` + a + `) / 2`, `sum (` + a + `) without (pod) / 2`, nil},
		{a + ` / 2`, `sum without (pod) (` + a + `) / 2`, errBinSpelling},
	} {
		if got := screenBinOp(c.text, plan(c.plan)); got != c.want {
			t.Errorf("%s over %s: got %v, want %v", c.text, c.plan, got, c.want)
		}
	}
}

// A binary operation with a bool modifier or a without clause lowers to the operators the string translator writes
// and allocates no more than it does, with and without the tenant's stream labels; the shapes where it would are
// left to the translator.
//
// conformance: architecture-syntax-package-layering
func TestBinOpMoreLoweringAllocatesNoMoreThanTheTranslator(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	checked, corpus := 0, 0
	grid := binOpMoreAllocGrid()
	for _, label := range []func(string) string{nil, benchStreamLabels} {
		for _, q := range grid {
			p := rangeAggPlan(q)
			if p == nil {
				continue
			}
			if _, ok := p.Root.(*queryplan.BinOp); !ok {
				continue
			}
			if _, err := PlanQuery(q, p, Profile{Label: label}); err != nil {
				continue
			}
			tr := testing.AllocsPerRun(5, func() { _, _ = translator.TranslateLogQLWithLabels(q, label) })
			lo := testing.AllocsPerRun(5, func() {
				expr, _ := logql.Parse(logql.CanonicalizeLogRanges(q))
				pl, _ := queryplan.Build(expr, queryplan.Inputs{})
				queryplan.Resolve(pl)
				_, _ = PlanQuery(q, pl, Profile{Label: label})
			})
			checked++
			if lo > tr {
				t.Errorf("label=%v: %s: lowered %.0f allocs, translator %.0f", label != nil, q, lo, tr)
			}
		}
	}
	for _, q := range binOpMoreCorpus() {
		if p := rangeAggPlan(q); p != nil {
			if _, err := PlanQuery(q, p, Profile{}); err == nil {
				corpus++
			}
		}
	}
	t.Logf("%d shapes checked (%d grid and corpus queries, two label modes), %d of them corpus queries", checked, len(grid), corpus)
	if checked < 300 || corpus < 20 {
		t.Fatalf("only %d lowered binary operations checked, %d from the corpus", checked, corpus)
	}
}

// binOpMoreAllocGrid is the binary operations with a bool modifier or a without clause of the plan corpus, and
// the ones over the range aggregations with a parser, a label filter and an unwrapped value.
func binOpMoreAllocGrid() []string {
	qs := binOpMoreCorpus()
	ranges := []string{`rate({app="a"}[5m])`, `count_over_time({app="a"} |= "x" [1m])`, `bytes_over_time({app="a"} | json | status >= 500 [5m])`,
		`rate({app="a"} | logfmt [5m])`, `max_over_time({app="a"} | json | unwrap amount [5m])`, `rate({app="a"} | unwrap amount [150s])`,
		`absent_over_time({app="a"}[5m])`, `sum_over_time({app="a"} | logfmt | level="error" | unwrap amount [5m])`,
		`rate({app="a"} | json | drop a [5m])`, `count_over_time({app="a"} | logfmt | keep a [5m])`, `rate({app="a"} | regexp "(?P<a>x)" [5m])`, `count_over_time({app="a"} | pattern "<a> <b>" [5m])`,
		`count_over_time({app="a"} | line_format "{{.a}}" [5m])`, `count_over_time({app="a"} | label_format b=a [5m])`, `bytes_rate({app="a"} | decolorize [5m])`,
		`count_over_time({app="a"} | json | a="b" | c="d" [5m])`, `avg_over_time({app="a"} | json | unwrap duration(t) [5m])`, `rate({app="a"} | logfmt | unwrap bytes(t) [5m])`,
		`count_over_time({app="a"} | json a="b" [5m])`, `count_over_time({app="a"} | status >= 500 [5m])`}
	for _, in := range ranges {
		for _, agg := range []string{"sum", "count", "max", "avg"} {
			for _, f := range []string{`%[1]s without (pod) (%[2]s) / 2`, `%[1]s(%[2]s) without (pod, node) > 1`, `2 * %[1]s without (pod) (%[2]s)`,
				`%[1]s without (pod) (%[2]s) > bool %[2]s`, `%[2]s / on(app) %[1]s without (pod) (%[2]s)`, `%[1]s by (app) (%[2]s) > bool 1`} {
				qs = append(qs, fmt.Sprintf(f, agg, in))
			}
		}
		for _, op := range []string{"==", "!=", ">=", "<=", ">", "<"} {
			qs = append(qs, in+` `+op+` bool 0`, in+` `+op+` bool on(app) `+ranges[0])
		}
	}
	return qs
}

// binOpMoreCorpus is the plan corpus's binary operations (under label_replace calls too) with a bool modifier or a
// without clause on an operand.
func binOpMoreCorpus() []string {
	all, err := plancorpus.Queries("../../..")
	if err != nil {
		panic(err)
	}
	var out []string
	for _, q := range all {
		p := rangeAggPlan(q)
		if p == nil {
			continue
		}
		x := p.Root
		for {
			lr, ok := x.(*queryplan.LabelReplace)
			if !ok {
				break
			}
			x = lr.In
		}
		if b, ok := x.(*queryplan.BinOp); ok {
			if _, withoutClauses := plannedWithout(b); b.Bool || withoutClauses > 0 {
				out = append(out, q)
			}
		}
	}
	return out
}

// FuzzBinOpMoreLowering holds the lowering of a binary operation with a bool modifier or a without clause to the
// string translator on any aggregation, label list, spelling of the clause and the modifier, operator, vector
// matching, number, the words as labels, and the values and line filters of the operands: both write the same
// operators and text, or the lowering leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzBinOpMoreLowering(f *testing.F) {
	f.Add("a", "x", "pod", uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "a  b", "bool", uint8(7), uint8(3), uint8(5), uint8(9), uint8(1))
	f.Add("k8s.pod", "without (x)", "without", uint8(200), uint8(91), uint8(11), uint8(70), uint8(2))
	f.Add("é", "bool", "node", uint8(13), uint8(250), uint8(130), uint8(255), uint8(3))
	f.Fuzz(func(t *testing.T, sel, line, label string, opSel, left, right, mod, form uint8) {
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 {
				return "x"
			}
			return b.String()
		}
		lbl := ident(label)
		labels := []string{lbl, lbl + `, app`, lbl + `,app`, ` ` + lbl + ` `, ``, ` `, `k8s.pod.name`, `service_name`}[form%8]
		selector := `{app="` + escapeValue(sel) + `"}`
		filter := []string{``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` | ` + lbl + `="` + escapeValue(line) + `"`}[mod%4]
		inner := []string{"rate", "count_over_time", "bytes_rate", "bytes_over_time", "absent_over_time"}[left%5] + `(` + selector + filter + `[5m])`
		agg := []string{"sum", "count", "min", "max", "avg", "stddev", "topk"}[left>>3%7]
		operand := func(k uint8) string {
			switch k % 9 {
			case 0:
				return inner
			case 1:
				return agg + ` without (` + labels + `) (` + inner + `)`
			case 2:
				return agg + `(` + inner + `) without (` + labels + `)`
			case 3:
				return agg + ` without(` + labels + `)(` + inner + `)`
			case 4:
				return `sum by (` + lbl + `) (` + inner + `)`
			case 5:
				return []string{"0.5", "-1", "1e3", "100", "2"}[k/9%5]
			case 6:
				return `sum(` + inner + `)`
			case 7:
				return `sum without (` + lbl + `) (sum by (app) (` + inner + `))`
			}
			return `vector(` + strconv.Itoa(int(k)) + `)`
		}
		op := []string{"/", "*", "+", "-", "%", "^", "==", "!=", ">=", "<=", ">", "<", "and", "or", "unless"}[opSel%15]
		if opSel&0x80 != 0 {
			switch op {
			case "==", "!=", ">=", "<=", ">", "<":
				op += []string{" bool", "  bool", "\nbool", " BOOL", "\tbool"}[opSel>>4%5]
			}
		}
		matching := []string{``, ` on(` + lbl + `)`, ` ignoring(` + lbl + `)`, ` on()`, ` on(` + lbl + `, app) group_left(x)`,
			` ignoring(` + lbl + `) group_right`, ` on (` + lbl + `)`, ` on(` + lbl + `,app)`}[mod>>2%8]
		q := operand(left>>3) + ` ` + op + matching + ` ` + operand(right)
		if mod&0x80 != 0 {
			q = `label_replace(` + q + `, "d", "$1", "` + lbl + `", "(.*)")`
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, construct, ok := binOpLowering(t, q, prof); ok && construct == "" && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}

var _ = errors.Is
