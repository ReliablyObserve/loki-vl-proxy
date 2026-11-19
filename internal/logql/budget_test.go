package logql

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
)

// tripBudget runs out at its tripAt-th question (never for tripAt 0) and counts the questions.
type tripBudget struct{ calls, tripAt int }

func (b *tripBudget) Exhausted() bool {
	b.calls++
	return b.tripAt > 0 && b.calls >= b.tripAt
}

// budgetDeepShapes are the deep shapes the parse, the calls it reads again and the semantic pass take longest on,
// each nested past the budget's first question.
func budgetDeepShapes() map[string]string {
	shapes := map[string]string{
		"parentheses": strings.Repeat("(", 3000) + "1" + strings.Repeat(")", 3000),
		"sums":        strings.Repeat("sum(", 800) + `rate({a="b"}[1m])` + strings.Repeat(")", 800),
		"binary":      strings.Repeat("1+", 3000) + "1",
		"json stages": `{a="b"}` + strings.Repeat(" | json", 1500),
		"range forms": strings.Repeat("sum(", 300) + `count_over_time(({a="b"} |= "x")[1m])` + strings.Repeat(")", 300),
	}
	for _, s := range deepCallShapes {
		shapes["calls "+s.name] = deepCall(s, 300)
	}
	return shapes
}

// checkUnderBudget validates q with a budget that runs out at its tripAt-th question (never for 0) and holds the
// result to the unbudgeted validation: a trip answers nothing else, and a validation the budget does not cut short is
// the unbudgeted one, byte for byte. It returns the questions asked and whether the budget tripped.
func checkUnderBudget(t testing.TB, q string, tripAt int) (asked int, tripped bool) {
	t.Helper()
	b := &tripBudget{tripAt: tripAt}
	expr, msg, build, exhausted := ValidateLogQLPhasesWithin(q, b)
	if exhausted {
		if expr != nil || msg != "" || build != "" {
			t.Fatalf("%.120q tripped at question %d and still answered %v %q %q", q, tripAt, expr, msg, build)
		}
		return b.calls, true
	}
	wantExpr, wantMsg, wantBuild := ValidateLogQLPhases(q)
	if msg != wantMsg || build != wantBuild || (expr == nil) != (wantExpr == nil) || expr != nil && expr.String() != wantExpr.String() {
		t.Fatalf("%.120q under a budget (trip at %d) answered %q %q, without one %q %q", q, tripAt, msg, build, wantMsg, wantBuild)
	}
	return b.calls, false
}

// The validation asks the budget as it parses, reads calls again and walks the tree, and stops once it runs out:
// over the plan corpus and the deep shapes, a budget that runs out at any question (every one from the first until the
// validation completes) is reported as a trip and answers nothing else, and one that never runs out answers what the
// validation without a budget answers. A query of fewer steps than the first question is never cut short: its verdict
// (Loki's rejection included) is always made.
//
// conformance: query-complexity-limits
func TestValidateUnderBudgetStopsAnywhere(t *testing.T) {
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if _, tripped := checkUnderBudget(t, q, 1); tripped {
			t.Errorf("%q: a corpus query tripped a budget out from the first question", q)
		}
	}
	for name, q := range budgetDeepShapes() {
		t.Run(name, func(t *testing.T) {
			asked, tripped := checkUnderBudget(t, q, 0)
			if tripped || asked == 0 {
				t.Fatalf("a budget that never runs out was asked %d times (tripped %v)", asked, tripped)
			}
			for k := 1; k <= asked; k++ {
				if _, tripped := checkUnderBudget(t, q, k); !tripped {
					t.Fatalf("a budget out at question %d of %d did not stop the validation", k, asked)
				}
			}
		})
	}
	// Loki's rejection of a short query is made whatever the budget says.
	for _, q := range []string{`sum(rate({a="b"}[1m])) by (`, `{a="b"} | json |= `, `label_replace(rate({a="b"}[1m]), "d", "$1", "s", "(")`} {
		if _, tripped := checkUnderBudget(t, q, 1); tripped {
			t.Errorf("%q: a short query tripped the budget", q)
		}
	}
}

// The deep shapes stop soon after the budget runs out: a budget out at its first question leaves the parse at the
// first check, a few hundred tokens past the first 1,024, whatever the query's size.
//
// conformance: query-complexity-limits
func TestValidateUnderBudgetStopsSoon(t *testing.T) {
	for name, q := range budgetDeepShapes() {
		b := &tripBudget{tripAt: 1}
		if _, _, _, exhausted := ValidateLogQLPhasesWithin(q, b); !exhausted {
			t.Errorf("%s: not stopped", name)
		}
		if b.calls != 1 {
			t.Errorf("%s: asked %d times after the budget ran out", name, b.calls)
		}
	}
}

// For any query and any question the budget runs out at, the validation answers a trip and nothing else, or the
// unbudgeted validation's answer; it never panics.
func FuzzValidateUnderBudget(f *testing.F) {
	for _, q := range budgetDeepShapes() {
		if len(q) < 4096 {
			f.Add(q, uint16(1))
		}
	}
	for _, q := range []string{
		strings.Repeat("(", 600) + "1" + strings.Repeat(")", 600), strings.Repeat("1+", 700) + "1",
		strings.Repeat(`label_replace(`, 40) + `rate({a="b"}[1m])` + strings.Repeat(`, "d", "$1", "s", "(.*)")`, 40),
		strings.Repeat("sum(", 300) + `rate({a="b"}[1m])` + strings.Repeat(")", 300), `{a="b"}` + strings.Repeat(" | json", 600),
		`sum(rate({a="b"}[1m])) by (`, `vector(x)`, `label_replace(rate({a="b"}[1m]), "d", "$1", "s")`,
	} {
		f.Add(q, uint16(1))
		f.Add(q, uint16(3))
	}
	f.Fuzz(func(t *testing.T, q string, tripAt uint16) {
		if len(q) > 8192 {
			t.Skip()
		}
		checkUnderBudget(t, q, int(tripAt))
	})
}

// BenchmarkValidateUnderBudget is the validation of a dashboard query and of a deep one without a budget and under one
// that never runs out (a clock read is not part of it): the cost of asking.
func BenchmarkValidateUnderBudget(b *testing.B) {
	for name, q := range map[string]string{
		"dashboard": `sum by (namespace, pod) (rate({cluster="prod", namespace=~"team-.+"} |= "level=error" != "healthcheck" | logfmt | status >= 500 [5m]))`,
		"deep":      strings.Repeat("sum(", 800) + `rate({a="b"}[1m])` + strings.Repeat(")", 800),
	} {
		b.Run(name+"/none", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _, _ = ValidateLogQLPhasesWithin(q, nil)
			}
		})
		b.Run(name+"/budget", func(b *testing.B) {
			budget := &tripBudget{}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _, _ = ValidateLogQLPhasesWithin(q, budget)
			}
		})
	}
}
