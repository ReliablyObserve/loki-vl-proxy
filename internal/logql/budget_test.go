package logql

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// syntaxError is the syntax verdict of q: the parser's, the lexer's or a call Loki's parser cannot read, "" for a
// query that parses (the checks over the tree are not run).
func syntaxError(q string) string {
	if msg := InputSizeError(q); msg != "" {
		return msg
	}
	if isWildcard(q) {
		return ""
	}
	expr, _, plainUnwrap, err := parseExpressionTree(q, nil)
	if err != nil {
		return formatParseError(err, q)
	}
	if plainUnwrap != nil {
		return plainUnwrap.Msg
	}
	return untypedCallsError(expr, q, nil)
}

// budgetMalformedShapes are deep queries with a syntax error past the budget's first question: at the end, in the
// middle, in a call kept as text, and a lexer error.
func budgetMalformedShapes() map[string]string {
	sums := strings.Repeat("sum(", 800) + `rate({a="b"}[1m])` + strings.Repeat(")", 800)
	return map[string]string{
		"open grouping":      sums + " by (",
		"middle":             strings.Repeat("1+", 1500) + "* *" + strings.Repeat("+1", 1500),
		"json stages":        `{a="b"}` + strings.Repeat(" | json", 1500) + " | json x=",
		"label_replace args": strings.Repeat("label_replace(", 300) + `rate({a="b"}[1m])` + strings.Repeat(`, "d", "$1", "s", "(.*)")`, 299) + `, "d", "$1", "s")`,
		"unclosed":           strings.Repeat("(", 3000) + "1" + strings.Repeat(")", 2999),
		"bad number":         strings.Repeat("1+", 1500) + "0x1g",
	}
}

// checkUnderBudget validates q with a budget that runs out at its tripAt-th question (never for 0) and holds the
// result to the unbudgeted validation: a trip answers nothing else and only for a query that parses (a syntax error is
// Loki's answer whatever the clock says), and a validation the budget does not cut short is the unbudgeted one, byte
// for byte. It returns the questions asked and whether the budget tripped.
func checkUnderBudget(t testing.TB, q string, tripAt int) (asked int, tripped bool) {
	t.Helper()
	b := &tripBudget{tripAt: tripAt}
	expr, msg, build, exhausted := ValidateLogQLPhasesWithin(q, b)
	if exhausted {
		if expr != nil || msg != "" || build != "" {
			t.Fatalf("%.120q tripped at question %d and still answered %v %q %q", q, tripAt, expr, msg, build)
		}
		if syntax := syntaxError(q); syntax != "" {
			t.Fatalf("%.120q tripped at question %d where Loki answers its syntax error %q", q, tripAt, syntax)
		}
		return b.calls, true
	}
	wantExpr, wantMsg, wantBuild := ValidateLogQLPhases(q)
	if msg != wantMsg || build != wantBuild || (expr == nil) != (wantExpr == nil) || expr != nil && expr.String() != wantExpr.String() {
		t.Fatalf("%.120q under a budget (trip at %d) answered %q %q, without one %q %q", q, tripAt, msg, build, wantMsg, wantBuild)
	}
	return b.calls, false
}

// The validation asks the budget as it parses, reads calls again and walks the tree: over the plan corpus and the deep
// shapes, a budget that runs out at any question (every one from the first until the validation completes) is reported
// as a trip that answers nothing else, only for a query that parses: a syntax error is Loki's answer at every trip
// point (the parse reads to its end). A budget that never runs out answers what the validation without a budget
// answers. A query of fewer steps than the first question is never cut short.
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
			// A shape Loki rejects for its syntax (the innermost call of three arguments) keeps its error at every trip
			// point; every other one is stopped.
			syntax := syntaxError(q)
			for k := 1; k <= asked; k++ {
				if _, tripped := checkUnderBudget(t, q, k); tripped == (syntax != "") {
					t.Fatalf("a budget out at question %d of %d: tripped %v, syntax error %q", k, asked, tripped, syntax)
				}
			}
		})
	}
	// A malformed query whose syntax error lies past the first question gets Loki's error at every trip point: the
	// parse reads to its end whatever the budget says.
	for name, q := range budgetMalformedShapes() {
		t.Run("malformed "+name, func(t *testing.T) {
			if syntaxError(q) == "" {
				t.Fatal("the shape parses")
			}
			asked, _ := checkUnderBudget(t, q, 0)
			for k := 1; k <= asked+1; k++ {
				if _, tripped := checkUnderBudget(t, q, k); tripped {
					t.Fatalf("tripped at question %d of %d", k, asked)
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

// A budget out at its first question is asked once: the parse reads on without asking again, and the checks over the
// tree stop, for every deep shape that parses.
//
// conformance: query-complexity-limits
func TestValidateUnderBudgetStopsSoon(t *testing.T) {
	for name, q := range budgetDeepShapes() {
		if syntaxError(q) != "" {
			continue
		}
		b := &tripBudget{tripAt: 1}
		if _, _, _, exhausted := ValidateLogQLPhasesWithin(q, b); !exhausted {
			t.Errorf("%s: not stopped", name)
		}
		if b.calls != 1 {
			t.Errorf("%s: asked %d times after the budget ran out", name, b.calls)
		}
	}
}

// Validations under budgets run at the same time, each with a budget check of its own taken from the pool and given
// back: every one answers what it answers alone, at every trip point.
//
// conformance: query-complexity-limits
func TestValidateUnderBudgetConcurrent(t *testing.T) {
	shapes := budgetDeepShapes()
	type want struct {
		msg, build string
		exhausted  bool
	}
	alone := map[string]want{}
	key := func(name string, tripAt int) string { return name + "/" + strconv.Itoa(tripAt) }
	for name, q := range shapes {
		for _, tripAt := range []int{0, 1, 3, 7} {
			_, msg, build, exhausted := ValidateLogQLPhasesWithin(q, &tripBudget{tripAt: tripAt})
			alone[key(name, tripAt)] = want{msg, build, exhausted}
		}
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				for name, q := range shapes {
					tripAt := []int{0, 1, 3, 7}[(g+i)%4]
					_, msg, build, exhausted := ValidateLogQLPhasesWithin(q, &tripBudget{tripAt: tripAt})
					if got := (want{msg, build, exhausted}); got != alone[key(name, tripAt)] {
						t.Errorf("%s trip at %d: %+v at the same time as others, %+v alone", name, tripAt, got, alone[key(name, tripAt)])
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// For any query and any question the budget runs out at, the validation answers a trip and nothing else (only for a
// query whose syntax Loki accepts), or the unbudgeted validation's answer; it never panics.
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
