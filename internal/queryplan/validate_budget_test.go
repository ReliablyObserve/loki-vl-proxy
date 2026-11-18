package queryplan

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

type countingBudget struct{ calls, tripAt int }

func (b *countingBudget) Exhausted() bool { b.calls++; return b.calls >= b.tripAt }

// ValidateWithin without a budget, or with one that is never exhausted, answers what Validate answers (the same
// rejection, the same text) on accepted and rejected trees; an exhausted budget answers the Exhausted rejection,
// asked at the first node and then every budgetCheckEvery nodes.
//
// conformance: query-complexity-limits
func TestValidateWithinBudget(t *testing.T) {
	for _, q := range []string{
		`rate({a="b"}[1m])`,
		`sum(rate({a="b"}[1m])) and 1`,
		`sort by (a) (rate({a="b"}[1m]))`,
		`quantile_over_time({a="b"} | unwrap x [1m])`,
		`label_replace({a="b"}, "a", "b", "c", "d")`,
		`1 + 2`,
	} {
		expr, err := logql.Parse(q)
		if err != nil {
			continue // a spelling the parser rejects is not a tree
		}
		want := Validate(expr)
		for name, got := range map[string]*Rejection{
			"nil budget":  ValidateWithin(expr, nil),
			"idle budget": ValidateWithin(expr, &countingBudget{tripAt: 1 << 30}),
			"first node":  ValidateWithin(expr, &countingBudget{tripAt: 1}),
		} {
			switch {
			case name == "first node":
				if got == nil || !got.Exhausted || got.Detail != "" {
					t.Errorf("%q %s: %+v", q, name, got)
				}
			case (want == nil) != (got == nil) || want != nil && want.Detail != got.Detail || got != nil && got.Exhausted:
				t.Errorf("%q %s: %+v, Validate says %+v", q, name, got, want)
			}
		}
	}
	deep, err := logql.Parse(strings.Repeat("sum(", 600) + `rate({a="b"}[1m])` + strings.Repeat(")", 600))
	if err != nil {
		t.Fatal(err)
	}
	b := &countingBudget{tripAt: 1 << 30}
	if r := ValidateWithin(deep, b); r != nil {
		t.Fatalf("an idle budget rejected: %+v", r)
	}
	if want := (601 + budgetCheckEvery - 1) / budgetCheckEvery; b.calls != want {
		t.Errorf("601 nodes asked the budget %d times, want %d", b.calls, want)
	}
	if r := ValidateWithin(deep, &countingBudget{tripAt: 2}); r == nil || !r.Exhausted {
		t.Errorf("a budget spent mid-walk: %+v", r)
	}
	if rej := (&Rejection{Detail: "x"}); rej.Exhausted {
		t.Error("a tree check's rejection is not an exhausted budget")
	}
}
