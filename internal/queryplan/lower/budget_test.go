package lower

import (
	"errors"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

type fixedBudget bool

func (b fixedBudget) Exhausted() bool { return bool(b) }

func budgetPlan(t *testing.T, q string) *queryplan.Plan {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	queryplan.Resolve(p)
	return p
}

// A lowering whose Profile budget is exhausted stops with ErrBudgetExhausted at the construct it is at, whatever the
// construct, and is not read as a construct that is not lowered; a budget that is not exhausted changes nothing.
//
// conformance: query-complexity-limits
func TestLoweringStopsWhenItsBudgetIsExhausted(t *testing.T) {
	for name, q := range map[string]string{
		"log select":    `{a="b"} |= "x"`,
		"range":         `rate({a="b"}[1m])`,
		"vector":        `sum by (a) (rate({a="b"}[1m]))`,
		"binary":        `rate({a="b"}[1m]) / rate({a="b"}[1m])`,
		"label_replace": `label_replace(rate({a="b"}[1m]), "x", "$1", "a", "(.*)")`,
		"rank":          `topk(2, rate({a="b"}[1m]))`,
		"nested binary": `sum(rate({a="b"}[1m])) / (sum(rate({a="b"}[1m])) + sum(rate({a="b"}[1m])))`,
	} {
		t.Run(name, func(t *testing.T) {
			plan := budgetPlan(t, q)
			spent := Profile{Budget: fixedBudget(true)}
			if _, err := Plan(plan, spent); !errors.Is(err, ErrBudgetExhausted) || errors.Is(err, ErrNotLowered) {
				t.Errorf("Plan with a spent budget: %v", err)
			}
			if plan.Root != nil {
				if _, ok := plan.Root.(*queryplan.LogSelect); ok {
					if _, err := PlanLogText(plan, spent); !errors.Is(err, ErrBudgetExhausted) {
						t.Errorf("PlanLogText with a spent budget: %v", err)
					}
				}
			}
			want, werr := Plan(plan, Profile{})
			got, gerr := Plan(plan, Profile{Budget: fixedBudget(false)})
			if (werr == nil) != (gerr == nil) || (werr == nil && physical.Encode(want) != physical.Encode(got)) {
				t.Errorf("a budget that is not exhausted changed the lowering: %v %v", werr, gerr)
			}
		})
	}
}
