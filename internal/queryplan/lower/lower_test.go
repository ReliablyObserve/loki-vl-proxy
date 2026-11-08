package lower

import (
	"errors"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// Until a construct's lowering lands, Plan delegates it to the string translator with ErrNotLowered naming the
// construct at the top of the plan, under every profile.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesEveryConstruct(t *testing.T) {
	for construct, q := range map[string]string{
		"LogSelect":    `{app="a"} | json | level="error"`,
		"RangeAgg":     `rate({app="a"}[5m])`,
		"VectorAgg":    `sum by (app) (count_over_time({app="a"}[5m]))`,
		"Rank":         `topk(3, sum by (app) (rate({app="a"}[5m])))`,
		"BinOp":        `sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`,
		"LabelReplace": `label_replace(rate({app="a"}[5m]), "b", "$1", "app", "(.*)")`,
		"VectorLit":    `vector(1)`,
		"Scalar":       `1`,
	} {
		t.Run(construct, func(t *testing.T) {
			expr, err := logql.Parse(q)
			if err != nil {
				t.Fatal(err)
			}
			p, err := queryplan.Build(expr, queryplan.Inputs{})
			if err != nil {
				t.Fatal(err)
			}
			queryplan.Resolve(p)
			for _, prof := range []Profile{{}, {KeepLine: true}, {StreamFields: map[string]bool{"app": true}}} {
				op, err := Plan(p, prof)
				var nl *NotLoweredError
				if op != nil || !errors.Is(err, ErrNotLowered) || !errors.As(err, &nl) || nl.Construct != construct {
					t.Fatalf("Plan = %v, %v; want ErrNotLowered naming %s", op, err, construct)
				}
			}
		})
	}
}
