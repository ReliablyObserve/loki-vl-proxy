package querycorpus_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The check that holds a binary operation with an operand that has a without clause to the proxy's binary route
// (checkProxyEvaluated, checkBinaryRoute) passes the lowering and fails each wrong one: the translator's reading (the clauses dropped
// from the operands and one fold over the whole operation), an operand without its fold, the fold's labels or
// operator changed, the operands swapped, another operator, the vector matching dropped, and an operand's request
// that is not its translation alone. Without it a lowering that drifts from the route would count as native.
//
// conformance: architecture-syntax-package-layering
func TestBinaryRouteCheckCatchesWrongLowerings(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	queries := []string{
		`sum without (pod) (rate({app="a"}[5m])) / 2`,
		`100 > max without (pod) (rate({app="uw"} | unwrap amount [5m]))`,
		`sum without (pod) (count_over_time({app="a"}[5m])) / on(app) group_left(env) sum by (app) (count_over_time({app="b"}[5m]))`,
		`sum by (app) (rate({app="b"}[5m])) - ignoring(pod) count(rate({app="a"}[5m])) without (pod, node)`,
	}
	for _, q := range queries {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queryplan.Resolve(plan)
		for _, prof := range planProfiles() {
			if prof.keepLine {
				continue
			}
			op, err := lower.PlanQuery(q, plan, prof.lowerProfile())
			if err != nil {
				t.Fatalf("%s: %s: %v", prof.name, q, err)
			}
			got, ok := op.(physical.Binary)
			if !ok || !lower.RoutedBinary(plan.Root) {
				t.Fatalf("%s: %s: lowered to %#v, want a binary operation held to the route", prof.name, q, op)
			}
			if msg := checkProxyEvaluated(q, prof, got); msg != "" {
				t.Fatalf("the lowering fails the route check: %s", msg)
			}
			legacy, _ := prof.translate(q)
			translatorReading, err := translator.DecodePhysical(legacy)
			if err != nil {
				t.Fatal(err)
			}
			for name, wrong := range map[string]physical.Op{
				"the translator's reading": translatorReading,
				"no fold on the operand":   mapFolds(got, func(f physical.Fold) physical.Op { return f.In }),
				"other fold labels": mapFolds(got, func(f physical.Fold) physical.Op {
					f.Levels = []physical.FoldLevel{{Op: f.Levels[0].Op, Labels: []string{"other"}, Without: true}}
					return f
				}),
				"other fold operator": mapFolds(got, func(f physical.Fold) physical.Op {
					f.Levels = []physical.FoldLevel{{Op: "min", Labels: f.Levels[0].Labels, Without: true}}
					return f
				}),
				"swapped operands": physical.Binary{Op: got.Op, Match: got.Match, Tail: got.Tail, L: got.R, R: got.L},
				"another operator": physical.Binary{Op: "atan2", Match: got.Match, Tail: got.Tail, L: got.L, R: got.R},
				"another request": mapFolds(got, func(f physical.Fold) physical.Op {
					f.In = physical.VLQuery{Text: physical.Encode(f.In) + " | limit 1"}
					return f
				}),
			} {
				if checkProxyEvaluated(q, prof, wrong) == "" {
					t.Errorf("%s: %s: %s passes the route check", prof.name, q, name)
				}
			}
			if got.Match != nil {
				if checkProxyEvaluated(q, prof, physical.Binary{Op: got.Op, L: got.L, R: got.R}) == "" {
					t.Errorf("%s: %s: no vector matching passes the route check", prof.name, q)
				}
			}
		}
	}
}

// mapFolds is b with each operand that is a Fold replaced by f of it.
func mapFolds(b physical.Binary, f func(physical.Fold) physical.Op) physical.Binary {
	out := b
	if v, ok := b.L.(physical.Fold); ok {
		out.L = f(v)
	}
	if v, ok := b.R.(physical.Fold); ok {
		out.R = f(v)
	}
	return out
}

// binaryRouteAllocs is what the proxy does for a binary operation with an operand that has a without clause before it
// asks VictoriaLogs (proxy.BinaryOperandsPhysical): the route's parse of the query, and for each operand that is not
// a number its printed LogQL translated alone (the canonical form of its ranges, then the translator) and decoded, as
// translateBinOpSide does. The proxy also translates the whole query first, and its translation cache and singleflight
// come on top, so this is the least the route costs.
func binaryRouteAllocs(rq string, label func(string) string) float64 {
	translate := func(q string) (string, error) {
		return translator.TranslateLogQLWithLabels(logql.CanonicalizeLogRanges(q), label)
	}
	return testing.AllocsPerRun(5, func() { _, _, _, _ = proxy.BinaryOperandsPhysical(rq, translate) })
}

// A binary operation with an operand that has a without clause allocates no more in the lowering (Parse, Build,
// Resolve, the text screen and Plan) than the proxy's binary route does for it (binaryRouteAllocs), with and without
// the tenant's stream labels, on every such corpus and grid row the lowering answers. The comparator is the route,
// not the string translator's text for the whole query, which the route does not send.
//
// conformance: architecture-syntax-package-layering
func TestBinaryRouteLoweringAllocatesNoMoreThanTheRoute(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	seen := map[string]bool{}
	var qs []string
	for _, q := range append(planCorpus(t), binOpMoreGrid()...) {
		if !seen[q] {
			seen[q] = true
			qs = append(qs, q)
		}
	}
	checked, worst, worstQuery, above := 0, 0.0, "", 0
	for _, label := range []func(string) string{nil, rankStreamLabels} {
		for _, q := range qs {
			expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
			if err != nil {
				continue
			}
			plan, err := queryplan.Build(expr, queryplan.Inputs{})
			if err != nil {
				continue
			}
			queryplan.Resolve(plan)
			if !lower.RoutedBinary(plan.Root) {
				continue
			}
			rq := routedQuery(q)
			if _, err := lower.PlanQuery(rq, plan, lower.Profile{Label: label}); err != nil {
				continue
			}
			lowered, route := rankLoweredAllocs(rq, label), binaryRouteAllocs(rq, label)
			checked++
			if lowered > route {
				above++
				t.Errorf("label=%v: %s: lowered %.0f allocs, the proxy's binary route %.0f", label != nil, q, lowered, route)
			}
			if route > 0 && lowered/route > worst {
				worst, worstQuery = lowered/route, q
			}
		}
	}
	t.Logf("%d lowered binary operations with a without operand checked against the binary route: %d above it, at most %.2f of it (%s)", checked, above, worst, worstQuery)
	if checked < 500 {
		t.Fatalf("only %d lowered shapes checked", checked)
	}
}
