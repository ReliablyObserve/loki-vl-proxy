package proxy

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The label_replace calls the handlers apply are read from the query's plan (planLabelReplaces), where they read the
// markers of the translated query before: on every corpus query the handlers take, the calls at the root of the query
// (and of the operand a ranking is evaluated over) are the markers its translation carries, in the same order. A
// translation that carries markers for a query whose root is no label_replace is a binary operation or an aggregation,
// whose operands the handlers evaluate with their own calls (the markers were discarded there).
//
// conformance: architecture-syntax-package-layering
func TestPlanLabelReplacesAreTheMarkers(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	p, _ := newRouteProxy(t, &routeStub{}, false)
	defer func() { _ = p.Shutdown(context.Background()) }()
	ctx := context.Background()
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	check := func(q, translated string, root queryplan.Node) bool {
		_, post := translator.SplitPost(translated)
		plan := planLabelReplaces(q)
		if _, ok := root.(*queryplan.LabelReplace); ok {
			if fmt.Sprint(plan) != fmt.Sprint(post.LabelReplace) {
				t.Fatalf("%q: the plan's label_replace calls %v, the translation's markers %v", q, plan, post.LabelReplace)
			}
			return true
		}
		if len(plan) != 0 {
			t.Fatalf("%q: calls %v under a root that is no label_replace", q, plan)
		}
		switch root.(type) {
		case *queryplan.BinOp, *queryplan.VectorAgg, *queryplan.Rank:
		default:
			if len(post.LabelReplace) != 0 {
				t.Fatalf("%q: markers %v the handlers would not discard", q, post.LabelReplace)
			}
		}
		return false
	}
	roots, ranked := 0, 0
	for _, q := range qs {
		q = routeHandlerText(q)
		if expr, msg := logqlpkg.ValidateLogQLExpr(q); msg != "" || expr == nil {
			continue
		}
		pl := resolvedPlan(q)
		if pl == nil {
			continue
		}
		if translated, err := p.translateLogResponseQuery(ctx, q); err == nil && check(q, translated, pl.Root) {
			roots++
		}
		postAgg, ok := parseInstantMetricPostAggQuery(q)
		if !ok {
			continue
		}
		inner := resolvedPlan(postAgg.inner)
		if inner == nil {
			continue
		}
		if translated, err := p.translatePostAggOperand(ctx, postAgg); err == nil && check(postAgg.inner, translated, inner.Root) {
			ranked++
		}
	}
	if roots < 50 || ranked < 5 {
		t.Fatalf("label_replace roots checked: %d, ranked operands: %d", roots, ranked)
	}
}
