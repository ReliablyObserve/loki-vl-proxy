package proxy

import (
	"fmt"
	"sync"
	"testing"
)

// The plan memo keeps two generations of planMemoLimit plans: a full current generation becomes the previous one,
// a plan read from the previous one moves back, and memory never holds more than two generations.
func TestPlanMemoGenerations(t *testing.T) {
	planMemoMu.Lock()
	planMemoCur.Store(&planGeneration{})
	planMemoPrev.Store(nil)
	planMemoMu.Unlock()
	hot := `sum(rate({app="hot"}[5m]))`
	first := resolvedPlan(hot)
	if first == nil || resolvedPlan(hot) != first {
		t.Fatal("a repeated query is not served from the memo")
	}
	for i := 0; i < 3*planMemoLimit; i++ {
		resolvedPlan(fmt.Sprintf(`sum(rate({app="once-%d"}[5m]))`, i))
		if i%(planMemoLimit/2) == 0 {
			if resolvedPlan(hot) != first {
				t.Fatalf("after %d one-off queries the hot plan was built again", i)
			}
		}
	}
	held := planMemoCur.Load().size.Load()
	if prev := planMemoPrev.Load(); prev != nil {
		held += prev.size.Load()
	}
	if held > 2*planMemoLimit {
		t.Fatalf("the memo holds %d plans, more than two generations of %d", held, planMemoLimit)
	}
	if resolvedPlan(`sum(rate({app="once-0"}[5m]))`) == nil {
		t.Fatal("an aged-out query is not planned again")
	}
	long := `sum(rate({app="` + string(make([]byte, validationCacheMaxQueryBytes)) + `"}[5m]))`
	before := planMemoCur.Load().size.Load()
	resolvedPlan(long)
	if planMemoCur.Load().size.Load() != before {
		t.Fatal("a query longer than validationCacheMaxQueryBytes is kept")
	}
}

// Concurrent readers share one plan per text and the memo rolls over safely (run under -race).
func TestPlanMemoConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < planMemoLimit/2; i++ {
				q := fmt.Sprintf(`sum by (a) (count_over_time({app="c-%d"} | json | drop __error__ [1m]))`, i%512)
				if !queryDropsOnlyErrorAfterParser(q) || queryOuterUngrouped(q) {
					t.Errorf("%q answered wrong", q)
					return
				}
				resolvedPlan(fmt.Sprintf(`rate({app="g%d-%d"}[1m])`, g, i))
			}
		}(g)
	}
	wg.Wait()
}

// A query's parse is shared: the read-only handlers and the plan read one parse, and asking for the plan after the
// parse keeps it (one entry per text, the plan added to it).
func TestSharedLogQLExprIsTheMemoisedParse(t *testing.T) {
	q := `sum by (level) (count_over_time({app="shared-parse"} |= "x" [1m]))`
	e1, ok := sharedLogQLExpr(q)
	if !ok {
		t.Fatal("the query does not parse")
	}
	if e2, _ := sharedLogQLExpr(q); e2 != e1 {
		t.Fatal("a repeated query is parsed again")
	}
	before := planMemoCur.Load().size.Load()
	p := resolvedPlan(q)
	if p == nil {
		t.Fatal("no plan")
	}
	if e3, _ := sharedLogQLExpr(q); e3 != e1 {
		t.Fatal("building the plan replaced the shared parse")
	}
	if resolvedPlan(q) != p {
		t.Fatal("the plan is built again")
	}
	if after := planMemoCur.Load().size.Load(); after > before+1 {
		t.Fatalf("one query holds %d entries", after-before)
	}
	if _, ok := sharedLogQLExpr(`sum(`); ok {
		t.Fatal("an invalid query parsed")
	}
}

// Concurrent requests read the shared parse and build the plan from it (run under -race).
func TestSharedLogQLExprConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				q := fmt.Sprintf(`sum(count_over_time({app="c-%d"}[5m]))`, i%17)
				if g%2 == 0 {
					sharedLogQLExpr(q)
					withoutAggregationOp(q)
					logQLProducesSamples(q)
				} else {
					resolvedPlan(q)
					totalsShapeOf(noFlowCtx, q)
				}
			}
		}(g)
	}
	wg.Wait()
}
