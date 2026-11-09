package proxy

import (
	"sync"
	"sync/atomic"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The handlers read Loki semantics of a query from its resolved logical plan
// (queryplan.Build, then queryplan.Resolve) instead of from its text. The plan
// is a function of the query text alone: no tenant, VictoriaLogs or time input
// enters it, so one memo serves every tenant.

// planMemoLimit is the number of plans one generation of the memo holds. The
// memo keeps two generations: when the current one is full it becomes the
// previous one and a new one starts, and a hit in the previous generation moves
// the plan into the current one. Memory stays bounded at two generations, the
// queries a dashboard keeps sending stay planned, and texts that were seen once
// (Grafana writes the time picker into the query) age out.
const planMemoLimit = 4096

type planGeneration struct {
	plans sync.Map // query text -> *planMemoEntry
	size  atomic.Int64
}

// planMemoEntry is a query's parse and, once a handler asked for it, its plan.
// Both are shared by every request that sends the query: nothing may modify
// them (a caller that rewrites the AST parses its own copy).
type planMemoEntry struct {
	expr logqlpkg.Expr
	plan *queryplan.Plan // resolved before it is stored; nil until built
}

var (
	planMemoMu   sync.Mutex
	planMemoCur  atomic.Pointer[planGeneration]
	planMemoPrev atomic.Pointer[planGeneration]
)

func init() { planMemoCur.Store(&planGeneration{}) }

// planMemoLoad is the memoised entry of query, if a generation holds it.
func planMemoLoad(query string) (*planMemoEntry, bool) {
	if v, ok := planMemoCur.Load().plans.Load(query); ok {
		return v.(*planMemoEntry), true
	}
	if prev := planMemoPrev.Load(); prev != nil {
		if v, ok := prev.plans.Load(query); ok {
			e := v.(*planMemoEntry)
			planMemoStore(query, e)
			return e, true
		}
	}
	return nil, false
}

// planMemoStore keeps e in the current generation, starting a new one when the
// current one is full. An entry with a plan replaces one without.
func planMemoStore(query string, e *planMemoEntry) {
	cur := planMemoCur.Load()
	if cur.size.Load() >= planMemoLimit {
		planMemoMu.Lock()
		if planMemoCur.Load() == cur {
			planMemoPrev.Store(cur)
			planMemoCur.Store(&planGeneration{})
		}
		planMemoMu.Unlock()
		cur = planMemoCur.Load()
	}
	if e.plan == nil {
		if _, loaded := cur.plans.LoadOrStore(query, e); !loaded {
			cur.size.Add(1)
		}
		return
	}
	if _, loaded := cur.plans.Swap(query, e); !loaded {
		cur.size.Add(1)
	}
}

// sharedLogQLExpr is query's parse from the memo, parsing it once on a miss.
// The expression is shared with every other request sending the query, so the
// caller reads it only: a caller that modifies the AST uses parseLogQLExpr.
func sharedLogQLExpr(query string) (logqlpkg.Expr, bool) {
	cacheable := len(query) <= validationCacheMaxQueryBytes
	if cacheable {
		if e, ok := planMemoLoad(query); ok {
			return e.expr, true
		}
	}
	expr, err := parseLogQLExpr(noFlowCtx, query)
	if err != nil {
		return nil, false
	}
	if cacheable {
		planMemoStore(query, &planMemoEntry{expr: expr})
	}
	return expr, true
}

// resolvedPlan is the resolved plan of query, memoised by the query text; nil
// when the query does not parse or build.
func resolvedPlan(query string) *queryplan.Plan {
	cacheable := len(query) <= validationCacheMaxQueryBytes
	var expr logqlpkg.Expr
	if cacheable {
		if e, ok := planMemoLoad(query); ok {
			if e.plan != nil {
				return e.plan
			}
			expr = e.expr
		}
	}
	if expr == nil {
		var err error
		if expr, err = parseLogQLExpr(noFlowCtx, query); err != nil {
			return nil
		}
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	// Resolve before the plan can be shared: it writes into the plan.
	queryplan.Resolve(p)
	if cacheable {
		planMemoStore(query, &planMemoEntry{expr: expr, plan: p})
	}
	return p
}

// queryOuterUngrouped reports a query whose outermost vector aggregation (under
// any label_replace) has no by or without clause: Loki answers it as one series
// without labels. A rank (topk, bottomk, sort, sort_desc) keeps its series.
func queryOuterUngrouped(query string) bool {
	p := resolvedPlan(query)
	return p != nil && p.Sem.Ungrouped
}

// queryDropsOnlyErrorAfterParser reports a query whose log pipeline (of a log
// query, or of the range aggregation under its vector aggregations, ranks and
// label_replace calls) ends with a parser followed only by `| drop __error__`
// (with or without __error_details__). That explicit opt-in keeps the lines a
// parser fails on, as VictoriaLogs' native stats count them.
func queryDropsOnlyErrorAfterParser(query string) bool {
	p := resolvedPlan(query)
	if p == nil {
		return false
	}
	s := pipelineUnderAggregations(p.Root)
	if s == nil || s.Sem == nil {
		return false
	}
	at := s.Sem.ErrorAt
	if s.Sem.ErrorPolicy != queryplan.ErrorDroppedLabel || at != len(s.Stages)-1 || at == 0 {
		return false
	}
	if _, parser := s.Stages[at-1].(*queryplan.Parse); !parser {
		return false
	}
	d := s.Stages[at].(*queryplan.Drop)
	if len(d.Matchers) != 0 {
		return false
	}
	for _, l := range d.Labels {
		if l != "__error__" && l != "__error_details__" {
			return false
		}
	}
	return true
}

// pipelineUnderAggregations is the log pipeline at the top of n: n itself, or
// the input of the range aggregation under label_replace calls, vector
// aggregations and ranks. nil for any other shape (a binary operation).
func pipelineUnderAggregations(n queryplan.Node) *queryplan.LogSelect {
	for {
		switch x := n.(type) {
		case *queryplan.LogSelect:
			return x
		case *queryplan.LabelReplace:
			n = x.In
		case *queryplan.VectorAgg:
			n = x.In
		case *queryplan.Rank:
			n = x.In
		case *queryplan.RangeAgg:
			return x.In
		default:
			return nil
		}
	}
}
