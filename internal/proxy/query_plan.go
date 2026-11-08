package proxy

import (
	"sync"
	"sync/atomic"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The handlers read Loki semantics of a query from its resolved logical plan
// (queryplan.Build, then queryplan.Resolve) instead of from its text. The plan
// is a function of the query text alone: no tenant, VictoriaLogs or time input
// enters it, so one memo serves every tenant.

// planMemoLimit bounds the plans kept: past it a plan is built per call and not kept.
const planMemoLimit = 4096

var (
	planMemo     sync.Map // query text -> *queryplan.Plan, resolved before it is stored
	planMemoSize atomic.Int64
)

// resolvedPlan is the resolved plan of query, or nil when the query does not
// parse or build. A handler reads only queries that validated, so nil means a
// text the proxy rewrote into something it cannot parse; callers then answer as
// for a query without the property they ask about. The plan is shared: callers
// read it and never change it.
func resolvedPlan(query string) *queryplan.Plan {
	cacheable := len(query) <= validationCacheMaxQueryBytes
	if cacheable {
		if v, ok := planMemo.Load(query); ok {
			return v.(*queryplan.Plan)
		}
	}
	expr, err := parseLogQLExpr(noFlowCtx, query)
	if err != nil {
		return nil
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	// Resolve before the plan can be shared: it writes into the plan.
	queryplan.Resolve(p)
	if cacheable && planMemoSize.Load() < planMemoLimit {
		if _, loaded := planMemo.LoadOrStore(query, p); !loaded {
			planMemoSize.Add(1)
		}
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
