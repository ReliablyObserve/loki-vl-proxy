package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// deletePipesConstruct names a log query whose lowering needs a pipe: VictoriaLogs deletes the rows of a filter
// (its delete task reads a filter, no pipe), so such a query cannot be deleted exactly.
const deletePipesConstruct = "LogSelect pipes in a delete"

// PlanLogFilter is the filter of a plan of a log query whose lowering writes no pipe: the filter of the query
// PlanLogText writes, typed. It is what a delete removes, the rows the same query reads. A query that is not a log
// query, or whose lowering needs a pipe (a parser, a formatting stage, a filter on a label a parser or the
// detected level derives), answers NotLoweredError. The filter is nil when the query reads every row (`*`).
func PlanLogFilter(p *queryplan.Plan, prof Profile) (logsql.FilterExpr, error) {
	sel, ok := p.Root.(*queryplan.LogSelect)
	if !ok {
		return nil, &NotLoweredError{Construct: Construct(p.Root)}
	}
	if keepsSpelling(sel, prof) {
		return nil, &NotLoweredError{Construct: escapedSpellingConstruct}
	}
	q, _, err := logSelectQuery(sel, prof)
	if err != nil {
		return nil, err
	}
	if len(q.Pipes) > 0 {
		return nil, &NotLoweredError{Construct: deletePipesConstruct}
	}
	return q.Filter, nil
}
