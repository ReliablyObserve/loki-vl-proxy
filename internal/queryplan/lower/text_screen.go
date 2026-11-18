package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// ScreenRoute turns away a plan whose root the proxy evaluates itself (a ranking, or a chain of levels holding a
// ranking, stddev or stdvar) when the proxy's route does not take the query the way the plan does: the route slices
// the operand out of the query text it holds (screenEvaluated). Every other root is decided by the plan alone: the
// handlers read the request's canonical spelling (proxy requestQueryText), and the lowering writes what Loki reads
// from the parsed tree whatever its spelling. A nil error means Plan may answer.
func ScreenRoute(query string, p *queryplan.Plan) error {
	switch n := p.Root.(type) {
	case *queryplan.Rank:
		return screenEvaluated(query, n)
	case *queryplan.VectorAgg:
		if evaluates(n) {
			return screenEvaluated(query, n)
		}
	}
	return nil
}

// PlanQuery is Plan for a query whose text is at hand: a root the proxy evaluates is screened for its route first
// (ScreenRoute).
func PlanQuery(query string, p *queryplan.Plan, prof Profile) (physical.Op, error) {
	if err := ScreenRoute(query, p); err != nil {
		return nil, err
	}
	return Plan(p, prof)
}

// skipQuoted returns the index of the byte that closes the quoted string opening at q[i] (the end of q when it is
// not closed). A double-quoted string skips what a backslash escapes.
func skipQuoted(q string, i int) int {
	quote := q[i]
	for i++; i < len(q); i++ {
		if q[i] == '\\' && quote == '"' {
			i++
		} else if q[i] == quote {
			return i
		}
	}
	return len(q)
}

func isBlank(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func isNameByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}
