package proxy

import (
	"net/http"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// holdsBinaryOperand reports an operand of a ranking, stddev or stdvar whose answer is a binary operation's under
// aggregations and label_replace calls (`sum by (x) (a / b)`, `label_replace(a / b, ...)`): the translation of such an
// operand is the marker of the binary operation's queries, never a query VictoriaLogs reads, so the post-aggregation
// handlers evaluate it as a query of its own (serveOperandAsQuery). A binary operation itself is evaluated by the binary
// route (proxyBinaryLogQL).
func holdsBinaryOperand(e logqlpkg.Expr) bool {
	for {
		switch x := e.(type) {
		case *logqlpkg.BinOpExpr:
			return true
		case *logqlpkg.VectorAggregation:
			e = x.Inner
		case *logqlpkg.OpaqueMetricExpr:
			if x.Func != "label_replace" || x.Inner == nil {
				return false
			}
			e = x.Inner
		default:
			return false
		}
	}
}

// serveRangeBinaryOperand answers the operand of a ranking, stddev or stdvar on query_range when its answer is a binary
// operation's and reports whether it did: a binary operation by the binary route (proxyBinaryLogQL), operand by operand,
// as the instant handler and a binary at the root are (the translation drops a bool modifier and the right side of and,
// or and unless); one under aggregations or label_replace calls as a query of its own (serveOperandAsQuery).
func (p *Proxy) serveRangeBinaryOperand(w http.ResponseWriter, r *http.Request, operand logqlpkg.Expr) bool {
	if binary, ok := operand.(*logqlpkg.BinOpExpr); ok {
		p.proxyBinaryLogQL(w, r, binary, "matrix")
		return true
	}
	return p.serveWrappedBinaryOperand(w, r, operand, "matrix")
}

// serveWrappedBinaryOperand answers an operand that is a binary operation under aggregations or label_replace calls as
// a query of its own (serveOperandAsQuery) and reports whether it did; a binary operation itself, or any other operand,
// is left to the caller.
func (p *Proxy) serveWrappedBinaryOperand(w http.ResponseWriter, r *http.Request, operand logqlpkg.Expr, resultType string) bool {
	if _, binary := operand.(*logqlpkg.BinOpExpr); binary || !holdsBinaryOperand(operand) {
		return false
	}
	p.serveOperandAsQuery(w, r, operand, resultType)
	return true
}

// serveOperandAsQuery answers the operand of a ranking, stddev or stdvar as a query of its own through the query
// handlers (evaluateBinaryLogQLOperand: the request's bounds and tenant, the shared evaluation budget, the operand's
// own response cache) and writes its answer, or its error, to w.
func (p *Proxy) serveOperandAsQuery(w http.ResponseWriter, r *http.Request, operand logqlpkg.Expr, resultType string) {
	response := p.evaluateBinaryLogQLOperand(r, operand, resultType)
	if response.status >= http.StatusBadRequest {
		setReplayContentType(w, response.status)
		w.WriteHeader(response.status)
		_, _ = w.Write(response.body.Bytes())
		if rec, ok := w.(failureRecorder); ok {
			rec.recordFailure(response.failure)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(response.body.Bytes()) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the operand handler's own JSON
}
