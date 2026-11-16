package proxy

import (
	"net/http"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// A Loki that serves data answers /patterns given a valid query that holds no stream selector (`1 + 1`,
// `vector(1)`) with its empty success, `{"status":"success","data":[]}`, where the other selector routes answer
// `400 only label matchers are supported` (recorded rows r0291 and r1600 of
// internal/lokierr/testdata/loki_error_answers.jsonl, state "serving"). Such a query names no stream, so the
// answer reads nothing: the proxy writes it before the response cache and any VictoriaLogs call.

// patternsEmptyBody is Loki's empty /patterns success.
const patternsEmptyBody = `{"status":"success","data":[]}` + "\n"

// hasStreamSelector reports whether expr reads a stream selector anywhere; an expression the proxy does not type
// (an untyped opaque call) is taken to read one.
func hasStreamSelector(expr logqlpkg.Expr) bool {
	switch e := expr.(type) {
	case *logqlpkg.LiteralExpr:
		return false
	case *logqlpkg.OpaqueMetricExpr:
		if e.Typed && e.Func == "vector" {
			return false
		}
		return !e.Typed || e.Inner == nil || hasStreamSelector(e.Inner)
	case *logqlpkg.VectorAggregation:
		return e.Inner == nil || hasStreamSelector(e.Inner)
	case *logqlpkg.BinOpExpr:
		return hasStreamSelector(e.Left) || hasStreamSelector(e.Right)
	}
	return true
}

// answerPatternsWithoutSelector writes Loki's empty /patterns success for a valid query that holds no stream
// selector; it reports whether it answered.
func (p *Proxy) answerPatternsWithoutSelector(w http.ResponseWriter, query string) bool {
	if query == "" || cachedQueryValidation(query).msg != "" {
		return false
	}
	expr, ok := sharedLogQLExpr(query)
	if !ok || expr == nil || hasStreamSelector(expr) {
		return false
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_, _ = w.Write([]byte(patternsEmptyBody))
	p.metrics.RecordRequest("patterns", http.StatusOK, 0)
	return true
}
