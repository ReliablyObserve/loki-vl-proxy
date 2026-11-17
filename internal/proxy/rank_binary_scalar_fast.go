package proxy

import (
	"net/http"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// rankScalarFastPath turns on rangeRankScalarOperand; tests turn it off to hold the fast path to the binary route.
var rankScalarFastPath = true

// rangeRankScalarOperand reports the binary operand of a ranking, stddev or stdvar on query_range that the scalar
// stats route (proxyScalarStatsBinary) answers as the binary route does, with one VictoriaLogs stats request and no
// sub-request: a range function of lines (count_over_time, rate, bytes_over_time or bytes_rate) with line filters only,
// no grouping, offset or unwrap, and a number, joined by an arithmetic operator with no bool and no vector matching,
// over a range equal to the request's step (on or off the step's grid), where the translator's text for the
// operand is that same function and number (stats and scalarStatsBinaryOf). The tumbling window is what makes the
// stats buckets the trailing windows Loki evaluates; any other operand takes the binary route
// (semantics/rank-binary-operand-label-replace-once, TestRankScalarFastPathMatchesBinaryRoute).
func rangeRankScalarOperand(r *http.Request, operand logqlpkg.Expr, stats physical.VLStats, isStats bool) (scalarStatsBinary, bool) {
	bin, ok := operand.(*logqlpkg.BinOpExpr)
	if !rankScalarFastPath || !ok || bin.ReturnBool || bin.VectorMatching != nil || !rankScalarArithmetic(bin.Op) {
		return scalarStatsBinary{}, false
	}
	lit, literalLeft := bin.Left.(*logqlpkg.LiteralExpr)
	side := bin.Right
	if !literalLeft {
		if lit, ok = bin.Right.(*logqlpkg.LiteralExpr); !ok {
			return scalarStatsBinary{}, false
		}
		side = bin.Left
	}
	ra, ok := side.(*logqlpkg.RangeAggregation)
	if !ok || !rankScalarLineRange(ra) {
		return scalarStatsBinary{}, false
	}
	if step, ok := parsePositiveStepDuration(r.FormValue("step")); !ok || ra.RangeDuration != step {
		return scalarStatsBinary{}, false
	}
	decoded, isBinary := binaryOfStats(stats, isStats)
	if !isBinary {
		return scalarStatsBinary{}, false
	}
	scalar, ok := scalarStatsBinaryOf(decoded)
	if !ok || scalar.op != bin.Op || scalar.scalar != lit.Value || scalar.scalarLeft != literalLeft {
		return scalarStatsBinary{}, false
	}
	return scalar, true
}

// rankScalarArithmetic reports the arithmetic operators: a comparison filters the series, which the fast path does
// not cover.
func rankScalarArithmetic(op string) bool {
	switch op {
	case "+", "-", "*", "/", "%", "^":
		return true
	}
	return false
}

// rankScalarLineRange reports a range function of lines with line filters only, no grouping, offset or parameter.
func rankScalarLineRange(ra *logqlpkg.RangeAggregation) bool {
	switch ra.Op {
	case logqlpkg.RangeCountOverTime, logqlpkg.RangeRate, logqlpkg.RangeBytesOverTime, logqlpkg.RangeBytesRate:
	default:
		return false
	}
	if ra.Grouping != nil || ra.Offset != "" || ra.HasParam || ra.RangeDuration <= 0 {
		return false
	}
	q, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok {
		return false
	}
	for _, st := range q.Pipeline {
		if _, line := st.(*logqlpkg.LineFilterStage); !line {
			return false
		}
	}
	return true
}
