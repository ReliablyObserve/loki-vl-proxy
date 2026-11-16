package proxy

import (
	"strings"
	"unicode"
	"unicode/utf8"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// emptyWithoutOperand is a binary operand as the binary route sends it to the query handlers: its outer sum, avg,
// min and max levels with a `without ()` clause are taken off (semantics/binary-operand-without-empty-collapses).
// Such a clause drops no label, so each series of the level's operand is a group of its own and the level answers
// that series unchanged (its sum, mean, minimum and maximum are its value), with every label it has: Loki's answer
// for the level is its operand's answer. The handlers read the clause as no clause and answered the level as one
// ungrouped series on a query_range whose step is the range (and a per-stream fold with no level elsewhere), so the
// operand is sent without the level: one query fewer for the handlers to misread, and the VictoriaLogs requests of
// the operand alone. A count, stddev, stdvar or ranking under the clause is no such level (each series answers 1 or
// 0, or is ranked), and is sent as it is written.
func emptyWithoutOperand(expr logqlpkg.Expr) logqlpkg.Expr {
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok || !keepsEverySeries(agg) {
			return expr
		}
		expr = agg.Inner
	}
}

// keepsEverySeries reports a vector aggregation whose answer is its operand's: sum, avg, min or max with a `without`
// clause of no label.
func keepsEverySeries(agg *logqlpkg.VectorAggregation) bool {
	switch agg.Op {
	case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax:
	default:
		return false
	}
	return agg.Grouping != nil && agg.Grouping.Without && len(agg.Grouping.Labels) == 0 && agg.Inner != nil
}

// emptyWithoutOperandResponseVersion fences the answers cached before a sum, avg, min or max `without ()` operand of a
// binary operation was sent as its operand alone (emptyWithoutOperand): one ungrouped series on a query_range whose
// step is the range, and series with an unknown detected_level on the other steps, then
// (semantics/binary-operand-without-empty-collapses).
const emptyWithoutOperandResponseVersion = "@binary-empty-without-v1:"

// emptyWithoutOperandResponseKeyVersion is the fence of a query whose plan holds a binary operation, at any depth,
// with such an operand. It is read from the query's shared plan after a text screen (emptyWithoutClauseIn: a
// `without` clause with nothing between its parentheses), so no other query's key changes and a query with no empty
// clause pays one scan of its text. It does not read the -query-plan-lowering flag: with the flag off the fence costs
// one more cache miss.
func emptyWithoutOperandResponseKeyVersion(query string) string {
	if !emptyWithoutClauseIn(query) {
		return ""
	}
	if plan := resolvedPlan(dynamicQuery(query)); plan != nil && holdsEmptyWithoutOperand(plan.Root) {
		return emptyWithoutOperandResponseVersion
	}
	return ""
}

// holdsEmptyWithoutOperand reports a plan with a binary operation whose operand is a sum, avg, min or max `without ()`
// (lower.EmptyWithoutLevelsOff takes it off).
func holdsEmptyWithoutOperand(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.BinOp:
		return lower.EmptyWithoutLevelsOff(v.L) != v.L || lower.EmptyWithoutLevelsOff(v.R) != v.R ||
			holdsEmptyWithoutOperand(v.L) || holdsEmptyWithoutOperand(v.R)
	case *queryplan.VectorAgg:
		return holdsEmptyWithoutOperand(v.In)
	case *queryplan.Rank:
		return holdsEmptyWithoutOperand(v.In)
	case *queryplan.LabelReplace:
		return holdsEmptyWithoutOperand(v.In)
	}
	return false
}

// emptyWithoutClauseIn reports the word without, in any spelling the parser reads (any case, U+0130 for its i),
// followed by an opening and a closing parenthesis with only blanks or comments before and between them. It scans
// quoted text too: a quoted `without ()` costs a plan read and fires nothing.
func emptyWithoutClauseIn(query string) bool {
	for from := 0; ; {
		i := indexFoldFrom(query, from, "without")
		if i < 0 {
			return false
		}
		end, _ := matchFoldAt(query[i:], "without")
		j := skipClauseBlanks(query, i+end)
		if j < len(query) && query[j] == '(' {
			if k := skipClauseBlanks(query, j+1); k < len(query) && query[k] == ')' {
				return true
			}
		}
		from = i + 1
	}
}

// skipClauseBlanks returns the index of the first byte at or after i that is neither a character Go calls white
// space nor part of a `#` comment (to the end of its line).
func skipClauseBlanks(s string, i int) int {
	for i < len(s) {
		if s[i] == '#' {
			end := strings.IndexByte(s[i:], '\n')
			if end < 0 {
				return len(s)
			}
			i += end + 1
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if !unicode.IsSpace(r) {
			return i
		}
		i += size
	}
	return i
}
