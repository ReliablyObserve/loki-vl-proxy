package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// ScreenText turns away a query whose spelling the string translator reads unlike the plan, which holds no text.
// The string translator finds the operand of a vector aggregation by matching the text of the query with regexps
// that do not skip blanks: a blank, tab or newline between the opening parenthesis of the operand and the range
// function, or between the function and its parenthesis, makes it write the whole query as a phrase search, where
// the plan lowers the aggregation as Loki reads it. The shapes are named VectorAgg operand spelling and left to the
// translator (the blanks of the other places a query takes them, around the labels of a clause, inside the
// selector, around the range, are read alike by both). A metric query wrapped whole in parentheses is turned away
// too (RangeAgg or VectorAgg parenthesized). Callers that hold the query text call it before Plan (or call
// PlanQuery); a nil error means Plan may answer. Only the construct at the top of the plan matters: a range
// aggregation alone reads blanks alike. A binary operation and a label_replace call have screens of their own
// (screenBinOp, screenLabelReplace), which screen their operands with this one.
func ScreenText(query string, p *queryplan.Plan) error {
	var parenthesized, padded *NotLoweredError
	switch n := p.Root.(type) {
	case *queryplan.BinOp:
		return screenBinOp(query, n)
	case *queryplan.LabelReplace:
		return screenLabelReplace(query, n)
	case *queryplan.Rank:
		return screenRank(query, n)
	case *queryplan.VectorAgg:
		parenthesized, padded = errVectorParenthesized, errBlankPadding
	case *queryplan.RangeAgg:
		parenthesized = errRangeParenthesized
	default:
		return nil
	}
	// A metric query wrapped whole in parentheses, `(rate({a="x"}[5m]))`, is read by the translator as a line
	// filter and written as a phrase (semantics/translator-reads-parenthesized-metric-as-text).
	if wrappedWhole(query) {
		return parenthesized
	}
	if padded != nil && paddedOperand(query) {
		return padded
	}
	if err := screenUnwrap(query, p); err != nil {
		return err
	}
	return nil
}

var (
	errBlankPadding        = &NotLoweredError{Construct: "VectorAgg operand spelling"}
	errVectorParenthesized = &NotLoweredError{Construct: "VectorAgg parenthesized"}
	errRangeParenthesized  = &NotLoweredError{Construct: "RangeAgg parenthesized"}
)

// wrappedWhole reports whether the query is one parenthesized expression whose content is no stream selector:
// `(rate(...))`, `( sum(...) )`, `((rate(...)))`. A binary of two parenthesized operands, `(a) / (b)`, is not.
func wrappedWhole(q string) bool {
	q = strings.TrimSpace(q)
	if len(q) < 2 || q[0] != '(' {
		return false
	}
	depth := 0
	for i := 0; i < len(q); i++ {
		switch q[i] {
		case '"', '`':
			i = skipQuoted(q, i)
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 && i != len(q)-1 {
				return false
			}
		}
	}
	inner := strings.TrimLeft(strings.TrimSpace(q[1:len(q)-1]), "(")
	return depth == 0 && inner != "" && inner[0] != '{'
}

// PlanQuery is Plan for a query whose text is at hand: it leaves the query to the string translator when ScreenText
// says the translator reads its spelling unlike the plan.
func PlanQuery(query string, p *queryplan.Plan, prof Profile) (physical.Op, error) {
	if err := ScreenText(query, p); err != nil {
		return nil, err
	}
	return Plan(p, prof)
}

// paddedOperand reports whether the text, outside its quoted strings, has a blank (space, tab or newline) right
// after a "(" that is followed by a name and the "(" of a call (`sum( rate(`), or between a name and the "(" of a
// call that follows an opening parenthesis (`(rate (`).
func paddedOperand(q string) bool {
	n := len(q)
	for i := 0; i < n; i++ {
		switch q[i] {
		case '"', '`':
			i = skipQuoted(q, i)
		case '(':
			j := i + 1
			blankBefore := false
			for j < n && isBlank(q[j]) {
				j++
				blankBefore = true
			}
			// An operand that is itself parenthesized, `sum((rate(...)))`, is read as text as well.
			if j < n && q[j] == '(' {
				return true
			}
			k := j
			for k < n && isNameByte(q[k]) {
				k++
			}
			if k == j {
				continue
			}
			l := k
			for l < n && isBlank(q[l]) {
				l++
			}
			if l < n && q[l] == '(' && (blankBefore || l > k) {
				return true
			}
		}
	}
	return false
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
