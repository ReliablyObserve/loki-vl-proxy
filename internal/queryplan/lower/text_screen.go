package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// ScreenText turns away a query whose spelling the string translator reads unlike the plan, which holds no text.
// The string translator finds the operand of a vector aggregation by matching the text of the query with regexps
// that do not skip blanks: a blank, tab or newline between the opening parenthesis of the operand and the range
// function, or between the function and its parenthesis, makes it write the whole query as a phrase search, where
// the plan lowers the aggregation as Loki reads it. The shapes are named VectorAgg blank padding and left to the
// translator (the blanks of the other places a query takes them, around the labels of a clause, inside the
// selector, around the range, are read alike by both). Callers that hold the query text call it before Plan (or
// call PlanQuery); a nil error means Plan may answer. Only the construct at the top of the plan matters: a range
// aggregation alone reads the same text alike.
func ScreenText(query string, p *queryplan.Plan) error {
	if _, ok := p.Root.(*queryplan.VectorAgg); !ok {
		return nil
	}
	if paddedOperand(query) {
		return errBlankPadding
	}
	return nil
}

var errBlankPadding = &NotLoweredError{Construct: "VectorAgg blank padding"}

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
		switch c := q[i]; {
		case c == '"' || c == '`':
			i = skipQuoted(q, i)
		case c == '(':
			j := i + 1
			blankBefore := false
			for j < n && isBlank(q[j]) {
				j++
				blankBefore = true
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
