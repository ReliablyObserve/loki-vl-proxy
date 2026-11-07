package translator

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// translateLabelFilterExpr renders the expression as a LogsQL filter,
// translating each leaf like a stand-alone label filter stage. Every binary
// node is parenthesized, so the result does not depend on the backend's
// operator precedence.
func translateLabelFilterExpr(e *logql.LabelFilterExpr, labelFn LabelTranslateFunc, caps logsql.Capabilities) (string, bool) {
	ok := true
	out := e.Render(func(leaf string) string {
		translated, leafOK := translateSingleLabelFilter(leaf, labelFn, caps)
		ok = ok && leafOK
		return parenthesizeCompound(translated)
	})
	return out, ok
}

// parenthesizeCompound wraps a translated filter that holds a top-level space
// (an `a OR b` alternation) so an enclosing and/or cannot split it.
func parenthesizeCompound(s string) string {
	var quote byte
	depth := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ' ' && depth == 0:
			return "(" + s + ")"
		}
	}
	return s
}

// mayBeCompound reports whether a label filter stage can hold more than one
// filter: it has a parenthesis, a comma, an and/or keyword or a second
// operator. It is a cheap scan that spares the common stage, a single filter,
// the parse; a false answer is exact, a true one is not.
func mayBeCompound(stage string) bool {
	const delimiters = " \t\r\n(),\"`=!<>"
	var quote byte
	operators, inOperator, inIP := 0, false, false
	for i := 0; i < len(stage); i++ {
		c := stage[i]
		if quote != 0 {
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '(':
			// The parentheses of ip("...") belong to the filter.
			if inIP || i < 2 || !strings.EqualFold(stage[i-2:i], "ip") || i > 2 && strings.IndexByte(delimiters, stage[i-3]) < 0 {
				return true
			}
			inIP = true
		case ')':
			if !inIP {
				return true
			}
			inIP = false
		case ',':
			return true
		case '"', '`':
			quote, inOperator = c, false
		case '=', '!', '<', '>', '~':
			if !inOperator {
				if operators++; operators > 1 {
					return true
				}
			}
			inOperator = true
		default:
			inOperator = false
			if (c == 'a' || c == 'A' || c == 'o' || c == 'O') && (i == 0 || strings.IndexByte(delimiters, stage[i-1]) >= 0) {
				end := i
				for end < len(stage) && strings.IndexByte(delimiters, stage[end]) < 0 {
					end++
				}
				if word := stage[i:end]; strings.EqualFold(word, "and") || strings.EqualFold(word, "or") {
					return true
				}
			}
		}
	}
	return false
}
