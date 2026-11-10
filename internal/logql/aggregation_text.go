package logql

import "strings"

// The text scanners the proxy's post-aggregation route (a ranking, a stddev or stdvar, evaluated by the proxy over
// the answer of its operand) and the lowering's operand screen read a query with: they slice the operand out of the
// query as written, the way the string translator reads it, so both slice the same text.

// AggregationArgs returns the text between the parentheses that hold the operand (and, for topk and bottomk, the
// parameter) of the aggregation name written at the start of the query, in any of its spellings: `name(args)`,
// `name by (l) (args)` and `name(args) by (l)`.
func AggregationArgs(query, name string) (string, bool) {
	rest := strings.TrimSpace(query)
	if len(rest) < len(name) || !strings.EqualFold(rest[:len(name)], name) {
		return "", false
	}
	rest = rest[len(name):]
	if rest != "" && (rest[0] == '_' || rest[0] >= 'a' && rest[0] <= 'z' || rest[0] >= '0' && rest[0] <= '9') {
		return "", false // a longer name: count_over_time is not count
	}
	rest = strings.TrimSpace(rest)
	leading := false
	if i := strings.IndexByte(rest, '('); i > 0 {
		// `by (l) (k, expr)`: the label list comes before the arguments.
		close := ClosingParen(rest, i)
		if close < 0 {
			return "", false
		}
		rest, leading = strings.TrimSpace(rest[close+1:]), true
	}
	if !strings.HasPrefix(rest, "(") {
		return "", false
	}
	close := ClosingParen(rest, 0)
	if close < 0 || (leading && strings.TrimSpace(rest[close+1:]) != "") {
		return "", false
	}
	return rest[1:close], true
}

// UnwrapParens strips the parentheses that wrap the whole of s.
func UnwrapParens(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "(") && ClosingParen(s, 0) == len(s)-1 {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

// ClosingParen returns the index of the ")" that closes the "(" at s[open], skipping quoted strings and raw
// strings, or -1.
func ClosingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '"', '`':
			quote := s[i]
			for i++; i < len(s) && s[i] != quote; i++ {
				if quote == '"' && s[i] == '\\' {
					i++
				}
			}
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

// TopLevelCommaIndex returns the index of the first comma of s outside parentheses and double-quoted strings, or -1.
func TopLevelCommaIndex(s string) int {
	depth := 0
	inQuote := false
	for i, r := range s {
		switch r {
		case '"':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
			}
		case ',':
			if !inQuote && depth == 0 {
				return i
			}
		}
	}
	return -1
}
