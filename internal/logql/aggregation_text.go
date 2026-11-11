package logql

import (
	"regexp"
	"strings"
)

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

// SeriesFoldOperand is the operand of the stddev or stdvar (op) a query is, sliced from the text the way the proxy's
// route reads it: the parentheses that wrap the whole query are stripped, the text must start with op, a leading
// by or without clause is skipped to its first closing parenthesis, and the operand is what the next parentheses
// hold. ok is false when the text does not read so. The proxy translates this text alone and folds the series of its
// answer itself.
func SeriesFoldOperand(query, op string) (operand string, ok bool) {
	source := strings.TrimSpace(query)
	for strings.HasPrefix(source, "(") && ClosingParen(source, 0) == len(source)-1 {
		source = strings.TrimSpace(source[1 : len(source)-1])
	}
	if !strings.HasPrefix(source, op) {
		return "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(source, op))
	if !strings.HasPrefix(rest, "(") {
		// A leading grouping clause: skip it to the operand.
		open := strings.IndexByte(rest, '(')
		if open < 0 {
			return "", false
		}
		rest = strings.TrimSpace(rest[open+1+strings.IndexByte(rest[open+1:], ')')+1:])
	}
	end := ClosingParen(rest, 0)
	if end < 0 {
		return "", false
	}
	return strings.TrimSpace(rest[1:end]), true
}

// ParserStageTextRE finds a parser stage (json, logfmt, pattern, regexp or unpack) written in the text of a query,
// quoted text included. The proxy reads the operand of a stddev or stdvar by a label this way to tell a parsed label
// from a stream one.
var ParserStageTextRE = regexp.MustCompile(`\|\s*(?:json|logfmt|pattern|regexp|unpack)\b`)
