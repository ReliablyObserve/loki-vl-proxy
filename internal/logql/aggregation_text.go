package logql

import (
	"regexp"
	"strings"
	"unicode"
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

// UnwrapParens strips the parentheses that wrap the whole of s. It reads s once: the parentheses of the leading run
// (each layer opens there, after the ones before it and white space) are matched in one pass, the way ClosingParen
// matches each, so a query nested n levels costs linear time, not n passes over it.
func UnwrapParens(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "(") {
		return s
	}
	// A query in a few parentheses keeps its bookkeeping on the stack.
	var closesBuf, openBuf [16]int
	closes := leadingParenCloses(s, closesBuf[:0], openBuf[:0])
	start, end := 0, len(s) // the text left is s[start:end]
	for start < end && s[start] == '(' && closes[start] == end-1 {
		inner := s[start+1 : end-1]
		lead := len(inner) - len(strings.TrimLeftFunc(inner, unicode.IsSpace))
		start, end = start+1+lead, start+1+lead+len(strings.TrimSpace(inner))
	}
	return s[start:end]
}

// leadingParenCloses is, for each "(" of the run of parentheses and white space s starts with, the index of the ")"
// that closes it (ClosingParen), -1 for one never closed and for the other positions of the run. Quoted and raw
// strings are skipped as ClosingParen skips them. closes and open are buffers to fill from (their length is 0).
func leadingParenCloses(s string, closes, open []int) []int {
	run := len(s) - len(strings.TrimLeftFunc(s, func(r rune) bool { return r == '(' || unicode.IsSpace(r) }))
	for i := 0; i < run; i++ {
		closes = append(closes, -1)
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"', '`':
			quote := s[i]
			for i++; i < len(s) && s[i] != quote; i++ {
				if quote == '"' && s[i] == '\\' {
					i++
				}
			}
		case '(':
			open = append(open, i)
		case ')':
			if len(open) == 0 {
				continue
			}
			o := open[len(open)-1]
			open = open[:len(open)-1]
			if o < run {
				closes[o] = i
			}
			if len(open) == 0 {
				return closes // every parenthesis of the run is closed or never will be
			}
		}
	}
	return closes
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
