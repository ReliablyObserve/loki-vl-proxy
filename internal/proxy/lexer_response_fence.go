package proxy

import "strings"

const lexerResponseVersion = "@lexer-v1:"

// lexerResponseKeyVersion fences the replies cached before the LogQL lexer read a query as Loki's does
// (internal/logql, semantics/logql-loki-whitespace) at the compat edge of query and query_range, which looks its
// cache up before the handler validates the query. A query that was answered and is Loki's 400 now would be served
// its old 200 otherwise:
//
//   - white space Loki's lexer does not skip (a vertical tab, a form feed, U+0085, U+00A0, U+2003, ...) and any other
//     control character, outside quoted text;
//   - a blank or line break inside a [range], a line break inside a double-quoted string;
//   - a by, without, on, ignoring or group list that is not names separated by commas, and on or ignoring without
//     its list;
//   - an unresolved Grafana variable ($__auto) in a range inside label_replace.
//
// A query is scanned once, without allocating, for a candidate of those; the validation, memoized by the query text,
// decides, so the fence applies to the queries now rejected only.
func lexerResponseKeyVersion(query string) string {
	if !lexerFenceCandidate(query) || cachedQueryValidation(query).msg == "" {
		return ""
	}
	return lexerResponseVersion
}

// lexerFenceCandidate reports a character or a word outside quoted text that one of the readings above could apply
// to.
func lexerFenceCandidate(query string) bool {
	inRange := false
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			end, lineBreak := skipQuotedText(query, i)
			if lineBreak {
				return true
			}
			i = end
			continue
		case c == '#':
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		case isFenceByte(c):
			return true
		case isWordByte(c):
			j := i + 1
			for j < len(query) && (isWordByte(query[j]) || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			if isListWord(query[i:j]) {
				return true
			}
			i = j
			continue
		case c == '[':
			inRange = true
		case c == ']':
			inRange = false
		case inRange && (c == ' ' || c == '\t' || c == '\n' || c == '\r'):
			return true
		}
		i++
	}
	return false
}

// isFenceByte reports a byte that is not ASCII, a control character that is not white space to Loki's lexer, or a
// dollar sign (a Grafana variable).
func isFenceByte(c byte) bool {
	return c >= 0x80 || c < 0x20 && c != '\t' && c != '\n' && c != '\r' || c == 0x7f || c == '$'
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// skipQuotedText returns the index after the quoted text that starts at query[i], and whether a double-quoted text
// holds a line break.
func skipQuotedText(query string, i int) (int, bool) {
	quote := query[i]
	for j := i + 1; j < len(query); j++ {
		switch {
		case query[j] == quote:
			return j + 1, false
		case quote == '"' && query[j] == '\\':
			j++
		case quote == '"' && query[j] == '\n':
			return j, true
		}
	}
	return len(query), false
}

// isListWord reports one of the words of Loki's lexer that open a list of names, in any case.
func isListWord(w string) bool {
	switch len(w) {
	case 2:
		return strings.EqualFold(w, "by") || strings.EqualFold(w, "on")
	case 7:
		return strings.EqualFold(w, "without")
	case 8:
		return strings.EqualFold(w, "ignoring")
	case 10, 11:
		return strings.EqualFold(w, "group_left") || strings.EqualFold(w, "group_right")
	}
	return false
}
