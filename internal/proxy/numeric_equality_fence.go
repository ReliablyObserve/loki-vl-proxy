package proxy

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// numericEqualityResponseVersion fences the answers cached before a numeric, duration or bytes equality in a label
// filter after a json or logfmt parser (`| json | status = 200`) was served from the plan, which compares the value
// as a number as Loki does: the string translator compared the literal's exact text, so `200.0` and `2e2` were
// missed and, under `!=`, the entries without the label were kept (semantics/label-filter-numeric-equality).
const numericEqualityResponseVersion = "@numeric-equality-v2:"

// numericEqualityResponseKeyVersion is the fence of a query whose plan writes such an equality
// (lower.WritesNumericEquality), read from the query's resolved plan after a screen of the text: the word json or
// logfmt, then an `=` followed by a number (equalityBeforeNumber). The response key chain reads the two words once
// for every fence that reads them (numericEqualityAfterParser), so a query without either costs nothing here.
func numericEqualityResponseKeyVersion(query string) string {
	other := spelledOtherwise(query)
	if !containsWord(query, "json", other) && !containsWord(query, "logfmt", other) {
		return ""
	}
	return numericEqualityAfterParser(query)
}

// numericEqualityAfterParser is numericEqualityResponseKeyVersion of a query that holds the word json or logfmt.
func numericEqualityAfterParser(query string) string {
	if !equalityBeforeNumber(query) {
		return ""
	}
	if plan := resolvedPlan(fenceVariablesResolved(query)); plan != nil && lower.WritesNumericEquality(plan) {
		return numericEqualityResponseVersion
	}
	return ""
}

// equalityBeforeNumber reports an equalityBeforeNumberAt anywhere in query, quoted text and comments included, or an
// equality on a literal that only follows a comment (the comment pass runs only for a query holding a `#`).
func equalityBeforeNumber(query string) bool {
	if equalityBeforeNumberPlain(query) {
		return true
	}
	return strings.IndexByte(query, '#') >= 0 && equalityBeforeNumberWithComments(query)
}

// equalityBeforeNumberPlain is the older screen, unchanged: every `=` followed, after blanks, by a literal.
func equalityBeforeNumberPlain(query string) bool {
	for i := 0; i < len(query); i++ {
		k := strings.IndexByte(query[i:], '=')
		if k < 0 {
			return false
		}
		if i += k; equalityBeforeNumberAt(query, i) {
			return true
		}
	}
	return false
}

// equalityBeforeNumberWithComments reads comments only outside quoted text. The earlier pass retains every
// older match, including numeric-looking text in strings and comments. After looking past blanks/comments for
// a literal, this pass resumes at that literal, so no comment is scanned repeatedly.
func equalityBeforeNumberWithComments(query string) bool {
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '"', '`':
			i = skipQuoted(query, i)
		case '#':
			i = skipComment(query, i) - 1
		case '=':
			if i > 0 && (query[i-1] == '<' || query[i-1] == '>' || query[i-1] == '=') {
				continue
			}
			j := i + 1
			if j < len(query) && query[j] == '=' {
				j++
			}
			j = skipLexerBlanks(query, j)
			if j < len(query) {
				c := query[j]
				if c >= '0' && c <= '9' || c == '.' || c == '-' || c == '+' {
					return true
				}
			}
			i = j - 1
		}
	}
	return false
}

// equalityBeforeNumberAt reports whether the `=` at query[i] is an `=`, `==` or `!=` followed, after blanks, by a
// digit, a dot or a sign: the spelling of every equality on a number, a duration or a byte size (and of some other
// text, which the plan then reads). The second `=` of `==`, an ordering comparison (`>=`, `<=`) and a regexp matcher
// (`=~`) are not one.
func equalityBeforeNumberAt(query string, i int) bool {
	if i > 0 && (query[i-1] == '<' || query[i-1] == '>' || query[i-1] == '=') {
		return false
	}
	j := i + 1
	if j < len(query) && query[j] == '=' {
		j++
	}
	for j < len(query) && (query[j] == ' ' || query[j] == '\t' || query[j] == '\n' || query[j] == '\r' || query[j] == '\v' || query[j] == '\f') {
		j++
	}
	if j >= len(query) {
		return false
	}
	c := query[j]
	return c >= '0' && c <= '9' || c == '.' || c == '-' || c == '+'
}
