package proxy

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// numericEqualityResponseVersion fences the answers cached before a numeric, duration or bytes equality in a label
// filter after a json or logfmt parser (`| json | status = 200`) was served from the plan, which compares the value
// as a number as Loki does: the string translator compared the literal's exact text, so `200.0` and `2e2` were
// missed and, under `!=`, the entries without the label were kept (semantics/label-filter-numeric-equality).
const numericEqualityResponseVersion = "@numeric-equality-v1:"

// numericEqualityResponseKeyVersion is the fence of a query whose plan writes such an equality
// (lower.WritesNumericEquality), read from the query's resolved plan after a screen of the text: outside quoted text,
// an equality written before a number (equalityBeforeNumberAt) and the word json or logfmt. Every other query asks
// for no plan here.
func numericEqualityResponseKeyVersion(query string) string {
	return numericEqualityFromScan(query, scanFenceWords(query))
}

// numericEqualityFromScan is numericEqualityResponseKeyVersion with the words of the query read already.
func numericEqualityFromScan(query string, words wordFences) string {
	if !words.equalityNumber || !words.parserWord {
		return ""
	}
	if plan := resolvedPlan(fenceVariablesResolved(query)); plan != nil && lower.WritesNumericEquality(plan) {
		return numericEqualityResponseVersion
	}
	return ""
}

// equalityBeforeNumberAt reports whether the `=` at query[i] is an `=`, `==` or `!=` followed, after any blanks, by a
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
