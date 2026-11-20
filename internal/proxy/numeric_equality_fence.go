package proxy

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// numericEqualityResponseVersion fences the answers cached before a numeric, duration or bytes equality in a label
// filter after a json or logfmt parser (`| json | status = 200`) was served from the plan, which compares the value
// as a number as Loki does: the string translator compared the literal's exact text, so `200.0` and `2e2` were
// missed and, under `!=`, the entries without the label were kept (semantics/label-filter-numeric-equality).
const numericEqualityResponseVersion = "@numeric-equality-v1:"

// numericEqualityResponseKeyVersion is the fence of a query whose plan writes such an equality
// (lower.WritesNumericEquality), read from the query's resolved plan after a screen of the text: a json or logfmt
// word and an equals sign. Every other query asks for no plan here.
func numericEqualityResponseKeyVersion(query string) string {
	if strings.IndexByte(query, '=') < 0 {
		return ""
	}
	other := spelledOtherwise(query)
	if !containsWord(query, "json", other) && !containsWord(query, "logfmt", other) {
		return ""
	}
	if plan := resolvedPlan(fenceVariablesResolved(dynamicQuery(query))); plan != nil && lower.WritesNumericEquality(plan) {
		return numericEqualityResponseVersion
	}
	return ""
}
