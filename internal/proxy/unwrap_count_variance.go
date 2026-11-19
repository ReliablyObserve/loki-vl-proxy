package proxy

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// countVarianceResponseVersion fences the answers cached before a count by labels over stdvar_over_time answered the
// number of series of each group (semantics/count-over-stdvar-answers-variance): the variance of the group's pooled
// samples, then.
const countVarianceResponseVersion = "@count-stdvar-series-v1:"

// countVarianceResponseKeyVersion is the fence of a query whose plan holds such a count at any depth
// (lower.WritesCountVarianceSeries). It is read from the query's shared plan after a text screen of the words every
// such plan holds (count, stdvar_over_time and unwrap, read as dynamicQuery lower cases the keywords; a word with
// U+0130, which lower cases to i, is compared case-folded), so no other query's key changes. It does not read the
// -query-plan-lowering flag: with the flag off the fence costs one more cache miss.
func countVarianceResponseKeyVersion(query string) string {
	query = dynamicQuery(query)
	other := strings.Contains(query, "İ")
	if !containsWord(query, "stdvar_over_time", other) || !containsWord(query, "count", other) || !containsWord(query, "unwrap", other) {
		return ""
	}
	if plan := resolvedPlan(fenceVariablesResolved(query)); plan != nil && lower.WritesCountVarianceSeries(plan) {
		return countVarianceResponseVersion
	}
	return ""
}
