package proxy

import (
	"net/http"
	"strings"
)

// responseKeySite names the response key a fence chain is appended to. The
// sites share the fences but not their order or their set, and the order is
// part of every cached key.
type responseKeySite uint8

const (
	// responseKeyCompatQuery is the compat edge key of an instant query.
	responseKeyCompatQuery responseKeySite = iota
	// responseKeyCompatQueryRange is the compat edge key of query_range, the
	// one compat key that carries the logfmt presence fence (last).
	responseKeyCompatQueryRange
	// responseKeyInnerQueryRange is queryRangeCacheKey, whose chain always
	// carries the logfmt presence fence, right after the JSON ones.
	responseKeyInnerQueryRange
	// responseKeyLogWindowShape feeds the log window fragment fingerprint,
	// which only the keep projection, keyword label, reserved field case and reserved
	// field label fences reach. It reads no request.
	responseKeyLogWindowShape
)

// responseKeyFences returns the version fences appended to the response cache
// key of query at site: one constant per predicate that fires on the query,
// concatenated in the order the site has always used, so an answer cached
// before a semantics fix is never served. r supplies the start, end and step
// that resolve Grafana range tokens; it may be nil at responseKeyLogWindowShape.
// The line version and the endpoint forms stay with the key builders.
func responseKeyFences(r *http.Request, query string, site responseKeySite) string {
	// The screens share three reads of the query: whether any word can be spelled other than in lower case ASCII,
	// whether it holds a keep stage, and its words.
	words := scanFenceWords(query)
	keep := keepStageIn(query)
	if site == responseKeyLogWindowShape {
		var b strings.Builder
		if keep {
			b.WriteString(keepMetadataProjectionResponseVersion)
		}
		b.WriteString(keywordLabelFromScan(query, words))
		if words.reservedCase {
			b.WriteString(reservedFieldCaseResponseVersion)
		}
		if words.reservedLabel {
			b.WriteString(reservedFieldLabelResponseVersion)
		}
		return b.String()
	}
	other := spelledOtherwise(query)
	var b strings.Builder
	b.WriteString(orderedJSONVectorFence(r, query, other))
	b.WriteString(bareJSONMetricFence(r, query, other))
	if site == responseKeyInnerQueryRange {
		b.WriteString(logfmtPresenceFence(r, query, other))
	}
	b.WriteString(detectedLevelMutationResponseKeyVersion(query))
	if keep {
		b.WriteString(keepMetadataProjectionResponseVersion)
	}
	if ipFilterIn(query, other) {
		b.WriteString(ipLabelFilterResponseVersion)
	}
	if repeatedLabelFilterStages(query) {
		b.WriteString(repeatedLabelFilterResponseVersion)
	}
	b.WriteString(keywordLabelFromScan(query, words))
	if words.reservedCase {
		b.WriteString(reservedFieldCaseResponseVersion)
	}
	if words.reservedLabel {
		b.WriteString(reservedFieldLabelResponseVersion)
	}
	if keep && containsWord(query, "unwrap", other) {
		b.WriteString(keepUnwrapIdentityResponseVersion)
	}
	if site == responseKeyCompatQueryRange {
		b.WriteString(logfmtPresenceFence(r, query, other))
	}
	if site == responseKeyCompatQuery || site == responseKeyCompatQueryRange {
		// The compat edge looks its cache up before the handler validates the query.
		b.WriteString(runeResponseKeyVersion(query))
	}
	return b.String()
}

// keepUnwrapIdentityResponseVersion fences the answers cached before a keep before an unwrap decided the series of
// the range function (semantics/keep-before-unwrap-series-identity): a series per stream, or per line, then.
const keepUnwrapIdentityResponseVersion = "@keep-unwrap-identity-v1:"

// keepUnwrapIdentityResponseKeyVersion is the fence of a query that holds a keep stage and an unwrap: a text screen,
// so a query whose keep decides nothing is fenced too (one more cache miss), and no other query's key changes.
func keepUnwrapIdentityResponseKeyVersion(query string) string {
	if keepStageIn(query) && containsWord(query, "unwrap", spelledOtherwise(query)) {
		return keepUnwrapIdentityResponseVersion
	}
	return ""
}
