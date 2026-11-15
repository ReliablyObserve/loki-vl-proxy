package proxy

import (
	"net/http"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
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
	if site == responseKeyLogWindowShape {
		return keepMetadataProjectionResponseKeyVersion(query) + keywordLabelResponseKeyVersion(query) + reservedFieldCaseResponseKeyVersion(query) + reservedFieldLabelResponseKeyVersion(query)
	}
	var b strings.Builder
	b.WriteString(orderedJSONVectorResponseKeyVersion(r, query))
	b.WriteString(bareJSONMetricResponseKeyVersion(r, query))
	if site == responseKeyInnerQueryRange {
		b.WriteString(logfmtPresenceResponseKeyVersion(r, query))
	}
	b.WriteString(detectedLevelMutationResponseKeyVersion(query))
	b.WriteString(keepMetadataProjectionResponseKeyVersion(query))
	b.WriteString(ipLabelFilterResponseKeyVersion(query))
	b.WriteString(repeatedLabelFilterResponseKeyVersion(query))
	b.WriteString(keywordLabelResponseKeyVersion(query))
	b.WriteString(reservedFieldCaseResponseKeyVersion(query))
	b.WriteString(reservedFieldLabelResponseKeyVersion(query))
	b.WriteString(keepUnwrapIdentityResponseKeyVersion(query))
	b.WriteString(dynamicUnwrapIdentityResponseKeyVersion(query))
	if site == responseKeyCompatQueryRange {
		b.WriteString(logfmtPresenceResponseKeyVersion(r, query))
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
	if strings.Contains(query, "unwrap") && keepMetadataProjectionStageRE.MatchString(query) {
		return keepUnwrapIdentityResponseVersion
	}
	return ""
}

// dynamicUnwrapIdentityResponseVersion fences the answers cached before the series of a range function over an
// unwrapped label after a json or logfmt parser were the stream and every parsed label
// (semantics/unwrap-per-series-identity-raw-rows): a series per stream, or per line, then.
const dynamicUnwrapIdentityResponseVersion = "@unwrap-dynamic-identity-v1:"

// dynamicUnwrapIdentityResponseKeyVersion is the fence of a query whose plan has that identity at any depth
// (lower.WritesDynamicUnwrapIdentity): at its root, and in an operand of a binary operation, a ranking or a
// label_replace, which the handlers evaluate as the operand alone, so their answers changed too. It is read from the
// query's shared plan after a text screen (dynamicIdentityFenceScreen), so no other query's key changes. It does not
// read the -query-plan-lowering flag or the step: with the flag off, or on a range longer than the step, the fence
// costs one more cache miss.
func dynamicUnwrapIdentityResponseKeyVersion(query string) string {
	query = dynamicQuery(query)
	if !dynamicIdentityFenceScreen(query) {
		return ""
	}
	if plan := resolvedPlan(query); plan != nil && lower.WritesDynamicUnwrapIdentity(plan) {
		return dynamicUnwrapIdentityResponseVersion
	}
	return ""
}

// dynamicIdentityFenceScreen is the text screen of the fence: the words every plan with a dynamic identity holds, in
// any position (an unwrap, a json or logfmt parser and a function taken per series). The plan decides.
func dynamicIdentityFenceScreen(query string) bool {
	if !strings.Contains(query, "unwrap") || !strings.Contains(query, "json") && !strings.Contains(query, "logfmt") {
		return false
	}
	for _, fn := range dynamicIdentityFuncs {
		if strings.Contains(query, fn) {
			return true
		}
	}
	return false
}
