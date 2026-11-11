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
	// which only the keep projection, keyword label and reserved field case fences
	// reach. It reads no request.
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
		return keepMetadataProjectionResponseKeyVersion(query) + keywordLabelResponseKeyVersion(query) + reservedFieldCaseResponseKeyVersion(query)
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
	if site == responseKeyCompatQueryRange {
		b.WriteString(logfmtPresenceResponseKeyVersion(r, query))
	}
	if site == responseKeyCompatQuery || site == responseKeyCompatQueryRange {
		// The compat edge looks its cache up before the handler validates the query.
		b.WriteString(runeResponseKeyVersion(query))
	}
	return b.String()
}
