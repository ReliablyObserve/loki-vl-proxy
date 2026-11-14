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
	if site == responseKeyLogWindowShape {
		return keepMetadataProjectionResponseKeyVersion(query) + keywordLabelResponseKeyVersion(query) + reservedFieldCaseResponseKeyVersion(query) + reservedFieldLabelResponseKeyVersion(query)
	}
	var b fenceChain
	b.add(orderedJSONVectorResponseKeyVersion(r, query))
	b.add(bareJSONMetricResponseKeyVersion(r, query))
	if site == responseKeyInnerQueryRange {
		b.add(logfmtPresenceResponseKeyVersion(r, query))
	}
	b.add(detectedLevelMutationResponseKeyVersion(query))
	b.add(keepMetadataProjectionResponseKeyVersion(query))
	b.add(ipLabelFilterResponseKeyVersion(query))
	b.add(repeatedLabelFilterResponseKeyVersion(query))
	b.add(keywordLabelResponseKeyVersion(query))
	b.add(reservedFieldCaseResponseKeyVersion(query))
	b.add(reservedFieldLabelResponseKeyVersion(query))
	b.add(keepUnwrapIdentityResponseKeyVersion(query))
	b.add(streamLevelSeriesResponseKeyVersion(query))
	if site == responseKeyCompatQueryRange {
		b.add(logfmtPresenceResponseKeyVersion(r, query))
	}
	if site == responseKeyCompatQuery || site == responseKeyCompatQueryRange {
		// The compat edge looks its cache up before the handler validates the query.
		b.add(runeResponseKeyVersion(query))
	}
	return b.String()
}

// fenceChain concatenates the fences that fire; one fence (or none) is
// returned as its constant, without building a string.
type fenceChain struct {
	one string
	n   int
	b   strings.Builder
}

func (c *fenceChain) add(s string) {
	if s == "" {
		return
	}
	switch c.n {
	case 0:
		c.one = s
	case 1:
		c.b.WriteString(c.one)
		c.b.WriteString(s)
	default:
		c.b.WriteString(s)
	}
	c.n++
}

func (c *fenceChain) String() string {
	if c.n <= 1 {
		return c.one
	}
	return c.b.String()
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

// streamLevelSeriesResponseVersion fences the answers cached before the series of a per-stream stats read took
// detected_level from the stream's level field (semantics/without-fold-stream-detected-level): they said unknown.
const streamLevelSeriesResponseVersion = "@stream-level-series-v1:"

// streamLevelSeriesResponseKeyVersion is the fence of a query that may read its series per stream without a level
// column: a without fold, an unwrap function, and topk, bottomk, sort, sort_desc or label_replace over a range
// function (each reads the per-stream series the proxy labels). The words are those of every query whose label sets
// changed in a diff of range functions, parsers and outer forms against the previous build. A text screen, so a
// query that reads no such series is fenced too (one more cache miss), and no other query's key changes. It jumps
// between the rare bytes the words hold (a log query has no parenthesis and returns at once): k for topk and bottomk,
// w for without and unwrap, and the name before each ( for sort, sort_desc and label_replace.
func streamLevelSeriesResponseKeyVersion(query string) string {
	if strings.IndexByte(query, '(') < 0 {
		return ""
	}
	for s, i := query, strings.IndexByte(query, 'k'); i >= 0; i = strings.IndexByte(s, 'k') {
		if i >= 3 && s[i-3:i+1] == "topk" || i >= 6 && s[i-6:i+1] == "bottomk" {
			return streamLevelSeriesResponseVersion
		}
		s = s[i+1:]
	}
	for s, i := query, strings.IndexByte(query, 'w'); i >= 0; i = strings.IndexByte(s, 'w') {
		if strings.HasPrefix(s[i:], "without") || i >= 2 && strings.HasPrefix(s[i-2:], "unwrap") {
			return streamLevelSeriesResponseVersion
		}
		s = s[i+1:]
	}
	for s, i := query, strings.IndexByte(query, '('); i >= 0; i = strings.IndexByte(s, '(') {
		j := i
		for j > 0 && (s[j-1] == ' ' || s[j-1] == '\t' || s[j-1] == '\n' || s[j-1] == '\r') {
			j--
		}
		if name := s[:j]; strings.HasSuffix(name, "sort") || strings.HasSuffix(name, "sort_desc") || strings.HasSuffix(name, "label_replace") {
			return streamLevelSeriesResponseVersion
		}
		s = s[i+1:]
	}
	return ""
}
