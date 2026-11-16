package proxy

import (
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

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
	b.add(dynamicUnwrapIdentityResponseKeyVersion(query))
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
// w for without and unwrap, and the name before each ( for sort, sort_desc and label_replace. The parser reads
// keywords in any case and any Unicode blank before a parenthesis (TOPK, sum WITHOUT, sort\v(), so the screen does
// too: it also jumps to K and W and compares the words case-folded.
func streamLevelSeriesResponseKeyVersion(query string) string {
	if strings.IndexByte(query, '(') < 0 {
		return ""
	}
	for _, c := range [...]byte{'k', 'K'} {
		for s, i := query, strings.IndexByte(query, c); i >= 0; i = strings.IndexByte(s, c) {
			if i >= 3 && strings.EqualFold(s[i-3:i+1], "topk") || i >= 6 && strings.EqualFold(s[i-6:i+1], "bottomk") {
				return streamLevelSeriesResponseVersion
			}
			s = s[i+1:]
		}
	}
	for _, c := range [...]byte{'w', 'W'} {
		for s, i := query, strings.IndexByte(query, c); i >= 0; i = strings.IndexByte(s, c) {
			if i+7 <= len(s) && strings.EqualFold(s[i:i+7], "without") || i >= 2 && i+4 <= len(s) && strings.EqualFold(s[i-2:i+4], "unwrap") {
				return streamLevelSeriesResponseVersion
			}
			s = s[i+1:]
		}
	}
	for s, i := query, strings.IndexByte(query, '('); i >= 0; i = strings.IndexByte(s, '(') {
		name := trimRightBlanks(s[:i])
		if hasSuffixFold(name, "sort") || hasSuffixFold(name, "sort_desc") || hasSuffixFold(name, "label_replace") {
			return streamLevelSeriesResponseVersion
		}
		s = s[i+1:]
	}
	return ""
}

// dynamicUnwrapIdentityResponseVersion fences the answers cached before the series of a range function over an
// unwrapped label after a json or logfmt parser were the stream and every parsed label
// (semantics/unwrap-per-series-identity-raw-rows): a series per stream, or per line, then. v2: the spellings with a
// label filter or a drop after the parser under a label the lowering resolves (`| json | unwrap duration_ms |
// __error__=""`) are served from the lowering too; v1 keys of those were answered with the stream's series.
const dynamicUnwrapIdentityResponseVersion = "@unwrap-dynamic-identity-v2:"

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

// trimRightBlanks drops the blanks the LogQL parser skips (unicode.IsSpace) from the end of s.
func trimRightBlanks(s string) string {
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if !unicode.IsSpace(r) {
			break
		}
		s = s[:len(s)-size]
	}
	return s
}

func hasSuffixFold(s, suffix string) bool {
	return len(s) >= len(suffix) && strings.EqualFold(s[len(s)-len(suffix):], suffix)
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
