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
	// which only the keep projection, keyword label, reserved field case, reserved
	// field label and marker byte fences reach. It reads no request.
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
		var b fenceChain
		if keep {
			b.add(keepMetadataProjectionResponseVersion)
		}
		b.add(keywordLabelFromScan(query, words))
		if words.reservedCase {
			b.add(reservedFieldCaseResponseVersion)
		}
		if words.reservedLabel {
			b.add(reservedFieldLabelResponseVersion)
		}
		b.add(markerBytesFence(query, words.markerQuoted))
		return b.String()
	}
	other := spelledOtherwise(query)
	var b fenceChain
	b.add(orderedJSONVectorFence(r, query, other))
	b.add(bareJSONMetricFence(r, query, other))
	if site == responseKeyInnerQueryRange {
		b.add(logfmtPresenceFence(r, query, other))
	}
	b.add(detectedLevelMutationResponseKeyVersion(query))
	if keep {
		b.add(keepMetadataProjectionResponseVersion)
	}
	if ipFilterIn(query, other) {
		b.add(ipLabelFilterResponseVersion)
	}
	if repeatedLabelFilterStages(query) {
		b.add(repeatedLabelFilterResponseVersion)
	}
	b.add(keywordLabelFromScan(query, words))
	if words.reservedCase {
		b.add(reservedFieldCaseResponseVersion)
	}
	if words.reservedLabel {
		b.add(reservedFieldLabelResponseVersion)
	}
	b.add(markerBytesFence(query, words.markerQuoted))
	if keep && containsWord(query, "unwrap", other) {
		b.add(keepUnwrapIdentityResponseVersion)
	}
	b.add(dynamicUnwrapIdentityResponseKeyVersion(query))
	streamLevel, ranked := streamLevelSeriesScan(query)
	if streamLevel || levelChainReadIn(query) {
		b.add(streamLevelSeriesResponseVersion)
	}
	b.add(unwrapBlankSpellingResponseKeyVersion(query))
	b.add(unwrapConversionBlankResponseKeyVersion(query))
	b.add(emptyWithoutOperandResponseKeyVersion(query))
	if site != responseKeyCompatQuery {
		b.add(listedWindowsResponseKeyVersion(r, query))
	}
	b.add(countVarianceResponseKeyVersion(query))
	if site == responseKeyCompatQueryRange {
		b.add(logfmtPresenceFence(r, query, other))
	}
	if site == responseKeyCompatQueryRange || site == responseKeyInnerQueryRange {
		// Only the range handler's answers of a ranking over a binary operation changed.
		b.add(rankOverBinaryResponseKeyVersion(query, ranked || words.stddev))
	}
	if site == responseKeyCompatQuery {
		// An instant ranking over a binary operand that holds a label_replace applies it once now.
		b.add(rankOverBinaryLabelReplaceResponseKeyVersion(query, ranked || words.stddev))
	}
	if site == responseKeyCompatQuery || site == responseKeyCompatQueryRange {
		// The compat edge looks its cache up before the handler validates the query.
		b.add(runeResponseKeyVersion(query))
	}
	return b.String()
}

// fenceVariablesResolved is query with its Grafana range variables (`[$__interval]`, `[$__auto]`, `[$__range]`,
// `${__rate_interval}`) resolved, as the handlers read it, for the fences that read the query's plan. The parser keeps a
// label_replace call whose first argument holds a variable as text it cannot type, and the handlers resolve the
// variable before they answer (resolveGrafanaRangeTemplateTokens), so the call is a typed one there, with whatever
// the call wraps (a binary operation, an aggregation of an unwrap, a `without ()` operand); the plan of the
// unresolved text holds an opaque node and hides all of it. What the tree holds does not depend on the length a
// variable resolves to, so the request's window and step are not read. A query with no variable (a `$1`
// replacement reference is none) is returned as it is, without a read of its text beyond one scan.
func fenceVariablesResolved(query string) string {
	for i := 0; i < len(query); i++ {
		j := strings.IndexByte(query[i:], '$')
		if j < 0 {
			return query
		}
		i += j
		if i+1 < len(query) && (query[i+1] == '_' || query[i+1] == '{') {
			return resolveGrafanaRangeTemplateTokens(query, "", "", "")
		}
	}
	return query
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
	if keepStageIn(query) && containsWord(query, "unwrap", spelledOtherwise(query)) {
		return keepUnwrapIdentityResponseVersion
	}
	return ""
}

// streamLevelSeriesResponseVersion fences the answers cached before every metric series built from one stream took
// detected_level from the stream's level fields, normalised (semantics/without-fold-stream-detected-level: they said
// unknown on the per-stream stats reads; semantics/detected-level-one-value-per-stream: the bare-parser stats read,
// the raw-row reads and the stats answers relabelled by stream copied the stored level, ERROR or Debug, and ignored
// severity, LEVEL and lvl; VictoriaLogs' detected_level chain read the level fields stored on a row before the
// stream's, so a stream labelled severity="error" whose lines carry structured metadata level="debug" said debug on
// every chain-served query, and a `| detected_level` filter kept those rows by that value). v1 named only the first
// change, v2 (never released) the first two. v4: a series read per stream from a stream without a level label carried
// no detected_level, where Loki gives its lines' level (semantics/detected-level-stream-stats-without-level).
const streamLevelSeriesResponseVersion = "@stream-level-series-v4:"

// streamLevelSeriesResponseKeyVersion is the fence of a query whose VictoriaLogs read may carry the detected_level
// chain (levelChainReadIn: any metric query, any query naming detected_level), and of a
// query that may build its series per stream on a proxy-side path: a without fold, an unwrap function, topk,
// bottomk, sort, sort_desc or label_replace over a range function (every metric query with a parser stage, whose
// series the raw-row and bare-parser reads build, holds a range too). One fence for both changes, so a key carries one
// constant and no chain is built. A text screen, so a query that builds no such series is fenced too (one more cache
// miss). Past the range check it jumps between the rare bytes the words hold (a log query has no parenthesis and
// returns at once): k for topk and bottomk, w for without and unwrap, and the name before each ( for sort, sort_desc
// and label_replace. The parser reads keywords in any case and any Unicode blank before a parenthesis (TOPK, sum
// WITHOUT, sort\v(), so the screen does too: it also jumps to K and W, compares the words case-folded, and reads a
// Kelvin sign as the k of topk and bottomk and U+0130 as the i of without (the letters outside ASCII that lower case
// into these words).
func streamLevelSeriesResponseKeyVersion(query string) string {
	if levelChainReadIn(query) {
		return streamLevelSeriesResponseVersion
	}
	if fires, _ := streamLevelSeriesScan(query); fires {
		return streamLevelSeriesResponseVersion
	}
	return ""
}

// streamLevelSeriesScan is the scan of streamLevelSeriesResponseKeyVersion: fires is the fence, and ranked reports a
// topk, bottomk, sort or sort_desc word, which the ranking over a binary operation fence reads
// (rankOverBinaryResponseKeyVersion), so neither fence scans the query again. It reads the words only: a caller fences
// a query levelChainReadIn reports too.
func streamLevelSeriesScan(query string) (fires, ranked bool) {
	if strings.IndexByte(query, '(') < 0 {
		return false, false
	}
	for _, c := range [...]byte{'k', 'K'} {
		for s, i := query, strings.IndexByte(query, c); i >= 0; i = strings.IndexByte(s, c) {
			if i >= 3 && strings.EqualFold(s[i-3:i+1], "topk") || i >= 6 && strings.EqualFold(s[i-6:i+1], "bottomk") {
				return true, true
			}
			s = s[i+1:]
		}
	}
	// The Kelvin sign (U+212A) lower cases to k, so the parser reads topK and bottomK as topk and bottomk.
	if strings.Contains(query, "\u212a") && (containsFold(query, "topk") || containsFold(query, "bottomk")) {
		return true, true
	}
	labelReplace := false
	for s, i := query, strings.IndexByte(query, '('); i >= 0; i = strings.IndexByte(s, '(') {
		name := trimRightBlanks(s[:i])
		s = s[i+1:]
		if name == "" {
			continue
		}
		// Each name ends in an ASCII letter of its word in any case: the last byte picks the one check that can match.
		switch name[len(name)-1] | 0x20 {
		case 't', 'c':
			if hasSuffixFold(name, "sort") || hasSuffixFold(name, "sort_desc") {
				return true, true
			}
		case 'y':
			// sort and sort_desc also take a grouping before their parenthesis (sort by (app) (…)).
			if ranked := groupedRankingName(name); hasSuffixFold(ranked, "sort") || hasSuffixFold(ranked, "sort_desc") {
				return true, true
			}
		case 'e':
			labelReplace = labelReplace || hasSuffixFold(name, "label_replace")
		}
	}
	if labelReplace {
		return true, false
	}
	for _, c := range [...]byte{'w', 'W'} {
		for s, i := query, strings.IndexByte(query, c); i >= 0; i = strings.IndexByte(s, c) {
			if hasFoldPrefix(s[i:], "without") || i >= 2 && i+4 <= len(s) && strings.EqualFold(s[i-2:i+4], "unwrap") {
				return true, false
			}
			s = s[i+1:]
		}
	}
	return false, false
}

// levelChainReadIn reports a query whose VictoriaLogs read may carry the detected_level chain: a [ outside quoted
// text (the range of a range aggregation, which every metric query holds) or the name detected_level outside quoted
// text (a label filter, keep or drop on it). A comment is read like any other text, as the other screens read it.
func levelChainReadIn(query string) bool {
	if strings.IndexByte(query, '[') < 0 && !strings.Contains(query, detectedLevelLabel) {
		return false
	}
	commentEnd := 0
	for i := 0; i < len(query); {
		switch c := query[i]; {
		case (c == '"' || c == '`') && i >= commentEnd:
			i = skipQuoted(query, i) + 1
		case c == '#' && i >= commentEnd:
			commentEnd = skipComment(query, i)
			i++
		case c == '[':
			return true
		case c == 'd' && strings.HasPrefix(query[i:], detectedLevelLabel) && !fenceNameByteBefore(query, i) && identEnd(query, i) == i+len(detectedLevelLabel):
			return true
		default:
			i++
		}
	}
	return false
}

// groupedRankingName returns what comes before a trailing `by` keyword (a whole word, any case, blanks the parser
// skips before it), or "" when name does not end with one.
func groupedRankingName(name string) string {
	if !hasSuffixFold(name, "by") {
		return ""
	}
	rest := name[:len(name)-2]
	trimmed := trimRightBlanks(rest)
	if len(trimmed) == len(rest) {
		return "" // no blank before by: part of a longer word
	}
	return trimmed
}

// dynamicUnwrapIdentityResponseVersion fences the answers cached before the series of a range function over an
// unwrapped label after a json or logfmt parser were the stream and every parsed label
// (semantics/unwrap-per-series-identity-raw-rows): a series per stream, or per line, then. v2: the spellings with a
// label filter or a drop after the parser under a label the lowering resolves (`| json | unwrap duration_ms |
// __error__=""`) are served from the lowering too; v1 keys of those were answered with the stream's series. v3: a root
// identity over a range longer than the step reads the series hash too (the window evaluator merges each series'
// buckets, the series that lie in one bucket folded in VictoriaLogs), so its answers cached under v2 with the stream's
// series are not served. v4: lines that hold the same keys in another order are one series (the hash reads the
// fields sorted by name, set_stream_fields, where pack_json followed the line's key order), so answers cached under v3
// with such a series split in two are not served.
const dynamicUnwrapIdentityResponseVersion = "@unwrap-dynamic-identity-v4:"

// dynamicUnwrapIdentityResponseKeyVersion is the fence of a query whose plan has that identity at any depth
// (lower.WritesDynamicUnwrapIdentity): at its root, and in an operand of a binary operation, a ranking or a
// label_replace, which the handlers evaluate as the operand alone, so their answers changed too. It is read from the
// query's shared plan after a text screen (dynamicIdentityFenceScreen), so no other query's key changes. It does not
// read the -query-plan-lowering flag or the step: with the flag off, or for an operand the handlers keep on the
// stream's series, the fence costs one more cache miss.
func dynamicUnwrapIdentityResponseKeyVersion(query string) string {
	query = dynamicQuery(query)
	// dynamicQuery lower cased every keyword spelled with an ASCII upper case letter. It leaves a word with U+0130,
	// which lower cases to i (avg_over_tİme): the one letter outside ASCII that lower cases into the screen's words,
	// none of which holds a k (U+212A).
	if !dynamicIdentityFenceScreen(query, strings.Contains(query, "\u0130")) {
		return ""
	}
	// The variables are resolved after the screen: it reads words a variable does not hold.
	if plan := resolvedPlan(fenceVariablesResolved(query)); plan != nil && lower.WritesDynamicUnwrapIdentity(plan) {
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
// any position (an unwrap, a json or logfmt parser and a function taken per series), in every spelling the parser
// reads (containsWord). The plan decides.
func dynamicIdentityFenceScreen(query string, other bool) bool {
	if !containsWord(query, "unwrap", other) || !containsWord(query, "json", other) && !containsWord(query, "logfmt", other) {
		return false
	}
	for _, fn := range dynamicIdentityFuncs {
		if containsWord(query, fn, other) {
			return true
		}
	}
	return false
}
