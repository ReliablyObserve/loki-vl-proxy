package proxy

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// markerBytesLabelValueResponseVersion fences the answers cached before a log query with a parser or another stage
// and a label filter value that holds a \x00, \x01 or \x02 byte was served from the query plan
// (semantics/translator-reads-marker-bytes-in-quoted-value): the string translator cut such a value at its stage
// markers and filtered on another one, so the answer kept other lines.
const markerBytesLabelValueResponseVersion = "@marker-bytes-label-value-v1:"

// markerBytesLabelValueResponseKeyVersion is the fence of a log query that holds such a value (markerBytesFence over
// the query's own word scan).
func markerBytesLabelValueResponseKeyVersion(query string) string {
	return markerBytesFence(query, scanFenceWords(query).markerQuoted)
}

// markerBytesFence is the fence of a log query that holds such a value, read from the query's shared plan
// (misreadFilterValue of the marker kind, on a log query with a stage the filter-only path does not serve already)
// when quoted is set: the word scan (scanFenceWords) found a quoted string that holds a byte from \x00 to \x02, raw in
// any quotes or as a \x, \u, \U or octal escape in double quotes (skipQuotedMarker), every spelling of such a value. A query that starts with a name or a number is a metric query and is not planned here. No other query's key
// changes.
func markerBytesFence(query string, quoted bool) string {
	if !quoted || startsWithName(query) {
		return ""
	}
	plan := resolvedPlan(fenceVariablesResolved(query))
	if plan == nil {
		return ""
	}
	if sel, ok := plan.Root.(*queryplan.LogSelect); ok && !filterOnly(sel) && misreadFilterValue(sel, misreadMarker) {
		return markerBytesLabelValueResponseVersion
	}
	return ""
}

// startsWithName reports a query whose first byte after blanks and parentheses starts a name or a number.
func startsWithName(query string) bool {
	i := 0
	for i < len(query) && (query[i] == ' ' || query[i] == '(' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
		i++
	}
	if i == len(query) {
		return true
	}
	c := query[i]
	return c|0x20 >= 'a' && c|0x20 <= 'z' || c == '_' || c >= '0' && c <= '9'
}

// skipQuotedMarker is skipQuoted that also reports whether the quoted text holds a byte from \x00 to \x02, raw or (in
// double quotes) as an escape that decodes to one (\x00, \u0001, \U00000002, \002), in the same pass.
func skipQuotedMarker(query string, i int) (end int, marker bool) {
	q := query[i]
	for j := i + 1; j < len(query); j++ {
		switch c := query[j]; {
		case c == '\\' && q == '"':
			marker = marker || lowEscape(query[j+1:])
			j++
		case c == q:
			return j, marker
		case c <= 2:
			marker = true
		}
	}
	return len(query) - 1, marker
}

// lowEscape reports an escape, read after its backslash, that decodes to a byte from \x00 to \x02.
func lowEscape(s string) bool {
	switch {
	case strings.HasPrefix(s, "x0"):
		s = s[2:]
	case strings.HasPrefix(s, "u000"):
		s = s[4:]
	case strings.HasPrefix(s, "U0000000"):
		s = s[8:]
	case strings.HasPrefix(s, "00"):
		s = s[2:]
	default:
		return false
	}
	return s != "" && s[0] >= '0' && s[0] <= '2'
}
