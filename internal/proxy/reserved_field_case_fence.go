package proxy

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"

const reservedFieldCaseResponseVersion = "@reserved-field-case-v1:"

// reservedFieldCaseResponseKeyVersion fences the replies cached before the outbound guard (guardOutboundQuery)
// rewrote a filter on a label named _time, _stream or _stream_id in another letter case, in every cache tier that
// keys on the response key. Such a filter was sent as `_TIME:...`, which VictoriaLogs reads as its own filter: a 400,
// or for `| _TIME >= 5s` a time range answered with every line of the window, a 200 that would be served again. A
// query is fenced when it names, outside quoted text, such a name followed by a matcher or a comparison (=, !, >, <).
// It is scanned once and allocates nothing.
func reservedFieldCaseResponseKeyVersion(query string) string {
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			i = closingQuote(query, i) + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(query) && (query[j] == '_' || query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			if logsql.ReservedFieldNameOtherCase(query[i:j]) && comparisonFollows(query, j) {
				return reservedFieldCaseResponseVersion
			}
			i = j
		default:
			i++
		}
	}
	return ""
}

// closingQuote is the index of the byte that closes the quoted text opened at query[i] (the last byte when unclosed).
func closingQuote(query string, i int) int {
	q := query[i]
	for j := i + 1; j < len(query); j++ {
		switch {
		case query[j] == '\\' && q == '"':
			j++
		case query[j] == q:
			return j
		}
	}
	return len(query) - 1
}

// comparisonFollows reports =, !, > or < after optional blanks from query[i].
func comparisonFollows(query string, i int) bool {
	for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
		i++
	}
	return i < len(query) && (query[i] == '=' || query[i] == '!' || query[i] == '>' || query[i] == '<')
}
