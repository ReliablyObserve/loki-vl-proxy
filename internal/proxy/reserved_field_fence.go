package proxy

import "strings"

const reservedFieldLabelResponseVersion = "@reserved-field-labels-v1:"

// reservedFieldLabelResponseKeyVersion fences the replies cached before the query plan filtered a label called
// after a field VictoriaLogs keeps for itself as VictoriaLogs keeps it (semantics/translator-writes-reserved-field-names),
// in every cache tier that keys on the response key. The plan used to write `| _time>=5s`, `{_msg="x"}` or
// `{_TIME="a"}` as field filters, which VictoriaLogs read as its time range or line filter (a 200 with the wrong
// lines) or rejected; it now filters the stream tag, or a copy of the field for another case. A query is fenced when
// it names, outside quoted text, _time, _stream or _stream_id in any case, or _msg, followed by a matcher or a
// comparison (=, !, >, <): a stream selector matcher or a label filter. It is scanned once without allocating.
func reservedFieldLabelResponseKeyVersion(query string) string {
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			i = skipQuoted(query, i) + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(query) && (query[j] == '_' || query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			if reservedLabelWord(query[i:j]) && followedByComparison(query, j) {
				return reservedFieldLabelResponseVersion
			}
			i = j
		default:
			i++
		}
	}
	return ""
}

// reservedLabelWord reports _time, _stream or _stream_id in any case, or _msg.
func reservedLabelWord(w string) bool {
	return strings.EqualFold(w, "_time") || strings.EqualFold(w, "_stream") || strings.EqualFold(w, "_stream_id") || w == "_msg"
}

// followedByComparison reports =, !, > or < after optional blanks from query[i].
func followedByComparison(query string, i int) bool {
	for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
		i++
	}
	return i < len(query) && (query[i] == '=' || query[i] == '!' || query[i] == '>' || query[i] == '<')
}
