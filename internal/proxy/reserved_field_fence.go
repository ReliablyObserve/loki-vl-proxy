package proxy

const reservedFieldLabelResponseVersion = "@reserved-field-labels-v1:"

// reservedFieldLabelResponseKeyVersion fences the replies cached before the query plan filtered a label called
// after a field VictoriaLogs keeps for itself as VictoriaLogs keeps it (semantics/translator-writes-reserved-field-names),
// in every cache tier that keys on the response key. The plan used to write `| _time>=5s`, `{_msg="x"}` or
// `{_TIME="a"}` as field filters, which VictoriaLogs read as its time range or line filter (a 200 with the wrong
// lines) or rejected; it now filters the stream tag, or a copy of the field for another case. A query is fenced when
// it names, outside quoted text, _time, _stream or _stream_id in any case, or _msg, followed by a matcher or a
// comparison (=, !, >, <): a stream selector matcher or a label filter. It is scanned once without allocating.
func reservedFieldLabelResponseKeyVersion(query string) string {
	commentEnd := 0 // quote characters inside a comment open no quoted text; its words are read like any other
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case (c == '"' || c == '`') && i >= commentEnd:
			i = skipQuoted(query, i) + 1
		case c == '#' && i >= commentEnd:
			commentEnd = skipComment(query, i)
			i++
		case isQueryWordStart(query, i):
			j := queryWordEnd(query, i)
			if reservedLabelWord(query[i:j]) && comparisonFollows(query, j) {
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
	return eqFoldWord(w, "_time") || eqFoldWord(w, "_stream") || eqFoldWord(w, "_stream_id") || w == "_msg"
}
