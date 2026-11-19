package proxy

const reservedFieldLabelResponseVersion = "@reserved-field-labels-v2:"

// reservedFieldLabelResponseKeyVersion fences the replies cached before the query plan filtered a label called
// after a field VictoriaLogs keeps for itself as VictoriaLogs keeps it (semantics/translator-writes-reserved-field-names),
// in every cache tier that keys on the response key. The plan used to write `| _time>=5s`, `{_msg="x"}` or
// `{_TIME="a"}` as field filters, which VictoriaLogs read as its time range or line filter (a 200 with the wrong
// lines) or rejected; it now filters the stream tag, or a copy of the field for another case. v2: a metric query
// with such a filter (`count_over_time({app="a"} | _time="x" [5m])`) is served from the plan too, so its answers
// cached under v1 came from the translator's filter. A query is fenced when
// it names, outside quoted text, _time, _stream or _stream_id in any case, or _msg, followed by a matcher or a
// comparison (=, !, >, <): a stream selector matcher or a label filter. It is scanned once without allocating.
func reservedFieldLabelResponseKeyVersion(query string) string {
	if scanFenceWords(query).reservedLabel {
		return reservedFieldLabelResponseVersion
	}
	return ""
}

// reservedLabelWord reports _time, _stream or _stream_id in any case, or _msg.
func reservedLabelWord(w string) bool {
	return eqFoldWord(w, "_time") || eqFoldWord(w, "_stream") || eqFoldWord(w, "_stream_id") || w == "_msg"
}
