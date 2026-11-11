package logsql

import "strings"

// KeywordFieldName reports whether VictoriaLogs (v1.49 and v1.53, in any case) cannot read name, written bare, as
// the name of a field where a filter begins or follows another: not and in are keywords there, and contains_all,
// contains_any, contains_common_case and equals_common_case are filter functions, so `not:="x"` and `in:="x"` are
// rejected; and and or are operators, so `a:="1" or:="x"` is rejected; options opens the query options, so
// `options:="x"` is rejected as the first filter. A name that VictoriaLogs accepts bare (i, exact, re, seq, range,
// offset, of, ...) is not listed: quoting it would change a query that already works. Quoted, every one of them
// is read as a field. A name that is already quoted is not a keyword.
func KeywordFieldName(name string) bool {
	switch len(name) {
	case 2:
		return strings.EqualFold(name, "or") || strings.EqualFold(name, "in")
	case 3:
		return strings.EqualFold(name, "and") || strings.EqualFold(name, "not")
	case 7:
		return strings.EqualFold(name, "options")
	case 12:
		return strings.EqualFold(name, "contains_all") || strings.EqualFold(name, "contains_any")
	case 18:
		return strings.EqualFold(name, "equals_common_case")
	case 20:
		return strings.EqualFold(name, "contains_common_case")
	}
	return false
}

// QuoteKeywordField is name quoted when it is a keyword field name (KeywordFieldName), else name. The string
// translator writes these names bare, and VictoriaLogs rejects the query (Loki accepts a label called not or in);
// the query plan writes them quoted, wherever a filter names a field.
func QuoteKeywordField(name string) string {
	if !KeywordFieldName(name) {
		return name
	}
	return quoteLogsQL(name)
}

// ReservedFieldName reports a name VictoriaLogs keeps for itself: _time (the timestamp), _stream and _stream_id
// (the stream and its id) and _msg (the line). A filter on _time, _stream or _stream_id is the time, stream or
// stream id filter, bare or quoted, so no field filter can name them; and a label with one of these names reaches
// VictoriaLogs only inside the stream: a stream label called so is a tag of _stream (`_stream:{_time="a"}` finds
// it), while a field of the entry called so is dropped (_time, _stream, _stream_id) or replaces the line (_msg).
// Only lower case: in another case they are fields (ReservedFieldNameOtherCase).
func ReservedFieldName(name string) bool {
	switch name {
	case "_time", "_stream", "_stream_id", "_msg":
		return true
	}
	return false
}
