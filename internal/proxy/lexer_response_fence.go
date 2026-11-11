package proxy

const lexerResponseVersion = "@lexer-v1:"

// lexerResponseKeyVersion fences the replies cached before the LogQL lexer read a query as Loki's does
// (internal/logql, semantics/logql-loki-whitespace) at the compat edge of query and query_range, which looks its
// cache up before the handler validates the query. A query that was answered and is Loki's 400 now would be served
// its old 200 otherwise:
//
//   - white space Loki's lexer does not skip (a vertical tab, a form feed, U+0085, U+00A0, U+2003, ...) and any other
//     control character, outside quoted text;
//   - a blank or line break inside a [range], a line break inside a double-quoted string;
//   - a by, without, on, ignoring or group list that is not names separated by commas, and on or ignoring without
//     its list;
//   - an unresolved Grafana variable ($__auto) in a range inside label_replace.
//
// The fence applies to a query the validation rejects, and to no other: the key of a valid query does not change.
// The validation is memoized by the query text (the step alignment reads the same entry before the handler does),
// so a candidate scan of the text would cost more than the lookup it saves.
func lexerResponseKeyVersion(query string) string {
	if cachedQueryValidation(query).msg == "" {
		return ""
	}
	return lexerResponseVersion
}
