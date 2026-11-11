package proxy

const runeResponseVersion = "@runes-v1:"

// runeResponseKeyVersion fences the replies cached before the LogQL scanner read the characters of a query as Loki's
// lexer does (internal/logql, semantics/logql-unicode-runes) at the compat edge of query and query_range, which looks
// its cache up before the handler validates the query. A query that was answered and is Loki's 400 now (a stage
// name followed by a character that starts no token: `| x ²`, `| x $`) would be served its old 200 otherwise.
//
// The fence applies to a query the validation rejects, and to no other: the key of a valid query does not change.
// The validation is memoized by the query text (the step alignment reads the same entry before the handler does).
func runeResponseKeyVersion(query string) string {
	if cachedQueryValidation(query).msg == "" {
		return ""
	}
	return runeResponseVersion
}
