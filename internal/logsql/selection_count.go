package logsql

// SelectionRowCount is the query that counts the rows the filter of query selects, into the field alias: the
// cheapest read of how many rows a window holds, since no pipe of query runs (no parser, no hash). A query the parser
// does not read is counted whole, which is exact and costs what query's own pipes cost.
func SelectionRowCount(query, alias string) string {
	count := PipeStats{Funcs: []StatsFuncAlias{{Func: Count{}, Alias: alias}}}
	if q, err := Parse(query); err == nil && q.Filter != nil {
		return Query{Filter: q.Filter, Pipes: []Pipe{count}}.String()
	}
	return query + " " + count.String()
}
