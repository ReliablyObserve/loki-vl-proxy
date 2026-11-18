package logql

// Complexity measures the size of a parsed expression, for the operator limits on query complexity
// (-max-query-ast-nodes, -max-query-nesting-depth).
//
// nodes counts every expression node (a log query, a range or vector aggregation, a binary operation, a
// literal, an opaque metric call such as label_replace and the typed expression inside it), every pipeline
// stage of a log query and every matcher of a stream selector. depth is the longest chain of nested
// expression nodes: a literal or a log query alone is 1, `sum(rate({a="b"}[1m]))` is 3. Parentheses create no
// node, so they add to neither.
//
// A query that parses has at most len(query) nodes and a depth of at most len(query): every node is made of at
// least one byte of the text (FuzzComplexityBoundedByLength holds the property), which is why limits derived from
// the query length never reject a query the length limit lets through. A nil expression (the bare `*` wildcard)
// is 0 and 0.
func Complexity(expr Expr) (nodes, depth int) {
	switch e := expr.(type) {
	case nil:
		return 0, 0
	case *LogQuery:
		n := 1 + len(e.Pipeline)
		if e.Selector != nil {
			n += len(e.Selector.Matchers)
		}
		return n, 1
	case *StreamSelector:
		return 1 + len(e.Matchers), 1
	case *RangeAggregation:
		return nested(1, e.Inner)
	case *VectorAggregation:
		return nested(1, e.Inner)
	case *OpaqueMetricExpr:
		return nested(1, e.Inner)
	case *BinOpExpr:
		ln, ld := Complexity(e.Left)
		rn, rd := Complexity(e.Right)
		return 1 + ln + rn, 1 + max(ld, rd)
	}
	return 1, 1 // a literal, or a node a later grammar adds
}

// nested is the complexity of a node of own nodes over one operand.
func nested(own int, inner Expr) (nodes, depth int) {
	n, d := Complexity(inner)
	return own + n, 1 + d
}
