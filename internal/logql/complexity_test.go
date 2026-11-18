package logql

import (
	"strings"
	"testing"
)

// conformance: query-complexity-limits
func TestComplexityCountsNodesAndDepth(t *testing.T) {
	cases := []struct {
		query        string
		nodes, depth int
	}{
		{`1`, 1, 1},
		{`(1)`, 1, 1},
		{`((((1))))`, 1, 1},
		{`1+1`, 3, 2},
		{`1+1+1`, 5, 3},
		{`1+(1+1)`, 5, 3},
		{`{a="b"}`, 2, 1},
		{`{a="b",c="d"}`, 3, 1},
		{`{a="b"} |= "x" | json | level="error"`, 5, 1},
		{`rate({a="b"}[1m])`, 3, 2},
		{`sum(rate({a="b"}[1m]))`, 4, 3},
		{`(sum by (a) ((rate({a="b"}[1m]))))`, 4, 3},
		{`topk(3, sum by (a) (rate({a="b"}[1m])))`, 5, 4},
		{`sum(rate({a="b"}[1m])) / sum(rate({a="b"}[1m]))`, 9, 4},
		{`label_replace(rate({a="b"}[1m]), "x", "$1", "a", "(.*)")`, 4, 3},
		{`vector(1)`, 1, 1},
		{`rate({a="b"} | json [1m]) > 1`, 6, 3},
	}
	for _, c := range cases {
		expr, err := Parse(c.query)
		if err != nil {
			t.Fatalf("%q: %v", c.query, err)
		}
		if nodes, depth := Complexity(expr); nodes != c.nodes || depth != c.depth {
			t.Errorf("%q: nodes %d depth %d, want %d %d", c.query, nodes, depth, c.nodes, c.depth)
		}
	}
	if n, d := Complexity(nil); n != 0 || d != 0 {
		t.Errorf("nil expression: %d %d", n, d)
	}
	if n, d := Complexity(&StreamSelector{Matchers: make([]LabelMatcher, 2)}); n != 3 || d != 1 {
		t.Errorf("selector: %d %d", n, d)
	}
	if n, d := Complexity(&BadLiteralExpr{}); n != 1 || d != 1 {
		t.Errorf("literal: %d %d", n, d)
	}
}

// The deep shapes Loki accepts under its 128 KB input limit: the measure is linear in the query and a node
// never takes less than a byte of it.
//
// conformance: query-complexity-limits
func TestComplexityOfDeepShapes(t *testing.T) {
	shapes := map[string]string{
		"parentheses": strings.Repeat("(", 6000) + "1" + strings.Repeat(")", 6000),
		"sums":        strings.Repeat("sum(", 2000) + `rate({a="b"}[1m])` + strings.Repeat(")", 2000),
		"binary":      strings.Repeat("1+", 6000) + "1",
		"json stages": `{a="b"}` + strings.Repeat(" | json", 3000),
	}
	for name, q := range shapes {
		expr, err := Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		nodes, depth := Complexity(expr)
		if nodes == 0 || depth == 0 || nodes > len(q) || depth > len(q) {
			t.Errorf("%s: nodes %d depth %d for %d bytes", name, nodes, depth, len(q))
		}
	}
}

// FuzzComplexityBoundedByLength holds the property the derived defaults of -max-query-ast-nodes and
// -max-query-nesting-depth rest on: for every query that parses, neither the node count nor the depth exceeds the
// query's length in bytes, so a limit set to the query length limit never rejects a query that limit lets through.
//
// conformance: query-complexity-limits
func FuzzComplexityBoundedByLength(f *testing.F) {
	for _, seed := range []string{
		`1`, `(1)`, `1+1+1+1`, `{a="b"}`, `{a="b"}|json|json`, `rate({a="b"}[1m])`,
		`sum(sum(sum(rate({a="b"}[1m]))))`, `vector(1)`, `label_replace(vector(1),"a","b","c","d")`,
		`1+(1+(1+(1+1)))`, `((((((1))))))`, `sum(1+1)`, `{a="b"}|="x"|~"y"|json|logfmt|drop a|keep b`,
		`topk(1,rate({a="b"}[1m]))`, `approx_topk(1,rate({a="b"}[1m]))`,
		strings.Repeat("(", 300) + "1" + strings.Repeat(")", 300),
		strings.Repeat("sum(", 100) + `rate({a="b"}[1m])` + strings.Repeat(")", 100),
		strings.Repeat("1+", 300) + "1",
		`{a="b"}` + strings.Repeat("|json", 200),
		strings.Repeat(`label_replace(`, 40) + `vector(1)` + strings.Repeat(`,"a","b","c","d")`, 40),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 4096 {
			return
		}
		expr, err := Parse(q)
		if err != nil || expr == nil {
			return
		}
		nodes, depth := Complexity(expr)
		if nodes > len(q) || depth > len(q) || depth > nodes {
			t.Fatalf("%q: nodes %d depth %d for %d bytes", q, nodes, depth, len(q))
		}
	})
}
