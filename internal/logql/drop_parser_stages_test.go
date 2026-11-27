package logql

import (
	"strings"
	"testing"
)

// DropParserStages removes a json or logfmt stage where the parser read it. A stage that is the last one before
// a range keeps the range, its offset and the closing parentheses (the text pattern it replaces cut them, #1821);
// everything else is written as it was.
//
// conformance: semantics/parser-probe-drop-keeps-range
func TestDropParserStages(t *testing.T) {
	for _, tc := range []struct {
		query            string
		dropJSON, dropLF string
	}{
		{`sum(count_over_time({a="b"} | json | logfmt [3h]))`, `sum(count_over_time({a="b"} | logfmt [3h]))`, `sum(count_over_time({a="b"} | json [3h]))`},
		{`sum(count_over_time({a="b"} | logfmt | json [3h]))`, `sum(count_over_time({a="b"} | logfmt [3h]))`, `sum(count_over_time({a="b"} | json [3h]))`},
		{`sum(count_over_time({a="b"} | json | logfmt | drop __error__, __error_details__ [3h]))`, `sum(count_over_time({a="b"} | logfmt | drop __error__, __error_details__ [3h]))`, `sum(count_over_time({a="b"} | json | drop __error__, __error_details__ [3h]))`},
		{`count_over_time({a="b"} | json | logfmt [3h] offset 1h)`, `count_over_time({a="b"} | logfmt [3h] offset 1h)`, `count_over_time({a="b"} | json [3h] offset 1h)`},
		{`count_over_time({a="b"}[3h] | json | logfmt)`, `count_over_time({a="b"}[3h] | logfmt)`, `count_over_time({a="b"}[3h] | json)`},
		{`quantile_over_time(0.9, {a="b"} | json | logfmt | unwrap x [3h])`, `quantile_over_time(0.9, {a="b"} | logfmt | unwrap x [3h])`, `quantile_over_time(0.9, {a="b"} | json | unwrap x [3h])`},
		{`label_replace(sum(count_over_time({a="b"} | json | logfmt [30m])), "a", "$1", "b", "(.*)")`, `label_replace(sum(count_over_time({a="b"} | logfmt [30m])), "a", "$1", "b", "(.*)")`, `label_replace(sum(count_over_time({a="b"} | json [30m])), "a", "$1", "b", "(.*)")`},
		{`approx_topk(2, sum by (x) (count_over_time({a="b"} | json | logfmt [15m])))`, `approx_topk(2, sum by (x) (count_over_time({a="b"} | logfmt [15m])))`, `approx_topk(2, sum by (x) (count_over_time({a="b"} | json [15m])))`},
		{`topk(2, sum by (x) (count_over_time({a="b"} | json | logfmt --strict a="b", c [15m])))`, `topk(2, sum by (x) (count_over_time({a="b"} | logfmt --strict a="b", c [15m])))`, `topk(2, sum by (x) (count_over_time({a="b"} | json [15m])))`},
		{`sum(count_over_time({a="b"} |= "| logfmt [1m]" | json a="b", c | logfmt [3h]))`, `sum(count_over_time({a="b"} |= "| logfmt [1m]" | logfmt [3h]))`, `sum(count_over_time({a="b"} |= "| logfmt [1m]" | json a="b", c [3h]))`},
		{`sum(count_over_time({a="b"} | json | logfmt [3h])) + 1`, `sum(count_over_time({a="b"} | logfmt [3h])) + 1`, `sum(count_over_time({a="b"} | json [3h])) + 1`},
		{`{a="b"} | json | logfmt`, `{a="b"} | logfmt`, `{a="b"} | json`},
		{`{a="b"} | json | line_format "{{.a}}" | json`, `{a="b"} | line_format "{{.a}}"`, `{a="b"} | json | line_format "{{.a}}" | json`},
		// Nothing to remove, or a query that does not parse: the text as it is.
		{`sum(count_over_time({a="b"} | regexp "(?P<x>.)" [3h]))`, `sum(count_over_time({a="b"} | regexp "(?P<x>.)" [3h]))`, `sum(count_over_time({a="b"} | regexp "(?P<x>.)" [3h]))`},
		{`sum(count_over_time({a="b"} | json | logfmt [3h]`, `sum(count_over_time({a="b"} | json | logfmt [3h]`, `sum(count_over_time({a="b"} | json | logfmt [3h]`},
		{`label_replace(vector(1), "a|b", "", "", "")`, `label_replace(vector(1), "a|b", "", "", "")`, `label_replace(vector(1), "a|b", "", "", "")`},
	} {
		if got := DropParserStages(tc.query, ParserJSON); got != tc.dropJSON {
			t.Errorf("DropParserStages(%q, json) = %q, want %q", tc.query, got, tc.dropJSON)
		}
		if got := DropParserStages(tc.query, ParserLogfmt); got != tc.dropLF {
			t.Errorf("DropParserStages(%q, logfmt) = %q, want %q", tc.query, got, tc.dropLF)
		}
	}
	if q := `{a="b"} | json | regexp "(?P<x>.)"`; DropParserStages(q, ParserRegexp) != q {
		t.Errorf("only json and logfmt stages are removed")
	}
}

// The text DropParserStages writes parses to the tree of the query it was given with those stages taken out: the
// same functions, ranges, offsets, groupings, operands and other stages, for every query of the seeds.
//
// conformance: semantics/parser-probe-drop-keeps-range
func TestDropParserStagesKeepsTheTree(t *testing.T) {
	for _, q := range dropParserStagesSeeds {
		for _, drop := range []ParserType{ParserJSON, ParserLogfmt} {
			checkDropParserStages(t, q, drop)
		}
	}
}

// FuzzDropParserStages: for any text, the stages removed leave a query whose tree is the given query's without
// them, and a text that does not parse is returned as it is.
//
// conformance: semantics/parser-probe-drop-keeps-range
func FuzzDropParserStages(f *testing.F) {
	for _, q := range dropParserStagesSeeds {
		f.Add(q, true)
	}
	f.Fuzz(func(t *testing.T, q string, json bool) {
		if len(q) > 4096 {
			return
		}
		drop := ParserLogfmt
		if json {
			drop = ParserJSON
		}
		checkDropParserStages(t, q, drop)
	})
}

var dropParserStagesSeeds = []string{
	`sum(count_over_time({a="b"} | json | logfmt | drop __error__, __error_details__ [3h]))`,
	`sum by (route) (rate({a="b"} | logfmt | json [5m] offset 1h))`,
	`topk(2, sort_desc(sum by (x) (bytes_over_time({a="b"} |= "x" | json | logfmt [1m]))))`,
	`quantile_over_time(0.9, {a="b"} | json a="b" | logfmt --strict | unwrap x | __error__="" [3h]) by (a)`,
	`label_replace(sum(count_over_time({a="b"} | json | logfmt [30m])), "a", "$1", "b", "(.*)")`,
	`approx_topk(2, sum by (x) (count_over_time({a="b"} | json | logfmt [15m])))`,
	`sum(count_over_time({a="b"} | json | logfmt [5m])) / on (a) group_left sum(rate({a="b"} | logfmt | json [5m]))`,
	`count_over_time({a="b"}[3h] | json | logfmt)`,
	`{a="b"} | json | logfmt | line_format "{{.x}} | logfmt"`,
	`vector(1) + absent_over_time({a="b"} | json | logfmt [5m])`,
}

// checkDropParserStages holds DropParserStages(q, drop) to q's tree without drop's stages.
func checkDropParserStages(t *testing.T, q string, drop ParserType) {
	t.Helper()
	got := DropParserStages(q, drop)
	before, err := Parse(q)
	if err != nil {
		if got != q {
			t.Fatalf("%q does not parse (%v) but was rewritten to %q", q, err, got)
		}
		return
	}
	after, err := Parse(got)
	if err != nil {
		t.Fatalf("DropParserStages(%q) = %q, which does not parse: %v", q, got, err)
	}
	want, gotTree := dropStagesTreeText(before, drop, false), dropStagesTreeText(after, drop, true)
	if want != gotTree {
		t.Fatalf("DropParserStages(%q) = %q\ntree  %s\nwant  %s", q, got, gotTree, want)
	}
}

// dropStagesTreeText prints e with the stages of type drop left out (strict: none may be there), and the inner
// expression of a typed label_replace printed as a tree, not as the call's text. A call kept as text untyped
// (approx_topk) prints its name only: its arguments are other queries, read on their own.
func dropStagesTreeText(e Expr, drop ParserType, strict bool) string {
	switch n := e.(type) {
	case *LogQuery:
		var b strings.Builder
		b.WriteString(n.Selector.String())
		for _, s := range n.Pipeline {
			if ps, ok := s.(*ParserStage); ok && ps.Type == drop {
				if strict {
					return "a " + ps.String() + " stage is left in " + n.String()
				}
				continue
			}
			b.WriteString(" " + s.String())
		}
		return b.String()
	case *RangeAggregation:
		return string(n.Op) + "<" + n.Range + "|" + n.Offset + "|" + formatNumber(n.Param) + "|" + groupingText(n.Grouping) + ">(" + dropStagesTreeText(n.Inner, drop, strict) + ")"
	case *VectorAggregation:
		return string(n.Op) + "<" + groupingText(n.Grouping) + "|" + formatNumber(n.Param) + ">(" + dropStagesTreeText(n.Inner, drop, strict) + ")"
	case *BinOpExpr:
		return "(" + dropStagesTreeText(n.Left, drop, strict) + " " + n.Op + " " + n.VectorMatching.String() + " " + dropStagesTreeText(n.Right, drop, strict) + ")"
	case *OpaqueMetricExpr:
		if n.Typed && n.Inner != nil {
			return n.Func + "(" + dropStagesTreeText(n.Inner, drop, strict) + "," + strings.Join(n.Args, ",") + ")"
		}
		return n.Func + "(...)"
	case nil:
		return "<nil>"
	default:
		return e.String()
	}
}

func groupingText(g *Grouping) string {
	if g == nil {
		return ""
	}
	return g.String()
}
