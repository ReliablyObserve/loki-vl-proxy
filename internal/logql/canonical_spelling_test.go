package logql

import (
	"strconv"
	"strings"
	"testing"
)

// The respellings CanonicalSpelling writes, and the spellings it keeps.
//
// conformance: semantics/translator-reads-blank-before-range-call-as-text, semantics/translator-reads-parenthesized-metric-as-text, semantics/vector-call-blank-before-parenthesis-as-query
func TestCanonicalSpelling(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// A blank between a metric call's name and its parenthesis.
		{`rate ({app="x"}[5m])`, `rate({app="x"}[5m])`},
		{"rate\t({app=\"x\"}[5m])", `rate({app="x"}[5m])`},
		{"count_over_time\n\r ({app=\"x\"}[5m])", `count_over_time({app="x"}[5m])`},
		{`sum (rate({app="x"}[5m]))`, `sum(rate({app="x"}[5m]))`},
		{`sum (rate({app="x"}[5m])) by (a)`, `sum(rate({app="x"}[5m])) by (a)`},
		{`topk (2, sum by (a) (rate({app="x"}[5m])))`, `topk(2, sum by (a) (rate({app="x"}[5m])))`},
		{`sort_desc (rate({app="x"}[5m]))`, `sort_desc(rate({app="x"}[5m]))`},
		{`rate({app="x"}[5m]) + vector (1)`, `rate({app="x"}[5m]) + vector(1)`},
		{`label_replace (rate({app="x"}[5m]), "a", "$1", "b", "(.*)")`, `label_replace(rate({app="x"}[5m]), "a", "$1", "b", "(.*)")`},
		{`min(max (count(rate({app="uw"} [1m])) without (rack))) by (svc)`, `min(max(count(rate({app="uw"} [1m])) without (rack))) by (svc)`},
		// Blanks right inside a metric call's parentheses.
		{`vector ( 2 ) * sum(rate({app="x"}[5m]))`, `vector(2) * sum(rate({app="x"}[5m]))`},
		{`sum by (a) ( rate({app="x"}[5m]) )`, `sum by (a) (rate({app="x"}[5m]))`},
		{`rate( {app="x"}[5m] )`, `rate({app="x"}[5m])`},
		{`quantile_over_time( 0.5, {app="x"} | unwrap n [5m] )`, `quantile_over_time(0.5, {app="x"} | unwrap n [5m])`},
		{`topk( 2, rate({app="x"}[5m]) )`, `topk(2, rate({app="x"}[5m]))`},
		{`label_replace( sum by (job) ( sum_over_time( {app="uw"} | unwrap n [5m] ) ), "foo", "$1", "service", "(.*):.*" )`,
			`label_replace(sum by (job) (sum_over_time({app="uw"} | unwrap n [5m])), "foo", "$1", "service", "(.*):.*")`},
		// Parentheses around a call, around parentheses, around a binary operation no operator reads.
		{`(count_over_time({app="x"}[5m]))`, `count_over_time({app="x"}[5m])`},
		{`((sum(rate({app="x"}[5m]))))`, `sum(rate({app="x"}[5m]))`},
		{`( rate({app="x"}[5m]) )`, `rate({app="x"}[5m])`},
		{`sum((rate({app="x"}[5m])))`, `sum(rate({app="x"}[5m]))`},
		{`(sum(rate({app="x"}[5m]))) / (sum(rate({app="y"}[5m])))`, `sum(rate({app="x"}[5m])) / sum(rate({app="y"}[5m]))`},
		{`(rate({app="x"}[5m]) + 1)`, `rate({app="x"}[5m]) + 1`},
		{`((rate({app="x"}[5m]) + 1))`, `rate({app="x"}[5m]) + 1`},
		{`sum((rate({app="x"}[5m]) + 1))`, `sum(rate({app="x"}[5m]) + 1)`},
		{`topk(2, (rate({app="x"}[5m]) * 2))`, `topk(2, rate({app="x"}[5m]) * 2)`},
		{`(vector (2) + sum(rate({app="x"}[5m]))) * 2`, `(vector(2) + sum(rate({app="x"}[5m]))) * 2`},
		{`rate({app="x"}[5m]) > bool (vector(1))`, `rate({app="x"}[5m]) > bool vector(1)`},
		{`rate({app="x"}[5m]) and(rate({app="y"}[5m]))`, `rate({app="x"}[5m]) and rate({app="y"}[5m])`},
		{`rate({app="x"}[5m]) and((rate({app="y"}[5m])))`, `rate({app="x"}[5m]) and rate({app="y"}[5m])`},
		{`rate({app="x"}[5m]) > bool((vector(1)))`, `rate({app="x"}[5m]) > bool vector(1)`},
		{`label_replace((rate({app="x"}[5m])), "a", "$1", "b", "(.*)")`, `label_replace(rate({app="x"}[5m]), "a", "$1", "b", "(.*)")`},
	} {
		got := CanonicalSpelling(tc.in)
		if got != tc.want {
			t.Errorf("CanonicalSpelling(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			continue
		}
		if again := CanonicalSpelling(got); again != got {
			t.Errorf("CanonicalSpelling(%q) is %q, and respelled again %q", tc.in, got, again)
		}
		if !sameParsedTree(t, tc.in, got) {
			t.Errorf("%q and its respelling %q parse to other trees", tc.in, got)
		}
	}
	for _, q := range []string{
		// Already in the spelling.
		`rate({app="x"}[5m])`,
		`sum by (a) (rate({app="x"}[5m]))`,
		`sum by(a)(rate({app="x"}[5m]))`,
		`sum(rate({app="x"}[5m])) by (a)`,
		`rate({app="x"}[5m]) / on(app) group_left(pod) sum by (app) (rate({app="x"}[5m]))`,
		// Parentheses right after a matching clause.
		`rate({app="x"}[5m]) / on (app) (sum by (app) (rate({app="x"}[5m])))`,
		`rate({app="x"}[5m]) / ignoring(pod) group_left (sum(rate({app="x"}[5m])))`,
		// Parentheses an operator reads, around a number, around a log query (CanonicalizeLogRanges' part).
		`(rate({app="x"}[5m]) + 1) * 2`,
		`2 * (rate({app="x"}[5m]) + 1)`,
		`rate({app="x"}[5m]) * (2)`,
		`rate({app="x"}[5m]) - (-1)`,
		`count_over_time(({app="x"} | json)[5m])`,
		`({app="x"} |= "a")`,
		// Blanks the function does not read: inside log queries and their pipelines, in clauses, between operands.
		`rate({ app = "x" } [5m])`,
		`rate({app="x"} | json | ( a="1" or b="2" ) [5m])`,
		`rate({app="x"} | json | a="1" and (b="2" or c="3") [5m])`,
		`sum_over_time({app="x"} | unwrap duration ( n ) [5m])`,
		`sum by ( a ) (rate({app="x"}[5m]))`,
		`rate({app="x"}[5m])  +  1`,
		`{app="x"} |= "rate (" |= "( x )"`,
		"{app=\"x\"} |= `sum ((`",
		// A comment, a rejected query, a lexer error, a check over the tree: kept as sent.
		"rate ({app=\"x\"}[5m]) # a comment",
		"rate (# c\n{app=\"x\"}[5m])",
		`rate ({app="x"}[5m]`,
		`rate ({app="x"}[5m]) +`,
		`rate ({app="x"}[5x])`,
		`topk (0, rate({app="x"}[5m]))`,
		`sum (rate({app="x"}[5m]) | json)`,
		`foo (rate({app="x"}[5m]))`,
		`approx_topk (2, rate({app="x"}[5m]))`,
		"label_replace((sum_over_time({}[$])\"\n) )",
		"",
	} {
		if got := CanonicalSpelling(q); got != q {
			t.Errorf("CanonicalSpelling(%q) = %q, want it kept", q, got)
		}
	}
}

// sameParsedTree reports whether a and b parse to the same tree: their printed forms, with the text of a
// label_replace or approx_topk call (kept as written) read as its parts.
func sameParsedTree(t testing.TB, a, b string) bool {
	t.Helper()
	ea, erra := Parse(a)
	eb, errb := Parse(b)
	if erra != nil || errb != nil {
		return erra != nil && errb != nil && erra.Error() == errb.Error()
	}
	return treeText(ea) == treeText(eb)
}

// treeText is the printed form of e, with every call kept as text printed from its parts (its blanks outside
// quoted strings removed when it has none).
func treeText(e Expr) string {
	switch v := e.(type) {
	case *VectorAggregation:
		g := ""
		if v.Grouping != nil {
			g = v.Grouping.String()
		}
		p := ""
		if v.HasParam {
			p = strconv.FormatFloat(v.Param, 'g', -1, 64) + ","
		}
		return string(v.Op) + " " + g + "(" + p + treeText(v.Inner) + ")"
	case *BinOpExpr:
		return "(" + treeText(v.Left) + " " + v.Op + " " + strconv.FormatBool(v.ReturnBool) + " " + v.VectorMatching.String() + " " + treeText(v.Right) + ")"
	case *OpaqueMetricExpr:
		if v.Typed && v.Inner != nil {
			return v.Func + "(" + treeText(v.Inner) + "," + strings.Join(v.Args, ",") + ")"
		}
		if v.Typed {
			return v.Func + "(" + strconv.FormatFloat(v.Value, 'g', -1, 64) + ")"
		}
		return v.Func + "(" + withoutBlanks(v.Raw) + ")"
	}
	return e.String()
}

// withoutBlanks is s without its blanks outside quoted strings.
func withoutBlanks(s string) string {
	var b strings.Builder
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' && i+1 < len(s) {
				b.WriteByte(c)
				i++
				c = s[i]
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// metricCall names every call the parser reads as a range function, a vector aggregation or a metric call kept as
// text, and nothing else.
func TestMetricCallNamesTheParsersCalls(t *testing.T) {
	n := 0
	for name := range rangeOps {
		n++
		if !metricCall(name) {
			t.Errorf("range function %s is not a metric call", name)
		}
	}
	for name := range vectorOps {
		n++
		if !metricCall(name) {
			t.Errorf("vector operation %s is not a metric call", name)
		}
	}
	for name := range opaqueMetricFunctions {
		n++
		if !metricCall(name) {
			t.Errorf("%s is not a metric call", name)
		}
	}
	for _, name := range []string{"by", "without", "on", "ignoring", "bool", "and", "ip", "duration", "bytes", "json", "unwrap", "SUM"} {
		if metricCall(name) {
			t.Errorf("%s is a metric call", name)
		}
	}
	if n != 29 {
		t.Errorf("the parser reads %d calls, metricCall lists 29", n)
	}
}

// spellAll is CanonicalSpelling without its pre-check.
func spellAll(q string) string { return respell(q, true) }

// A query already in the spelling costs CanonicalSpelling one scan of its bytes and no allocation.
func TestCanonicalSpellingAllocatesNothingWhenKept(t *testing.T) {
	for _, q := range []string{
		`sum by (level) (count_over_time({service_name="api"} | json | level="error" [$__auto]))`,
		`topk(5, sum by (app) (rate({app=~".+"} |= "x" [5m]))) / on(app) group_left sum by (app) (rate({app=~".+"}[5m]))`,
		`{app="x"} | logfmt | line_format "{{.msg}}" |= "a (b)"`,
	} {
		if n := testing.AllocsPerRun(50, func() { _ = CanonicalSpelling(q) }); n != 0 {
			t.Errorf("CanonicalSpelling(%q) allocates %v times", q, n)
		}
	}
}

// CanonicalSpelling holds for any input: a respelled query reads as the query did (the same tree, or the same
// error), respelling it again changes nothing, and the pre-check never keeps a query the parse would respell.
//
// conformance: semantics/translator-reads-blank-before-range-call-as-text, semantics/translator-reads-parenthesized-metric-as-text, semantics/vector-call-blank-before-parenthesis-as-query
func FuzzCanonicalSpelling(f *testing.F) {
	for _, s := range []string{
		`rate ({app="x"}[5m])`,
		`(count_over_time({app="x"}[5m]))`,
		`vector ( 2 ) * sum by (app) (rate({env="p"}[5m]))`,
		`(vector (2) + sum by (app) (rate({env="p"}[5m]))) * 2`,
		`label_replace( sum by (job) ( sum_over_time( {app="uw"} | unwrap n [5m] ) ), "foo", "$1", "service", "(.*):.*" )`,
		`min(max (count(rate({app="uw"} [1m])) without (rack))) by (svc)`,
		`sum((rate({app="x"}[5m]) + 1))`,
		`x and(rate({a="b"}[5m]))`,
		`rate({a="b"}[5m]) and((rate({a="c"}[5m])))`,
		`rate({a="b"}[5m]) > bool((vector(1)))`,
		`rate({app="x"}[5m]) / on (app) (sum by (app) (rate({app="x"}[5m])))`,
		`sum by (a) ( rate({app="x"}[5m]) )`,
		`rate({app="x"} | json | a="1" and (b="2") [5m])`,
		"rate ({app=\"x\"}[5m]) # c",
		`topk (0, rate({app="x"}[5m]))`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		got := CanonicalSpelling(q)
		if all := spellAll(q); all != got {
			t.Fatalf("the pre-check keeps %q, which the parse respells to %q", q, all)
		}
		if got == q {
			return
		}
		if again := CanonicalSpelling(got); again != got {
			t.Fatalf("%q respelled to %q, then to %q", q, got, again)
		}
		if _, err := Parse(q); err != nil {
			t.Fatalf("%q does not parse (%v) and is respelled to %q", q, err, got)
		}
		if !sameParsedTree(t, q, got) {
			ea, _ := Parse(q)
			eb, _ := Parse(got)
			t.Fatalf("%q respelled to %q reads another tree:\n %s\n %s", q, got, treeText(ea), treeText(eb))
		}
		if ValidateLogQL(q) != ValidateLogQL(got) {
			t.Fatalf("%q respelled to %q validates otherwise: %q, %q", q, got, ValidateLogQL(q), ValidateLogQL(got))
		}
	})
}
