package logql

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// A zero range in every place a range stands, with Loki's answers (the grammar sweep's recordings,
// internal/querycorpus/testdata/grammar-sweep-loki.jsonl.gz, Loki 3.7.9): a bare 0 is a duration of nothing, as are
// 0s and 00m, and the range holds no time; 00 and any other number without a unit are rejected; an offset of 0s is
// accepted and a bare 0 there is a number where Loki expects a duration.
//
// conformance: semantics/range-duration-grammar-wider-than-loki, semantics/zero-range-metric
func TestZeroRangeLikeLoki(t *testing.T) {
	cases := []struct {
		query, lokiErr, rangeText string
	}{
		{`rate({a="b"}[0])`, "", "0"},
		{`rate({a="b"}[0] offset 5m)`, "", "0"},
		{`rate({a="b"}[0s])`, "", "0s"},
		{`rate({a="b"}[0s] offset 0s)`, "", "0s"},
		{`rate({a="b"}[00m] offset 0s)`, "", "00m"},
		{`rate({a="b"} | line_format "x"[0])`, "", "0"},
		{`rate_counter({a="b"} | unwrap c[0])`, "", "0"},
		{`rate_counter({a="b"} | json | unwrap c[0] offset 0s)`, "", "0"},
		{`rate_counter({a="b"} | unwrap bytes(c) | c>1[0])`, "", "0"},
		{`max_over_time({a="b"} | json | unwrap c[0])`, "", "0"},
		{`rate({a="b"}[5m] offset 0s)`, "", "5m"},
		{`rate({a="b"}[00])`, `parse error at line 0, col 13: not a valid duration string: "00"`, ""},
		{`rate({a="b"}[5m] offset 0)`, "parse error at line 1, col 25: syntax error: unexpected NUMBER, expecting DURATION", ""},
		{`rate({a="b"}[1w] offset 0)`, "parse error at line 1, col 25: syntax error: unexpected NUMBER, expecting DURATION", ""},
		{`rate_counter({a="b"} | json | unwrap c[5m] offset 0)`, "parse error at line 1, col 51: syntax error: unexpected NUMBER, expecting DURATION", ""},
		{`rate({a="b"}[] offset 0s)`, "parse error at line 0, col 13: empty duration string", ""},
	}
	for _, c := range cases {
		expr, msg := ValidateLogQLExpr(c.query)
		if msg != c.lokiErr {
			t.Errorf("%q: %q, Loki %q", c.query, msg, c.lokiErr)
			continue
		}
		if msg != "" {
			continue
		}
		ra, ok := expr.(*RangeAggregation)
		if !ok {
			t.Fatalf("%q: %T, want a range aggregation", c.query, expr)
		}
		if ra.Range != c.rangeText || (strings.Trim(c.rangeText, "0ms") == "" && ra.RangeDuration != 0) {
			t.Errorf("%q: range %q (%v), want %q", c.query, ra.Range, ra.RangeDuration, c.rangeText)
		}
		// The tree prints a query that reads back to the same range.
		back, err := Parse(expr.String())
		if err != nil || back.(*RangeAggregation).RangeDuration != ra.RangeDuration {
			t.Errorf("%q prints %q, which reads back as %v, %v", c.query, expr.String(), back, err)
		}
	}
}

// A comment between a vector aggregation's name and its parenthesis or `by (` grouping makes the name an identifier
// to Loki's lexer, which looks ahead over white space only: Loki answers a syntax error at the name. The proxy keeps
// the comments of such a query, so the validation reads what Loki reads; a comment anywhere else is removed. Loki's
// answers are the grammar sweep's recordings (Loki 3.7.9).
//
// conformance: semantics/comment-between-function-and-parenthesis-accepted
func TestCommentBetweenVectorAggregationAndCallLikeLoki(t *testing.T) {
	const col1 = "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"
	cases := []struct {
		query, lokiErr string
		kept           bool // StripComments returns the query as it is
	}{
		{"sum # c\n(rate({a=\"b\"}[5m])) by (a)", col1, true},
		{"sum#c\n(rate({a=\"b\"}[5m])) by (a)", col1, true},
		{"sum # c\nby (a) (rate({a=\"b\"}[5m]))", col1, true},
		{"sum#c\nby (a) (rate({a=\"b\"}[5m]))", col1, true},
		{"sum by # c\n(a) (rate({a=\"b\"}[5m]))", col1, true},
		{"sum by#c\n(a) (rate({a=\"b\"}[5m]))", col1, true},
		{"sum # c\nby # c\n(a) # c\n(rate({a=\"b\"}[5m]))", col1, true},
		{"sum#c\n(rate({a=\"b\"}[5m]))#c\nby#c\n(a)", col1, true},
		{"topk(2, sum # c\nby (a) (rate({a=\"b\"}[5m])))", "parse error at line 1, col 9: syntax error: unexpected IDENTIFIER", true},
		{"topk(2, sum by#c\n(a) (rate({a=\"b\"}[5m])))", "parse error at line 1, col 9: syntax error: unexpected IDENTIFIER", true},
		{"topk(2, # c\nsum # c\nby # c\n(a) # c\n(rate({a=\"b\"}[5m])))", "parse error at line 2, col 1: syntax error: unexpected IDENTIFIER", true},
		{"topk(2,#c\nsum#c\nby#c\n(a)#c\n(rate({a=\"b\"}[5m])))", "parse error at line 2, col 1: syntax error: unexpected IDENTIFIER", true},
		// the grouping after the call, and a comment past the grouping's parenthesis, are no part of the lookahead
		{"sum (rate({a=\"b\"}[5m])) by # c\n(a)", "", false},
		{"sum (rate({a=\"b\"}[5m])) by#c\n(a)", "", false},
		{"quantile_over_time(0.5, {a=\"b\"} | unwrap c [5m]) by#c\n(d)", "", false},
		{"sum by (a) (rate({a=\"b\"}[5m]))\n# c", "", false},
		{"rate({a=\"b\"}[5m]) / on # c\n(a) group_left (b) rate({a=\"c\"}[5m] offset 1h)", "", false},
		{"sum(rate({a=\"x\"}[5m])) # by (a)\n by (b)", "", false},
	}
	for _, c := range cases {
		stripped := StripComments(c.query)
		if (stripped == c.query) != c.kept {
			t.Errorf("%q: StripComments %q, want the query kept=%v", c.query, stripped, c.kept)
		}
		if msg := ValidateLogQL(stripped); msg != c.lokiErr {
			t.Errorf("%q: %q, Loki %q", c.query, msg, c.lokiErr)
		}
		if msg := ValidateLogQL(c.query); msg != c.lokiErr {
			t.Errorf("%q unstripped: %q, Loki %q", c.query, msg, c.lokiErr)
		}
	}
	// Upper case keeps the comment too and stays an identifier.
	if q := "SUM # c\n(rate({a=\"b\"}[5m]))"; StripComments(q) != q || ValidateLogQL(LowercaseKeywords(StripComments(q))) != col1 {
		t.Errorf("%q: %q, %q", q, StripComments(q), ValidateLogQL(LowercaseKeywords(StripComments(q))))
	}
}

const commentFormsLokiAnswers = "testdata/comment-forms-loki.jsonl"

// commentFormsGrid holds a comment between every function name and its parenthesis, between a vector aggregation
// and its grouping keyword and parenthesis, the subquery-like ranges and the negative zero offsets, and their
// neighbours that Loki accepts.
func commentFormsGrid() []string {
	return []string{
		// range aggregations
		"rate # c\n({a=\"b\"}[5m])",
		"rate#c\n({a=\"b\"}[5m])",
		"count_over_time#c\n({a=\"b\"}[5m])",
		"count_over_time # c\n({a=\"b\"}[5m])",
		"bytes_over_time # c\n({a=\"b\"}[5m])",
		"absent_over_time # c\n({a=\"b\"}[5m])",
		"rate_counter # c\n({a=\"b\"} | unwrap c [5m])",
		"quantile_over_time # c\n(0.5, {a=\"b\"} | unwrap c [5m])",
		"quantile_over_time#c\n(0.5, {a=\"b\"} | unwrap c [5m]) by (a)",
		"sum(rate # c\n({a=\"b\"}[5m]))",
		"rate # c\nby (a) ({a=\"b\"}[5m])",
		"rate by # c\n(a) ({a=\"b\"}[5m])",
		"RATE # c\n({a=\"b\"}[5m])",
		"Count_Over_Time#c\n({a=\"b\"}[5m])",
		// vector and label_replace
		"vector # c\n(1)",
		"vector#c\n(1)",
		"Vector # c\n(1)",
		"LABEL_REPLACE # c\n(rate({a=\"b\"}[5m]), \"d\", \"$1\", \"a\", \"(.*)\")",
		"sum(vector # c\n(1))",
		"sum(rate({a=\"b\"}[5m])) + vector # c\n(1)",
		"label_replace # c\n(rate({a=\"b\"}[5m]), \"d\", \"$1\", \"a\", \"(.*)\")",
		"label_replace#c\n(rate({a=\"b\"}[5m]), \"d\", \"$1\", \"a\", \"(.*)\")",
		"label_replace # c\n(vector(1), \"d\", \"$1\", \"a\", \"(.*)\") or vector(2)",
		// ip, as a line filter and a label filter
		"{a=\"b\"} |= ip # c\n(\"1.2.3.4\")",
		"{a=\"b\"} |= ip#c\n(\"1.2.3.4\")",
		"{a=\"b\"} |= IP # c\n(\"1.2.3.4\")",
		"{a=\"b\"} | x = Ip # c\n(\"1.2.3.4\")",
		"{a=\"b\"} != ip # c\n(\"1.2.3.4\")",
		"{a=\"b\"} |= \"x\" or ip # c\n(\"1.2.3.4\")",
		"{a=\"b\"} | x = ip # c\n(\"1.2.3.4\")",
		"{a=\"b\"} | x != ip # c\n(\"1.2.3.4\")",
		"{a=\"b\"} | x = ip # c\n(\"1.2.3.4\") or y = ip(\"::1\")",
		"count_over_time({a=\"b\"} |= ip # c\n(\"1.2.3.4\") [5m])",
		// the unwrap conversions: the name becomes the label, and the parenthesis is the unexpected token
		"sum_over_time({a=\"b\"} | unwrap bytes # c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap duration # c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap duration_seconds # c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap bytes#c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap BYTES # c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap Duration_Seconds # c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap BYTES(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap bytes # c\n # d\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap bytes # c\n(c) | d=\"e\" [5m])",
		"sum_over_time({a=\"b\"} | unwrap bytes # c\n(c) [5m]) by (a)",
		"sum by (a) (sum_over_time({a=\"b\"} | unwrap duration#c\n(c) [5m]))",
		"sum_over_time({a=\"b\"} | json | unwrap bytes # c\n(c) [5m])",
		"sum_over_time({a=\"b\"} | unwrap bytes # c\n [5m])",
		"sum_over_time({a=\"b\"} | unwrap bytes(c) [5m])",
		"{a=\"b\"} | unwrap bytes # c\n(c)",
		// vector aggregations not recorded before
		"sum without # c\n(a) (rate({a=\"b\"}[5m]))",
		"sum without#c\n(a) (rate({a=\"b\"}[5m]))",
		"sum # c\nwithout (a) (rate({a=\"b\"}[5m]))",
		"avg # c\n(rate({a=\"b\"}[5m]))",
		"avg#c\n(rate({a=\"b\"}[5m]))",
		"count # c\n(rate({a=\"b\"}[5m]))",
		"topk # c\n(2, rate({a=\"b\"}[5m]))",
		"sort # c\n(rate({a=\"b\"}[5m]))",
		"AVG # c\n(rate({a=\"b\"}[5m]))",
		// a name that is no function's: the comment is removed, and Loki's answer is the same
		"{a=\"b\"} | json # c\n(a)",
		"sum by (json # c\n) (rate({a=\"b\"}[5m]))",
		// comments where Loki's lexer does not look
		"{a=\"b\"} |= ip(\"1.2.3.4\") # c\n",
		"rate({a=\"b\"}[5m]) # c\n",
		"rate( # c\n{a=\"b\"}[5m])",
		"rate\n({a=\"b\"}[5m])",
		"sum\t(rate({a=\"b\"}[5m]))",
		// subquery-like ranges and negative zero offsets
		"rate({a=\"b\"}[0:1m])",
		"rate({a=\"b\"}[5m:1m])",
		"rate({a=\"b\"}[0s:1m])",
		"rate({a=\"b\"}[5m] offset -0s)",
		"rate({a=\"b\"}[5m] offset -0)",
		"rate({a=\"b\"}[5m] offset -5m)",
		"rate({a=\"b\"}[5m] offset 0s)",
	}
}

// A comment between a function's name and its parenthesis (a range aggregation, vector, label_replace, ip, an unwrap
// conversion) makes the name an identifier to Loki's lexer, as for the vector aggregations: the proxy keeps the
// comments of such a query and validation answers Loki's text, read where Loki reads it (an unwrap conversion's name
// becomes the label, and the parenthesis is the unexpected token). A comment anywhere else is removed and the query
// answered as Loki answers it; the subquery-like ranges get Loki's duration error, and the negative zero offsets are
// accepted. Loki's answers are recorded with the Loki oracle (test/oracle/loki check; LVP_RUN_ORACLES=1 records them
// again); the HTTP answers of the same forms are TestCommentFormsAnswerLikeLoki's (internal/proxy).
//
// conformance: semantics/comment-between-function-and-parenthesis-accepted
func TestCommentBetweenFunctionAndCallLikeLoki(t *testing.T) {
	grid := commentFormsGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordOracleAnswers(t, "check", grid, commentFormsLokiAnswers)
	}
	b, err := os.ReadFile(commentFormsLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != len(grid) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(lines), len(grid))
	}
	rejected, kept := 0, 0
	for i, q := range grid {
		var loki patternFilterAnswer
		if err := json.Unmarshal([]byte(lines[i]), &loki); err != nil {
			t.Fatal(err)
		}
		if !loki.Accept {
			rejected++
		}
		stripped := StripComments(q)
		if stripped == q && commentKeptForName(q) {
			kept++
			if loki.Accept {
				t.Errorf("%q: Loki accepts it, and StripComments keeps its comments", q)
			}
		}
		// What the handlers validate (lowercaseQueryKeywords), and the text as it is.
		if msg := ValidateLogQL(LowercaseKeywords(StripParserFlags(stripped))); msg != loki.Error {
			t.Errorf("%q:\n proxy %q\n Loki  %q", q, msg, loki.Error)
		}
		if msg := ValidateLogQL(q); msg != loki.Error {
			t.Errorf("%q unstripped:\n proxy %q\n Loki  %q", q, msg, loki.Error)
		}
	}
	if rejected == 0 || rejected == len(grid) || kept == 0 {
		t.Fatalf("%d of %d recorded answers are rejections, %d kept: the grid lost its neighbours", rejected, len(grid), kept)
	}
}

func TestSkipSpaceAndComments(t *testing.T) {
	for in, want := range map[string]int{
		"": 0, "(": 0, " \t\r\n(": 4, "# c\n(": 4, "#c": 2, " # a\n # b\n (": 11, "#\n#\n": 4, "x # c\n": 0,
	} {
		if got := skipSpaceAndComments(in, 0); got != want {
			t.Errorf("skipSpaceAndComments(%q) = %d, want %d", in, got, want)
		}
	}
}

// commentMakesName finds the function name before a comment, or the vector aggregation name before the grouping
// keyword before it, and only a whole name: the end of a dotted or a longer name is none.
func TestCommentMakesName(t *testing.T) {
	for q, want := range map[string]bool{
		"sum # c\n(":                true,
		"SUM\t#c\n(":                true,
		"sum by # c\n(a) (":         true,
		"sum WITHOUT#c\n(a) (":      true,
		"topk # a\n# b\n(2, x)":     true,
		"sum # c\nby (a) (":         true,
		"sum # c\n[":                false,
		"sum(x) by # c\n(a)":        false,
		"rate # c\n(":               true,
		"vector#c\n(":               true,
		"x = ip # c\n(":             true,
		"unwrap bytes # c\n(":       true,
		"rate by # c\n(a) (":        false,
		"json # c\n(":               false,
		"x.sum # c\n(":              false,
		"ésum # c\n(":               false,
		"my_sum # c\n(":             false,
		"# c\n(":                    false,
		"sum # c":                   false,
		"sum by # c\n a (":          false,
		"sum by (a) # c\n(rate(x))": false,
	} {
		if got := commentMakesName(q, strings.IndexByte(q, '#')); got != want {
			t.Errorf("commentMakesName(%q) = %v, want %v", q, got, want)
		}
	}
}
