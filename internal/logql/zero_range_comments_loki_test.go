package logql

import (
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

// The other function names (range aggregations, label_replace, vector, ip, the unwrap conversions) have no recorded
// Loki answer with a comment before their parenthesis: the comment is still removed there, as before, until those
// answers are recorded (semantics/comment-between-function-and-parenthesis-accepted).
func TestCommentBeforeOtherCallsStillRemoved(t *testing.T) {
	for _, q := range []string{
		"rate # c\n({a=\"b\"}[5m])", "count_over_time#c\n({a=\"b\"}[5m])", "vector # c\n(1)",
		"label_replace # c\n(rate({a=\"b\"}[5m]), \"d\", \"$1\", \"a\", \"(.*)\")",
		"sum_over_time({a=\"b\"} | unwrap bytes # c\n(c) [5m])",
	} {
		if !commentBeforeCall(q) {
			t.Errorf("%q: no comment before a call found", q)
		}
		if got := StripComments(q); got == q {
			t.Errorf("%q: comments kept", q)
		}
	}
}

// commentBeforeCall reports a query with a comment between a function name (other than a vector aggregation's) and
// its parenthesis or grouping: the forms StripComments leaves as they were, whose Loki answers are not recorded.
func commentBeforeCall(q string) bool {
	s := &scanner{src: q, extra: &scanExtra{}}
	for {
		t := s.next()
		switch t.Typ {
		case TokEOF, TokError:
			return false
		case TokIdent:
			lower := strings.ToLower(t.Val)
			call := rangeAggNames[lower] || unwrapConversions[lower] || lower == "vector" || lower == "ip" || lower == "label_replace"
			if call && !parenFollows(q, s.pos) && parenAfter(q, s.pos, skipSpaceAndComments) {
				return true
			}
		}
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

// commentMakesName finds the vector aggregation name before a comment, or before the grouping keyword before it, and
// only a whole name: the end of a dotted or a longer name is none.
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
		"rate # c\n(":               false,
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
