package logql

import "testing"

// conformance: semantics/parenthesized-log-range, semantics/line-filter-or-alternatives
func TestLogRangeAlternativesMatchLoki(t *testing.T) {
	for _, q := range lokiAcceptedLogRanges {
		if msg := ValidateLogQL(q); msg != "" {
			t.Errorf("%s: Loki accepts it, got %q", q, msg)
		}
	}
	for _, c := range lokiRejectedLogRanges {
		if msg := ValidateLogQL(c.query); msg != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.query, msg, c.want)
		}
	}
}

// conformance: semantics/parenthesized-log-range
func TestCanonicalizeLogRanges(t *testing.T) {
	cases := []struct{ in, want string }{
		{`rate(({app="x"} |= "err")[5m])`, `rate({app="x"} |= "err" [5m])`},
		{`count_over_time(({app="x"} | json)[1m] offset 1h)`, `count_over_time({app="x"} | json [1m] offset 1h)`},
		{`rate(({app="x"})[5m])`, `rate({app="x"} [5m])`},
		{`rate((({app="x"} |= "e")[5m]))`, `rate({app="x"} |= "e" [5m])`},
		{`rate(({app="x"}[5m] |= "e"))`, `rate({app="x"} |= "e" [5m])`},
		{`rate({app="x"}[5m] offset 5m | json)`, `rate({app="x"} | json [5m] offset 5m)`},
		{`sum_over_time(({app="x"} | json | unwrap v)[5m])`, `sum_over_time({app="x"} | json | unwrap v [5m])`},
		{`sum_over_time(({app="x"})[5m] | unwrap v)`, `sum_over_time({app="x"} | unwrap v [5m])`},
		{`sum_over_time({app="x"}[5m] | json | unwrap v | v > 5)`, `sum_over_time({app="x"} | json | unwrap v | v > 5 [5m])`},
		{`quantile_over_time(0.5, ({app="x"} | json | unwrap v)[5m]) by (a)`, `quantile_over_time(0.5, {app="x"} | json | unwrap v [5m]) by (a)`},
		{`sum by (a) (rate(({app="x"} |= "e")[5m])) / sum by (a) (rate(({app="x"})[5m]))`, `sum by (a) (rate({app="x"} |= "e" [5m])) / sum by (a) (rate({app="x"} [5m]))`},
		{`({app="x"} |= "a")`, `{app="x"} |= "a"`},
		{`(({app="x"}))`, `{app="x"}`},
		{`rate(({app="x"} |= "(")[5m])`, `rate({app="x"} |= "(" [5m])`},
		// The query text is kept as written: numbers, durations and spacing inside strings.
		{`count_over_time(({app="x"} | json | x != 1e3 |= "a  b")[5m])`, `count_over_time({app="x"} | json | x != 1e3 |= "a  b" [5m])`},
		{`count_over_time(({app="x"} | json | d > 1m30s)[5m])`, `count_over_time({app="x"} | json | d > 1m30s [5m])`},
		// Any white space around the offset still takes the pipeline after the range along.
		{"rate({app=\"x\"}[5m]\noffset 1h | json)", "rate({app=\"x\"} | json [5m]\noffset 1h)"},
		{"rate({app=\"x\"}[5m] offset\t1h\n| json)", "rate({app=\"x\"} | json [5m] offset\t1h)"},
		// The first argument of label_replace / label_join is a metric expression.
		{`label_replace(rate(({app="x"} |= "x, (")[1m]), "a", "$1", "b", "(.*)")`, `label_replace(rate({app="x"} |= "x, (" [1m]), "a", "$1", "b", "(.*)")`},
		{`label_replace(sum by (a) (rate(({app="x"} |= "x")[1m])), "a", "b", "c", "d") > 1`, `label_replace(sum by (a) (rate({app="x"} |= "x" [1m])), "a", "b", "c", "d") > 1`},
		// Canonical forms and strings that merely look like the alternative ones stay as they are.
		{`rate({app="x"}[5m])`, `rate({app="x"}[5m])`},
		{`rate({app="x"} |= "a" [5m]) / rate({app="y"}[5m])`, `rate({app="x"} |= "a" [5m]) / rate({app="y"}[5m])`},
		{`{app="x"} |= "(" | json`, `{app="x"} |= "(" | json`},
		{`sum(rate({app="x"} |= "]" [5m]))`, `sum(rate({app="x"} |= "]" [5m]))`},
		{`(rate({app="x"}[5m]))`, `(rate({app="x"}[5m]))`},
		// Not valid LogQL: returned untouched for the validator to report.
		{`rate((({app="x"} |= "e"))[5m])`, `rate((({app="x"} |= "e"))[5m])`},
	}
	for _, c := range cases {
		got := CanonicalizeLogRanges(c.in)
		if got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
			continue
		}
		if again := CanonicalizeLogRanges(got); again != got {
			t.Errorf("%s: not idempotent: %s", c.in, again)
		}
		if ValidateLogQL(c.in) == "" && ValidateLogQL(got) != "" {
			t.Errorf("%s: canonical form %s is rejected: %s", c.in, got, ValidateLogQL(got))
		}
	}
}

// conformance: semantics/parenthesized-log-range
func TestParenthesizedLogRangeAST(t *testing.T) {
	for _, q := range []string{
		`rate(({app="x"} | json)[5m] offset 1h)`,
		`rate({app="x"}[5m] offset 1h | json)`,
		`rate((({app="x"} | json)[5m] offset 1h))`,
	} {
		expr, err := Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		ra, ok := expr.(*RangeAggregation)
		if !ok || ra.Range != "5m" || ra.Offset != "1h" || len(ra.Inner.(*LogQuery).Pipeline) != 1 {
			t.Errorf("%s: parsed as %#v", q, expr)
		}
	}
}

// conformance: semantics/line-filter-or-alternatives
func TestLineFilterOrAlternativesAST(t *testing.T) {
	lq, err := ParseLogQuery(`{app="x"} != "a" or "b" or ip("1.2.3.4") |~ "c" or "d" |> "<_>e" or "<_>f"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(lq.Pipeline) != 3 {
		t.Fatalf("stages = %d, want 3", len(lq.Pipeline))
	}
	neg := lq.Pipeline[0].(*LineFilterStage)
	if neg.Op != LineFilterExcludes || neg.Value != "a" || len(neg.Or) != 2 || neg.Or[1] != (LineFilterAlt{Value: "1.2.3.4", IP: true}) {
		t.Errorf("negated chain parsed as %#v", neg)
	}
	if got, want := neg.String(), `!= "a" or "b" or ip("1.2.3.4")`; got != want {
		t.Errorf("String = %s, want %s", got, want)
	}
	if got, want := neg.flatString(), `!= "a" != "b" != ip("1.2.3.4")`; got != want {
		t.Errorf("flatString = %s, want %s", got, want)
	}
}

// conformance: semantics/line-filter-or-alternatives
func TestLineFilterOrChainsFollowLokiGrammar(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []LineFilterAlt
	}{
		// An ip(...) alternative ends the orFilter; the next `or` re-attaches to the head.
		{`{a="x"} |= "a" or ip("1.2.3.4") or "b"`, []LineFilterAlt{{Value: "b"}}},
		{`{a="x"} |= "a" or "b" or ip("1.2.3.4") or "c" or "d"`, []LineFilterAlt{{Value: "c"}, {Value: "d"}}},
		{`{a="x"} |= "a" or "b" or ip("1.2.3.4")`, []LineFilterAlt{{Value: "b"}, {Value: "1.2.3.4", IP: true}}},
		// A negated chain is a conjunction and keeps every alternative.
		{`{a="x"} != "a" or ip("1.2.3.4") or "b"`, []LineFilterAlt{{Value: "1.2.3.4", IP: true}, {Value: "b"}}},
		// Keywords are case-insensitive in Loki's lexer; any white space may follow `or`.
		{`{a="x"} |= "a" OR "b"`, []LineFilterAlt{{Value: "b"}}},
		{"{a=\"x\"} |= \"a\" or\n\"b\"", []LineFilterAlt{{Value: "b"}}},
		{"{a=\"x\"} |= \"a\" or ip(`1.2.3.4`)", []LineFilterAlt{{Value: "1.2.3.4", IP: true}}},
	} {
		lq, err := ParseLogQuery(tc.query)
		if err != nil {
			t.Errorf("%q: %v", tc.query, err)
			continue
		}
		got := lq.Pipeline[0].(*LineFilterStage).Or
		if len(got) != len(tc.want) {
			t.Errorf("%q: alternatives %v, want %v", tc.query, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%q: alternatives %v, want %v", tc.query, got, tc.want)
			}
		}
	}
}

// Loki reports the line and the character column of the offending token.
// conformance: semantics/parenthesized-log-range
func TestSyntaxErrorPosition(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{"sum(\n rate((" + `{a="x"}|json)[5m] | json)` + "\n)", "parse error at line 2, col 26: syntax error: unexpected |, expecting )"},
		{`rate(({a="żółć"} |= "e")[5m]) foo`, "parse error at line 1, col 31: syntax error: unexpected IDENTIFIER"},
		{"{a=\"x\"} |= \"a\"\n  |= |= \"b\"", "parse error at line 0, col 6: syntax error: unexpected |=, expecting STRING or ip"},
	} {
		if got := ValidateLogQL(tc.query); got != tc.want {
			t.Errorf("%q:\n got %q\nwant %q", tc.query, got, tc.want)
		}
	}
}
