package logql

import "testing"

// stageNameFollowerAnswers are Loki's answers, recorded with test/oracle/loki check, for white space around numbers,
// dots and names inside label filters and after keep and drop: a stage that opens with a name Loki has no stage for
// is a label filter, so the token after the name must be a comparison operator. The proxy passed such a stage to the
// backend as text and printed it without the white space of the source (`| a .5` as `| a.5`), so a rewrite of the
// query (the keep series grouping) changed its tokens. An empty answer is a query Loki accepts.
var stageNameFollowerAnswers = []struct{ query, loki string }{
	{`{a="b"} | foo on`, "parse error at line 1, col 15: syntax error: unexpected on"},
	{`{a="b"} | foo by`, "parse error at line 1, col 15: syntax error: unexpected by"},
	{`{a="b"} | foo without`, "parse error at line 1, col 15: syntax error: unexpected without"},
	{`{a="b"} | foo offset`, "parse error at line 1, col 15: syntax error: unexpected offset"},
	{`{a="b"} | foo bool`, "parse error at line 1, col 15: syntax error: unexpected bool"},
	{`{a="b"} | foo ignoring`, "parse error at line 1, col 15: syntax error: unexpected ignoring"},
	{`{a="b"} | foo group_left`, "parse error at line 1, col 15: syntax error: unexpected group_left"},
	{`{a="b"} | foo and`, "parse error at line 1, col 15: syntax error: unexpected and"},
	{`{a="b"} | foo or`, "parse error at line 1, col 15: syntax error: unexpected or"},
	{`{a="b"} | foo unless`, "parse error at line 1, col 15: syntax error: unexpected unless"},
	{`{a="b"} | foo "s"`, "parse error at line 1, col 15: syntax error: unexpected STRING"},
	{"{a=\"b\"} | foo `r`", "parse error at line 1, col 15: syntax error: unexpected STRING"},
	{`{a="b"} | foo 5`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{a="b"} | foo .5`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{a="b"} | foo 5m`, "parse error at line 0, col 15: syntax error: unexpected DURATION"},
	{`{a="b"} | foo 5KB`, "parse error at line 0, col 15: syntax error: unexpected BYTES"},
	{`{a="b"} | foo ,`, "parse error at line 1, col 15: syntax error: unexpected ,"},
	{`{a="b"} | foo (`, "parse error at line 1, col 15: syntax error: unexpected ("},
	{`{a="b"} | foo {`, "parse error at line 1, col 15: syntax error: unexpected {"},
	{`{a="b"} | foo +`, "parse error at line 1, col 15: syntax error: unexpected +"},
	{`{a="b"} | foo -`, "parse error at line 1, col 15: syntax error: unexpected -"},
	{`{a="b"} | foo *`, "parse error at line 1, col 15: syntax error: unexpected *"},
	{`{a="b"} | foo =`, "parse error at line 1, col 16: syntax error: unexpected $end"},
	{`{a="b"} | foo :b`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{a="b"} | foo .5x`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{a="b"} | foo json`, "parse error at line 1, col 15: syntax error: unexpected json"},
	{`{a="b"} | foo foo`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{a="b"} | foo !`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{a="b"} | foo ==`, "parse error at line 1, col 17: syntax error: unexpected $end"},
	{`{a="b"} | foo x+y=1`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{a="b"} | x+y=1`, "parse error at line 1, col 12: syntax error: unexpected +"},
	{`{a="b"} | a :b`, "parse error at line 1, col 13: syntax error: unexpected IDENTIFIER"},
	{`{a="b"} | a:b`, "parse error at line 1, col 12: syntax error: unexpected IDENTIFIER"},
	{`{a="b"} | a .5x`, "parse error at line 1, col 13: syntax error: unexpected NUMBER"},
	{`{a="b"} | foo, bar`, "parse error at line 1, col 14: syntax error: unexpected ,"},
	{`{a="b"} | keep a | a, b`, "parse error at line 1, col 21: syntax error: unexpected ,"},
	{`rate({app="x"} | keep a | a, b [1m])`, "parse error at line 1, col 28: syntax error: unexpected ,"},
	{`rate({app="x"} | keep a | a .5 [1m])`, "parse error at line 1, col 29: syntax error: unexpected NUMBER"},
	{`{a="b"} | a > .5`, ""},
	{`{a="b"} | a = 1 or b < .5`, ""},
	{`{a="b"} | a>.5`, ""},
	{`{a="b"} | keep a .5`, "parse error at line 1, col 18: syntax error: unexpected NUMBER"},
	{`{a="b"} | drop a .5`, "parse error at line 1, col 18: syntax error: unexpected NUMBER"},
	{`{a="b"} | keep a, b .c`, "parse error at line 1, col 21: syntax error: unexpected ."},
	{`{a="b"} | drop a , b`, ""},
	{`{a="b"} | a > 1 and b < .5s`, ""},
	{`{a="b"} | a > 1.5KB`, ""},
	{`{a="b"} | a >= .5 , b <= 1`, ""},
	{`{a="b"} | ( a > .5 )`, ""},
	{`{a="b"} | foo )`, "parse error at line 1, col 15: syntax error: unexpected )"},
	{"{a=\"b\"} | foo\n.5", "parse error at line 2, col 1: syntax error: unexpected NUMBER"},
	{`{app="x"} | a > .5`, ""},
	{`{app="x"} | a>.5`, ""},
	{`{app="x"} | a > 0.5`, ""},
	{`{app="x"} | a > 1 .5`, "parse error at line 1, col 19: syntax error: unexpected NUMBER"},
	{`{app="x"} | a > 1. 5`, "parse error at line 1, col 20: syntax error: unexpected NUMBER"},
	{`{app="x"} | a > -.5`, ""},
	{`{app="x"} | a > - .5`, ""},
	{`{app="x"} | a >= .5 , b <= 1`, ""},
	{`{app="x"} | a = 1 or b < .5`, ""},
	{`{app="x"} | a > 1 and b < .5s`, ""},
	{`{app="x"} | a > .5KB`, ""},
	{`{app="x"} | a > 1e3`, ""},
	{`{app="x"} | a == .0000`, ""},
	{`{app="x"} | a .5`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{app="x"} | a .0000`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{app="x"} | a 5`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{app="x"} | a .5 > 1`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{app="x"} | a .0000!`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{app="x"} | a .0000!= 1`, "parse error at line 1, col 15: syntax error: unexpected NUMBER"},
	{`{app="x"} | a b`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{app="x"} | a b = "v"`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{app="x"} | a, b`, "parse error at line 1, col 14: syntax error: unexpected ,"},
	{`{app="x"} | a , b`, "parse error at line 1, col 15: syntax error: unexpected ,"},
	{`{app="x"} | a,b`, "parse error at line 1, col 14: syntax error: unexpected ,"},
	{`{app="x"} | a = "v", b`, "parse error at line 1, col 23: syntax error: unexpected $end"},
	{`{app="x"} | a = "v" , b = "w"`, ""},
	{`{app="x"} | a :b`, "parse error at line 1, col 15: syntax error: unexpected IDENTIFIER"},
	{`{app="x"} | a "v"`, "parse error at line 1, col 15: syntax error: unexpected STRING"},
	{"{app=\"x\"} | a `v`", "parse error at line 1, col 15: syntax error: unexpected STRING"},
	{`{app="x"} | keep a`, ""},
	{`{app="x"} | keep a, b`, ""},
	{`{app="x"} | keep a , b`, ""},
	{`{app="x"} | keep a,b`, ""},
	{`{app="x"} | keep  a,  b`, ""},
	{`{app="x"} | keep a .5`, "parse error at line 1, col 20: syntax error: unexpected NUMBER"},
	{`{app="x"} | keep a .b`, "parse error at line 1, col 20: syntax error: unexpected ."},
	{`{app="x"} | keep a, b .c`, "parse error at line 1, col 23: syntax error: unexpected ."},
	{`{app="x"} | drop a .5`, "parse error at line 1, col 20: syntax error: unexpected NUMBER"},
	{`{app="x"} | drop a .b`, "parse error at line 1, col 20: syntax error: unexpected ."},
	{`{app="x"} | drop a , b`, ""},
	{`{app="x"} | keep a="v" , b`, ""},
	{`{app="x"} | drop a = "v", b`, ""},
	{`{app="x"} | keep a b`, "parse error at line 1, col 20: syntax error: unexpected IDENTIFIER"},
	{`{app="x"} | drop a 5`, "parse error at line 1, col 20: syntax error: unexpected NUMBER"},
	{`{app="x"} | keep a | a, b`, "parse error at line 1, col 23: syntax error: unexpected ,"},
	{`{app="x"} | keep a | a .5`, "parse error at line 1, col 24: syntax error: unexpected NUMBER"},
	{`{app="x"} | keep a | a > .5`, ""},
	{`{app="x"} | drop a | a, b`, "parse error at line 1, col 23: syntax error: unexpected ,"},
	{`rate({app="x"} | keep a | a > .5 [1m])`, ""},
	{`rate({app="x"} | keep a, b | a > .5 or b < 1 [1m])`, ""},
	{`sum by (a) (rate({app="x"} | keep a | a > .5 [1m]))`, ""},
	{`bytes_rate({app="x"} | drop b | keep a | a >= .5 [5m])`, ""},
	{`rate({A="x"}|keep  A, A |A, A | A .0000![0y])`, "parse error at line 1, col 27: syntax error: unexpected ,"},
	{`rAte({A=""}|keep  A, A |A, A | A .0000![0y])   `, "parse error at line 1, col 26: syntax error: unexpected ,"},
}

// spacedDottedStages are rejected by Loki (`unexpected .`) and kept by the proxy: a dot after a stage name goes on as
// a dotted name, also spaced (`| custom . x = "v"`, which the translation joins into one name). The printed stage
// keeps the space before the dot, so it reads back as the same tokens.
var spacedDottedStages = []struct{ query, printed string }{
	{`{app="x"} | a .b`, `{app="x"} | a .b`},
	{`{app="x"} | a . b`, `{app="x"} | a . b`},
	{`{app="x"} | a .b = "v"`, `{app="x"} | a .b="v"`},
	{`{app="x"} | a .b, c`, `{app="x"} | a .b, c`},
	{`{app="x"} | a .b .5`, `{app="x"} | a .b .5`},
	{"{app=\"x\"} | custom . `p.q.` = `v`", "{app=\"x\"} | custom .`p.q.`=`v`"},
}

// A name that opens a stage and is followed by more of the stage is rejected with Loki's status and text at the token
// after the name; what Loki accepts prints a text that parses to the same text again.
//
// conformance: semantics/stage-name-followed-by-more-of-stage
func TestStageNameFollowersMatchLoki(t *testing.T) {
	for _, tc := range stageNameFollowerAnswers {
		got := ValidateLogQL(tc.query)
		if got != tc.loki {
			t.Errorf("%q\n  proxy: %q\n  Loki:  %q", tc.query, got, tc.loki)
			continue
		}
		if tc.loki != "" {
			continue
		}
		e, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		printed := e.String()
		again, err := Parse(printed)
		if err != nil || again.String() != printed || ValidateLogQL(printed) != "" {
			t.Errorf("%q printed as %q, which does not read back: %v", tc.query, printed, err)
		}
	}
	for _, tc := range spacedDottedStages {
		e, err := Parse(tc.query)
		if err != nil || ValidateLogQL(tc.query) != "" {
			t.Errorf("%q: %v; the proxy keeps a dotted name after a stage name", tc.query, err)
			continue
		}
		again, err := Parse(e.String())
		if e.String() != tc.printed || err != nil || again.String() != tc.printed {
			t.Errorf("%q printed as %q (want %q), read back %v %v", tc.query, e.String(), tc.printed, again, err)
		}
	}
}

// BenchmarkParseStageName parses the stages that open with a name: a bare name passed to the backend, a label filter,
// a filter after a keep, and a name followed by more of its stage (rejected, where the proxy accepted it before).
func BenchmarkParseStageName(b *testing.B) {
	for _, bc := range []struct{ name, query string }{
		{"bare_name", `{app="x"} | foo`},
		{"label_filter", `{app="x"} | json | a > .5 or b = "v"`},
		{"keep_filter_metric", `rate({app="x"} | keep a , b | a > - .5 or b < .5KB [1m])`},
		{"name_then_comma", `rate({app="x"} | keep a | a, b [1m])`},
		{"name_then_number", `rate({A="x"}|keep  A, A |A, A | A .0000![0y])`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ParseAndValidate(bc.query)
			}
		})
	}
}
