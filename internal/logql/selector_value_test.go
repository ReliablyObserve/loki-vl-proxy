package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// A stream-selector matcher value is a string in Loki, and only a string: the
// ip("...") form belongs to line and label filters. Expected messages are the
// exact 400 bodies Loki 3.7.9 returned for the same queries on the e2e-compat
// stack.
// conformance: semantics/selector-values-are-strings
func TestSelectorMatcherValueMustBeString(t *testing.T) {
	cases := []struct{ query, want string }{
		// Accepted: a quoted string and a raw string, with every operator.
		{`{a="1"}`, ""},
		{"{a=`1`}", ""},
		{`{a=~"1.*"}`, ""},
		{`{a="x", b!~"y"}`, ""},
		// Line and label filters keep ip().
		{`{a="x"} |= ip("1.2.3.4")`, ""},
		{`{a="x"} != ip("1.2.3.0/24")`, ""},
		{`{a="x"} | addr=ip("1.2.3.4")`, ""},
		{`{a="x"} | addr!=ip("1.2.3.4")`, ""},

		// ip() as the value, in the first and a later position.
		{`{a=ip("1.2.3.4")}`, "parse error at line 1, col 4: syntax error: unexpected ip, expecting STRING"},
		{`{a="1", addr!=ip("1.1.1.1")}`, "parse error at line 1, col 15: syntax error: unexpected ip, expecting STRING"},
		{`{a=~ip("1.2.3.4")}`, "parse error at line 1, col 5: syntax error: unexpected ip, expecting STRING"},
		{`{a!~ip("1.2.3.4")}`, "parse error at line 1, col 5: syntax error: unexpected ip, expecting STRING"},
		{`{a=ip ("x")}`, "parse error at line 1, col 4: syntax error: unexpected ip, expecting STRING"},
		{`{a=IP("x")}`, "parse error at line 1, col 4: syntax error: unexpected ip, expecting STRING"},
		{`{a=ip(}`, "parse error at line 1, col 4: syntax error: unexpected ip, expecting STRING"},
		{`sum(count_over_time({a=ip("x")}[5m]))`, "parse error at line 1, col 24: syntax error: unexpected ip, expecting STRING"},

		// A bare identifier: a function name is a keyword token only before '('.
		{`{a=foo}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a="x",b=foo}`, "parse error at line 1, col 10: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=ip}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=rate}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=sum}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=label_replace}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=rate (}`, "parse error at line 1, col 4: syntax error: unexpected RATE, expecting STRING"},
		{`{a=json}`, "parse error at line 1, col 4: syntax error: unexpected json, expecting STRING"},
		{`{a=by}`, "parse error at line 1, col 4: syntax error: unexpected by, expecting STRING"},

		// A number and a duration.
		{`{a=1}`, "parse error at line 1, col 4: syntax error: unexpected NUMBER, expecting STRING"},
		{`{a!~1.5}`, "parse error at line 1, col 5: syntax error: unexpected NUMBER, expecting STRING"},
		{`{a="x", b=1}`, "parse error at line 1, col 11: syntax error: unexpected NUMBER, expecting STRING"},
		{`{a=1h}`, "parse error at line 0, col 4: syntax error: unexpected DURATION, expecting STRING"},

		// Punctuation and a missing value.
		{`{a=}`, "parse error at line 1, col 4: syntax error: unexpected }, expecting STRING"},
		{`{a=`, "parse error at line 1, col 4: syntax error: unexpected $end, expecting STRING"},
		{`{a=(}`, "parse error at line 1, col 4: syntax error: unexpected (, expecting STRING"},
		{`{a={}`, "parse error at line 1, col 4: syntax error: unexpected {, expecting STRING"},
		{`{a=-1}`, "parse error at line 1, col 4: syntax error: unexpected -, expecting STRING"},
		{`{a=|="x"}`, "parse error at line 0, col 4: syntax error: unexpected |=, expecting STRING"},
		{`{a=[5m]}`, "parse error at line 0, col 4: syntax error: unexpected RANGE, expecting STRING"},

		// A character that starts no token is an identifier to Loki's lexer; a bad string literal is its own error.
		{`{a=@}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=!}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=?}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a='x'}`, "parse error at line 1, col 4: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`{a=%}`, "parse error at line 1, col 4: syntax error: unexpected %, expecting STRING"},
		{`{a="x`, "parse error at line 1, col 4: literal not terminated"},
		{`{a="x\q"}`, "parse error at line 1, col 4: invalid char escape"},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := logql.ValidateLogQL(tc.query); got != tc.want {
				t.Fatalf("ValidateLogQL(%s) = %q, want %q", tc.query, got, tc.want)
			}
			if _, err := logql.Parse(tc.query); (err != nil) != (tc.want != "") {
				t.Fatalf("Parse(%s) error = %v, want error %v", tc.query, err, tc.want != "")
			}
		})
	}
}
