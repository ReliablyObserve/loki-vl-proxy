package logql

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// Loki's grammar takes only a metric expression inside a vector aggregation
// (pkg/logql/syntax/syntax.y:176-185). A log expression there is read as the
// left operand of a binary operation, so the parser fails at the closing
// parenthesis: Loki's own test pkg/logql/syntax/parser_test.go:689-690 has
// `stddev({ foo = "bar" })` -> "syntax error: unexpected )" at line 1, col 23.
// The columns of the first rows are the ones Loki v3.7.9 answered in the
// differential run of 2026-11-04.
// conformance: semantics/vector-aggregation-over-log-selector-status
func TestParse_VectorAggregationOverLogExpression(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`topk(5, {app="api-gateway"})`, "parse error at line 1, col 28: syntax error: unexpected )"},
		{`topk(2, {app="api-gateway",env="production"})`, "parse error at line 1, col 45: syntax error: unexpected )"},
		{`bottomk(1, {app="api-gateway",env="production"})`, "parse error at line 1, col 48: syntax error: unexpected )"},
		{`sort({app="api-gateway"})`, "parse error at line 1, col 25: syntax error: unexpected )"},
		{`sort({app="api-gateway",env="production"})`, "parse error at line 1, col 42: syntax error: unexpected )"},
		{`sort_desc({app="api-gateway"})`, "parse error at line 1, col 30: syntax error: unexpected )"},
		{`sort_desc({app="api-gateway",env="production"})`, "parse error at line 1, col 47: syntax error: unexpected )"},
		{`stddev({namespace="prod"})`, "parse error at line 1, col 26: syntax error: unexpected )"},
		{`stddev({ foo = "bar" })`, "parse error at line 1, col 23: syntax error: unexpected )"},
		// Every vector operator, with a grouping before or after, a pipeline,
		// a line filter, parentheses around the selector, a multi-byte value
		// and lines.
		{`sum({app="x"})`, "parse error at line 1, col 14: syntax error: unexpected )"},
		{`avg by (a) ({app="x"})`, "parse error at line 1, col 22: syntax error: unexpected )"},
		{`max({app="x"}) by (a)`, "parse error at line 1, col 14: syntax error: unexpected )"},
		{`min without (a) ({app="x"} | json)`, "parse error at line 1, col 34: syntax error: unexpected )"},
		{`count({app="x"} |= "boom" | logfmt | level="error")`, "parse error at line 1, col 51: syntax error: unexpected )"},
		{`stdvar(({app="x"}))`, "parse error at line 1, col 19: syntax error: unexpected )"},
		{`sum({app="žluťoučký"})`, "parse error at line 1, col 22: syntax error: unexpected )"},
		{"sum(\n  {app=\"x\"}\n)", "parse error at line 3, col 1: syntax error: unexpected )"},
	} {
		_, err := Parse(tc.query)
		if err == nil || err.Error() != tc.want {
			t.Errorf("%s: %v, want %q", tc.query, err, tc.want)
		}
	}
	// A metric expression inside stays valid.
	for _, q := range []string{
		`topk(5, rate({app="x"}[5m]))`,
		`sum(count_over_time({app="x"} | json [1m]))`,
		`sort(sum by (a) (rate({app="x"}[1m])))`,
		`sum(vector(1))`,
	} {
		if _, err := Parse(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// FuzzVectorAggregationOverLogExpression: a log query built from the fuzzed
// label name, values and stages (a selector, a parser, a line filter, a label
// filter, a line_format), put inside any vector aggregation, fails with
// Loki's error at the closing parenthesis.
func FuzzVectorAggregationOverLogExpression(f *testing.F) {
	f.Add("app", "api-gateway", "boom", uint8(0), uint8(0))
	f.Add("namespace", "prod", "ž\"x", uint8(3), uint8(9))
	f.Add("a", "", "{{.a}}", uint8(5), uint8(4))
	ops := []string{"sum(", "avg(", "min(", "max(", "count(", "stddev(", "stdvar(", "sort(", "sort_desc(", "topk(3, ", "bottomk(1, ", "sum by (a) (", "count without (b) ("}
	f.Fuzz(func(t *testing.T, label, value, text string, stage, op uint8) {
		name := strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r == '_' {
				return r
			}
			return -1
		}, label)
		if name == "" || !utf8.ValidString(value) || !utf8.ValidString(text) || strings.ContainsAny(value+text, "\n\r") {
			return
		}
		logQuery := "{" + name + "=" + strconv.Quote(value) + "}"
		switch stage % 6 {
		case 1:
			logQuery += " | json"
		case 2:
			logQuery += " | logfmt"
		case 3:
			logQuery += " |= " + strconv.Quote(text)
		case 4:
			logQuery += " | logfmt | " + name + "=" + strconv.Quote(text)
		case 5:
			logQuery += " | line_format " + strconv.Quote(text)
		}
		if ValidateLogQL(logQuery) != "" {
			return // a selector Loki rejects (an empty-value matcher alone)
		}
		prefix := ops[int(op)%len(ops)]
		query := prefix + logQuery + ")"
		_, err := Parse(query)
		want := fmt.Sprintf("parse error at line 1, col %d: syntax error: unexpected )", utf8.RuneCountInString(prefix+logQuery)+1)
		if err == nil || err.Error() != want {
			t.Fatalf("%s: %v, want %q", query, err, want)
		}
	})
}
