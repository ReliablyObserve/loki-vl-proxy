package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Loki v3.7.7 has no group() or label_join() and no PromQL math or time
// functions: its lexer turns only the names in pkg/logql/syntax/lex.go
// functionTokens into function tokens, so any other name is an IDENTIFIER that
// the grammar (syntax.y vectorOp, rangeOp, LABEL_REPLACE, vectorExpr) cannot
// place. The messages below are what syntax.ParseExpr returned for each query,
// including the column the unexpected token starts at.
//
// conformance: semantics/proxy-accepts-operators-not-in-loki
func TestValidateLogQL_NamesLokiDoesNotKnowAreRejected(t *testing.T) {
	cases := []struct{ query, want string }{
		{`group(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`GROUP(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`group by (app) (count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`group without (app) (count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`group(count_over_time({app="a"}[5m])) by (app)`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`sum(group(count_over_time({app="a"}[5m])))`, `parse error at line 1, col 5: syntax error: unexpected IDENTIFIER`},
		{`group(count_over_time({app="a"}[5m])) > 0`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`2 * group(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 5: syntax error: unexpected IDENTIFIER`},
		{`sum(rate({app="a"}[5m])) + group(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 28: syntax error: unexpected IDENTIFIER`},
		{`vector(1) + group(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 13: syntax error: unexpected IDENTIFIER`},
		{`topk(3, group(count_over_time({app="a"}[5m])))`, `parse error at line 1, col 9: syntax error: unexpected IDENTIFIER`},
		{`sum by (app) (group(count_over_time({app="a"}[5m])))`, `parse error at line 1, col 15: syntax error: unexpected IDENTIFIER`},
		{`(group(count_over_time({app="a"}[5m])))`, `parse error at line 1, col 2: syntax error: unexpected IDENTIFIER`},
		{`sum(  group (rate({app="a"}[5m])))`, `parse error at line 1, col 7: syntax error: unexpected IDENTIFIER`},
		{`label_join(count_over_time({app="a"}[5m]), "d", ",", "app", "pod")`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`LABEL_JOIN(count_over_time({app="a"}[5m]), "d", ",", "app")`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`label_join(sum by (app) (count_over_time({app="a"}[5m])), "d", ",", "app")`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`label_replace(label_join(count_over_time({app="a"}[5m]), "d", ",", "app"), "a", "b", "c", "d")`, `parse error at line 1, col 15: syntax error: unexpected IDENTIFIER`},
		{`sum(label_replace(group(count_over_time({app="a"}[5m])), "a", "b", "c", "d"))`, `parse error at line 1, col 19: syntax error: unexpected IDENTIFIER`},
		{`count_over_time({app="a"}[5m]) + label_replace(label_join(count_over_time({app="a"}[5m]), "d", ",", "app"), "a", "b", "c", "d")`, `parse error at line 1, col 48: syntax error: unexpected IDENTIFIER`},
		{`abs(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`sgn(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`round(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`clamp(count_over_time({app="a"}[5m]), 1, 2)`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`clamp_min(count_over_time({app="a"}[5m]), 1)`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`ceil(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`floor(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`sqrt(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`ln(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`exp(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`log2(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`sin(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`scalar(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`time()`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`timestamp(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`hour(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`quantile(0.5, count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`count_values("x", count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`present_over_time({app="a"}[5m])`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`increase(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`delta(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`irate({app="a"}[5m])`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`absent(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`histogram_quantile(0.5, count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`foo(count_over_time({app="a"}[5m]))`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`bar(1)`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`group`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`ln`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{`rate({app="a"}[5m]) + foo`, `parse error at line 1, col 23: syntax error: unexpected IDENTIFIER`},
		{`rate({app="a"}[5m]) OFFSET 1m`, `parse error at line 1, col 21: syntax error: unexpected offset`},
		{`count_over_time({app="a"}[5m]) OFFSET 5m`, `parse error at line 1, col 32: syntax error: unexpected offset`},
		{`count_over_time({app="a"}[5m]) + Count`, `parse error at line 1, col 34: syntax error: unexpected IDENTIFIER`},
		{`rate({app="a"}[5m]) + Rate`, `parse error at line 1, col 23: syntax error: unexpected IDENTIFIER`},
		{`{app="a"} | json | JSON="x"`, `parse error at line 1, col 24: syntax error: unexpected =`},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := logql.ValidateLogQL(tc.query); got != tc.want {
				t.Fatalf("ValidateLogQL(%q)\n got  %q\n want %q", tc.query, got, tc.want)
			}
		})
	}
}

// Loki's lexer lower-cases every word before it looks it up (lex.go: tokens,
// functionTokens), so SUM, TOPK, RATE, BY, OFFSET and the rest are the keywords
// they spell. A function name is a function only when a "(" follows it, or by or
// without and then "(", so a label called Rate or Count stays a label.
func TestValidateLogQL_KeywordsAreCaseInsensitive(t *testing.T) {
	queries := []string{
		`TOPK(3, count_over_time({app="a"}[5m]))`,
		`Topk(3, count_over_time({app="a"}[5m]))`,
		`SUM(count_over_time({app="a"}[5m]))`,
		`Sum by (app) (count_over_time({app="a"}[5m]))`,
		`SUM BY (app) (count_over_time({app="a"}[5m]))`,
		`SUM WITHOUT (app) (count_over_time({app="a"}[5m]))`,
		`sum(count_over_time({app="a"}[5m])) BY (app)`,
		`sum(rate({app="a"}[5m])) By (app)`,
		`AVG(count_over_time({app="a"}[5m]))`,
		`MIN(count_over_time({app="a"}[5m]))`,
		`MAX(count_over_time({app="a"}[5m]))`,
		`COUNT(count_over_time({app="a"}[5m]))`,
		`STDDEV(count_over_time({app="a"}[5m]))`,
		`STDVAR(count_over_time({app="a"}[5m]))`,
		`BOTTOMK(3, count_over_time({app="a"}[5m]))`,
		`SORT(count_over_time({app="a"}[5m]))`,
		`SORT_DESC(count_over_time({app="a"}[5m]))`,
		`RATE({app="a"}[5m])`,
		`Rate({app="a"}[5m])`,
		`COUNT_OVER_TIME({app="a"}[5m])`,
		`BYTES_RATE({app="a"}[5m])`,
		`BYTES_OVER_TIME({app="a"}[5m])`,
		`ABSENT_OVER_TIME({app="a"}[5m])`,
		`RATE_COUNTER({app="a"} | unwrap x [5m])`,
		`FIRST_OVER_TIME({app="a"} | unwrap x [5m])`,
		`LAST_OVER_TIME({app="a"} | unwrap x [5m])`,
		`QUANTILE_OVER_TIME(0.5, {app="a"} | unwrap x [5m])`,
		`AVG_OVER_TIME({app="a"} | unwrap x [5m])`,
		`SUM(RATE({app="a"}[5m]))`,
		`LABEL_REPLACE(count_over_time({app="a"}[5m]), "d", "$1", "app", "(.*)")`,
		`VECTOR(1)`,
		`VECTOR(1) + count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) AND count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) OR count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) UNLESS count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) > BOOL 1`,
		`count_over_time({app="a"}[5m]) + ON (app) count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) + IGNORING (app) count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) + ON (app) GROUP_LEFT count_over_time({app="a"}[5m])`,
		`count_over_time({app="a"}[5m]) + ON (app) GROUP_RIGHT (x) count_over_time({app="a"}[5m])`,
		`rate({app="a"}[5m] OFFSET 1m)`,
		`{app="a"} | JSON`,
		`{app="a"} | LOGFMT`,
		`{app="a"} | LINE_FORMAT "x"`,
		`{app="a"} | LABEL_FORMAT a=b`,
		`{app="a"} | DROP a`,
		`{app="a"} | KEEP a`,
		`rate({app="a"} | json | UNWRAP x [5m])`,
		`rate({app="a"} | json | unwrap BYTES(x) [5m])`,
		`rate({app="a"} | json | unwrap DURATION(x) [5m])`,
		`sum by (Rate) (count_over_time({app="a"}[5m]))`,
		`sum by (Count) (count_over_time({app="a"}[5m]))`,
		`{app="a"} | logfmt | Count > 1`,
		`{app="a"} | json | Sum > 1`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			if got := logql.ValidateLogQL(query); got != "" {
				t.Fatalf("ValidateLogQL(%q) = %q, want it accepted like Loki", query, got)
			}
		})
	}
}

// LowercaseKeywords writes the keywords Loki reads case-insensitively in lower
// case and leaves every other word, string and label name as written.
//
// conformance: semantics/proxy-accepts-operators-not-in-loki
func TestLowercaseKeywords(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`TOPK(3, RATE({app="a"}[5m]))`, `topk(3, rate({app="a"}[5m]))`},
		{`SUM BY (App) (COUNT_OVER_TIME({app="a"} |= "ERROR" [5m]))`, `sum by (App) (count_over_time({app="a"} |= "ERROR" [5m]))`},
		{`Sum(Rate({app="a"}[5m])) By (app) > BOOL 1`, `sum(rate({app="a"}[5m])) by (app) > bool 1`},
		{`rate({app="a"} | JSON | UNWRAP BYTES(Size) [5m] OFFSET 1m)`, `rate({app="a"} | json | unwrap bytes(Size) [5m] offset 1m)`},
		{`A + ON (app) GROUP_LEFT (Pod) B`, `A + on (app) group_left (Pod) B`},
		{`{app="a"} | logfmt | Count > 1 | Rate < 2`, `{app="a"} | logfmt | Count > 1 | Rate < 2`},
		{`sum by (Rate, Count) (count_over_time({App="a"}[5m]))`, `sum by (Rate, Count) (count_over_time({App="a"}[5m]))`},
		{`{app="a"} |= "SUM(BY)" | line_format "{{.OFFSET}}"`, `{app="a"} |= "SUM(BY)" | line_format "{{.OFFSET}}"`},
		{`rate({app="a"}[5m])`, `rate({app="a"}[5m])`},
	} {
		if got := logql.LowercaseKeywords(tc.query); got != tc.want {
			t.Errorf("LowercaseKeywords(%q)\n got  %q\n want %q", tc.query, got, tc.want)
		}
	}
}
