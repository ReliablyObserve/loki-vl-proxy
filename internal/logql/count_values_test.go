package logql

import "testing"

// Loki v3.7.7 has no count_values: pkg/logql/syntax/lex.go:114-125 maps no
// token to it and the vectorOp production (syntax.y:477-490, ast.go:1243-1254)
// has no entry for it, so its parser rejects the name as an IDENTIFIER wherever
// an expression starts, with the position of the identifier. The messages are
// the output of syntax.ParseExpr on v3.7.7.
// conformance: semantics/count-values-unsupported
func TestCountValuesRejectedLikeLoki(t *testing.T) {
	const inner = `count_over_time({env="production"}[5m])`
	const unexpected = ": syntax error: unexpected IDENTIFIER"
	for _, tc := range []struct{ query, want string }{
		{`count_values("v", sum by (level) (` + inner + `))`, "parse error at line 1, col 1" + unexpected},
		{`count_values("v", ` + inner + `)`, "parse error at line 1, col 1" + unexpected},
		{`count_values by (level) ("v", sum by (level) (` + inner + `))`, "parse error at line 1, col 1" + unexpected},
		{`count_values("v", ` + inner + `) by (level)`, "parse error at line 1, col 1" + unexpected},
		{`count_values("v", rate({env="a"}[5m])`, "parse error at line 1, col 1" + unexpected},
		{`count_values`, "parse error at line 1, col 1" + unexpected},
		{`count_values()`, "parse error at line 1, col 1" + unexpected},
		{`sum(count_values("v", ` + inner + `))`, "parse error at line 1, col 5" + unexpected},
		{`sum by (level) (count_values("v", ` + inner + `))`, "parse error at line 1, col 17" + unexpected},
		{`topk(1, count_values("v", ` + inner + `))`, "parse error at line 1, col 9" + unexpected},
		{`(count_values("v", ` + inner + `))`, "parse error at line 1, col 2" + unexpected},
		{inner + ` + count_values("v", ` + inner + `)`, "parse error at line 1, col 43" + unexpected},
		{`sum(count_over_time({env="a"}[5m])) > count_values("v", rate({env="a"}[5m]))`, "parse error at line 1, col 39" + unexpected},
		{`sum(rate({env="a"}[5m])) by (x) + on(x) count_values("v", rate({env="a"}[5m]))`, "parse error at line 1, col 41" + unexpected},
		{`vector(1) + count_values("v", rate({env="a"}[5m]))`, "parse error at line 1, col 13" + unexpected},
		{`label_replace(count_values("v", rate({env="a"}[5m])), "a", "b", "c", "d")`, "parse error at line 1, col 15" + unexpected},
		{"sum(\n  count_values(\"v\", " + inner + "))", "parse error at line 2, col 3" + unexpected},
		// Columns count characters, not bytes.
		{`sum(count_over_time({env="żółć"}[5m])) + count_values("v", ` + inner + `)`, "parse error at line 1, col 42" + unexpected},
	} {
		if got := ValidateLogQL(tc.query); got != tc.want {
			t.Errorf("%q:\n got %q\nwant %q", tc.query, got, tc.want)
		}
	}
}

// count_values stays an ordinary label name: Loki parses `| count_values="x"`
// as a label filter.
// conformance: semantics/count-values-unsupported
func TestCountValuesAsLabelNameStaysValid(t *testing.T) {
	for _, q := range []string{
		`{env="production"} | count_values="x"`,
		`sum by (count_values) (count_over_time({env="production"}[5m]))`,
	} {
		if got := ValidateLogQL(q); got != "" {
			t.Errorf("%q rejected: %q", q, got)
		}
	}
}
