package logql

import (
	"testing"
)

// conformance: semantics/label-filter-expression-syntax
func TestParseLogQuery_LabelFilterExpressionStages(t *testing.T) {
	cases := []struct {
		query string
		want  []string // the pipeline, each stage as the AST prints it
	}{
		// The registry request.
		{`{service_name="api-gateway"} | json | (status>=500 or duration_ms>1000), method="GET"`,
			[]string{`| json`, `| (status>=500 or duration_ms>1000), method="GET"`}},
		{`{a="b"} | json | foo>=5,bar<25ms`, []string{`| json`, `| foo>=5, bar<25ms`}},
		{`{a="b"} | json | (status_code < 500 and status_code > 200) or latency >= 250ms`,
			[]string{`| json`, `| (status_code<500 and status_code>200)or latency>=250ms`}},
		// A filter operator after a comma is a label filter operator.
		{`{a="b"} | json | a="1", b!="2", c!~"x"`, []string{`| json`, `| a="1", b!="2", c!~"x"`}},
		// A line filter after a complete filter is a line filter stage.
		{`{a="b"} | json | (a="1" or b="2") != "x"`, []string{`| json`, `| (a="1" or b="2")`, `!= "x"`}},
		{`{a="b"} | json | a="1" !~ "x"`, []string{`| json`, `| a="1"`, `!~ "x"`}},
		{`{a="b"} | json | a="1", b=5s |= "x"`, []string{`| json`, `| a="1", b=5s`, `|= "x"`}},
		// Several stages.
		{`{a="b"} | (a="1" or b="2") | c="3", d="4" | logfmt`, []string{`| (a="1" or b="2")`, `| c="3", d="4"`, `| logfmt`}},
		// Durations and numbers keep their text.
		{`{a="b"} | size>10KB, v>5, w<=2.5`, []string{`| size>10KB, v>5, w<=2.5`}},
		// A group without a parser in front.
		{`{a="b"} | (a="1")`, []string{`| (a="1")`}},
	}
	for _, tc := range cases {
		lq, err := ParseLogQuery(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		var got []string
		for _, stage := range lq.Pipeline {
			got = append(got, stage.String())
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s\n  got  %q\n  want %q", tc.query, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: stage %d\n  got  %q\n  want %q", tc.query, i, got[i], tc.want[i])
			}
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestValidateLogQL_LabelFilterExpressionContexts(t *testing.T) {
	for _, query := range []string{
		`{a="b"} | json | (status>=500 or duration_ms>1000), method="GET"`,
		`{a="b"} | logfmt | (a="1" or b="2"), c>5`,
		`sum by (a) (count_over_time({a="b"} | json | (status>=500 or duration_ms>1000), method="GET" [5m]))`,
		`sum without (a) (rate({a="b"} | json | (status>=500 or duration_ms>1000), method="GET" [5m]))`,
		`sum by (a) (rate({a="b"} | json | (status>=500 or x>1), method="GET" | unwrap duration_ms [5m]))`,
		`topk(3, sum by (a) (bytes_over_time({a="b"} | logfmt | b>1, (c="x" or d=~"y") [1m])))`,
		`sum(count_over_time({a="b"} | json | a="1", b="2" [5m])) / sum(count_over_time({a="b"} | json | (a="1" or b="2") [5m]))`,
		`{a="b"} | json | ((a="1")) | line_format "{{.a}}"`,
		`{a="b"} | json | a=ip("1.2.3.4"), b!=ip("10.0.0.0/8")`,
	} {
		if msg := ValidateLogQL(query); msg != "" {
			t.Errorf("%s: %s", query, msg)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestValidateLogQL_IPCallIsNotAStage(t *testing.T) {
	// Loki answers `| ip("...")` with "unexpected ip" at the name (recorded from
	// the e2e stack's Loki); the value forms stay valid.
	for query, want := range map[string]string{
		`{app="api-gateway"} | json | ip("not-a-valid-cidr")`: "parse error at line 1, col 30: syntax error: unexpected ip",
		`{app="nginx"} | ip("10.0.0.0/8")`:                    "parse error at line 1, col 17: syntax error: unexpected ip",
		`{app="a"} | json | addr = ip("10.0.0.0/8")`:          "",
		`{app="a"} |= ip("10.0.0.0/8")`:                       "",
		`{app="a"} | json | ip="x"`:                           "",
	} {
		if got := ValidateLogQL(query); got != want {
			t.Errorf("%s\n  got  %q\n  want %q", query, got, want)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestValidateLogQL_LabelFilterExpressionErrorsInMetrics(t *testing.T) {
	// Messages recorded from Loki v3.7.9.
	cases := []struct{ query, want string }{
		{`sum(count_over_time({a="b"} | json | (a="1" [5m]))`, "parse error at line 0, col 45: syntax error: unexpected RANGE"},
		{`sum(count_over_time({a="b"} | json | (a="1" or b="2" [5m]))`, "parse error at line 0, col 54: syntax error: unexpected RANGE"},
		{`sum(count_over_time({a="b"} | json | a="1" or [5m]))`, "parse error at line 0, col 47: syntax error: unexpected RANGE, expecting IDENTIFIER or ("},
		{`sum(count_over_time({a="b"} | json | a="1", [5m]))`, "parse error at line 0, col 45: syntax error: unexpected RANGE, expecting IDENTIFIER or ("},
		{`sum(rate({a="b"} | json | (a="1" or b="2" | unwrap c [5m]))`, "parse error at line 1, col 43: syntax error: unexpected |"},
		{`{a="b"} | json | (a="1" or b="2" | unwrap c`, "parse error at line 1, col 34: syntax error: unexpected |"},
		// Loki's lexer reads a string to its closing quote.
		{`{a="b"} | json | a="1`, "parse error at line 1, col 20: literal not terminated"},
		{`{a="b"} | json | a="1", b="2`, "parse error at line 1, col 27: literal not terminated"},
		{"{a=\"b\"} | json | a=`x", "parse error at line 1, col 20: literal not terminated"},
		{`{a="b"} | json | (a="1) or b="2"`, "parse error at line 1, col 31: syntax error: unexpected NUMBER"},
	}
	for _, tc := range cases {
		if got := ValidateLogQL(tc.query); got != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.query, got, tc.want)
		}
	}
}
