package logql_test

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Expected messages below are the exact error strings Loki 3.7.1 returned (HTTP
// 400) for the same query on the e2e-compat stack.

const lokiEmptyCompatible = `parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~".*" does not meet this requirement, but app=~".+" will`

func TestValidateLogQL_LokiRejections(t *testing.T) {
	cases := []struct{ query, want string }{
		// No subquery grammar.
		{`max_over_time(sum(rate({app="a"}[1m]))[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected SUM, expecting NUMBER or { or ("},
		{`max_over_time(rate({app="a"}[1m])[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected RATE, expecting NUMBER or { or ("},
		{`max_over_time(count_over_time({app="a"}[1m])[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected COUNT_OVER_TIME, expecting NUMBER or { or ("},
		{`rate(count_over_time({app="a"}[5m])[30m:5m])`, "parse error at line 1, col 6: syntax error: unexpected COUNT_OVER_TIME, expecting NUMBER or { or ("},
		{`quantile_over_time(0.5, rate({app="a"}[1m])[30m:5m])`, "parse error at line 1, col 25: syntax error: unexpected RATE, expecting { or ("},
		{`max_over_time((rate({app="a"}[1m]))[30m:5m])`, "parse error at line 1, col 16: syntax error: unexpected RATE, expecting { or ("},
		{`sum_over_time(vector(1)[5m:1m])`, "parse error at line 1, col 15: syntax error: unexpected VECTOR, expecting NUMBER or { or ("},
		{`max_over_time(label_replace(rate({app="a"}[1m]),"a","b","c","d")[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected label_replace, expecting NUMBER or { or ("},
		{`sum(max_over_time(rate({app="a"}[1m])[30m:5m]))`, "parse error at line 1, col 19: syntax error: unexpected RATE, expecting NUMBER or { or ("},
		{`count_over_time({app="a"}[30m:5m])`, `parse error at line 0, col 26: unknown unit "m:" in duration "30m:5m"`},

		// topk/bottomk need an integer k > 0.
		{`topk(0, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter (must be greater than 0) topk(0"},
		{`bottomk(0, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter (must be greater than 0) bottomk(0"},
		{`topk(00, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter (must be greater than 0) topk(00"},
		{`topk by (pod) (0, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter (must be greater than 0) topk(0"},
		{`sum(topk(0, count_over_time({app="a"}[5m])))`, "parse error : invalid parameter (must be greater than 0) topk(0"},
		{`topk(0.5, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter topk(0.5,"},
		{`topk(1.5, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter topk(1.5,"},

		// Empty-compatible selectors, at any depth.
		{`{app=""}`, lokiEmptyCompatible},
		{`{app=~""}`, lokiEmptyCompatible},
		{`{app=~".*"}`, lokiEmptyCompatible},
		{`{app!="x"}`, lokiEmptyCompatible},
		{`{app!~"x"}`, lokiEmptyCompatible},
		{`{app!~".+"}`, lokiEmptyCompatible},
		{`{app=~"a|"}`, lokiEmptyCompatible},
		{`{app=~".+|"}`, lokiEmptyCompatible},
		{`{app=~"(?i)"}`, lokiEmptyCompatible},
		{`{}`, lokiEmptyCompatible},
		{`sum(count_over_time({app=""}[5m]))`, lokiEmptyCompatible},
		{`count_over_time({app="a"}[5m]) / count_over_time({app=""}[5m])`, lokiEmptyCompatible},
		{`vector(1) + count_over_time({app=~".*"}[5m])`, lokiEmptyCompatible},

		// label_format.
		{`{app="a"} | label_format x="{{ .pod"`, `parse error : stage '| label_format x="{{ .pod"' : invalid template for label 'x': template: label:1: unclosed action`},
		{`{app="a"} | label_format x="{{ nofunc .pod }}"`, `parse error : stage '| label_format x="{{ nofunc .pod }}"' : invalid template for label 'x': template: label:1: function "nofunc" not defined`},
		{`{app="a"} | label_format x="a", x="b"`, `parse error : stage '| label_format x="a",x="b"' : multiple label name 'x' not allowed in a single format operation`},
		{`{app="a"} | label_format __error__="a"`, `parse error : stage '| label_format __error__="a"' : __error__ cannot be formatted`},
		{`{app="a"} | label_format x="{{ end }}"`, `parse error : stage '| label_format x="{{ end }}"' : invalid template for label 'x': template: label:1: unexpected {{end}}`},
		{`{app="a"} | label_format x="{{ .pod }"`, `parse error : stage '| label_format x="{{ .pod }"' : invalid template for label 'x': template: label:1: unexpected "}" in operand`},
		{`count_over_time({app="a"} | label_format x="{{" [5m])`, `parse error : stage '| label_format x="{{"' : invalid template for label 'x': template: label:1: unclosed action`},
		{`{app="a"} | label_format x="a",`, "parse error at line 1, col 32: syntax error: unexpected $end, expecting IDENTIFIER"},
		{`{app="a"} | label_format x`, "parse error at line 1, col 27: syntax error: unexpected $end, expecting ="},

		// line_format (log queries).
		{`{app="a"} | line_format "{{ .pod"`, `parse error : stage '| line_format "{{ .pod"' : invalid line template: template: line:1: unclosed action`},
		{`{app="a"} | line_format "{{ nofunc .pod }}"`, `parse error : stage '| line_format "{{ nofunc .pod }}"' : invalid line template: template: line:1: function "nofunc" not defined`},

		// json expressions.
		{`{app="a"} | json foo="bar["`, `parse error : stage '| json foo="bar["' : cannot parse expression [bar[]: syntax error: unexpected $end, expecting STRING or INDEX`},
		{`{app="a"} | json foo="a.b[0"`, `parse error : stage '| json foo="a.b[0"' : cannot parse expression [a.b[0]: syntax error: unexpected $end, expecting STRING or INDEX`},
		{`{app="a"} | json foo="a..b"`, `parse error : stage '| json foo="a..b"' : cannot parse expression [a..b]: syntax error: unexpected DOT, expecting FIELD`},
		{`{app="a"} | json foo=""`, `parse error : stage '| json foo=""' : cannot parse expression []: syntax error: unexpected $end, expecting LSB or FIELD`},
		{`{app="a"} | json foo="a.b-c"`, `parse error : stage '| json foo="a.b-c"' : cannot parse expression [a.b-c]: unexpected char -`},
		{`{app="a"} | json foo="a b"`, `parse error : stage '| json foo="a b"' : cannot parse expression [a b]: syntax error: unexpected FIELD`},
		{`{app="a"} | json foo="a.[0]"`, `parse error : stage '| json foo="a.[0]"' : cannot parse expression [a.[0]]: syntax error: unexpected LSB, expecting FIELD`},
		{`{app="a"} | json foo="a[x]"`, `parse error : stage '| json foo="a[x]"' : cannot parse expression [a[x]]: syntax error: unexpected FIELD, expecting STRING or INDEX`},
		{`{app="a"} | json foo="a]"`, `parse error : stage '| json foo="a]"' : cannot parse expression [a]]: syntax error: unexpected RSB`},
		{`{app="a"} | json foo=`, "parse error at line 1, col 22: syntax error: unexpected $end, expecting STRING"},
		{`{app="a"} | json foo=bar`, "parse error : syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`count_over_time({app="a"} | json foo="a[" [5m])`, `parse error : stage '| json foo="a["' : cannot parse expression [a[]: syntax error: unexpected $end, expecting STRING or INDEX`},

		// logfmt expressions.
		{`{app="a"} | logfmt foo="bar["`, `parse error : stage '| logfmt foo="bar["' : cannot parse expression [bar[]: unexpected char [`},
		{`{app="a"} | logfmt foo="a.b"`, `parse error : stage '| logfmt foo="a.b"' : cannot parse expression [a.b]: unexpected char .`},
		{`{app="a"} | logfmt foo=""`, `parse error : stage '| logfmt foo=""' : cannot parse expression []: syntax error: unexpected $end, expecting STRING or KEY`},
		{`{app="a"} | logfmt foo="a b"`, `parse error : stage '| logfmt foo="a b"' : cannot parse expression [a b]: syntax error: unexpected KEY`},

		// regexp and pattern parsers.
		{`{app="a"} | regexp "(["`, "parse error : invalid regexp parser: error parsing regexp: missing closing ]: `[`"},
		{`{app="a"} | regexp "abc"`, "parse error : invalid regexp parser: at least one named capture must be supplied"},
		{`{app="a"} | regexp "(x)"`, "parse error : invalid regexp parser: at least one named capture must be supplied"},
		{`{app="a"} | regexp "(?P<a>x)(?P<a>y)"`, "parse error : invalid regexp parser: duplicate extracted label name 'a'"},
		{`{app="a"} | pattern "<_><_>"`, "parse error : invalid pattern parser: at least one capture is required"},
		{`{app="a"} | pattern "abc"`, "parse error : invalid pattern parser: at least one capture is required"},
		{`{app="a"} | pattern "<a> <a>"`, "parse error : invalid pattern parser: duplicate capture name (a): invalid expression"},
		{`{app="a"} | pattern "<a><b>"`, "parse error : invalid pattern parser: found consecutive capture '<a><b>': invalid expression"},
		{`{app="a"} | pattern ""`, "parse error : invalid pattern parser: parse error at line 1, col 1: syntax error: unexpected $end, expecting IDENTIFIER or LITERAL"},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := logql.ValidateLogQL(tc.query); got != tc.want {
				t.Fatalf("ValidateLogQL(%q)\n got: %s\nwant: %s", tc.query, got, tc.want)
			}
		})
	}
}

// Near misses Loki 3.7.1 answers with 200: the validation must not over-reject.
func TestValidateLogQL_LokiAccepts(t *testing.T) {
	for _, query := range []string{
		`{app=~".+"}`,
		`{app="", pod="a"}`,
		`{app="a", pod=~".*"}`,
		`{app=~"", pod!=""}`,
		`max by(level)(rate({__error__!=""}[5m]))`,
		`topk(1, count_over_time({app="a"}[5m]))`,
		`bottomk(2, count_over_time({app="a"}[5m]))`,
		`count_over_time(({app="a"})[5m])`,
		`count_over_time(({app="a"} |= "x")[5m])`,
		`{app="a"} | label_format x=pod`,
		`{app="a"} | label_format x=pod, y="{{.app}}"`,
		"{app=\"a\"} | label_format x=`{{ __line__ | trunc 3 }}`",
		`{app="a"} | label_format x="{{ regexReplaceAll \"a\" .pod \"b\" }}"`,
		`{app="a"} | label_format x="{{ .pod | unixEpoch }}"`,
		`{app="a"} | label_format x="{{ .pod | printf \"%s\" }}"`,
		`{app="a"} | line_format "{{ __line__ | trunc 3 }}"`,
		`count_over_time({app="a"} | line_format "{{.msg" [5m])`,
		`{app="a"} | json foo="a.b[0]"`,
		`{app="a"} | json foo="a[\"b\"]"`,
		`{app="a"} | json foo="[0]"`,
		`{app="a"} | json foo, bar="baz"`,
		`{app="a"} | json foo="a", foo="b"`,
		`{app="a"} | json foo`,
		"{app=\"a\"} | json status=`http.status`, method",
		`{app="a"} | logfmt foo="a"`,
		`{app="a"} | logfmt foo, bar="baz"`,
		`{app="a"} | regexp "(?P<1a>x)"`,
		`{app="a"} | regexp "(?P<a>x)(y)"`,
		`{app="a"} | pattern "<_> <a> <_>"`,
		`{app="a"} | pattern "<ip> - <_> <path>"`,
	} {
		t.Run(query, func(t *testing.T) {
			if got := logql.ValidateLogQL(query); got != "" {
				t.Fatalf("ValidateLogQL(%q) = %s, want accepted", query, got)
			}
		})
	}
}

func TestValidateMatchersQuery(t *testing.T) {
	cases := []struct{ query, want string }{
		{`{app="a"}`, ""},
		{`{app=~".+"}`, ""},
		{`{app="", pod="a"}`, ""},
		{`*`, ""},
		{`{}`, lokiEmptyCompatible},
		{`{app=""}`, lokiEmptyCompatible},
		{`{app!~"x"}`, lokiEmptyCompatible},
		{`{app="a"} |= "foo"`, "only label matchers are supported"},
		{`{app="a"} | logfmt`, "only label matchers are supported"},
		{`count_over_time({app="a"}[5m])`, "only label matchers are supported"},
		// Loki's ParseExpr validates every selector before the type check.
		{`{app=""} |= "foo"`, lokiEmptyCompatible},
		{`count_over_time({app=""}[5m])`, lokiEmptyCompatible},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := logql.ValidateMatchersQuery(tc.query); got != tc.want {
				t.Fatalf("ValidateMatchersQuery(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
	if got := logql.ValidateMatchersQuery(`{app="a"`); got == "" {
		t.Fatal("unterminated selector accepted")
	}
}

// Expected messages are Loki 3.7.1's /series answers for the same match and
// match[] values.
func TestValidateSeriesMatchers(t *testing.T) {
	cases := []struct {
		groups []string
		want   string
	}{
		{nil, ""},
		{[]string{`{}`}, ""},
		{[]string{`{ }`}, ""},
		{[]string{`{}`, `{}`}, ""},
		{[]string{`{app="a"}`, `{pod="b"}`}, ""},
		{[]string{`{app="", pod="b"}`}, ""},
		{[]string{`*`}, ""},
		{[]string{`{app=""}`}, lokiEmptyCompatible},
		{[]string{`{app="a"} |= "x"`}, "only label matchers are supported"},
		{[]string{`{ }`, `{app="a"}`}, "0 matchers in group: { }"},
		{[]string{`{app=""}`, `{}`}, "0 matchers in group: {}"},
		{[]string{"{\t}"}, "0 matchers in group: {\t}"},
		{[]string{`{app="a"}`, `{app=""}`}, lokiEmptyCompatible},
		// A group that only matches the empty string is still a selector
		// (ParseMatchers without validation), so the syntax pass succeeds
		// and the querier's validation rejects it.
		{[]string{`{app=~".*"}`, `{app="a"} | json`}, "only label matchers are supported"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.groups, "&"), func(t *testing.T) {
			if got := logql.ValidateSeriesMatchers(tc.groups); got != tc.want {
				t.Fatalf("ValidateSeriesMatchers(%q) = %q, want %q", tc.groups, got, tc.want)
			}
		})
	}
}

// Loki rejects any query of syntax.maxInputSize bytes or more before parsing.
func TestValidateInputSizeLimit(t *testing.T) {
	big := `{app="` + strings.Repeat("x", logql.MaxInputSize) + `"}`
	want := "parse error : input size too long (131080 > 131072)"
	for name, got := range map[string]string{
		"ValidateLogQL":            logql.ValidateLogQL(big),
		"ValidateMatchersQuery":    logql.ValidateMatchersQuery(big),
		"ValidateLogSelectorQuery": logql.ValidateLogSelectorQuery(big),
		"ValidateSeriesMatchers":   logql.ValidateSeriesMatchers([]string{big}),
	} {
		if got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if logql.InputSizeError(big[:logql.MaxInputSize-1]) != "" {
		t.Error("input below the limit rejected")
	}
}

// Loki's removeLineformat keeps line_format in a line-independent range
// aggregation when a parser or line filter follows it; only then is an
// invalid template reported (verified against Loki 3.7.1).
func TestValidateLogQL_LineFormatInRangeAggregation(t *testing.T) {
	rejected := `parse error : stage '| line_format "{{bad"' : invalid line template: template: line:1: function "bad" not defined`
	cases := []struct{ query, want string }{
		{`count_over_time({app="a"} | line_format "{{bad" | json [5m])`, rejected},
		{`count_over_time({app="a"} | line_format "{{bad" |= "x" [5m])`, rejected},
		{`rate({app="a"} | line_format "{{bad" | logfmt foo="bar" [5m])`, rejected},
		{`count_over_time({app="a"} | line_format "{{bad" | pattern "<a> x" [5m])`, rejected},
		{`bytes_over_time({app="a"} | line_format "{{bad" [5m])`, rejected},
		{`count_over_time({app="a"} | line_format "{{bad" | label_format a=b [5m])`, ""},
		{`count_over_time({app="a"} | json | line_format "{{bad" [5m])`, ""},
		{`sum(rate({app="a"} | line_format "{{bad" | drop x [5m]))`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := logql.ValidateLogQL(tc.query); got != tc.want {
				t.Fatalf("ValidateLogQL(%q)\n got: %s\nwant: %s", tc.query, got, tc.want)
			}
		})
	}
}

func TestValidateLogSelectorQuery(t *testing.T) {
	cases := []struct{ query, want string }{
		{`{app="a"}`, ""},
		{`{app="a"} |= "foo"`, ""},
		{`{app="a"} | logfmt`, ""},
		{`{app=""}`, lokiEmptyCompatible},
		{`{}`, lokiEmptyCompatible},
		{``, "parse error : syntax error: unexpected $end"},
		{`count_over_time({app="a"}[5m])`, "only log selector is supported"},
		{`{app="a"} | json foo="bar["`, `parse error : stage '| json foo="bar["' : cannot parse expression [bar[]: syntax error: unexpected $end, expecting STRING or INDEX`},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := logql.ValidateLogSelectorQuery(tc.query); got != tc.want {
				t.Fatalf("ValidateLogSelectorQuery(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}

// Label format entries keep the rendering the string translator consumes.
func TestParseLabelFormatStructure(t *testing.T) {
	lq, err := logql.ParseLogQuery("{app=\"a\"} | label_format dst=src, tmpl=\"{{.app}}\", raw=`x`")
	if err != nil {
		t.Fatal(err)
	}
	stage := lq.Pipeline[0].(*logql.LabelFormatStage)
	if stage.Raw != "dst=src, tmpl=\"{{.app}}\", raw=`x`" {
		t.Fatalf("raw=%q", stage.Raw)
	}
	want := []logql.LabelFormat{{Name: "dst", Value: "src", Rename: true}, {Name: "tmpl", Value: "{{.app}}"}, {Name: "raw", Value: "x"}}
	if len(stage.Formats) != len(want) {
		t.Fatalf("formats=%+v", stage.Formats)
	}
	for i := range want {
		if stage.Formats[i] != want[i] {
			t.Fatalf("formats[%d]=%+v want %+v", i, stage.Formats[i], want[i])
		}
	}
}
