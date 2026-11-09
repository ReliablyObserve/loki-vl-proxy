package logql

import (
	"fmt"
	"strconv"
	"testing"
)

// stageParseErrorGrid is the grid the parse-time stage checks are held to Loki on: label filter, drop and keep
// regexes Loki cannot parse, in every filter form, next to the stages whose errors Loki reports before or after
// them, under a selector that matches the empty string, and inside metric queries with grouping and unwrap rules.
func stageParseErrorGrid() []string {
	regexes := []string{`(`, `a)|(b`, `[z-a]`, `x{1001}`, `*`, `\8`, `(?P<n>a)(?P<n>b)`, `\p{Foo}`, `a.*`}
	forms := []string{`| x=~%s`, `| x!~%s`, `| x="1" or y=~%s`, `| x=~"a", y!~%s`, `| x="1" and (y!~%s or z="1")`,
		`| drop x=~%s`, `| keep a, x!~%s`, `| drop x="a", y!~%s`, `| json | x=~%s`, `| x > 1 or y=~%s`}
	var out []string
	for _, re := range regexes {
		for _, f := range forms {
			out = append(out, `{app="a"} `+fmt.Sprintf(f, strconv.Quote(re)))
		}
		out = append(out, "{app=\"a\"} | x=~`"+re+"`")
	}
	for _, o := range []string{`|~ "["`, `| regexp "x"`, `| pattern "<a><b>"`, `| json x="a["`, `| line_format "{{.x}"`,
		`| label_format x="{{.y}"`, `| b=ip("b")`, `| x=~"["`, `| drop y=~"["`} {
		out = append(out, `{app="a"} `+o+` | x=~"("`, `{app="a"} | x=~"(" `+o)
	}
	return append(out,
		`{app=~".*"} | x=~"("`, `{app=~".*"} | regexp "x"`, `{app=~".*"} | drop x=~"("`, `{app=~".*"} |~ "("`,
		`{app=~".*"} | pattern "<a><b>"`, `{app=~".*"} | json x="a["`, `{app=~".*"} | b=ip("b")`,
		`count_over_time({app="a"} | regexp "x" | unwrap y [5m])`, `sum_over_time({app="a"} | x=~"(" [5m])`,
		`sum_over_time({app="a"} | x=~"(" | unwrap y [5m])`, `count_over_time({app="a"} | x=~"(" [5m]) by (z)`,
		`rate({app=~".*"} | x=~"(" [5m])`, `count_over_time({app="a"} | pattern "<a><b>" [5m])`,
		`count_over_time({app="a"} |~ "(" [5m]) by (z)`, `count_over_time({app="a"} | drop x=~"(" [5m])`,
		`sum_over_time({app="a"} | unwrap y | x=~"(" [5m])`, `sum_over_time({app="a"} | unwrap y | b=ip("b") [5m])`,
		`sum_over_time({app="a"} | json | unwrap y | b=ip("b") [5m])`, `count_over_time({app=~".*"} [5m]) by (z)`,
		`count_over_time({app=~".*"} | unwrap y [5m])`, `sum(count_over_time({app="a"} | x=~"(" [5m])) by (z)`,
		`count_over_time({app="a"} | regexp "x" | unwrap a | unwrap b [5m])`,
		`count_over_time({app="a"} | x=~"(" | line_format "{{.x}" [5m])`,
		`count_over_time({app="a"} | line_format "{{.x}" | x=~"(" [5m])`, `count_over_time({app="a"} |~ "(" | regexp "x" [5m])`,
		`rate({app="a"} | logfmt | x=~"[" [1m]) > 0`, `sum by (a) (rate({app="a"} | x=~"(" [1m])) / sum(rate({app="b"} | regexp "x" [1m]))`,
		`label_replace(rate({app="a"} | x=~"(" [1m]), "a", "$1", "b", "(.*)")`, `count_over_time({app="a"} | b=ip("b") [5m])`,
		`sum_over_time({app="a"} | b=ip("b") | unwrap y [5m])`, `count_over_time({app="a"} |~ "(" | b=ip("b") [5m])`,
	)
}

const stageParseErrorLokiAnswers = "testdata/stage-parse-errors-loki.jsonl"

// A label filter, drop or keep regex Loki cannot parse is Loki's 400 with Loki's text, and like the regexp and
// pattern parsers it is reported in pipeline order before anything Loki checks once the query is parsed: the
// selector's empty-compatible rule, a range aggregation's grouping and unwrap rules, and the stages it builds.
// Loki's answers are recorded with the Loki oracle check command, which builds metric query pipelines too
// (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/label-filter-regex-parse-error
func TestStageParseErrorsMatchLoki(t *testing.T) {
	requireLokiAnswers(t, "check", stageParseErrorGrid(), stageParseErrorLokiAnswers, nil)
}
