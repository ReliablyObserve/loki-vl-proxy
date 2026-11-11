package logql

import (
	"strings"
	"testing"
)

// labelListBodies are what stands between the parentheses of a label list written with parentheses, as the client
// may type it: a trailing comma, a doubled one, a leading one, no name, whitespace in every place.
var labelListBodies = []string{
	"a", "a,b", "a, b", "a ,b", " a ", " a , b ",
	"a,", "a ,", "a, ", "a , ", "a,b,", "a, b, ", "a,,", "a,,b", "a, ,b", "a,b,,c",
	",a", " ,a", ",", " , ", ",,", ",a,",
	"", " ", "  ",
	"a\n,", "a,\n", "a b", "a,b c",
}

// labelListPositions are the places a label list written with parentheses stands in LogQL, with L for the list.
var labelListPositions = []string{
	`sum by (L) (count_over_time({app="a"}[1m]))`,
	`sum without (L) (count_over_time({app="a"}[1m]))`,
	`sum (count_over_time({app="a"}[1m])) by (L)`,
	`sum (count_over_time({app="a"}[1m])) without (L)`,
	`topk(2, count_over_time({app="a"}[1m])) by (L)`,
	`topk by (L) (2, count_over_time({app="a"}[1m]))`,
	`count_values("x", count_over_time({app="a"}[1m])) by (L)`,
	`sum_over_time({app="a"} | json | unwrap x [1m]) by (L)`,
	`sum_over_time({app="a"} | json | unwrap x [1m]) without (L)`,
	`sum by (a) (sum_over_time({app="a"} | json | unwrap x [1m]) by (L))`,
	`sum(count_over_time({app="a"}[1m])) / on(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / ignoring(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(a) group_left(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(a) group_right(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / ignoring(a) group_left(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(a) group_left sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(L) group_left(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) and on(L) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) > bool on(L) sum(count_over_time({app="a"}[1m]))`,
	`sum by (L) (count_over_time({app="a"} | json | drop b [1m]))`,
}

// labelListFreeBodies are what follows the keyword of a list written without parentheses (json, logfmt, drop,
// keep, label_format): the same mistakes.
var labelListFreeBodies = []string{
	"a", "a, b", "a ,b", "a,b",
	"a,", "a ,", "a, ", "a,b,", "a, b, ", "a,,", "a,,b", "a, ,b",
	",a", " ,a", ",", "",
	"a,\n", "a b",
}

// labelListFreePositions are the places such a list stands, with L for the list.
var labelListFreePositions = []string{
	`{app="a"} | json L`,
	`{app="a"} | logfmt L`,
	`{app="a"} | logfmt --strict L`,
	`{app="a"} | logfmt --keep-empty L`,
	`{app="a"} | drop L`,
	`{app="a"} | keep L`,
	`{app="a"} | json x | drop L`,
	`{app="a"} | drop L | json`,
	`{app="a"} | json L | logfmt`,
	`sum(count_over_time({app="a"} | json L [1m]))`,
	`sum(count_over_time({app="a"} | drop L [1m]))`,
}

// labelListPairBodies are what follows `label_format` or an assignment in json and drop lists: pairs written with =.
var labelListPairBodies = []string{
	`a=b`, `a=b, c=d`, `a=b,`, `a=b, c=d,`, `a=b,,c=d`, `,a=b`, `a=b ,`,
	`a="x"`, `a="x",`, `a="x", b="y"`, `a="x", b="y",`, `a="x",,b="y"`, `,a="x"`,
}

var labelListPairPositions = []string{
	`{app="a"} | json L`,
	`{app="a"} | drop L`,
	`{app="a"} | keep L`,
	`{app="a"} | label_format L`,
}

// labelListMatcherBodies are the drop and keep items that hold a matcher.
var labelListMatcherBodies = []string{
	`a!="x"`, `a!="x",`, `a=~"x"`, `a=~"x",`, `a=~"x", b!~"y"`, `a=~"x", b!~"y",`, `a="x", b,`, `a, b="x",`, `a,,b="x"`, `,a=~"x"`,
}

var labelListMatcherPositions = []string{
	`{app="a"} | drop L`,
	`{app="a"} | keep L`,
	`sum(count_over_time({app="a"} | drop L [1m]))`,
	`{app="a"} | keep L | json`,
}

// selectorListBodies are what a stream selector holds in its braces: Loki accepts one trailing comma there.
var selectorListBodies = []string{
	`app="a"`, `app="a",`, `app="a", `, `app="a",,`, `app="a",,b="c"`, `,app="a"`, `,`, ``, `app="a" , b="c" ,`,
}

// labelListGrid is every position with every body, and the pair and selector variants.
func labelListGrid() []string {
	var grid []string
	for _, p := range labelListPositions {
		for _, b := range labelListBodies {
			grid = append(grid, strings.ReplaceAll(p, "L", b))
		}
	}
	for _, p := range labelListFreePositions {
		for _, b := range labelListFreeBodies {
			grid = append(grid, strings.ReplaceAll(p, "L", b))
		}
	}
	for _, p := range labelListPairPositions {
		for _, b := range labelListPairBodies {
			grid = append(grid, strings.ReplaceAll(p, "L", b))
		}
	}
	for _, p := range labelListMatcherPositions {
		for _, b := range labelListMatcherBodies {
			grid = append(grid, strings.ReplaceAll(p, "L", b))
		}
	}
	for _, b := range selectorListBodies {
		grid = append(grid, "{"+b+"}", "sum(count_over_time({"+b+"}[1m]))")
	}
	// Loki's lexer reads white space that is no ASCII white space as a name of its own, so a comma followed by one
	// is not a trailing comma to it; the scanner skips such a character.
	for _, w := range []string{"\u00a0", "\u2003", "\u3000", "\u0085"} {
		grid = append(grid,
			"sum by (a,"+w+") (count_over_time({app=\"a\"}[1m]))",
			"sum by (a, "+w+") (count_over_time({app=\"a\"}[1m]))",
			"sum (count_over_time({app=\"a\"}[1m])) without (a,"+w+")",
			"sum(count_over_time({app=\"a\"}[1m])) / on(a,"+w+") sum(count_over_time({app=\"a\"}[1m]))",
			"{app=\"a\"} | drop a,"+w, "{app=\"a\"} | keep a,"+w+" | json", "{app=\"a\"} | drop a, b,"+w)
	}
	return grid
}

const labelListLokiAnswers = "testdata/label-list-commas-loki.jsonl"

// A label list is accepted where Loki accepts it and rejected with Loki's text where it rejects it: a trailing
// comma, a doubled or a leading one and a name that is missing are Loki's 400 in every list (grouping, matching,
// group_left and group_right, json, logfmt, drop, keep, and label_format), the empty list and the
// whitespace around names stay accepted where Loki accepts them. Loki's answers are recorded with the Loki oracle
// check command (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/label-list-trailing-comma
func TestLabelListCommasMatchLoki(t *testing.T) {
	requireLokiAnswers(t, "check", labelListGrid(), labelListLokiAnswers, nil)
}
