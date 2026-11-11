package logql

import (
	"strings"
	"testing"
)

// keywordLabelWords are the words Loki's lexer returns as keyword tokens of their own and the parser does not read
// as an identifier where a label name stands.
var keywordLabelWords = []string{
	"of", "bool", "offset", "unwrap", "json", "regexp", "logfmt", "unpack", "pattern", "label_format", "line_format",
	"decolorize", "drop", "keep", "variants",
}

// keywordLabelPositions are the places a label name stands in LogQL, with W for the name.
var keywordLabelPositions = []string{
	`{W="a"}`,
	`{app="a",W="b"}`,
	`{app="a"} | json W`,
	`{app="a"} | json a, W`,
	`{app="a"} | json W="a.b"`,
	`{app="a"} | json a="b", W="c"`,
	`{app="a"} | logfmt W`,
	`{app="a"} | logfmt a, W`,
	`{app="a"} | drop W`,
	`{app="a"} | drop a, W`,
	`{app="a"} | drop W="x"`,
	`{app="a"} | keep W`,
	`{app="a"} | keep a, W=~"x"`,
	`{app="a"} | label_format W=x`,
	`{app="a"} | label_format a=b, W=c`,
	`{app="a"} | label_format x=W`,
	`sum by (W) (count_over_time({app="a"}[1m]))`,
	`sum by (a, W) (count_over_time({app="a"}[1m]))`,
	`sum without (W) (count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(W) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(a, W) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / ignoring(W) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(a) group_left(W) sum(count_over_time({app="a"}[1m]))`,
	`sum(count_over_time({app="a"}[1m])) / on(a) group_right(W) sum(count_over_time({app="a"}[1m]))`,
	`sum(sum_over_time({app="a"} | json | unwrap W [1m]))`,
	`sum(sum_over_time({app="a"} | json | unwrap bytes(W) [1m]))`,
}

// keywordLabelGrid is every position of keywordLabelPositions with each keyword word in lower case, upper case and
// capitalised (Loki's lexer reads a keyword in any case), and with the names Loki does read as labels: the
// words that are keywords only before a parenthesis, and not and in, which are keywords of LogsQL and not of LogQL.
func keywordLabelGrid() []string {
	var grid []string
	add := func(w string) {
		for _, p := range keywordLabelPositions {
			grid = append(grid, strings.ReplaceAll(p, "W", w))
		}
	}
	for _, w := range keywordLabelWords {
		add(w)
		add(strings.ToUpper(w))
		add(strings.ToUpper(w[:1]) + w[1:])
	}
	for _, w := range []string{"not", "in", "ip", "bytes", "duration", "duration_seconds", "rate", "sum", "label_replace", "vector", "status"} {
		add(w)
	}
	return grid
}

const keywordLabelLokiAnswers = "testdata/keyword-label-names-loki.jsonl"

// A label cannot be called after one of the words Loki's lexer reads as a keyword (of, bool, offset, unwrap, json,
// regexp, logfmt, unpack, pattern, label_format, line_format, decolorize, drop, keep, variants, in any case): in a
// stream selector, a grouping or matching list, a drop or keep list, a json or logfmt extraction, a label_format
// or an unwrap Loki's grammar wants an identifier and the query is its 400 with its text. The proxy answers the
// same, where it used to translate the query and write the name in another case. Loki's answers are recorded with
// the Loki oracle check command (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/loki-keyword-label-names
func TestKeywordLabelNamesMatchLoki(t *testing.T) {
	requireLokiAnswers(t, "check", keywordLabelGrid(), keywordLabelLokiAnswers, nil)
}
