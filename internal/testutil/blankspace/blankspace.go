// Package blankspace is test support: it writes a query with each run of the blanks Loki's lexer skips as the one
// space the string translator reads. Tests of the proxy package use it; nothing in the proxy imports it.
package blankspace

import "strings"

// Collapse is q with every run of the four blanks Loki's lexer skips (a space, a tab, a line feed, a carriage return)
// outside quoted strings and backquoted strings written as a single space: the spelling the string translator reads as
// Loki does (semantics/translator-reads-only-space-between-tokens). The metric flip gate holds the translator, which the
// rollback flag serves, to the query plan's text of that spelling. A comment is not read: the request path strips them
// before any translation (logql.StripComments).
func Collapse(q string) string {
	isBlank := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	var out strings.Builder
	for i := 0; i < len(q); i++ {
		switch c := q[i]; {
		case c == '"' || c == '`':
			j := i + 1
			for j < len(q) && q[j] != c {
				if c == '"' && q[j] == '\\' {
					j++
				}
				j++
			}
			j = min(j, len(q)-1)
			out.WriteString(q[i : j+1])
			i = j
		case isBlank(c):
			for i+1 < len(q) && isBlank(q[i+1]) {
				i++
			}
			out.WriteByte(' ')
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

// Mark stands for the blank under test in a Templates entry.
const Mark = "¤"

// Skipped are the spellings of a blank Loki's lexer skips between tokens: a tab, a line feed, a carriage return, a line
// break of two bytes and a run of mixed blanks (the space itself is the spelling every template is held to).
var Skipped = []string{"\t", "\n", "\r", "\r\n", " \t ", "\n\t"}

// Rejected are the characters Go reads as white space and Loki's lexer does not skip (a vertical tab, a form feed,
// U+0085, U+00A0), and a control byte: Loki rejects a query that has one between two tokens.
var Rejected = []string{"\v", "\f", "\u0085", " ", "\x1f"}

// Templates are valid queries with Mark where a blank may stand after a parser keyword, a stage name, an unwrap
// conversion, a grouping keyword, a binary operator or a function name: log queries and metric queries over every
// parser and stage the translator reads as text. With a space for Mark each is the spelling the translator reads.
func Templates() []string {
	const m = Mark
	return []string{
		// Log queries: a parser and the stages after it.
		`{A="0"} | json` + m + `A0`, `{A="0"} | json` + m + `a="b", c`, `{A="0"} | json a,` + m + `b`, `{A="0"} | json a=` + m + `"b"`,
		`{A="0"} | logfmt` + m + `a`, `{A="0"} | logfmt` + m + `--strict`, `{A="0"} | logfmt --strict` + m + `--keep-empty`,
		`{A="0"} | regexp` + m + "`(?P<a>x)`", `{A="0"} | pattern` + m + `"<a> x"`, `{A="0"} | unpack` + m + `| json`,
		`{A="0"} | json | label_format` + m + `a=b`, `{A="0"} | json | label_format a` + m + `=b`, `{A="0"} | json | label_format a=` + m + `b`,
		`{A="0"} | json | line_format` + m + `"x"`, `{A="0"} | json | drop` + m + `a`, `{A="0"} | json | drop a,` + m + `b`,
		`{A="0"} | json | keep` + m + `a`, `{A="0"} | decolorize` + m + `| json`, `{A="0"} |` + m + `json`, `{A="0"}` + m + `| json`,
		`{A="0"} | json | a` + m + `="b"`, `{A="0"} | json | a=` + m + `"b"`, `{A="0"} |=` + m + `"x"`,
		// A parser after a pipe with no blank around it, a double-quoted regexp and capture names with a leading digit
		// (Loki accepts `(?<0>0)`; found by the log flip gate's fuzz target, FuzzLogFlipGate).
		"{A=\"0\"}|regexp" + m + "`(?<0>0)`", `{A="0"}|pattern` + m + `"<a> x"`, `{A="0"} | regexp` + m + `"(?P<0a>x)(?P<b>y)"`,
		// Metric queries: the same parsers, stages and unwrap.
		`count_over_time({A="0"} | json` + m + `A0 [1m])`, `count_over_time({A="0"} | json` + m + `a="b" [1m])`,
		`count_over_time({A="0"} | logfmt` + m + `a [1m])`, `count_over_time({A="0"} | logfmt` + m + `--strict [1m])`,
		`count_over_time({A="0"} | regexp` + m + "`(?P<a>x)` [1m])", `count_over_time({A="0"} | pattern` + m + `"<a> x" [1m])`,
		"count_over_time({A=\"0\"}|regexp" + m + "`(?<0>0)` [1m])", `count_over_time({A="0"} | unpack` + m + `[1m])`, `count_over_time({A="0"} | json | label_format` + m + `a=b [1m])`,
		`count_over_time({A="0"} | json | line_format` + m + `"x" [1m])`, `count_over_time({A="0"} | json | drop` + m + `a [1m])`,
		`count_over_time({A="0"} | json | keep` + m + `a [1m])`, `count_over_time({A="0"} | json |` + m + `a="b" [1m])`,
		`count_over_time({A="0"} | json | a` + m + `="b" [1m])`, `count_over_time({A="0"} |=` + m + `"x" [1m])`,
		`first_over_time({A="0"} | json` + m + `A0 | unwrap A0 [1m])`, `sum_over_time({A="0"} | json | unwrap` + m + `a [1m])`,
		`sum_over_time({A="0"} | json | unwrap` + m + `duration(a) [1m])`, `sum_over_time({A="0"} | json | unwrap bytes` + m + `(a) [1m])`,
		`sum_over_time({A="0"} | json | unwrap duration_seconds(` + m + `a) [1m])`, `sum_over_time({A="0"} | json | unwrap a` + m + `[1m])`,
		`sum_over_time({A="0"} | json | unwrap a |` + m + `a="b" [1m])`, `quantile_over_time(0.9,` + m + `{A="0"} | json | unwrap a [1m])`,
		`avg_over_time({A="0"} | json | unwrap a [1m]` + m + `) by (a)`, `max_over_time({A="0"} | unwrap` + m + `A [1m])`,
		`max_over_time({A="0"}| unwrap` + m + `A [1m])`,
		// Aggregations, grouping clauses, offsets and binary operations.
		`sum` + m + `by (a) (rate({A="0"}[1m]))`, `sum by` + m + `(a) (rate({A="0"}[1m]))`, `sum(rate({A="0"}[1m])) by` + m + `(a)`,
		`sum without` + m + `(a) (rate({A="0"}[1m]))`, `sum` + m + `(rate({A="0"}[1m]))`, `rate({A="0"}[1m]` + m + `offset 5m)`,
		`rate({A="0"}[1m] offset` + m + `5m)`, `sum(rate({A="0"}[1m])) /` + m + `sum(rate({A="0"}[1m]))`,
		`sum(rate({A="0"}[1m]))` + m + `/ sum(rate({A="0"}[1m]))`, `sum(rate({A="0"}[1m])) / on` + m + `(a) sum(rate({A="0"}[1m]))`,
		`sum(rate({A="0"}[1m])) / ignoring` + m + `(a) sum(rate({A="0"}[1m]))`, `sum(rate({A="0"}[1m])) > bool` + m + `1`,
		`sum(rate({A="0"}[1m])) / on(a) group_left` + m + `(b) sum(rate({A="0"}[1m]))`, `topk` + m + `(1, rate({A="0"}[1m]))`,
		`topk(1,` + m + `rate({A="0"}[1m]))`, `rate({A="0"}[1m]) +` + m + `1`,
		`label_replace(rate({A="0"}[1m]),` + m + `"a", "b", "c", "d")`,
	}
}

// UnwrapSpaced is Collapse(q) with a space written between a pipe and an unwrap right after it (`|unwrap n` is
// `| unwrap n`): the spelling the string translator, which looks for `| unwrap `, reads as Loki does. The query plan
// serves both (semantics/translator-reads-only-space-between-tokens).
func UnwrapSpaced(q string) string {
	q = Collapse(q)
	if !strings.Contains(q, "|unwrap") {
		return q
	}
	var out strings.Builder
	for i := 0; i < len(q); i++ {
		switch c := q[i]; {
		case c == '"' || c == '`':
			j := i + 1
			for j < len(q) && q[j] != c {
				if c == '"' && q[j] == '\\' {
					j++
				}
				j++
			}
			j = min(j, len(q)-1)
			out.WriteString(q[i : j+1])
			i = j
		case c == '|' && strings.HasPrefix(q[i+1:], "unwrap"):
			out.WriteString("| ")
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}
