package logql

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// whitespaceAnswers holds what Loki's parser answers for every query of whitespaceQueries.
const whitespaceAnswers = "testdata/whitespace_loki.jsonl.gz"

// whitespaceAnswer is one recorded answer of the Loki oracle.
type whitespaceAnswer struct {
	Q      string `json:"q"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// whitespaceMark stands for the character under test in a whitespaceTemplates entry.
const whitespaceMark = "¤"

// whitespaceChars is every character the grid puts in every position: all the white space unicode.IsSpace reads
// (Loki's lexer skips only the first four of those it knows: space, tab, line feed and carriage return), the other
// control bytes, and the Unicode separators and zero-width characters that are not white space to Go. NUL is
// left out: Loki's lexer reports it as an error of its own, (lokiLexerQueries holds its grid).
func whitespaceChars() []string {
	var out []string
	for c := 1; c <= 0x20; c++ {
		out = append(out, string(rune(c)))
	}
	out = append(out, "\x7f")
	for _, r := range []rune{0x85, 0xa0, 0x1680, 0x180e} {
		out = append(out, string(r))
	}
	for r := rune(0x2000); r <= 0x200f; r++ {
		out = append(out, string(r))
	}
	for _, r := range []rune{0x2028, 0x2029, 0x202f, 0x205f, 0x2060, 0x3000, 0xfeff} {
		out = append(out, string(r))
	}
	return out
}

// whitespaceTemplates are the queries, with whitespaceMark in one position each: between the selector and a
// pipe, after a pipe, inside a selector, between the tokens of a metric query, in front of and behind the whole
// query, around the operators and the arguments of the stages.
func whitespaceTemplates() []string {
	const m = whitespaceMark
	return []string{
		// the whole query
		m + `{app="a"}`, `{app="a"}` + m, m + `{app="a"}` + m, m + `rate({app="a"}[5m])` + m, m, m + m,
		m + `| json`, m + `{app="a"} | json`,
		// a stream selector
		`{` + m + `app="a"}`, `{app` + m + `="a"}`, `{app=` + m + `"a"}`, `{app="a"` + m + `}`, `{app="a"` + m + `,b="c"}`,
		`{app="a",` + m + `b="c"}`, `{app="a",b` + m + `!="c"}`, `{app="a",b!=` + m + `"c"}`, `{app=~` + m + `"a"}`,
		`{app!~` + m + `"a",b="c"}`, `{` + m + `}`, `{a` + m + `b="c"}`, `{` + m + `app="a"}` + m,
		// a line filter
		`{app="a"}` + m + `|= "x"`, `{app="a"} ` + m + `|= "x"`, `{app="a"}` + m + ` |= "x"`, `{app="a"} |` + m + `= "x"`,
		`{app="a"} |=` + m + `"x"`, `{app="a"} |= ` + m + `"x"`, `{app="a"} |= "x"` + m, `{app="a"} |= "x"` + m + `!= "y"`,
		`{app="a"} |~` + m + `"x"`, `{app="a"} !=` + m + `"x"`, `{app="a"} !~` + m + `"x"`, `{app="a"} |>` + m + `"<_>"`,
		`{app="a"} !>` + m + `"<_>"`, `{app="a"} |= ip` + m + `("1.2.3.4")`,
		`{app="a"} |= ip(` + m + `"1.2.3.4")`, `{app="a"} |= ip("1.2.3.4"` + m + `)`, `{app="a"} |= ` + m + `ip("1.2.3.4")`,
		`{app="a"} |= "x" or` + m + `"y"`, `{app="a"} |= "x"` + m + `or "y"`,
		// a stage
		`{app="a"} |` + m + `json`, `{app="a"} |` + m + ` json`, `{app="a"} | ` + m + `json`, `{app="a"}` + m + `| json`,
		`{app="a"} | json` + m, `{app="a"} | json` + m + `| logfmt`, `{app="a"} |= "x" |` + m + `logfmt`,
		`{app="a"} |` + m + `drop x`, `{app="a"} | drop` + m + `x`, `{app="a"} | drop x` + m + `,y`, `{app="a"} | drop x,` + m + `y`,
		`{app="a"} | keep` + m + `x`, `{app="a"} | keep ` + m + `x`, `{app="a"} | drop x` + m,
		`{app="a"} | json` + m + `a="b"`, `{app="a"} | json ` + m + `a="b"`, `{app="a"} | json a` + m + `="b"`,
		`{app="a"} | json a=` + m + `"b"`, `{app="a"} | json a="b"` + m + `, c="d"`, `{app="a"} | json a="b",` + m + `c="d"`,
		`{app="a"} | logfmt` + m + `--strict`, `{app="a"} | logfmt ` + m + `--strict`, `{app="a"} | logfmt --strict` + m + `--keep-empty`,
		`{app="a"} | logfmt a` + m + `="b"`, `{app="a"} | logfmt a=` + m + `"b"`,
		`{app="a"} | unpack` + m + `| json`, `{app="a"} | unpack` + m, `{app="a"} |` + m + `unpack`,
		`{app="a"} | decolorize` + m, `{app="a"} |` + m + `decolorize`, `{app="a"} | decolorize` + m + `| json`,
		`{app="a"} | pattern` + m + `"<a>"`, `{app="a"} | pattern ` + m + `"<a>"`, `{app="a"} | pattern "<a>"` + m,
		`{app="a"} | regexp` + m + `"(?P<a>.)"`, `{app="a"} | regexp "(?P<a>.)"` + m,
		`{app="a"} | line_format` + m + `"x"`, `{app="a"} | line_format ` + m + `"x"`, `{app="a"} | line_format "x"` + m,
		`{app="a"} | line_format "x"` + m + `| json`, `{app="a"} | line_format` + m + "`x`",
		`{app="a"} | label_format a` + m + `=b`, `{app="a"} | label_format a=` + m + `b`, `{app="a"} | label_format a=b` + m + `,c=d`,
		`{app="a"} | label_format a=b,` + m + `c=d`, `{app="a"} | label_format a=` + m + `"{{.x}}"`, `{app="a"} | label_format` + m + `a=b`,
		`{app="a"} | distinct` + m + `a`, `{app="a"} | distinct a,` + m + `b`, `{app="a"} | distinct a` + m + `,b`,
		// a label filter
		`{app="a"} | a` + m + `="b"`, `{app="a"} | a=` + m + `"b"`, `{app="a"} | a="b"` + m + `and c="d"`, `{app="a"} | a="b" and` + m + `c="d"`,
		`{app="a"} | a="b"` + m + `or c="d"`, `{app="a"} | a="b" or` + m + `c="d"`, `{app="a"} | a="b",` + m + `c="d"`,
		`{app="a"} | a="b"` + m + `, c="d"`, `{app="a"} | ` + m + `a="b"`, `{app="a"} | a="b"` + m, `{app="a"} | a` + m + `>5`, `{app="a"} | a >` + m + `5`,
		`{app="a"} | a` + m + `== 5`, `{app="a"} | a ==` + m + `5`, `{app="a"} | a>=` + m + `5`, `{app="a"} | a` + m + `!= "x"`,
		`{app="a"} | a !=` + m + `"x"`, `{app="a"} | a` + m + `=~ "x"`, `{app="a"} | a =~` + m + `"x"`, `{app="a"} | a` + m + `!~ "x"`,
		`{app="a"} | (` + m + `a="b")`, `{app="a"} | (a="b"` + m + `)`, `{app="a"} | (a="b")` + m, `{app="a"} | a=ip` + m + `("1.2.3.4")`,
		`{app="a"} | a=ip(` + m + `"1.2.3.4")`, `{app="a"} | a=` + m + `ip("1.2.3.4")`, `{app="a"} | a>5` + m + `kB`, `{app="a"} | a>5` + m + `m`,
		`{app="a"} | a>1` + m + `.5`, `{app="a"} | a>-` + m + `5`, `{app="a"} | a>` + m + `-5`, `{app="a"} | a>+` + m + `5`,
		`{app="a"} | a="b"` + m + `| json`,
		// a metric query
		`rate` + m + `({app="a"}[5m])`, `rate(` + m + `{app="a"}[5m])`, `rate({app="a"}` + m + `[5m])`, `rate({app="a"}[` + m + `5m])`,
		`rate({app="a"}[5` + m + `m])`, `rate({app="a"}[5m` + m + `])`, `rate({app="a"}[5m]` + m + `)`, `rate({app="a"}[5m])` + m,
		`rate({app="a"}[5m]` + m + `offset 5m)`, `rate({app="a"}[5m] offset` + m + `5m)`, `rate({app="a"}[5m] offset 5m` + m + `)`,
		`rate({app="a"}[5m] offset -` + m + `5m)`, `rate({app="a"}[5m]` + m + `@ 100)`, `rate({app="a"}[5m] @` + m + `100)`,
		`rate({app="a"}[5m] @ end` + m + `())`, `rate({app="a"}[5m] @` + m + `end())`,
		`rate({app="a"}[5m]) ` + m, `rate({app="a"} | json [5m]` + m + `)`, `rate({app="a"} | json` + m + `[5m])`,
		`rate({app="a"} |= "x"` + m + `[5m])`, `rate({app="a"}` + m + `|= "x" [5m])`, `rate({app="a"} |` + m + `json [5m])`,
		`count_over_time` + m + `({app="a"}[5m])`, `count_over_time({app="a"}[5m])` + m, `bytes_over_time` + m + `({app="a"}[5m])`,
		`sum` + m + `(rate({app="a"}[5m]))`, `sum(` + m + `rate({app="a"}[5m]))`, `sum(rate({app="a"}[5m])` + m + `)`,
		`sum(rate({app="a"}[5m]))` + m, `sum` + m + `by (app) (rate({app="a"}[5m]))`, `sum by` + m + `(app) (rate({app="a"}[5m]))`,
		`sum by (` + m + `app) (rate({app="a"}[5m]))`, `sum by (app` + m + `) (rate({app="a"}[5m]))`, `sum by (app,` + m + `b) (rate({app="a"}[5m]))`,
		`sum by (app` + m + `,b) (rate({app="a"}[5m]))`, `sum by (app)` + m + `(rate({app="a"}[5m]))`, `sum by (app) (` + m + `rate({app="a"}[5m]))`,
		`sum(rate({app="a"}[5m]))` + m + `by (app)`, `sum(rate({app="a"}[5m])) by` + m + `(app)`, `sum(rate({app="a"}[5m])) by (app)` + m,
		`sum(rate({app="a"}[5m])) without` + m + `(app)`, `sum without` + m + `(app) (rate({app="a"}[5m]))`,
		`topk(` + m + `5, rate({app="a"}[5m]))`, `topk(5` + m + `, rate({app="a"}[5m]))`, `topk(5,` + m + `rate({app="a"}[5m]))`,
		`topk` + m + `(5, rate({app="a"}[5m]))`, `topk(5, rate({app="a"}[5m])` + m + `)`, `topk by` + m + `(app) (5, rate({app="a"}[5m]))`,
		`quantile_over_time(0.9` + m + `, {app="a"} | unwrap x [5m])`, `quantile_over_time(` + m + `0.9, {app="a"} | unwrap x [5m])`,
		`quantile_over_time(0.9,` + m + `{app="a"} | unwrap x [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap` + m + `x [5m])`,
		`quantile_over_time(0.9, {app="a"} | unwrap x` + m + `[5m])`, `quantile_over_time(0.9, {app="a"} | unwrap bytes(` + m + `x) [5m])`,
		`quantile_over_time(0.9, {app="a"} | unwrap bytes` + m + `(x) [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap x` + m + `| a="b" [5m])`,
		`quantile_over_time(0.9, {app="a"} | unwrap x | a="b"` + m + `[5m])`, `quantile_over_time(0.9, {app="a"} |` + m + `unwrap x [5m])`,
		`sum_over_time({app="a"} | unwrap x [5m]) by` + m + `(app)`, `sum_over_time({app="a"} | unwrap x [5m]` + m + `) by (app)`,
		`sum_over_time({app="a"} | unwrap duration(` + m + `x) [5m])`, `sum_over_time({app="a"} | unwrap duration_seconds` + m + `(x) [5m])`,
		`rate({app="a"}[5m]) ` + m + `> 1`, `rate({app="a"}[5m]) >` + m + `1`, `rate({app="a"}[5m]) > bool` + m + `1`, `rate({app="a"}[5m]) >` + m + `bool 1`,
		`rate({app="a"}[5m]) > ` + m + `bool 1`, `rate({app="a"}[5m])` + m + `+` + m + `rate({app="a"}[5m])`, `1` + m + `+` + m + `1`, `1+` + m + `1`,
		`1 +` + m + `1`, `1 ` + m + `+ 1`, `(` + m + `1+1)`, `(1+1` + m + `)`, `(1+1)` + m, `vector(` + m + `1)`, `vector(1` + m + `)`, `vector` + m + `(1)`,
		`1 ` + m + `and 1`, `1 and` + m + `1`, `rate({app="a"}[5m]) or` + m + `rate({app="b"}[5m])`, `rate({app="a"}[5m])` + m + `unless rate({app="b"}[5m])`,
		`sum(rate({a="b"}[5m])) / on` + m + `(a) sum(rate({a="b"}[5m]))`, `sum(rate({a="b"}[5m])) / on(` + m + `a) sum(rate({a="b"}[5m]))`,
		`sum(rate({a="b"}[5m])) / ignoring` + m + `(a) sum(rate({a="b"}[5m]))`, `sum(rate({a="b"}[5m])) / on(a) group_left` + m + `(c) sum(rate({a="b"}[5m]))`,
		`sum(rate({a="b"}[5m])) / on(a)` + m + `group_left sum(rate({a="b"}[5m]))`, `sum(rate({a="b"}[5m])) / on(a) group_left(` + m + `c) sum(rate({a="b"}[5m]))`,
		`label_replace(` + m + `rate({app="a"}[5m]), "a", "b", "c", "d")`, `label_replace(rate({app="a"}[5m]),` + m + `"a", "b", "c", "d")`,
		`label_replace(rate({app="a"}[5m]), "a", "b", "c", "d"` + m + `)`, `label_replace` + m + `(rate({app="a"}[5m]), "a", "b", "c", "d")`,
		`count_values(` + m + `"x", rate({app="a"}[5m]))`, `count_values("x"` + m + `, rate({app="a"}[5m]))`,
		`-` + m + `1`, `-` + m + `rate({app="a"}[5m])`, `!` + m + `1`,
		`absent_over_time({app="a"}[5m])` + m, `absent_over_time` + m + `({app="a"}[5m])`,
		`sort(` + m + `rate({app="a"}[5m]))`, `sort_desc` + m + `(rate({app="a"}[5m]))`,
		`rate({app="a"}[5m]) ` + m + `# c`,
		// inside strings, backquotes and comments: part of the value
		`{app="a` + m + `b"}`, `{app=` + "`a" + m + "b`}", `{` + m + `="a"}`, `{app="a"} |= "a` + m + `b"`, `{app="a"} |= ` + "`a" + m + "b`",
		`{app="a"} |= "` + m + `"`, `{app="a"} |= ` + "`" + m + "`", `{app="a"} |~ "a` + m + `b"`, `{app="a"} | line_format "a` + m + `b"`,
		`{app="a"} | line_format ` + "`a" + m + "b`", `{app="a"} | a="b` + m + `c"`, `{app="a"} | a=` + "`b" + m + "c`", `{app="a"} | label_format a="b` + m + `c"`,
		`{app="a"} | json a="b` + m + `c"`, `{app="a"} | pattern "<a>` + m + `<b>"`, `{app="a"} | regexp "(?P<a>.)` + m + `"`,
		`{app="a"} | drop a="b` + m + `c"`, `{app="a"} | keep a="b` + m + `c"`, `{app="a"} | decolorize | a="b` + m + `c"`,
		`{app="a"} # c` + m + `d` + "\n" + `|= "x"`, `{app="a"} #` + m + "\n" + `|= "x"`, `{app="a"} # c` + m, `{app="a"} #` + m + `
|= "x"`, `{app="a"} # c` + "\n" + m + `|= "x"`, `{app="a"} # c` + m + "\n" + m + `|= "x"`, `# c` + m + "\n" + `{app="a"}`,
		`{app="a"} |= "x" # c` + m + "\n", `{app="a"} |= "x" #` + m, `rate({app="a"}[5m]) # c` + m + `d`, `rate({app="a"}[5m] # c` + m + "\n" + `)`,
		`sum(rate({app="a"}[5m])) # c` + m + "\n" + `by (app)`,
		// a string followed by or preceded by the character
		`{app="a"}` + m + `|= "x"` + m + `|= "y"`, `{app="a"} |= "x"` + m + `| json` + m + `| a="b"` + m,
		`{app="a"} |= "a` + m + `b"` + m + `| json`, `{app="a"} | a="b` + m + `"` + m + `| json`,
	}
}

// whitespaceQueries is the grid: every template with every character in its position, plus the same
// characters in pairs and in runs with the four characters Loki skips.
func whitespaceQueries() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	chars := whitespaceChars()
	for _, tpl := range whitespaceTemplates() {
		for _, c := range chars {
			add(strings.ReplaceAll(tpl, whitespaceMark, c))
		}
	}
	for _, q := range whitespacePlainQueries() {
		add(q)
	}
	// A run mixing Loki's four with another character, and two other characters side by side, before a pipe
	// and a token.
	for _, tpl := range []string{`{app="a"}` + whitespaceMark + `| json`, `{app="a"} |` + whitespaceMark + `json`, `{app="a"}` + whitespaceMark + `|= "x"`, `rate(` + whitespaceMark + `{app="a"}[5m])`, whitespaceMark + `{app="a"}`, `{app="a"}` + whitespaceMark} {
		for _, c := range []string{"\v", "\f", "\u00a0", "\u0085", "\u2003", "\x1f", "\x7f", "\ufeff", "\u3000"} {
			for _, run := range []string{" " + c, c + " ", "\t" + c, c + "\n", " " + c + " ", "\r\n" + c, c + c, c + "\f", "\v" + c, c + "\u00a0"} {
				add(strings.ReplaceAll(tpl, whitespaceMark, run))
			}
		}
	}
	return out
}

// whitespacePlainQueries are the plain-text shapes whose acceptance or error text this change brings to Loki's: a
// blank or a line break where Loki's lexer reads none (inside a [range], in a double-quoted string), the lists of
// names (by, without, on, ignoring, group_left, group_right) and the errors of a selector, a call, a stage and an
// unwrap, which the white space grid reaches through the character that is not white space.
func whitespacePlainQueries() []string {
	const r = `rate({a="b"}[5m])`
	var out []string
	for _, list := range []string{"a b", "a,", ",a", "a,,b", "a 1", `a "x"`, "1", `"a"`, "a b c", "a,b", "", "a"} {
		out = append(out, "sum by ("+list+") ("+r+")", "sum without ("+list+") ("+r+")", "sum ("+r+") by ("+list+")", r+" / on("+list+") "+r,
			r+" / ignoring("+list+") "+r, r+" / on(a) group_left("+list+") "+r, r+" / on(a) group_right("+list+") "+r, "topk by ("+list+") (5, "+r+")")
	}
	for _, rng := range []string{" 5m", "5m ", " 5m ", "\t5m", "5m\n", "\n5m", "5m\r", "5 m", "5m ]", "", " ", "5", "m", "5x", "5M", "5m5", "1h30m", "30m:5m", "5m#c", "$__auto", "-5m", "+5m", "1y2w3d4h5m6s7ms", "5ms "} {
		out = append(out, "rate({a=\"b\"}["+rng+"])", "sum(rate({a=\"b\"}["+rng+"]))", `count_over_time({a="b"} | json | x="y" [`+rng+`])`, `quantile_over_time(0.9, {a="b"} | unwrap x [`+rng+`])`, `rate({a="b"}[5m] offset 5m) / rate({a="b"}[`+rng+`])`)
	}
	for _, str := range []string{"a\nb", "\n", "a\\nb", "a\rb", "a\tb"} {
		out = append(out, `{a="`+str+`"}`, `{a="b"} |= "`+str+`"`, `{a="b"} | x="`+str+`"`, `{a="b"} | json | x="`+str+`" | json`, `{a="b"} | line_format "`+str+`"`,
			`{a="b"} | label_format x="`+str+`"`, `{a="b"} | json x="`+str+`"`, `{a="b"} | regexp "`+str+`"`, `{a="b"} | drop x="`+str+`"`, `{a="b"} | x="`+str+`" and y="z"`)
	}
	out = append(out,
		`{app}`, `{app="a",}`, `{app="a",,}`, `{"a"="b"}`, `{1}`, `{`, `{app="a"`, `{,}`, `{,app="a"}`, `{app 1}`, `{app="a",1}`, `{app="a",b}`, `{app="a" "b"}`, `{app="a" b="c"}`, `{a b="c"}`, `{a b}`, `{app!"a"}`, `{app!}`, `{a="b",c d="e"}`, `{app="a" |`, `{app="a"] `,
		`{app="a"} | json a=`, `{app="a"} | json a=1`, `{app="a"} | json a=z`, `{app="a"} | json a="b",1`, `{app="a"} | label_format 1`, `{app="a"} | label_format a 1`, `{app="a"} | label_format a=1`, `{app="a"} | label_format a=b,1`,
		`{app="a"} | line_format`, `{app="a"} | line_format 1`, `{app="a"} | line_format z`, `{app="a"} | regexp z`, `{app="a"} | pattern 1`, `{app="a"} | pattern`, `{app="a"} | json foo=bar`, `{app="a"} | keep level=`, `{app="a"} | drop level!=~"x"`,
		`-z`, `-`, `- 1`, `-1`, `-\n1`, `- rate({a="b"}[5m])`, `!1`, `! 1`,
		`rate({app="a"})`, `rate({app="a"} x [5m])`, `rate({app="a"} | json)`, `rate({app="a"} | json x [5m])`, `rate({app="a"} 5 [5m])`, `rate({app="a"} |= "x" y [5m])`, `rate(({app="a"}) y [5m])`, `rate({app="a"}[5m] y)`, `rate({app="a"}[5m]) y`,
		`quantile_over_time(0.9, {app="a"} | unwrap x)`, `quantile_over_time(0.9, {app="a"} | unwrap x y [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap x 5 [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap bytes(x) y [5m])`,
		`quantile_over_time(0.9, {app="a"} | unwrap bytes(x y) [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap bytes(x [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap bytes x) [5m])`,
		`rate x({app="a"}[5m])`, `sum x(rate({app="a"}[5m]))`, `sum by x (rate({app="a"}[5m]))`, `sum by (a) x (rate({app="a"}[5m]))`, `sum(rate({app="a"}[5m]) x)`, `topk(5, rate({app="a"}[5m]) x)`, `vector x (1)`, `label_replace x (rate({app="a"}[5m]), "a", "b", "c", "d")`,
		`{app="a"} | a=ip x("1.2.3.4")`, `{app="a"} | a=ip`, `{app="a"} | a=ip(`, `{app="a"} |= ip x("1.2.3.4")`, `{app="a"} | a=ip ("1.2.3.4")`, `{app="a"} | a=ip5`,
		`{app="a"} | logfmt x --strict`, `{app="a"} | logfmt --strict x`, `{app="a"} | json --strict`, `{app="a"} | logfmt --strict --keep-empty`, `{app="a"} | logfmt --strict x --keep-empty`,
		`rate({a="b"}[5m]) / on a rate({a="b"}[5m])`, `rate({a="b"}[5m]) / on rate({a="b"}[5m])`, `rate({a="b"}[5m]) / ignoring a rate({a="b"}[5m])`,
		"", " ", "  ", "\n", "\t", "\r", "\n\n", "#c", "#c\n", " #c", " | json", " |", "|", "\n| json", "\t|= \"x\"",
		// A decimal digit that is not ASCII (U+0663, U+07C6) starts no number for Loki's lexer: it is an identifier of
		// its own at the start of a token, a letter of a name after one.
		`{a="b"} | x ߆`, `{a="b"} | x=߆`, `{a="b"} | x ٣`, `{a="b"} ٣`, `{a="b"} | x>٣`, `rate({a="b"}[٣m])`, `rate({a="b"}[5٣])`, `{a="b"} | x>5٣`, `{a="b"} | a٣="c"`, `{a٣="b"}`, `߆`, `{a="b"} |= ٣`,
		`{a="b"} | x ٣ | y ٣`, `{a="b"} | x ٣ and y=٣`,
	)
	return out
}

// recordWhitespaceAnswers runs Loki's parser (the oracle's check command) over the queries and writes its answers.
func recordWhitespaceAnswers(t *testing.T, queries []string) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(filepath.Join("..", "..", "test", "oracle", "loki"), "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for i, q := range queries {
		if err := enc.Encode(map[string]any{"id": fmt.Sprint(i), "kind": "client", "q": q, "ours": true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	r, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var b strings.Builder
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	i := 0
	for sc.Scan() {
		var v struct {
			Accept bool   `json:"accept"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		line, err := json.Marshal(whitespaceAnswer{Q: queries[i], Accept: v.Accept, Error: v.Error})
		if err != nil {
			t.Fatal(err)
		}
		b.Write(line)
		b.WriteByte('\n')
		i++
	}
	if i != len(queries) {
		t.Fatalf("Loki answered %d of %d queries", i, len(queries))
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	var zipped bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&zipped, gzip.BestCompression)
	if _, err := zw.Write([]byte(b.String())); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(whitespaceAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readWhitespaceAnswers(t *testing.T) []whitespaceAnswer {
	t.Helper()
	f, err := os.Open(whitespaceAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []whitespaceAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a whitespaceAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	return answers
}

// White space is read as Loki reads it: only a space, a tab, a line feed and a carriage return separate tokens;
// every other character Go calls white space (a vertical tab, a form feed, U+0085, U+00A0, U+2003, ...) and every
// other control character is a character Loki's lexer rejects, with Loki's error text, wherever it stands outside
// a string, a backquoted string and a comment. Loki's answers are recorded with the Loki oracle
// (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/logql-loki-whitespace
func TestWhitespaceMatchesLoki(t *testing.T) {
	queries := whitespaceQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordWhitespaceAnswers(t, queries)
	}
	answers := readWhitespaceAnswers(t)
	if len(answers) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(answers), len(queries))
	}
	var accepted, rejected, diffs, nameLimits, bomLimits int
	for i, a := range answers {
		if a.Q != queries[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, a.Q, queries[i])
		}
		got := ValidateLogQL(a.Q)
		if a.Accept {
			accepted++
		} else {
			rejected++
		}
		switch {
		case strings.Contains(got, "unsupported name character"):
			// Loki reads a character that starts no token as an identifier of its own, which its grammar accepts
			// as a name; the translation to LogsQL does not carry such a name, so the proxy turns the query away
			// (a documented limit). Where Loki rejects the query for a reason of its own, the proxy's message is
			// the one difference.
			nameLimits++
		case got != "" && strings.HasPrefix(a.Q, "\ufeff"):
			// Loki's lexer drops a byte order mark at the start of the query (and counts it in its columns); the
			// proxy reads it as a name character and turns the query away (a documented limit).
			bomLimits++
		case (got == "") != a.Accept || !a.Accept && got != a.Error:
			diffs++
			if diffs <= 40 {
				t.Errorf("%q\n  proxy: %q\n  Loki:  accept=%v %q", a.Q, got, a.Accept, a.Error)
			}
		}
	}
	t.Logf("%d queries: Loki accepts %d and rejects %d; %d differ, %d are the name limit and %d the byte order mark limit", len(answers), accepted, rejected, diffs, nameLimits, bomLimits)
	if diffs > 40 {
		t.Errorf("%d queries differ from Loki", diffs)
	}
	if nameLimits > 1000 || bomLimits > 40 {
		t.Errorf("%d queries are turned away for a name character and %d for a byte order mark: the limits grew", nameLimits, bomLimits)
	}
	if accepted < 1000 || rejected < 1000 {
		t.Fatalf("grid too small: %d accepted, %d rejected", accepted, rejected)
	}
}

// A decimal digit that is not ASCII after a label name once made the parser loop without end: the scanner read it as
// the start of a number and made no progress. Loki's lexer reads it as an identifier of its own.
//
// conformance: semantics/logql-loki-whitespace
func TestNonASCIIDigitDoesNotStopTheParser(t *testing.T) {
	for _, q := range []string{"{a=\"b\"} | x \u07c6", "{a=\"b\"} | x=\u0663", "{}|\xec:A\xb9\u07c6", "{a=\"b\"} | x \u0663 | y \u0663", "rate({a=\"b\"}[\u0663m])"} {
		done := make(chan string, 1)
		go func() { done <- ValidateLogQL(q) }()
		select {
		case msg := <-done:
			if msg == "" {
				t.Errorf("%q is accepted", q)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%q: the parser does not return", q)
		}
	}
}
