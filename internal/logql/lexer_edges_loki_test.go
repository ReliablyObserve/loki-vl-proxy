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
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// lexerEdgeAnswer is what Loki's parser says about one query of a lexer-edge grid: whether it accepts it, its error
// text, and (for a query with a reference spelling) whether it reads the same expression as the reference.
type lexerEdgeAnswer struct {
	Q      string `json:"q"`
	Ref    string `json:"ref,omitempty"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
	Same   bool   `json:"same,omitempty"`
}

// recordLexerEdgeAnswers asks the Loki oracle (check command) about every query, with refs[i] as its printed form
// when it is set, and writes the answers to path, gzipped.
func recordLexerEdgeAnswers(t *testing.T, path string, queries, refs []string) {
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
		row := map[string]any{"id": fmt.Sprint(i), "kind": "client", "q": q, "ours": true}
		if refs[i] != "" {
			row["printed"] = refs[i]
		}
		if err := enc.Encode(row); err != nil {
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
	var b bytes.Buffer
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	i := 0
	for sc.Scan() {
		var v struct {
			Accept      bool   `json:"accept"`
			Error       string `json:"error"`
			SameMeaning *bool  `json:"same_meaning"`
		}
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		line, err := json.Marshal(lexerEdgeAnswer{Q: queries[i], Ref: refs[i], Accept: v.Accept, Error: v.Error, Same: v.SameMeaning != nil && *v.SameMeaning})
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
	var zipped bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&zipped, gzip.BestCompression)
	if _, err := zw.Write(b.Bytes()); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(path, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// readLexerEdgeAnswers reads the answers recorded at path and checks they are for queries and refs, in order.
func readLexerEdgeAnswers(t *testing.T, path string, queries, refs []string) []lexerEdgeAnswer {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers []lexerEdgeAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a lexerEdgeAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		answers = append(answers, a)
	}
	if len(answers) != len(queries) {
		t.Fatalf("%s: %d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", path, len(answers), len(queries))
	}
	for i, a := range answers {
		if a.Q != queries[i] || a.Ref != refs[i] {
			t.Fatalf("%s: answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", path, i, a.Q, queries[i])
		}
	}
	return answers
}

const nulByteAnswers = "testdata/nul-byte-loki.jsonl.gz"

// nulMark stands for the NUL byte in a nulTemplates entry.
const nulMark = "¤"

// nulTemplates are queries with nulMark inside a quoted string, a backquoted string or a comment, wherever a log
// query, a metric query or a wrapper of one holds a string, with the errors a string may also have (an escape the
// lexer rejects, a line break, a missing closing quote) before and after the byte, and next to a token as controls.
var nulTemplates = []string{
	// a selector's value
	`{app="a¤b"}`, `{app="¤"}`, `{app="a¤"}`, `{app=~"a¤"}`, `{app!="a¤"}`, "{app=`a¤b`}", `{app="a", b="¤"}`,
	// a line filter
	`{app="a"} |= "a¤b"`, `{app="a"} |= "¤"`, `{app="a"} != "a¤"`, `{app="a"} |~ "a¤"`, `{app="a"} !~ "a¤"`, "{app=\"a\"} |= `a¤b`",
	`{app="a"} |> "<_>¤"`, `{app="a"} |= "a" or "b¤"`, `{app="a"} |= ip("::¤!")`, `{app="a"} != ip("1.2.3.4¤")`,
	// a label filter (its own lexer)
	`{app="a"} | x="a¤b"`, `{app="a"} | x=~"a¤"`, "{app=\"a\"} | x=`a¤`", `{app="a"} | x="a" and y="b¤"`, `{app="a"} | x="a" or y="b¤"`,
	`{app="a"} | (x="a¤")`, `{app="a"} | json | x="a¤"`, `{app="a"} | x = ip("1.2.3.4¤")`, `{app="a"} | x="a" # c¤`,
	"{app=\"a\"} | x=\"a\" # c¤\n and y=\"b\"", `{app="a"} | x > 1 and y="¤"`,
	// a stage's string
	`{app="a"} | json x="a¤"`, `{app="a"} | logfmt x="a¤"`, `{app="a"} | regexp "(?P<a>¤)"`, "{app=\"a\"} | regexp `(?P<a>¤)`",
	`{app="a"} | pattern "<a>¤"`, `{app="a"} | line_format "a¤"`, "{app=\"a\"} | line_format `{{.a}}¤`", `{app="a"} | label_format a="¤"`,
	`{app="a"} | label_format a="{{.b}}¤"`, `{app="a"} | drop a="¤"`, `{app="a"} | keep a="¤"`, `{app="a"} | decolorize | x="¤"`,
	// escapes, line breaks and a missing closing quote around the byte
	`{app="a"} |= "a\¤"`, `{app="a"} |= "\q¤"`, `{app="a"} |= "¤\q"`, `{app="a"} |= "a\n¤"`, "{app=\"a\"} |= \"a¤\nb\"",
	"{app=\"a\"} |= \"a\n¤b\"", `{app="a"} |= "a¤`, `{app="a"} |= "a¤"¤`, `{app="a"} |= "a" |= "b¤" |= "\q"`, `{app="a"} |= "\q" |= "b¤"`,
	"{app=\"a\"} |= `a\n¤b`", "{app=\"a\"} |= `a¤", `{app="a"} | x="a¤`, `{app="a"} | x="\q¤"`, "{app=\"a\"} | x=\"a¤\nb\"",
	// a comment
	`{app="a"} # a¤b`, "{app=\"a\"} # a¤\n|= \"b\"", "# ¤\n{app=\"a\"}", "{app=\"a\"}\n# a¤", "{app=\"a\"} |= \"b\"\n\n# ¤",
	"{app=\"a\"} # a\n# ¤", "{app=\"a\"} # \"¤\n|= \"b\"", "rate({app=\"a\"}[1m]) # ¤", "rate({app=\"a\"} # ¤\n[1m])",
	"sum by (a) # ¤\n(rate({app=\"a\"}[1m]))",
	// the escapes Go's text scanner reads digit by digit, where the byte stops the escape, and the ones whose value it
	// checks only at the end of the string
	`{app="a"} |= "\x¤"`, `{app="a"} |= "\x4¤"`, `{app="a"} |= "\0¤"`, `{app="a"} |= "\u00¤"`, `{app="a"} |= "\U0001¤"`,
	`{app="a"} |= "\'¤"`, `{app="a"} |= "\777¤"`, `{app="a"} |= "\U00110000¤"`, `{app="a"} |= "a\"¤"`, `{app="a"} | x="a¤\q"`,
	`{app="a"} | x="\x¤"`, "{app=\"a\"} |= `a` |= \"b\n¤\"", "{app=\"a\"} | x=\"a\n¤b\"", `{app="a"} |= "a¤\`,
	// next to a token (the lexer reads the character after a token before it hands the token over); a NUL at the very
	// start is left out: Loki answers it with the position of the query its pooled parser read before
	`{app="a"} | json¤`, `{app="a"}¤`, `{app="a"} ¤|= "b"`, `{app="a"} |= "b"¤`, `rate({app="a"}[1m])¤`,
	// metric queries and the wrappers Loki keeps an error through
	`rate({app="a"} |= "a¤b" [1m])`, `count_over_time({app="a"} | x="¤" [1m])`, `sum by (a) (count_over_time({app="a"} |= "a¤b" [1m]))`,
	`topk(1, rate({app="a"} |= "a¤b" [1m]))`, `bottomk(1, rate({app="a"} |= "a¤b" [1m]))`, `sort(rate({app="a"} |= "a¤b" [1m]))`,
	`sort_desc(rate({app="a"} |= "a¤b" [1m]))`, `label_replace(rate({app="a"}[1m]), "a", "¤", "c", "d")`,
	`label_replace(rate({app="a"}[1m]), "a", "b", "c", "(¤)")`, `label_replace(rate({app="a"} |= "a¤b" [1m]), "a", "b", "c", "d")`,
	`sum(rate({app="a"} |= "a¤b" [1m])) / sum(rate({app="a"}[1m]))`, `sum(rate({app="a"}[1m])) / sum(rate({app="a"} |= "a¤b" [1m]))`,
	`rate({app="a"} |= "a¤b" [1m]) > 1`, `1 + rate({app="a"} |= "a¤b" [1m])`, `sum(rate({app="a"} |= "a¤b" [1m])) or vector(0)`,
	`sum(rate({app="a"} |= "a¤b" [1m])) and on (a) sum(rate({app="a"}[1m]))`, `rate({app="a"} |= "a¤b" [1m] offset 5m)`,
	`max_over_time({app="a"} | unwrap a | a="¤" [1m])`, `sum_over_time({app="a"} | json | unwrap a | x="a¤" [1m])`,
	`quantile_over_time(0.5, {app="a"} | json x="a¤" | unwrap a [1m]) by (a)`, `avg(stddev_over_time({app="a"} | logfmt | x="¤" | unwrap a [1m]))`,
	`sum by (a) (avg_over_time({app="a"} |= "a¤b" | json | unwrap a [1m]))`, `rate(({app="a"} |= "a¤b")[1m])`,
	`count_over_time({app="a"}[1m] |= "a¤b")`,
}

// nulGrid is every template with the NUL byte for the mark.
func nulGrid() (queries, refs []string) {
	for _, tpl := range nulTemplates {
		queries = append(queries, strings.ReplaceAll(tpl, nulMark, "\x00"))
		refs = append(refs, "")
	}
	return queries, refs
}

// A NUL byte anywhere in the query text is Loki's lexer error, `invalid character NUL`: inside a quoted or a
// backquoted string (at the string, line and column of its opening quote, ahead of a line break, a missing closing
// quote or a rejected escape after it), inside a comment (at the comment, on line 0), and next to a token. Loki
// rejects the byte through every wrapper of a log query (a range function, an aggregation, a ranking, a sort,
// label_replace, a binary operation, an offset), and the proxy answers its error, with Loki's text, at the
// validation of every route, before any cache, plan or VictoriaLogs call. Loki's answers are recorded with the Loki
// oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/raw-nul-in-string-literal-accepted
func TestNULByteMatchesLoki(t *testing.T) {
	queries, refs := nulGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordLexerEdgeAnswers(t, nulByteAnswers, queries, refs)
	}
	answers := readLexerEdgeAnswers(t, nulByteAnswers, queries, refs)
	for _, a := range answers {
		if a.Accept {
			t.Errorf("%q: Loki accepts a NUL byte; the grid holds queries Loki rejects", a.Q)
			continue
		}
		// The handlers read the query with its keywords in lower case, its comments and its logfmt flags removed
		// (lowercaseQueryKeywords): the validation sees that text, and must answer Loki's error for it too.
		for _, q := range []string{a.Q, LowercaseKeywords(StripParserFlags(StripComments(a.Q)))} {
			if msg := ValidateLogQL(q); msg != a.Error {
				t.Errorf("%q:\n proxy %q\n Loki  %q", q, msg, a.Error)
			}
		}
	}
	t.Logf("%d queries with a NUL byte: the proxy answers Loki's error for each", len(answers))
}

const keywordFoldAnswers = "testdata/keyword-fold-runes-loki.jsonl.gz"

// keywordMark stands for the keyword in a keywordTemplates entry.
const keywordMark = "@"

// keywordTemplates are a query for every word Loki's lexer reads as a keyword (lokiWords), with keywordMark where the
// keyword stands: a function, an aggregation, a grouping, a binary operator or modifier, a stage, a conversion.
var keywordTemplates = []struct{ word, tpl string }{
	{"by", `sum @ (a) (rate({app="a"}[1m]))`}, {"without", `sum @ (a) (rate({app="a"}[1m]))`},
	{"bool", `sum(rate({app="a"}[1m])) > @ 1`}, {"offset", `rate({app="a"}[1m] @ 5m)`},
	{"on", `sum by (a) (rate({app="a"}[1m])) / @ (a) sum by (a) (rate({app="b"}[1m]))`},
	{"ignoring", `sum by (a) (rate({app="a"}[1m])) / @ (a) sum by (a) (rate({app="b"}[1m]))`},
	{"group_left", `sum by (a, b) (rate({app="a"}[1m])) / on (a) @ (b) sum by (a) (rate({app="b"}[1m]))`},
	{"group_right", `sum by (a) (rate({app="a"}[1m])) / on (a) @ (b) sum by (a, b) (rate({app="b"}[1m]))`},
	{"unless", `sum by (a) (rate({app="a"}[1m])) @ sum by (a) (rate({app="b"}[1m]))`},
	{"and", `sum by (a) (rate({app="a"}[1m])) @ sum by (a) (rate({app="b"}[1m]))`},
	{"or", `sum by (a) (rate({app="a"}[1m])) @ sum by (a) (rate({app="b"}[1m]))`},
	{"and", `{app="a"} | x="1" @ y="2"`}, {"or", `{app="a"} | x="1" @ y="2"`},
	{"unwrap", `sum_over_time({app="a"} | @ a [1m])`}, {"unwrap", `sum_over_time({app="a"} | json | @ a [1m])`},
	{"json", `{app="a"} | @`}, {"json", `count_over_time({app="a"} | @ [1m])`}, {"logfmt", `{app="a"} | @`},
	{"logfmt", `sum by (a) (count_over_time({app="a"} | @ [1m]))`}, {"unpack", `{app="a"} | @`}, {"decolorize", `{app="a"} | @`},
	{"regexp", "{app=\"a\"} | @ `(?P<a>x)`"}, {"pattern", `{app="a"} | @ "<a> x"`}, {"label_format", `{app="a"} | @ a=b`},
	{"line_format", `{app="a"} | @ "x"`}, {"drop", `{app="a"} | @ a`}, {"keep", `{app="a"} | @ a`},
	{"keep", `sum by (a) (count_over_time({app="a"} | json | @ a [1m]))`},
	{"ip", `{app="a"} |= @("1.2.3.4")`}, {"ip", `{app="a"} | x = @("1.2.3.4")`},
	{"label_replace", `@(rate({app="a"}[1m]), "a", "b", "c", "d")`}, {"vector", `@(1)`},
	{"vector", `sum(rate({app="a"}[1m])) or @(0)`}, {"bytes", `sum_over_time({app="a"} | unwrap @(a) [1m])`},
	{"duration", `sum_over_time({app="a"} | unwrap @(a) [1m])`}, {"duration_seconds", `sum_over_time({app="a"} | unwrap @(a) [1m])`},
	{"rate", `@({app="a"}[1m])`}, {"count_over_time", `@({app="a"}[1m])`}, {"bytes_rate", `@({app="a"}[1m])`},
	{"bytes_over_time", `@({app="a"}[1m])`}, {"absent_over_time", `@({app="a"}[1m])`}, {"rate_counter", `@({app="a"} | unwrap a [1m])`},
	{"sum_over_time", `@({app="a"} | unwrap a [1m])`}, {"avg_over_time", `@({app="a"} | unwrap a [1m])`},
	{"min_over_time", `@({app="a"} | unwrap a [1m])`}, {"max_over_time", `@({app="a"} | unwrap a [1m])`},
	{"stdvar_over_time", `@({app="a"} | unwrap a [1m])`}, {"stddev_over_time", `@({app="a"} | unwrap a [1m])`},
	{"first_over_time", `@({app="a"} | unwrap a [1m])`}, {"last_over_time", `@({app="a"} | unwrap a [1m])`},
	{"quantile_over_time", `@(0.5, {app="a"} | unwrap a [1m])`},
	{"avg_over_time", `sum by (a) (@({app="a"} | json | unwrap a [1m]))`}, {"max_over_time", `max(@({app="a"} | logfmt | unwrap a [1m]))`},
	{"stddev_over_time", `sum(@({app="a"} | json | unwrap a [1m]))`},
	{"sum", `@(rate({app="a"}[1m]))`}, {"sum", `@ by (a) (rate({app="a"}[1m]))`}, {"avg", `@(rate({app="a"}[1m]))`},
	{"min", `@(rate({app="a"}[1m]))`}, {"max", `@(rate({app="a"}[1m]))`}, {"count", `@(rate({app="a"}[1m]))`},
	{"stddev", `@(rate({app="a"}[1m]))`}, {"stdvar", `@(rate({app="a"}[1m]))`}, {"sort", `@(rate({app="a"}[1m]))`},
	{"sort_desc", `@(rate({app="a"}[1m]))`}, {"topk", `@(1, rate({app="a"}[1m]))`}, {"bottomk", `@(1, rate({app="a"}[1m]))`},
	{"topk", `@(1, sum by (a) (rate({app="a"} | json [1m])))`}, {"bottomk", `@ by (a) (1, rate({app="a"}[1m]))`},
	{"approx_topk", `@(1, rate({app="a"}[1m]))`},
}

// foldRunes are the letters outside ASCII written for an ASCII keyword letter: the two that lower case into one (the
// Kelvin sign U+212A for k, U+0130 for i), which Loki's lexer reads as the keyword, and U+017F (ſ), which only
// case-folds to s and is no keyword letter to it.
var foldRunes = []struct {
	ascii byte
	r     string
}{{'k', "\u212A"}, {'i', "\u0130"}, {'s', "\u017F"}}

// keywordSpellings are the spellings of word the grid holds: each fold rune for every one of its letters, for the
// first one only, and with the other letters in ASCII capitals.
func keywordSpellings(word string) []string {
	var out []string
	for _, f := range foldRunes {
		at := strings.IndexByte(word, f.ascii)
		if at < 0 {
			continue
		}
		out = append(out, strings.ReplaceAll(word, string(f.ascii), f.r), strings.ToUpper(word[:at])+f.r+strings.ToUpper(word[at+1:]))
		if strings.Count(word, string(f.ascii)) > 1 {
			out = append(out, strings.Replace(word, string(f.ascii), f.r, 1))
		}
	}
	return out
}

// nameLetter is a letter Loki's lexer reads as a letter of a name and never folds into an ASCII one: the control the
// grid writes for each U+017F spelling, which Loki and the proxy must read alike.
const nameLetter = "é"

// keywordFoldGrid is every keyword template with each spelling of its keyword, and the same with a trailing `)`
// that makes it a syntax error, each with the lower case spelling as its reference. A spelling with U+017F has a
// twin with nameLetter in its place.
func keywordFoldGrid() (queries, refs []string) {
	seen := map[string]bool{}
	add := func(q, ref string) {
		if !seen[q] {
			seen[q] = true
			queries, refs = append(queries, q), append(refs, ref)
		}
	}
	for _, kt := range keywordTemplates {
		i := strings.Index(kt.tpl, keywordMark)
		ref := kt.tpl[:i] + kt.word + kt.tpl[i+1:]
		for _, sp := range keywordSpellings(kt.word) {
			for _, w := range []string{sp, strings.ReplaceAll(sp, "\u017F", nameLetter)} {
				q := kt.tpl[:i] + w + kt.tpl[i+1:]
				add(q, ref)
				add(q+")", ref+")")
			}
		}
	}
	for _, q := range keywordFoldEdges {
		add(q, "")
	}
	return queries, refs
}

// keywordFoldEdges are the rows the lower casing's fuzz target found, each answered by Loki as its lower case
// spelling: an error column after a fold rune, on a later line or after another non-ASCII letter (counted in
// characters), U+0130 in an ip() a grouped label filter starts with and a keyword inside a range (whose text Loki's
// error quotes). A keyword right after a digit (`0KEEP`, which in lower case would read as the bytes literal 0k) is
// held by the fuzz target's corpus.
var keywordFoldEdges = []string{
	"sum w\u0130thout()(rate(0y", "sum W\u0130THOUT()(rAte(0y", "sum by (é) (rate(0y", "sum by (a)\n(rate(0y", "sum by (é)\n(rate(0y",
	`{A="0"}|(\u0130p(0000000`, `{A="0"}|(IP(0000000`, `{A="0"}| \u0130p(0000000`, `{A="0"}| x="a" or \u0130p("1.2.3.4")`,
	`{A="0"}| (x=\u0130p("1.2.3.4"))`, "rAte({}[GROUP_R\u0130GHT ]0", "rate({}[1m] of\u0130set 5m)",
}

// keywordNameDifferences are the templates where a keyword written with a letter Loki reads as a name letter
// (U+017F, or nameLetter) is a known difference of the proxy's parser with any name in its place, not one of the fold
// runes: a stage that is a bare name alone, which the proxy passes through (docs/logql-parser.md, Not covered), and an
// unwrap of a name followed by a parenthesis, which the proxy reads as a conversion
// (semantics/unwrap-name-before-parenthesis-accepted).
var keywordNameDifferences = map[string]string{
	`{app="a"} | @`:                               "bare name stage",
	`count_over_time({app="a"} | @ [1m])`:         "bare name stage",
	`sum_over_time({app="a"} | unwrap @(a) [1m])`: "unwrap name before parenthesis",
}

// A keyword written with the Kelvin sign (U+212A) for a k or U+0130 for an i (`topK(`, `bottomK(`, `| Keep`,
// `avg_over_tİme(`, `wİthout`, `İp(`) is the keyword to Loki's lexer, which lower cases a word before it looks it up:
// Loki accepts the query and reads the expression of the lower case spelling. A letter that only case-folds to an
// ASCII one (U+017F, ſ, for an s) is not: Loki reads a name there, as it does for any other letter (é). The proxy's
// parser answers as Loki does for every keyword in each spelling, and LowercaseKeywords, which the request path
// applies before any handler, the cache keys and the translation read the query, writes the lower case spelling the
// handlers match. A syntax error after the keyword is Loki's text at Loki's column. Loki's answers are recorded with
// the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/keyword-fold-runes-read-as-keyword, semantics/unwrap-name-before-parenthesis-accepted
func TestKeywordFoldRunesMatchLoki(t *testing.T) {
	queries, refs := keywordFoldGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordLexerEdgeAnswers(t, keywordFoldAnswers, queries, refs)
	}
	answers := readLexerEdgeAnswers(t, keywordFoldAnswers, queries, refs)
	byQuery := make(map[string]lexerEdgeAnswer, len(answers))
	for _, a := range answers {
		byQuery[a.Q] = a
	}
	var accepted, rejected, known int
	for _, a := range answers {
		lowered := LowercaseKeywords(a.Q)
		if utf8.RuneCountInString(lowered) != utf8.RuneCountInString(a.Q) {
			t.Errorf("%q: the lower case spelling %q moves Loki's columns", a.Q, lowered)
		}
		if a.Ref == "" {
			// An edge row: the proxy answers Loki's error for it as written and in its lower case spelling.
			for _, q := range []string{a.Q, lowered} {
				if msg := ValidateLogQL(q); msg != a.Error {
					t.Errorf("%q:\n proxy %q\n Loki  %q", q, msg, a.Error)
				}
			}
			continue
		}
		if nameSpelled := strings.Contains(a.Q, "\u017F") || strings.Contains(a.Q, nameLetter); nameSpelled {
			// A name letter on both sides: the U+017F spelling is answered as its nameLetter twin, by Loki and by the
			// proxy, and differs from Loki only where any name does.
			twin := byQuery[strings.ReplaceAll(a.Q, "\u017F", nameLetter)]
			if twin.Error != a.Error || twin.Accept != a.Accept {
				t.Errorf("%q: Loki answers %q, and %q for %q", a.Q, a.Error, twin.Error, twin.Q)
			}
			if twin.Q != a.Q && lowered != strings.ReplaceAll(LowercaseKeywords(twin.Q), nameLetter, "\u017F") {
				t.Errorf("%q: the request path reads %q, unlike its twin %q", a.Q, lowered, twin.Q)
			}
			if msg := ValidateLogQL(a.Q); msg != a.Error {
				if tpl := keywordTemplateOf(a.Q); keywordNameDifferences[tpl] == "" {
					t.Errorf("%q:\n proxy %q\n Loki  %q", a.Q, msg, a.Error)
				} else if twinMsg := ValidateLogQL(twin.Q); twinMsg != msg {
					t.Errorf("%q: the proxy answers %q, and %q for %q", a.Q, msg, twinMsg, twin.Q)
				}
				known++
			}
			rejected++
			continue
		}
		for _, q := range []string{a.Q, lowered} {
			if msg := ValidateLogQL(q); msg != a.Error {
				t.Errorf("%q:\n proxy %q\n Loki  %q", q, msg, a.Error)
			}
		}
		if !a.Accept {
			rejected++
			continue
		}
		accepted++
		if !a.Same {
			t.Errorf("%q: Loki reads another query than %q", a.Q, a.Ref)
		}
		if lowered != a.Ref {
			t.Errorf("%q: the request path reads %q, not the lower case spelling %q", a.Q, lowered, a.Ref)
		}
		// The request path parses the lower case spelling; an opaque call (approx_topk) keeps the text as written.
		got, err := Parse(lowered)
		want, _ := Parse(a.Ref)
		if err != nil || want == nil || got.String() != want.String() {
			t.Errorf("%q: the proxy reads %v (%v), the lower case spelling %v", a.Q, got, err, want)
		}
	}
	if accepted == 0 || rejected == 0 {
		t.Fatalf("%d accepted, %d rejected: the grid must hold both", accepted, rejected)
	}
	t.Logf("%d spellings: Loki accepts %d (the lower case query) and rejects %d; %d are a name's known difference", len(answers), accepted, rejected, known)
}

// keywordTemplateOf is the keyword template q was written from, with the keyword's place marked, or "".
func keywordTemplateOf(q string) string {
	for _, kt := range keywordTemplates {
		i := strings.Index(kt.tpl, keywordMark)
		for _, sp := range keywordSpellings(kt.word) {
			for _, w := range []string{sp, strings.ReplaceAll(sp, "\u017F", nameLetter)} {
				if body := kt.tpl[:i] + w + kt.tpl[i+1:]; q == body || q == body+")" {
					return kt.tpl
				}
			}
		}
	}
	return ""
}

// FuzzLowercaseKeywords holds the request path's keyword lower casing to the parser: the lower case spelling of any
// query is the same query to the proxy's parser (the same error, at the same line and column, or the same tree), and
// the same number of characters, so every column Loki reports stays where it was. The seeds are the keyword and NUL
// grids.
//
// conformance: semantics/keyword-fold-runes-read-as-keyword, semantics/raw-nul-in-string-literal-accepted
func FuzzLowercaseKeywords(f *testing.F) {
	queries, _ := keywordFoldGrid()
	nul, _ := nulGrid()
	for _, q := range append(queries, nul...) {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 {
			return
		}
		if want := strings.IndexFunc(q, func(r rune) bool { return r >= 'A' && r <= 'Z' || r == '\u212A' || r == '\u0130' }) >= 0; foldsToKeyword(q) != want {
			t.Fatalf("%q: foldsToKeyword answers %v", q, !want)
		}
		lowered := LowercaseKeywords(q)
		if utf8.RuneCountInString(lowered) != utf8.RuneCountInString(q) {
			t.Fatalf("%q: lower case spelling %q has another length", q, lowered)
		}
		if LowercaseKeywords(lowered) != lowered {
			t.Fatalf("%q: lower casing %q again changes it", q, lowered)
		}
		want, wantMsg := ValidateLogQLExpr(q)
		got, gotMsg := ValidateLogQLExpr(lowered)
		if gotMsg != wantMsg {
			t.Fatalf("%q: the parser answers %q, and %q for its lower case spelling %q", q, wantMsg, gotMsg, lowered)
		}
		if want != nil && got != nil && reparseKey(want) != reparseKey(got) {
			t.Fatalf("%+q: the parser reads %v, and %v for its lower case spelling (%+q vs %+q)", q, want, got, reparseKey(want), reparseKey(got))
		}
	})
}

// reparseKey is the printed form of e read again: an opaque call keeps its text as written, so two spellings of
// one call are compared by the tree of their lower case text.
func reparseKey(e Expr) string {
	again, err := Parse(LowercaseKeywords(e.String()))
	if err != nil {
		return e.String()
	}
	return again.String()
}

// foldsToKeyword reads eight bytes at a time: a capital, the Kelvin sign or U+0130 in any position of a word, at
// the end of the text or split across two words is found, and nothing else is.
//
// conformance: semantics/keyword-fold-runes-read-as-keyword
func TestFoldsToKeywordEveryPosition(t *testing.T) {
	for _, mark := range []string{"A", "Z", "Q", "\u212A", "\u0130"} {
		for n := 0; n < 24; n++ {
			for at := 0; at <= n; at++ {
				q := strings.Repeat("a", at) + mark + strings.Repeat("z", n-at)
				if !foldsToKeyword(q) {
					t.Errorf("%q: not found", q)
				}
			}
		}
	}
	for _, q := range []string{"", "sum by (app) (rate({app=\"api\"}[5m]))", "@[`{|~", "é\u017F\u0131\u212B\u0131", "\xe2\x84", "\xc4", "\x7f\x80\xff"} {
		if foldsToKeyword(q) {
			t.Errorf("%q: found a letter that folds to a keyword letter", q)
		}
	}
}

// BenchmarkLowercaseKeywords measures the keyword lower casing every query and query_range request runs before its
// handler: a query in lower case (the common case: one pass over the bytes), one with an ASCII capital, and one with
// a Kelvin sign or U+0130.
func BenchmarkLowercaseKeywords(b *testing.B) {
	for _, q := range []struct{ name, q string }{
		{"lower", `sum by (app) (rate({app="api", env="prod"} |= "error" | json | status >= 500 [5m]))`},
		{"capital", `SUM by (app) (rate({app="api", env="prod"} |= "error" | json | status >= 500 [5m]))`},
		{"kelvin", `top` + "K" + `(5, sum by (app) (rate({app="api", env="prod"} |= "error" | json [5m])))`},
		{"dotted_i", `sum by (app) (avg_over_t` + "İ" + `me({app="api"} | json | unwrap latency [5m]))`},
	} {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = LowercaseKeywords(q.q)
			}
		})
	}
}
