package lower

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// jsonKeyIsOperator matches a json or logfmt stage that reads a key named like a LogQL keyword (any case):
// `| json x="OR"`, `| json x="BY"`, `| logfmt x="offset"`. The string translator searches the query text for these
// words (and, or, unless, the grouping and vector matching words, offset, bool, and the stage keywords) wherever they
// stand, so it reads the quoted key as syntax.
var jsonKeyIsOperator = regexp.MustCompile("(?i)\\b(?:json|logfmt)\\b[^|]*=\\s*[\"`](?:or|and|unless|by|without|on|ignoring|group_left|group_right|offset|bool|of|unwrap|json|logfmt|regexp|pattern|unpack|label_format|line_format|decolorize|variants|keep|drop)[\"`]")

// translatorMangledByKeywordKey reports a query the string translator cannot write: it splits a label filter at
// the word or and the word and wherever they stand, and a json or logfmt stage that reads a key named like a LogQL
// keyword puts that word bare into its text, so a filter beside it comes out cut (`x_extracted="x"`, a quote without its
// colon, or an unbalanced quote) and VictoriaLogs rejects the query. Loki accepts it, and the lowering writes it
// (semantics/translator-writes-keyword-field-names-bare). The fuzz targets leave such a query out: the translator's
// text for it does not parse, or parses to something else.
func translatorMangledByKeywordKey(q string) bool { return jsonKeyIsOperator.MatchString(q) }

// wordHoldingSyntax matches a word of letters, digits, underscores and dots that holds detected_level or
// service_name inside a longer name (`dtedetected_level`).
var wordHoldingSyntax = regexp.MustCompile(`[A-Za-z0-9_.]*(?:detected_level|service_name)[A-Za-z0-9_.]*`)

// translatorReadsNameAsSyntax reports a query with a label name that holds the word detected_level or service_name
// inside a longer name: the string translator searches the query text for those two words (the text-reading of
// semantics/translator-reads-quoted-text-as-syntax, for a name instead of a value) and unpacks the line for the
// label, where Loki, the query plan and the lowering read a label called dtedetected_level like any other.
func translatorReadsNameAsSyntax(q string) bool {
	for _, w := range wordHoldingSyntax.FindAllString(q, -1) {
		if w != "detected_level" && w != "service_name" {
			return true
		}
	}
	return false
}

// translatorWritesUnicodeAsName reports a LogsQL parser error on the string translator's text that comes from a
// character the scanner cannot read as part of a name: a label filter value that holds a character such as U+070F
// (written in the query as a \u escape) is read by the translator as a stream label name and written bare
// (`copy "__lxp0_\u070f" as __lxp_\u070f`), where the lowering holds only the value and writes the filter right
// (semantics/translator-reads-quoted-text-as-syntax). err is the parser's error on the translator's text.
func translatorWritesUnicodeAsName(err error) bool {
	msg := err.Error()
	_, after, found := strings.Cut(msg, "unexpected character")
	if !found {
		return false
	}
	// The error names the character: a non-ASCII one is printed raw or as a \u escape.
	if strings.Contains(after, `\u`) || strings.Contains(after, `\U`) {
		return true
	}
	for _, r := range after {
		if r > unicode.MaxASCII {
			return true
		}
	}
	return false
}

func TestTranslatorReadsNameAsSyntax(t *testing.T) {
	for q, want := range map[string]bool{
		`{app="a"} | keep x | dtedetected_level="1"`: true,
		`{app="a"} | my_service_name="1"`:            true,
		`{app="a"} | detected_level="1"`:             false,
		`{service_name="a"} | x="1"`:                 false,
		`{app="a"} | x="1"`:                          false,
	} {
		if got := translatorReadsNameAsSyntax(q); got != want {
			t.Errorf("%s: got %v, want %v", q, got, want)
		}
	}
}

func TestTranslatorMangledByKeywordKey(t *testing.T) {
	for q, want := range map[string]bool{
		`{app="a"} | json x="OR" | x>=1`:     true,
		"{app=\"a\"} | json x=`or`, y=\"b\"": true,
		`{app="a"} | logfmt x="And"`:         true,
		`{app="a"} | json x="k" | y="or"`:    false,
		`{app="a"} |= "or" | json x="k"`:     false,
		`{app="a"} | json x="order"`:         false,
		`{app="a"} | A00="" | json A00="BY" | (x_extracted="x", A00!=ip("10.0.0.0/8")) or x_extracted="x"`: true,
		`{app="a"} | logfmt x="offset"`:          true,
		`{app="a"} | json x="Group_Left", y="b"`: true,
		"{app=\"a\"} | json x=`unwrap`":          true,
		`{app="a"} | json x="bypass"`:            false,
		`{app="a"} | json x="k" | y="by"`:        false,
	} {
		if got := translatorMangledByKeywordKey(q); got != want {
			t.Errorf("%s: got %v, want %v", q, got, want)
		}
	}
}

func TestTranslatorWritesUnicodeAsName(t *testing.T) {
	if !translatorWritesUnicodeAsName(errors.New("logsql: unexpected token \"unexpected character: \\u070f\" after query")) {
		t.Error("the scanner's error on a character is not recognised")
	}
	if !translatorWritesUnicodeAsName(errors.New("logsql: unexpected token \"unexpected character: \u00e9\" after query")) {
		t.Error("a raw non-ASCII character is not recognised")
	}
	if translatorWritesUnicodeAsName(errors.New("logsql: expected RParen, got EOF")) ||
		translatorWritesUnicodeAsName(errors.New("logsql: unexpected token \"unexpected character: @\" after query")) {
		t.Error("another parser error, or an ASCII character, is recognised")
	}
}

func TestQuoteKeywordFieldText(t *testing.T) {
	for in, want := range map[string]string{
		`app:="a"`:                           `app:="a"`,
		`not:="a"`:                           `"not":="a"`,
		`app:="a" in:="x"`:                   `app:="a" "in":="x"`,
		`app:="a" (a:="1" or:="x")`:          `app:="a" (a:="1" "or":="x")`,
		`app:="a" -NOT:*`:                    `app:="a" -"NOT":*`,
		`app:="a" | filter Options:>5`:       `app:="a" | filter "Options":>5`,
		`x:contains_all:="a"`:                `x:contains_all:="a"`,
		`app:="not:" | copy "in:" as x`:      `app:="not:" | copy "in:" as x`,
		"app:=`in:x` in:=\"b\"":              "app:=`in:x` \"in\":=\"b\"",
		`app:="a\"in:" or:=1`:                `app:="a\"in:" "or":=1`,
		`app:="a" | delete not`:              `app:="a" | delete not`,
		`app:="a" | fields _time, not, in`:   `app:="a" | fields _time, not, in`,
		`app:="a" i:="x" range:>5 offset:=1`: `app:="a" i:="x" range:>5 offset:=1`,
		`x.not:="a" not_x:="b" xnot:="c"`:    `x.not:="a" not_x:="b" xnot:="c"`,
	} {
		if got := QuoteKeywordFieldText(in); got != want {
			t.Errorf("%s:\n got %s\nwant %s", in, got, want)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = QuoteKeywordFieldText(`app:="a" (b:="1" or c:~"x.+") | unpack_json`) }); allocs != 0 {
		t.Errorf("a text with no keyword name allocates %v", allocs)
	}
}
