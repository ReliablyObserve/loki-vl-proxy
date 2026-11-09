package querycorpus_test

import (
	"regexp"
	"strings"
	"testing"
)

// labelFilterValue is a label filter's value: a name, a comparison and a string literal (backquoted, or
// double-quoted with escapes).
var labelFilterValue = regexp.MustCompile("[A-Za-z_][A-Za-z0-9_.]*\\s*(?:!=|=~|!~|==|=)\\s*(`[^`]*`|\"(?:[^\"\\\\]|\\\\.)*\")")

// translatorMisreadsQuotedText reports a query the string translator reads wrongly whatever its spelling: a label
// filter value after the stream selector holds a word the translator takes as syntax and so unpacks the line
// (semantics/translator-reads-quoted-text-as-syntax). The query plan reads the value as text, as Loki does, so the
// two differ and the translator is the wrong one. Generated corpus rows (fuzz seeds, crafted values) bring new
// spellings of it, so it is recognised by content, not by row id.
func translatorMisreadsQuotedText(q string) bool {
	rest := q
	if i := strings.IndexByte(q, '}'); i >= 0 {
		rest = q[i+1:]
	}
	for _, m := range labelFilterValue.FindAllStringSubmatch(rest, -1) {
		if strings.Contains(m[1], "detected_level") || strings.Contains(m[1], "service_name") {
			return true
		}
	}
	return false
}

func TestTranslatorMisreadsQuotedText(t *testing.T) {
	for q, want := range map[string]bool{
		"{app=\"x\"} | s=`a detected_level b`":                                true,
		" \t{app=\"x\"} | s=`a detected_level b`\n ":                          true,
		`{a="x"} | json | level="detected_level"`:                             true,
		"{a=\"x\"} | s=`\x01service_name\x00`":                                true,
		`{a="x"} | s!~"service_name.*"`:                                       true,
		`{service_name="api"} | s="x"`:                                        false, // the selector, not a filter value
		`{a="x"} |= "detected_level"`:                                         false, // a line filter
		`label_replace(rate({a="x"}[5m]), "d", "$1", "service_name", "(.*)")`: false,
		`{a="x"} | detected_level="error"`:                                    false, // the name, not the value
	} {
		if got := translatorMisreadsQuotedText(q); got != want {
			t.Errorf("%q: got %v, want %v", q, got, want)
		}
	}
}
