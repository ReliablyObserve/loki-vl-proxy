package querycorpus_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
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

// translatorMisreadsParenthesizedMetric reports a metric query wrapped whole in parentheses, `(rate({a="x"}[5m]))`:
// the string translator reads it as a line filter and writes the query text as a phrase
// (semantics/translator-reads-parenthesized-metric-as-text), while Loki and the query plan read the metric inside.
// Generated corpus rows (fuzz mutations) bring new spellings of it, so it is recognised by content, not by row id.
func translatorMisreadsParenthesizedMetric(q string) bool {
	q = strings.TrimSpace(q)
	if len(q) < 2 || q[0] != '(' {
		return false
	}
	depth := 0
	for i := 0; i < len(q); i++ {
		switch c := q[i]; c {
		case '"':
			for i++; i < len(q) && q[i] != '"'; i++ {
				if q[i] == '\\' {
					i++
				}
			}
		case '`':
			for i++; i < len(q) && q[i] != '`'; i++ {
			}
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 && i != len(q)-1 {
				return false // the first parenthesis closes before the end: `(a) / (b)`
			}
		}
	}
	inner := strings.TrimLeft(strings.TrimSpace(q[1:len(q)-1]), "(")
	return depth == 0 && inner != "" && inner[0] != '{'
}

// conformance: semantics/translator-reads-parenthesized-metric-as-text
func TestTranslatorMisreadsParenthesizedMetric(t *testing.T) {
	for q, want := range map[string]bool{
		`(rate({app="a"}[5m]))`:                         true,
		"(rate({app=\"a\"}[800ms])\n)":                  true,
		` ( count_over_time({app="a"} |= "x" [5m]) ) `:  true,
		`((bytes_rate({app="a"}[5m] offset 10m)))`:      true,
		`(count_over_time({app="a"} |= ")" [5m]))`:      true,
		"(count_over_time({app=\"a\"} |= `(` [5m]))":    true,
		`rate({app="a"}[5m])`:                           false,
		`(rate({app="a"}[5m])) / (rate({app="b"}[5m]))`: false,
		`{app="a"} |= "(rate({app=\"a\"}[5m]))"`:        false,
		`({app="a"})`:                                   false, // a log query
		`(rate({app="a"}[5m])`:                          false, // unbalanced
	} {
		if got := translatorMisreadsParenthesizedMetric(q); got != want {
			t.Errorf("%q: got %v, want %v", q, got, want)
		}
		if !want {
			continue
		}
		// The defect recorded: the translator writes the whole query as a phrase.
		if text, err := translator.TranslateLogQL(q); err != nil || !strings.HasPrefix(text, `"`) {
			t.Errorf("%q: the string translator wrote %q (%v), not a phrase: the case may be fixed", q, text, err)
		}
	}
}
