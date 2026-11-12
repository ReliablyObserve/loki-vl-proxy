package querycorpus_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
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

// translatorReadsStageMarker reports a query whose label filter value the string translator reads as syntax: it
// brackets the names a `<name>_extracted` filter reads with a \x02 byte in the text of that filter's stage and later
// takes the first pair of \x02 bytes of every stage that holds none for such brackets, cutting the bytes between them
// out of the stage. A label filter value that holds two \x02 bytes (`o="\x02\x00\x02"`, `o="a\x02b\x02c"`), a single one on a name with an underscore (the translator writes such a name in three
// spellings, the value after each: `a_b="\x02\x00b"`), or two
// values of one stage that hold one each, in a stage with no `_extracted` filter of its own, in a query with a json or
// logfmt parser and an `_extracted` filter elsewhere, is cut that way: the translator writes the filter on the value without those bytes (`o:=""`), copies the
// stream where the bytes were instead of where the `_extracted` filter reads it, and may write a query VictoriaLogs
// rejects. Loki and the query plan read the value as written (semantics/translator-reads-marker-bytes-in-quoted-value).
// internal/queryplan/lower holds the same function for its fuzz targets. The value is the decoded one, so the query may spell it with an escape, a raw byte or backquotes. A single \x02
// byte, and the \x00 and \x01 bytes the translator also uses, are read as text on every shape the grid found.
func translatorReadsStageMarker(q string) bool {
	expr, err := logql.Parse(q)
	if err != nil {
		return false
	}
	lq := innerLogQuery(expr)
	if lq == nil {
		return false
	}
	var stages [][]*logql.LabelFilterExpr
	extracted, parsed := false, false
	for _, st := range lq.Pipeline {
		if _, ok := st.(*logql.ParserStage); ok {
			parsed = true
		}
		lf, ok := st.(*logql.LabelFilterStage)
		if !ok || lf.Expr == nil {
			continue
		}
		var leaves []*logql.LabelFilterExpr
		collectLabelFilterLeaves(lf.Expr, &leaves)
		stages = append(stages, leaves)
		for _, l := range leaves {
			extracted = extracted || strings.HasSuffix(l.Name, "_extracted")
		}
	}
	if !extracted || !parsed {
		return false
	}
	for _, leaves := range stages {
		marks, own := 0, false
		for _, l := range leaves {
			own = own || strings.HasSuffix(l.Name, "_extracted")
			if l.Kind == logql.LabelFilterValueString {
				// The translator writes a name with an underscore in its dotted and dashed spellings too, the
				// value after each: one byte in such a filter's value is three in the stage.
				copies := 1
				if strings.Contains(l.Name, "_") {
					copies = 3
				}
				marks += copies * strings.Count(l.Value, "\x02")
			}
		}
		if !own && marks >= 2 {
			return true
		}
	}
	return false
}

// innerLogQuery is the log query a range aggregation reads, under any vector aggregations around it, or expr when it
// is a log query: the stages of a metric query are written by the same code that writes a log query's.
func innerLogQuery(expr logql.Expr) *logql.LogQuery {
	for {
		switch e := expr.(type) {
		case *logql.LogQuery:
			return e
		case *logql.VectorAggregation:
			expr = e.Inner
		case *logql.RangeAggregation:
			expr = e.Inner
		default:
			return nil
		}
	}
}

func collectLabelFilterLeaves(e *logql.LabelFilterExpr, out *[]*logql.LabelFilterExpr) {
	if e == nil {
		return
	}
	if e.IsLeaf() {
		*out = append(*out, e)
		return
	}
	collectLabelFilterLeaves(e.Left, out)
	collectLabelFilterLeaves(e.Right, out)
}

// conformance: architecture-syntax-package-layering
func TestTranslatorReadsStageMarker(t *testing.T) {
	const ext = ` | json | K_extracted!~"p|q"`
	for q, want := range map[string]bool{
		`{app="a"} | o="\x02\x00\x02"` + ext:                           true,
		`{app="a"} | o="\x02\x02"` + ext:                               true,
		`{app="a"} | o="a\x02b\x02c" | json | K_extracted="x"`:         true,
		`{app="a"} | o=` + "`\x02\x00\x02`" + ext:                      true, // a raw byte in backquotes
		`{app="a"} | o="\u0002a\U00000002"` + ext:                      true, // the same bytes spelled otherwise
		`{app="a"} | json | K_extracted="x" | o="\x02\x00\x02"`:        true, // behind the filter that reads the name
		`{app="a"} | logfmt | K_extracted="x" | o="\x02" and z="\x02"`: true, // one byte in each of two values
		`{app="a"} | o="\x02" and z="\x02"` + ext:                      true,
		`{app="a"} | o="\x02"` + ext:                                   false, // one byte
		`{app="a"} | o_p="\x02"` + ext:                                 true,  // one byte on a name with an underscore: three spellings
		`{app="a"} | logfmt | K_extracted="x" | o_p=` + "`\x02\x00b`":  true,
		`{app="a"} | o_p_extracted="\x02"` + ext:                       false, // the stage's own marker comes first
		`{app="a"} | o="\x02" | z="\x02"` + ext:                        false, // one byte in each of two stages
		`{app="a"} | o="\x00\x01\x00"` + ext:                           false, // the other bytes the translator uses
		`{app="a"} | json | o="\x02\x02"`:                              false, // no _extracted filter
		`{app="a"} | o="\x02\x02"`:                                     false, // filter-only
		`{app="a"} | json | K_extracted="x" and o="\x02\x02"`:          false, // the stage's own marker comes first
		`{app="a"} | json | o="\x02\x02" or K_extracted="x"`:           false,
		`{app="a"} |= "\x02\x02" | json | K_extracted="x"`:             false, // a line filter
		`{app="a"} | o="\\x02\\x02"` + ext:                             false, // a backslash and the text x02
		`rate({app="a"} | o="\x02\x02"` + ext + `[5m])`:                true,  // a range aggregation over it
		`sum by (app) (rate({app="a"} | o="\x02\x02"` + ext + `[5m]))`: true,  // and a vector aggregation over that
		`rate({app="a"} | o="\x02"` + ext + `[5m])`:                    false, // one byte
		`vector(1)`:   false, // no log query at all
		`not a query`: false,
	} {
		if got := translatorReadsStageMarker(q); got != want {
			t.Errorf("%q: got %v, want %v", q, got, want)
		}
	}
}
