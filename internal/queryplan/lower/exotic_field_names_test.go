package lower

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// exoticFieldNames are label names Loki accepts and VictoriaLogs' lexer does not join into a word: punctuation, a
// fullwidth mark, a symbol, a combining mark, a superscript, a roman numeral, a mathematical symbol, an emoji, a
// private-use character and a zero-width space. Bare, VictoriaLogs rejects the filter that names one.
var exoticFieldNames = []string{"、", ";", "@", "³", "Ⅰ", "𝛁", "😀", "ा", "\u200b", "·", "☃", "»", "\U000f0000", "゠", "⃝"}

// wordFieldNames are names VictoriaLogs reads bare: letters of any script, digits, and the underscore.
var wordFieldNames = []string{"é", "ж", "日本", "x1", "_x", "a_b", "Ω", "٣"}

// The lowering quotes a label name VictoriaLogs cannot read bare wherever a filter names a field, and the text the
// string translator wrote for the same query, made right (QuoteKeywordFieldText), is the lowering's text byte for
// byte. The proxy's LogsQL parser rejects the bare spelling and reads the quoted one as the field.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestPlanQuotesExoticFieldNames(t *testing.T) {
	forms := []string{
		`{a="b"} | x = "y" and N = "z"`,
		`{a="b"} | (N = "z")`,
		`{a="b"} | x = "y" or N > 5`,
		`{a="b"} | x = "y" and N =~ "z.*"`,
		`{a="b"} | x = "y" and N != ""`,
		`{a="b"} | x = "y" and N = ip("10.0.0.1")`,
		`{a="b"} | x = "y" and N < 5m`,
		`{a="b"} | x = "y" and not (N = "z" or N = "w")`,
		`{a="b"} | json | x = "y" and N = "z"`,
		`{a="b"} | logfmt | (N = "z")`,
		`{a="b"} | json | x = "y" and N != "z"`,
		`rate({a="b"} | x = "y" and N = "z" [5m])`,
		`sum by (x) (count_over_time({a="b"} | x = "y" and N = "z" [5m]))`,
	}
	profiles := parserProfiles()
	checked := 0
	for _, name := range exoticFieldNames {
		for _, form := range forms {
			q := strings.ReplaceAll(form, "N", name)
			for _, prof := range profiles {
				if _, err := logql.Parse(q); err != nil {
					continue // the LogQL parser does not read this spelling
				}
				expr := planOf(t, q)
				op, err := PlanQuery(q, expr, Profile{Caps: prof.caps, Label: prof.label, StreamFields: prof.stream})
				if err != nil {
					if !errors.Is(err, ErrNotLowered) {
						t.Errorf("%s %s: %v", prof.name, q, err)
					}
					continue
				}
				_, operand := physical.SplitEval(op)
				vq, ok := operand.(physical.VLQuery)
				if !ok {
					continue
				}
				text := vq.Text
				if _, err := logsql.Parse(text); err != nil {
					t.Errorf("%s %s: the proxy's parser does not read %s: %v", prof.name, q, text, err)
					continue
				}
				quoted := logsql.QuoteFieldName(name)
				if quoted == name || !strings.Contains(text, quoted+":") {
					t.Errorf("%s %s: %s does not name %s quoted", prof.name, q, text, quoted)
				}
				if strings.Contains(text, " "+name+":") || strings.Contains(text, "("+name+":") {
					t.Errorf("%s %s: %s names %q bare", prof.name, q, text, name)
				}
				legacy, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.label, prof.stream, prof.caps)
				if err != nil {
					continue
				}
				if made := QuoteKeywordFieldText(legacy); made != text {
					t.Errorf("%s %s: the translator's text made right is\n %s\nthe lowering writes\n %s", prof.name, q, made, text)
				}
				checked++
			}
		}
	}
	if checked < len(exoticFieldNames)*len(forms) {
		t.Errorf("only %d texts were compared", checked)
	}
}

// A name VictoriaLogs reads bare stays bare: quoting it would change a query that already works.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestPlanKeepsWordFieldNamesBare(t *testing.T) {
	for _, name := range wordFieldNames {
		q := `{a="b"} | x = "y" and ` + name + ` = "z"`
		if _, err := logql.Parse(q); err != nil {
			continue
		}
		op, err := PlanQuery(q, planOf(t, q), Profile{Caps: logsql.CapabilitiesFor("v1.53.0")})
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		want := `a:="b" (x:="y" and ` + name + `:="z")`
		if got := op.(physical.VLQuery).Text; got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
}

// QuoteKeywordFieldText quotes the names the lowering quotes and nothing else, and leaves a text with no such name
// as it is with no allocation.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestQuoteKeywordFieldTextExoticNames(t *testing.T) {
	for in, want := range map[string]string{
		`a:="b" ;:="z"`:                          `a:="b" ";":="z"`,
		`a:="b" (x:="y" and 、:="z")`:             `a:="b" (x:="y" and "、":="z")`,
		`a:="b" -😀:*`:                            `a:="b" -"😀":*`,
		`a:="b" !³:>5`:                           `a:="b" !"³":>5`,
		`a:="b" | filter ;:~"x" Ⅰ:<=5`:           `a:="b" | filter ";":~"x" "Ⅰ":<=5`,
		`a:="b" "x;":="1"`:                       `a:="b" "x;":="1"`,
		`a:="b" x:=";:=" y:="1"`:                 `a:="b" x:=";:=" y:="1"`,
		`a:="b" | copy ; as x`:                   `a:="b" | copy ; as x`,
		`a:="b" é:="z" ж:="1" _x:="2" a_b:="3"`:  `a:="b" é:="z" ж:="1" _x:="2" a_b:="3"`,
		`a:="b" "k8s.pod":="z" k8s.pod:="y"`:     `a:="b" "k8s.pod":="z" k8s.pod:="y"`,
		`__binary__:!=:app:="x"`:                 `__binary__:!=:app:="x"`,
		`a:="b" _stream:{a="b"} !x:="1" -y:~"2"`: `a:="b" _stream:{a="b"} !x:="1" -y:~"2"`,
		`a:="b" x;y:="1"`:                        `a:="b" "x;y":="1"`,
		`a:="b" ;`:                               `a:="b" ;`,
		`a:="b" ;:`:                              `a:="b" ;:`,
		`;:="z"`:                                 `";":="z"`,
		`a:="b" ;:"phrase" 、:in("x")`:            `a:="b" ";":"phrase" "、":in("x")`,
		`a:="b" [x]:="1"`:                        `a:="b" "[x]":="1"`,
	} {
		if got := QuoteKeywordFieldText(in); got != want {
			t.Errorf("%s:\n got %s\nwant %s", in, got, want)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = QuoteKeywordFieldText(`app:="a" (b:="1" or c:~"x.+" é:="2") | unpack_json | filter "k8s.x":*`)
	}); allocs != 0 {
		t.Errorf("a text with no such name allocates %v", allocs)
	}
}

// A stage that reads or writes a label called _time, _stream, _stream_id or _msg is left to the string translator:
// the field filter it writes for it is VictoriaLogs' own time, stream or stream id filter, which the oracle rejects
// even quoted, and which no copy of the field avoids once a parser may have written the key. The label filters
// that name them are lowered as the tag of the stream (TestReservedFieldLabelsMatchLoki).
//
// conformance: semantics/translator-writes-reserved-field-names
func TestPlanLeavesReservedNamesInStagesToTheTranslator(t *testing.T) {
	prof := Profile{Caps: logsql.CapabilitiesFor("v1.53.0")}
	for _, name := range []string{"_time", "_stream", "_stream_id", "_msg"} {
		for _, form := range []struct{ q, construct string }{
			{`sum_over_time({app="a"} | json | unwrap N [1m])`, "RangeAgg " + reservedStageConstruct},
			{`avg_over_time({app="a"} | logfmt | unwrap bytes(N) [1m])`, "RangeAgg " + reservedStageConstruct},
			{`max_over_time({app="a"} | logfmt | unwrap N [1m]) by (app)`, "RangeAgg " + reservedStageConstruct},
			{`quantile_over_time(0.5, {app="a"} | json | unwrap N [1m])`, "RangeAgg " + reservedStageConstruct},
			{`sum by (app) (sum_over_time({app="a"} | json | unwrap N [1m]))`, "VectorAgg " + reservedStageConstruct},
			{`{app="a"} | json x="N" | x="1"`, reservedStageConstruct},
			{`{app="a"} | json x="N"`, reservedStageConstruct},
			{`{app="a"} | json N="x"`, reservedStageConstruct},
			{`{app="a"} | label_format x=N`, reservedStageConstruct},
			{`{app="a"} | label_format N=x`, reservedStageConstruct},
		} {
			q := strings.ReplaceAll(form.q, "N", name)
			expr := planOf(t, q)
			_, err := PlanQuery(q, expr, prof)
			var nl *NotLoweredError
			if !errors.As(err, &nl) || !strings.HasSuffix(nl.Construct, form.construct) {
				t.Errorf("%s: got %v, want NotLoweredError %q", q, err, form.construct)
			}
		}
	}
	// A name in another case is a field like any other there (the outbound guard writes its filter), and a name that
	// only holds one is no reserved field.
	for _, q := range []string{
		`sum_over_time({app="a"} | json | unwrap _TIME [1m])`,
		`sum_over_time({app="a"} | json | unwrap _times [1m])`,
		`{app="a"} | json x="_Stream"`,
		`{app="a"} | label_format x=_streams`,
		`{app="a"} | json x="a_time"`,
	} {
		if _, err := PlanQuery(q, planOf(t, q), prof); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}
