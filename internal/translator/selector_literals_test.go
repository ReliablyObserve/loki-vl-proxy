package translator

import (
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestSelectorAndPipelineExactValuesAreDecodedOnce(t *testing.T) {
	for _, value := range []string{`quote"slash\`, `a",b`, " spaced ", `tick` + "`" + `value`, "carriage\rreturn", "separator\u2028"} {
		for _, quote := range []string{strconv.Quote(value), "`" + value + "`"} {
			if strings.Contains(value, "`") && strings.HasPrefix(quote, "`") {
				continue
			}
			for _, label := range []string{"path", "service_name"} {
				query := "sum(count_over_time({" + label + "=" + quote + `,app="tail"}[5m]))`
				translated, err := TranslateLogQL(query)
				if err != nil || !strings.Contains(translated, label+":="+logsql.QuoteValue(value)) || !strings.Contains(translated, `app:="tail"`) || !strings.Contains(translated, "| stats ") {
					t.Fatalf("query=%s translation=%s err=%v", query, translated, err)
				}
			}
			pipeline, err := TranslateLogQL(`{app="tail"}|regexp "(?P<path>.*)"|path=` + quote)
			if err != nil || !strings.Contains(pipeline, "path:="+logsql.QuoteValue(value)) {
				t.Fatalf("pipeline=%s err=%v", pipeline, err)
			}
		}
	}
}

func TestLabelFormatEscapedTemplateAndComma(t *testing.T) {
	value := "say \"hello\", tick`value Łódź 日本語"
	translated, err := TranslateLogQL(`{app="tail"}|label_format cap=` + strconv.Quote(value) + `,other="ok"`)
	want := (logsql.PipeFormat{Template: value, ResultField: "cap"}).String()
	if err != nil || !strings.Contains(translated, want) || !strings.Contains(translated, (logsql.PipeFormat{Template: "ok", ResultField: "other"}).String()) {
		t.Fatalf("translation=%s want=%s err=%v", translated, want, err)
	}
}

func TestIPShapedExactLabelValueIsNotFunctionCall(t *testing.T) {
	value := `ip("10.0.0.1")`
	literal, err := TranslateLogQL(`{app="a"}|json|path=` + strconv.Quote(value))
	if err != nil || !strings.Contains(literal, "path:="+logsql.QuoteValue(value)) {
		t.Fatalf("literal=%s err=%v", literal, err)
	}
	call, err := TranslateLogQL(`{app="a"}|json|path=ip("10.0.0.1")`)
	if err != nil || call == literal {
		t.Fatalf("call=%s literal=%s err=%v", call, literal, err)
	}
}
