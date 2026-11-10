package translate

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// conformance: architecture-syntax-package-layering
func TestFormatTemplates(t *testing.T) {
	for _, c := range []struct{ text, want, reason string }{
		{"{{.a}}", "<a>", ""},
		{"{{ .a }} x {{.b}}", "<a> x <b>", ""},
		{"plain", "plain", ""},
		{"", "", ""},
		{"{{.a | upper}}", "", TemplateNotText},
		{"{{.a.b}}", "", TemplateNotText},
		{"{{if .a}}x{{end}}", "", TemplateNotText},
		{"{{- .a -}}", "", TemplateTrimMarker},
		{"x <b> {{.a}}", "", TemplatePlaceholderText},
		{"a\rb", "", TemplateCarriageReturn},
	} {
		got, reason := FormatTemplate(c.text)
		// the typed template says the same about what is text and plain fields
		if tmpl := logql.ParseLineTemplate(c.text); tmpl != nil && tmpl.Simple != (reason == "" || reason == TemplateTrimMarker || reason == TemplatePlaceholderText || reason == TemplateCarriageReturn) {
			t.Errorf("%q: the typed template says Simple=%v, FormatTemplate reason %q", c.text, tmpl.Simple, reason)
		}
		if got != c.want || reason != c.reason {
			t.Errorf("FormatTemplate(%q) = %q, %q; want %q, %q", c.text, got, reason, c.want, c.reason)
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestLineFormatPipes(t *testing.T) {
	for keep, want := range map[bool]string{false: `| format "<a>-x"`, true: `| copy _msg as _lvp_line | format "<a>-x"`} {
		pipes, reason := LineFormat("{{.a}}-x", keep)
		got := ""
		for _, p := range pipes {
			if got != "" {
				got += " "
			}
			got += p.String()
		}
		if reason != "" || got != want {
			t.Errorf("LineFormat keepLine=%v = %q, %q; want %q", keep, got, reason, want)
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestLabelFormatPipes(t *testing.T) {
	text := func(formats []logql.LabelFormat) (string, string) {
		pipes, reason := LabelFormats(formats)
		out := ""
		for _, p := range pipes {
			if out != "" {
				out += " "
			}
			out += p.String()
		}
		return out, reason
	}
	if got, reason := text([]logql.LabelFormat{{Name: "a", Value: "b", Rename: true}, {Name: "c", Value: "{{.d}}-x"}}); reason != "" ||
		got != `| format if (b:*) "<b>" as a skip_empty_results | delete b | format "<d>-x" as c` {
		t.Errorf("LabelFormats = %q, %q", got, reason)
	}
	for _, c := range []struct {
		formats []logql.LabelFormat
		reason  string
	}{
		{[]logql.LabelFormat{{Name: "a", Value: "a", Rename: true}}, TemplateRenameOntoItself},
		{[]logql.LabelFormat{{Name: "a", Value: "{{.b | upper}}"}}, TemplateNotText},
		{[]logql.LabelFormat{{Name: "of", Value: "x", Rename: true}}, TemplateKeywordName},
		{[]logql.LabelFormat{{Name: "a", Value: "json", Rename: true}}, TemplateKeywordName},
	} {
		if _, reason := text(c.formats); reason != c.reason {
			t.Errorf("LabelFormats(%v) reason %q, want %q", c.formats, reason, c.reason)
		}
	}
}
