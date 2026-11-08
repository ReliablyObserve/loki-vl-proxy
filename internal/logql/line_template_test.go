package logql

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"text/template"
	"text/template/parse"
)

// conformance: architecture-syntax-package-layering, formatter-line-format
func TestTypeLineTemplate(t *testing.T) {
	cases := []struct {
		text      string
		canonical string
		fields    []string
		funcs     []string
		readsDot  bool
		simple    bool
	}{
		{text: "", canonical: "", simple: true},
		{text: "plain", canonical: "plain", simple: true},
		{text: "{{.a}}", canonical: "{{.a}}", fields: []string{"a"}, simple: true},
		{text: "{{ .a }} - {{.b}}{{.a}}", canonical: "{{.a}} - {{.b}}{{.a}}", fields: []string{"a", "b"}, simple: true},
		{text: "x {{- .a -}} y", canonical: "x{{.a}}y", fields: []string{"a"}, simple: true},
		{text: "a{{/* c */}}b", canonical: "ab", simple: true},
		{text: "{{.a.b}}", canonical: "{{.a.b}}", fields: []string{"a"}},
		{text: "{{ .a | ToUpper }}", canonical: "{{.a | ToUpper}}", fields: []string{"a"}, funcs: []string{"ToUpper"}},
		{text: "{{ __line__ }} {{ __timestamp__ | unixEpoch }}", canonical: "{{__line__}} {{__timestamp__ | unixEpoch}}", funcs: []string{"__line__", "__timestamp__", "unixEpoch"}},
		{text: `{{ if eq .level "error" }}E{{ else }}{{ .msg }}{{ end }}`, canonical: `{{if eq .level "error"}}E{{else}}{{.msg}}{{end}}`, fields: []string{"level", "msg"}, funcs: []string{"eq"}},
		{text: `{{ index . "a.b" }}`, canonical: `{{index . "a.b"}}`, funcs: []string{"index"}, readsDot: true},
		{text: "{{.}}", canonical: "{{.}}", readsDot: true},
		{text: "{{$.a}}", canonical: "{{$.a}}", fields: []string{"a"}},
		{text: "{{$x := .a}}{{$x}}", canonical: "{{$x := .a}}{{$x}}", fields: []string{"a"}},
		{text: "{{range .a}}{{.b}}{{end}}", canonical: "{{range .a}}{{.b}}{{end}}", fields: []string{"a", "b"}},
		{text: `{{define "t"}}{{.z}}{{end}}{{template "t" .}}`, canonical: `{{define "t"}}{{.z}}{{end}}{{template "t" .}}`, fields: []string{"z"}, readsDot: true},
		{text: "{{(.a)}}", canonical: "{{(.a)}}", fields: []string{"a"}},
	}
	for _, c := range cases {
		lt := typeLineTemplate(c.text)
		if lt == nil {
			t.Errorf("%q: not typed", c.text)
			continue
		}
		if lt.Canonical != c.canonical || !reflect.DeepEqual(lt.Fields, c.fields) || !reflect.DeepEqual(lt.Funcs, c.funcs) || lt.ReadsDot != c.readsDot || lt.Simple != c.simple {
			t.Errorf("%q = %+v, want canonical %q fields %v funcs %v readsDot %v simple %v", c.text, *lt, c.canonical, c.fields, c.funcs, c.readsDot, c.simple)
		}
	}
	for _, bad := range []string{"{{", "{{.a", "{{nosuchfunc}}", "{{end}}", "{{if}}{{end}}"} {
		if lt := typeLineTemplate(bad); lt != nil {
			t.Errorf("%q typed as %+v; Loki rejects it", bad, *lt)
		}
	}
}

// conformance: architecture-syntax-package-layering, formatter-line-format
func TestLineFormatStageIsTyped(t *testing.T) {
	e, err := Parse(`{a="x"} | line_format "{{ .a | ToUpper }}"`)
	if err != nil {
		t.Fatal(err)
	}
	lf := e.(*LogQuery).Pipeline[0].(*LineFormatStage)
	if lf.Tmpl == nil || lf.KeyText() != "{{.a | ToUpper}}" || lf.Template != "{{ .a | ToUpper }}" {
		t.Fatalf("stage = %+v tmpl %+v", lf, lf.Tmpl)
	}
	if (&LineFormatStage{Template: "{{"}).KeyText() != "{{" {
		t.Fatal("an untyped stage keys on its text")
	}
}

// templateShape prints a parse tree without positions, with adjacent text
// nodes joined: two trees with one shape execute the same way.
func templateShape(t *template.Template) string {
	var b strings.Builder
	defs := t.Templates()
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name() < defs[j].Name() })
	for _, d := range defs {
		if d.Tree == nil {
			continue
		}
		fmt.Fprintf(&b, "[%s]", d.Name())
		shapeNode(&b, reflect.ValueOf(d.Root))
	}
	return b.String()
}

func shapeNode(b *strings.Builder, v reflect.Value) {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		if l, ok := v.Interface().(*parse.ListNode); ok {
			b.WriteString("List(")
			var text strings.Builder
			flush := func() {
				if text.Len() > 0 {
					fmt.Fprintf(b, "Text(%q)", text.String())
					text.Reset()
				}
			}
			for _, n := range l.Nodes {
				if tn, ok := n.(*parse.TextNode); ok {
					text.Write(tn.Text)
					continue
				}
				flush()
				shapeNode(b, reflect.ValueOf(n))
			}
			flush()
			b.WriteString(")")
			return
		}
		shapeNode(b, v.Elem())
	case reflect.Struct:
		fmt.Fprintf(b, "%s{", v.Type().Name())
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if !f.IsExported() || f.Name == "Pos" || f.Name == "Line" {
				continue
			}
			fmt.Fprintf(b, "%s:", f.Name)
			shapeNode(b, v.Field(i))
			b.WriteString(",")
		}
		b.WriteString("}")
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			fmt.Fprintf(b, "%q", v.Bytes())
			return
		}
		b.WriteString("[")
		for i := 0; i < v.Len(); i++ {
			shapeNode(b, v.Index(i))
			b.WriteString(",")
		}
		b.WriteString("]")
	default:
		fmt.Fprintf(b, "%#v", v.Interface())
	}
}

func parseLineTemplate(text string) (*template.Template, error) {
	return template.New("line").Option("missingkey=zero").Funcs(lokiTemplateFunctions).Parse(text)
}

// A template is typed exactly when validation accepts it, and its canonical
// form parses to a tree that executes the same way, so equal canonical forms
// mean equal templates.
func FuzzLineTemplateCanonical(f *testing.F) {
	for _, s := range []string{
		"", "plain", "{{.a}}", "x {{- .a -}} y", "a{{/* c */}}b", "{{ .a | ToUpper }}",
		"{{ __line__ }}", `{{ if eq .level "error" }}E{{ else if .x }}X{{ else }}{{ .msg }}{{ end }}`,
		`{{ index . "a.b" }}`, "{{$x := .a}}{{$x = 2}}{{$x}}", "{{range $i, $v := .a}}{{$i}}{{break}}{{end}}",
		`{{define "t"}}{{.z}}{{end}}{{template "t" .}}`, `{{block "b" .}}x{{end}}`, "{{(.a).b}}",
		`{{printf "%5.2f" 1.5e3}}`, "{{0x1F}} {{'a'}} {{-1}} {{nil}}", "{{with .a}}{{.}}{{else with .b}}b{{end}}",
		"{{`raw`}}", "{{\"q\\n\"}}", "}}", "{{- /* x */ -}}",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, text string) {
		if len(text) > 4096 {
			return
		}
		lt := typeLineTemplate(text)
		if (lt != nil) != (validateTemplate("line", text) == nil) {
			t.Fatalf("%q: typed %v, validation error %v", text, lt != nil, validateTemplate("line", text))
		}
		if lt == nil {
			return
		}
		again := typeLineTemplate(lt.Canonical)
		if again == nil {
			t.Fatalf("%q: canonical %q does not parse", text, lt.Canonical)
		}
		if again.Canonical != lt.Canonical {
			t.Fatalf("%q: canonical %q prints again as %q", text, lt.Canonical, again.Canonical)
		}
		a, _ := parseLineTemplate(text)
		b, _ := parseLineTemplate(lt.Canonical)
		if sa, sb := templateShape(a), templateShape(b); sa != sb {
			t.Fatalf("%q: canonical %q changes the tree\n%s\n%s", text, lt.Canonical, sa, sb)
		}
		if lt.Simple && (lt.ReadsDot || len(lt.Funcs) != 0) {
			t.Fatalf("%q: simple template %+v", text, *lt)
		}
	})
}
