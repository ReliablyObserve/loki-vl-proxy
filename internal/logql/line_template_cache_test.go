package logql

import (
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

// The typed template of a line_format text is parsed once: a second query with the text shares it, and the cache
// keeps a copy of the text, not the query it came from. A template that does not parse is remembered as such, a
// long one is not kept, and once the cache is full a template is typed as before.
//
// conformance: architecture-syntax-package-layering, formatter-line-format
func TestLineTemplateCache(t *testing.T) {
	query := `{a="b"} | line_format "{{.cache_probe}} x"`
	lq, err := ParseLogQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	first := lq.Pipeline[0].(*LineFormatStage).Tmpl
	again, _ := ParseLogQuery(`{c="d"} |= "y" | line_format "{{.cache_probe}} x"`)
	if second := again.Pipeline[1].(*LineFormatStage).Tmpl; second != first || !reflect.DeepEqual(first, typeLineTemplateUncached("{{.cache_probe}} x")) {
		t.Fatalf("the template is typed again: %p %p", first, second)
	}
	key := ""
	lineTemplates.Range(func(k, _ any) bool {
		if k.(string) == "{{.cache_probe}} x" {
			key = k.(string)
		}
		return true
	})
	if key == "" || unsafe.StringData(key) == unsafe.StringData(query[strings.Index(query, "{{"):]) {
		t.Fatalf("the cache keeps the query's own text: %q", key)
	}
	if first, cached := typeLineTemplate("{{.a"), typeLineTemplate("{{.a"); first != nil || cached != nil {
		t.Fatal("a template that does not parse is typed")
	}
	long := "{{.a}}" + strings.Repeat("x", lineTemplateMaxBytes)
	if lt := typeLineTemplate(long); lt == nil || lt.Canonical != long {
		t.Fatalf("a long template: %v", lt)
	}
	if _, ok := lineTemplates.Load(long); ok {
		t.Fatal("a long template is kept")
	}
	count := lineTemplateCount.Load()
	lineTemplateCount.Store(lineTemplatesMax)
	defer lineTemplateCount.Store(count)
	if lt := typeLineTemplate("{{.full_cache}}"); lt == nil || lt.Canonical != "{{.full_cache}}" {
		t.Fatalf("a full cache types the template: %v", lt)
	}
	if _, ok := lineTemplates.Load("{{.full_cache}}"); ok {
		t.Fatal("a full cache grows")
	}
}

// ParseUntypedTemplates gives Parse's tree with every line_format template untyped, and parses no template: a query
// with a template no query sent before costs no text/template parse.
//
// conformance: architecture-syntax-package-layering, formatter-line-format
func TestParseUntypedTemplates(t *testing.T) {
	for _, q := range []string{
		`{a="b"} | json | line_format "{{.untyped_probe}} {{.b}}"`, `{a="b"} | line_format "{{"`,
		`sum(count_over_time({a="b"} | line_format "{{.c}}" [5m]))`, `{a="b"} | json | x="y"`,
	} {
		want, werr := Parse(q)
		got, gerr := ParseUntypedTemplates(q)
		if (werr == nil) != (gerr == nil) {
			t.Fatalf("%q: %v, Parse %v", q, gerr, werr)
		}
		if werr != nil {
			continue
		}
		untype(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%q: the tree differs from Parse's", q)
		}
	}
	q := `{a="b"} | line_format "{{.never_typed}}"`
	if _, err := ParseUntypedTemplates(q); err != nil {
		t.Fatal(err)
	}
	if _, ok := lineTemplates.Load("{{.never_typed}}"); ok {
		t.Error("the template was typed")
	}
	// Coverage instrumentation changes allocation counts.
	if n := parseAllocs(20, func() { _, _ = ParseUntypedTemplates(q) }); n > 5 && testing.CoverMode() == "" {
		t.Errorf("%v allocations", n)
	}
}

// untype clears the typed templates of a parsed tree.
func untype(e Expr) {
	switch x := e.(type) {
	case *LogQuery:
		for _, st := range x.Pipeline {
			if lf, ok := st.(*LineFormatStage); ok {
				lf.Tmpl = nil
			}
		}
	case *RangeAggregation:
		untype(x.Inner)
	case *VectorAggregation:
		untype(x.Inner)
	}
}
