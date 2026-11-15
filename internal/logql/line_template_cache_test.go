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
