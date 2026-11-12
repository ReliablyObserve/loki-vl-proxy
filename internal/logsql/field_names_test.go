package logsql

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

// BareFieldName is VictoriaLogs' own reading of a word (lib/logstorage isTokenRune: a letter, a digit, an
// underscore), and QuoteFieldName quotes a name it cannot read bare or reads as a keyword and nothing else.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestQuoteFieldName(t *testing.T) {
	for name, want := range map[string]string{
		"app": "app", "_x": "_x", "a_b1": "a_b1", "é": "é", "ж": "ж", "日本": "日本", "Ω": "Ω", "٣": "٣", "": "",
		"_time": "_time", "_stream": "_stream", "_msg": "_msg", "i": "i", "range": "range",
		"、": `"、"`, ";": `";"`, "@": `"@"`, "³": `"³"`, "Ⅰ": `"Ⅰ"`, "𝛁": `"𝛁"`, "😀": `"😀"`, "ा": `"ा"`,
		"\u200b": "\"\u200b\"", "·": `"·"`, "☃": `"☃"`, "»": `"»"`, "\U000f0000": "\"\U000f0000\"", "⃝": `"⃝"`,
		"a.b": `"a.b"`, "a-b": `"a-b"`, "k8s.container.name": `"k8s.container.name"`, "a b": `"a b"`, "a/b": `"a/b"`,
		"é;": `"é;"`, "x\u0301": "\"x\u0301\"", `a"b`: `"a\"b"`, `a\b`: `"a\\b"`, "a\nb": `"a\nb"`, "\xff": `"\xff"`,
		"not": `"not"`, "IN": `"IN"`, "Or": `"Or"`, "options": `"options"`, "contains_any": `"contains_any"`,
		`"a.b"`: `"a.b"`, "`a`": "`a`", `"`: `"`,
	} {
		if got := QuoteFieldName(name); got != want {
			t.Errorf("QuoteFieldName(%q) = %q, want %q", name, got, want)
		}
	}
	for name, want := range map[string]bool{
		"app": true, "_x": true, "é": true, "٣": true, "": true, "a.b": false, "a-b": false, "、": false, ";": false,
		"³": false, "Ⅰ": false, "\u200b": false, "x\u0301": false, "\xff": false, "a\xffb": false, "not": true,
	} {
		if got := BareFieldName(name); got != want {
			t.Errorf("BareFieldName(%q) = %v, want %v", name, got, want)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = QuoteFieldName("kubernetes_pod_name")
		_ = QuoteFieldName("日本語_name")
	}); allocs != 0 {
		t.Errorf("a name that stays bare allocates %v times", allocs)
	}
}

// The proxy's LogsQL parser reads the quoted name as the field it is, in every place a filter or a pipe names one,
// and rejects the bare spelling of a name VictoriaLogs cannot read bare, as VictoriaLogs does.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestParseReadsQuotedFieldNames(t *testing.T) {
	names := []string{"、", ";", "@", "³", "Ⅰ", "𝛁", "😀", "ा", "\u200b", "·", "☃", "»", "a.b", "a-b", "a b", `a"b`, `a\b`, "not", "in", "contains_any", "é", "日本"}
	places := []string{`a:="b" N:="z"`, `a:="b" (x:="y" and N:="z")`, `a:="b" -N:*`, `a:="b" | filter N:~"x"`, `* | copy N as x`, `* | copy x as N`,
		`* | fields N`, `* | delete N`, `* | unpack_json fields (N)`, `* | format if (N:*) "<x>" as y`}
	for _, name := range names {
		quoted := QuoteFieldName(name)
		for _, place := range places {
			if _, err := Parse(strings.ReplaceAll(place, "N", quoted)); err != nil {
				t.Errorf("%s: the quoted name %s is not read: %v", place, quoted, err)
			}
		}
		if BareFieldName(name) || name == "" {
			continue
		}
		if _, err := Parse(`a:="b" ` + name + `:="z"`); err == nil && !strings.ContainsAny(name, ".- \"\\") {
			t.Errorf("the bare name %q is read as a field", name)
		}
	}
}

// FuzzQuoteFieldName holds the property that matters to every caller: a filter that names a field with the quoted
// spelling is read back by the LogsQL parser as that field.
func FuzzQuoteFieldName(f *testing.F) {
	for _, s := range []string{"app", "、", ";", "😀", "a.b", "not", "ा", "\u200b", `a"b`, `a\b`, "a b", "é", "x\u0301", "𝛁", "[x]", "a:b", "a|b", "a(b)", "a,b", "a{b}", "#x", "'x'", "`x`", "-x", "!x", "*"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if name == "" || !utf8.ValidString(name) || strings.ContainsAny(name, "\x00") || ReservedFieldName(name) || ReservedFieldNameOtherCase(name) {
			return
		}
		if name[0] == '"' || name[0] == '`' {
			return // already quoted, by definition
		}
		quoted := QuoteFieldName(name)
		q, err := Parse(`a:="b" ` + quoted + `:="z"`)
		if err != nil {
			t.Fatalf("name %q written as %s is not read: %v", name, quoted, err)
		}
		all, ok := q.Filter.(AllOf)
		if !ok || len(all) != 2 {
			t.Fatalf("name %q written as %s: filter %T %v", name, quoted, q.Filter, q.Filter)
		}
		ff, ok := all[1].(FieldFilter)
		if !ok {
			t.Fatalf("name %q written as %s: second filter %T", name, quoted, all[1])
		}
		if got := unquoteName(ff.Field); got != name {
			t.Fatalf("name %q written as %s is read back as %q", name, quoted, got)
		}
	})
}

// exoticFieldNameVerdicts are VictoriaLogs' own verdicts (its parser, v1.49.0 and v1.53.0) on names its lexer does not
// join into a word, bare and quoted, at the places a filter or a pipe names a field: every bare spelling is
// rejected, every quoted one read. Recorded with the VictoriaLogs oracle (go test ./internal/querycorpus -run
// TestRecordExoticFieldNameVerdicts with LVP_RUN_ORACLES=1 records them again).
const exoticFieldNameVerdicts = "testdata/exotic-field-names-vl.jsonl"

// The proxy's LogsQL parser reads every recorded query as VictoriaLogs does, so the fuzz property above (the quoted
// name is read back as the field) holds for VictoriaLogs too.
//
// conformance: semantics/translator-writes-exotic-field-names-bare
func TestParseFieldNamesMatchVictoriaLogs(t *testing.T) {
	f, err := os.Open(exoticFieldNameVerdicts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r keywordFieldVerdict
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows++
		if r.V149 != r.V153 {
			t.Errorf("%s: VictoriaLogs v1.49.0 accepts=%v, v1.53.0 accepts=%v: the grid is for one reading", r.Query, r.V149, r.V153)
		}
		// The parser's looseness on a bare keyword is registered (looserThanVL), and the lowering quotes those names.
		if _, err := Parse(r.Query); (err == nil) != r.V153 && (err != nil || !looserThanVL.MatchString(r.Query)) {
			t.Errorf("%s: VictoriaLogs accepts=%v, the proxy's parser error is %v", r.Query, r.V153, err)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if rows < 500 {
		t.Fatalf("%d recorded verdicts", rows)
	}
}
