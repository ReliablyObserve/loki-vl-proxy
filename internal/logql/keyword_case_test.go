package logql

import (
	"reflect"
	"testing"
)

// A label keeps the case it is written in unless Loki's lexer reads the word as a keyword, which it does for ip and
// label_replace only before a parenthesis: Ip, IP and Label_replace are label names (Loki's tree holds them as
// written; the Loki oracle facts command on each query below answered the names given here), while of, Of and OF
// are the keyword and no label name.
//
// conformance: semantics/loki-keyword-label-names
func TestLabelsKeepCaseAroundFunctionWords(t *testing.T) {
	for q, names := range map[string][]string{
		`{Ip="a"}`:                       {"Ip"},
		`{IP="a",Label_replace="b"}`:     {"IP", "Label_replace"},
		`{app="a"} | drop Ip, IP`:        {"Ip", "IP"},
		`{app="a"} | drop Label_replace`: {"Label_replace"},
		`{app="a"} | json | drop Bytes`:  {"Bytes"},
	} {
		expr, err := Parse(q)
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		lq, ok := expr.(*LogQuery)
		if !ok {
			t.Errorf("%s: not a log query", q)
			continue
		}
		var got []string
		for _, m := range lq.Selector.Matchers {
			if m.Name != "app" {
				got = append(got, m.Name)
			}
		}
		for _, st := range lq.Pipeline {
			if d, ok := st.(*DropStage); ok {
				got = append(got, d.Labels...)
			}
		}
		if !reflect.DeepEqual(got, names) {
			t.Errorf("%s: names %v, want %v", q, got, names)
		}
	}
	for _, q := range []string{`{app="a"} | drop ip(x)`, `{app="a"} | drop IP(x)`, `{app="a"} | label_format x=ip(y)`} {
		if err := ParseAndValidate(q); err == nil {
			t.Errorf("%s: parsed, Loki rejects it", q)
		}
	}
}

// keywordLabelWord is the words of lokiAlwaysKeywords that are identifiers to the scanner (the others have a token
// type of their own), and no other.
func TestKeywordLabelWordIsTheIdentifierKeywords(t *testing.T) {
	for w := range lokiAlwaysKeywords {
		tok := (&scanner{src: w}).scanIdent()
		if want := tok.Typ == TokIdent; keywordLabelWord(w) != want {
			t.Errorf("keywordLabelWord(%q) = %v, want %v", w, keywordLabelWord(w), want)
		}
	}
	for _, w := range []string{"", "o", "of_", "app", "json2", "Json", "status", "ip", "label_replace"} {
		if keywordLabelWord(w) {
			t.Errorf("keywordLabelWord(%q) is true", w)
		}
	}
}
