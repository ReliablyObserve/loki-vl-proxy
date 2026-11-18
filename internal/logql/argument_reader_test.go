package logql

import "testing"

// argumentReaderTexts are texts with brackets of every kind, strings holding brackets and commas, escapes, and
// brackets that never close.
var argumentReaderTexts = []string{
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`sum(rate({app="a, (b"} |= "x)" [1m])) by (a), 1)`,
	"a(`b,(`), c] , d",
	`a("\"(", [b, {c)}], d`,
	`((((`,
	`)))), x`,
	`a, b`,
	`f(g(h(i)), j), k`,
	`"unterminated ( , x`,
	"x\\\"(,",
}

// The reader's first argument of the text after every offset is FirstArgumentLen's.
//
// conformance: semantics/nested-calls-read-once
func TestArgumentReaderMatchesFirstArgumentLen(t *testing.T) {
	for _, s := range argumentReaderTexts {
		if msg := argumentReaderDiffers(s); msg != "" {
			t.Error(msg)
		}
	}
}

// argumentReaderDiffers reports an offset outside the strings of s where the reader's first argument differs from
// FirstArgumentLen's.
func argumentReaderDiffers(s string) string {
	r := NewArgumentReader(s)
	var quote byte
	for from := 0; from <= len(s); from++ {
		if quote == 0 {
			if got, want := r.FirstArgumentLen(from), FirstArgumentLen(s[from:]); got != want {
				return "text " + s + ": the reader and FirstArgumentLen differ"
			}
		}
		if from == len(s) {
			break
		}
		switch c := s[from]; {
		case quote != 0:
			if c == '\\' && quote == '"' {
				from++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		}
	}
	return ""
}

// conformance: semantics/nested-calls-read-once
func FuzzArgumentReaderMatchesFirstArgumentLen(f *testing.F) {
	for _, s := range argumentReaderTexts {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 512 {
			return
		}
		if msg := argumentReaderDiffers(s); msg != "" {
			t.Error(msg)
		}
	})
}
