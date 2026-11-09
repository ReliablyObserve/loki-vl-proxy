package logql

import (
	"strconv"
	"testing"
	"unicode/utf8"
)

// A label matcher regex is rejected alike in every place a pipeline holds one (a label filter alone or in a chain,
// drop, keep), with the same text, and that parse error is reported before an earlier stage's build error. Each
// form is held to Loki by TestStageParseErrorsMatchLoki.
func FuzzLabelFilterRegexFormsAgree(f *testing.F) {
	for _, re := range []string{"(", "a)|(b", "[z-a]", "x{1001}", "*", `\8`, "(?P<n>a)(?P<n>b)", `\p{Foo}`, "a.*", ""} {
		f.Add(re)
	}
	f.Fuzz(func(t *testing.T, re string) {
		if !utf8.ValidString(re) {
			t.Skip() // the forms read a string that is not UTF-8 differently, before any regex
		}
		q := strconv.Quote(re)
		text := func(query string) (string, bool) {
			if _, err := ParseLogQuery(query); err != nil {
				return "", false
			}
			if err := ParseAndValidate(query); err != nil {
				return err.Error(), true
			}
			return "", true
		}
		want, ok := text(`{app="a"} | x=~` + q)
		if !ok {
			t.Skip()
		}
		for _, form := range []string{`{app="a"} | x="1" or y=~` + q, `{app="a"} | a="b", x!~` + q, `{app="a"} | drop x=~` + q,
			`{app="a"} | keep a, x!~` + q, `{app="a"} | json | x=~` + q} {
			if got, ok := text(form); ok && got != want {
				t.Fatalf("%s: %q, but %q for the filter alone", form, got, want)
			}
		}
		if want != "" {
			if got, ok := text(`{app="a"} | json x="a[" | x=~` + q); ok && got != want {
				t.Fatalf("an earlier stage's build error came first: %q, want %q", got, want)
			}
		}
	})
}
