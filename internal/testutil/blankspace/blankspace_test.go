package blankspace

import "testing"

// Collapse writes a run of the four blanks as one space and leaves quoted text, backquoted text and the other
// characters (a vertical tab, a form feed, U+0085, U+00A0, which Loki's lexer rejects) as they are.
func TestCollapse(t *testing.T) {
	for in, want := range map[string]string{
		"{A=\"0\"} | json\rA0":                  `{A="0"} | json A0`,
		"{A=\"0\"} | json\r\nA0":                `{A="0"} | json A0`,
		"{A=\"0\"} |\t\tjson \t\n a":            `{A="0"} | json a`,
		`{A="0"} | json A0`:                     `{A="0"} | json A0`,
		"{A=\"0\"} |= \"a\tb  c\"":              "{A=\"0\"} |= \"a\tb  c\"",
		"{A=\"0\"} |= `a\r\nb  c`":              "{A=\"0\"} |= `a\r\nb  c`",
		"{A=\"0\"} |= \"x\\\"\t\" |\tjson":      "{A=\"0\"} |= \"x\\\"\t\" | json",
		"{A=\"0\"} | json\va":                   "{A=\"0\"} | json\va",
		"{A=\"0\"} | json a":                    "{A=\"0\"} | json a",
		"{A=\"0\"} | json\u0085a":               "{A=\"0\"} | json\u0085a",
		"sum by (\t) (rate({A=\"0\"}[1m]))\r\n": `sum by ( ) (rate({A="0"}[1m])) `,
		"\"unterminated\t":                      "\"unterminated\t",
		"":                                      "",
	} {
		if got := Collapse(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}
