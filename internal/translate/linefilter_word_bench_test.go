package translate

import "testing"

// The cost of the word-boundary check on the regexp line filters the translators see: a bounded word (parsed and
// turned into a phrase), a bounded alternation, and the regexps that carry no \b, which the substring check rejects.
func BenchmarkWordBoundaryFilter(b *testing.B) {
	for _, c := range []struct{ name, pattern string }{
		{"word", `\bfailed\b`},
		{"alternation", `\b(?:error|failed|timeout)\b`},
		{"no_boundary", `status=[45]\d{2}`},
		{"boundary_not_literal", `\bfail\w+\b`},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				WordBoundaryFilter(c.pattern)
			}
		})
	}
}
