package logql

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
)

// unwrapParensByLayers is UnwrapParens as it read a query before it matched the leading parentheses in one pass: one
// ClosingParen scan per layer. It is the reference the one-pass reading must equal.
func unwrapParensByLayers(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "(") && ClosingParen(s, 0) == len(s)-1 {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

var unwrapParensCases = []string{
	``, `(`, `)`, `()`, `(())`, `( ( x ) )`, `(a)(b)`, `((a) (b))`, `(a))`, `((a)`, `(")")`, "(`)`)", `("\")")`,
	`("a" ) `, " ( x ) ", `((1+1)) + (2)`, `(rate({a="b"}[5m]))`, `((sum by (a) (rate({a="b"}[5m]))))`,
	`(label_replace(rate({a="b"}[5m]), "d", "$1", "s", "(.*)"))`, `("unclosed)`, "(`raw", `( (x) ) )`,
	strings.Repeat("(", 50) + "1" + strings.Repeat(")", 50), strings.Repeat("( ", 30) + "x" + strings.Repeat(" )", 29),
}

// UnwrapParens reads each query as the reading by layers does: the plan corpus, edge cases (quotes holding
// parentheses, unicode blanks, unbalanced and adjacent groups) and deep nests.
//
// conformance: query-complexity-limits
func TestUnwrapParensMatchesLayerReading(t *testing.T) {
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range append(qs, unwrapParensCases...) {
		for _, s := range []string{q, "(" + q + ")", "( (" + q + ") )"} {
			if got, want := UnwrapParens(s), unwrapParensByLayers(s); got != want {
				t.Errorf("UnwrapParens(%.120q) = %.120q, the layer reading %.120q", s, got, want)
			}
		}
	}
}

// For any text, UnwrapParens answers what the reading by layers answers.
func FuzzUnwrapParensMatchesLayerReading(f *testing.F) {
	for _, q := range unwrapParensCases {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if got, want := UnwrapParens(s), unwrapParensByLayers(s); got != want {
			t.Fatalf("UnwrapParens(%q) = %q, the layer reading %q", s, got, want)
		}
	})
}

// BenchmarkUnwrapParens is UnwrapParens on a query in a few parentheses and on deep nests; the reading by layers
// grows with the square of the depth.
func BenchmarkUnwrapParens(b *testing.B) {
	for name, q := range map[string]string{
		"shallow": `((sum by (a) (rate({a="b"} |= "x" [5m]))))`,
		"deep1k":  strings.Repeat("(", 1000) + "1" + strings.Repeat(")", 1000),
		"deep10k": strings.Repeat("(", 10000) + "1" + strings.Repeat(")", 10000),
	} {
		b.Run(name+"/one-pass", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = UnwrapParens(q)
			}
		})
		b.Run(name+"/by-layers", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = unwrapParensByLayers(q)
			}
		})
	}
}
