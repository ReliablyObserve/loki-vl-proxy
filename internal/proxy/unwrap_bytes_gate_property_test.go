package proxy

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The bytes gate keeps exactly the values humanize.ParseBytes (Loki's
// convertBytes; parseHumanBytes is its port) accepts, plus the sizes of 2^64
// bytes or more (its "too large" error), which the range filter after the math
// pipe drops (translator.UnwrapInRange). The inputs are short strings of digits,
// dots, commas, spaces, the Unicode spaces TrimSpace trims, the unit letters and
// the two non-ASCII letters strings.ToLower folds into them (the Kelvin sign and
// U+0130); any disagreement is a new divergence a later rewrite of the gate must
// not add.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapBytesPatternAgreesWithParseBytesOnGeneratedInputs(t *testing.T) {
	re := regexp.MustCompile(translator.UnwrapBytesPattern)
	r := rand.New(rand.NewSource(3))
	alphabet := []rune("0123456789.,,, kKmMgGiIbBeEtTpP\u00a0\u2003\u0085\v\u212a\u0130\u0131\u0663\u200b")
	tokens := []string{"1", "9", "18", "446744073709551615", ".", ",", " ", "\u00a0", "k", "M", "G", "T", "P", "E", "i", "B", "b", "\u212a", "\u0130", "\u3000"}
	for i := 0; i < 400000; i++ {
		var b strings.Builder
		if i%2 == 0 {
			for n := r.Intn(8); n > 0; n-- {
				b.WriteRune(alphabet[r.Intn(len(alphabet))])
			}
		} else {
			for n := r.Intn(8); n > 0; n-- {
				b.WriteString(tokens[r.Intn(len(tokens))])
			}
		}
		v := b.String()
		_, err := parseHumanBytes(v)
		want := err == nil || strings.HasPrefix(err.Error(), "too large")
		if keeps := re.MatchString(v); keeps != want {
			t.Fatalf("%q: gate keeps=%v, ParseBytes err=%v", v, keeps, err)
		}
	}
}
