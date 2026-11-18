package proxy

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/dustin/go-humanize"
)

// The bytes gate keeps exactly the values go-humanize v1.1.0's ParseBytes
// (Loki 3.7.10's convertBytes; parseHumanBytes is its port, checked here against
// that function on every input) accepts, plus the
// sizes of 2^64 bytes or more (its "too large" error), which the range filter after the math pipe drops
// (translator.UnwrapInRange). The inputs are short strings of digits,
// dots, commas, spaces, the Unicode spaces TrimSpace trims, the unit letters and
// the two non-ASCII letters strings.ToLower folds into them (the Kelvin sign and
// U+0130); any disagreement is a new divergence a later rewrite of the gate must
// not add.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences, semantics/unwrap-bytes-whole-size-exact
func TestUnwrapBytesPatternAgreesWithParseBytesOnGeneratedInputs(t *testing.T) {
	re := regexp.MustCompile(translator.UnwrapBytesPattern)
	r := rand.New(rand.NewSource(3))
	alphabet := []rune("0123456789.,,, kKmMgGiIbBeEtTpP\u00a0\u2003\u0085\v\u212a\u0130\u0131\u0663\u200b")
	tokens := []string{"1", "9", "18", "446744073709551615", "18446744073709551615", "1844674407370955161", ".", ",", " ", "\u00a0", "k", "M", "G", "T", "P", "E", "i", "B", "b", "\u212a", "\u0130", "\u3000"}
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
		got, err := parseHumanBytes(v)
		// The port is what the raw evaluators run; it must be the function Loki calls
		// (go-humanize v1.1.0, the version Loki 3.7.10 resolves), error text included.
		if wantBytes, wantErr := humanize.ParseBytes(v); got != wantBytes || (err == nil) != (wantErr == nil) || (err != nil && err.Error() != wantErr.Error()) {
			t.Fatalf("%q: parseHumanBytes = %v, %v; Loki's ParseBytes (go-humanize v1.1.0) = %v, %v", v, got, err, wantBytes, wantErr)
		}
		want := err == nil || strings.HasPrefix(err.Error(), "too large")
		if keeps := re.MatchString(v); keeps != want {
			t.Fatalf("%q: gate keeps=%v, ParseBytes err=%v", v, keeps, err)
		}
	}
}
