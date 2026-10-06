package translator

import (
	"errors"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// randomValue builds a string of up to maxLen runes drawn from alphabet. The
// generator is seeded so a failure names an input that reproduces.
func randomValue(r *rand.Rand, alphabet []rune, maxLen int) string {
	n := r.Intn(maxLen + 1)
	out := make([]rune, n)
	for i := range out {
		out[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(out)
}

// The number gate keeps exactly the values strconv.ParseFloat (Loki's
// convertFloat) accepts, for inputs made of digits, signs, dots, underscores and
// exponents. Short inputs cannot reach the documented differences (hex floats,
// inf/nan spellings, exponents above 307); an underscore in the exponent
// (`1e1_0`, which ParseFloat reads) and a positive exponent above 307 (`0e867`,
// `1e308`) are documented differences they reach and are skipped, and so is a
// value that overflows float64 (ParseFloat's ErrRange, which the gate keeps). Any other disagreement is a new divergence: a later rewrite of
// the gate for speed must keep this passing.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapNumberPatternAgreesWithParseFloatOnGeneratedInputs(t *testing.T) {
	re := regexp.MustCompile(UnwrapNumberPattern)
	r := rand.New(rand.NewSource(1))
	alphabet := []rune("0123456789._+-eE")
	for i := 0; i < 200000; i++ {
		v := randomValue(r, alphabet, 8)
		if i := strings.IndexAny(v, "eE"); i >= 0 {
			exp := strings.TrimPrefix(v[i+1:], "+")
			if strings.Contains(exp, "_") {
				continue // documented: the gate drops an underscore in the exponent
			}
			if n, err := strconv.Atoi(exp); err == nil && n > 307 {
				continue // documented: the gate drops a positive exponent above 307
			}
		}
		_, err := strconv.ParseFloat(v, 64)
		if errors.Is(err, strconv.ErrRange) {
			continue // documented: an overflowing value passes the gate
		}
		if re.MatchString(v) != (err == nil) {
			t.Fatalf("%q: gate keeps=%v, ParseFloat ok=%v (%v)", v, re.MatchString(v), err == nil, err)
		}
	}
}

// The duration gate keeps exactly the values time.ParseDuration (Loki's
// convertDuration) accepts, for short inputs (no overflow) of digits, signs,
// dots and the unit letters.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapDurationPatternAgreesWithParseDurationOnGeneratedInputs(t *testing.T) {
	re := regexp.MustCompile(UnwrapDurationPattern)
	r := rand.New(rand.NewSource(2))
	alphabet := []rune("0123456789.+-smhunµμ")
	for i := 0; i < 200000; i++ {
		v := randomValue(r, alphabet, 7)
		_, err := time.ParseDuration(v)
		if re.MatchString(v) != (err == nil) {
			t.Fatalf("%q: gate keeps=%v, ParseDuration ok=%v (%v)", v, re.MatchString(v), err == nil, err)
		}
	}
}
