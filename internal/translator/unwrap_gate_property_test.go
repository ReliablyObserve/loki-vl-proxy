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

// The number gate keeps exactly the syntactically valid floats strconv.ParseFloat
// (Loki's convertFloat) accepts, plus the ones it reports as out of range (the
// range filter after the math pipe drops those: TestUnwrapGateRange), for random
// strings of digits, signs, dots, underscores, exponent and hexadecimal letters,
// and for strings joined from float tokens (so hexadecimal floats and exponents
// with separators are reached). The two spellings the gate leaves out on purpose
// (infinity, nan) are skipped; any other disagreement is a new divergence: a later
// rewrite of the gate for speed must keep this passing.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapNumberPatternAgreesWithParseFloatOnGeneratedInputs(t *testing.T) {
	re := regexp.MustCompile(UnwrapNumberPattern)
	r := rand.New(rand.NewSource(1))
	alphabet := []rune("0123456789._+-eEpPxXaAfF")
	tokens := []string{"0x", "0X", "0", "1", "7", "9", "f", "A", "e", "E", "p", "P", ".", "_", "+", "-", "00", "10", "309", "inf", "nan", "x"}
	check := func(v string) {
		t.Helper()
		if lower := strings.ToLower(strings.TrimLeft(v, "+-")); lower == "infinity" || lower == "nan" {
			return // documented residuals
		}
		_, err := strconv.ParseFloat(v, 64)
		valid := err == nil || errors.Is(err, strconv.ErrRange)
		if re.MatchString(v) != valid {
			t.Fatalf("%q: gate keeps=%v, ParseFloat err=%v", v, re.MatchString(v), err)
		}
	}
	for i := 0; i < 300000; i++ {
		check(randomValue(r, alphabet, 8))
		var b strings.Builder
		for n := r.Intn(9); n > 0; n-- {
			b.WriteString(tokens[r.Intn(len(tokens))])
		}
		check(b.String())
	}
}

// The duration gate keeps exactly the values time.ParseDuration (Loki's
// convertDuration) accepts, for short inputs (no overflow) of digits, signs,
// dots and the unit letters. Inputs of at most 7 characters cannot reach int64
// nanoseconds (999999h is the largest), so the overflow handled by the range
// filter (TestUnwrapGateRange) is not reached.
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
