package proxy

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The bytes gate keeps exactly the values humanize.ParseBytes (Loki's
// convertBytes; parseHumanBytes is its port) accepts, for short inputs (no
// 64-bit overflow) of digits, dots, commas, spaces and unit letters. The forms
// it leaves out on purpose are documented in the registry case
// semantics/unwrap-gate-parsefloat-divergences and skipped here; any other
// disagreement is a new divergence a later rewrite of the gate must not add.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapBytesPatternAgreesWithParseBytesOnGeneratedInputs(t *testing.T) {
	re := regexp.MustCompile(translator.UnwrapBytesPattern)
	r := rand.New(rand.NewSource(3))
	alphabet := []rune("0123456789., kKmMgGiIbBeE")
	for i := 0; i < 200000; i++ {
		n := r.Intn(7)
		buf := make([]rune, n)
		for j := range buf {
			buf[j] = alphabet[r.Intn(len(alphabet))]
		}
		v := string(buf)
		_, err := parseHumanBytes(v)
		if err != nil && strings.HasPrefix(err.Error(), "too large") {
			continue // documented: a size of 2^64 bytes or more passes the gate
		}
		num := strings.TrimLeft(v, " ")
		if strings.HasPrefix(num, ",") || (strings.Contains(num, ".") && strings.Contains(num[strings.Index(num, "."):], ",")) {
			continue // documented: a leading comma or a comma after the dot (`,5`, `1.5,0`)
		}
		keeps := re.MatchString(v)
		if keeps != (err == nil) {
			t.Fatalf("%q: gate keeps=%v, ParseBytes ok=%v (%v)", v, keeps, err == nil, err)
		}
	}
}
