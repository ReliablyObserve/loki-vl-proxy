package lower

import (
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// lokiReadsAs is the value Loki compares with the literal for a value of kind, as its label filters read it: Go's
// float parsing for a number, Go's duration parsing for a duration, and for a size the digits, dots and commas, then
// a unit name of bytesUnitSize in any case between blanks, the float product truncated to a whole byte count.
func lokiReadsAs(kind logql.LabelFilterValueKind, v string) (float64, bool) {
	switch kind {
	case logql.LabelFilterValueDuration:
		d, err := time.ParseDuration(v)
		return float64(d), err == nil
	case logql.LabelFilterValueBytes:
		n := 0
		for n < len(v) && (v[n] >= '0' && v[n] <= '9' || v[n] == '.' || v[n] == ',') {
			n++
		}
		f, err := strconv.ParseFloat(strings.ReplaceAll(v[:n], ",", ""), 64)
		size, ok := bytesUnitSize[strings.ToLower(strings.TrimSpace(v[n:]))]
		if err != nil || !ok {
			return 0, false
		}
		return float64(uint64(f * size)), true
	}
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

// The syntax guard of each kind accepts what Loki's reading accepts, and the spellings' regexp matches only values
// Loki reads as the literal's value, on the values of the oracle grid and spellings around them.
//
// conformance: semantics/label-filter-numeric-equality
func TestNumericSyntaxAndSpellings(t *testing.T) {
	for _, c := range []struct {
		kind logql.LabelFilterValueKind
		lit  string
		vals []string
	}{
		{logql.LabelFilterValueNumber, "200", append(append([]string{}, numericNumberValues...), "2_0_0", "0x_c8p0", "1e2_0")},
		{logql.LabelFilterValueNumber, "-3", numericNumberValues},
		{logql.LabelFilterValueDuration, "5s", append(append([]string{}, numericDurationValues...), "+05.000s", "5.0000000000s", "05000ms", "4s1000ms")},
		{logql.LabelFilterValueDuration, "1m30s", numericDurationValues},
		{logql.LabelFilterValueDuration, "0s", []string{"0", "0s", "-0s", "+0ms", ".0s", "0.s", "00h", "1s", "-1s"}},
		{logql.LabelFilterValueBytes, "1KB", append(append([]string{}, numericBytesValues...), ",1,000,", "0.001MB", "1000.0b ", "1.0004KB")},
		{logql.LabelFilterValueBytes, "1.5MiB", numericBytesValues},
		{logql.LabelFilterValueBytes, "10B", numericBytesValues},
	} {
		t.Run(c.lit, func(t *testing.T) {
			e := &logql.LabelFilterExpr{Name: "x", Cmp: "=", Kind: c.kind, Value: c.lit}
			want, ok := lokiReadsAs(c.kind, c.lit)
			if !ok {
				t.Fatalf("Loki does not read the literal %q", c.lit)
			}
			guard := regexp.MustCompile(numericSyntaxPattern(e))
			var spell *regexp.Regexp
			if p := exactSpellingsPattern(e); p != "" {
				spell = regexp.MustCompile(p)
			}
			for _, v := range c.vals {
				got, reads := lokiReadsAs(c.kind, v)
				if f, err := strconv.ParseFloat(v, 64); c.kind == logql.LabelFilterValueNumber && (math.IsInf(f, 0) || math.IsNaN(f)) && (err == nil || errors.Is(err, strconv.ErrRange)) {
					// Infinities, NaN and a number out of float range never equal a finite literal: the guard need not
					// name them.
					continue
				}
				if guard.MatchString(v) != reads {
					t.Errorf("%q: guard %v, Loki reads it %v", v, guard.MatchString(v), reads)
				}
				if spell != nil && spell.MatchString(v) && (!reads || got != want) {
					t.Errorf("%q: the spellings match it, Loki reads %v (%v), the literal %v", v, got, reads, want)
				}
			}
		})
	}
}
