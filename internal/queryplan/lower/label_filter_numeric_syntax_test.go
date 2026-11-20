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
		{logql.LabelFilterValueDuration, "0s", []string{"0", "00", "+00", "-00", "0s", "-0s", "+0ms", ".0s", "0.s", "00h", "1s", "-1s"}},
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

// Alias pairs use the same scale. Sharing their regexp branch must preserve
// case folding, byte suffixes, separators and the nearby values that do not
// compare equal.
// conformance: semantics/label-filter-numeric-equality
func TestNumericSizeSpellingAliases(t *testing.T) {
	for _, lit := range []string{"0B", "1KB", "1.5MiB", "10B", "1GiB"} {
		t.Run(lit, func(t *testing.T) {
			e := &logql.LabelFilterExpr{Kind: logql.LabelFilterValueBytes, Value: lit}
			pattern := regexp.MustCompile(exactSpellingsPattern(e))
			want, _ := lokiReadsAs(e.Kind, lit)
			for _, number := range []string{"0", "1", "1.0", "10", "1000", "1,000", "0.001", ".9765625", "1.5", "1536", "1572864", "1073741824"} {
				for _, unit := range []string{"", "k", "ki", "m", "mi", "g", "gi", "t", "ti", "p", "pi"} {
					reference := pattern.MatchString(number + unit)
					for _, suffix := range []string{"", "b", "B"} {
						for _, space := range []string{"", " ", "\u00a0", "\t"} {
							for _, name := range []string{unit, strings.ToUpper(unit)} {
								value := number + space + name + suffix + space
								got, valid := lokiReadsAs(e.Kind, value)
								if matched := pattern.MatchString(value); matched != reference || matched && (!valid || got != want) {
									t.Fatalf("%q: matched=%v, base spelling=%v, converted=%v, valid=%v, want=%v", value, matched, reference, got, valid, want)
								}
							}
						}
					}
				}
			}
		})
	}
}

// Positive examples are independent of the generated regexp. Otherwise a
// regexp matching nothing would pass the no-false-positives and alias checks.
// conformance: semantics/label-filter-numeric-equality
func TestNumericSpellingsPositiveExamples(t *testing.T) {
	for _, tc := range []struct {
		kind   logql.LabelFilterValueKind
		lit    string
		values []string
	}{
		{logql.LabelFilterValueDuration, "0s", []string{"0", "+0", "-0", "0s", "00h", ".0s", "0.s", "+0ms", "-0s"}},
		{logql.LabelFilterValueDuration, "5s", []string{"5s", "05.000s", "5000ms", "5000000us", "5000000µs"}},
		{logql.LabelFilterValueBytes, "0B", []string{"0", "0B", "0KB", "0 kiB", "0.0MiB", "0,0b"}},
		{logql.LabelFilterValueBytes, "1KB", []string{"1KB", "1k", "1 kb", "1,000B", "0.001mb", "1000"}},
		{logql.LabelFilterValueBytes, "1.5MiB", []string{"1.5MiB", "1.50 miB", "1536KiB", "1572864B"}},
	} {
		t.Run(tc.lit, func(t *testing.T) {
			e := &logql.LabelFilterExpr{Kind: tc.kind, Value: tc.lit}
			pattern := regexp.MustCompile(exactSpellingsPattern(e))
			for _, v := range tc.values {
				if !pattern.MatchString(v) {
					t.Errorf("positive spelling %q no longer matches %s", v, tc.lit)
				}
			}
		})
	}
}

// Factoring regexp branches may change their accepted language. Every matched
// spelling must still convert to the literal value; incomplete spellings remain
// an explicitly tracked compatibility gap rather than a false-positive match.
// conformance: semantics/label-filter-numeric-equality
func FuzzNumericEqualitySpellings(f *testing.F) {
	type spelling struct {
		kind    logql.LabelFilterValueKind
		pattern *regexp.Regexp
		want    float64
	}
	var cases []spelling
	for _, c := range []struct {
		kind logql.LabelFilterValueKind
		lit  string
	}{
		{logql.LabelFilterValueDuration, "0s"},
		{logql.LabelFilterValueDuration, "5s"},
		{logql.LabelFilterValueDuration, "1m30s"},
		{logql.LabelFilterValueBytes, "0B"},
		{logql.LabelFilterValueBytes, "1KB"},
		{logql.LabelFilterValueBytes, "1.5MiB"},
	} {
		e := &logql.LabelFilterExpr{Kind: c.kind, Value: c.lit}
		want, _ := lokiReadsAs(c.kind, c.lit)
		cases = append(cases, spelling{c.kind, regexp.MustCompile(exactSpellingsPattern(e)), want})
	}
	for _, v := range append(append(append([]string{"00", "+00", "-00", ",.,0", ",.0", "0000.000s"}, numericDurationValues...), numericBytesValues...), "1 KB\u00a0") {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 512 {
			return
		}
		for _, c := range cases {
			if c.pattern.MatchString(value) {
				got, valid := lokiReadsAs(c.kind, value)
				if !valid || got != c.want {
					t.Fatalf("%q matches %s but converts to %v (valid %v), want %v", value, c.pattern, got, valid, c.want)
				}
			}
		}
	})
}
