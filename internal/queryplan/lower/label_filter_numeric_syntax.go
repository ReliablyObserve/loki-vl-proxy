package lower

import (
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The value Loki compares with the literal of a numeric equality is the label's text read the way of the literal's
// kind: a number with Go's float parsing, a duration with Go's duration parsing, a size with the size parser Loki
// uses (digits, dots and commas, then a unit after any blanks, in any case). VictoriaLogs' range filter reads every
// field value in one value space of its own: it reads `1KB` as a number and `1000000000` as a duration, which Loki
// marks with __error__, and it does not read `1kb`, `1 KB` or `5000000us`, which Loki reads as the literal's value.
//
// The numeric comparison is therefore written in two parts, both decided by VictoriaLogs:
//   - the range pair, guarded by a regexp of Loki's syntax for the literal's kind (numericSyntaxPattern): a value
//     VictoriaLogs reads as the bound but Loki does not read at all is left out;
//   - a regexp of the spellings Loki reads as exactly the literal's value in one unit (exactSpellingsPattern): the
//     value written in a unit VictoriaLogs does not read, in any letter case, with blanks, commas and leading zeros
//     where Loki's parser takes them. Each spelling is checked with Loki's own arithmetic (Go's duration parsing, the
//     size parser's float product truncated to a whole byte count) before it is written.
//
// What neither part decides is left (semantics/label-filter-numeric-equality): a duration of several units spelled
// with one VictoriaLogs does not read (`1m30000000us`), a size or a duration with more fraction digits than its value
// needs (`1.0004KB`, which the size parser truncates to 1000), and a present but empty json value under !=, which
// Loki keeps with __error__ and VictoriaLogs stores as no field.

// Loki's syntax of each kind, anchored: what Go's float parsing, Go's duration parsing and the size parser read
// without an error. A number may be a decimal or a hexadecimal float (with a p exponent), its digits separated by
// underscores as in Go's literals; the special values inf and nan are left out, as they never equal a finite literal. The blanks of a size are the ones Go's TrimSpace removes.
const (
	numberSyntax = `^[+-]?(?:(?:` + decimalDigits + `(?:\.(?:` + decimalDigits + `)?)?|\.` + decimalDigits + `)(?:[eE][+-]?` + decimalDigits + `)?|0[xX](?:_?` + hexDigits + `(?:\.(?:` + hexDigits + `)?)?|\.` + hexDigits + `)[pP][+-]?` + decimalDigits + `)$`
	// decimalDigits and hexDigits are runs of digits an underscore may separate, as in Go's number literals.
	decimalDigits  = `[0-9](?:_?[0-9])*`
	hexDigits      = `[0-9a-fA-F](?:_?[0-9a-fA-F])*`
	durationSyntax = `^[-+]?(?:0|(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:ns|us|µs|μs|ms|s|m|h))+)$`
	sizeBlanks     = `[\t\n\v\f\r \x{85}\x{A0}\p{Z}]*`
)

// sizeSyntax is the anchored syntax of a size: a number of digits, dots and commas the float parser reads once the
// commas are gone, then a unit of bytesUnitSize in any case (or none) between blanks.
var sizeSyntax = `^,*(?:[0-9],*)*(?:[0-9],*\.?|\.,*[0-9]),*(?:[0-9],*)*` + sizeBlanks + `(?i:` + strings.Join(sizeUnitNames(), "|") + `)?` + sizeBlanks + `$`

// sizeUnitNames are the unit names of bytesUnitSize, longest first so the alternation reads the whole name.
func sizeUnitNames() []string {
	var names []string
	for name := range bytesUnitSize {
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		if len(names[i]) != len(names[j]) {
			return len(names[i]) > len(names[j])
		}
		return names[i] < names[j]
	})
	return names
}

// numericSyntaxPattern is the regexp of Loki's syntax for the kind of e's literal.
func numericSyntaxPattern(e *logql.LabelFilterExpr) string {
	switch e.Kind {
	case logql.LabelFilterValueDuration:
		return durationSyntax
	case logql.LabelFilterValueBytes:
		return sizeSyntax
	}
	return numberSyntax
}

// durationUnits are Go's duration units and their length in nanoseconds.
var durationUnits = []struct {
	name string
	ns   int64
}{
	{"ns", 1}, {"us", 1e3}, {"µs", 1e3}, {"μs", 1e3}, {"ms", 1e6}, {"s", 1e9}, {"m", 60e9}, {"h", 3600e9},
}

// exactDecimal is v/unit written as a decimal with no exponent, or false when it does not end (v/60 seconds, say).
func exactDecimal(v, unit int64) (intPart, frac string, ok bool) {
	r := new(big.Rat).SetFrac(big.NewInt(v), big.NewInt(unit))
	for digits := 0; digits <= 20; digits++ {
		s := r.FloatString(digits)
		back, _ := new(big.Rat).SetString(s)
		if back.Cmp(r) == 0 {
			i, f, _ := strings.Cut(s, ".")
			return i, strings.TrimRight(f, "0"), true
		}
	}
	return "", "", false
}

// exactSpellingsPattern is the anchored regexp of the spellings Loki reads as exactly the value of e's literal in one
// unit, for a duration or a size; "" for a number (VictoriaLogs reads every decimal spelling Loki reads) and for a
// literal whose bound the lowering does not write.
func exactSpellingsPattern(e *logql.LabelFilterExpr) string {
	bound, ok := numericEqualityBound(e)
	if !ok {
		return ""
	}
	v, err := strconv.ParseInt(bound, 10, 64)
	if err != nil {
		return ""
	}
	switch e.Kind {
	case logql.LabelFilterValueDuration:
		return durationSpellings(v)
	case logql.LabelFilterValueBytes:
		return sizeSpellings(v)
	}
	return ""
}

// durationSpellings are the one-unit spellings of v nanoseconds Go's duration parsing reads as v: the decimal v/unit
// with leading zeros, a + sign (or either sign for zero), and as many trailing zeros as keep the parser's fraction
// product exact (fraction digits up to the unit's nanosecond digits).
func durationSpellings(v int64) string {
	var alts []string
	sign := `\+?`
	if v == 0 {
		sign = `[-+]?`
		alts = append(alts, "0")
	}
	for _, u := range durationUnits {
		intPart, frac, ok := exactDecimal(v, u.ns)
		if !ok {
			continue
		}
		maxFrac := len(strconv.FormatInt(u.ns, 10)) - 1
		if u.ns%10 != 0 {
			maxFrac = 0
		}
		if len(frac) > maxFrac {
			continue
		}
		spelled := intPart
		if frac != "" {
			spelled += "." + frac
		}
		if d, err := time.ParseDuration(spelled + u.name); err != nil || int64(d) != v {
			continue
		}
		alts = append(alts, "(?:"+mantissaPattern(intPart, frac, maxFrac-len(frac), false)+")"+u.name)
	}
	if len(alts) == 0 {
		return ""
	}
	return "^" + sign + "(?:" + strings.Join(alts, "|") + ")$"
}

// sizeSpellings are the one-unit spellings of v bytes the size parser reads as v: the decimal v/unit with leading
// and trailing zeros and commas anywhere in the number, the unit in any case between blanks.
func sizeSpellings(v int64) string {
	var alts []string
	names := append(sizeUnitNames(), "")
	for _, name := range names {
		size := bytesUnitSize[name]
		if size != math.Trunc(size) {
			continue
		}
		intPart, frac, ok := exactDecimal(v, int64(size))
		if !ok || len(intPart)+len(frac) > 17 {
			// Past 17 digits the float parser rounds the number: no spelling there is the value's exactly.
			continue
		}
		spelled := intPart
		if frac != "" {
			spelled += "." + frac
		}
		f, err := strconv.ParseFloat(spelled, 64)
		if err != nil || f*size >= maxExactBound || int64(f*size) != v {
			continue
		}
		unit := ""
		if name != "" {
			unit = `(?i:` + name + `)`
		}
		alts = append(alts, "(?:"+mantissaPattern(intPart, frac, -1, true)+")"+sizeBlanks+unit)
	}
	if len(alts) == 0 {
		return ""
	}
	return "^(?:" + strings.Join(alts, "|") + ")" + sizeBlanks + "$"
}

// mantissaPattern is the regexp of the decimal intPart.frac with any leading zeros, up to extraZeros trailing zeros
// (any number for -1), and, with commas, a comma anywhere in the number (the size parser drops them). A zero integer
// part may be left out when the fraction holds a digit.
func mantissaPattern(intPart, frac string, extraZeros int, commas bool) string {
	gap, zeros := "", "0*"
	if commas {
		gap, zeros = ",*", "[0,]*"
	}
	digits := func(s string) string {
		var b strings.Builder
		for _, c := range s {
			b.WriteRune(c)
			b.WriteString(gap)
		}
		return b.String()
	}
	trailing := zeros
	if extraZeros >= 0 {
		trailing = "0{0," + strconv.Itoa(extraZeros) + "}"
		if commas {
			trailing = "(?:0,*){0," + strconv.Itoa(extraZeros) + "}"
		}
	}
	lead := gap + zeros
	switch {
	case intPart == "0" && frac == "":
		// Zero: at least one zero digit, then an optional fraction of zeros.
		return lead + "0" + gap + `(?:\.` + gap + trailing + `)?|` + lead + `\.` + gap + "0" + gap + trailing
	case intPart == "0":
		return lead + `\.` + gap + digits(frac) + trailing
	case frac == "":
		return lead + digits(intPart) + `(?:\.` + gap + trailing + `)?`
	}
	return lead + digits(intPart) + `\.` + gap + digits(frac) + trailing
}
