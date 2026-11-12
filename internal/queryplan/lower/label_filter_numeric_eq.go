package lower

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// Numeric, duration and bytes equality (`=`, `==`, `!=`) in a label filter after a json or logfmt parser.
//
// Loki reads the label's value as a number, a Go duration or a bytes size and compares it with the literal as
// numbers: `status == 200` keeps 200, 200.0 and 2e2. A value that is none of these is kept with an __error__
// label, and a missing or empty label is dropped under both operators. The string translator writes the exact
// text instead (`status:="200"`), which keeps 200 alone and, under `!=`, keeps the entries without the label.
//
// VictoriaLogs compares a field's value as a number in its range filters, reading a plain number, a duration
// (5s, 1m30s) or a bytes size (1KB, 1MiB) the same way, so equality is the pair `field:>=v field:<=v` and
// inequality is the same pair negated on the entries that have the field. The bound is written as the plain
// number of nanoseconds or bytes, because VictoriaLogs reads the units of a bound by its own table (it reads `us`
// as text, which turns the bound into a string range). The values VictoriaLogs reads as numbers that Loki marks
// with __error__ (0xC8, 1_000 for a bytes size), and the spellings of a duration or a size Loki reads and
// VictoriaLogs does not (100us, 1 KB, 1kb), are the difference that remains
// (semantics/label-filter-numeric-equality).
//
// The literal is written only when it has one reading in both: a plain decimal number, a duration in Go's syntax
// and a bytes size whose byte count is a whole number. Any other literal (hexadecimal, octal, separators, whole
// days, weeks or years, a fractional byte count) stays with the string translator.

// numericEqualityConstruct names the literals of a numeric equality the lowering does not write.
const numericEqualityConstruct = "LogSelect label filter numeric equality literal form"

// isNumericEquality reports a leaf that compares a number, duration or bytes size with =, == or !=.
func isNumericEquality(e *logql.LabelFilterExpr) bool {
	switch e.Kind {
	case logql.LabelFilterValueNumber, logql.LabelFilterValueDuration, logql.LabelFilterValueBytes:
		return e.Cmp == "=" || e.Cmp == "==" || e.Cmp == "!="
	}
	return false
}

// plainNumber reports a decimal number with an optional sign, fraction and exponent: the literals Go's float
// parsing and VictoriaLogs read as the same number.
func plainNumber(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	digits := 0
	for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		digits++
	}
	if i < len(s) && s[i] == '.' {
		for i++; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		exp := 0
		for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
			exp++
		}
		if exp == 0 {
			return false
		}
	}
	return i == len(s)
}

// splitBytesLiteral splits a bytes size literal into its number (digits and at most one fraction) and its unit.
func splitBytesLiteral(s string) (number, unit string, ok bool) {
	i := 0
	for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
	}
	if i == 0 {
		return "", "", false
	}
	if i < len(s) && s[i] == '.' {
		j := i + 1
		for ; j < len(s) && s[j] >= '0' && s[j] <= '9'; j++ {
		}
		if j == i+1 {
			return "", "", false
		}
		i = j
	}
	return s[:i], s[i:], true
}

// bytesUnitSize is the size of each bytes unit by its lower case name, as Loki's size parser takes them.
var bytesUnitSize = map[string]float64{
	"": 1, "b": 1,
	"k": 1e3, "kb": 1e3, "m": 1e6, "mb": 1e6, "g": 1e9, "gb": 1e9, "t": 1e12, "tb": 1e12, "p": 1e15, "pb": 1e15,
	"ki": 1 << 10, "kib": 1 << 10, "mi": 1 << 20, "mib": 1 << 20, "gi": 1 << 30, "gib": 1 << 30,
	"ti": 1 << 40, "tib": 1 << 40, "pi": 1 << 50, "pib": 1 << 50,
}

// maxExactBound is the largest whole number a float64 holds exactly.
const maxExactBound = 1 << 53

// numericEqualityBound is the number the literal of a numeric equality compares with, as VictoriaLogs reads it: the
// plain number of the literal, of nanoseconds or of bytes. ok is false for a literal the lowering does not write.
func numericEqualityBound(e *logql.LabelFilterExpr) (bound string, ok bool) {
	switch e.Kind {
	case logql.LabelFilterValueNumber:
		if !plainNumber(e.Value) {
			return "", false
		}
		f, err := strconv.ParseFloat(e.Value, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return "", false
		}
		if f == 0 {
			return "0", true // -0
		}
		bound = strconv.FormatFloat(f, 'f', -1, 64)
		return bound, len(bound) <= 32
	case logql.LabelFilterValueDuration:
		d, err := time.ParseDuration(e.Value)
		if err != nil || d < 0 || d >= maxExactBound {
			return "", false
		}
		return strconv.FormatInt(int64(d), 10), true
	case logql.LabelFilterValueBytes:
		number, unit, ok := splitBytesLiteral(e.Value)
		if !ok || len(unit) > 3 {
			return "", false
		}
		var lower [3]byte
		for i := 0; i < len(unit); i++ {
			c := unit[i]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			lower[i] = c
		}
		size, known := bytesUnitSize[string(lower[:len(unit)])]
		f, err := strconv.ParseFloat(number, 64)
		if !known || err != nil {
			return "", false
		}
		n := f * size
		if n != math.Trunc(n) || n >= maxExactBound {
			return "", false // a fractional byte count: Loki and VictoriaLogs round it their own ways
		}
		return strconv.FormatInt(int64(n), 10), true
	}
	return "", false
}

// numericEqualityFilter is the filter of a numeric equality on field (translated, and quoted where LogsQL needs
// it) for a literal numericEqualityBound accepted: the entries whose value equals the bound as a number, or, for
// !=, the entries that have a value and whose value does not.
func numericEqualityFilter(field string, e *logql.LabelFilterExpr) logsql.FilterExpr {
	bound, _ := numericEqualityBound(e)
	equal := logsql.Group{Expr: logsql.AllOf{
		logsql.FieldFilter{Field: field, Op: logsql.FieldOpGTE, Value: bound},
		logsql.FieldFilter{Field: field, Op: logsql.FieldOpLTE, Value: bound},
	}}
	if e.Cmp != "!=" {
		return equal
	}
	return logsql.Group{Expr: logsql.AllOf{
		logsql.FieldFilter{Field: field, Op: logsql.FieldOpAny},
		logsql.Exclude{Expr: equal},
	}}
}

// parsedFieldLeaf is the filter of one leaf on a parsed key's field: the numeric comparison for a numeric
// equality, else the filter of a leaf without a parser (translate.LabelFilterLeaf).
func parsedFieldLeaf(field string, e *logql.LabelFilterExpr, caps logsql.Capabilities) logsql.FilterExpr {
	if isNumericEquality(e) {
		return numericEqualityFilter(field, e)
	}
	f, _ := translate.LabelFilterLeaf(field, e.Cmp, e.Value, e.Kind == logql.LabelFilterValueIP, caps) // the operator is known
	return f
}

// leafFilter is the filter of a leaf on one field after a parser: the numeric comparison of a numeric equality after a
// json or logfmt parser, else labelFilterLeaf's (a regexp or pattern parser's labels are text, written as the
// translator writes them).
func (lb *leafBuilder) leafFilter(e *logql.LabelFilterExpr, labelFn func(string) string) logsql.FilterExpr {
	if lb.keyParsed {
		return parsedLabelLeaf(e, labelFn, lb.caps)
	}
	return labelFilterLeaf(e, labelFn, lb.caps)
}

// parsedLabelLeaf is labelFilterLeaf for a leaf after a json or logfmt parser: the label named like the selector
// names it, and the numeric comparison for a numeric equality.
func parsedLabelLeaf(e *logql.LabelFilterExpr, labelFn func(string) string, caps logsql.Capabilities) logsql.FilterExpr {
	if !isNumericEquality(e) {
		return labelFilterLeaf(e, labelFn, caps)
	}
	label := translate.SanitizeFieldIdentifier(e.Name)
	if labelFn != nil {
		label = translate.SanitizeFieldIdentifier(labelFn(label))
	}
	if strings.ContainsAny(label, ".-") {
		label = `"` + label + `"`
	} else {
		label = logsql.QuoteKeywordField(label)
	}
	return numericEqualityFilter(label, e)
}

// WritesNumericEquality reports a plan with a numeric, duration or bytes equality label filter after a json or
// logfmt parser that the lowering writes as a numeric comparison. The string translator writes the exact text of
// the literal for these, which VictoriaLogs compares as text, so the shadows do not hold the lowering to its text
// there (semantics/label-filter-numeric-equality).
func WritesNumericEquality(p *queryplan.Plan) bool {
	return writesNumericEquality(p.Root)
}

func writesNumericEquality(n queryplan.Node) bool {
	switch n := n.(type) {
	case *queryplan.LogSelect:
		return selectWritesNumericEquality(n)
	case *queryplan.RangeAgg:
		return n.In != nil && selectWritesNumericEquality(n.In)
	case *queryplan.VectorAgg:
		return writesNumericEquality(n.In)
	case *queryplan.Rank:
		return writesNumericEquality(n.In)
	case *queryplan.LabelReplace:
		return writesNumericEquality(n.In)
	case *queryplan.BinOp:
		return writesNumericEquality(n.L) || writesNumericEquality(n.R)
	}
	return false
}

func selectWritesNumericEquality(sel *queryplan.LogSelect) bool {
	keyParsed := false
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Parse:
			keyParsed = keyParsed || st.Type == logql.ParserJSON || st.Type == logql.ParserLogfmt
		case *queryplan.LabelFilter:
			if keyParsed && st.Expr != nil && exprWritesNumericEquality(st.Expr) {
				return true
			}
		}
	}
	return false
}

func exprWritesNumericEquality(e *logql.LabelFilterExpr) bool {
	if !e.IsLeaf() {
		return exprWritesNumericEquality(e.Left) || exprWritesNumericEquality(e.Right)
	}
	if !isNumericEquality(e) {
		return false
	}
	_, ok := numericEqualityBound(e)
	return ok
}
