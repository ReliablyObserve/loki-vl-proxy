package translator

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The unwrap gate (the pipes that keep the rows of an unwrapped field that make a Loki sample and convert the
// value) is built in internal/translate, where the query plan's lowering shares it; the names below forward to it.

const (
	// UnwrapValueAlias is the field holding the converted unwrapped value.
	UnwrapValueAlias = translate.UnwrapValueAlias
	// UnwrapNumberPattern is the syntax of strconv.ParseFloat (see translate.UnwrapNumberPattern).
	UnwrapNumberPattern = translate.UnwrapNumberPattern
	// UnwrapInfinityPattern is the word the math pipe does not read.
	UnwrapInfinityPattern = translate.UnwrapInfinityPattern
	// UnwrapDurationPattern is time.ParseDuration's syntax.
	UnwrapDurationPattern = translate.UnwrapDurationPattern
	// UnwrapBytesPattern is humanize.ParseBytes's syntax.
	UnwrapBytesPattern = translate.UnwrapBytesPattern
)

// UnwrapInRange returns the LogsQL filter a converted value of the conversion conv satisfies.
func UnwrapInRange(conv string) string { return translate.UnwrapInRange(conv) }

// UnwrapPattern returns the expression the gate of the unwrap conversion conv keeps a row by.
func UnwrapPattern(conv string) string { return translate.UnwrapPattern(conv) }

// UnwrapGate returns the pipes that select the rows of an unwrapped field that make a sample.
func UnwrapGate(field string) string { return translate.UnwrapGate(field) }

// UnwrapPatternWithoutInfinity is UnwrapPattern without the word infinity.
func UnwrapPatternWithoutInfinity(conv string) string {
	return translate.UnwrapPatternWithoutInfinity(conv)
}

// UnwrapPlainConvert returns the pipes that convert a plain unwrapped value into UnwrapValueAlias.
func UnwrapPlainConvert(field, infinityIf string) string {
	return translate.UnwrapPlainConvert(field, infinityIf)
}

// UnwrapGateFor is UnwrapGate for the unwrap conversion conv.
func UnwrapGateFor(field, conv string) string { return translate.UnwrapGateFor(field, conv) }

// UnwrapGateParts returns the pipes of the gate of UnwrapGateFor in two pieces.
func UnwrapGateParts(field, conv string) (match, convert string) {
	return translate.UnwrapGateParts(field, conv)
}

// SplitUnwrapGate splits a translated query that ends with an unwrap gate into the query before it and the
// unwrapped field.
func SplitUnwrapGate(query string) (base, field string, ok bool) {
	return translate.SplitUnwrapGate(query)
}

// UnwrapConv returns the conversion of the unwrap function converter (a LogQL parser's UnwrapStage.Converter).
func UnwrapConv(converter string) string { return translate.UnwrapConv(converter) }

// unwrapGateField returns the field and conversion to gate for the unwrap in
// inner: ("", "") for a query without unwrap.
func unwrapGateField(inner, field string) (gateField, conv string) {
	if field == "" {
		return "", ""
	}
	idx := strings.Index(inner, "| unwrap ")
	if idx < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(inner[idx+len("| unwrap "):])
	for _, c := range translate.UnwrapConversions {
		if strings.HasPrefix(rest, c.Call) {
			return field, c.Conv
		}
	}
	return field, ""
}
