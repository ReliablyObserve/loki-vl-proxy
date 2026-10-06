package translator

import (
	"regexp"
	"strconv"
	"strings"
)

// Unwrap sample validity (Loki v3.7.7 pkg/logql/log/metrics_extraction.go,
// streamLabelSampleExtractor.Process): a line whose unwrapped label is absent
// or empty makes no sample, and a value its conversion rejects makes a sample
// marked __error__="SampleExtractionErr". VictoriaLogs' stats functions parse
// values leniently (a unit such as "86282s" or "1KiB" becomes a number, min and
// max compare strings, a group without a value answers "" or NaN), so every stats
// pipe over an unwrapped label first keeps the rows whose value converts in
// Loki's own syntax, with the value converted into UnwrapValueAlias by the math
// pipe (which parses each row the same way whatever the other rows hold):
//
//   - plain: strconv.ParseFloat's decimal syntax;
//   - duration(): time.ParseDuration's syntax, in seconds;
//   - bytes(): humanize.ParseBytes's syntax (case-insensitive SI and IEC units,
//     an optional space, commas in the number), in bytes.
//
// The filter is one regular-expression test of the field per row.

const (
	// UnwrapValueAlias is the field holding the converted unwrapped value.
	UnwrapValueAlias = "__lvp_v"
	// UnwrapNumberPattern is the decimal syntax of strconv.ParseFloat (digits, with
	// underscores between them, an optional point, an optional sign) plus the
	// case-insensitive inf word the math pipe also reads, with a decimal exponent of
	// at most 307 (a larger one overflows float64, which ParseFloat reports as an
	// error). Known differences from ParseFloat, all dropped by the filter:
	// hexadecimal floats, "infinity", nan (VictoriaLogs' sum restarts at a NaN where
	// Loki's sum is NaN), an exponent with underscores, the exponents 308 and 309 of
	// a small mantissa, and a mantissa of many digits whose exponent still overflows.
	UnwrapNumberPattern = `^[+-]?(?:[0-9]+(?:_[0-9]+)*(?:[.](?:[0-9]+(?:_[0-9]+)*)?)?|[.][0-9]+(?:_[0-9]+)*)(?:[eE](?:[+]?0*(?:[0-9]{1,2}|[12][0-9]{2}|30[0-7])|-[0-9]+))?$|^[+-]?(?i:inf)$`
	// UnwrapDurationPattern is time.ParseDuration's syntax: a sign, then "0" or
	// terms of a number and one of ns, us, µs (U+00B5 and U+03BC), ms, s, m, h.
	UnwrapDurationPattern = `^[+-]?(?:0|(?:(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:ns|us|µs|μs|ms|s|m|h))+)$`
	// UnwrapBytesPattern is humanize.ParseBytes's syntax up to its 64-bit limit: a
	// number with commas (not a leading one) and an optional fraction, optional
	// spaces, an optional SI or IEC prefix (k m g t p e, with i) and an optional b,
	// in any case, optional trailing spaces.
	UnwrapBytesPattern = `^(?:[0-9][0-9,]*(?:[.][0-9]*)?|[.][0-9]+)\s*(?i:(?:[kmgtpe]i?)?b?)\s*$`
)

// UnwrapGate returns the pipes that select the rows of an unwrapped field that
// make a sample and convert the value into UnwrapValueAlias. The field is quoted:
// the math pipe reads a bare name such as max, abs or rand as a function, and
// stops a bare name at a minus or a slash.
func UnwrapGate(field string) string { return UnwrapGateFor(field, "") }

// UnwrapGateFor is UnwrapGate for the unwrap conversion conv: "" (a plain
// number), "duration" or "bytes".
func UnwrapGateFor(field, conv string) string {
	q := strconv.Quote(field)
	switch conv {
	case "duration":
		return " | filter " + q + ":~" + strconv.Quote(UnwrapDurationPattern) +
			" | copy " + q + " as __lvp_d" +
			// time.ParseDuration reads a leading plus, "us", the Greek mu, a leading point
			// (".5s") and a trailing one ("5.s"); the math pipe reads none of them.
			` | replace_regexp if (__lvp_d:~"^[+]") ("^[+]","") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"us|μ") ("([0-9.])(?:us|μs)","${1}µs") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"[.]") ("(^|[^0-9])[.]([0-9])","${1}0.${2}") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"[.]") ("([0-9])[.]([^0-9])","${1}${2}") at __lvp_d` +
			" | math __lvp_d/1000000000 as " + UnwrapValueAlias
	case "bytes":
		// The number and the unit are read apart: the unit's prefix letter is a power
		// of 1000, or of 1024 with an i, and the product is floored as ParseBytes does.
		return " | filter " + q + ":~" + strconv.Quote(UnwrapBytesPattern) +
			" | copy " + q + " as __lvp_n" +
			` | replace_regexp if (__lvp_n:~"[,\\s]") ("[,\\s]","") at __lvp_n` +
			` | extract_regexp if (__lvp_n:~"[A-Za-z]") "^(?P<__lvp_q>[0-9.]*)(?i:(?P<__lvp_p>[kmgtpe]?)(?P<__lvp_i>i?)b?)$" from __lvp_n` +
			` | format if (__lvp_q:*) "<__lvp_q>" as __lvp_n` +
			` | format "0" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^k$") "1" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^m$") "2" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^g$") "3" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^t$") "4" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^p$") "5" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^e$") "6" as __lvp_e` +
			` | format "1000" as __lvp_b` +
			` | format if (__lvp_i:~"(?i)^i$") "1024" as __lvp_b` +
			" | math floor(__lvp_n * (__lvp_b ^ __lvp_e)) as " + UnwrapValueAlias
	}
	return " | filter " + q + ":~" + strconv.Quote(UnwrapNumberPattern) + " | math " + q + " as " + UnwrapValueAlias
}

// unwrapGateMarkerRE reads the quoted field of the filter that starts a gate.
var unwrapGateMarkerRE = regexp.MustCompile(`^ \| filter ("(?:[^"\\]|\\.)*"):~`)

// SplitUnwrapGate splits a translated query that ends with an unwrap gate
// (UnwrapGateFor, any conversion) into the query before it and the unwrapped
// field. ok is false for a query without the gate.
func SplitUnwrapGate(query string) (base, field string, ok bool) {
	idx := strings.LastIndex(query, ` | filter "`)
	if idx < 0 {
		return query, "", false
	}
	m := unwrapGateMarkerRE.FindStringSubmatch(query[idx:])
	if m == nil {
		return query, "", false
	}
	field, err := strconv.Unquote(m[1])
	if err != nil {
		return query, "", false
	}
	for _, conv := range []string{"", "duration", "bytes"} {
		if query[idx:] == UnwrapGateFor(field, conv) {
			return query[:idx], field, true
		}
	}
	return query, "", false
}

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
	switch {
	case strings.HasPrefix(rest, "duration("):
		return field, "duration"
	case strings.HasPrefix(rest, "bytes("):
		return field, "bytes"
	}
	return field, ""
}
