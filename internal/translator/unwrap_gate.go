package translator

import (
	"math"
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
	// UnwrapNumberPattern is the syntax of strconv.ParseFloat: decimal floats (digits
	// with underscores between them, an optional point, an optional sign, an
	// exponent whose digits may carry underscores) and hexadecimal floats (0x, hex
	// digits with an optional point and underscores, the mandatory p exponent), plus
	// the case-insensitive inf and infinity words. The expression is
	// syntactic: a value ParseFloat reports as out of range (1e309, 1000e307, 0x1p2000)
	// matches it and is dropped by the range filter after the math pipe, which
	// VictoriaLogs answers NaN for. The math pipe does not read "infinity", so
	// UnwrapGateFor spells it inf. Left out (registered in
	// semantics/unwrap-gate-parsefloat-divergences): nan.
	UnwrapNumberPattern = `^[+-]?(?:` + unwrapDecimalFloat + `|` + unwrapHexFloat + `)$|^[+-]?(?i:inf(?:inity)?)$`
	unwrapDecimalFloat  = `(?:[0-9]+(?:_[0-9]+)*(?:[.](?:[0-9]+(?:_[0-9]+)*)?)?|[.][0-9]+(?:_[0-9]+)*)(?:[eE][+-]?[0-9]+(?:_[0-9]+)*)?`
	unwrapHexFloat      = `0[xX](?:_?[0-9a-fA-F]+(?:_[0-9a-fA-F]+)*(?:[.](?:[0-9a-fA-F]+(?:_[0-9a-fA-F]+)*)?)?|[.][0-9a-fA-F]+(?:_[0-9a-fA-F]+)*)[pP][+-]?[0-9]+(?:_[0-9]+)*`
	// UnwrapDurationPattern is time.ParseDuration's syntax: a sign, then "0" or
	// terms of a number and one of ns, us, µs (U+00B5 and U+03BC), ms, s, m, h. A
	// duration beyond int64 nanoseconds (2562048h) matches it and is dropped by the
	// range filter after the math pipe (VictoriaLogs saturates it).
	UnwrapDurationPattern = `^[+-]?(?:0|(?:(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:ns|us|µs|μs|ms|s|m|h))+)$`
	// UnwrapBytesPattern is humanize.ParseBytes's syntax: a number whose digit run
	// may hold commas anywhere (humanize drops them all; a leading one and one after
	// the point included) and one point, any Unicode space (strings.TrimSpace) around
	// an optional SI or IEC unit (k m g t p e, with i, and an optional b; compared
	// after strings.ToLower, so the Kelvin sign reads as k and U+0130 as i). A size
	// of 2^64 bytes or more matches it and is dropped by the range filter.
	UnwrapBytesPattern = `^,*(?:[0-9][0-9,]*(?:[.][0-9,]*)?|[.][,]*[0-9][0-9,]*)` + unwrapSpace + `*(?i:(?:[kmgtpe][iİ]?)?b?)` + unwrapSpace + `*$`
	// unwrapSpace is unicode.IsSpace: ASCII white space including the vertical
	// tab, U+0085 and every Unicode separator (RE2's \s is only ASCII, without \v).
	unwrapSpaceClass = `\t-\r\x{85}\p{Z}`
	unwrapSpace      = `[` + unwrapSpaceClass + `]`
)

// unwrapDurationLimit is the saturation value of VictoriaLogs' duration reading in
// seconds: int64 nanoseconds, where time.ParseDuration reports an overflow. The
// shortest decimal that reads back as the same float64 is the filter threshold.
var unwrapDurationLimit = strconv.FormatFloat(float64(math.MaxInt64)/1e9, 'g', -1, 64)

// UnwrapInRange returns the LogsQL filter a converted value (UnwrapValueAlias) of
// the unwrap conversion conv ("" a plain number, "duration" or "bytes") satisfies
// when Loki makes a sample of it. A value whose conversion overflows has no
// sample (ParseFloat's range error, ParseDuration's invalid duration,
// ParseBytes's "too large"):
//
//   - plain: the math pipe reads a number with ParseFloat and answers NaN when it
//     fails, and a range filter compares NaN false; +Inf and -Inf (inf) pass.
//   - duration: VictoriaLogs saturates at int64 nanoseconds, the largest duration
//     (and the smallest) Go converts, so a saturated value is dropped; the
//     durations within the float64 resolution of the limit (about one
//     microsecond at 292 years) cannot be told from an overflow.
//   - bytes: the size is a float64 product floored as humanize does, and
//     ParseBytes fails from 2^64 (float64(math.MaxUint64)) up.
func UnwrapInRange(conv string) string {
	switch conv {
	case "duration":
		return UnwrapValueAlias + ":>-" + unwrapDurationLimit + " " + UnwrapValueAlias + ":<" + unwrapDurationLimit
	case "bytes":
		return UnwrapValueAlias + ":<18446744073709551616"
	}
	return UnwrapValueAlias + ":<=inf"
}

// UnwrapPattern returns the expression the gate of the unwrap conversion conv
// ("" a plain number, "duration" or "bytes") keeps a row by.
func UnwrapPattern(conv string) string {
	switch conv {
	case "duration":
		return UnwrapDurationPattern
	case "bytes":
		return UnwrapBytesPattern
	}
	return UnwrapNumberPattern
}

// UnwrapGate returns the pipes that select the rows of an unwrapped field that
// make a sample and convert the value into UnwrapValueAlias. The field is quoted:
// the math pipe reads a bare name such as max, abs or rand as a function, and
// stops a bare name at a minus or a slash.
func UnwrapGate(field string) string { return UnwrapGateFor(field, "") }

// UnwrapGateFor is UnwrapGate for the unwrap conversion conv: "" (a plain
// number), "duration" or "bytes".
func UnwrapGateFor(field, conv string) string {
	match, convert := UnwrapGateParts(field, conv)
	return match + convert + " | filter " + UnwrapInRange(conv)
}

// UnwrapGateParts returns the pipes of the gate of UnwrapGateFor in two pieces:
// the filter that keeps the values whose syntax converts, and the pipes that
// convert the value into UnwrapValueAlias. The gate ends with the range filter
// (UnwrapInRange) that drops the values whose conversion overflows.
func UnwrapGateParts(field, conv string) (match, convert string) {
	q := strconv.Quote(field)
	match = " | filter " + q + ":~" + strconv.Quote(UnwrapPattern(conv))
	switch conv {
	case "duration":
		return match, " | copy " + q + " as __lvp_d" +
			// time.ParseDuration reads a leading plus, "us", the Greek mu, a leading point
			// (".5s"), a trailing one ("5.s") and zeros before an integer ("01m"); the math
			// pipe reads none of them.
			` | replace_regexp if (__lvp_d:~"(^|[^0-9.])0[0-9]") ("(^|[^0-9.])0+([0-9])","${1}${2}") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"^[+]") ("^[+]","") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"us|μ") ("([0-9.])(?:us|μs)","${1}µs") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"[.]") ("(^|[^0-9])[.]([0-9])","${1}0.${2}") at __lvp_d` +
			` | replace_regexp if (__lvp_d:~"[.]") ("([0-9])[.]([^0-9])","${1}${2}") at __lvp_d` +
			" | math __lvp_d/1000000000 as " + UnwrapValueAlias
	case "bytes":
		// The number and the unit are read apart: the unit's prefix letter is a power
		// of 1000, or of 1024 with an i, and the product is floored as ParseBytes does.
		// Commas and spaces are dropped first (ParseBytes drops the commas of the number
		// and trims the spaces around the unit).
		return match, " | copy " + q + " as __lvp_n" +
			" | replace_regexp if (__lvp_n:~" + strconv.Quote("[,"+unwrapSpaceClass+"]") + ") (" + strconv.Quote("[,"+unwrapSpaceClass+"]") + `,"") at __lvp_n` +
			` | extract_regexp if (__lvp_n:~"[^0-9.]") "^(?P<__lvp_q>[0-9.]*)(?i:(?P<__lvp_p>[kmgtpe]?)(?P<__lvp_i>[iİ]?)b?)$" from __lvp_n` +
			` | format if (__lvp_q:*) "<__lvp_q>" as __lvp_n` +
			` | format "0" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^k$") "1" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^m$") "2" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^g$") "3" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^t$") "4" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^p$") "5" as __lvp_e` +
			` | format if (__lvp_p:~"(?i)^e$") "6" as __lvp_e` +
			` | format "1000" as __lvp_b` +
			` | format if (__lvp_i:*) "1024" as __lvp_b` +
			" | math floor(__lvp_n * (__lvp_b ^ __lvp_e)) as " + UnwrapValueAlias
	}
	// The math pipe does not read "infinity" (it reads inf): the word is shortened on
	// a copy, and only in the blocks that hold it.
	return match, " | copy " + q + " as __lvp_n" +
		` | replace_regexp if (__lvp_n:~"(?i)inity$") ("(?i)inity$","") at __lvp_n` +
		" | math __lvp_n as " + UnwrapValueAlias
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
	for _, c := range unwrapConversions {
		if strings.HasPrefix(rest, c.call) {
			return field, c.conv
		}
	}
	return field, ""
}

// unwrapConversions are the unwrap conversion functions and the conversion each
// selects. Loki converts duration_seconds() with convertDuration, as duration()
// (pkg/logql/syntax/extractor.go:63-71).
var unwrapConversions = []struct{ call, conv string }{
	{"duration_seconds(", "duration"},
	{"duration(", "duration"},
	{"bytes(", "bytes"},
}

// UnwrapConv returns the conversion ("duration" or "bytes") of the unwrap
// function converter (a LogQL parser's UnwrapStage.Converter): duration_seconds
// is converted as duration; any other function is returned unchanged.
func UnwrapConv(converter string) string {
	for _, c := range unwrapConversions {
		if converter+"(" == c.call {
			return c.conv
		}
	}
	return converter
}
