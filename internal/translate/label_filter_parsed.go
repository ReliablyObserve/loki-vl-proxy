package translate

import (
	"regexp"
	"slices"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The builders of a label filter after a json or logfmt parser: the keys the filter's label can be stored under,
// and the filter that combines the filter on each key. The string translator reads the keys from the same
// functions (it forwards to them), so the lowering and the translator write the same filter.
//
// Loki's json and logfmt parsers sanitize every extracted key into a label name (http.method and http-method both
// become http_method), while VictoriaLogs' unpack_json and unpack_logfmt keep the original key. A label filter
// written after a parser therefore names a sanitized label and is matched against every original key that
// sanitizes to it.

// ExtractedMarker starts the first candidate key of a label name_extracted that may be a collision rename (the
// string translator's marker; a label holding it cannot be encoded as a key list).
const ExtractedMarker = "\x01"

// maxParsedKeyUnderscores bounds the 2^n candidate keys of a name with n underscores; a longer name is matched by
// its uniform spellings only.
const maxParsedKeyUnderscores = 6

// ParsedKeyVariants returns the original keys a parser can extract for the sanitized label name: every underscore
// kept, or replaced by a dot (nested JSON objects and OTel attributes), plus the all-hyphen spelling (HTTP header
// style). The label itself comes first.
func ParsedKeyVariants(label string) []string {
	n := strings.Count(label, "_")
	if n == 0 {
		return []string{label}
	}
	// At most 2^n spellings from the underscores, the label itself and the hyphenated one: one slice holds them
	// (n is bounded below), and a spelling is told from the ones before it by comparison, not a set.
	size := 3
	if n <= maxParsedKeyUnderscores {
		size += 1 << n
	}
	out := make([]string, 1, size)
	out[0] = label
	add := func(v string) {
		// A key with adjacent, leading or trailing dots is not one the field
		// identifier normalisation keeps, so it cannot be matched by name.
		if strings.Contains(v, "..") || v[0] == '.' || v[len(v)-1] == '.' {
			return
		}
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	if n > maxParsedKeyUnderscores {
		add(strings.ReplaceAll(label, "_", "."))
		add(strings.ReplaceAll(label, "_", "-"))
		return out
	}
	var posBuf [maxParsedKeyUnderscores]int
	pos := posBuf[:0]
	for i := 0; i < len(label); i++ {
		if label[i] == '_' {
			pos = append(pos, i)
		}
	}
	b := []byte(label)
	for mask := 1; mask < 1<<n; mask++ {
		for i, p := range pos {
			if mask&(1<<i) != 0 {
				b[p] = '.'
			} else {
				b[p] = '_'
			}
		}
		add(string(b))
	}
	add(strings.ReplaceAll(label, "_", "-"))
	return out
}

// ParsedKeysOf are the keys a label filter after a json or logfmt parser matches for label, which the label
// translation (translated) left as it was. nil when the label cannot be a sanitized key: a label the translation
// maps (a known or learned VictoriaLogs field), one that starts with an underscore or holds none, a dotted or
// hyphenated one and one holding the bytes that separate and mark the candidate keys.
func ParsedKeysOf(label, translated string) []string {
	if translated != label || label == "" || label[0] == '_' || !hasByte(label, '_') {
		return nil
	}
	for _, c := range []byte{'.', '-', ParsedKeySep[0], ExtractedMarker[0]} {
		if hasByte(label, c) {
			return nil
		}
	}
	return ParsedKeyVariants(label)
}

// ParsedKeyMatchesEmpty reports whether a label filter accepts an absent (empty) label, where value is the
// filter's value as Loki reads it (the unquoted text; the pattern of an ip filter).
func ParsedKeyMatchesEmpty(value string, op LabelFilterOp, isIP bool) bool {
	if op.IsComp || isIP {
		// Loki's ip() filter drops a line without the label under both operators.
		return false
	}
	if !op.IsRe {
		return (value == "") != op.Negate
	}
	re, err := regexp.Compile(AnchorLabelFilterPattern(LokiLabelFilterPattern(value)))
	if err != nil {
		return op.Negate
	}
	return re.MatchString("") != op.Negate
}

// ParsedKeysKeepsFirst reports whether the filter is an existence check (label!=""), which keeps its first key
// alone: the Drilldown single-field fast paths recognise that exact `filter field:!""` shape.
func ParsedKeysKeepsFirst(value string, op LabelFilterOp, isIP bool) bool {
	return op.Negate && !op.IsRe && !op.IsComp && !isIP && value == ""
}

// ParsedKeyFilter combines one filter per candidate key into the filter on the label. A parser produces one key
// of the set at most, so the label's value is that key's value, or empty when none exists: a filter that rejects
// the empty value holds when any candidate satisfies it, and one that accepts the empty value holds when every
// candidate does. ok is false when the filters cannot be joined as the string translator joins them, by plain
// text: an and-join of a filter that holds a top-level space would change meaning.
func ParsedKeyFilter(filters []logsql.FilterExpr, matchesEmpty bool) (logsql.FilterExpr, bool) {
	if len(filters) == 1 {
		return filters[0], true
	}
	if matchesEmpty {
		for _, f := range filters {
			if HasTopLevelSpace(f.String()) {
				return nil, false
			}
		}
		return logsql.Group{Expr: logsql.AllOf(filters)}, true
	}
	return logsql.Group{Expr: logsql.AnyOf(filters)}, true
}

// LookupLabelFilterOp is the meaning of a LogQL label filter operator, false for one that is none of
// LabelFilterOps.
func LookupLabelFilterOp(cmp string) (LabelFilterOp, bool) {
	for i := range LabelFilterOps {
		if LabelFilterOps[i].LogQL == cmp {
			return LabelFilterOps[i].Op, true
		}
	}
	return LabelFilterOp{}, false
}
