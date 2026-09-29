package translator

import (
	"regexp"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// Loki's json and logfmt parsers sanitize every extracted key into a label name
// (http.method and http-method both become http_method), while VictoriaLogs'
// unpack_json and unpack_logfmt keep the original key. A label filter written
// after a parser therefore names a sanitized label and has to be matched against
// every original key that sanitizes to it.
//
// The variants are carried to translateSingleLabelFilter through the label
// translation function: a name with several candidates is returned joined by
// parsedKeySep.
const parsedKeySep = "\x00"

// maxParsedKeyUnderscores bounds the 2^n candidate keys of a name with n
// underscores; a longer name is matched by its uniform spellings only.
const maxParsedKeyUnderscores = 6

// parsedKeyVariants returns the original keys a parser can extract for the
// sanitized label name: every underscore kept, or replaced by a dot (nested
// JSON objects and OTel attributes), plus the all-hyphen spelling (HTTP header
// style). The label itself comes first.
func parsedKeyVariants(label string) []string {
	n := strings.Count(label, "_")
	if n == 0 {
		return []string{label}
	}
	out := []string{label}
	seen := map[string]struct{}{label: {}}
	add := func(v string) {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	pos := make([]int, 0, n)
	for i := 0; i < len(label); i++ {
		if label[i] == '_' {
			pos = append(pos, i)
		}
	}
	if n > maxParsedKeyUnderscores {
		add(strings.ReplaceAll(label, "_", "."))
		add(strings.ReplaceAll(label, "_", "-"))
		return out
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

// isKeyParserStage reports whether the stage is a json or logfmt parser (or the
// json-flavoured unpack), the parsers whose keys Loki sanitizes.
func isKeyParserStage(stage string) bool {
	for _, p := range []string{"json", "unpack", "logfmt"} {
		if stage == p || strings.HasPrefix(stage, p+" ") {
			return true
		}
	}
	return false
}

// isLabelFilterStage reports whether a pipeline stage is a label filter, which
// translatePipelineStage reaches last, after every named stage.
func isLabelFilterStage(stage string) bool {
	if isKeyParserStage(stage) || strings.HasPrefix(stage, "ip(") || stage == "unwrap" || stage == "decolorize" {
		return false
	}
	for _, p := range []string{"unwrap ", "pattern ", "regexp ", "extract ", "line_format ", "label_format ", "drop ", "keep "} {
		if strings.HasPrefix(stage, p) {
			return false
		}
	}
	return !isBareIdentifier(stage)
}

// translateParsedKeyFilter combines one filter per candidate original key into
// the filter on the sanitized label. A parser produces one key of the set at
// most, so the label's value is that key's value, or empty when none exists:
// a filter that rejects the empty value holds when any candidate satisfies it,
// and one that accepts the empty value holds when every candidate does.
func translateParsedKeyFilter(stage string, keys []string, value string, op logqlSingleFilterOp, caps logsql.Capabilities) (string, bool) {
	filters := make([]string, 0, len(keys))
	for _, key := range keys {
		f, ok := translateSingleLabelFilter(stage, func(string) string { return key }, caps)
		if !ok {
			return "", false
		}
		filters = append(filters, f)
	}
	if len(filters) == 1 {
		return filters[0], true
	}
	if parsedKeyMatchesEmpty(value, op) {
		return "(" + strings.Join(filters, " ") + ")", true
	}
	return "(" + strings.Join(filters, " OR ") + ")", true
}

// parsedKeyMatchesEmpty reports whether the label filter accepts an absent
// (empty) label.
func parsedKeyMatchesEmpty(value string, op logqlSingleFilterOp) bool {
	if op.isComp {
		return false
	}
	if strings.HasPrefix(value, `ip("`) && strings.HasSuffix(value, `")`) {
		return op.negate
	}
	v := streamMatcherValue(value, op.isRe)
	if !op.isRe {
		return (v == "") != op.negate
	}
	re, err := regexp.Compile("^(?:" + v + ")$")
	if err != nil {
		return op.negate
	}
	return re.MatchString("") != op.negate
}

// withParsedKeyVariants wraps a label translation so a label filter written
// after a json or logfmt parser matches every original key that sanitizes to
// its name. Names a parser cannot have produced keep their plain translation:
// dotted names, names without an underscore, and the caller's exclusions
// (regexp captures, json expressions that already name the original key).
func withParsedKeyVariants(labelFn LabelTranslateFunc, exclude func(string) bool) LabelTranslateFunc {
	return func(label string) string {
		translated := label
		if labelFn != nil {
			translated = labelFn(label)
		}
		// A label the translation maps (a known or learned VictoriaLogs field)
		// names that field; only an unmapped label can be a sanitized key.
		if translated != label || strings.HasPrefix(label, "_") || !strings.Contains(label, "_") ||
			strings.ContainsAny(label, ".-") || exclude(label) {
			return translated
		}
		return strings.Join(parsedKeyVariants(label), parsedKeySep)
	}
}
