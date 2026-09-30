package translator

import (
	"regexp"
	"sort"
	"strconv"
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
		// A key with adjacent, leading or trailing dots is not one the field
		// identifier normalisation keeps, so it cannot be matched by name.
		if strings.Contains(v, "..") || v[0] == '.' || v[len(v)-1] == '.' {
			return
		}
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

var patternCaptureRE = regexp.MustCompile(`<([A-Za-z][A-Za-z0-9_]*)>`)

// stageDefinedLabels returns the label names a pattern or label_format stage
// creates: pattern captures and label_format destinations.
func stageDefinedLabels(stage string) []string {
	var out []string
	switch {
	case strings.HasPrefix(stage, "pattern "):
		for _, m := range patternCaptureRE.FindAllStringSubmatch(stage, -1) {
			out = append(out, m[1])
		}
	case strings.HasPrefix(stage, "label_format "):
		for _, assign := range splitLabelFormatAssignments(stage[len("label_format "):]) {
			if name, _, ok := strings.Cut(assign, "="); ok {
				out = append(out, strings.TrimSpace(name))
			}
		}
	}
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
	// An existence check (label!="") keeps its single key: the Drilldown
	// single-field fast paths recognise that exact `filter field:!""` shape, and
	// the labels Drilldown checks come from detected_fields, which resolves them
	// to stored fields.
	if op.negate && !op.isRe && !op.isComp && streamMatcherValue(value, false) == "" {
		keys = keys[:1]
	}
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

// jsonAliasCopies returns the `| copy "x.y" as a` pipes that give the labels a
// json expression defines (`| json a="x.y"`) their value as fields.
func jsonAliasCopies(stage string) string {
	aliases := parseJSONFieldAliases(stage)
	names := make([]string, 0, len(aliases))
	for alias, orig := range aliases {
		if alias != orig {
			names = append(names, alias)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	for _, alias := range names {
		b.WriteString(" | copy " + strconv.Quote(aliases[alias]) + " as " + quoteFieldName(alias))
	}
	return b.String()
}

func quoteFieldName(name string) string {
	if strings.ContainsAny(name, ".-") {
		return strconv.Quote(name)
	}
	return name
}

var (
	labelIdentRE   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	statsFuncArgRE = regexp.MustCompile(`^\s*[A-Za-z_]+\((?:[^,()]*,\s*)?([A-Za-z_][A-Za-z0-9_]*)\)\s*$`)
)

// resolveStatsKeys prepares the pipeline of a stats clause that groups by or
// aggregates a label a json or logfmt parser produced under its sanitized
// name. VictoriaLogs' parsers keep the original key (http.method), so the
// pipes that precede the stats give the label the value of each original
// spelling (a line holds at most one), and a parser-derived unwrap label also
// drops the lines without it, as Loki yields no sample for them. A label the
// query defines itself (json expression copy, pattern capture, format) and a
// label the line already stores keep their value. The pipes sit right before
// the stats, after every stage that could define the label.
func resolveStatsKeys(query, byLabels, statsExpr string) string {
	if !strings.Contains(query, "| unpack_json") && !strings.Contains(query, "| unpack_logfmt") {
		return query
	}
	var labels []string
	seen := map[string]bool{}
	add := func(l string) {
		l = strings.TrimSpace(l)
		if seen[l] || !resolvableLabel(query, l) {
			return
		}
		seen[l] = true
		labels = append(labels, l)
	}
	if byLabels != emptyByGrouping {
		for _, l := range strings.Split(byLabels, ",") {
			add(l)
		}
	}
	unwrap := ""
	if m := statsFuncArgRE.FindStringSubmatch(statsExpr); m != nil {
		unwrap = m[1]
		add(unwrap)
	}
	var pipes strings.Builder
	for _, label := range labels {
		for _, key := range parsedKeyVariants(label)[1:] {
			pipes.WriteString(" | format if (" + strconv.Quote(key) + `:*) "<` + key + `>" as ` + label + " keep_original_fields")
		}
	}
	// An existence check on a resolved label keeps its single key (the
	// Drilldown fast paths match that shape), so it runs after the pipes that
	// give the label its value.
	for _, label := range labels {
		if tok := " | filter " + label + `:!""`; strings.Contains(query, tok) {
			query = strings.Replace(query, tok, "", 1)
			pipes.WriteString(tok)
		}
	}
	// A parser-derived unwrap label: resolved above, or copied from a json expression.
	if unwrap != "" && (seen[unwrap] || strings.Contains(query, "| copy ") && strings.Contains(query, " as "+unwrap)) {
		pipes.WriteString(" | filter " + unwrap + ":*")
	}
	return query + pipes.String()
}

// resolvableLabel reports whether label can be a sanitized parser key that the
// query does not define itself.
func resolvableLabel(query, label string) bool {
	if !labelIdentRE.MatchString(label) || !strings.Contains(label, "_") || strings.HasPrefix(label, "_") ||
		label == "service_name" || label == "detected_level" {
		return false
	}
	return !strings.Contains(query, " as "+label) && !strings.Contains(query, "<"+label+">")
}
