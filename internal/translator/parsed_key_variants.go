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
	// name_extracted: the parsed value of the key name where the stream has the
	// label name (read from the scratch field extractedScratchPipes fills), the
	// key name_extracted otherwise.
	if base, ok := strings.CutPrefix(keys[0], extractedMarker); ok {
		literal, ok := translateParsedKeyFilter(stage, keys[1:], value, op, caps)
		parsed, pok := translateSingleLabelFilter(stage, func(string) string { return extractedScratch + "_" + base }, caps)
		if !ok || !pok {
			return "", false
		}
		collides := extractedCollides(base)
		return "((" + collides + " " + parsed + ") OR (NOT (" + collides + ") (" + literal + ")))", true
	}
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
func withParsedKeyVariants(labelFn LabelTranslateFunc, exclude func(string) bool, extracted func(base string) bool) LabelTranslateFunc {
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
		keys := parsedKeyVariants(label)
		if base, ok := strings.CutSuffix(label, extractedSuffix); ok && isBareIdentifier(base) && extracted(base) {
			keys = append([]string{extractedMarker + base}, keys...)
		}
		return strings.Join(keys, parsedKeySep)
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

var unpackPipeRE = regexp.MustCompile(`\| (unpack_json|unpack_logfmt)\b`)

var fieldBreakdownShapeRE = regexp.MustCompile(`\| unpack_(?:json|logfmt)(?: \| delete __error__(?:, ?__error_details__)?)? \| filter ([A-Za-z][A-Za-z0-9_]*):!""$`)

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
	// A single-field count breakdown behind its existence check
	// (`| unpack_json | filter f:!"" | stats by (f) count()`) keeps the exact-key
	// semantics the Drilldown field breakdown rewrite relies on (the key f, stored
	// or parsed); Drilldown names a dotted key through a json expression instead.
	if m := fieldBreakdownShapeRE.FindStringSubmatch(query); m != nil && statsExpr == "count()" && strings.TrimSpace(byLabels) == m[1] {
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
	var extractedBases, extractedLabels []string
	for _, label := range labels {
		for _, key := range parsedKeyVariants(label)[1:] {
			pipes.WriteString(" | format if (" + strconv.Quote(key) + `:*) "<` + key + `>" as ` + label + " keep_original_fields")
		}
		if base, ok := strings.CutSuffix(label, extractedSuffix); ok && isBareIdentifier(base) {
			extractedBases = append(extractedBases, base)
			extractedLabels = append(extractedLabels, label)
		}
	}
	if len(extractedBases) > 0 {
		bare := bareUnpackPipes(query)
		parsers := make([]extractedParser, len(bare))
		for i, p := range bare {
			parsers[i] = extractedParser{unpack: p.unpack}
		}
		// A filter on the same label already computed it behind the last
		// parser: reuse it (and keep its scratch fields until here).
		var need []string
		reused := false
		for _, base := range extractedBases {
			idx := strings.LastIndex(query, " as "+extractedScratch+"_"+base)
			if idx >= 0 && (len(bare) == 0 || bare[len(bare)-1].pos < idx) {
				reused = true
			} else {
				need = append(need, base)
			}
		}
		if reused {
			query = strings.Replace(query, " | delete "+extractedScratch+"*", "", 1)
		}
		streamCopied := reused
		pipes.WriteString(strings.Join(extractedUnpackPipes(parsers, need), "") + extractedCoalescePipes(parsers, need, &streamCopied))
		for i, label := range extractedLabels {
			pipes.WriteString(" | format if (" + extractedCollides(extractedBases[i]) + ") \"<" + extractedScratch + "_" + extractedBases[i] + ">\" as " + label)
		}
		pipes.WriteString(" | delete " + extractedScratch + "*")
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

// extractedSuffix is what Loki appends to a parsed label that shares its name
// with a stream label (pkg/logql/log/parser.go duplicateSuffix). A parser
// stage then leaves the stream label alone and exposes the parsed value as
// name_extracted, where VictoriaLogs' unpack_json and unpack_logfmt overwrite
// the stored field of that name. A later parser skips a key an earlier one
// extracted (ParserHint.Extracted), so the first parser that reads the key
// decides the value.
const extractedSuffix = "_extracted"

// extractedScratch prefixes the scratch fields the collision rename uses
// (deleted again by the pipe that ends the query's use of them).
const extractedScratch = "__lxp"

// extractedMarker starts the first candidate key of a label name_extracted
// that may be a collision rename; the rest are the key spellings.
const extractedMarker = "\x01"

// extractedParser is one json or logfmt stage of a query: its unpack pipe and
// the labels it extracts, by the key each reads (nil: every key).
type extractedParser struct {
	unpack  string
	targets map[string]string
}

// source returns the key the parser reads for label base, if it extracts it.
func (p extractedParser) source(base string) (string, bool) {
	if p.targets == nil {
		return base, true
	}
	src, ok := p.targets[base]
	return src, ok
}

// extractionTargets returns the labels a json or logfmt extraction list
// extracts with the key each reads (`| json a, b="x.y"`, `| logfmt a, b="k"`),
// nil for a stage without a list. A json path with an index or bracket is left
// out: the label then keeps the plain translation.
func extractionTargets(stage string) map[string]string {
	name, rest, ok := strings.Cut(stage, " ")
	if !ok || strings.TrimSpace(rest) == "" || (name != "json" && name != "logfmt") {
		return nil
	}
	targets := map[string]string{}
	for _, item := range splitCSV(rest) {
		item = strings.TrimSpace(item)
		label, expr, hasExpr := strings.Cut(item, "=")
		label = strings.TrimSpace(label)
		src := label
		if hasExpr {
			expr = strings.TrimSpace(expr)
			if len(expr) < 2 || (expr[0] != '"' && expr[0] != '`') {
				continue
			}
			src = resolveJSONBracketPath(expr[1 : len(expr)-1])
		}
		if isBareIdentifier(label) && src != "" && !strings.ContainsAny(src, `[]"'\ `) {
			targets[label] = src
		}
	}
	return targets
}

// extractedCollides is the condition under which the label base is a stream
// label of the entry, so a parsed key base is renamed: every stream has
// service_name in Loki; any other name is in _stream (a dotted spelling too).
func extractedCollides(base string) string {
	value := extractedScratch + "_" + base + ":*"
	if base == "service_name" {
		return value
	}
	return extractedScratch + "_stream:~" + strconv.Quote(`[{,]`+strings.ReplaceAll(base, "_", "[._]")+`="`) + " " + value
}

// extractedUnpackPipes returns, per parser, the pipe that unpacks the keys of
// the labels bases it extracts into scratch fields (the stored fields were
// overwritten by the first unpack).
func extractedUnpackPipes(parsers []extractedParser, bases []string) []string {
	after := make([]string, len(parsers))
	for i, parser := range parsers {
		var srcs []string
		for _, base := range bases {
			if src, ok := parser.source(base); ok && !containsStr(srcs, logsqlFieldName(src)) {
				srcs = append(srcs, logsqlFieldName(src))
			}
		}
		if len(srcs) > 0 {
			after[i] = " | " + parser.unpack + " from _msg fields (" + strings.Join(srcs, ", ") + ") result_prefix " + strconv.Quote(extractedScratch+strconv.Itoa(i)+"_")
		}
	}
	return after
}

// extractedCoalescePipes returns the pipes that give each base its Loki value
// in __lxp_<base> from the scratch fields of parsers: the first parser's. It
// copies _stream first (once: streamCopied) when a base needs it.
func extractedCoalescePipes(parsers []extractedParser, bases []string, streamCopied *bool) string {
	var fin strings.Builder
	for _, base := range bases {
		if base != "service_name" && !*streamCopied {
			*streamCopied = true
			fin.WriteString(" | copy _stream as " + extractedScratch + "_stream")
		}
	}
	for _, base := range bases {
		var fields []string
		for i, parser := range parsers {
			if src, ok := parser.source(base); ok {
				fields = append(fields, logsqlFieldName(extractedScratch+strconv.Itoa(i)+"_"+src))
			}
		}
		for j := len(fields) - 1; j >= 0; j-- {
			if j == len(fields)-1 {
				fin.WriteString(" | copy " + fields[j] + " as " + extractedScratch + "_" + base)
			} else {
				fin.WriteString(" | format if (" + fields[j] + `:*) "<` + strings.Trim(fields[j], `"`) + `>" as ` + extractedScratch + "_" + base)
			}
		}
	}
	return fin.String()
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// extractedStageMark brackets, in the translation of a label filter stage, the
// name_extracted bases it reads (comma-joined); withExtractedScratch turns it
// into the pipes that compute them.
const extractedStageMark = "\x02"

// withExtractedScratch adds the scratch pipes of the name_extracted labels a
// query's filters name: each parser's unpack right behind its pipe (a later
// line_format must not change the line it reads), before each filter the
// first-parser values of the parsers that precede it, and the deletion of the
// scratch fields at the end.
func withExtractedScratch(parts []string, parsers []extractedParser, partIdx []int, bases []string) []string {
	after := extractedUnpackPipes(parsers, bases)
	out := make([]string, 0, len(parts)+len(parsers)+3)
	seen, streamCopied := 0, false
	for i, part := range parts {
		if lo := strings.Index(part, extractedStageMark); lo >= 0 {
			if n := strings.Index(part[lo+1:], extractedStageMark); n >= 0 {
				hi := lo + 1 + n
				out = append(out, extractedCoalescePipes(parsers[:seen], strings.Split(part[lo+1:hi], ","), &streamCopied))
				part = part[:lo] + part[hi+1:]
			}
		}
		out = append(out, part)
		for j, idx := range partIdx {
			if idx == i {
				out = append(out, after[j])
				seen = j + 1
			}
		}
	}
	cleaned := out[:0]
	for _, part := range out {
		if part = strings.TrimSpace(part); part != "" {
			cleaned = append(cleaned, part)
		}
	}
	return append(cleaned, "| delete "+extractedScratch+"*")
}

// bareUnpackPipe is a plain `| unpack_json` / `| unpack_logfmt` pipe of a
// translated query at byte offset pos: a parser stage, not a scratch unpack
// (`from _msg ...`) or a conditional one of the detected_level chain (`if (...)`).
type bareUnpackPipe struct {
	unpack string
	pos    int
}

func bareUnpackPipes(query string) []bareUnpackPipe {
	var pipes []bareUnpackPipe
	for _, m := range unpackPipeRE.FindAllStringSubmatchIndex(query, -1) {
		if rest := strings.TrimLeft(query[m[1]:], " "); rest == "" || rest[0] == '|' {
			pipes = append(pipes, bareUnpackPipe{query[m[2]:m[3]], m[0]})
		}
	}
	return pipes
}
