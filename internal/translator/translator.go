// Package translator converts LogQL queries to LogsQL queries.
//
// Based on: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/
package translator

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// LabelTranslateFunc translates a Loki label name to a VL field name (query direction).
// If nil, no translation is performed.
type LabelTranslateFunc func(lokiLabel string) string

// emptyByGrouping is a sentinel value used as the byLabels argument to buildStatsQuery
// to indicate an explicit "by ()" clause (aggregate all into one series).
// This is distinct from an empty string, which means "no grouping clause at all".
const emptyByGrouping = "__lvp_by_empty__"

// Marker suffixes embedded in translated queries for proxy post-processing.
// The proxy strips them before forwarding to VL and applies the corresponding transform.
const (
	groupMarker        = "__lvp_group__"
	labelReplacePrefix = "__lvp_lr:"
	labelJoinPrefix    = "__lvp_lj:"
)

// LabelReplaceSpec holds the parsed arguments of a label_replace() expression.
type LabelReplaceSpec struct {
	DstLabel    string
	Replacement string
	SrcLabel    string
	Regex       string
}

// LabelJoinSpec holds the parsed arguments of a label_join() expression.
type LabelJoinSpec struct {
	DstLabel  string
	Separator string
	SrcLabels []string
}

// ParseGroupMarker reports whether the translated query carries a group-normalization
// marker, and returns the clean query (marker stripped).
func ParseGroupMarker(query string) (string, bool) {
	if strings.HasSuffix(query, groupMarker) {
		return query[:len(query)-len(groupMarker)], true
	}
	return query, false
}

// ParseLabelReplaceMarker extracts a label_replace spec embedded in a translated query.
// Returns the clean query (marker stripped) and the spec, or nil if absent.
func ParseLabelReplaceMarker(query string) (string, *LabelReplaceSpec) {
	idx := strings.LastIndex(query, labelReplacePrefix)
	if idx < 0 {
		return query, nil
	}
	payload := strings.TrimSuffix(query[idx+len(labelReplacePrefix):], "__")
	b, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return query, nil
	}
	parts := strings.SplitN(string(b), "\x00", 4)
	if len(parts) != 4 {
		return query, nil
	}
	return query[:idx], &LabelReplaceSpec{
		DstLabel:    parts[0],
		Replacement: parts[1],
		SrcLabel:    parts[2],
		Regex:       parts[3],
	}
}

// ParseLabelJoinMarker extracts a label_join spec embedded in a translated query.
// Returns the clean query (marker stripped) and the spec, or nil if absent.
func ParseLabelJoinMarker(query string) (string, *LabelJoinSpec) {
	idx := strings.LastIndex(query, labelJoinPrefix)
	if idx < 0 {
		return query, nil
	}
	payload := strings.TrimSuffix(query[idx+len(labelJoinPrefix):], "__")
	b, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return query, nil
	}
	s := string(b)
	n1 := strings.IndexByte(s, 0)
	if n1 < 0 {
		return query, nil
	}
	dst := s[:n1]
	rest := s[n1+1:]
	n2 := strings.IndexByte(rest, 0)
	if n2 < 0 {
		return query, nil
	}
	sep := rest[:n2]
	srcsRaw := rest[n2+1:]
	var srcs []string
	if srcsRaw != "" {
		srcs = strings.Split(srcsRaw, "\x01")
	}
	return query[:idx], &LabelJoinSpec{DstLabel: dst, Separator: sep, SrcLabels: srcs}
}

// TranslateLogQL converts a LogQL query string to a LogsQL query string.
// It handles stream selectors, line filters, label filters, parsers, and metric queries.
func TranslateLogQL(logql string) (string, error) {
	return TranslateLogQLWithLabels(logql, nil)
}

// TranslateLogQLWithStreamFields converts a LogQL query to LogsQL, using VL native stream
// selectors for labels known to be _stream_fields (faster index path), and field filters
// for everything else. If streamFields is nil or empty, all matchers use field filters.
func TranslateLogQLWithStreamFields(logql string, labelFn LabelTranslateFunc, streamFields map[string]bool) (string, error) {
	return translateLogQLFull(logql, labelFn, streamFields)
}

// TranslateLogQLWithLabels converts a LogQL query to LogsQL, applying label name
// translation in stream selectors and label filters.
func TranslateLogQLWithLabels(logql string, labelFn LabelTranslateFunc) (string, error) {
	return translateLogQLFull(logql, labelFn, nil)
}

func translateLogQLFull(logql string, labelFn LabelTranslateFunc, streamFields map[string]bool) (string, error) {
	logql = strings.TrimSpace(logql)
	if logql == "" {
		return "*", nil
	}

	// Detect subquery syntax: outer_func(inner_query[range:step])
	// The proxy evaluates these by running the inner query at sub-step intervals.
	if result, ok := tryTranslateSubquery(logql); ok {
		return result, nil
	}

	// label_replace and label_join are transform wrappers around a complete metric
	// expression. Handle them before the without/binary/metric path so the inner
	// expression is translated correctly and the marker is appended last.
	if result, ok := tryTranslateLabelReplace(logql, labelFn); ok {
		return result, nil
	}
	if result, ok := tryTranslateLabelJoin(logql, labelFn); ok {
		return result, nil
	}

	// Extract without() labels before translation.
	// The proxy will post-process VL results to remove these labels from metric grouping.
	var withoutLabels []string
	logql, withoutLabels = extractWithoutLabels(logql)

	// Strip "bool" modifier from comparison operators before translation.
	// Loki: "A > bool B" returns 1/0 instead of filtering. Our applyOp always
	// returns 1/0 for comparisons, so "bool" is a no-op — just strip it.
	boolRe := regexp.MustCompile(`\s+bool\s+`)
	logql = boolRe.ReplaceAllString(logql, " ")

	// count_values groups by the VALUES of the inner metric — VL cannot compute this.
	if outerAgg, _, _ := extractOuterAggregation(logql); outerAgg == "count_values" {
		return "", fmt.Errorf("count_values is not supported: it groups by metric values which VictoriaLogs cannot compute; use count() grouped by an existing label instead")
	}

	// Check binary metric expressions FIRST — they may contain metric sub-expressions.
	// E.g., "rate({...}[5m]) > 0" is a binary expr, not just a metric query.
	if binResult, ok := tryTranslateBinaryMetricExpr(logql, labelFn); ok {
		return appendWithoutMarker(binResult, withoutLabels), nil
	}

	// Check if this is a plain metric query (no binary operator at top level)
	if metricResult, ok := tryTranslateMetricQuery(logql, labelFn); ok {
		return appendWithoutMarker(metricResult, withoutLabels), nil
	}
	if unwrapFunc := missingUnwrapRangeMetricFunc(logql); unwrapFunc != "" {
		return "", fmt.Errorf("%s requires `| unwrap <field>` for range aggregation", unwrapFunc)
	}

	// Guard: metric aggregation (sum/avg/topk/etc) applied directly to a log stream
	// without a range vector has no meaning and would fall through to bare-text wrapping,
	// producing malformed VL queries. Return a clear error instead.
	if outerAgg, _, _ := extractOuterAggregation(logql); outerAgg != "" && !strings.Contains(logql, "[") {
		return "", fmt.Errorf("%s() requires a range metric inside (e.g. rate({...}[5m]), count_over_time({...}[5m]))", outerAgg)
	}

	return translateLogQuery(logql, labelFn, streamFields)
}

// WithoutMarkerSuffix is appended to translated queries that need without() post-processing.
// Format: "...actual_query...__without__:pod,node"
const WithoutMarkerSuffix = "__without__:"

func appendWithoutMarker(query string, labels []string) string {
	if len(labels) == 0 {
		return query
	}
	return query + WithoutMarkerSuffix + strings.Join(labels, ",")
}

// ParseWithoutMarker extracts the without labels from a translated query.
// Returns the clean query and the list of labels to exclude.
func ParseWithoutMarker(query string) (cleanQuery string, excludeLabels []string) {
	idx := strings.LastIndex(query, WithoutMarkerSuffix)
	if idx < 0 {
		return query, nil
	}
	cleanQuery = query[:idx]
	labelStr := query[idx+len(WithoutMarkerSuffix):]
	for _, l := range strings.Split(labelStr, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			excludeLabels = append(excludeLabels, l)
		}
	}
	return cleanQuery, excludeLabels
}

// extractWithoutLabels extracts the excluded labels stored by rewriteWithoutToMarker.
// Called by the translator to attach metadata to the translated query.
func extractWithoutLabels(logql string) (cleaned string, labels []string) {
	withoutRe := regexp.MustCompile(`\bwithout\s*\(([^)]+)\)`)
	m := withoutRe.FindStringSubmatch(logql)
	if m == nil {
		return logql, nil
	}
	labelStr := m[1]
	for _, l := range strings.Split(labelStr, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			labels = append(labels, l)
		}
	}
	cleaned = withoutRe.ReplaceAllString(logql, "")
	for strings.Contains(cleaned, "  ") {
		cleaned = strings.ReplaceAll(cleaned, "  ", " ")
	}
	return cleaned, labels
}

// translateLogQuery handles log queries (non-metric).
func translateLogQuery(logql string, labelFn LabelTranslateFunc, streamFields ...map[string]bool) (string, error) {
	var sf map[string]bool
	if len(streamFields) > 0 {
		sf = streamFields[0]
	}

	var parts []string
	var streamParts []string // VL native stream selectors for known _stream_fields

	remaining := logql

	// 1. Extract stream selector {key="value", ...}
	// In VL, stream selectors `{...}` only match labels that were declared as
	// _stream_fields at ingestion time. Labels like "level" are typically NOT
	// stream fields. If we pass {level="error"} to VL, the stream filter
	// returns zero results.
	//
	// Optimization: if streamFields is configured, use VL native stream selectors
	// for known _stream_fields (faster index path). Use field filters for the rest.
	if strings.HasPrefix(remaining, "{") {
		end := findMatchingBrace(remaining)
		if end < 0 {
			return "", fmt.Errorf("unmatched '{' in stream selector")
		}
		streamContent := remaining[1:end]
		remaining = strings.TrimSpace(remaining[end+1:])

		matchers := splitStreamMatchers(streamContent)
		for _, m := range matchers {
			if sf != nil && canUseStreamSelector(m, sf, labelFn) {
				streamParts = append(streamParts, m)
			} else {
				ff := streamMatcherToFieldFilter(m, labelFn)
				if ff != "" {
					parts = append(parts, ff)
				}
			}
		}
	}

	// Track whether we've seen a parser pipe (json, logfmt, pattern, regexp).
	// After a parser, label filters must become VL `| filter` pipes.
	afterParser := false
	// Track canonical label-filter stages so repeated drilldown include/exclude
	// clicks don't accumulate duplicate or contradictory filters.
	labelFilterLatest := make(map[string]int)

	// 2. Process pipeline stages: | operator ...
	// LogQL line filters: |= "text", != "text", |~ "regexp", !~ "regexp"
	// LogQL pipe stages: | json, | logfmt, | label == "value", etc.
	for remaining != "" {
		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			break
		}

		// Check for line filter operators first (these are NOT pipe stages)
		// CRITICAL: Loki |= is SUBSTRING match, not word match.
		// VL's "text" is word-only; VL's ~"text" is substring/regexp.
		// We must use ~"text" to match Loki's substring semantics.
		// The proxy's reconstructLogLine puts the full JSON into _msg, so
		// searching _msg via ~"text" finds text in any original JSON field.
		if strings.HasPrefix(remaining, "|= ") || strings.HasPrefix(remaining, "|=\"") {
			// Substring match: |= "text" → ~"text"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			parts = append(parts, "~"+val)
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "!= ") || strings.HasPrefix(remaining, "!=\"") {
			// Negative substring: != "text" → NOT ~"text"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			parts = append(parts, "NOT ~"+val)
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "|~ ") || strings.HasPrefix(remaining, "|~\"") {
			// Regexp match: |~ "regexp" → ~"regexp"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			parts = append(parts, "~"+val)
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "!~ ") || strings.HasPrefix(remaining, "!~\"") {
			// Negative regexp: !~ "regexp" → NOT ~"regexp"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			parts = append(parts, "NOT ~"+val)
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "|> ") || strings.HasPrefix(remaining, "|>\"") || strings.HasPrefix(remaining, "|>`") {
			// Pattern filter match: |> "foo <_> bar" → ~"foo .* bar"
			remaining = strings.TrimSpace(remaining[2:])
			expr, rest := extractPipelineStage(remaining)
			if translated := translatePatternLineFilter(expr, false); translated != "" {
				parts = append(parts, translated)
			}
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "!> ") || strings.HasPrefix(remaining, "!>\"") || strings.HasPrefix(remaining, "!>`") {
			// Negative pattern filter: !> "foo <_> bar" → NOT ~"foo .* bar"
			remaining = strings.TrimSpace(remaining[2:])
			expr, rest := extractPipelineStage(remaining)
			if translated := translatePatternLineFilter(expr, true); translated != "" {
				parts = append(parts, translated)
			}
			remaining = rest
			continue
		}

		if !strings.HasPrefix(remaining, "|") {
			// Bare text after stream selector — treat as a phrase filter.
			//
			// Guard against a class of malformed inputs where a label matcher
			// arrives without surrounding `{...}` (e.g. `app="json-test"`
			// instead of `{app="json-test"}`). Loki rejects such queries with
			// a parse error and the bare-text fallback would otherwise emit
			// LogsQL like `"app="json-test""`, which VictoriaLogs cannot
			// parse. Surface this as a translation error so the proxy returns
			// 400, matching Loki's behavior.
			if looksLikeBareLabelMatcher(remaining) {
				return "", fmt.Errorf("parse error : syntax error: stream selector must be wrapped in braces, got %q", remaining)
			}
			parts = append(parts, translateBareFilter(remaining))
			break
		}

		// Skip the pipe character for pipe stages
		remaining = strings.TrimSpace(remaining[1:])

		// Determine the pipeline stage type
		stage, rest := extractPipelineStage(remaining)
		remaining = rest

		translated := translatePipelineStage(stage, labelFn)
		if strings.HasPrefix(translated, errUnknownParser) {
			parserName := strings.TrimPrefix(translated, errUnknownParser)
			return "", fmt.Errorf("unknown pipeline stage %q — not a valid LogQL parser or label filter", parserName)
		}
		if translated != "" {
			// Track parser state — after a parser, label filters become | filter
			if isParserStage(translated) {
				afterParser = true
			}
			// If this is a bare field filter after a parser, wrap it as | filter
			if afterParser && !strings.HasPrefix(translated, "|") && isFieldFilter(translated) {
				translated = "| filter " + translated
			}
			if _, baseKey, ok := canonicalLabelFilterStage(stage, labelFn); ok {
				if idx, exists := labelFilterLatest[baseKey]; exists {
					// Latest action wins for the same field/value filter identity.
					parts[idx] = translated
				} else {
					labelFilterLatest[baseKey] = len(parts)
					parts = append(parts, translated)
				}
			} else {
				parts = append(parts, translated)
			}
		}
	}

	result := strings.Join(parts, " ")

	// Prepend VL native stream selector for known _stream_fields
	if len(streamParts) > 0 {
		streamSelector := "{" + strings.Join(streamParts, ", ") + "}"
		if result == "" {
			result = streamSelector
		} else {
			result = streamSelector + " " + result
		}
	}

	if result == "" {
		return "*", nil
	}
	return result, nil
}

// knownParsers is the set of bare-word LogQL parser names.
// If a bare identifier stage doesn't match any of these it is an unknown parser
// and should surface as a 400 rather than silently passing through.
var knownParsers = map[string]bool{
	"json": true, "logfmt": true, "unpack": true, "labels": true,
	"pattern": true, "regexp": true, "grok": true, "clf": true,
	"nginx": true, "apache": true, "csv": true, "decolorize": true,
}

// errUnknownParser is the sentinel prefix used to propagate unknown-parser
// errors from translatePipelineStage through the string-returning call chain.
const errUnknownParser = "__ERR_UNKNOWN_PARSER__:"

// translatePipelineStage converts a single LogQL pipeline stage to LogsQL.
func translatePipelineStage(stage string, labelFn LabelTranslateFunc) string {
	stage = strings.TrimSpace(stage)

	// Note: Line filters (|=, !=, |~, !~) are handled in translateLogQuery
	// before this function is called. Only pipe stages reach here.

	// Unwrap — VL doesn't need unwrap, stats functions take field names directly.
	// | unwrap field_name → silently dropped (the field name is used in the stats function)
	// | unwrap (bare, no field) → also dropped; Grafana query builder emits this
	// while the user is still typing a field name.
	if stage == "unwrap" || strings.HasPrefix(stage, "unwrap ") {
		return "" // drop — VL handles this implicitly
	}

	// Parsers — handle both bare (| json) and field-specific (| json field1, field2)
	// VL's unpack_json/unpack_logfmt always extracts all fields,
	// so field-specific variants map to the same VL command.
	if stage == "json" || stage == "unpack" || strings.HasPrefix(stage, "json ") || strings.HasPrefix(stage, "unpack ") {
		return "| unpack_json"
	}
	if stage == "logfmt" || strings.HasPrefix(stage, "logfmt ") {
		return "| unpack_logfmt"
	}
	if strings.HasPrefix(stage, "pattern ") {
		// | pattern "..." → | extract "..."
		patternExpr := normalizeQuotedStageExpr(stage[8:])
		if isNoopPatternExpression(patternExpr) {
			// Defensive compatibility: some Drilldown flows emit wildcard-only
			// patterns like `(.*)`. VL extract requires named placeholders and
			// rejects these, so treat them as a no-op stage.
			return ""
		}
		return "| extract " + patternExpr
	}
	if strings.HasPrefix(stage, "regexp ") {
		// | regexp "..." → | extract_regexp "..."
		return "| extract_regexp " + normalizeQuotedStageExpr(stage[7:])
	}
	if strings.HasPrefix(stage, "extract ") {
		// Defensive pass-through for pre-translated queries.
		patternExpr := normalizeQuotedStageExpr(stage[8:])
		if isNoopPatternExpression(patternExpr) {
			return ""
		}
		return "| extract " + patternExpr
	}

	// Line formatting
	if strings.HasPrefix(stage, "line_format ") {
		tmpl := stage[12:]
		return "| format " + convertGoTemplate(tmpl)
	}
	// Label formatting
	if strings.HasPrefix(stage, "label_format ") {
		return translateLabelFormat(stage[13:])
	}

	// Drop / keep labels
	if strings.HasPrefix(stage, "drop ") {
		return "| delete " + stage[5:]
	}
	if strings.HasPrefix(stage, "keep ") {
		// Always include _time and _msg so the proxy can build the response
		fields := stage[5:]
		return "| fields _time, _msg, _stream, " + fields
	}

	// decolorize — strips ANSI color codes.
	// VL doesn't have native ANSI stripping yet.
	// Proxy applies this post-processing on response log lines.
	// TODO: Replace with VL native pipe when available.
	if stage == "decolorize" {
		return "| decolorize" // proxy-side post-processing marker
	}

	// ip() label filter — CIDR matching on label values.
	// VL doesn't have native IP range filtering yet.
	// Proxy applies this post-processing on response labels.
	// TODO: Replace with VL native filter when available.
	if strings.HasPrefix(stage, "ip(") {
		return "| " + stage // proxy-side post-processing marker
	}

	// Detect unknown bare-word parsers (e.g. `| badparser`).
	// A stage that is a bare identifier with no operator characters is very likely
	// an attempt to invoke a named parser. If it doesn't match any known parser,
	// return an error sentinel so the proxy can surface a 400 rather than silently
	// passing through to VL and returning 200 with wrong results.
	if isBareIdentifier(stage) && !knownParsers[stage] {
		return errUnknownParser + stage
	}

	// Label filters: label op value
	return translateLabelFilter(stage, labelFn)
}

func isBareIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

func isNoopPatternExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	if strings.HasPrefix(expr, "`") && strings.HasSuffix(expr, "`") && len(expr) >= 2 {
		expr = expr[1 : len(expr)-1]
	} else if strings.HasPrefix(expr, `"`) && strings.HasSuffix(expr, `"`) && len(expr) >= 2 {
		if unquoted, err := strconv.Unquote(expr); err == nil {
			expr = unquoted
		} else {
			expr = expr[1 : len(expr)-1]
		}
	}
	expr = strings.TrimSpace(expr)
	switch expr {
	case "(.*)", ".*", "^.*$", "(?s).*", "(?s:.*)":
		return true
	default:
		return false
	}
}

// translateLabelFilter handles label comparison filters.
func translateLabelFilter(stage string, labelFn LabelTranslateFunc) string {
	if chained, ok := translateLogicalLabelFilterChain(stage, labelFn); ok {
		return chained
	}

	if translated, ok := translateSingleLabelFilter(stage, labelFn); ok {
		return translated
	}

	if translated, ok := translateMalformedDottedStage(stage, labelFn); ok {
		return translated
	}

	// Unknown stage — pass through as-is with pipe
	return "| " + stage
}

func translateLogicalLabelFilterChain(stage string, labelFn LabelTranslateFunc) (string, bool) {
	parts, ops, ok := splitLogicalStage(stage)
	if !ok || len(parts) < 2 {
		return "", false
	}

	translated := make([]string, 0, len(parts))
	for _, part := range parts {
		item, ok := translateSingleLabelFilter(part, labelFn)
		if !ok {
			return "", false
		}
		translated = append(translated, item)
	}

	var b strings.Builder
	b.WriteString("(")
	for i, item := range translated {
		if i > 0 {
			b.WriteByte(' ')
			b.WriteString(ops[i-1])
			b.WriteByte(' ')
		}
		b.WriteString(item)
	}
	b.WriteString(")")
	return b.String(), true
}

func splitLogicalStage(stage string) ([]string, []string, bool) {
	var (
		parts   []string
		ops     []string
		start   int
		depth   int
		inQuote rune
	)

	flush := func(end int) bool {
		part := strings.TrimSpace(stage[start:end])
		if part == "" {
			return false
		}
		parts = append(parts, part)
		start = end
		return true
	}

	for i := 0; i < len(stage); i++ {
		ch := rune(stage[i])
		if inQuote != 0 {
			if ch == '\\' {
				i++
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}

		switch ch {
		case '"', '`':
			inQuote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}

		if strings.HasPrefix(stage[i:], " or ") {
			if !flush(i) {
				return nil, nil, false
			}
			ops = append(ops, "or")
			i += len(" or ") - 1
			start = i + 1
			continue
		}
		if strings.HasPrefix(stage[i:], " and ") {
			if !flush(i) {
				return nil, nil, false
			}
			ops = append(ops, "and")
			i += len(" and ") - 1
			start = i + 1
			continue
		}
	}

	last := strings.TrimSpace(stage[start:])
	if last == "" {
		return nil, nil, false
	}
	parts = append(parts, last)
	if len(parts) != len(ops)+1 {
		return nil, nil, false
	}
	return parts, ops, len(parts) > 1
}

func translateSingleLabelFilter(stage string, labelFn LabelTranslateFunc) (string, bool) {
	// Try: label == "value", label = "value", label != "value",
	//      label =~ "value", label !~ "value", label > value, etc.
	ops := []struct {
		logql  string
		logsql string
		isRe   bool
	}{
		{"==", ":=", false},
		{"!=", "-:=", false},
		{"=~", ":~", true},
		{"!~", "-:~", true},
		{">=", ":>=", false},
		{"<=", ":<=", false},
		{">", ":>", false},
		{"<", ":<", false},
		{"=", ":=", false},
	}

	for _, op := range ops {
		idx := strings.Index(stage, op.logql)
		if idx > 0 {
			label := sanitizeFieldIdentifier(stage[:idx])
			value := strings.TrimSpace(stage[idx+len(op.logql):])
			if label == "" {
				return "", false
			}
			if label == "detected_level" {
				label = "level"
			}
			if labelFn != nil {
				label = sanitizeFieldIdentifier(labelFn(label))
				if label == "" {
					return "", false
				}
			}

			value = strings.Trim(value, "\"`")
			label = quoteLogsQLFieldNameIfNeeded(label)

			if op.isRe {
				// Regex values need quotes in VL
				if strings.HasPrefix(op.logsql, "-") {
					return fmt.Sprintf(`-%s%s"%s"`, label, op.logsql[1:], value), true
				}
				return fmt.Sprintf(`%s%s"%s"`, label, op.logsql, value), true
			}
			if value == "" {
				// detected_level="" means "no level detected": match log entries where
				// the level field is absent or has an empty value. VL's -field:* does
				// exactly this (absent OR empty), while field:="" only matches explicit
				// empty strings and misses absent fields entirely.
				if label == "level" && !strings.HasPrefix(op.logsql, "-") {
					return `-level:*`, true
				}
				if strings.HasPrefix(op.logsql, "-") {
					return fmt.Sprintf(`%s:!""`, label), true
				}
				return fmt.Sprintf(`%s:=""`, label), true
			}
			formattedValue := value
			if op.logsql == ":=" || op.logsql == "-:=" {
				// Equality filters can carry arbitrary string payloads
				// (for example stacktraces). Keep simple tokens bare and
				// quote/escape complex values to preserve valid LogsQL.
				formattedValue = formatLogsQLEqualityValue(value)
			}
			if strings.HasPrefix(op.logsql, "-") {
				return fmt.Sprintf("-%s%s%s", label, op.logsql[1:], formattedValue), true
			}
			return fmt.Sprintf("%s%s%s", label, op.logsql, formattedValue), true
		}
	}

	return "", false
}

func quoteLogsQLFieldNameIfNeeded(label string) string {
	if label == "" {
		return label
	}
	for _, r := range label {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return `"` + label + `"`
	}
	return label
}

func formatLogsQLEqualityValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return `""`
	}
	if logsQLEqualityValueNeedsQuoting(value) {
		return strconv.Quote(value)
	}
	return value
}

func logsQLEqualityValueNeedsQuoting(value string) bool {
	if value == "" {
		return true
	}
	hasAlnum := false
	for _, r := range value {
		if unicode.IsSpace(r) {
			return true
		}
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') {
			hasAlnum = true
			continue
		}
		if r == '_' || r == '-' || r == '.' || r == '/' {
			continue
		}
		return true
	}
	// Values made entirely of punctuation (e.g. "/" or "-") are compound tokens
	// in VL's parser and must be quoted. Require at least one alphanumeric character.
	return !hasAlnum
}

func translateMalformedDottedStage(stage string, labelFn LabelTranslateFunc) (string, bool) {
	rawCandidate := normalizeFieldIdentifier(stage)
	if rawCandidate == "" {
		return "", false
	}
	if strings.ContainsAny(rawCandidate, `=<>!~`) {
		return "", false
	}
	trailingDot := strings.HasSuffix(rawCandidate, ".")
	candidate := strings.Trim(rawCandidate, ".")
	candidate = strings.Trim(candidate, ".")
	if !strings.Contains(candidate, ".") {
		return "", false
	}
	for _, r := range candidate {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-' {
			continue
		}
		return "", false
	}
	if labelFn != nil {
		candidate = sanitizeFieldIdentifier(labelFn(candidate))
	}
	if candidate == "" {
		return "", false
	}
	if trailingDot {
		// Some upstream Drilldown paths emit malformed stages like:
		//   k8s . `cluster.`
		// Treat them as prefix regex filters to avoid generating an
		// impossible field matcher such as "k8s.cluster":!"".
		return fmt.Sprintf(`~"%s"`, regexp.QuoteMeta(candidate+".")), true
	}
	return fmt.Sprintf(`%s:!""`, quoteLogsQLFieldNameIfNeeded(candidate)), true
}

func canonicalLabelFilterStage(stage string, labelFn LabelTranslateFunc) (canonical string, baseKey string, ok bool) {
	if translated, ok := translateSingleLabelFilter(stage, labelFn); ok {
		if fieldKey, op, ok := translatedFilterFieldOp(translated); ok {
			// Drilldown include/exclude interactions emit equality/regex filters.
			// Keep only the latest filter per field so repeated clicks on the same
			// field do not accumulate into impossible AND chains.
			if op == ":=" || op == ":~" {
				return translated, fieldKey, true
			}
		}
		return translated, strings.TrimPrefix(translated, "-"), true
	}
	if translated, ok := translateMalformedDottedStage(stage, labelFn); ok {
		return translated, translated, true
	}
	return "", "", false
}

func translatedFilterFieldOp(translated string) (field string, op string, ok bool) {
	s := strings.TrimSpace(translated)
	if s == "" {
		return "", "", false
	}
	s = strings.TrimPrefix(s, "-")
	for _, candidate := range []string{":>=", ":<=", ":~", ":=", ":>", ":<"} {
		idx := strings.Index(s, candidate)
		if idx <= 0 {
			continue
		}
		field = strings.TrimSpace(s[:idx])
		if field == "" {
			return "", "", false
		}
		return field, candidate, true
	}
	return "", "", false
}

func normalizeFieldIdentifier(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return ""
	}
	label = strings.ReplaceAll(label, "`", "")
	label = strings.ReplaceAll(label, `"`, "")
	label = fieldDotSpacingRE.ReplaceAllString(label, ".")
	return strings.TrimSpace(label)
}

func sanitizeFieldIdentifier(label string) string {
	label = normalizeFieldIdentifier(label)
	if label == "" {
		return ""
	}
	label = strings.Trim(label, ".")
	for strings.Contains(label, "..") {
		label = strings.ReplaceAll(label, "..", ".")
	}
	return strings.TrimSpace(label)
}

var fieldDotSpacingRE = regexp.MustCompile(`\s*\.\s*`)

// translateLabelFormat converts label_format expressions.
// Supports multiple renames: label_format a="{{.x}}", b="{{.y}}"
func translateLabelFormat(expr string) string {
	// Split by comma, respecting quotes
	assignments := splitLabelFormatAssignments(expr)
	var pipes []string
	for _, assign := range assignments {
		parts := strings.SplitN(assign, "=", 2)
		if len(parts) != 2 {
			continue
		}
		labelName := strings.TrimSpace(parts[0])
		template := strings.TrimSpace(parts[1])
		pipes = append(pipes, fmt.Sprintf("| format %s as %s", convertGoTemplate(template), labelName))
	}
	if len(pipes) == 0 {
		return "| " + expr
	}
	return strings.Join(pipes, " ")
}

// splitLabelFormatAssignments splits "a=X, b=Y" respecting quoted values.
func splitLabelFormatAssignments(s string) []string {
	var result []string
	inQuote := false
	start := 0
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			part := strings.TrimSpace(s[start:i])
			if part != "" {
				result = append(result, part)
			}
			start = i + 1
		}
	}
	part := strings.TrimSpace(s[start:])
	if part != "" {
		result = append(result, part)
	}
	return result
}

// convertGoTemplate converts Go template syntax {{.label}} to LogsQL <label> syntax.
// Handles dotted field names like {{.service.name}} → <service.name>.
func convertGoTemplate(tmpl string) string {
	tmpl = strings.Trim(tmpl, "\"")
	re := regexp.MustCompile(`\{\{\s*\.([\w.]+)\s*\}\}`)
	result := re.ReplaceAllString(tmpl, "<$1>")
	return `"` + result + `"`
}

// splitFuncFirstArg splits the first argument from a parenthesised function arg list,
// returning (firstArg, rest, ok). rest is the text after the first comma at depth 0.
// Input starts right after the opening '(' of the outer function.
func splitFuncFirstArg(s string) (first, rest string, ok bool) {
	depth := 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return strings.TrimSpace(s[:i]), "", true
			}
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:]), true
			}
		}
	}
	return "", "", false
}

// parseAllStringArgs parses all quoted string arguments (double- or back-tick-quoted)
// from a comma-separated list, stopping at ')' or unrecognised input.
func parseAllStringArgs(s string) []string {
	var args []string
	for {
		s = strings.TrimSpace(s)
		if s == "" || s[0] == ')' {
			break
		}
		if s[0] != '"' && s[0] != '`' {
			break
		}
		q := s[0]
		end := strings.IndexByte(s[1:], q)
		if end < 0 {
			break
		}
		args = append(args, s[1:end+1])
		s = strings.TrimSpace(s[end+2:])
		if len(s) > 0 && s[0] == ',' {
			s = strings.TrimSpace(s[1:])
		}
	}
	return args
}

// tryTranslateLabelReplace handles label_replace(v, "dst", "repl", "src", "regex").
// It translates the inner expression and embeds a marker for proxy post-processing.
func tryTranslateLabelReplace(logql string, labelFn LabelTranslateFunc) (string, bool) {
	const prefix = "label_replace("
	if !strings.HasPrefix(logql, prefix) {
		return "", false
	}
	inner, rest, ok := splitFuncFirstArg(logql[len(prefix):])
	if !ok {
		return "", false
	}
	args := parseAllStringArgs(rest)
	if len(args) != 4 {
		return "", false
	}
	translated, err := translateLogQLFull(inner, labelFn, nil)
	if err != nil {
		return "", false
	}
	payload := base64.StdEncoding.EncodeToString([]byte(
		args[0] + "\x00" + args[1] + "\x00" + args[2] + "\x00" + args[3],
	))
	return translated + labelReplacePrefix + payload + "__", true
}

// tryTranslateLabelJoin handles label_join(v, "dst", "sep", "src1", ...).
// It translates the inner expression and embeds a marker for proxy post-processing.
func tryTranslateLabelJoin(logql string, labelFn LabelTranslateFunc) (string, bool) {
	const prefix = "label_join("
	if !strings.HasPrefix(logql, prefix) {
		return "", false
	}
	inner, rest, ok := splitFuncFirstArg(logql[len(prefix):])
	if !ok {
		return "", false
	}
	args := parseAllStringArgs(rest)
	if len(args) < 3 { // dst + sep + at least one src
		return "", false
	}
	translated, err := translateLogQLFull(inner, labelFn, nil)
	if err != nil {
		return "", false
	}
	payload := base64.StdEncoding.EncodeToString([]byte(
		args[0] + "\x00" + args[1] + "\x00" + strings.Join(args[2:], "\x01"),
	))
	return translated + labelJoinPrefix + payload + "__", true
}

// tryTranslateMetricQuery attempts to translate a metric/aggregation query.
func tryTranslateMetricQuery(logql string, labelFn LabelTranslateFunc) (string, bool) {
	// Match patterns like: sum(rate({...}[5m])) by (label)
	// or: count_over_time({...}[5m])
	// or: rate({...}[5m])

	metricFuncs := map[string]string{
		"rate":             "count()",
		"count_over_time":  "count()",
		"bytes_over_time":  "sum_len(_msg)",
		"bytes_rate":       "sum_len(_msg)",
		"sum_over_time":    "sum",
		"avg_over_time":    "avg",
		"max_over_time":    "max",
		"min_over_time":    "min",
		"first_over_time":  "first",
		"last_over_time":   "last",
		"stddev_over_time": "stddev",
		"stdvar_over_time": "stddev",
		"absent_over_time": "count()",
		"rate_counter":     "__rate_counter__",
	}

	// Try to match outer aggregation: sum(...) by (labels)
	outerAgg, innerExpr, byLabels := extractOuterAggregation(logql)

	// group() returns 1 for every series that has data. Translate the inner metric
	// normally (for grouping/presence), then the proxy normalises all values to 1.
	isGroup := outerAgg == "group"
	if isGroup {
		outerAgg = ""
	}

	// count_values() groups by the VALUES of the inner metric — VL has no equivalent.
	if outerAgg == "count_values" {
		return "", false // caught by the count_values guard in translateLogQLFull
	}

	// If no outer aggregation, work with the raw expression
	if innerExpr == "" {
		innerExpr = logql
	}

	// Special handling for quantile_over_time(phi, {query} | unwrap field [duration])
	if result, ok := tryTranslateQuantileOverTime(innerExpr, outerAgg, byLabels, labelFn); ok {
		if isGroup {
			return result + groupMarker, true
		}
		return result, true
	}

	// Try to match metric function: func({...} | pipeline [duration])
	for funcName, logsqlFunc := range metricFuncs {
		prefix := funcName + "("
		if !strings.HasPrefix(innerExpr, prefix) {
			continue
		}

		// Find the matching closing paren
		fullRest := innerExpr[len(prefix):]
		end := findLastMatchingParen(fullRest)
		if end < 0 {
			continue
		}
		inner := fullRest[:end]
		// Detect trailing by (...) modifier on the range aggregation, e.g.
		// avg_over_time({...} | json | unwrap field [5s]) by ()
		rangeByLabels, rangeByExplicit := extractRangeByClause(strings.TrimSpace(fullRest[end+1:]))

		// Extract the query and duration: {stream} | pipeline [5m]
		query, duration := extractQueryAndDuration(inner)
		if strings.TrimSpace(duration) == "" {
			continue
		}

		// Translate the inner log query part
		logsqlQuery, err := translateLogQuery(query, labelFn)
		if err != nil {
			continue
		}

		if funcName == "rate" || funcName == "bytes_rate" {
			if rateResult, ok := buildRateLikeQuery(logsqlQuery, query, logsqlFunc, duration, outerAgg, byLabels, labelFn); ok {
				if isGroup {
					return rateResult + groupMarker, true
				}
				return rateResult, true
			}
		}

		// Build the LogsQL stats query
		var result string
		if isUnwrapFunc(funcName) {
			// For unwrap functions, extract the unwrap field
			unwrapField := extractUnwrapField(inner)
			if unwrapField == "" {
				continue
			}
			statsExpr := fmt.Sprintf("%s(%s)", logsqlFunc, unwrapField)
			innerBy := unwrapInnerGrouping(query, byLabels, outerAgg, labelFn, rangeByExplicit, rangeByLabels)

			// stdvar_over_time uses stddev + square, since VL doesn't have stdvar().
			if funcName == "stdvar_over_time" {
				if outerAgg != "" && byLabels == "" {
					innerAliased := buildStatsQuery(logsqlQuery, statsExpr, innerBy, "__lvp_inner")
					withVariance := innerAliased + " | math __lvp_inner*__lvp_inner as __lvp_inner_var"
					if outerResult, ok := applyOuterAggregation(withVariance, outerAgg, "__lvp_inner_var"); ok {
						return outerResult, true
					}
				}
				baseStddev := buildStatsQuery(logsqlQuery, statsExpr, innerBy, "")
				return fmt.Sprintf("%s^:%s|||2", BinaryMetricPrefix, baseStddev), true
			}

			if outerAgg != "" && byLabels == "" {
				innerAliased := buildStatsQuery(logsqlQuery, statsExpr, innerBy, "__lvp_inner")
				if outerResult, ok := applyOuterAggregation(innerAliased, outerAgg, "__lvp_inner"); ok {
					result = outerResult
				} else {
					result = buildStatsQuery(logsqlQuery, statsExpr, innerBy, "")
				}
			} else {
				result = buildStatsQuery(logsqlQuery, statsExpr, innerBy, "")
			}
		} else {
			result = fmt.Sprintf("%s | stats %s", logsqlQuery, logsqlFunc)
		}

		// Add by labels
		if byLabels != "" || outerAgg != "" {
			if byLabels != "" {
				result = addByClause(result, byLabels, labelFn)
			}
		}

		if isGroup {
			return result + groupMarker, true
		}
		return result, true
	}

	return "", false
}

func defaultRateInnerGrouping(query string) string {
	if hasParserPipeline(query) {
		return "_stream"
	}
	// Keep level-split streams distinct when level isn't configured as a VL _stream field.
	return "_stream, level"
}

func buildRateLikeQuery(logsqlQuery, originalQuery, statsExpr, duration, outerAgg, byLabels string, labelFn LabelTranslateFunc) (string, bool) {
	seconds := durationSeconds(duration)
	if seconds <= 0 {
		return "", false
	}

	innerBy := defaultRateInnerGrouping(originalQuery)
	if byLabels != "" {
		innerBy = normalizeByLabels(byLabels, labelFn)
	}

	innerAliased := buildStatsQuery(logsqlQuery, statsExpr, innerBy, "__lvp_inner")
	withRate := fmt.Sprintf("%s | math __lvp_inner/%s as __lvp_rate", innerAliased, strconv.FormatFloat(seconds, 'f', -1, 64))

	if outerAgg != "" && byLabels == "" {
		if outerResult, ok := applyOuterAggregation(withRate, outerAgg, "__lvp_rate"); ok {
			return outerResult, true
		}
	}

	return buildStatsQuery(withRate, "sum(__lvp_rate)", innerBy, ""), true
}

func buildStatsQuery(baseQuery, statsExpr, byLabels, alias string) string {
	query := strings.TrimSpace(baseQuery)
	if query == "" {
		query = "*"
	}
	switch byLabels {
	case emptyByGrouping:
		// Explicit by () — emit the clause so the proxy can detect it and return one series.
		query = fmt.Sprintf("%s | stats by () %s", query, statsExpr)
	case "":
		query = fmt.Sprintf("%s | stats %s", query, statsExpr)
	default:
		query = fmt.Sprintf("%s | stats by (%s) %s", query, byLabels, statsExpr)
	}
	if alias != "" {
		query += " as " + alias
	}
	return query
}

func outerAggregationStatsFn(outerAgg string) (statsFn string, pow2 bool) {
	switch outerAgg {
	case "sum":
		return "sum", false
	case "avg":
		return "avg", false
	case "max":
		return "max", false
	case "min":
		return "min", false
	case "count":
		return "count", false
	case "stddev":
		return "stddev", false
	case "stdvar":
		return "stddev", true
	default:
		return "", false
	}
}

func applyOuterAggregation(baseQuery, outerAgg, field string) (string, bool) {
	statsFn, pow2 := outerAggregationStatsFn(outerAgg)
	if statsFn == "" {
		return "", false
	}
	result := fmt.Sprintf("%s | stats %s(%s)", baseQuery, statsFn, field)
	if pow2 {
		result = fmt.Sprintf("%s^:%s|||2", BinaryMetricPrefix, result)
	}
	return result, true
}

var parserStageForUnwrapRE = regexp.MustCompile(`\|\s*(json|logfmt|pattern|regexp|unpack)\b`)

func hasParserPipeline(query string) bool {
	return parserStageForUnwrapRE.MatchString(query)
}

var rangeByClauseRE = regexp.MustCompile(`^by\s*\(([^)]*)\)`)

// extractRangeByClause parses a trailing "by (...)" modifier that appears after
// the closing paren of a range aggregation, e.g. "avg_over_time(...[5s]) by ()".
// Returns the label list (empty string for "by ()") and whether the clause was present.
func extractRangeByClause(suffix string) (labels string, explicit bool) {
	m := rangeByClauseRE.FindStringSubmatch(suffix)
	if m == nil {
		return "", false
	}
	return strings.TrimSpace(m[1]), true
}

func unwrapInnerGrouping(query, byLabels, outerAgg string, labelFn LabelTranslateFunc, rangeByExplicit bool, rangeByLabels string) string {
	// Explicit range-level by (...) takes precedence over all heuristics.
	// by () means aggregate all entries into one series (no label grouping).
	if rangeByExplicit {
		if rangeByLabels == "" {
			// by () means aggregate all entries into one series — use sentinel so
			// buildStatsQuery emits "by ()" and the proxy can detect it downstream.
			return emptyByGrouping
		}
		return normalizeByLabels(rangeByLabels, labelFn)
	}
	if byLabels != "" {
		return normalizeByLabels(byLabels, labelFn)
	}
	if hasParserPipeline(query) {
		return "_stream, _msg"
	}
	if outerAgg != "" {
		return "_stream"
	}
	return ""
}

func durationSeconds(duration string) float64 {
	duration = strings.TrimSpace(duration)
	if duration == "" {
		return 0
	}
	if d, err := time.ParseDuration(duration); err == nil {
		return d.Seconds()
	}

	partRE := regexp.MustCompile(`([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h|d|w|y)`)
	parts := partRE.FindAllStringSubmatch(duration, -1)
	if len(parts) == 0 {
		return 0
	}

	total := 0.0
	consumed := strings.Builder{}
	for _, part := range parts {
		value, err := strconv.ParseFloat(part[1], 64)
		if err != nil {
			return 0
		}
		switch part[2] {
		case "ns":
			total += value / 1e9
		case "us", "µs":
			total += value / 1e6
		case "ms":
			total += value / 1e3
		case "s":
			total += value
		case "m":
			total += value * 60
		case "h":
			total += value * 3600
		case "d":
			total += value * 86400
		case "w":
			total += value * 7 * 86400
		case "y":
			total += value * 365 * 86400
		default:
			return 0
		}
		consumed.WriteString(part[0])
	}
	if consumed.String() != duration {
		return 0
	}
	return total
}

// BinaryMetricOp represents a binary arithmetic expression between two metric queries.
// The proxy evaluates both sides against VL and combines the results.
const BinaryMetricPrefix = "__binary__:"

// tryTranslateBinaryMetricExpr handles expressions like:
//
//	sum(rate({app="x"}[5m])) / sum(rate({app="x",level="error"}[5m]))
//	rate({app="x"}[5m]) * 100
//
// Returns a special string "__binary__:op:leftQuery|||rightQuery" that the proxy
// parses and evaluates by running both queries independently.
func tryTranslateBinaryMetricExpr(logql string, labelFn LabelTranslateFunc) (string, bool) {
	logql = strings.TrimSpace(logql)

	// Strip the "bool" modifier from comparison operators.
	// Loki: "A > bool B" means return 1/0 instead of filtering.
	// We strip "bool" and let applyOp return 1/0 for all comparisons (matching Loki behavior).
	boolRe := regexp.MustCompile(`\s+bool\s+`)
	logql = boolRe.ReplaceAllString(logql, " ")

	// Extract vector matching modifiers before stripping them.
	// on(labels), ignoring(labels) control join behavior.
	// group_left(labels), group_right(labels) control one-to-many cardinality.
	// We pass them through the binary expression format so the proxy can use them.
	vectorMatchRe := regexp.MustCompile(`\s+(on|ignoring|group_left|group_right)\s*\(([^)]*)\)`)
	var vectorMatchMeta []string
	for _, m := range vectorMatchRe.FindAllStringSubmatch(logql, -1) {
		if len(m) >= 3 {
			vectorMatchMeta = append(vectorMatchMeta, m[1]+":"+strings.TrimSpace(m[2]))
		}
	}
	logql = vectorMatchRe.ReplaceAllString(logql, "")
	for strings.Contains(logql, "  ") {
		logql = strings.ReplaceAll(logql, "  ", " ")
	}

	// Find a binary operator at the top level (not inside parens)
	// Includes: /, *, +, -, %, ^, ==, !=, >, <, >=, <=
	ops := []string{" / ", " * ", " + ", " - ", " % ", " ^ ", " == ", " != ", " >= ", " <= ", " > ", " < "}
	depth := 0
	for i, ch := range logql {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 {
			for _, op := range ops {
				if i+len(op) <= len(logql) && logql[i:i+len(op)] == op {
					left := strings.TrimSpace(logql[:i])
					right := strings.TrimSpace(logql[i+len(op):])
					operator := strings.TrimSpace(op)

					// Both sides must be valid metric queries
					leftQL, leftOK := tryTranslateMetricQuery(left, labelFn)
					rightQL, rightOK := tryTranslateMetricQuery(right, labelFn)

					vmSuffix := ""
					if len(vectorMatchMeta) > 0 {
						vmSuffix = "@@@" + strings.Join(vectorMatchMeta, "@@@")
					}

					if !leftOK || !rightOK {
						// One side might be a scalar (e.g., `rate(...) * 100`)
						if leftOK && IsScalar(right) {
							return fmt.Sprintf("%s%s:%s|||%s%s", BinaryMetricPrefix, operator, leftQL, right, vmSuffix), true
						}
						if rightOK && IsScalar(left) {
							return fmt.Sprintf("%s%s:%s|||%s%s", BinaryMetricPrefix, operator, left, rightQL, vmSuffix), true
						}
						continue
					}

					return fmt.Sprintf("%s%s:%s|||%s%s", BinaryMetricPrefix, operator, leftQL, rightQL, vmSuffix), true
				}
			}
		}
	}
	return "", false
}

// IsScalar returns true if the string is a numeric constant.
// Handles integers, floats, negatives, and scientific notation (e.g., 1e5, 1.5e-3, -42).
func IsScalar(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// Use strconv.ParseFloat which handles all numeric formats
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// VectorMatchInfo holds vector matching modifiers for binary expressions.
type VectorMatchInfo struct {
	On         []string // on(labels) — match on these labels only
	Ignoring   []string // ignoring(labels) — match ignoring these labels
	GroupLeft  []string // group_left(extra_labels) — one-to-many, left side is "many"
	GroupRight []string // group_right(extra_labels) — one-to-many, right side is "many"
}

// ParseBinaryMetricExpr parses a "__binary__:op:left|||right[@@@modifier:labels...]" string.
// Returns the operator, left query, right query, vector matching info, and whether it's valid.
func ParseBinaryMetricExpr(s string) (op, left, right string, ok bool) {
	op, left, right, _, ok = ParseBinaryMetricExprFull(s)
	return op, left, right, ok
}

// ParseBinaryMetricExprFull parses binary expression with vector matching metadata.
func ParseBinaryMetricExprFull(s string) (op, left, right string, vm *VectorMatchInfo, ok bool) {
	if !strings.HasPrefix(s, BinaryMetricPrefix) {
		return "", "", "", nil, false
	}
	rest := s[len(BinaryMetricPrefix):]
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return "", "", "", nil, false
	}
	op = rest[:colonIdx]
	body := rest[colonIdx+1:]

	// Split off vector matching metadata at @@@
	vm = &VectorMatchInfo{}
	if atIdx := strings.Index(body, "@@@"); atIdx >= 0 {
		metaPart := body[atIdx+3:]
		body = body[:atIdx]
		for _, segment := range strings.Split(metaPart, "@@@") {
			parts := strings.SplitN(segment, ":", 2)
			if len(parts) != 2 {
				continue
			}
			modifier := parts[0]
			labels := splitLabels(parts[1])
			switch modifier {
			case "on":
				vm.On = labels
			case "ignoring":
				vm.Ignoring = labels
			case "group_left":
				vm.GroupLeft = labels
			case "group_right":
				vm.GroupRight = labels
			}
		}
	}

	sepIdx := strings.Index(body, "|||")
	if sepIdx < 0 {
		return "", "", "", nil, false
	}
	left = body[:sepIdx]
	right = body[sepIdx+3:]
	return op, left, right, vm, true
}

func splitLabels(s string) []string {
	var labels []string
	for _, l := range strings.Split(s, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			labels = append(labels, l)
		}
	}
	return labels
}

func isUnwrapFunc(name string) bool {
	unwrapFuncs := map[string]bool{
		"sum_over_time": true, "avg_over_time": true,
		"max_over_time": true, "min_over_time": true,
		"first_over_time": true, "last_over_time": true,
		"stddev_over_time": true, "stdvar_over_time": true,
		"quantile_over_time": true, "rate_counter": true,
	}
	return unwrapFuncs[name]
}

func missingUnwrapRangeMetricFunc(logql string) string {
	logql = strings.TrimSpace(logql)
	if logql == "" {
		return ""
	}

	if _, inner, _ := extractOuterAggregation(logql); inner != "" {
		logql = strings.TrimSpace(inner)
	}

	funcs := []string{
		"sum_over_time", "avg_over_time",
		"max_over_time", "min_over_time",
		"first_over_time", "last_over_time",
		"stddev_over_time", "stdvar_over_time",
		"quantile_over_time", "rate_counter",
	}

	for _, funcName := range funcs {
		prefix := funcName + "("
		if !strings.HasPrefix(logql, prefix) {
			continue
		}
		inner := logql[len(prefix):]
		end := findLastMatchingParen(inner)
		if end < 0 {
			return ""
		}
		inner = inner[:end]
		if funcName == "quantile_over_time" {
			if strings.Contains(inner, "[") && strings.Contains(inner, "]") && extractUnwrapField(inner) == "" {
				return funcName
			}
			return ""
		}
		_, duration := extractQueryAndDuration(inner)
		if strings.TrimSpace(duration) == "" {
			return ""
		}
		if strings.Contains(duration, ":") {
			return ""
		}
		if extractUnwrapField(inner) == "" {
			return funcName
		}
		return ""
	}

	return ""
}

func extractOuterAggregation(logql string) (agg, inner, byLabels string) {
	// Match two forms:
	// 1. sum(...) by (labels)     — by AFTER
	// 2. sum by (labels) (...)    — by BEFORE
	// 3. topk(K, sum by (labels) (...))  — nested

	aggList := `sum|avg|max|min|count|topk|bottomk|stddev|stdvar|sort|sort_desc|group|count_values`

	// Try form 2 first: sum by (labels) (...) or sum without (labels) (...)
	byBeforeRe := regexp.MustCompile(`^(` + aggList + `)\s+(?:by|without)\s*\(([^)]*)\)\s*\(`)
	bm := byBeforeRe.FindStringSubmatch(logql)
	if bm != nil {
		agg = bm[1]
		byLabels = bm[2]
		rest := logql[len(bm[0]):]
		end := findLastMatchingParen(rest)
		if end >= 0 {
			inner = rest[:end]
			// topk/bottomk carry a leading K arg: topk by (l) (K, inner_expr).
			// count_values carries a quoted label-name first arg: count_values("l", expr).
			// Strip both so innerExpr starts with the actual metric expression.
			switch agg {
			case "topk", "bottomk":
				if commaIdx := strings.Index(inner, ","); commaIdx >= 0 {
					inner = strings.TrimSpace(inner[commaIdx+1:])
				}
			case "count_values":
				args := parseAllStringArgs(inner)
				if len(args) >= 1 {
					q := `"` + args[0] + `"`
					if idx := strings.Index(inner, q); idx >= 0 {
						rest2 := strings.TrimSpace(inner[idx+len(q):])
						if strings.HasPrefix(rest2, ",") {
							inner = strings.TrimSpace(rest2[1:])
						}
					}
				}
			}
			return agg, inner, byLabels
		}
	}

	// Form 1: sum(...) by (labels) or sum(...) without (labels)
	aggRe := regexp.MustCompile(`^(` + aggList + `)\s*\(`)
	m := aggRe.FindStringSubmatch(logql)
	if m == nil {
		return "", "", ""
	}

	agg = m[1]
	rest := logql[len(m[0]):]

	// For topk/bottomk, skip the first numeric arg: topk(10, ...)
	// For count_values, skip the first quoted string arg: count_values("label", ...)
	switch agg {
	case "topk", "bottomk":
		commaIdx := strings.Index(rest, ",")
		if commaIdx >= 0 {
			rest = strings.TrimSpace(rest[commaIdx+1:])
		}
	case "count_values":
		args := parseAllStringArgs(rest)
		if len(args) >= 1 {
			q := `"` + args[0] + `"`
			if idx := strings.Index(rest, q); idx >= 0 {
				rest2 := strings.TrimSpace(rest[idx+len(q):])
				if strings.HasPrefix(rest2, ",") {
					rest = strings.TrimSpace(rest2[1:])
				}
			}
		}
	}

	// Find matching paren
	end := findLastMatchingParen(rest)
	if end < 0 {
		return "", "", ""
	}
	inner = rest[:end]
	rest = strings.TrimSpace(rest[end+1:])

	// Extract by or without clause after: ... by (labels) or ... without (labels)
	byRe := regexp.MustCompile(`^(?:by|without)\s*\(([^)]+)\)`)
	bm2 := byRe.FindStringSubmatch(rest)
	if bm2 != nil {
		byLabels = bm2[1]
	}

	return agg, inner, byLabels
}

func extractQueryAndDuration(inner string) (query, duration string) {
	// Find [duration] at the end
	bracketIdx := strings.LastIndex(inner, "[")
	if bracketIdx >= 0 {
		closeBracket := strings.Index(inner[bracketIdx:], "]")
		if closeBracket >= 0 {
			duration = inner[bracketIdx+1 : bracketIdx+closeBracket]
			query = strings.TrimSpace(inner[:bracketIdx])
			return
		}
	}
	return inner, ""
}

func extractUnwrapField(inner string) string {
	// Find | unwrap field_name or | unwrap duration(field) or | unwrap bytes(field)
	idx := strings.Index(inner, "| unwrap ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(inner[idx+9:])
	// Field name is everything before the next | or [
	end := strings.IndexAny(rest, "|[")
	if end >= 0 {
		rest = strings.TrimSpace(rest[:end])
	} else {
		rest = strings.TrimSpace(rest)
	}

	// Strip conversion wrappers: duration(field) → field, bytes(field) → field
	for _, wrapper := range []string{"duration(", "bytes("} {
		if strings.HasPrefix(rest, wrapper) && strings.HasSuffix(rest, ")") {
			rest = rest[len(wrapper) : len(rest)-1]
		}
	}

	return rest
}

// tryTranslateQuantileOverTime handles quantile_over_time(phi, {query} | unwrap field [duration]).
// Maps to VL: query | stats quantile(phi, field)
func tryTranslateQuantileOverTime(innerExpr, outerAgg, byLabels string, labelFn LabelTranslateFunc) (string, bool) {
	prefix := "quantile_over_time("
	if !strings.HasPrefix(innerExpr, prefix) {
		return "", false
	}

	rest := innerExpr[len(prefix):]
	end := findLastMatchingParen(rest)
	if end < 0 {
		return "", false
	}
	body := rest[:end]

	// Extract phi (first arg before comma): quantile_over_time(0.95, ...)
	commaIdx := strings.Index(body, ",")
	if commaIdx < 0 {
		return "", false
	}
	phi := strings.TrimSpace(body[:commaIdx])
	queryPart := strings.TrimSpace(body[commaIdx+1:])

	// Extract query and duration
	query, duration := extractQueryAndDuration(queryPart)
	if strings.TrimSpace(duration) == "" {
		return "", false
	}

	// Translate the inner log query
	logsqlQuery, err := translateLogQuery(query, labelFn)
	if err != nil {
		return "", false
	}

	// Extract unwrap field
	unwrapField := extractUnwrapField(queryPart)
	if unwrapField == "" {
		return "", false
	}

	statsExpr := fmt.Sprintf("quantile(%s, %s)", phi, unwrapField)
	innerBy := unwrapInnerGrouping(query, byLabels, outerAgg, labelFn, false, "")

	if outerAgg != "" && byLabels == "" {
		innerAliased := buildStatsQuery(logsqlQuery, statsExpr, innerBy, "__lvp_inner")
		if outerResult, ok := applyOuterAggregation(innerAliased, outerAgg, "__lvp_inner"); ok {
			return outerResult, true
		}
	}

	return buildStatsQuery(logsqlQuery, statsExpr, innerBy, ""), true
}

func addByClause(query, labels string, labelFn LabelTranslateFunc) string {
	labels = normalizeByLabels(labels, labelFn)
	if labels == "" {
		return query
	}
	// Insert by(labels) into the stats pipe
	idx := strings.Index(query, "| stats ")
	if idx < 0 {
		return query + " | stats by (" + labels + ")"
	}
	statsStart := idx + len("| stats ")
	return query[:statsStart] + "by (" + labels + ") " + query[statsStart:]
}

func normalizeByLabels(labels string, labelFn LabelTranslateFunc) string {
	parts := strings.Split(labels, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if labelFn != nil {
			part = strings.TrimSpace(labelFn(part))
		}
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return strings.Join(out, ", ")
}

func findMatchingBrace(s string) int {
	depth := 0
	var inQuote rune
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		// Handle backslash escapes inside quotes
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			i++ // skip escaped character
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if inQuote != 0 {
			continue
		}
		if c == '{' {
			depth++
		}
		if c == '}' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func findLastMatchingParen(s string) int {
	depth := 0
	var inQuote rune
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			i++
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if inQuote != 0 {
			continue
		}
		if c == '(' {
			depth++
		}
		if c == ')' {
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

func extractPipelineStage(s string) (stage, rest string) {
	// A pipeline stage goes until the next unquoted |
	var inQuote rune
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			i++
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if inQuote != 0 {
			continue
		}
		if c == '|' {
			return strings.TrimSpace(s[:i]), s[i:]
		}
	}
	return strings.TrimSpace(s), ""
}

// extractQuotedValue extracts a quoted string (respecting escaped quotes) and returns (quoted_value, remaining).
func extractQuotedValue(s string) (string, string) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "\"") {
		// Find the closing quote, skipping escaped quotes (\")
		for i := 1; i < len(s); i++ {
			if s[i] == '"' && (i == 1 || s[i-1] != '\\') {
				return s[:i+1], strings.TrimSpace(s[i+1:])
			}
		}
		// No closing quote found — return everything
		return s, ""
	}
	if strings.HasPrefix(s, "`") {
		if i := strings.IndexByte(s[1:], '`'); i >= 0 {
			// Raw strings should behave like regular quoted literals in the translated query.
			return strconv.Quote(s[1 : i+1]), strings.TrimSpace(s[i+2:])
		}
		return strconv.Quote(s[1:]), ""
	}
	// Not quoted — take until next space or pipe
	end := strings.IndexAny(s, " |")
	if end >= 0 {
		return `"` + s[:end] + `"`, strings.TrimSpace(s[end:])
	}
	return `"` + s + `"`, ""
}

func normalizeQuotedStageExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return expr
	}
	quoted, rest := extractQuotedValue(expr)
	if strings.TrimSpace(rest) == "" && quoted != "" {
		return quoted
	}
	return expr
}

func translatePatternLineFilter(expr string, negative bool) string {
	values, ok := extractPatternFilterValues(expr)
	if !ok || len(values) == 0 {
		return ""
	}

	regexes := make([]string, 0, len(values))
	for _, value := range values {
		regexes = append(regexes, patternFilterValueToRegex(value))
	}

	combined := regexes[0]
	if len(regexes) > 1 {
		combined = "(?:" + strings.Join(regexes, ")|(?:") + ")"
	}

	if negative {
		return "NOT ~" + strconv.Quote(combined)
	}
	return "~" + strconv.Quote(combined)
}

func extractPatternFilterValues(expr string) ([]string, bool) {
	remaining := strings.TrimSpace(expr)
	if remaining == "" {
		return nil, false
	}

	values := make([]string, 0, 1)
	for remaining != "" {
		quoted, rest := extractQuotedValue(remaining)
		value, err := strconv.Unquote(quoted)
		if err != nil {
			return nil, false
		}
		values = append(values, value)

		remaining = strings.TrimSpace(rest)
		if remaining == "" {
			break
		}
		if !strings.HasPrefix(remaining, "or ") {
			return nil, false
		}
		remaining = strings.TrimSpace(remaining[2:])
	}

	return values, len(values) > 0
}

func patternFilterValueToRegex(value string) string {
	if value == "" {
		return ".*"
	}

	parts := strings.Split(value, "<_>")
	for i, part := range parts {
		parts[i] = regexp.QuoteMeta(part)
	}
	return strings.Join(parts, ".*")
}

// splitStreamMatchers splits stream selector content like `app="x",level="error"`
// into individual matchers, respecting quotes.
func splitStreamMatchers(s string) []string {
	var matchers []string
	var quote rune
	start := 0
	for i, c := range s {
		if (c == '"' || c == '`') && (quote == 0 || quote == c) {
			if quote == 0 {
				quote = c
			} else {
				quote = 0
			}
		}
		if c == ',' && quote == 0 {
			m := strings.TrimSpace(s[start:i])
			if m != "" {
				matchers = append(matchers, m)
			}
			start = i + 1
		}
	}
	m := strings.TrimSpace(s[start:])
	if m != "" {
		matchers = append(matchers, m)
	}
	return matchers
}

// streamMatcherToFieldFilter converts a stream matcher like `level="error"`
// to a LogsQL field filter like `level:="error"`.
// Returns "" if the matcher can't be converted (shouldn't happen).
func streamMatcherToFieldFilter(matcher string, labelFn LabelTranslateFunc) string {
	matcher = strings.TrimSpace(matcher)

	// Try operators in order of specificity
	ops := []struct {
		logql  string
		logsql string
		neg    bool
		isRe   bool // regex values need quotes preserved
	}{
		{"!~", ":~", true, true},
		{"=~", ":~", false, true},
		{"!=", ":=", true, false},
		{"=", ":=", false, false},
	}

	for _, op := range ops {
		idx := strings.Index(matcher, op.logql)
		if idx > 0 {
			origLabel := sanitizeFieldIdentifier(matcher[:idx])
			label := origLabel
			value := strings.TrimSpace(matcher[idx+len(op.logql):])
			if label == "" {
				return ""
			}

			// Apply label name translation (e.g., service_name → service.name)
			if origLabel == "service_name" {
				return serviceNameMatcherFilter(op.logsql, value, op.neg, op.isRe)
			}
			// detected_level is a synthetic Loki label synthesized by the proxy.
			// VL stores the field as "level"; translate unconditionally before
			// applying any user-supplied labelFn.
			if origLabel == "detected_level" {
				label = "level"
			}
			if labelFn != nil {
				label = sanitizeFieldIdentifier(labelFn(label))
				if label == "" {
					return ""
				}
			}

			// VL requires quoting for dotted field names
			if strings.Contains(label, ".") {
				label = `"` + label + `"`
			}

			if op.isRe {
				value = strings.Trim(value, "\"`")
				if op.neg {
					return fmt.Sprintf(`-%s%s"%s"`, label, op.logsql, value)
				}
				return fmt.Sprintf(`%s%s"%s"`, label, op.logsql, value)
			}

			value = strings.Trim(value, "\"`")
			if value == "" {
				// detected_level="" in the stream selector means "no level detected":
				// match entries where level is absent or empty. -level:* covers both
				// cases; level:="" would only match explicit empty strings.
				if label == "level" && !op.neg {
					return `-level:*`
				}
				if op.neg {
					return fmt.Sprintf(`%s:!""`, label)
				}
				return fmt.Sprintf(`%s:=""`, label)
			}
			formattedValue := value
			if op.logsql == ":=" {
				formattedValue = formatLogsQLEqualityValue(value)
			}
			if op.neg {
				return fmt.Sprintf("-%s%s%s", label, op.logsql, formattedValue)
			}
			return fmt.Sprintf("%s%s%s", label, op.logsql, formattedValue)
		}
	}
	return ""
}

// canUseStreamSelector returns true if a stream matcher can be converted to a VL native
// stream selector (faster index path) instead of a field filter.
// Only exact-match (=) on known _stream_fields qualifies.
// Regex (=~, !~) and negation (!=) always use field filters.
func canUseStreamSelector(matcher string, streamFields map[string]bool, labelFn LabelTranslateFunc) bool {
	matcher = strings.TrimSpace(matcher)
	// Only exact positive match qualifies for stream selectors
	// Reject regex and negation operators
	if strings.Contains(matcher, "!~") || strings.Contains(matcher, "=~") || strings.Contains(matcher, "!=") {
		return false
	}
	idx := strings.Index(matcher, "=")
	if idx <= 0 {
		return false
	}
	label := sanitizeFieldIdentifier(matcher[:idx])
	if label == "" {
		return false
	}
	if label == "service_name" {
		return false
	}
	if labelFn != nil {
		label = sanitizeFieldIdentifier(labelFn(label))
		if label == "" {
			return false
		}
	}
	return streamFields[label]
}

var syntheticServiceNameFields = []string{
	"service_name",
	"service.name",
	"service",
	"app",
	"application",
	"app_name",
	"name",
	"app_kubernetes_io_name",
	"container",
	"container_name",
	"k8s.container.name",
	"k8s_container_name",
	"component",
	"workload",
	"job",
	"k8s.job.name",
	"k8s_job_name",
}

func serviceNameMatcherFilter(op, value string, neg, isRegex bool) string {
	value = strings.TrimSpace(strings.Trim(value, "\"`"))
	formattedValue := value
	if !isRegex {
		formattedValue = formatLogsQLEqualityValue(value)
	}
	parts := make([]string, 0, len(syntheticServiceNameFields))
	for _, field := range syntheticServiceNameFields {
		name := field
		if strings.Contains(name, ".") {
			name = `"` + name + `"`
		}
		if isRegex {
			if neg {
				parts = append(parts, fmt.Sprintf(`-%s%s"%s"`, name, op, value))
			} else {
				parts = append(parts, fmt.Sprintf(`%s%s"%s"`, name, op, value))
			}
			continue
		}
		if value == "" {
			if neg {
				parts = append(parts, fmt.Sprintf(`%s:!""`, name))
			} else {
				parts = append(parts, fmt.Sprintf(`%s:=""`, name))
			}
			continue
		}
		if neg {
			parts = append(parts, fmt.Sprintf(`-%s%s%s`, name, op, formattedValue))
		} else {
			parts = append(parts, fmt.Sprintf(`%s%s%s`, name, op, formattedValue))
		}
	}
	if value == "" && !isRegex {
		if neg {
			return "(" + strings.Join(parts, " OR ") + ")"
		}
		return strings.Join(parts, " ")
	}
	if neg {
		return strings.Join(parts, " ")
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func isParserStage(translated string) bool {
	return translated == "| unpack_json" || translated == "| unpack_logfmt" ||
		strings.HasPrefix(translated, "| extract ") || strings.HasPrefix(translated, "| extract_regexp ")
}

func isFieldFilter(s string) bool {
	// Field filters contain :=, :!, :~, :>, :<, :>=, :<=
	return strings.Contains(s, ":=") || strings.Contains(s, ":~") ||
		strings.Contains(s, ":!") || strings.Contains(s, ":>") || strings.Contains(s, ":<")
}

func translateBareFilter(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Bare text after stream selector — treat as phrase filter
	return `"` + s + `"`
}

// looksLikeBareLabelMatcher reports whether s is an unbraced LogQL
// stream matcher such as `app="json-test"` or `level=~"warn|error"`. Such
// inputs must be rejected before falling into translateBareFilter, otherwise
// the translator emits malformed LogsQL like `"app="json-test""` that
// VictoriaLogs rejects.
//
// The check is intentionally conservative: it only matches strings that BEGIN
// with `<identifier> [op] "value"` (or backtick value). This avoids
// false-positives on text-with-equals or expressions that contain matchers
// nested inside parens (e.g. `sum(rate({app="x"}[5m])) by (app)` — those
// reach this code path only after the metric/binary/subquery extractors
// declined them, but the leading char would be `s` followed by `u`, not an
// identifier directly followed by `=`).
func looksLikeBareLabelMatcher(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// Must contain a quote and an `=`.
	if !strings.ContainsAny(s, "\"`") || !strings.Contains(s, "=") {
		return false
	}
	// Must START with an identifier (label name).
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '_' || c == '.' || c == '-' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') {
			i++
			continue
		}
		break
	}
	if i == 0 {
		// Doesn't start with an identifier (might start with `(`, `{`, `"`, etc.)
		return false
	}
	// Skip optional whitespace.
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= len(s) {
		return false
	}
	// Must be followed by an operator: `=`, `=~`, `!=`, `!~`.
	switch s[i] {
	case '=':
		i++
	case '!':
		i++
		if i >= len(s) || (s[i] != '=' && s[i] != '~') {
			return false
		}
		i++
	default:
		return false
	}
	// Tolerate `=~` after the leading `=`.
	if i < len(s) && s[i] == '~' {
		i++
	}
	// Skip optional whitespace.
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= len(s) {
		return false
	}
	// Must be followed by an opening quote (`"` or `` ` ``).
	return s[i] == '"' || s[i] == '`'
}

// =============================================================================
// Subquery support: outer_func(inner_metric_query[range:step])
// Proxy evaluates inner query at sub-step intervals and aggregates.
// =============================================================================

// SubqueryPrefix marks a translated subquery expression for proxy-side evaluation.
const SubqueryPrefix = "__subquery__:"

// tryTranslateSubquery detects and translates subquery syntax.
// Input: max_over_time(rate({app="nginx"}[5m])[1h:5m])
// Output: __subquery__:max_over_time:<translated inner query>:1h:5m
func tryTranslateSubquery(logql string) (string, bool) {
	// Look for [range:step] pattern inside the expression
	subqInlineRe := regexp.MustCompile(`\[(\d+[smhd]+):(\d+[smhd]+)\]`)
	if !subqInlineRe.MatchString(logql) {
		return "", false
	}

	// Extract the outer function: everything before the first "("
	// E.g., "max_over_time(rate({app="nginx"}[5m])[1h:5m])" → "max_over_time"
	parenIdx := strings.Index(logql, "(")
	if parenIdx < 0 {
		return "", false
	}
	outerFunc := strings.TrimSpace(logql[:parenIdx])

	// Validate it's a known aggregation function
	knownOuter := map[string]bool{
		"max_over_time": true, "min_over_time": true,
		"avg_over_time": true, "sum_over_time": true,
		"count_over_time": true, "stddev_over_time": true,
		"stdvar_over_time": true, "last_over_time": true,
		"first_over_time": true, "quantile_over_time": true,
	}
	if !knownOuter[outerFunc] {
		return "", false
	}

	// The body is everything inside the outer function's parens
	body := logql[parenIdx+1:]
	// Find the last closing paren
	lastParen := strings.LastIndex(body, ")")
	if lastParen < 0 {
		return "", false
	}
	body = body[:lastParen]

	// Find [range:step] at the end of body
	loc := subqInlineRe.FindStringSubmatchIndex(body)
	if loc == nil {
		return "", false
	}

	// Check that this [range:step] is at the END of the body (after the inner query's closing paren)
	// The inner query ends just before the [range:step]
	rangeStepStart := loc[0]
	rng := body[loc[2]:loc[3]]
	step := body[loc[4]:loc[5]]

	innerQuery := strings.TrimSpace(body[:rangeStepStart])

	// The inner query should be a complete metric expression.
	// Translate it as a normal metric query.
	translatedInner, err := TranslateLogQL(innerQuery)
	if err != nil {
		return "", false
	}

	return fmt.Sprintf("%s%s:%s:%s:%s", SubqueryPrefix, outerFunc, translatedInner, rng, step), true
}

// ParseSubqueryExpr parses a "__subquery__:func:innerQuery:range:step" string.
// Returns the outer function, inner translated query, range, step, and whether it's a subquery.
func ParseSubqueryExpr(s string) (outerFunc, innerQuery, rng, step string, ok bool) {
	if !strings.HasPrefix(s, SubqueryPrefix) {
		return "", "", "", "", false
	}
	rest := s[len(SubqueryPrefix):]

	// Format: "func:innerQuery:range:step"
	// The innerQuery may contain colons (e.g., in field filters), so we parse from both ends.
	// The last two colon-separated segments are range and step (simple duration strings).
	// Find the step (last segment)
	lastColon := strings.LastIndex(rest, ":")
	if lastColon < 0 {
		return "", "", "", "", false
	}
	step = rest[lastColon+1:]
	rest = rest[:lastColon]

	// Find the range (now last segment)
	lastColon = strings.LastIndex(rest, ":")
	if lastColon < 0 {
		return "", "", "", "", false
	}
	rng = rest[lastColon+1:]
	rest = rest[:lastColon]

	// Find the outer function (first segment)
	firstColon := strings.Index(rest, ":")
	if firstColon < 0 {
		return "", "", "", "", false
	}
	outerFunc = rest[:firstColon]
	innerQuery = rest[firstColon+1:]

	return outerFunc, innerQuery, rng, step, true
}
