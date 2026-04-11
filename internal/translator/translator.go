// Package translator converts LogQL queries to LogsQL queries.
//
// Based on: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/
package translator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// LabelTranslateFunc translates a Loki label name to a VL field name (query direction).
// If nil, no translation is performed.
type LabelTranslateFunc func(lokiLabel string) string

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

	// Extract without() labels before translation.
	// The proxy will post-process VL results to remove these labels from metric grouping.
	var withoutLabels []string
	logql, withoutLabels = extractWithoutLabels(logql)

	// Strip "bool" modifier from comparison operators before translation.
	// Loki: "A > bool B" returns 1/0 instead of filtering. Our applyOp always
	// returns 1/0 for comparisons, so "bool" is a no-op — just strip it.
	boolRe := regexp.MustCompile(`\s+bool\s+`)
	logql = boolRe.ReplaceAllString(logql, " ")

	// Check binary metric expressions FIRST — they may contain metric sub-expressions.
	// E.g., "rate({...}[5m]) > 0" is a binary expr, not just a metric query.
	if binResult, ok := tryTranslateBinaryMetricExpr(logql, labelFn); ok {
		return appendWithoutMarker(binResult, withoutLabels), nil
	}

	// Check if this is a plain metric query (no binary operator at top level)
	if metricResult, ok := tryTranslateMetricQuery(logql, labelFn); ok {
		return appendWithoutMarker(metricResult, withoutLabels), nil
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
		if strings.HasPrefix(remaining, "|= ") || strings.HasPrefix(remaining, "|=\"") {
			// Substring match: |= "text" → ~"text" (NOT "text" which is word-only)
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

		if !strings.HasPrefix(remaining, "|") {
			// Might be bare text — treat as filter
			parts = append(parts, translateBareFilter(remaining))
			break
		}

		// Skip the pipe character for pipe stages
		remaining = strings.TrimSpace(remaining[1:])

		// Determine the pipeline stage type
		stage, rest := extractPipelineStage(remaining)
		remaining = rest

		translated := translatePipelineStage(stage, labelFn)
		if translated != "" {
			// Track parser state — after a parser, label filters become | filter
			if isParserStage(translated) {
				afterParser = true
			}
			// If this is a bare field filter after a parser, wrap it as | filter
			if afterParser && !strings.HasPrefix(translated, "|") && isFieldFilter(translated) {
				translated = "| filter " + translated
			}
			parts = append(parts, translated)
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

// translatePipelineStage converts a single LogQL pipeline stage to LogsQL.
func translatePipelineStage(stage string, labelFn LabelTranslateFunc) string {
	stage = strings.TrimSpace(stage)

	// Note: Line filters (|=, !=, |~, !~) are handled in translateLogQuery
	// before this function is called. Only pipe stages reach here.

	// Unwrap — VL doesn't need unwrap, stats functions take field names directly.
	// | unwrap field_name → silently dropped (the field name is used in the stats function)
	if strings.HasPrefix(stage, "unwrap ") {
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
		return "| extract " + stage[8:]
	}
	if strings.HasPrefix(stage, "regexp ") {
		// | regexp "..." → | extract_regexp "..."
		return "| extract_regexp " + stage[7:]
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

	// Label filters: label op value
	return translateLabelFilter(stage, labelFn)
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
			label := normalizeFieldIdentifier(stage[:idx])
			value := strings.TrimSpace(stage[idx+len(op.logql):])
			if label == "detected_level" {
				label = "level"
			}
			if labelFn != nil {
				label = normalizeFieldIdentifier(labelFn(label))
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
				if strings.HasPrefix(op.logsql, "-") {
					return fmt.Sprintf(`%s:!""`, label), true
				}
				return fmt.Sprintf(`%s:=""`, label), true
			}
			if strings.HasPrefix(op.logsql, "-") {
				return fmt.Sprintf("-%s%s%s", label, op.logsql[1:], value), true
			}
			return fmt.Sprintf("%s%s%s", label, op.logsql, value), true
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

func translateMalformedDottedStage(stage string, labelFn LabelTranslateFunc) (string, bool) {
	candidate := normalizeFieldIdentifier(stage)
	if candidate == "" {
		return "", false
	}
	if strings.ContainsAny(candidate, `=<>!~`) {
		return "", false
	}
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
		candidate = normalizeFieldIdentifier(labelFn(candidate))
	}
	if candidate == "" {
		return "", false
	}
	return fmt.Sprintf(`%s:!""`, quoteLogsQLFieldNameIfNeeded(candidate)), true
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

// tryTranslateMetricQuery attempts to translate a metric/aggregation query.
func tryTranslateMetricQuery(logql string, labelFn LabelTranslateFunc) (string, bool) {
	// Match patterns like: sum(rate({...}[5m])) by (label)
	// or: count_over_time({...}[5m])
	// or: rate({...}[5m])

	metricFuncs := map[string]string{
		"rate":             "rate()",
		"count_over_time":  "count()",
		"bytes_over_time":  "sum(len(_msg))",
		"bytes_rate":       "rate_sum(len(_msg))",
		"sum_over_time":    "sum",
		"avg_over_time":    "avg",
		"max_over_time":    "max",
		"min_over_time":    "min",
		"first_over_time":  "first",
		"last_over_time":   "last",
		"stddev_over_time": "stddev",
		"stdvar_over_time": "stdvar",
		"absent_over_time": "count()",
	}

	// Try to match outer aggregation: sum(...) by (labels)
	outerAgg, innerExpr, byLabels := extractOuterAggregation(logql)

	// If no outer aggregation, work with the raw expression
	if innerExpr == "" {
		innerExpr = logql
	}

	// Special handling for quantile_over_time(phi, {query} | unwrap field [duration])
	if result, ok := tryTranslateQuantileOverTime(innerExpr, outerAgg, byLabels, labelFn); ok {
		return result, true
	}

	// Try to match metric function: func({...} | pipeline [duration])
	for funcName, logsqlFunc := range metricFuncs {
		prefix := funcName + "("
		if !strings.HasPrefix(innerExpr, prefix) {
			continue
		}

		// Find the matching closing paren
		inner := innerExpr[len(prefix):]
		end := findLastMatchingParen(inner)
		if end < 0 {
			continue
		}
		inner = inner[:end]

		// Extract the query and duration: {stream} | pipeline [5m]
		query, _ := extractQueryAndDuration(inner)

		// Translate the inner log query part
		logsqlQuery, err := translateLogQuery(query, labelFn)
		if err != nil {
			continue
		}

		// Build the LogsQL stats query
		var result string
		if isUnwrapFunc(funcName) {
			// For unwrap functions, extract the unwrap field
			unwrapField := extractUnwrapField(inner)
			if unwrapField != "" {
				result = fmt.Sprintf("%s | stats %s(%s)", logsqlQuery, logsqlFunc, unwrapField)
			} else {
				result = fmt.Sprintf("%s | stats %s", logsqlQuery, logsqlFunc)
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

		return result, true
	}

	return "", false
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
	vm := &VectorMatchInfo{}
	op, left, right, vm, ok = ParseBinaryMetricExprFull(s)
	_ = vm
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
		"quantile_over_time": true,
	}
	return unwrapFuncs[name]
}

func extractOuterAggregation(logql string) (agg, inner, byLabels string) {
	// Match two forms:
	// 1. sum(...) by (labels)     — by AFTER
	// 2. sum by (labels) (...)    — by BEFORE
	// 3. topk(K, sum by (labels) (...))  — nested

	aggList := `sum|avg|max|min|count|topk|bottomk|stddev|stdvar|sort|sort_desc`

	// Try form 2 first: sum by (labels) (...) or sum without (labels) (...)
	byBeforeRe := regexp.MustCompile(`^(` + aggList + `)\s+(?:by|without)\s*\(([^)]+)\)\s*\(`)
	bm := byBeforeRe.FindStringSubmatch(logql)
	if bm != nil {
		agg = bm[1]
		byLabels = bm[2]
		rest := logql[len(bm[0]):]
		end := findLastMatchingParen(rest)
		if end >= 0 {
			inner = rest[:end]
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
	if agg == "topk" || agg == "bottomk" {
		commaIdx := strings.Index(rest, ",")
		if commaIdx >= 0 {
			rest = strings.TrimSpace(rest[commaIdx+1:])
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
func tryTranslateQuantileOverTime(innerExpr, _, byLabels string, labelFn LabelTranslateFunc) (string, bool) {
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
	query, _ := extractQueryAndDuration(queryPart)

	// Translate the inner log query
	logsqlQuery, err := translateLogQuery(query, labelFn)
	if err != nil {
		return "", false
	}

	// Extract unwrap field
	unwrapField := extractUnwrapField(queryPart)
	if unwrapField == "" {
		unwrapField = "_msg"
	}

	result := fmt.Sprintf("%s | stats quantile(%s, %s)", logsqlQuery, phi, unwrapField)

	if byLabels != "" {
		result = addByClause(result, byLabels, labelFn)
	}

	return result, true
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
	for i, c := range s {
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
	for i, c := range s {
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
			origLabel := normalizeFieldIdentifier(matcher[:idx])
			label := origLabel
			value := strings.TrimSpace(matcher[idx+len(op.logql):])

			// Apply label name translation (e.g., service_name → service.name)
			if origLabel == "service_name" {
				return serviceNameMatcherFilter(op.logsql, value, op.neg, op.isRe)
			}
			if labelFn != nil {
				label = normalizeFieldIdentifier(labelFn(label))
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
				if op.neg {
					return fmt.Sprintf(`%s:!""`, label)
				}
				return fmt.Sprintf(`%s:=""`, label)
			}
			if op.neg {
				return fmt.Sprintf("-%s%s%s", label, op.logsql, value)
			}
			return fmt.Sprintf("%s%s%s", label, op.logsql, value)
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
	label := strings.TrimSpace(matcher[:idx])
	if label == "service_name" {
		return false
	}
	if labelFn != nil {
		label = labelFn(label)
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
			parts = append(parts, fmt.Sprintf(`-%s%s%s`, name, op, value))
		} else {
			parts = append(parts, fmt.Sprintf(`%s%s%s`, name, op, value))
		}
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
