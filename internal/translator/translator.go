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

// TranslateLogQLWithLabels converts a LogQL query to LogsQL, applying label name
// translation in stream selectors and label filters.
func TranslateLogQLWithLabels(logql string, labelFn LabelTranslateFunc) (string, error) {
	logql = strings.TrimSpace(logql)
	if logql == "" {
		return "*", nil
	}

	// Reject without() clause — VL has no equivalent (it cannot compute the complement label set).
	// Detect both forms: "sum without (...) (...)" and "sum(...) without (...)"
	if containsWithoutClause(logql) {
		return "", fmt.Errorf("without() grouping clause is not supported; use by() with explicit labels instead")
	}

	// Check if this is a metric query (wrapping function like rate, count_over_time, etc.)
	if metricResult, ok := tryTranslateMetricQuery(logql); ok {
		return metricResult, nil
	}

	// Check if this is a binary metric expression: A / B, A + B, A - B, A * B
	if binResult, ok := tryTranslateBinaryMetricExpr(logql); ok {
		return binResult, nil
	}

	return translateLogQuery(logql, labelFn)
}

// containsWithoutClause detects the without() grouping clause in metric queries.
// Only matches top-level "without" keyword, not the word inside quoted strings.
// Handles backslash-escaped quotes inside strings (e.g., "foo\"without\"bar").
func containsWithoutClause(logql string) bool {
	withoutRe := regexp.MustCompile(`\bwithout\s*\(`)
	// Quick check — if the word isn't there at all, skip deeper analysis
	if !withoutRe.MatchString(logql) {
		return false
	}
	// Walk the string to ensure "without" is not inside quotes
	inQuote := false
	for i := 0; i < len(logql); i++ {
		ch := logql[i]
		// Handle backslash escapes inside quoted strings
		if ch == '\\' && inQuote && i+1 < len(logql) {
			i++ // skip escaped character
			continue
		}
		if ch == '"' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		rest := logql[i:]
		if strings.HasPrefix(rest, "without") && withoutRe.MatchString(rest) {
			return true
		}
	}
	return false
}

// translateLogQuery handles log queries (non-metric).
func translateLogQuery(logql string, labelFn LabelTranslateFunc) (string, error) {
	var parts []string

	remaining := logql

	// 1. Extract stream selector {key="value", ...}
	// In VL, stream selectors `{...}` only match labels that were declared as
	// _stream_fields at ingestion time. Labels like "level" are typically NOT
	// stream fields. If we pass {level="error"} to VL, the stream filter
	// returns zero results.
	//
	// Solution: convert ALL Loki stream matchers to LogsQL field filters.
	// This works regardless of whether the label is a stream field or not,
	// because VL's field filters match all indexed fields.
	if strings.HasPrefix(remaining, "{") {
		end := findMatchingBrace(remaining)
		if end < 0 {
			return "", fmt.Errorf("unmatched '{' in stream selector")
		}
		streamContent := remaining[1:end]
		remaining = strings.TrimSpace(remaining[end+1:])

		matchers := splitStreamMatchers(streamContent)
		for _, m := range matchers {
			ff := streamMatcherToFieldFilter(m, labelFn)
			if ff != "" {
				parts = append(parts, ff)
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

		translated := translatePipelineStage(stage)
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
	if result == "" {
		return "*", nil
	}
	return result, nil
}

// translatePipelineStage converts a single LogQL pipeline stage to LogsQL.
func translatePipelineStage(stage string) string {
	stage = strings.TrimSpace(stage)

	// Note: Line filters (|=, !=, |~, !~) are handled in translateLogQuery
	// before this function is called. Only pipe stages reach here.

	// Unwrap — VL doesn't need unwrap, stats functions take field names directly.
	// | unwrap field_name → silently dropped (the field name is used in the stats function)
	if strings.HasPrefix(stage, "unwrap ") {
		return "" // drop — VL handles this implicitly
	}

	// Parsers
	if stage == "json" || stage == "unpack" {
		return "| unpack_json"
	}
	if stage == "logfmt" {
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
	return translateLabelFilter(stage)
}

// translateLabelFilter handles label comparison filters.
func translateLabelFilter(stage string) string {
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
			label := strings.TrimSpace(stage[:idx])
			value := strings.TrimSpace(stage[idx+len(op.logql):])
			value = strings.Trim(value, "\"")

			if op.isRe {
				// Regex values need quotes in VL
				if strings.HasPrefix(op.logsql, "-") {
					return fmt.Sprintf(`-%s%s"%s"`, label, op.logsql[1:], value)
				}
				return fmt.Sprintf(`%s%s"%s"`, label, op.logsql, value)
			}
			if strings.HasPrefix(op.logsql, "-") {
				return fmt.Sprintf("-%s%s%s", label, op.logsql[1:], value)
			}
			return fmt.Sprintf("%s%s%s", label, op.logsql, value)
		}
	}

	// Unknown stage — pass through as-is with pipe
	return "| " + stage
}

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
func tryTranslateMetricQuery(logql string) (string, bool) {
	// Match patterns like: sum(rate({...}[5m])) by (label)
	// or: count_over_time({...}[5m])
	// or: rate({...}[5m])

	metricFuncs := map[string]string{
		"rate":                "rate()",
		"count_over_time":    "count()",
		"bytes_over_time":    "sum(len(_msg))",
		"bytes_rate":         "rate_sum(len(_msg))",
		"sum_over_time":      "sum",
		"avg_over_time":      "avg",
		"max_over_time":      "max",
		"min_over_time":      "min",
		"first_over_time":    "first",
		"last_over_time":     "last",
		"stddev_over_time":   "stddev",
		"stdvar_over_time":   "stdvar",
		"absent_over_time":   "count()",
	}

	// Try to match outer aggregation: sum(...) by (labels)
	outerAgg, innerExpr, byLabels := extractOuterAggregation(logql)

	// If no outer aggregation, work with the raw expression
	if innerExpr == "" {
		innerExpr = logql
	}

	// Special handling for quantile_over_time(phi, {query} | unwrap field [duration])
	if result, ok := tryTranslateQuantileOverTime(innerExpr, outerAgg, byLabels); ok {
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
		logsqlQuery, err := translateLogQuery(query, nil)
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
				result = addByClause(result, byLabels)
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
func tryTranslateBinaryMetricExpr(logql string) (string, bool) {
	logql = strings.TrimSpace(logql)

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
					leftQL, leftOK := tryTranslateMetricQuery(left)
					rightQL, rightOK := tryTranslateMetricQuery(right)

					if !leftOK || !rightOK {
						// One side might be a scalar (e.g., `rate(...) * 100`)
						if leftOK && IsScalar(right) {
							return fmt.Sprintf("%s%s:%s|||%s", BinaryMetricPrefix, operator, leftQL, right), true
						}
						if rightOK && IsScalar(left) {
							return fmt.Sprintf("%s%s:%s|||%s", BinaryMetricPrefix, operator, left, rightQL), true
						}
						continue
					}

					return fmt.Sprintf("%s%s:%s|||%s", BinaryMetricPrefix, operator, leftQL, rightQL), true
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

// ParseBinaryMetricExpr parses a "__binary__:op:left|||right" string.
// Returns the operator, left query, right query, and whether it's a binary expression.
func ParseBinaryMetricExpr(s string) (op, left, right string, ok bool) {
	if !strings.HasPrefix(s, BinaryMetricPrefix) {
		return "", "", "", false
	}
	rest := s[len(BinaryMetricPrefix):]
	// Format: "op:left|||right"
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return "", "", "", false
	}
	op = rest[:colonIdx]
	body := rest[colonIdx+1:]
	sepIdx := strings.Index(body, "|||")
	if sepIdx < 0 {
		return "", "", "", false
	}
	left = body[:sepIdx]
	right = body[sepIdx+3:]
	return op, left, right, true
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
func tryTranslateQuantileOverTime(innerExpr, _, byLabels string) (string, bool) {
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
	logsqlQuery, err := translateLogQuery(query, nil)
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
		result = addByClause(result, byLabels)
	}

	return result, true
}

func addByClause(query, labels string) string {
	// Insert by(labels) into the stats pipe
	idx := strings.Index(query, "| stats ")
	if idx < 0 {
		return query + " | stats by (" + labels + ")"
	}
	statsStart := idx + len("| stats ")
	return query[:statsStart] + "by (" + labels + ") " + query[statsStart:]
}

func findMatchingBrace(s string) int {
	depth := 0
	inQuote := false
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if inQuote {
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
	inQuote := false
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if inQuote {
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
	inQuote := false
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if inQuote {
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
	inQuote := false
	start := 0
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
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
			label := strings.TrimSpace(matcher[:idx])
			value := strings.TrimSpace(matcher[idx+len(op.logql):])

			// Apply label name translation (e.g., service_name → service.name)
			if labelFn != nil {
				label = labelFn(label)
			}

			// VL requires quoting for dotted field names
			if strings.Contains(label, ".") {
				label = `"` + label + `"`
			}

			if op.isRe {
				value = strings.Trim(value, `"`)
				if op.neg {
					return fmt.Sprintf(`-%s%s"%s"`, label, op.logsql, value)
				}
				return fmt.Sprintf(`%s%s"%s"`, label, op.logsql, value)
			}

			value = strings.Trim(value, `"`)
			if op.neg {
				return fmt.Sprintf("-%s%s%s", label, op.logsql, value)
			}
			return fmt.Sprintf("%s%s%s", label, op.logsql, value)
		}
	}
	return ""
}

func isParserStage(translated string) bool {
	return translated == "| unpack_json" || translated == "| unpack_logfmt" ||
		strings.HasPrefix(translated, "| extract ") || strings.HasPrefix(translated, "| extract_regexp ")
}

func isFieldFilter(s string) bool {
	// Field filters contain :=, :~, :>, :<, :>=, :<=
	return strings.Contains(s, ":=") || strings.Contains(s, ":~") ||
		strings.Contains(s, ":>") || strings.Contains(s, ":<")
}

func translateBareFilter(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Bare text after stream selector — treat as phrase filter
	return `"` + s + `"`
}
