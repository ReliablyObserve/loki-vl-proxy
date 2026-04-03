// Package translator converts LogQL queries to LogsQL queries.
//
// Based on: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/
package translator

import (
	"fmt"
	"regexp"
	"strings"
)

// TranslateLogQL converts a LogQL query string to a LogsQL query string.
// It handles stream selectors, line filters, label filters, parsers, and metric queries.
func TranslateLogQL(logql string) (string, error) {
	logql = strings.TrimSpace(logql)
	if logql == "" {
		return "*", nil
	}

	// Check if this is a metric query (wrapping function like rate, count_over_time, etc.)
	if metricResult, ok := tryTranslateMetricQuery(logql); ok {
		return metricResult, nil
	}

	return translateLogQuery(logql)
}

// translateLogQuery handles log queries (non-metric).
func translateLogQuery(logql string) (string, error) {
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
			ff := streamMatcherToFieldFilter(m)
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

	// Line filters
	if strings.HasPrefix(stage, "= ") || strings.HasPrefix(stage, "=\"") {
		// |= "text" → "text"
		return extractQuotedOrWord(stage[1:])
	}
	if strings.HasPrefix(stage, "!= ") || strings.HasPrefix(stage, "!=\"") {
		// != "text" → -"text"
		return "-" + extractQuotedOrWord(stage[2:])
	}
	if strings.HasPrefix(stage, "~ ") || strings.HasPrefix(stage, "~\"") {
		// |~ "regexp" → ~"regexp"
		return "~" + extractQuotedOrWord(stage[1:])
	}
	if strings.HasPrefix(stage, "!~ ") || strings.HasPrefix(stage, "!~\"") {
		// !~ "regexp" → NOT ~"regexp"
		return "NOT ~" + extractQuotedOrWord(stage[2:])
	}

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
func translateLabelFormat(expr string) string {
	// label_name="{{.other}}" → | format "<other>" as label_name
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return "| " + expr
	}
	labelName := strings.TrimSpace(parts[0])
	template := strings.TrimSpace(parts[1])
	return fmt.Sprintf("| format %s as %s", convertGoTemplate(template), labelName)
}

// convertGoTemplate converts Go template syntax {{.label}} to LogsQL <label> syntax.
func convertGoTemplate(tmpl string) string {
	tmpl = strings.Trim(tmpl, "\"")
	re := regexp.MustCompile(`\{\{\s*\.(\w+)\s*\}\}`)
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
	}

	// Try to match outer aggregation: sum(...) by (labels)
	outerAgg, innerExpr, byLabels := extractOuterAggregation(logql)

	// If no outer aggregation, work with the raw expression
	if innerExpr == "" {
		innerExpr = logql
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
		logsqlQuery, err := translateLogQuery(query)
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

func isUnwrapFunc(name string) bool {
	unwrapFuncs := map[string]bool{
		"sum_over_time": true, "avg_over_time": true,
		"max_over_time": true, "min_over_time": true,
		"first_over_time": true, "last_over_time": true,
		"stddev_over_time": true, "stdvar_over_time": true,
	}
	return unwrapFuncs[name]
}

func extractOuterAggregation(logql string) (agg, inner, byLabels string) {
	// Match two forms:
	// 1. sum(...) by (labels)     — by AFTER
	// 2. sum by (labels) (...)    — by BEFORE
	// 3. topk(K, sum by (labels) (...))  — nested

	// Try form 2 first: sum by (labels) (...)
	byBeforeRe := regexp.MustCompile(`^(sum|avg|max|min|count|topk|bottomk)\s+by\s*\(([^)]+)\)\s*\(`)
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

	// Form 1: sum(...) by (labels)
	aggRe := regexp.MustCompile(`^(sum|avg|max|min|count|topk|bottomk)\s*\(`)
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

	// Extract by clause after: ... by (labels)
	byRe := regexp.MustCompile(`^by\s*\(([^)]+)\)`)
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
	// Find | unwrap field_name
	idx := strings.Index(inner, "| unwrap ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(inner[idx+9:])
	// Field name is everything before the next | or [
	end := strings.IndexAny(rest, "|[")
	if end >= 0 {
		return strings.TrimSpace(rest[:end])
	}
	return strings.TrimSpace(rest)
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

// extractQuotedValue extracts a quoted string and returns (quoted_value, remaining).
func extractQuotedValue(s string) (string, string) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "\"") {
		end := strings.Index(s[1:], "\"")
		if end >= 0 {
			return s[:end+2], strings.TrimSpace(s[end+2:])
		}
	}
	// Not quoted — take until next space or pipe
	end := strings.IndexAny(s, " |")
	if end >= 0 {
		return `"` + s[:end] + `"`, strings.TrimSpace(s[end:])
	}
	return `"` + s + `"`, ""
}

func extractQuotedOrWord(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "\"") {
		end := strings.Index(s[1:], "\"")
		if end >= 0 {
			return s[:end+2]
		}
	}
	// Return as quoted
	return `"` + s + `"`
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
func streamMatcherToFieldFilter(matcher string) string {
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

			if op.isRe {
				// Regex values: keep quotes for VL field regex filters
				// VL syntax: field:~"regex"
				value = strings.Trim(value, `"`)
				if op.neg {
					return fmt.Sprintf(`-%s%s"%s"`, label, op.logsql, value)
				}
				return fmt.Sprintf(`%s%s"%s"`, label, op.logsql, value)
			}

			// Exact match: strip quotes
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
