package logql

import (
	"fmt"
	"strings"
)

// ValidateLogQL validates a LogQL query string and returns a Loki-compatible
// error message, or "" if the query is syntactically and semantically valid.
//
// The error format matches what Loki 3.x returns so that Grafana datasource
// clients receive the same error shape they expect.
//
// This is the intended replacement for the regex-based validateLogQLSyntax in
// the proxy — it uses the typed parser for structural validation and a semantic
// pass for constraints that require understanding the AST.
func ValidateLogQL(query string) string {
	query = strings.TrimSpace(query)

	// Fast path: Loki's exact error for empty queries.
	if query == "" {
		return "parse error : syntax error: unexpected $end"
	}

	// Fast path: queries starting with | have no stream selector.
	if strings.HasPrefix(query, "|") {
		return "parse error at line 1, col 1: syntax error: unexpected |"
	}

	// Bare wildcard: some clients send query=* as "all streams" — pass through.
	if query == "*" {
		return ""
	}

	// Reject the <> operator — not valid LogQL syntax.
	if idx := strings.Index(query, "<>"); idx >= 0 {
		return fmt.Sprintf("parse error at line 1, col %d: syntax error: unexpected >", idx+2)
	}

	// Structural parse.
	expr, err := Parse(query)
	if err != nil {
		return formatParseError(err, query)
	}

	// Semantic pass over the typed AST.
	if msg := validateSemantics(expr, query); msg != "" {
		return msg
	}

	return ""
}

// formatParseError converts a Go parse error to a Loki-compatible error
// string. Where we can determine the exact Loki message, we emit it; for
// everything else we prefix with "parse error :" so clients still recognise
// it as a query error.
func formatParseError(err error, raw string) string {
	msg := err.Error()

	switch {
	case strings.Contains(msg, "empty expression"):
		return "parse error : syntax error: unexpected $end"
	case strings.Contains(msg, "expected label operator") ||
		strings.Contains(msg, "expected label value"):
		return "parse error at line 1, col 1: syntax error: unexpected }, expecting STRING"
	case strings.Contains(msg, "expected '[' for range"):
		return "parse error at line 1, col 1: syntax error: unexpected ), expecting RANGE"
	case strings.Contains(msg, "unknown pipeline stage"):
		// Extract the stage name for a more helpful message.
		return "parse error : syntax error: unexpected IDENT"
	case strings.Contains(msg, "expected stage name after |"):
		return "parse error at line 1, col 1: syntax error: unexpected |"
	case strings.Contains(msg, "unexpected identifier"):
		return "parse error : syntax error: unexpected IDENT"
	default:
		return "parse error : " + msg
	}
}
