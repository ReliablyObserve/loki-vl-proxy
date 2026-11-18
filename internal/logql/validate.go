package logql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// MaxInputSize is Loki's syntax.maxInputSize: ParseExpr rejects any query of
// this many bytes or more before lexing it.
const MaxInputSize = 131072

// InputSizeError returns Loki's "input size too long" parse error for a query
// at or above MaxInputSize, or "".
func InputSizeError(query string) string {
	if len(query) >= MaxInputSize {
		return fmt.Sprintf("parse error : input size too long (%d > %d)", len(query), MaxInputSize)
	}
	return ""
}

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
	_, msg := validateLogQL(query)
	return msg
}

// ValidateLogQLExpr is ValidateLogQL that also returns the parsed expression
// (nil for the bare wildcard or on error).
func ValidateLogQLExpr(query string) (Expr, string) {
	return validateLogQL(query)
}

// ValidateLogQLPhases is ValidateLogQLExpr with the error of Loki's pipeline build apart: msg is the error of every
// earlier phase (the parser and its actions, the lexer, the checks over the parsed tree), build the error of a stage
// Loki refuses to build, reported only when msg is "". With a build error the parsed expression is returned, so a
// caller can run checks Loki makes over the tree before it builds (the query plan's) and answer in Loki's order.
func ValidateLogQLPhases(query string) (expr Expr, msg, build string) {
	expr, msg, unknown := validateLogQLTree(query)
	if msg != "" || expr == nil {
		return nil, msg, ""
	}
	if build = semanticPass(expr, query, true); build != "" {
		return expr, "", build
	}
	if unknown != "" {
		// A name the translation cannot carry: the proxy's own refusal, after every check Loki makes.
		return nil, unknown, ""
	}
	return expr, "", ""
}

// validateLogQL is ValidateLogQL that also returns the parsed expression (nil
// for the bare wildcard or on error), so callers can inspect it without a
// second parse.
func validateLogQL(query string) (Expr, string) {
	expr, msg, build := ValidateLogQLPhases(query)
	if build != "" {
		return nil, build
	}
	return expr, msg
}

// validateLogQLTree is validateLogQL without the pipeline build: the parser, its actions, the lexer and the checks
// over the parsed tree. unknown is the refusal of a name the translation cannot carry, for the caller to report
// after the build.
func validateLogQLTree(query string) (expr Expr, msg, unknown string) {
	if msg := InputSizeError(query); msg != "" {
		return nil, msg, ""
	}
	// Bare wildcard: some clients send query=* as "all streams". The parser
	// would reject it, but translateQueryWithContext handles * specially so
	// it must be allowed through validation unchanged. Only the white space
	// Loki's lexer skips is dropped around it.
	if isWildcard(query) {
		return nil, "", ""
	}

	// Structural parse. Loki does not trim the query: white space before the first token and after the last one
	// counts in the line and column of an error, a trailing character that is not white space to Loki (a
	// vertical tab) is a token, and a 0b literal after leading white space is read as after any white space.
	// An empty query and one of white space only are errors of the parser, at the end of the text.
	expr, unknownName, plainUnwrap, err := parseExpressionTree(query)
	if err != nil {
		return nil, formatParseError(err, query), ""
	}
	if plainUnwrap != nil {
		return nil, plainUnwrap.Msg, ""
	}

	// A call kept as text the parser could not type is Loki's syntax error, which ends its parse where it stands.
	if msg := untypedCallsError(expr, query); msg != "" {
		return nil, msg, ""
	}
	// Semantic pass over the typed AST. Loki raises its parser actions while it reads the query, ahead of every
	// check over the parsed tree, in the order of the text.
	if msg := semanticPass(expr, query, false); msg != "" {
		if action := firstActionError(expr); action != "" {
			return nil, action, ""
		}
		return nil, msg, ""
	}
	if unknownName != nil {
		unknown = unknownName.Msg
	}
	return expr, "", unknown
}

// formatParseError converts a Go parse error to a Loki-compatible error
// string. Where we can determine the exact Loki message, we emit it; for
// everything else we prefix with "parse error :" so clients still recognise
// it as a query error.
func formatParseError(err error, raw string) string {
	if lokiErr, ok := err.(*ParseError); ok {
		return lokiErr.Msg
	}
	var lokiErr *ParseError
	if errors.As(err, &lokiErr) {
		return lokiErr.Msg
	}
	msg := err.Error()

	switch {
	case strings.Contains(msg, "empty expression"):
		if raw == "" {
			return "parse error : syntax error: unexpected $end"
		}
		// Only white space and comments: Loki's lexer reaches the end after them.
		line := 1 + strings.Count(raw, "\n")
		col := utf8.RuneCountInString(raw[strings.LastIndexByte(raw, '\n')+1:]) + 1
		return fmt.Sprintf("parse error at line %d, col %d: syntax error: unexpected $end", line, col)
	case strings.Contains(msg, "expected label operator") ||
		strings.Contains(msg, "expected label value"):
		return "parse error at line 1, col 1: syntax error: unexpected }, expecting STRING"
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

// Loki's metadata-endpoint rejections (a selector that cannot be parsed).
const (
	errOnlyLabelMatchers = "only label matchers are supported"
	errOnlyLogSelector   = "only log selector is supported"
)

// ValidateMatchersQuery mirrors syntax.ParseMatchers(query, true), which Loki
// applies to the query parameter of /labels, /label/<name>/values,
// /index/stats, /index/volume(_range), /patterns and /detected_labels. Only a
// bare stream selector is accepted, and it must carry at least one matcher
// that does not match the empty string. Returns "" when valid.
func ValidateMatchersQuery(query string) string {
	// Bare wildcard: tolerated for the same clients ValidateLogQL tolerates.
	if isWildcard(query) {
		return ""
	}
	if msg := InputSizeError(query); msg != "" {
		return msg
	}
	expr, err := parseForValidation(query)
	if err != nil {
		return formatParseError(err, query)
	}
	// syntax.ParseMatchers(query, true) runs syntax.ParseExpr, which validates
	// every selector's matchers, before rejecting a non-selector expression.
	lq, ok := expr.(*LogQuery)
	if !ok || len(lq.Pipeline) > 0 {
		// Loki's parser actions (a selector, label filter, drop or keep regex, a regexp or pattern parser) fail
		// syntax.ParseExpr itself, ahead of the selector rule.
		if msg := firstActionError(expr); msg != "" {
			return msg
		}
		if msg := validateSemantics(expr, query); msg == errEmptyCompatibleMatchers {
			return msg
		}
		return errOnlyLabelMatchers
	}
	return validateSelectorSemantics(lq.Selector)
}

// parseMatchersOnly parses query as a bare stream selector, mirroring
// syntax.ParseMatchers(query, false): parse errors, invalid regexes and
// anything but a selector without pipeline are rejected.
func parseMatchersOnly(query string) (*LogQuery, string) {
	if msg := InputSizeError(query); msg != "" {
		return nil, msg
	}
	expr, err := parseForValidation(query)
	if err != nil {
		return nil, formatParseError(err, query)
	}
	lq, ok := expr.(*LogQuery)
	if !ok || len(lq.Pipeline) > 0 {
		return nil, errOnlyLabelMatchers
	}
	for _, m := range lq.Selector.Matchers {
		if _, err := matcherMatchesEmpty(m); err != nil {
			return nil, invalidRegexMessage(err)
		}
	}
	return lq, ""
}

// ValidateSeriesMatchers mirrors Loki's /series parameter handling
// (loghttp.ParseAndValidateSeriesQuery plus the querier's per-group
// syntax.ParseExpr): `match` and `match[]` values are merged, deduplicated
// and sorted; a single `{}` (ignoring spaces) selects every series; otherwise
// every group must be a bare selector with at least one matcher ("0 matchers
// in group"), and each group must pass the empty-compatible matcher rule.
// Returns "" when valid.
func ValidateSeriesMatchers(groups []string) string {
	seen := make(map[string]struct{}, len(groups))
	deduped := make([]string, 0, len(groups))
	for _, g := range groups {
		if _, ok := seen[g]; !ok {
			seen[g] = struct{}{}
			deduped = append(deduped, g)
		}
	}
	sort.Strings(deduped)
	if len(deduped) == 1 && strings.ReplaceAll(deduped[0], " ", "") == "{}" {
		return ""
	}
	selectors := make([]*StreamSelector, 0, len(deduped))
	for _, g := range deduped {
		// Bare wildcard: tolerated for the same clients ValidateLogQL tolerates.
		if isWildcard(g) {
			continue
		}
		lq, msg := parseMatchersOnly(g)
		if msg != "" {
			return msg
		}
		if len(lq.Selector.Matchers) == 0 {
			return "0 matchers in group: " + g
		}
		selectors = append(selectors, lq.Selector)
	}
	for _, sel := range selectors {
		if msg := validateSelectorSemantics(sel); msg != "" {
			return msg
		}
	}
	return ""
}

// ValidateLogSelectorQuery mirrors syntax.ParseLogSelector(query, true) plus
// the pipeline construction Loki's /detected_fields and
// /detected_field/<name>/values run: a log query with an optional pipeline,
// but never a metric expression. Returns "" when valid.
func ValidateLogSelectorQuery(query string) string {
	expr, msg := validateLogQL(query)
	if msg != "" || expr == nil {
		return msg
	}
	if _, ok := expr.(*LogQuery); !ok {
		return errOnlyLogSelector
	}
	return ""
}
