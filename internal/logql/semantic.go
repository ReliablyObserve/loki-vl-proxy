package logql

import (
	"errors"
	"regexp"
	"regexp/syntax"
	"strconv"
)

// errEmptyCompatibleMatchers is Loki's syntax.errAtleastOneEqualityMatcherRequired,
// rendered through logqlmodel.NewParseError.
const errEmptyCompatibleMatchers = `parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~".*" does not meet this requirement, but app=~".+" will`

// validateSemantics walks the AST and enforces constraints that cannot be
// caught by the parser alone. Returns a Loki-compatible error string, or ""
// if the expression is valid.
func validateSemantics(expr Expr, raw string) string {
	switch x := expr.(type) {
	case *LogQuery:
		return validateLogQuerySemantics(x, false)
	case *RangeAggregation:
		return validateRangeAggSemantics(x, raw)
	case *VectorAggregation:
		return validateSemantics(x.Inner, raw)
	case *BinOpExpr:
		// Log stream queries cannot participate in binary metric operations.
		if _, ok := x.Left.(*LogQuery); ok {
			return "parse error at line 1, col 1: unexpected expression for binary operation"
		}
		if _, ok := x.Right.(*LogQuery); ok {
			return "parse error at line 1, col 1: unexpected expression for binary operation"
		}
		if s := validateSemantics(x.Left, raw); s != "" {
			return s
		}
		return validateSemantics(x.Right, raw)
	case *OpaqueMetricExpr:
		// label_replace(inner, ...) and similar wrappers: Loki validates the
		// wrapped sample expression like any other.
		inner, err := opaqueFirstArgument(x.Raw)
		var lokiErr *ParseError
		if errors.As(err, &lokiErr) {
			return lokiErr.Msg
		}
		if inner != nil {
			return validateSemantics(inner, raw)
		}
	}
	return ""
}

// opaqueFirstArgument parses the leading expression argument of an opaque
// metric function call such as `label_replace(rate({app="a"}[5m]), ...)`.
// It returns a nil Expr when the call has no parseable expression argument,
// and the parse error when that argument is itself rejected.
func opaqueFirstArgument(call string) (Expr, error) {
	p := &parser{sc: newScanner(call), input: call}
	p.advance()
	if p.cur.Typ != TokIdent {
		return nil, nil
	}
	p.advance()
	if p.cur.Typ != TokLParen {
		return nil, nil
	}
	p.advance()
	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ != TokComma {
		return nil, nil
	}
	return inner, nil
}

// validateLogQuerySemantics validates a log query. lineIndependent is true
// inside range aggregations whose result does not depend on the log line
// (every op except bytes_rate/bytes_over_time): there Loki's removeLineformat
// optimisation (pkg/logql/optimize.go) drops a line_format stage before the
// pipeline is built, unless a parser or line filter follows it, so only a
// line_format that survives that rule can report an invalid template.
func validateLogQuerySemantics(lq *LogQuery, lineIndependent bool) string {
	if msg := validateSelectorSemantics(lq.Selector); msg != "" {
		return msg
	}

	unwrapCount := 0
	for _, s := range lq.Pipeline {
		if _, ok := s.(*UnwrapStage); ok {
			unwrapCount++
			if unwrapCount > 1 {
				return "parse error : syntax error: unexpected unwrap"
			}
		}
	}
	for i, s := range lq.Pipeline {
		if _, isLineFormat := s.(*LineFormatStage); isLineFormat && lineIndependent && !lineConsumedLater(lq.Pipeline[i+1:]) {
			continue
		}
		if msg := validateStageSemantics(s); msg != "" {
			return msg
		}
	}
	return ""
}

// lineConsumedLater reports whether a later stage reads the log line — a
// parser or a line filter — which makes Loki keep an earlier line_format.
func lineConsumedLater(stages []Stage) bool {
	for _, s := range stages {
		switch s.(type) {
		case *ParserStage, *LineFilterStage:
			return true
		}
	}
	return false
}

// validateSelectorSemantics validates matcher regexes and Loki's
// syntax.validateMatchers rule: at least one matcher must not match the empty
// string, so {app=""}, {app=~""}, {app=~".*"}, {app!="x"} and {} are rejected
// while {app=~".+"} and {app="", pod="a"} are accepted.
func validateSelectorSemantics(sel *StreamSelector) string {
	selective := false
	for _, m := range sel.Matchers {
		matchesEmpty, err := matcherMatchesEmpty(m)
		if err != nil {
			return invalidRegexMessage(err)
		}
		if !matchesEmpty {
			selective = true
		}
	}
	if !selective {
		return errEmptyCompatibleMatchers
	}
	return ""
}

// invalidRegexPrefix starts the message for a selector regex that does not parse.
const invalidRegexPrefix = "parse error at line 1, col 1: parse error : invalid regex: "

func invalidRegexMessage(err error) string {
	return invalidRegexPrefix + err.Error()
}

// matcherMatchesEmpty mirrors Prometheus labels.Matcher.Matches(""). Like
// labels.NewFastRegexMatcher it checks the raw expression's syntax first (so
// `a)|(b` is rejected even though its anchored form parses), then compiles
// the anchored ^(?s:value)$ form once (util.SplitFiltersAndMatchers).
func matcherMatchesEmpty(m LabelMatcher) (bool, error) {
	switch m.Op {
	case MatchEq:
		return m.Value == "", nil
	case MatchNeq:
		return m.Value != "", nil
	case MatchRe, MatchNotRe:
		if _, err := syntax.Parse(m.Value, syntax.Perl); err != nil {
			return false, err
		}
		re, err := regexp.Compile("^(?s:" + m.Value + ")$")
		if err != nil {
			return false, err
		}
		return re.MatchString("") == (m.Op == MatchRe), nil
	}
	return false, nil
}

func validateRangeAggSemantics(ra *RangeAggregation, raw string) string {
	// quantile_over_time phi must be in [0, 1] — negative is rejected, >1 is clamped externally.
	if ra.Op == RangeQuantileOverTime && ra.HasParam && ra.Param < 0 {
		phi := strconv.FormatFloat(ra.Param, 'f', -1, 64)
		return "parse error at line 1, col 1: invalid parameter for quantile_over_time: expected range [0, 1] but got " + phi
	}

	lq, isLogQuery := ra.Inner.(*LogQuery)
	if !isLogQuery {
		return validateSemantics(ra.Inner, raw)
	}

	// rate_counter requires | unwrap inside the range vector.
	if ra.Op == RangeRateCounter {
		hasUnwrap := false
		for _, s := range lq.Pipeline {
			if _, ok := s.(*UnwrapStage); ok {
				hasUnwrap = true
				break
			}
		}
		if !hasUnwrap {
			return "parse error : rate_counter requires | unwrap expression"
		}
	}

	// Recurse into the inner log query for its own semantic checks.
	lineIndependent := ra.Op != RangeBytesRate && ra.Op != RangeBytesOverTime
	if err := validateLogQuerySemantics(lq, lineIndependent); err != "" {
		return err
	}
	return ""
}
