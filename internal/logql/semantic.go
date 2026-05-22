package logql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// validateSemantics walks the AST and enforces constraints that cannot be
// caught by the parser alone. Returns a Loki-compatible error string, or ""
// if the expression is valid.
func validateSemantics(expr Expr, raw string) string {
	switch x := expr.(type) {
	case *LogQuery:
		return validateLogQuerySemantics(x, raw)
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
	}
	return ""
}

func validateLogQuerySemantics(lq *LogQuery, raw string) string {
	// Loki requires at least one non-wildcard matcher.
	if len(lq.Selector.Matchers) == 0 {
		return "parse error at line 1, col 2: parse error : queries require at least one matcher that is not a wildcard"
	}

	// Validate regex matchers.
	for _, m := range lq.Selector.Matchers {
		if m.Op == MatchRe || m.Op == MatchNotRe {
			if _, err := regexp.Compile(m.Value); err != nil {
				return fmt.Sprintf("parse error at line 1, col 1: parse error : invalid regex: %v", err)
			}
		}
	}

	unwrapCount := 0
	for _, s := range lq.Pipeline {
		switch stage := s.(type) {
		case *UnwrapStage:
			unwrapCount++
			if unwrapCount > 1 {
				return "parse error : syntax error: unexpected unwrap"
			}
		case *LineFormatStage:
			if err := validateLineFormatTemplate(stage.Template); err != "" {
				return err
			}
		case *ParserStage:
			// no additional semantic constraints on parsers
		}
	}
	return ""
}

func validateRangeAggSemantics(ra *RangeAggregation, raw string) string {
	// quantile_over_time phi must be in [0, 1] — negative is rejected, >1 is clamped externally.
	if ra.Op == RangeQuantileOverTime && ra.HasParam && ra.Param < 0 {
		phi := strconv.FormatFloat(ra.Param, 'f', -1, 64)
		return "parse error at line 1, col 1: invalid parameter for quantile_over_time: expected range [0, 1] but got " + phi
	}

	// rate()/bytes_rate() cannot be used as the outer function of a subquery.
	// Loki rejects e.g. rate(count_over_time({...}[5m])[30m:5m]) at parse time.
	if (ra.Op == RangeRate || ra.Op == RangeBytesRate) && ra.Step != "" {
		return "parse error at line 1, col 1: parse error : syntax error: unexpected RANGE"
	}

	// For subqueries, the inner is a metric expression — skip log-query-specific checks.
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

	// __error__ / __error_details__ are not accessible inside rate() / bytes_rate().
	if ra.Op == RangeRate || ra.Op == RangeBytesRate {
		if containsErrorLabel(lq) {
			return "parse error : __error__ and __error_details__ are not allowed inside rate() range vectors"
		}
	}

	// Recurse into the inner log query for its own semantic checks.
	if err := validateLogQuerySemantics(lq, raw); err != "" {
		return err
	}
	return ""
}

// containsErrorLabel returns true if any label matcher or label filter stage
// in the log query references __error__ or __error_details__.
func containsErrorLabel(lq *LogQuery) bool {
	for _, m := range lq.Selector.Matchers {
		if m.Name == "__error__" || m.Name == "__error_details__" {
			return true
		}
	}
	for _, s := range lq.Pipeline {
		if lf, ok := s.(*LabelFilterStage); ok {
			if strings.Contains(lf.Raw, "__error__") || strings.Contains(lf.Raw, "__error_details__") {
				return true
			}
		}
	}
	return false
}

// validateLineFormatTemplate checks that the Go template in a line_format
// stage has balanced {{ }} action delimiters.
func validateLineFormatTemplate(tmpl string) string {
	if strings.Count(tmpl, "{{") > strings.Count(tmpl, "}}") {
		stage := `| line_format "` + tmpl + `"`
		return fmt.Sprintf("parse error : stage '%s' : invalid line template: template: line:1: unclosed action", stage)
	}
	return ""
}
