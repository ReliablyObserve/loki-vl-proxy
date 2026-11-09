package logql

import (
	"regexp/syntax"
)

// parseTimeStageError is the error Loki raises for a pipeline stage while it parses the query, or "": a regexp or
// pattern parser it cannot build, a label filter, drop or keep regex that does not parse (`| x=~"("` answers
// `parse error : error parsing regexp: missing closing ): ...`), and a second unwrap. Recorded with the Loki oracle
// (TestStageParseErrorsMatchLoki): these come in pipeline order, before the selector's empty-compatible check, the
// range aggregation's grouping and unwrap rules and every stage Loki builds afterwards.
func parseTimeStageError(stages []Stage) string {
	unwraps := 0
	for _, s := range stages {
		var err error
		switch st := s.(type) {
		case *UnwrapStage:
			if unwraps++; unwraps > 1 {
				return "parse error : syntax error: unexpected unwrap"
			}
		case *ParserStage:
			if st.Type == ParserRegexp || st.Type == ParserPattern {
				if msg := validateParserStage(st); msg != "" {
					return msg
				}
			}
		case *LabelFilterStage:
			err = labelFilterRegexError(st.Expr)
		case *DropStage:
			err = matcherListRegexError(st.Matchers)
		case *KeepStage:
			err = matcherListRegexError(st.Matchers)
		}
		if err != nil {
			return newParseError(0, 0, err.Error()).Msg
		}
	}
	return ""
}

// labelFilterRegexError is the parse error of the first =~ or !~ regex of a label filter, left to right, or nil.
func labelFilterRegexError(e *LabelFilterExpr) error {
	switch {
	case e == nil:
		return nil
	case e.IsLeaf():
		if (e.Cmp == "=~" || e.Cmp == "!~") && e.Kind == LabelFilterValueString {
			return matcherRegexError(e.Value)
		}
		return nil
	}
	if err := labelFilterRegexError(e.Left); err != nil {
		return err
	}
	return labelFilterRegexError(e.Right)
}

// matcherListRegexError is the parse error of the first =~ or !~ regex of a drop or keep list, or nil.
func matcherListRegexError(matchers []DropMatcher) error {
	for _, m := range matchers {
		if m.Op == "=~" || m.Op == "!~" {
			if err := matcherRegexError(m.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

// matcherRegexError is the error of a label matcher regex that does not parse, read as written.
func matcherRegexError(re string) error {
	_, err := syntax.Parse(re, syntax.Perl)
	return err
}
