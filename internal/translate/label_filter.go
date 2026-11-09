package translate

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// ParsedKeySep joins the keys a label that names several parsed keys translates to; a label filter on it is
// the string translator's to write.
const ParsedKeySep = "\x00"

// LabelFilterOp is what a LogQL label filter operator means: the LogsQL field operator, whether the filter is
// excluded, whether the value is a regexp and whether it is a comparison (written unquoted).
type LabelFilterOp struct {
	VLOp   logsql.FieldOp
	Negate bool
	IsRe   bool
	IsComp bool
}

// LabelFilterOps lists the LogQL label filter operators in decreasing specificity, so that "!~" is found before
// "!" and "=" last.
var LabelFilterOps = []struct {
	LogQL string
	Op    LabelFilterOp
}{
	{"==", LabelFilterOp{logsql.FieldOpExact, false, false, false}},
	{"!=", LabelFilterOp{logsql.FieldOpExact, true, false, false}},
	{"=~", LabelFilterOp{logsql.FieldOpRegexp, false, true, false}},
	{"!~", LabelFilterOp{logsql.FieldOpRegexp, true, true, false}},
	{">=", LabelFilterOp{logsql.FieldOpGTE, false, false, true}},
	{"<=", LabelFilterOp{logsql.FieldOpLTE, false, false, true}},
	{">", LabelFilterOp{logsql.FieldOpGT, false, false, true}},
	{"<", LabelFilterOp{logsql.FieldOpLT, false, false, true}},
	{"=", LabelFilterOp{logsql.FieldOpExact, false, false, false}},
}

// KnownLabelFilterCmp reports whether cmp is one of LabelFilterOps.
func KnownLabelFilterCmp(cmp string) bool {
	for i := range LabelFilterOps {
		if LabelFilterOps[i].LogQL == cmp {
			return true
		}
	}
	return false
}

// LabelFilterLeaf is the filter of one label filter `field cmp value`, where field is the VictoriaLogs field name
// (translated, and quoted where LogsQL needs it) and value the filter's value as Loki reads it: the unquoted text
// of a string, the literal text of a number, duration or bytes size, and the pattern of an ip("...") filter when
// isIP. ok is false for an operator that is none of LabelFilterOps.
//
// A regexp is the one Loki matches (LokiLabelFilterPattern), `level=""` selects the rows without a level, and a
// comparison value is not quoted.
func LabelFilterLeaf(field, cmp, value string, isIP bool, caps logsql.Capabilities) (logsql.FilterExpr, bool) {
	var op LabelFilterOp
	found := false
	for i := range LabelFilterOps {
		if LabelFilterOps[i].LogQL == cmp {
			op, found = LabelFilterOps[i].Op, true
			break
		}
	}
	if !found {
		return nil, false
	}
	if isIP {
		// label = ip("pattern") or label != ip("pattern") (IPLabelFilter).
		if filter, ok := IPLabelFilter(field, value, op.Negate); ok {
			return filter, true
		}
		filter := logsql.NewBuilder(caps).BestIPv4Range(field, value)
		if ff, ok := filter.(logsql.FieldFilter); ok && op.Negate {
			ff.Negate = true
			return ff, true
		}
		return filter, true
	}
	if op.IsRe {
		// Loki simplifies a label filter's regexp before matching it
		// (LokiLabelFilterPattern); `=~""` keeps every line.
		return WholeValueRegexpFilterExpr(field, LokiLabelFilterPattern(value), op.Negate, false), true
	}
	if op.IsComp {
		return BuildFieldFilter(field, op.VLOp, value, op.Negate), true
	}
	if value == "" {
		// detected_level="" means "no level detected": match log entries where
		// the level field is absent or has an empty value. VL's -field:* does
		// exactly this (absent OR empty), while field:="" only matches explicit
		// empty strings and misses absent fields entirely.
		if field == "level" && !op.Negate {
			return logsql.Exclude{Expr: logsql.FieldFilter{Field: "level", Op: logsql.FieldOpAny}}, true
		}
		if op.Negate {
			// field:!"" means "field exists and is non-empty".
			return logsql.FieldFilter{Field: field, Op: logsql.FieldOpNotExact}, true
		}
		return BuildFieldFilter(field, logsql.FieldOpExact, "", false), true
	}
	return BuildFieldFilter(field, op.VLOp, value, op.Negate), true
}

// ParenthesizeCompound wraps a translated filter that holds a top-level space (an `a OR b` alternation) so an
// enclosing and/or cannot split it.
func ParenthesizeCompound(s string) string {
	if HasTopLevelSpace(s) {
		return "(" + s + ")"
	}
	return s
}

// HasTopLevelSpace reports whether a filter text holds a space outside quotes and parentheses.
func HasTopLevelSpace(s string) bool {
	var quote byte
	depth := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ' ' && depth == 0:
			return true
		}
	}
	return false
}

// IsFieldFilter reports whether a translated filter is a field filter (:=, :!, :~, :>, :<), or the match-all or
// match-none filter a service_name matcher can reduce to: the filters the string translator gives their own
// `| filter` stage after a pipe.
func IsFieldFilter(s string) bool {
	if s == "*" || s == "-*" {
		return true
	}
	return strings.Contains(s, ":=") || strings.Contains(s, ":~") ||
		strings.Contains(s, ":!") || strings.Contains(s, ":>") || strings.Contains(s, ":<")
}
