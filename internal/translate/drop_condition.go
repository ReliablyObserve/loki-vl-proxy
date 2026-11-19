package translate

import (
	"fmt"
	"regexp"
	"strconv"
)

// DropCondition represents a matcher-form conditional drop from `| drop field=value`.
// Only entries where the field value satisfies the condition have the field removed.
type DropCondition struct {
	Field string
	Op    string // "=", "!=", "=~", "!~"
	Value string
	re    *regexp.Regexp // non-nil for =~ and !~
	ip    *ipCondition   // non-nil for a label filter `field = ip("...")` (NewIPLabelFilterCondition)
}

// Matches reports whether val satisfies the drop condition.
func (dc DropCondition) Matches(val string) bool {
	if dc.ip != nil {
		return dc.ip.matches(val, dc.Op == "!=")
	}
	switch dc.Op {
	case "=":
		return val == dc.Value
	case "!=":
		return val != dc.Value
	case "=~":
		if dc.re == nil {
			return false
		}
		return dc.re.MatchString(val)
	case "!~":
		if dc.re == nil {
			return false
		}
		return !dc.re.MatchString(val)
	}
	return false
}

// NewDropCondition creates a DropCondition with the regex compiled for =~ and !~ operators.
func NewDropCondition(field, op, value string) (DropCondition, error) {
	dc := DropCondition{Field: field, Op: op, Value: value}
	if op == "=~" || op == "!~" {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return dc, fmt.Errorf("invalid regex %q in drop/keep condition: %w", value, err)
		}
		dc.re = re
	}
	return dc, nil
}

// Operator and Operand are the condition as LevelCondition reads it.
func (dc DropCondition) Operator() string { return dc.Op }

// Operand is the value the condition compares with.
func (dc DropCondition) Operand() string { return dc.Value }

// NewLabelFilterCondition is the DropCondition of a label filter
// `| field op value`: its regexp matches what Loki's label filter matches
// (LokiLabelFilterPattern), where NewDropCondition compiles a drop or keep
// matcher's.
func NewLabelFilterCondition(field, op, value string) (DropCondition, error) {
	dc := DropCondition{Field: field, Op: op, Value: value}
	if op == "=~" || op == "!~" {
		re, err := regexp.Compile(AnchorLabelFilterPattern(LokiLabelFilterPattern(value)))
		if err != nil {
			return dc, fmt.Errorf("invalid regex %q in label filter: %w", value, err)
		}
		dc.re = re
	}
	return dc, nil
}

// NewIPLabelFilterCondition is the DropCondition of a label filter
// `| field = ip("pattern")` (negate: `!=`). Its Value is that LogQL text, so
// code that tells a condition from the empty-value ones keeps working; Matches
// answers what the LogsQL of IPFilter answers on VictoriaLogs for the value, so
// the rows a plan reads itself, its stats pushdown and the log query agree.
func NewIPLabelFilterCondition(field, pattern string, negate bool) (DropCondition, error) {
	op := "="
	if negate {
		op = "!="
	}
	dc := DropCondition{Field: field, Op: op, Value: "ip(" + strconv.Quote(pattern) + ")"}
	ip, ok := newIPCondition(pattern)
	if !ok {
		return dc, fmt.Errorf("invalid ip pattern %q in label filter", pattern)
	}
	dc.ip = ip
	return dc, nil
}

// IPFilter is the LogsQL filter of an ip() label condition on field (the
// label's name, quoted the way LogsQL needs it); ok is false for any other
// condition.
func (dc DropCondition) IPFilter(field string) (filter string, ok bool) {
	if dc.ip == nil {
		return "", false
	}
	f, ok := IPLabelFilter(field, dc.ip.raw, dc.Op == "!=")
	if !ok {
		return "", false
	}
	return f.String(), true
}
