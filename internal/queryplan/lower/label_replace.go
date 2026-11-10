package lower

import (
	"slices"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// labelReplace lowers label_replace calls (nested ones too) around an expression the lowering writes natively: a
// range aggregation, a vector aggregation (a without clause included) or a binary operation. The string translator
// writes the expression's query, then one marker per call, innermost first, which the proxy applies to the answer
// (physical.LabelMap). It translates the expression with no capabilities and no stream fields, then derives
// service_name and detected_level over the whole text again with the request's capabilities: the lowering writes
// the expression under that profile, and leaves to the translator an expression that groups by service_name,
// which the translator then derives twice.
//
// Left to the string translator under a construct name: an expression that is a ranking, vector(), a number or a
// call the parser keeps as text, one the lowering of its construct leaves to the translator, the service_name
// grouping, and a spelling the translator reads unlike the plan (screenLabelReplace, before the lowering).
func labelReplace(n *queryplan.LabelReplace, prof Profile) (physical.Op, error) {
	var specs []physical.LabelReplace
	x := queryplan.Node(n)
	for {
		lr, ok := x.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		specs = append(specs, physical.LabelReplace{DstLabel: lr.Dst, Replacement: lr.Replacement, SrcLabel: lr.Src, Regex: lr.Regex})
		x = lr.In
	}
	// The calls are read outermost first; the markers are written innermost first.
	slices.Reverse(specs)
	if err := labelReplaceShape(x); err != nil {
		return nil, err
	}
	inner := Profile{Label: prof.Label, KeepLine: prof.KeepLine}
	var op physical.Op
	var err error
	switch v := x.(type) {
	case *queryplan.RangeAgg:
		op, err = rangeAgg(v, inner)
	case *queryplan.VectorAgg:
		op, err = vectorAgg(v, inner)
	case *queryplan.BinOp:
		op, err = binOp(v, inner)
	}
	if err != nil {
		return nil, operandError(err, &NotLoweredError{Construct: "LabelReplace operand " + Construct(x)})
	}
	if derivesServiceName(op) {
		return nil, errLabelReplaceServiceName
	}
	return physical.LabelMap{Replace: specs, In: op}, nil
}

var (
	errLabelReplaceServiceName     = &NotLoweredError{Construct: "LabelReplace service_name grouping"}
	errLabelReplaceSpelling        = &NotLoweredError{Construct: "LabelReplace spelling"}
	errLabelReplaceOperandSpelling = &NotLoweredError{Construct: "LabelReplace operand spelling"}
)

// labelReplaceShape answers the NotLoweredError of the expression under the label_replace calls when it is not one
// the lowering writes.
func labelReplaceShape(x queryplan.Node) error {
	switch x.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg, *queryplan.BinOp:
		return nil
	}
	return &NotLoweredError{Construct: "LabelReplace operand " + Construct(x)}
}

// derivesServiceName reports whether a query lowered with no capabilities holds the pipes that derive service_name.
func derivesServiceName(op physical.Op) bool {
	switch v := op.(type) {
	case physical.VLQuery:
		return strings.Contains(v.Text, "service_name") && strings.Contains(v.Text, translate.ServiceNameGroupingPipes(logsql.Capabilities{}))
	case physical.Fold:
		return derivesServiceName(v.In)
	case physical.Binary:
		return derivesServiceName(v.L) || derivesServiceName(v.R)
	}
	return false
}

// screenLabelReplace turns away a label_replace call whose text the string translator does not read as one: it
// reads a call only when the query starts with `label_replace(` (no blank before the parenthesis), and then the
// first argument, as written, as a query of its own, which the screen of its construct reads (ScreenText).
func screenLabelReplace(query string, n *queryplan.LabelReplace) error {
	x := n.In
	for {
		lr, ok := x.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		x = lr.In
	}
	if labelReplaceShape(x) != nil {
		return nil
	}
	rest, ok := strings.CutPrefix(strings.TrimSpace(logql.CanonicalizeLogRanges(query)), "label_replace(")
	if !ok {
		return errLabelReplaceSpelling
	}
	end := logql.FirstArgumentLen(rest)
	if end == len(rest) || rest[end] != ',' {
		return errLabelReplaceSpelling
	}
	if ScreenText(strings.TrimSpace(rest[:end]), &queryplan.Plan{Root: n.In}) != nil {
		return errLabelReplaceOperandSpelling
	}
	return nil
}
