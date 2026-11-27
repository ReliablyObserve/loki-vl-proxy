package lower

import (
	"slices"
	"strings"

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
// grouping, more than maxLabelReplaceNesting calls, and a NUL byte in the destination, replacement or source label (the marker's separator, which the
// proxy would read back as one).
func labelReplace(n *queryplan.LabelReplace, prof Profile) (physical.Op, error) {
	if prof.spent() {
		return nil, ErrBudgetExhausted
	}
	return labelReplaceOver(n, prof, true)
}

// labelReplaceOver is labelReplace; rankOK says a ranking, stddev or stdvar may stand under the calls. It is false for
// the operand of a level the proxy evaluates, which the proxy translates as text: a level inside that text is dropped
// by the translator, so the route evaluates nothing the plan could say there.
func labelReplaceOver(n *queryplan.LabelReplace, prof Profile, rankOK bool) (physical.Op, error) {
	var specs []physical.LabelReplace
	x := queryplan.Node(n)
	for {
		lr, ok := x.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		// The marker separates the arguments with a NUL byte: the proxy reads one in the first three as a separator.
		if strings.ContainsRune(lr.Dst, 0) || strings.ContainsRune(lr.Replacement, 0) || strings.ContainsRune(lr.Src, 0) {
			return nil, errLabelReplaceNUL
		}
		specs = append(specs, physical.LabelReplace{DstLabel: lr.Dst, Replacement: lr.Replacement, SrcLabel: lr.Src, Regex: lr.Regex})
		x = lr.In
	}
	if len(specs) > maxLabelReplaceNesting {
		return nil, errLabelReplaceNesting
	}
	// The calls are read outermost first; the markers are written innermost first.
	slices.Reverse(specs)
	if err := labelReplaceShape(x); err != nil {
		return nil, err
	}
	if !rankOK && evaluates(x) {
		return nil, &NotLoweredError{Construct: "LabelReplace operand " + Construct(x)}
	}
	inner := Profile{Label: prof.Label, KeepLine: prof.KeepLine, AliasField: prof.AliasField, FieldAlias: prof.FieldAlias, ServiceNameStreamLabelOnly: prof.ServiceNameStreamLabelOnly}
	var op physical.Op
	var err error
	switch v := x.(type) {
	case *queryplan.RangeAgg:
		op, err = rangeAgg(v, inner)
	case *queryplan.VectorAgg:
		if evaluates(v) {
			// A stddev or stdvar is evaluated by the proxy, like a ranking (below).
			op, err = evaluated(v, prof)
		} else {
			op, err = vectorAgg(v, inner)
		}
	case *queryplan.BinOp:
		if withoutOperand(v) {
			// The proxy evaluates the operands alone, folded by their own clauses; the translator's text for the
			// whole call drops the clauses (binOp). Not lowered under the calls yet.
			err = errBinOperandWithout
		} else {
			op, err = binOp(v, inner)
		}
	case *queryplan.Rank:
		// The ranking is evaluated by the proxy, whose operand is translated with the request's own options.
		op, err = evaluated(v, prof)
	}
	if err != nil {
		return nil, operandError(err, &NotLoweredError{Construct: "LabelReplace operand " + Construct(x)})
	}
	if derivesServiceName(op) {
		return nil, errLabelReplaceServiceName
	}
	return physical.LabelMap{Replace: specs, In: op}, nil
}

// maxLabelReplaceNesting bounds the label_replace calls one expression is wrapped in that the lowering writes. Queries
// nest a few; deeper ones are left to the translator.
const maxLabelReplaceNesting = 16

var (
	errLabelReplaceServiceName = &NotLoweredError{Construct: "LabelReplace service_name grouping"}
	errLabelReplaceNUL         = &NotLoweredError{Construct: "LabelReplace argument with NUL"}
	errLabelReplaceNesting     = &NotLoweredError{Construct: "LabelReplace nesting"}
)

// labelReplaceShape answers the NotLoweredError of the expression under the label_replace calls when it is not one
// the lowering writes.
func labelReplaceShape(x queryplan.Node) error {
	switch x.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg, *queryplan.BinOp, *queryplan.Rank:
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
