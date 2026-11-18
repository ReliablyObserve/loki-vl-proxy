package lower

import (
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// binOp lowers a binary operation between two operands the lowering writes natively: a range aggregation rangeAgg
// lowers, a vector aggregation vectorAgg lowers, or a number (not two numbers). The string translator writes the
// operands as two metric queries in one text, `__binary__:<op>:<left>|||<right>`, with the vector matching
// modifiers after it (`@@@on:app@@@group_left:pod`), and the proxy evaluates the operation over their answers; the
// lowering returns the physical.Binary that text decodes to. Each operand is the query the lowering writes for it
// alone (the translator derives service_name and detected_level per operand), a number is written as Go prints it,
// and the labels of a modifier are joined with ", ".
//
// An operand with a without clause (at any level of its vector aggregations) is lowered alone too, the fold on the
// operand as Loki reads it (physical.Fold of the operand's clause over its per-series stats). The translator's text
// for the whole query is no reference there: it removes every without clause from the text, writes the operands as
// if they had none, and appends one fold of the first clause's labels after the whole binary
// (semantics/translator-folds-binary-by-operand-without). The proxy never sends that text: the binary route reads
// the query again and evaluates each operand as a query of its own, so such a binary operation is held to the
// proxy's route (proxy.BinaryOperandsPhysical), the text screen does not read it, and RoutedBinary tells it apart.
//
// A `without ()` clause removes no label in Loki, so each series of the operand stays its own: the operand lowers to a
// physical.Fold with a without level and no labels, over the series rows the same operand writes under a clause with
// a label (vectorAggKeepAll). The string translator reads the clause as none and writes the operand ungrouped, and the
// handlers serve that text, so the lowering is not what the proxy sends: EmptyWithoutOperand tells such an operation
// apart, and the corpus shadow records it as a known divergence
// (semantics/binary-operand-without-empty-collapses) instead of counting it native. The text of a fold with no labels
// is the operand's request alone (physical.Encode: the marker needs a label), which is the series rows a proxy that
// folds nothing returns, Loki's answer for the clause.
//
// A vector(n) operand next to a range or vector aggregation is lowered to the physical.VectorOp the proxy's binary
// route answers it from (binop_vector.go), and such a binary operation is held to that route too (RoutedBinary): the
// translator reads the operand as a phrase to search for or drops it.
//
// A bool modifier, the set operators and, or and unless, and an operand that is itself a binary operation (a chain) are
// lowered too, held to the binary route like a without operand (RoutedBinary): the translator's text for the whole
// query drops the modifier, writes the left operand of a set operation alone and splits a chain at its first
// operator, while the route reads the query's tree again and evaluates each operand as a query of its own (a binary
// operand by the same route). Such an operand lowers to its own physical.Binary.
//
// Left to the string translator under a construct name:
//   - and, or and unless with a number or vector(n) operand (BinOp set operator: Loki rejects a number at parse, and the
//     validation answers its error before any route), and a bool modifier next to a vector(n) or between constants
//     (BinOp bool);
//   - two numbers, or numbers and vector(n) only, also through a chain (BinOp constant operands: the proxy folds an instant query of
//     constants itself, before any binary route, and the route's answer for a range query is a constant, which costs
//     less than the plan), an operand that is a binary
//     operation, label_replace, a ranking or a log query, an operand the range or vector aggregation lowering leaves
//     to the translator, and a `without ()` operand over an unwrapped label (BinOp operand without empty unwrap) or
//     in a nest of vector aggregations (BinOp operand without empty nest: a fold carries one level, and the levels
//     above an empty clause are not in it, `count without ()` turns every series into 1);
//   - an operand whose client values hold the bytes of the binary text's own markers (BinOp marker text, decided from
//     the tree): physical.Encode writes the operands between those markers, and their readers split the text at the
//     first one, quoted or not.
func binOp(n *queryplan.BinOp, prof Profile) (physical.Op, error) {
	if prof.spent() {
		return nil, ErrBudgetExhausted
	}
	if err := binOpShape(n); err != nil {
		return nil, err
	}
	if holdsBinaryMarker(n.L) || holdsBinaryMarker(n.R) {
		return nil, errBinMarkerText
	}
	l, err := binOperand(EmptyWithoutLevelsOff(n.L), prof)
	if err != nil {
		return nil, err
	}
	r, err := binOperand(EmptyWithoutLevelsOff(n.R), prof)
	if err != nil {
		return nil, err
	}
	b := physical.Binary{Op: n.Op, Bool: n.Bool, L: l, R: r}
	b.Match, b.Tail = binMatching(n.Match)
	return b, nil
}

// errBinMarkerText is a binary operation an operand of which holds the bytes of a marker of the binary text.
var errBinMarkerText = &NotLoweredError{Construct: "BinOp marker text"}

// holdsBinaryMarker reports an operand whose log queries hold, in a value the client wrote (a matcher, a line filter,
// a label filter, a parser's expression, a template, a drop or keep matcher), the separator of a binary text's operands,
// its prefix, a vector matching modifier or a label_replace marker. The lowering writes such a value into the operand's
// query, where the readers of the binary text (translator.DecodePhysical, read by the ranking and scalar routes) would
// split at it. Decided from the tree: the values are short, and nothing is printed.
func holdsBinaryMarker(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.LogSelect:
		return logSelectHoldsMarker(v)
	case *queryplan.RangeAgg:
		return v.In != nil && logSelectHoldsMarker(v.In)
	case *queryplan.VectorAgg:
		return holdsBinaryMarker(v.In)
	case *queryplan.Rank:
		return holdsBinaryMarker(v.In)
	case *queryplan.LabelReplace:
		return holdsBinaryMarker(v.In)
	case *queryplan.BinOp:
		return holdsBinaryMarker(v.L) || holdsBinaryMarker(v.R)
	}
	return false
}

// logSelectHoldsMarker is holdsBinaryMarker of one log query's values.
func logSelectHoldsMarker(ls *queryplan.LogSelect) bool {
	for _, m := range ls.Matchers {
		if markerBytes(m.Value) {
			return true
		}
	}
	for _, st := range ls.Stages {
		switch v := st.(type) {
		case *logql.LineFilterStage:
			if markerBytes(v.Value) {
				return true
			}
			for _, alt := range v.Or {
				if markerBytes(alt.Value) {
					return true
				}
			}
		case *logql.ParserStage:
			if markerBytes(v.Param) {
				return true
			}
			for _, f := range v.Fields {
				if markerBytes(f.Expression) {
					return true
				}
			}
		case *logql.LabelFilterStage:
			if markerBytes(v.Raw) {
				return true
			}
		case *logql.DropStage:
			if dropMatchersHoldMarker(v.Matchers) {
				return true
			}
		case *logql.KeepStage:
			if dropMatchersHoldMarker(v.Matchers) {
				return true
			}
		case *logql.LineFormatStage:
			if markerBytes(v.Template) {
				return true
			}
		case *logql.LabelFormatStage:
			if markerBytes(v.Raw) {
				return true
			}
		}
	}
	return false
}

func dropMatchersHoldMarker(ms []logql.DropMatcher) bool {
	for _, m := range ms {
		if markerBytes(m.Value) {
			return true
		}
	}
	return false
}

// markerBytes reports a query text that holds one of the binary text's markers.
func markerBytes(s string) bool {
	return strings.Contains(s, "|||") || strings.Contains(s, physical.MatchingPrefix) ||
		strings.Contains(s, physical.BinaryPrefix) || strings.Contains(s, physical.LabelReplacePrefix)
}

var (
	errBinSetOperator    = &NotLoweredError{Construct: "BinOp set operator"}
	errBinBool           = &NotLoweredError{Construct: "BinOp bool"}
	errBinScalarOperands = &NotLoweredError{Construct: "BinOp scalar operands"}
	errBinOperandWithout = &NotLoweredError{Construct: "BinOp operand without empty"}
	errBinWithoutNest    = &NotLoweredError{Construct: "BinOp operand without empty nest"}
	errBinWithoutUnwrap  = &NotLoweredError{Construct: "BinOp operand without empty unwrap"}
	errBinOperandRange   = &NotLoweredError{Construct: "BinOp operand RangeAgg"}
	errBinOperandVector  = &NotLoweredError{Construct: "BinOp operand VectorAgg"}
	errBinConstants      = &NotLoweredError{Construct: "BinOp constant operands"}
	errBinOperandBinOp   = &NotLoweredError{Construct: "BinOp operand BinOp"}
)

// binOpShape answers the NotLoweredError of a binary operation the lowering does not write, judged on the plan
// alone: the operator, the bool modifier and the kinds of the operands.
func binOpShape(n *queryplan.BinOp) error {
	_, ls := n.L.(*queryplan.Scalar)
	_, rs := n.R.(*queryplan.Scalar)
	lc, rc := constantOperand(n.L), constantOperand(n.R)
	_, lv := n.L.(*queryplan.VectorLit)
	_, rv := n.R.(*queryplan.VectorLit)
	switch {
	case setOperator(n.Op) && (lc || rc):
		// Loki rejects a number operand at parse; vector(n) next to a set operator is left as it was.
		return errBinSetOperator
	case n.Bool && (lv || rv || lc && rc):
		return errBinBool
	}
	if ls && rs {
		return errBinScalarOperands
	}
	if lc && rc {
		return errBinConstants
	}
	for _, x := range [2]queryplan.Node{EmptyWithoutLevelsOff(n.L), EmptyWithoutLevelsOff(n.R)} {
		switch v := x.(type) {
		case *queryplan.Scalar, *queryplan.RangeAgg, *queryplan.VectorLit:
		case *queryplan.VectorAgg:
			if err := emptyWithoutShape(v); err != nil {
				return err
			}
		case *queryplan.BinOp:
			if binOpShape(v) != nil {
				return errBinOperandBinOp
			}
		default:
			return &NotLoweredError{Construct: "BinOp operand " + Construct(x)}
		}
	}
	return nil
}

// setOperator reports and, or and unless.
func setOperator(op string) bool { return translate.BinaryOperatorToken(op) == "" }

// translatorShape is the binary operation the string translator's text for the whole query describes: no bool modifier,
// no set operator, no operand that is a binary operation. The route rule keeps the others' delegations (RouteQuery).
func translatorShape(n *queryplan.BinOp) error {
	switch {
	case setOperator(n.Op):
		return errBinSetOperator
	case n.Bool:
		return errBinBool
	}
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		if _, ok := x.(*queryplan.BinOp); ok {
			return errBinOperandBinOp
		}
	}
	return nil
}

// EmptyWithoutLevelsOff is a binary operand as the proxy's binary route sends it (proxy emptyWithoutOperand): its outer
// sum, avg, min and max levels with a `without ()` clause taken off. Such a clause drops no label, so each series of
// the level's operand is a group of its own, and the level answers that series unchanged with every label it has:
// Loki's answer for the level is its operand's (semantics/binary-operand-without-empty-collapses). A count, stddev,
// stdvar or ranking under the clause is kept (each series answers 1 or 0, or is ranked).
func EmptyWithoutLevelsOff(x queryplan.Node) queryplan.Node {
	for {
		v, ok := x.(*queryplan.VectorAgg)
		if !ok || !keepsEverySeries(v) {
			return x
		}
		x = v.In
	}
}

// keepsEverySeries reports a vector aggregation whose answer is its operand's: sum, avg, min or max with a `without`
// clause of no label.
func keepsEverySeries(v *queryplan.VectorAgg) bool {
	switch v.Op {
	case logql.VectorSum, logql.VectorAvg, logql.VectorMin, logql.VectorMax:
	default:
		return false
	}
	return v.By != nil && v.By.Without && len(v.By.Labels) == 0 && v.In != nil
}

// emptyWithoutShape answers the NotLoweredError of an operand with a `without ()` clause the lowering does not write:
// one in a nest of vector aggregations (the fold carries one level), or over a range aggregation of an unwrapped
// label (that lowering reads the clause the string translator's way). Both are named here, as the operand's own
// error would be renamed. An operand without an empty clause, or a single level over a range aggregation, is nil.
func emptyWithoutShape(n *queryplan.VectorAgg) error {
	empty, levels := false, 0
	var in queryplan.Node = n
	for {
		lv, ok := in.(*queryplan.VectorAgg)
		if !ok {
			break
		}
		empty = empty || lv.By != nil && lv.By.Without && len(lv.By.Labels) == 0
		levels++
		in = lv.In
	}
	switch {
	case !empty:
		return nil
	case levels > 1:
		return errBinWithoutNest
	}
	if ra, ok := in.(*queryplan.RangeAgg); ok && ra.Op != logql.RangeAbsentOverTime && unwrapStage(ra.In) != nil {
		return errBinWithoutUnwrap
	}
	return nil
}

// withoutOperand reports whether an operand of n is one the proxy's binary route evaluates alone and the translator's
// text for the whole query does not describe: a vector aggregation with a without clause at one of its levels, or a
// vector(n), which the route answers from a constant. The label_replace lowering leaves such a binary operation
// under its calls to the translator for the same reason.
func withoutOperand(n *queryplan.BinOp) bool {
	if vectorOperand(n) || translatorShape(n) != nil {
		return true
	}
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		for lv, _ := x.(*queryplan.VectorAgg); lv != nil; lv, _ = lv.In.(*queryplan.VectorAgg) {
			if lv.By != nil && lv.By.Without {
				return true
			}
		}
	}
	return false
}

// EmptyWithoutOperand reports whether root is a binary operation with an operand that has a `without ()` clause at
// one of its vector aggregations. Loki keeps every series of such an operand; the string translator reads the clause as
// none and the handlers serve the operand ungrouped (semantics/binary-operand-without-empty-collapses), so the
// lowering of such an operation is held to Loki's reading, not to the route.
func EmptyWithoutOperand(root queryplan.Node) bool {
	n, ok := root.(*queryplan.BinOp)
	if !ok {
		return false
	}
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		for lv, _ := x.(*queryplan.VectorAgg); lv != nil; lv, _ = lv.In.(*queryplan.VectorAgg) {
			if lv.By != nil && lv.By.Without && len(lv.By.Labels) == 0 {
				return true
			}
		}
	}
	return false
}

// RoutedBinary reports whether root is a binary operation with an operand that has a without clause (at any level
// of its vector aggregations) or is a vector(n): the lowering writes each operand alone, which the string translator's
// text for the whole query does not describe, so it is held to the proxy's binary route
// (proxy.BinaryOperandsPhysical), not to that text (see binOp).
func RoutedBinary(root queryplan.Node) bool {
	n, ok := root.(*queryplan.BinOp)
	return ok && withoutOperand(n)
}

// binOperand lowers one operand binOpShape accepted: a number, a vector(n), or the query of a range or vector
// aggregation.
func binOperand(x queryplan.Node, prof Profile) (physical.Op, error) {
	var op physical.Op
	var err error
	switch v := x.(type) {
	case *queryplan.Scalar:
		return physical.ScalarOp{V: v.V, Text: binNumber(v.V)}, nil
	case *queryplan.VectorLit:
		return physical.VectorOp{V: v.V, Text: vectorText(v.V)}, nil
	case *queryplan.RangeAgg:
		if op, err = rangeAgg(v, prof); err != nil {
			return nil, operandError(err, errBinOperandRange)
		}
	case *queryplan.VectorAgg:
		if op, err = vectorAggKeepAll(v, prof); err != nil {
			return nil, operandError(err, errBinOperandVector)
		}
	case *queryplan.BinOp:
		// A chain: the route evaluates the operand as a binary operation of its own.
		if op, err = binOp(v, prof); err != nil {
			return nil, operandError(err, errBinOperandBinOp)
		}
	}
	return op, nil
}

// operandError is err, named as the construct named when it is a NotLoweredError.
func operandError(err error, named *NotLoweredError) error {
	if _, ok := err.(*NotLoweredError); ok {
		return named
	}
	return err
}

// binNumber is a number operand as the lowering writes it.
func binNumber(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// binMatching is the vector matching of a binary operation as the translator's modifier text decodes to it: nil
// with no modifier; on() is MatchOn with no labels, an empty label list is nil.
func binMatching(m *queryplan.Matching) (*physical.Matching, string) {
	if m == nil || m.Card == "" {
		return nil, ""
	}
	pm := &physical.Matching{}
	if m.Card == "on" {
		pm.MatchOn, pm.On = true, labelsOrNil(m.MatchLabels)
	} else {
		pm.Ignoring = labelsOrNil(m.MatchLabels)
	}
	if m.GroupSide != "" {
		pm.GroupSide = m.GroupSide
		if m.GroupSide == "group_left" {
			pm.GroupLeft = labelsOrNil(m.Include)
		} else {
			pm.GroupRight = labelsOrNil(m.Include)
		}
	}
	var buf [128]byte
	return pm, string(matchingTail(buf[:0], m))
}

// matchingTail appends the modifier text of a vector matching: `@@@<card>:<labels>` and, with a group side,
// `@@@<side>:<labels>`. The text is built on the caller's buffer, so a screen compares it without a copy.
func matchingTail(dst []byte, m *queryplan.Matching) []byte {
	if m == nil || m.Card == "" {
		return dst
	}
	dst = appendMatchingModifier(dst, m.Card, m.MatchLabels)
	if m.GroupSide != "" {
		dst = appendMatchingModifier(dst, m.GroupSide, m.Include)
	}
	return dst
}

func labelsOrNil(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	return labels
}

// appendMatchingModifier appends one modifier of the translator's modifier text: `@@@<keyword>:<labels>`, the
// labels joined with ", ".
func appendMatchingModifier(dst []byte, keyword string, labels []string) []byte {
	dst = append(append(append(dst, physical.MatchingPrefix...), keyword...), ':')
	for i, l := range labels {
		if i > 0 {
			dst = append(dst, ", "...)
		}
		dst = append(dst, l...)
	}
	return dst
}
