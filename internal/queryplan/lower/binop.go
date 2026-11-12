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
// Left to the string translator under a construct name:
//   - and, or and unless (the translator writes the left operand alone), and a bool modifier (it drops it);
//   - two numbers, or numbers and vector(n) only (BinOp constant operands: the proxy folds an instant query of
//     constants itself, before any binary route, and the route's answer for a range query is a constant, which costs
//     less than the plan), an operand that is a binary
//     operation, label_replace, a ranking or a log query, an operand the range or vector aggregation lowering leaves
//     to the translator, and a `without ()` operand over an unwrapped label (BinOp operand without empty unwrap) or
//     in a nest of vector aggregations (BinOp operand without empty nest: a fold carries one level, and the levels
//     above an empty clause are not in it, `count without ()` turns every series into 1);
//   - a spelling the translator reads unlike the plan (screenBinOp, before the lowering).
func binOp(n *queryplan.BinOp, prof Profile) (physical.Op, error) {
	if err := binOpShape(n); err != nil {
		return nil, err
	}
	l, err := binOperand(n.L, prof)
	if err != nil {
		return nil, err
	}
	r, err := binOperand(n.R, prof)
	if err != nil {
		return nil, err
	}
	b := physical.Binary{Op: n.Op, L: l, R: r}
	b.Match, b.Tail = binMatching(n.Match)
	return b, nil
}

var (
	errBinSetOperator     = &NotLoweredError{Construct: "BinOp set operator"}
	errBinBool            = &NotLoweredError{Construct: "BinOp bool"}
	errBinScalarOperands  = &NotLoweredError{Construct: "BinOp scalar operands"}
	errBinOperandWithout  = &NotLoweredError{Construct: "BinOp operand without empty"}
	errBinWithoutNest     = &NotLoweredError{Construct: "BinOp operand without empty nest"}
	errBinWithoutUnwrap   = &NotLoweredError{Construct: "BinOp operand without empty unwrap"}
	errBinOperandRange    = &NotLoweredError{Construct: "BinOp operand RangeAgg"}
	errBinOperandVector   = &NotLoweredError{Construct: "BinOp operand VectorAgg"}
	errBinQuotedText      = &NotLoweredError{Construct: "BinOp quoted text"}
	errBinSpelling        = &NotLoweredError{Construct: "BinOp spelling"}
	errBinOperatorSpell   = &NotLoweredError{Construct: "BinOp operator spelling"}
	errBinMatchingSpell   = &NotLoweredError{Construct: "BinOp vector matching spelling"}
	errBinNumberSpelling  = &NotLoweredError{Construct: "BinOp number spelling"}
	errBinOperandSpelling = &NotLoweredError{Construct: "BinOp operand spelling"}
	errBinConstants       = &NotLoweredError{Construct: "BinOp constant operands"}
)

// binOpShape answers the NotLoweredError of a binary operation the lowering does not write, judged on the plan
// alone: the operator, the bool modifier and the kinds of the operands.
func binOpShape(n *queryplan.BinOp) error {
	if translate.BinaryOperatorToken(n.Op) == "" {
		return errBinSetOperator
	}
	if n.Bool {
		return errBinBool
	}
	_, ls := n.L.(*queryplan.Scalar)
	_, rs := n.R.(*queryplan.Scalar)
	if ls && rs {
		return errBinScalarOperands
	}
	if constantOperand(n.L) && constantOperand(n.R) {
		return errBinConstants
	}
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		switch v := x.(type) {
		case *queryplan.Scalar, *queryplan.RangeAgg, *queryplan.VectorLit:
		case *queryplan.VectorAgg:
			if err := emptyWithoutShape(v); err != nil {
				return err
			}
		default:
			return &NotLoweredError{Construct: "BinOp operand " + Construct(x)}
		}
	}
	return nil
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
	if vectorOperand(n) {
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

// screenBinOp turns away a binary operation whose text the string translator reads unlike the plan. The
// translator reads the whole text with regexps and scans that see no quoted strings: it removes every ` bool `
// and every vector matching modifier (` on(...)`, ` ignoring(...)`, ` group_left(...)`, ` group_right(...)`) and
// folds every run of two blanks, then splits at the first operator at parenthesis depth 0 that has a blank on
// either side and sends each side, as written, to its metric translation (a number as written). So the screen
// leaves to the translator:
//   - a quoted string holding a parenthesis, bool, two blanks or a marker the translator writes (`|||`, `@@@`,
//     `__binary__`, `__lvp_lr:`, `without`): BinOp quoted text;
//   - bool, without or two blanks anywhere else: BinOp spelling;
//   - modifiers spelled unlike the lowering's text, or a group_left or group_right without its parenthesis:
//     BinOp vector matching spelling;
//   - no operator, or more than one, at depth 0, or another one than the plan's: BinOp operator spelling;
//   - a number spelled unlike binNumber: BinOp number spelling; an operand the range and vector aggregation
//     screens turn away (ScreenText): BinOp operand spelling.
//
// A binary operation the lowering does not write (binOpShape) is left to it to name, and one with an operand that
// has a without clause or is a vector(n) is not screened for the translator's reading (RoutedBinary: the proxy's
// route does not read the query's text); a vector(n) operand's own spelling is (screenVectorOperands).
func screenBinOp(query string, n *queryplan.BinOp) error {
	if binOpShape(n) != nil {
		return nil
	}
	if vectorOperand(n) {
		// The route answers a vector(n) from the constant only when it reads the operand's text as one.
		if err := screenVectorOperands(query, n); err != nil {
			return err
		}
	}
	if withoutOperand(n) {
		// The proxy's binary route translates each operand alone, as its printed LogQL (logql.Expr.String), which
		// holds none of the spellings the screens read (the client's blanks, parentheses and modifier spelling,
		// and its quoted text is never read with the whole query): nothing to screen. The corpus shadow holds the
		// lowering to that route (proxy.BinaryOperandsPhysical).
		return nil
	}
	q := strings.TrimSpace(logql.CanonicalizeLogRanges(query))
	if quotedReadsAsSyntax(q) {
		return errBinQuotedText
	}
	if strings.Contains(q, "bool") || strings.Contains(q, "without") || strings.Contains(q, "  ") {
		return errBinSpelling
	}
	stripped, tail := stripMatching(q)
	var buf [128]byte
	if tail != string(matchingTail(buf[:0], n.Match)) {
		return errBinMatchingSpell
	}
	at, tok := topLevelOperator(stripped)
	if at < 0 || tok != translate.BinaryOperatorToken(n.Op) {
		return errBinOperatorSpell
	}
	for i, x := range [2]queryplan.Node{n.L, n.R} {
		text := strings.TrimSpace(stripped[:at])
		if i == 1 {
			text = strings.TrimSpace(stripped[at+len(tok):])
		}
		if s, ok := x.(*queryplan.Scalar); ok {
			if text != binNumber(s.V) {
				return errBinNumberSpelling
			}
			continue
		}
		if ScreenText(text, &queryplan.Plan{Root: x}) != nil {
			return errBinOperandSpelling
		}
	}
	return nil
}

// quotedReadsAsSyntax reports whether a quoted string of q holds text the translator's scans of the whole query
// read as syntax.
func quotedReadsAsSyntax(q string) bool {
	for i := 0; i < len(q); i++ {
		if c := q[i]; c != '"' && c != '`' {
			continue
		}
		end := skipQuoted(q, i)
		s := q[i+1 : min(end, len(q))]
		if strings.ContainsAny(s, "()") || strings.Contains(s, "  ") {
			return true
		}
		for _, w := range [...]string{"bool", "|||", physical.MatchingPrefix, physical.BinaryPrefix, physical.LabelReplacePrefix, "without"} {
			if strings.Contains(s, w) {
				return true
			}
		}
		i = end
	}
	return false
}

// stripMatching removes the vector matching modifiers from q as the translator's regexp does
// (`\s+(on|ignoring|group_left|group_right)\s*\(([^)]*)\)`, every match, left to right) and returns the text left
// and the modifier text the translator writes for them.
func stripMatching(q string) (stripped, tail string) {
	var out, t strings.Builder
	last := 0
	for i := 0; i < len(q); i++ {
		if !isRegexpSpace(q[i]) || (i > 0 && isRegexpSpace(q[i-1])) {
			continue
		}
		j := i
		for j < len(q) && isRegexpSpace(q[j]) {
			j++
		}
		kw := matchingKeyword(q[j:])
		if kw == "" {
			continue
		}
		k := j + len(kw)
		for k < len(q) && isRegexpSpace(q[k]) {
			k++
		}
		if k >= len(q) || q[k] != '(' {
			continue
		}
		closeAt := strings.IndexByte(q[k:], ')')
		if closeAt < 0 {
			break
		}
		closeAt += k
		if out.Len() == 0 {
			out.Grow(len(q))
		}
		if t.Len() == 0 {
			t.Grow(64)
		}
		out.WriteString(q[last:i])
		t.WriteString(physical.MatchingPrefix)
		t.WriteString(kw)
		t.WriteByte(':')
		t.WriteString(strings.TrimSpace(q[k+1 : closeAt]))
		last = closeAt + 1
		i = closeAt
	}
	if t.Len() == 0 {
		return q, ""
	}
	out.WriteString(q[last:])
	return out.String(), t.String()
}

// matchingKeyword is the modifier keyword s starts with, "" for none.
func matchingKeyword(s string) string {
	for _, kw := range [...]string{"on", "ignoring", "group_left", "group_right"} {
		if strings.HasPrefix(s, kw) {
			return kw
		}
	}
	return ""
}

// topLevelOperator is the offset and token of the one operator token (translate.BinaryOperatorTokens) at
// parenthesis depth 0 in q, counted as the translator counts it (every parenthesis, quoted or not), or -1 when
// there is none or more than one.
func topLevelOperator(q string) (int, string) {
	at, tok, depth := -1, "", 0
	for i := 0; i < len(q); i++ {
		switch q[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ' ':
			if depth != 0 {
				continue
			}
			for _, t := range translate.BinaryOperatorTokens {
				if strings.HasPrefix(q[i:], t) {
					if at >= 0 {
						return -1, ""
					}
					at, tok = i, t
					break
				}
			}
		}
	}
	return at, tok
}
