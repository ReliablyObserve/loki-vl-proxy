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
// Two spellings are read as the translator reads them (binop_more.go): the bool modifier is dropped (Bool stays
// false, as the legacy encoding writes it), and a without clause of an operand is taken out of the operand and
// written once after the whole binary, as a Fold of the labels of the first clause.
//
// Left to the string translator under a construct name:
//   - and, or and unless (the translator writes the left operand alone);
//   - two numbers, an operand that is a binary operation, vector(), label_replace, a ranking or a log query, a
//     vector aggregation with a without clause that has no label or sits over another aggregation, an operand the
//     range or vector aggregation lowering leaves to the translator;
//   - a spelling the translator reads unlike the plan (screenBinOp, before the lowering).
func binOp(n *queryplan.BinOp, prof Profile) (physical.Op, error) {
	if err := binOpShape(n); err != nil {
		return nil, err
	}
	s, labels := withoutOperands(n)
	l, err := binOperand(s.L, prof)
	if err != nil {
		return nil, operandWithoutError(err, labels)
	}
	r, err := binOperand(s.R, prof)
	if err != nil {
		return nil, operandWithoutError(err, labels)
	}
	b := physical.Binary{Op: n.Op, L: l, R: r}
	b.Match, b.Tail = binMatching(n.Match)
	if len(labels) > 0 {
		return physical.Fold{Levels: []physical.FoldLevel{{Labels: labels, Without: true}}, In: b}, nil
	}
	return b, nil
}

var (
	errBinSetOperator     = &NotLoweredError{Construct: "BinOp set operator"}
	errBinScalarOperands  = &NotLoweredError{Construct: "BinOp scalar operands"}
	errBinOperandWithout  = &NotLoweredError{Construct: "BinOp operand without"}
	errBinOperandRange    = &NotLoweredError{Construct: "BinOp operand RangeAgg"}
	errBinOperandVector   = &NotLoweredError{Construct: "BinOp operand VectorAgg"}
	errBinQuotedText      = &NotLoweredError{Construct: "BinOp quoted text"}
	errBinSpelling        = &NotLoweredError{Construct: "BinOp spelling"}
	errBinOperatorSpell   = &NotLoweredError{Construct: "BinOp operator spelling"}
	errBinMatchingSpell   = &NotLoweredError{Construct: "BinOp vector matching spelling"}
	errBinNumberSpelling  = &NotLoweredError{Construct: "BinOp number spelling"}
	errBinOperandSpelling = &NotLoweredError{Construct: "BinOp operand spelling"}
)

// binOpShape answers the NotLoweredError of a binary operation the lowering does not write, judged on the plan
// alone: the operator and the kinds of the operands.
func binOpShape(n *queryplan.BinOp) error {
	if translate.BinaryOperatorToken(n.Op) == "" {
		return errBinSetOperator
	}
	_, ls := n.L.(*queryplan.Scalar)
	_, rs := n.R.(*queryplan.Scalar)
	if ls && rs {
		return errBinScalarOperands
	}
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		switch v := x.(type) {
		case *queryplan.Scalar, *queryplan.RangeAgg:
		case *queryplan.VectorAgg:
			if v.By != nil && v.By.Without && !withoutLowers(v) {
				return errBinOperandWithout
			}
		default:
			return &NotLoweredError{Construct: "BinOp operand " + Construct(x)}
		}
	}
	return binStagesCost(n)
}

// binOperand lowers one operand binOpShape accepted: a number, or the query of a range or vector aggregation.
func binOperand(x queryplan.Node, prof Profile) (physical.Op, error) {
	var op physical.Op
	var err error
	switch v := x.(type) {
	case *queryplan.Scalar:
		return physical.ScalarOp{V: v.V, Text: binNumber(v.V)}, nil
	case *queryplan.RangeAgg:
		if op, err = rangeAgg(v, prof); err != nil {
			return nil, operandError(err, errBinOperandRange)
		}
	case *queryplan.VectorAgg:
		if op, err = vectorAgg(v, prof); err != nil {
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
// `@@@<side>:<labels>`.
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
//   - two blanks, a bool or a without that is not the plan's modifier or clause (dropBool, dropWithoutClauses; the
//     translator removes the modifier and the clauses from the text first): BinOp spelling;
//   - modifiers spelled unlike the lowering's text, or a group_left or group_right without its parenthesis:
//     BinOp vector matching spelling;
//   - no operator, or more than one, at depth 0, or another one than the plan's: BinOp operator spelling;
//   - a number spelled unlike binNumber: BinOp number spelling; an operand the range and vector aggregation
//     screens turn away (ScreenText): BinOp operand spelling.
//
// A binary operation the lowering does not write (binOpShape) is left to it to name.
func screenBinOp(query string, n *queryplan.BinOp) error {
	if binOpShape(n) != nil {
		return nil
	}
	q := strings.TrimSpace(logql.CanonicalizeLogRanges(query))
	if quotedReadsAsSyntax(q) {
		return errBinQuotedText
	}
	if strings.Contains(q, "  ") {
		return errBinSpelling
	}
	q, err := dropWithoutClauses(q, n)
	if err != nil {
		return err
	}
	q, boolAt, err := dropBool(q, n.Bool)
	if err != nil {
		return err
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
	// The modifier follows the operator: the blank the translator leaves in its place is the operator's own.
	if boolAt >= 0 && boolAt != at+len(tok)-1 {
		return errBinSpelling
	}
	s, _ := withoutOperands(n)
	for i, x := range [2]queryplan.Node{s.L, s.R} {
		text := strings.TrimSpace(stripped[:at])
		if i == 1 {
			text = strings.TrimSpace(stripped[at+len(tok):])
		}
		if sc, ok := x.(*queryplan.Scalar); ok {
			if text != binNumber(sc.V) {
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
