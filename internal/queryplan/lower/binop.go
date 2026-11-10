package lower

import (
	"math"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// binOp lowers a binary operation between two operands the lowering writes natively: a range aggregation rangeAgg
// lowers, a vector aggregation vectorAgg lowers with no without clause, or a number (not two numbers). The string
// translator writes the operands as two metric queries in one text, `__binary__:<op>:<left>|||<right>`, with the
// vector matching modifiers after it (`@@@on:app@@@group_left:pod`), and the proxy evaluates the operation over
// their answers; the lowering returns the physical.Binary that text decodes to. Each operand is the query the
// lowering writes for it alone (the translator derives service_name and detected_level per operand), a number is
// written as Go prints it, and the labels of a modifier are joined with ", ".
//
// Left to the string translator under a construct name:
//   - and, or and unless (the translator writes the left operand alone), and a bool modifier (it drops it);
//   - two numbers, an operand that is a binary operation, vector(), label_replace, a ranking or a log query, a
//     vector aggregation with a without clause (the translator folds the whole binary by it), an operand the
//     range or vector aggregation lowering leaves to the translator, and a number that is not finite;
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
	errBinNumber          = &NotLoweredError{Construct: "BinOp number"}
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
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		switch v := x.(type) {
		case *queryplan.Scalar:
			if math.IsNaN(v.V) || math.IsInf(v.V, 0) {
				return errBinNumber
			}
		case *queryplan.RangeAgg:
		case *queryplan.VectorAgg:
			if v.By != nil && v.By.Without {
				return errBinOperandWithout
			}
		default:
			return &NotLoweredError{Construct: "BinOp operand " + Construct(x)}
		}
	}
	return nil
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
	tail := appendMatchingModifier(nil, m.Card, m.MatchLabels)
	if m.Card == "on" {
		pm.MatchOn, pm.On = true, labelsOrNil(m.MatchLabels)
	} else {
		pm.Ignoring = labelsOrNil(m.MatchLabels)
	}
	if m.GroupSide != "" {
		tail = appendMatchingModifier(tail, m.GroupSide, m.Include)
		pm.GroupSide = m.GroupSide
		if m.GroupSide == "group_left" {
			pm.GroupLeft = labelsOrNil(m.Include)
		} else {
			pm.GroupRight = labelsOrNil(m.Include)
		}
	}
	return pm, string(tail)
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
// A binary operation the lowering does not write (binOpShape) is left to it to name.
func screenBinOp(query string, n *queryplan.BinOp) error {
	if binOpShape(n) != nil {
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
	_, want := binMatching(n.Match)
	if tail != want {
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
	var out, t []byte
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
		out = append(out, q[last:i]...)
		t = append(append(append(append(t, physical.MatchingPrefix...), kw...), ':'), strings.TrimSpace(q[k+1:closeAt])...)
		last = closeAt + 1
		i = closeAt
	}
	if t == nil {
		return q, ""
	}
	return string(append(out, q[last:]...)), string(t)
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

// isRegexpSpace is \s of Go's regexp syntax.
func isRegexpSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r'
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
