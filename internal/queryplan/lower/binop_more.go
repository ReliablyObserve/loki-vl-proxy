package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The two spellings of a binary operation the string translator reads from the text of the whole query, before it
// splits the query at the operator:
//
//   - a bool modifier: `\s+bool\s+` is replaced by one blank, so the operation is written as it is without bool.
//     The lowering writes the same Binary, with Bool false as the legacy encoding does (physical.Canon compares
//     Bool); the proxy evaluates a binary operation from the LogQL and answers 1 or 0 for a comparison;
//   - a without clause of an operand: `\bwithout\s*\(([^)]+)\)` is removed everywhere in the text, the labels of the
//     first match are kept, and the marker `__without__:<labels>` is appended to the whole binary. Each operand is
//     then translated as the aggregation with no grouping at all, so the lowering writes the operand without its
//     clause and wraps the binary in the Fold of those labels (no folding operator: the translator names none for
//     a binary). The fold applies to the binary where Loki folds the operand: recorded, not Loki's. A clause with
//     no label (`without ()` is not removed, `without ( )` is removed and leaves no marker) and one over another
//     aggregation are left to the translator.

var (
	errBinBoolStages    = &NotLoweredError{Construct: "BinOp bool stages"}
	errBinWithoutStages = &NotLoweredError{Construct: "BinOp operand without stages"}
)

// binStagesCost names the construct of a bool modifier or a without clause over an operand whose lowering
// allocates more than the translator does for its text. Measured over binOpMoreAllocGrid (with and without stream
// labels), the lowering writes an operand for them at no more than the translator's cost only when its log query
// holds line filters, an unwrap stage and bare json or logfmt parsers (no extraction list, no flags): a label
// filter, drop, keep, line_format, label_format, decolorize, regexp or pattern stage, and a rate or bytes_rate
// over a parser with no unwrapped value, cost more, and the shapes are left to the translator (a ranking over the
// same operand is, parserCostsMore).
func binStagesCost(n *queryplan.BinOp) error {
	_, withoutClauses := plannedWithout(n)
	if !n.Bool && withoutClauses == 0 {
		return nil
	}
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		var ra *queryplan.RangeAgg
		switch v := x.(type) {
		case *queryplan.RangeAgg:
			ra = v
		case *queryplan.VectorAgg:
			ra, _ = v.In.(*queryplan.RangeAgg)
		}
		if ra == nil || cheapStages(ra) {
			continue
		}
		if withoutClauses > 0 {
			return errBinWithoutStages
		}
		return errBinBoolStages
	}
	return nil
}

// cheapStages reports whether the log query of the range aggregation holds only line filters, an unwrap stage and
// bare json or logfmt parsers, and is not a rate over a parser with no unwrapped value.
func cheapStages(ra *queryplan.RangeAgg) bool {
	parsed := false
	for _, st := range ra.In.Stages {
		switch v := st.(type) {
		case *queryplan.LineFilter, *queryplan.Unwrap:
		case *queryplan.Parse:
			if v.Type != logql.ParserJSON && v.Type != logql.ParserLogfmt || v.Strict || v.KeepEmpty || len(v.Fields) > 0 || v.Param != "" {
				return false
			}
			parsed = true
		default:
			return false
		}
	}
	return !parsed || unwrapStage(ra.In) != nil || (ra.Op != logql.RangeRate && ra.Op != logql.RangeBytesRate)
}

// withoutLowers reports whether the without clause of a vector aggregation operand is one the lowering writes: it
// has a label, and the aggregation reads a range aggregation.
func withoutLowers(va *queryplan.VectorAgg) bool {
	_, ok := va.In.(*queryplan.RangeAgg)
	return ok && len(va.By.Labels) > 0
}

// withoutOperands is n with the without clause of each vector aggregation operand taken out of it, and the labels
// of the first clause (left operand first, the order of the text). It is n and nil when no operand has one. Call it
// on a binary operation binOpShape accepted.
func withoutOperands(n *queryplan.BinOp) (*queryplan.BinOp, []string) {
	lv, lok := n.L.(*queryplan.VectorAgg)
	rv, rok := n.R.(*queryplan.VectorAgg)
	lok = lok && lv.By != nil && lv.By.Without
	rok = rok && rv.By != nil && rv.By.Without
	if !lok && !rok {
		return n, nil
	}
	// One allocation holds the copies.
	c := &struct {
		n    queryplan.BinOp
		l, r queryplan.VectorAgg
	}{n: *n}
	var labels []string
	if lok {
		c.l, c.l.By, c.n.L = *lv, nil, &c.l
		labels = lv.By.Labels
	}
	if rok {
		c.r, c.r.By, c.n.R = *rv, nil, &c.r
		if labels == nil {
			labels = rv.By.Labels
		}
	}
	return &c.n, labels
}

// operandWithoutError is err, or the without construct when an operand carried a clause (the rows keep the name
// they had while the clause was left to the translator).
func operandWithoutError(err error, labels []string) error {
	if _, ok := err.(*NotLoweredError); ok && len(labels) > 0 {
		return errBinOperandWithout
	}
	return err
}

// plannedWithout are the labels of each without clause of the operands, in the order of the text, and how many.
func plannedWithout(n *queryplan.BinOp) (want [2][]string, count int) {
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		if va, ok := x.(*queryplan.VectorAgg); ok && va.By != nil && va.By.Without {
			want[count] = va.By.Labels
			count++
		}
	}
	return want, count
}

// dropWithoutClauses is q as the translator leaves it after extractWithoutLabels: every
// `\bwithout\s*\(([^)]+)\)` removed and the runs of blanks folded into one. It answers errBinSpelling unless the
// text holds exactly the plan's clauses, each with the plan's labels read as the translator reads them (split at
// commas, trimmed, the empty ones dropped): a clause the plan does not hold, one it holds that the regexp does not
// match (`without ()`, a letter in front of the word), or a different label list is a spelling the plan does not
// hold. The word anywhere else (a label, a name) is no clause to the translator either, and stays in the text.
func dropWithoutClauses(q string, n *queryplan.BinOp) (string, error) {
	want, wants := plannedWithout(n)
	if wants == 0 && !strings.Contains(q, "without") {
		return q, nil
	}
	var out strings.Builder
	out.Grow(len(q))
	last, got := 0, 0
	for i := 0; ; {
		at := strings.Index(q[i:], "without")
		if at < 0 {
			break
		}
		at += i
		i = at + len("without")
		if at > 0 && isWordByte(q[at-1]) {
			continue
		}
		j := i
		for j < len(q) && isRegexpSpace(q[j]) {
			j++
		}
		if j >= len(q) || q[j] != '(' {
			continue
		}
		closeAt := strings.IndexByte(q[j+1:], ')')
		if closeAt < 1 {
			continue
		}
		if got >= wants || !sameLabelList(q[j+1:j+1+closeAt], want[got]) {
			return "", errBinSpelling
		}
		writeFolded(&out, q[last:at])
		last = j + 1 + closeAt + 1
		i = last
		got++
	}
	if got != wants {
		return "", errBinSpelling
	}
	writeFolded(&out, q[last:])
	return out.String(), nil
}

// writeFolded writes s with each run of blanks (spaces) folded into one, as the translator folds the text it left
// (the fold reaches across the join with what was written before).
func writeFolded(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' && b.Len() > 0 && lastByte(b) == ' ' {
			continue
		}
		b.WriteByte(s[i])
	}
}

// lastByte is the last byte written to b, which is not empty.
func lastByte(b *strings.Builder) byte {
	s := b.String()
	return s[len(s)-1]
}

// sameLabelList reports whether the text of a clause's labels reads, as the translator reads it (split at commas,
// each trimmed, the empty ones dropped), as the labels.
func sameLabelList(text string, labels []string) bool {
	k := 0
	for text != "" {
		part := text
		if c := strings.IndexByte(text, ','); c >= 0 {
			part, text = text[:c], text[c+1:]
		} else {
			text = ""
		}
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		if k >= len(labels) || labels[k] != part {
			return false
		}
		k++
	}
	return k == len(labels)
}

// dropBool is q as the translator leaves it after `\s+bool\s+` is replaced by one blank, and the offset of that
// blank. The plan has a bool modifier exactly when the text has one `bool` with blanks on each side (the modifier
// after the operator, which the caller checks by the offset); any other count of the word, in a label for one, is a
// spelling the plan does not hold. A text without the modifier is returned as it is, with offset -1.
func dropBool(q string, bool bool) (string, int, error) {
	n := strings.Count(q, "bool")
	if !bool {
		if n > 0 {
			return "", 0, errBinSpelling
		}
		return q, -1, nil
	}
	if n != 1 {
		return "", 0, errBinSpelling
	}
	b := strings.Index(q, "bool")
	s, e := b, b+len("bool")
	for s > 0 && isRegexpSpace(q[s-1]) {
		s--
	}
	for e < len(q) && isRegexpSpace(q[e]) {
		e++
	}
	if s == b || e == b+len("bool") {
		return "", 0, errBinSpelling
	}
	return q[:s] + " " + q[e:], s, nil
}
