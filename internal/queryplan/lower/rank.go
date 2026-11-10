package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// rank lowers topk, bottomk, sort and sort_desc, without a grouping, over a range aggregation that rangeAgg lowers.
//
// The ranking itself writes nothing: the string translator has no ranking text (no marker, no pipe, k is never
// written), so the text of `topk(3, rate(...))` is the text of `rate(...)`, and DecodePhysical reads no ranking
// operator from it. The lowered operator is therefore the operand's own: the query VictoriaLogs is asked, which is
// also the one the proxy asks (it reads the ranking from the LogQL, parseInstantMetricPostAggQuery, before any
// translation, translates the operand alone, and ranks each step of the answer itself, applyTopKToMatrix). A
// physical.RankStep around it could not be compared with the translator's text, so it stays declared and unused;
// nothing serves a Rank from the plan (it is not a flipped construct), which keeps the lowered operator from
// standing in for the ranked answer.
//
// Where the translator's text for the whole ranking is not the operand's, the shape is left to it under a named
// construct:
//   - a by or without clause (Rank grouping): the translator groups the operand's stats by the clause's labels (or
//     appends a `without` marker with no operator), where Loki ranks the operand's series k per group; the proxy
//     does not send that text, so lowering it would write a grouping nobody asked for. A clause with no label
//     (`by ()`) is no grouping and lowers;
//   - an operand that is not a range aggregation (Rank operand VectorAgg, Rank, BinOp, LabelReplace, ...): the
//     translator writes the whole query as a phrase search, which is not a query to reproduce;
//   - an operand rangeAgg leaves out, named Rank operand RangeAgg;
//   - an unwrapped value with no parser (Rank operand unwrap): a ranking is an outer name to the translator, which
//     then groups the unwrapped stats by the stream, where the operand alone groups by nothing; the proxy sends the
//     operand alone. With a parser both groupings are the same, and the text is the operand's;
//   - an operand with a parser whose lowering allocates more than the translator's text for the ranking (Rank operand
//     parser): a label filter after a parser, and rate and bytes_rate over a parser. The operand's own lowering has
//     the same cost, which the ranking adds nothing to; the shape is left to the translator rather than written
//     with more allocations than it makes (TestRankLoweringAllocatesNoMoreThanTheTranslator);
//   - a spelling the translator reads unlike the plan (screenRank).
func rank(n *queryplan.Rank, prof Profile) (physical.Op, error) {
	if g := n.By; g != nil && (g.Without || len(g.Labels) > 0) {
		return nil, errRankGrouping
	}
	ra, ok := n.In.(*queryplan.RangeAgg)
	if !ok {
		return nil, &NotLoweredError{Construct: "Rank operand " + Construct(n.In)}
	}
	if ra.Op != logql.RangeAbsentOverTime && unwrapStage(ra.In) != nil && !hasParser(ra.In) {
		return nil, errRankUnwrap
	}
	op, err := rangeAgg(ra, prof)
	if err != nil {
		return nil, operandError(err, errRankOperandRange)
	}
	if parserCostsMore(ra) {
		return nil, errRankParser
	}
	return op, nil
}

// parserCostsMore reports whether the lowering of the operand allocates more than the translator does for its text
// (measured over the rank grid under the profile with and without stream labels): a label filter after a parser,
// except behind an unwrapped value other than a rate's, and rate and bytes_rate over a parser with none.
func parserCostsMore(ra *queryplan.RangeAgg) bool {
	if !hasParser(ra.In) {
		return false
	}
	unwrap := unwrapStage(ra.In) != nil && ra.Op != logql.RangeAbsentOverTime
	rateLike := ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate
	if unwrap && !rateLike {
		return false
	}
	if !unwrap && rateLike {
		return true
	}
	parsed := false
	for _, st := range ra.In.Stages {
		switch st.(type) {
		case *queryplan.Parse:
			parsed = true
		case *queryplan.LabelFilter:
			if parsed {
				return true
			}
		}
	}
	return false
}

var (
	errRankGrouping      = &NotLoweredError{Construct: "Rank grouping"}
	errRankUnwrap        = &NotLoweredError{Construct: "Rank operand unwrap"}
	errRankParser        = &NotLoweredError{Construct: "Rank operand parser"}
	errRankOperandRange  = &NotLoweredError{Construct: "Rank operand RangeAgg"}
	errRankSpelling      = &NotLoweredError{Construct: "Rank spelling"}
	errRankParenthesized = &NotLoweredError{Construct: "Rank parenthesized"}
)

// screenRank turns away a ranking whose spelling the string translator reads unlike the plan, which holds no text.
// The translator finds the operand by matching the text with regexps that do not skip blanks, as it does for a
// vector aggregation: a blank right after the opening parenthesis of a sort (`sort( rate(...) )`), between the
// operand's function and its parenthesis, or a parenthesized operand makes it write the whole query as a phrase
// search (paddedOperand, and paddedCall for the operand of a topk or bottomk, which follows the comma after k); a
// ranking wrapped whole in parentheses is read as text as well (wrappedWhole). The operand's unwrap stage is read
// out of the text (screenUnwrap). Only an ungrouped ranking over a range aggregation reaches the lowering, so the
// others are left to rank.
func screenRank(query string, n *queryplan.Rank) error {
	ra, ok := n.In.(*queryplan.RangeAgg)
	if !ok || n.By != nil && (n.By.Without || len(n.By.Labels) > 0) {
		return nil
	}
	if n.Op == logql.VectorTopK || n.Op == logql.VectorBottomK {
		// The first comma is the one after k: the clause, when written before the arguments, has no label here.
		if i := strings.IndexByte(query, ','); i >= 0 && paddedCall(query[i+1:]) {
			return errRankSpelling
		}
	}
	if wrappedWhole(query) {
		return errRankParenthesized
	}
	if paddedOperand(query) {
		return errRankSpelling
	}
	if screenUnwrap(query, &queryplan.Plan{Root: ra}) != nil {
		return errRankSpelling
	}
	return nil
}

// paddedCall reports whether the operand text rest (what follows the comma after k) is parenthesized, or has a blank
// between its function name and the call's parenthesis: the shapes the translator reads as text.
func paddedCall(rest string) bool {
	j := 0
	for j < len(rest) && isBlank(rest[j]) {
		j++
	}
	if j < len(rest) && rest[j] == '(' {
		return true
	}
	k := j
	for k < len(rest) && isNameByte(rest[k]) {
		k++
	}
	l := k
	for l < len(rest) && isBlank(rest[l]) {
		l++
	}
	return k > j && l > k && l < len(rest) && rest[l] == '('
}
