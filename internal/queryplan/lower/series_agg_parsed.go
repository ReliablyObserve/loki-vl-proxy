package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// parsedLabelOperand lowers the operand of a stddev or stdvar by labels over a query with a parser, the way the
// proxy's route writes it (postAggOperandText): the labels may be parsed ones, which the operand alone does not keep,
// so the route translates the series-level form of `max by (labels) (operand)` (count, min, max and avg keep the
// series identity next to the clause's labels in their first stats pipe) and cuts the text:
//   - the last stats pipe (the fold across the series, which is the proxy's) is dropped;
//   - with no math pipe after the first stats pipe, the alias of the value is removed (nothing reads it), so the
//     request answers one metric per series, the shape of a bare range aggregation;
//   - after a rate's math pipe (a second metric in the answer), a stats by the first grouping found from the first
//     stats pipe on sums the rate, the shape of a grouped rate.
//
// The lowering builds the max by form as vectorAgg does (a range aggregation, a nest of sum, avg, min, max and
// count over one) and makes the same cut on its typed pipes; the derived-label pipes in front stay the ones the whole
// form needs, as in the text the route cuts. Left to the string translator under a construct name, where the route
// does something else or its text cut does not read the pipes the way the plan holds them:
//   - another operand (an unwrapped value, a label_replace, a ranking, a binary operation): VectorAgg stddev parsed
//     label operand;
//   - a form the route does not cut, or cuts by a grouping its scan reads short (no grouping to sum by after a
//     math pipe, a field name holding a parenthesis): VectorAgg stddev parsed label cut;
//   - a rewrite that would allocate more than the route's (parsedLabelCostsMore): VectorAgg stddev parsed label cost;
//   - a grouping by a name that differs from _time, _stream or _stream_id only by case: VectorAgg stddev parsed
//     label reserved name;
//   - everything the max by form leaves to the translator keeps its name.
func parsedLabelOperand(va *queryplan.VectorAgg, x queryplan.Node, prof Profile) (physical.Op, error) {
	for _, l := range va.By.Labels {
		if reservedFieldCase(l) {
			return nil, errSeriesParsedLabelReserved
		}
	}
	if parsedLabelCostsMore(x) {
		return nil, errSeriesParsedLabelCost
	}
	maxBy := queryplan.VectorAgg{Op: logql.VectorMax, By: va.By, In: x}
	b, err := parsedLabelForm(&maxBy, prof)
	if err != nil {
		return nil, err
	}
	pipes, ok := routeOperandCut(b.stats)
	if !ok {
		return nil, errSeriesParsedLabelCut
	}
	return physical.VLQuery{Text: b.text(prof, pipes)}, nil
}

// parsedLabelForm builds the max by form of a parsed-label operand up to its stats (buildVectorAgg,
// buildNestedVector).
func parsedLabelForm(maxBy *queryplan.VectorAgg, prof Profile) (builtVector, error) {
	switch in := maxBy.In.(type) {
	case *queryplan.RangeAgg:
		if unwrapStage(in.In) != nil {
			if in.Op != logql.RangeAbsentOverTime {
				return builtVector{}, errSeriesParsedLabelOperand
			}
			// absent_over_time reads no unwrapped value, as in vectorAgg.
			stripped, err := withoutUnwrap(in, "VectorAgg")
			if err != nil {
				return builtVector{}, err
			}
			return buildVectorAgg(maxBy, stripped, in.In, prof)
		}
		return buildVectorAgg(maxBy, in, in.In, prof)
	case *queryplan.VectorAgg:
		return buildNestedVector(maxBy, prof)
	}
	return builtVector{}, errSeriesParsedLabelOperand
}

// routeOperandCut is the cut the route makes on the text of the max by form (postAggOperandText), made on its stats
// pipes: false where the route does not cut (fewer than two stats pipes) or falls back to the operand alone (a math
// pipe and no grouping to sum by), and where its scan of the text would read a pipe unlike the typed one (a
// grouping field holding a parenthesis, a math pipe whose expression holds a pipe or whose alias is not a word).
func routeOperandCut(stats []logsql.Pipe) ([]logsql.Pipe, bool) {
	first, last, n := -1, -1, 0
	for i, p := range stats {
		if _, ok := p.(logsql.PipeStats); ok {
			if first < 0 {
				first = i
			}
			last, n = i, n+1
		}
	}
	if n < 2 {
		return nil, false
	}
	cut := make([]logsql.Pipe, last, last+1)
	copy(cut, stats[:last])
	math := ""
	for _, p := range cut[first+1:] {
		if m, ok := p.(logsql.PipeMath); ok {
			if strings.Contains(m.Expr, "|") || !wordName(m.Alias) {
				return nil, false
			}
			math = m.Alias
		}
	}
	if math == "" {
		// The alias of the last value is removed where the text ends with it.
		if st, ok := cut[len(cut)-1].(logsql.PipeStats); ok && len(st.Funcs) > 0 && st.Funcs[len(st.Funcs)-1].Alias == innerValue {
			funcs := append([]logsql.StatsFuncAlias(nil), st.Funcs...)
			funcs[len(funcs)-1].Alias = ""
			st.Funcs = funcs
			cut[len(cut)-1] = st
		}
		return cut, true
	}
	for _, p := range cut[first:] {
		st, ok := p.(logsql.PipeStats)
		if !ok || len(st.By) == 0 {
			continue
		}
		for _, k := range st.By {
			if strings.ContainsAny(k.String(), "()") {
				return nil, false
			}
		}
		return append(cut, logsql.PipeStats{By: st.By, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: math}}}}), true
	}
	return nil, false
}

// parsedLabelCostsMore reports whether the rewrite of x allocates more than the proxy's route for it (measured over
// the grids, TestRankLoweringAllocatesNoMoreThanTheProxyRoute): a sum with no clause over a rate or bytes_rate, whose
// max by form the lowering builds through two levels of rate pipes (67 allocations against 65 and 66 for the route's
// translation and cut). The route is the comparator, so the shape is left to the translator.
func parsedLabelCostsMore(x queryplan.Node) bool {
	va, ok := x.(*queryplan.VectorAgg)
	if !ok || va.Op != logql.VectorSum || va.By != nil {
		return false
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	return ok && (ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate)
}

// reservedFieldCase reports whether a label name differs from _time, _stream or _stream_id only by letter case: a
// query VictoriaLogs accepts with such a field may not survive its own print and re-parse
// (VictoriaMetrics/VictoriaLogs#1942), so the plan writes no new text that holds one.
func reservedFieldCase(name string) bool {
	for _, r := range [...]string{"_time", "_stream", "_stream_id"} {
		if name != r && strings.EqualFold(name, r) {
			return true
		}
	}
	return false
}

// wordName reports whether s is a non-empty run of letters, digits and underscores (the \w+ of the route's scan).
func wordName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

var (
	errSeriesParsedLabelOperand = &NotLoweredError{Construct: "VectorAgg stddev parsed label operand"}
	errSeriesParsedLabelCut     = &NotLoweredError{Construct: "VectorAgg stddev parsed label cut"}
	// errSeriesParsedLabelCost is a rewrite that would allocate more than the route (parsedLabelCostsMore).
	errSeriesParsedLabelCost = &NotLoweredError{Construct: "VectorAgg stddev parsed label cost"}
	// errSeriesParsedLabelReserved is a grouping by a name that differs from a VictoriaLogs reserved field only by
	// case (reservedFieldCase).
	errSeriesParsedLabelReserved = &NotLoweredError{Construct: "VectorAgg stddev parsed label reserved name"}
)
