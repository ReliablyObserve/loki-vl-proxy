package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// nestedVectorAgg lowers a vector aggregation over other vector aggregations: two or more levels of sum, avg, min,
// max and count over a range aggregation that vectorAggOver lowers. The string translator writes one query for
// the nest (tryTranslateVectorChain): the innermost level as a query of its own, then one stats pipe per level
// above it over the rows of the one below, the previous stats' value named __lvp_inner:
//
//	stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as
//	__lvp_inner | stats by (zone) min(__lvp_inner)
//
// Each level keeps exactly the grouping labels its input series have (translate.LevelKeeps, Loki's
// VectorAggEvaluator): a `by` list is cut to the labels the level below kept, a `without` list is subtracted from
// them, and a level with no clause keeps none and gets a stats pipe with no grouping. When the innermost level is
// a without, its labels are the series' own, which VictoriaLogs cannot list: the query is the innermost level's
// series rows alone, and the lowering returns the Fold the string translator's marker decodes to, which carries
// the innermost clause's labels and the operator of the level at the top of the nest; the proxy folds every level
// over the rows from the LogQL.
//
// Left to the string translator under a construct name:
//   - a level that is not sum, avg, min, max or count (stddev, stdvar, a ranking; the proxy evaluates them, see
//     evaluated), and an operand that is not a range aggregation (a binary, label_replace, a vector): VectorAgg
//     operand;
//   - a nest over an unwrapped label (VectorAgg nested unwrap), and a without clause with no label at any level
//     (VectorAgg without empty);
//   - everything the innermost level leaves to the translator when it stands alone keeps its name.
func nestedVectorAgg(top *queryplan.VectorAgg, prof Profile) (physical.Op, error) {
	b, err := buildNestedVector(top, prof)
	if err != nil {
		return nil, err
	}
	return b.op(string(top.Op), prof), nil
}

// buildNestedVector is nestedVectorAgg up to its stats: the innermost level's build, with one stats pipe appended for
// each level above it (none under a without fold at the innermost level, which the proxy folds).
func buildNestedVector(top *queryplan.VectorAgg, prof Profile) (builtVector, error) {
	var levels []*queryplan.VectorAgg // outermost first
	var node queryplan.Node = top
	for {
		va, ok := node.(*queryplan.VectorAgg)
		if !ok {
			break
		}
		switch va.Op {
		case logql.VectorSum, logql.VectorCount, logql.VectorMin, logql.VectorMax, logql.VectorAvg:
		default:
			return builtVector{}, &NotLoweredError{Construct: "VectorAgg operand"}
		}
		// The translator reads a without clause by a regexp that needs a label, and strips it from the text.
		if g := va.By; g != nil && g.Without && len(g.Labels) == 0 {
			return builtVector{}, &NotLoweredError{Construct: "VectorAgg without empty"}
		}
		levels = append(levels, va)
		node = va.In
	}
	ra, ok := node.(*queryplan.RangeAgg)
	if !ok {
		return builtVector{}, &NotLoweredError{Construct: "VectorAgg operand"}
	}
	src := ra.In
	if unwrapStage(ra.In) != nil {
		if ra.Op != logql.RangeAbsentOverTime {
			return builtVector{}, &NotLoweredError{Construct: "VectorAgg nested unwrap"}
		}
		// absent_over_time reads no unwrapped value: the aggregation is the same over the query without the stage.
		stripped, err := withoutUnwrap(ra, "VectorAgg")
		if err != nil {
			return builtVector{}, err
		}
		ra = stripped
	}
	innermost := levels[len(levels)-1]
	b, err := buildVectorAgg(innermost, ra, src, prof)
	if err != nil || b.without {
		return b, err
	}
	var kept []string
	if g := innermost.By; g != nil {
		kept = g.Labels
	}
	// The stats of the innermost level are the builder's own; the levels above add one pipe each.
	stats := make([]logsql.Pipe, len(b.stats), len(b.stats)+len(levels)-1)
	copy(stats, b.stats)
	for i := len(levels) - 2; i >= 0; i-- {
		lv := levels[i]
		var labels []string
		without := false
		if g := lv.By; g != nil {
			labels, without = g.Labels, g.Without
		}
		kept = translate.LevelKeeps(kept, labels, without)
		stats = append(nameLastValue(stats), statsPipe(translate.NormalizeByLabels(kept, prof.Label), levelFunc(lv.Op), ""))
	}
	b.stats = stats
	return b, nil
}

// innerValue is the name the stats of one level give their value for the stats of the level above.
const innerValue = "__lvp_inner"

// nameLastValue names the value of the last stats pipe, as the translator does by appending an alias to its text.
func nameLastValue(stats []logsql.Pipe) []logsql.Pipe {
	last := stats[len(stats)-1].(logsql.PipeStats)
	last.Funcs = []logsql.StatsFuncAlias{{Func: last.Funcs[0].Func, Alias: innerValue}}
	stats[len(stats)-1] = last
	return stats
}

// levelFunc is the stats function of a level above the first: it aggregates the values the level below named.
func levelFunc(op logql.VectorOp) logsql.StatsFunc {
	switch op {
	case logql.VectorSum:
		return logsql.Sum{Field: innerValue}
	case logql.VectorMin:
		return logsql.Min{Field: innerValue}
	case logql.VectorMax:
		return logsql.Max{Field: innerValue}
	case logql.VectorAvg:
		return logsql.Avg{Field: innerValue}
	}
	return logsql.Count{}
}
