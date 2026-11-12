package lower

import (
	"slices"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// groupPushed reports whether the handlers answer the sliding windows of va (over the range aggregation rng, an
// unwrapped label) with the extreme taken per bucket and per group of its labels, in VictoriaLogs, instead of per
// series first. The outer aggregation must be the extreme the range function takes (a max of max_over_time, a min of
// min_over_time: the extreme of the extremes is the extreme of the group), by labels or none, with no grouping of the
// range's own. A label that is the level, or that the stats would not keep as the clause names it (a repeat, the
// stream, the line, a name the label function turns into another one the clause already holds), keeps the
// per-series read: the series' level comes from it.
func groupPushed(va *queryplan.VectorAgg, rng *queryplan.RangeAgg, prof Profile) bool {
	if va.In != queryplan.Node(rng) || rng.By != nil || va.By != nil && va.By.Without {
		return false
	}
	switch {
	case va.Op == logql.VectorMax && rng.Op == logql.RangeMaxOverTime, va.Op == logql.VectorMin && rng.Op == logql.RangeMinOverTime:
	default:
		return false
	}
	if va.By == nil {
		return true
	}
	if slices.ContainsFunc(va.By.Labels, func(name string) bool { return name == "level" || name == translate.DetectedLevelLabel }) {
		return false
	}
	var buf [6]string
	kept := translate.AppendNormalizedLabels(buf[:0], va.By.Labels, prof.Label)
	return len(kept) == len(va.By.Labels) && !slices.ContainsFunc(kept, func(name string) bool { return name == "_stream" || name == "_msg" })
}
