package lower

import (
	"slices"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// WithoutFold is how the handlers fold the series of a `without` clause. VictoriaLogs cannot name "every label but
// these", so a query that holds one reads the series of the stream (and its level) and folds them by the labels left:
// in the proxy, or, where the windows are VictoriaLogs' own buckets and every step is exact, in VictoriaLogs.
type WithoutFold uint8

const (
	// FoldNone is a query with no `without` clause.
	FoldNone WithoutFold = iota
	// FoldRows is the proxy folding the series rows VictoriaLogs answers: an instant request, a window the proxy
	// builds from several buckets, and a query whose labels or levels the pushdown does not take.
	FoldRows
	// FoldPushed is VictoriaLogs folding the series of a range equal to the step: the excluded labels are cut out
	// of the stream text between the stats pipe that counts a series and the pipe that combines the series that now
	// share their labels, one pipe more for each level above the `without`.
	FoldPushed
	// FoldCut is the cut alone, on the buckets of a sliding window: the stream's count or bytes per bucket, the
	// excluded labels cut out of its stream text, and their sum per remaining stream. It takes a single `sum
	// without` over count_over_time or bytes_over_time.
	FoldCut
)

// routeFold is how the handlers fold the `without` of the vector aggregations va over rng for the windows w. b is
// the log query of va's lowering: the text the pushdown refuses to rewrite is read from it.
func routeFold(va *queryplan.VectorAgg, rng *queryplan.RangeAgg, w Window, log metricLog, prof Profile) WithoutFold {
	var buf [6]*queryplan.VectorAgg
	levels := buf[:0]
	for n := queryplan.Node(va); ; {
		v, ok := n.(*queryplan.VectorAgg)
		if !ok {
			break
		}
		levels = append(levels, v)
		n = v.In
	}
	hasWithout := false
	for _, lv := range levels {
		hasWithout = hasWithout || lv.By != nil && lv.By.Without
	}
	switch {
	case !hasWithout:
		return FoldNone
	case w == WindowTumbling && pushable(levels, rng, log, prof):
		return FoldPushed
	case (w == WindowBuckets || w == WindowPhase) && len(levels) == 1 && va.Op == logql.VectorSum && cuttable(va, rng, prof):
		return FoldCut
	}
	return FoldRows
}

// pushable reports whether VictoriaLogs can fold the series of levels (outermost first) by cutting the excluded
// labels out of the stream text: the innermost level is a `without` of labels the cut can name, every level above
// groups `by` columns of the stats pipes, the function reads no unwrapped value, and the query holds no text the
// rewrite would misread: the stats pipes are rewritten between the series' own, and a line filter or a value that
// names _msg or an unpack pipe reads as a parser.
func pushable(levels []*queryplan.VectorAgg, rng *queryplan.RangeAgg, log metricLog, prof Profile) bool {
	// The rewrite reads the series of a count, a byte sum or a rate (two stats pipes over the stream and its level);
	// the stats of an unwrapped value, behind the gate that converts it, are not that shape.
	if unwrapStage(rng.In) != nil || !withoutCut(levels[len(levels)-1], prof) {
		return false
	}
	for _, lv := range levels[:len(levels)-1] {
		if lv.By != nil && (lv.By.Without || slices.ContainsFunc(lv.By.Labels, func(name string) bool { return !groupLabel(name) })) {
			return false
		}
	}
	return !textHas(log, "_msg") && !textHas(log, "unpack_")
}

// cuttable reports whether the buckets of a sliding window can be read with the labels cut: a single `sum without`
// over a count or a byte sum, whose query holds line filters only (the buckets keep the stream and its level).
func cuttable(va *queryplan.VectorAgg, rng *queryplan.RangeAgg, prof Profile) bool {
	if rng.Op != logql.RangeCountOverTime && rng.Op != logql.RangeBytesOverTime || !withoutCut(va, prof) {
		return false
	}
	for _, st := range rng.In.Stages {
		if _, ok := st.(*queryplan.LineFilter); !ok {
			return false
		}
	}
	return true
}

// withoutCut reports whether lv is a `without` of labels whose pairs the stream text holds under their own names.
func withoutCut(lv *queryplan.VectorAgg, prof Profile) bool {
	if lv.By == nil || !lv.By.Without || len(lv.By.Labels) == 0 {
		return false
	}
	return !slices.ContainsFunc(lv.By.Labels, func(name string) bool { return !cutLabel(name, prof) })
}

// cutLabel reports whether cutting name out of the stream text is the same as dropping the label from the series:
// letters and digits only (no label style maps a second stream key to such a name, and it needs no escaping), no label
// the proxy derives from others (level, detected_level, service_name and its sources), and one no label translation
// renames.
func cutLabel(name string, prof Profile) bool {
	if name == "" || name[0] >= '0' && name[0] <= '9' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	switch name {
	case "level", "detected_level", "service_name", "_stream", "_msg", "_time":
		return false
	}
	if slices.Contains(translate.SyntheticServiceNameFields, name) {
		return false
	}
	return prof.Label == nil || prof.Label(name) == name
}

// groupLabel reports whether a level above the `without` can group by name: a column of the stats pipes, so any name
// the proxy does not derive per row.
func groupLabel(name string) bool {
	switch name {
	case "level", "detected_level", "service_name", "_stream", "_msg", "_time", "":
		return false
	}
	for i := 0; i < len(name); i++ {
		switch name[i] {
		case ' ', '\t', '\n', '"', '\'', '`', '(', ')', ',', '|':
			return false
		}
	}
	return true
}
