package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The drop and keep stages of a metric row. The string translator writes the log query of a metric row with the
// builders of a log query (translateMetricInnerQuery turns the metric flag on) and the flag changes these two:
//   - a drop deletes every bare name it lists, where a log query's keeps the fields detected_level is derived from
//     (the response projection applies a log query's drop later; a metric row has none);
//   - a keep that names detected_level without level also deletes level after its fields: the derived-level chain
//     runs ahead of the pipeline and needs the stored level, which Loki's keep removes before the metric groups the row.

// metricDropPipes are the pipes of a drop stage of a metric row: one delete of every bare name the stage lists. A
// drop of matchers writes none (the proxy applies it to the response), as in a log query; a matcher value the
// translator's splitting reads wrongly stays with it, as there (dropPipes).
func metricDropPipes(st *queryplan.Drop) ([]logsql.Pipe, error) {
	for _, m := range st.Matchers {
		if hasQuote(m.Value) {
			return nil, &NotLoweredError{Construct: "LogSelect drop matcher value"}
		}
	}
	if len(st.Labels) == 0 {
		return nil, nil
	}
	return []logsql.Pipe{logsql.PipeDelete{Labels: st.Labels}}, nil
}

// metricKeepPipes are the pipes of a keep stage of a metric row: a log query's, then the delete of level.
func metricKeepPipes(st *queryplan.Keep, label func(string) string, prof Profile) ([]logsql.Pipe, error) {
	ps, err := keepPipes(st, label, prof)
	if err != nil {
		return nil, err
	}
	var detected, level bool
	note := func(n string) {
		switch n {
		case translate.DetectedLevelLabel:
			detected = true
		case "level":
			level = true
		}
	}
	for _, n := range st.Labels {
		note(n)
	}
	for _, m := range st.Matchers {
		note(m.Name)
	}
	if detected && !level {
		ps = append(ps, logsql.PipeDelete{Labels: []string{"level"}})
	}
	return ps, nil
}

// keepIdentityError answers the construct "<owner> keep identity" when the aggregation reads the series of the rows
// (seriesLevel: a rate, or count, min, max and avg) and the translator takes a trailing keep to decide them
// (trailingKeep), nil otherwise.
func keepIdentityError(sel *queryplan.LogSelect, owner string, seriesLevel bool) error {
	if seriesLevel && trailingKeep(sel) {
		return &NotLoweredError{Construct: owner + " keep identity"}
	}
	return nil
}

// trailingKeep reports whether the string translator takes a keep stage of the log query to decide the labels of every
// sample (trailingKeepGrouping): the last keep is followed by no parser, label_format, drop or unwrap. It then groups
// a rate, and the one value per series of count, min, max and avg, by the labels the keep leaves, where the lowering
// groups by the stream: such an aggregation is left to the translator.
func trailingKeep(sel *queryplan.LogSelect) bool {
	found := false
	for _, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.Keep:
			found = true
		case *queryplan.Parse, *queryplan.LabelFormat, *queryplan.Drop, *queryplan.Unwrap:
			found = false
		}
	}
	return found
}

// anyClientString reports whether any text the client wrote in the log query satisfies f: the values and names of
// the selector, the line filters, the label filters (as written), the parsers' arguments and extraction lists, the
// names and conditions of a drop and a keep, the templates of line_format and the assignments of label_format. The
// string translator reads quoted text of the query as syntax (a parser stage, an unwrap, a without clause, a stats
// pipe), and the printed query may not hold the client's text: a drop of matchers writes no pipe.
func anyClientString(sel *queryplan.LogSelect, f func(string) bool) bool {
	for _, m := range sel.Matchers {
		if f(m.Name) || f(m.Value) {
			return true
		}
	}
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.LineFilter:
			if f(st.Value) {
				return true
			}
			for _, alt := range st.Or {
				if f(alt.Value) {
					return true
				}
			}
		case *queryplan.Parse:
			if f(st.Param) {
				return true
			}
			for _, fld := range st.Fields {
				if f(fld.Name) || f(fld.Expression) {
					return true
				}
			}
		case *queryplan.LabelFilter:
			if f(st.Raw) {
				return true
			}
		case *queryplan.Drop:
			if anyDropString(st.Labels, st.Matchers, f) {
				return true
			}
		case *queryplan.Keep:
			if anyDropString(st.Labels, st.Matchers, f) {
				return true
			}
		case *queryplan.LineFormat:
			if f(st.Template) {
				return true
			}
		case *queryplan.LabelFormat:
			if f(st.Raw) {
				return true
			}
			for _, lf := range st.Formats {
				if f(lf.Name) || f(lf.Value) {
					return true
				}
			}
		case *queryplan.Unwrap:
			if f(st.Label) || f(st.Converter) {
				return true
			}
		}
	}
	return false
}

func anyDropString(labels []string, matchers []logql.DropMatcher, f func(string) bool) bool {
	for _, l := range labels {
		if f(l) {
			return true
		}
	}
	for _, m := range matchers {
		if f(m.Name) || f(m.Value) {
			return true
		}
	}
	return false
}
