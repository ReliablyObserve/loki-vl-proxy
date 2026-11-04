package proxy

import (
	"net/http"
	"slices"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// keepSeriesGrouping returns query with every range aggregation over a
// pipeline that ends in a bare | keep grouped by the labels that keep leaves,
// and whether it changed anything.
//
// Loki's keep stage deletes every label it does not name, stream labels and
// structured metadata included (pkg/logql/log/keep_labels.go:15-45), so the
// samples of every stream that keeps the same labels fall into one series
// (pkg/logql/range_vector.go groups samples by their labels after the
// pipeline). `rate({app="api"} | json | keep foo [5m])` is one series for the
// whole selector, and `count(...)` over it is 1. The series identity the
// proxy and VictoriaLogs build is the stream, so the query is rewritten to the
// same answer in a form they group by: count_over_time, rate,
// bytes_over_time and bytes_rate sum their samples per series, so
// `f(X | keep a, b [r])` is `sum by (a, b) (f(X | keep a, b [r]))`.
//
// Only a keep with no matcher, after which no stage adds or removes a label
// (a parser, label_format, drop, unwrap or another conditional keep), is
// rewritten; the pipeline itself is left as it is, so the labels a keep
// removed stay removed for every other stage.
func keepSeriesGrouping(query string) (string, bool) {
	if !strings.Contains(query, "keep") {
		return query, false
	}
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return query, false
	}
	out, changed := keepSeriesGroupingExpr(expr)
	if !changed {
		return query, false
	}
	return out.String(), true
}

func keepSeriesGroupingExpr(expr logqlpkg.Expr) (logqlpkg.Expr, bool) {
	switch e := expr.(type) {
	case *logqlpkg.BinOpExpr:
		left, lc := keepSeriesGroupingExpr(e.Left)
		right, rc := keepSeriesGroupingExpr(e.Right)
		if !lc && !rc {
			return expr, false
		}
		out := *e
		out.Left, out.Right = left, right
		return &out, true
	case *logqlpkg.VectorAggregation:
		if keepGrouped(e) {
			return expr, false // rewritten already (a child request of a rewritten one)
		}
		inner, changed := keepSeriesGroupingExpr(e.Inner)
		if !changed {
			return expr, false
		}
		out := *e
		out.Inner = inner
		return &out, true
	case *logqlpkg.RangeAggregation:
		return keepSeriesGroupingRange(e)
	}
	return expr, false
}

func keepSeriesGroupingRange(ra *logqlpkg.RangeAggregation) (logqlpkg.Expr, bool) {
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok || ra.Grouping != nil {
		return ra, false
	}
	switch ra.Op {
	case logqlpkg.RangeCountOverTime, logqlpkg.RangeRate, logqlpkg.RangeBytesOverTime, logqlpkg.RangeBytesRate:
	default:
		return ra, false
	}
	kept, ok := trailingKeepLabels(lq.Pipeline)
	if !ok {
		return ra, false
	}
	return &logqlpkg.VectorAggregation{Op: logqlpkg.VectorSum, Grouping: &logqlpkg.Grouping{Labels: kept}, Inner: ra}, true
}

// keepGrouped reports whether v is the sum by (kept labels) keepSeriesGrouping
// wraps a range aggregation in.
func keepGrouped(v *logqlpkg.VectorAggregation) bool {
	ra, ok := v.Inner.(*logqlpkg.RangeAggregation)
	if !ok || v.Op != logqlpkg.VectorSum || v.Grouping == nil || v.Grouping.Without || v.HasParam {
		return false
	}
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok || ra.Grouping != nil {
		return false
	}
	kept, ok := trailingKeepLabels(lq.Pipeline)
	return ok && slices.Equal(kept, v.Grouping.Labels)
}

// trailingKeepLabels returns the labels the last | keep of a pipeline names
// (Loki's special error labels left out: keep never removes them, and a
// metric whose series carries one is an error) and whether that keep decides
// the series labels: it has no matcher, and every stage after it leaves the
// labels alone. A pipeline with an unwrap is left out: its series also drop
// the unwrapped label.
func trailingKeepLabels(pipeline []logqlpkg.Stage) (kept []string, ok bool) {
	last := -1
	for i, stage := range pipeline {
		if k, isKeep := stage.(*logqlpkg.KeepStage); isKeep {
			if len(k.Matchers) > 0 {
				return nil, false
			}
			last = i
		}
	}
	if last < 0 {
		return nil, false
	}
	for _, stage := range pipeline[last+1:] {
		switch stage.(type) {
		case *logqlpkg.LineFilterStage, *logqlpkg.LabelFilterStage, *logqlpkg.LineFormatStage, *logqlpkg.DecolorizeStage:
		default:
			return nil, false
		}
	}
	for _, l := range pipeline[last].(*logqlpkg.KeepStage).Labels {
		switch l {
		case errorLabel, errorDetailsLabel, "__preserve_error__":
			continue
		}
		kept = append(kept, l)
	}
	return kept, true
}

// applyKeepSeriesGrouping rewrites the query of a metric request with
// keepSeriesGrouping, in every place a handler reads it from. It runs after
// validateQuery: a query reaching it holds concrete range durations (Grafana
// resolves [$__auto] before it sends the query).
func applyKeepSeriesGrouping(r *http.Request, query string) string {
	rewritten, changed := keepSeriesGrouping(query)
	if changed {
		setRequestQuery(r, rewritten)
	}
	return rewritten
}
