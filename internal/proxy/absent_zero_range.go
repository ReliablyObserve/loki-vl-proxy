package proxy

import (
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// absent_over_time over a zero range (`[0s]`, `[0]`) selects no sample in any window (t, t], whatever the data, so
// Loki answers its one series valued 1 at every step of a range query and at the time of an instant one, with the
// labels of the selector's equality matchers (TestAbsentZeroRangeAnswersLikeLoki, Loki's engine). The answer reads
// nothing: the proxy writes it from the parsed query, before the response cache, the plan and any VictoriaLogs call,
// through the wrappers that keep one series a series (vector aggregations, rankings, sorts and label_replace). A
// binary operation keeps its route.

// absentEveryStep reports whether expr is an absent_over_time over a zero range under such wrappers only. It reads
// the tree without allocating: it runs on every query the validation cache has not seen.
func absentEveryStep(expr logqlpkg.Expr) bool {
	for {
		switch e := expr.(type) {
		case *logqlpkg.RangeAggregation:
			lq, isLog := e.Inner.(*logqlpkg.LogQuery)
			return e.Op == logqlpkg.RangeAbsentOverTime && e.Grouping == nil && isLog && lq != nil && isZeroDuration(e.Range)
		case *logqlpkg.VectorAggregation:
			expr = e.Inner
		case *logqlpkg.OpaqueMetricExpr:
			if !e.Typed || e.Func != "label_replace" || len(e.Args) != 4 || e.Inner == nil {
				return false
			}
			expr = e.Inner
		default:
			return false
		}
	}
}

// absentZeroSeries is the one series of an absent_over_time over a zero range after its wrappers; none is false
// when a wrapper leaves no series (a ranking of k < 1).
type absentZeroSeries struct {
	labels map[string]string
	value  string
	none   bool
}

// absentZeroRangeSeries folds expr from its absent_over_time outwards; ok is false for any other expression.
func absentZeroRangeSeries(expr logqlpkg.Expr) (absentZeroSeries, bool) {
	switch e := expr.(type) {
	case *logqlpkg.RangeAggregation:
		lq, isLog := e.Inner.(*logqlpkg.LogQuery)
		if e.Op != logqlpkg.RangeAbsentOverTime || e.Grouping != nil || !isLog || lq == nil || !isZeroDuration(e.Range) {
			return absentZeroSeries{}, false
		}
		return absentZeroSeries{labels: absentLabelsOf(lq), value: "1"}, true
	case *logqlpkg.VectorAggregation:
		s, ok := absentZeroRangeSeries(e.Inner)
		if !ok || s.none {
			return s, ok
		}
		s.labels, s.value, s.none = foldAbsentSeries(e, s.labels, s.value)
		return s, true
	case *logqlpkg.OpaqueMetricExpr:
		if !e.Typed || e.Func != "label_replace" || len(e.Args) != 4 || e.Inner == nil {
			return absentZeroSeries{}, false
		}
		re, err := regexp.Compile("^(?:" + e.Args[3] + ")$")
		if err != nil {
			return absentZeroSeries{}, false
		}
		s, ok := absentZeroRangeSeries(e.Inner)
		if !ok || s.none {
			return s, ok
		}
		s.labels = replaceLabel(s.labels, re, physical.LabelReplace{DstLabel: e.Args[0], Replacement: e.Args[1], SrcLabel: e.Args[2], Regex: e.Args[3]})
		return s, true
	}
	return absentZeroSeries{}, false
}

// answerAbsentEveryStep answers a query cachedQueryValidation found to be an absent_over_time over a zero range
// with Loki's answer; it reports whether it answered.
func (p *Proxy) answerAbsentEveryStep(w http.ResponseWriter, r *http.Request, endpoint, query string, start time.Time) bool {
	expr, ok := sharedLogQLExpr(query)
	if !ok {
		return false
	}
	series, ok := absentZeroRangeSeries(expr)
	if !ok {
		return false
	}
	var body []byte
	if endpoint == "query" {
		evalNs, ok := parseFlexibleUnixNanos(r.FormValue("time"))
		if !ok {
			evalNs = time.Now().UnixNano()
		}
		result := []lokiVectorResult{}
		if !series.none {
			result = append(result, lokiVectorResult{Metric: series.labels, Value: []any{lokiSampleSeconds(evalNs), series.value}})
		}
		body, _ = json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	} else {
		startNs, okS := parseLokiTimeToUnixNano(r.FormValue("start"))
		endNs, okE := parseLokiTimeToUnixNano(r.FormValue("end"))
		step, okStep := parsePositiveStepDuration(r.FormValue("step"))
		if !okS || !okE || !okStep {
			return false // the route's own checks answer it
		}
		body = absentMatrix(series.labels, series.value, !series.none, nil, startNs, endNs, step.Nanoseconds())
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
	p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(start))
	p.queryTracker.Record(endpoint, query, time.Since(start), false)
	return true
}
