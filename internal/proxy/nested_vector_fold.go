package proxy

import (
	"encoding/json"
	"net/http"
	"slices"
	"sync/atomic"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// applyWithoutChain folds the series rows of a translated `... without (...)`
// query, in the proxy because VictoriaLogs cannot list a series' own labels.
// For a vector aggregation over another one whose innermost level is a
// `without`, every level folds the output of the one below it with its own
// grouping, innermost first, the way Loki's VectorAggEvaluator does
// (pkg/logql/evaluator.go:435-529); otherwise the whole answer is one fold by
// the outermost operator, as before.
func applyWithoutChain(body []byte, withoutLabels []string, logql, withoutOp string) []byte {
	chain, ok := withoutAggregationChain(logql)
	if !ok {
		return applyWithoutGrouping(body, withoutLabels, withoutOp)
	}
	levels := make([]foldLevel, len(chain))
	for i, level := range chain {
		levels[i] = foldLevelOf(level)
	}
	if folded, ok := foldLevels(body, levels...); ok {
		return folded
	}
	// An answer foldLevels does not read: the generic fold, level by level.
	for _, level := range chain {
		exclude := level.Grouping.Labels
		if !level.Grouping.Without {
			// by (l...) keeps the listed labels a series has: no label, none.
			exclude = []string{withoutAllLabels}
			if len(level.Grouping.Labels) > 0 {
				exclude = exclude[:0]
				for _, name := range seriesLabelNames(body) {
					if !slices.Contains(level.Grouping.Labels, name) {
						exclude = append(exclude, name)
					}
				}
			}
		}
		body = applyWithoutGroupingGeneric(body, exclude, string(level.Op))
	}
	return body
}

// foldLevelOf is the fold of one vector aggregation: `without` drops the listed
// labels, `by` keeps the listed labels a series has, no grouping keeps none.
func foldLevelOf(agg *logqlpkg.VectorAggregation) foldLevel {
	switch {
	case agg.Grouping != nil && agg.Grouping.Without:
		return withoutFoldLevel(agg.Grouping.Labels, string(agg.Op))
	case agg.Grouping != nil:
		return byFoldLevel(agg.Grouping.Labels, string(agg.Op))
	}
	return byFoldLevel(nil, string(agg.Op))
}

// withoutAggregationChain returns the levels of a vector aggregation over
// another one, innermost first, when its innermost level is a `without` and
// every level is a sum, avg, min, max or count (the chain the translator keeps
// as series rows), under any ranking, sorting or label transform the proxy
// applies to the folded answer.
func withoutAggregationChain(logql string) ([]*logqlpkg.VectorAggregation, bool) {
	if !translator.HasNestedVectorKeywords(logql) {
		return nil, false
	}
	expr, err := logqlpkg.Parse(translator.PeelWithoutWrappers(logql))
	if err != nil {
		return nil, false
	}
	var chain []*logqlpkg.VectorAggregation
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		switch agg.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount:
		default:
			return nil, false
		}
		if agg.Grouping == nil {
			agg.Grouping = &logqlpkg.Grouping{}
		}
		chain = append([]*logqlpkg.VectorAggregation{agg}, chain...)
		expr = agg.Inner
	}
	if len(chain) < 2 || !chain[0].Grouping.Without {
		return nil, false
	}
	return chain, true
}

// seriesLabelNames returns the label names the series of a matrix or vector
// body carry.
func seriesLabelNames(body []byte) []string {
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return nil
	}
	var names []string
	for _, series := range resp.Data.Result {
		for name := range series.Metric {
			if !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	return names
}

// nestedVectorRange returns the range aggregation under a vector aggregation
// over another one (two or more levels of sum, avg, min, max and count, each
// grouped by or without labels, in either spelling), as it was written. The
// leading-spelling peeling of parseOriginalRangeMetricSpec reads only
// `sum by (l) (...)`, not `sum(...) by (l)`.
func nestedVectorRange(logql string) (string, bool) {
	if !translator.HasNestedVectorKeywords(logql) {
		return "", false
	}
	text := translator.PeelWithoutWrappers(logql)
	expr, err := logqlpkg.Parse(text)
	if err != nil {
		return "", false
	}
	levels := 0
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		switch agg.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount:
		default:
			return "", false
		}
		levels++
		expr = agg.Inner
	}
	if _, ok := expr.(*logqlpkg.RangeAggregation); !ok || levels < 2 {
		return "", false
	}
	if rng, peeled := translator.VectorChainRange(text); peeled == levels {
		return rng, true
	}
	return "", false
}

// withoutPushdownOff switches the VictoriaLogs-side fold of a `without` off, for
// the tests that compare it with the proxy's.
var withoutPushdownOff atomic.Bool

// pushDownWithoutFold returns the translation that has VictoriaLogs fold a
// `<op> without (...)` itself (translator.PushDownWithout), for a range query
// whose windows are the step buckets VictoriaLogs answers natively (range equal
// to step; a sliding window is evaluated from the series rows by the proxy, and
// an instant query keeps the fold of the rows it asks for). series is the
// translation with its without marker removed. ok is false when the proxy has to
// fold the series rows itself.
func (p *Proxy) pushDownWithoutFold(r *http.Request, logql, series string) (string, bool) {
	if withoutPushdownOff.Load() {
		return "", false
	}
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok {
		return "", false
	}
	if spec, ok := parseOriginalRangeMetricSpec(logql); !ok || !statsRangeIsTumbling(spec.Window, step) {
		return "", false
	}
	return translator.PushDownWithout(logql, series, p.labelTranslator.ToVL, p.labelTranslator.ToLoki)
}
