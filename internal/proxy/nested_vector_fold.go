package proxy

import (
	"encoding/json"
	"slices"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// applyWithoutChain folds the series rows of a translated `... without (...)`
// query, in the proxy because VictoriaLogs cannot list a series' own labels.
// For a vector aggregation over another one whose innermost level is a
// `without`, every level folds the output of the one below it with its own
// grouping, innermost first, the way Loki's VectorAggEvaluator does
// (pkg/logql/evaluator.go:435-529); otherwise the whole answer is one fold by
// the outermost operator, as before.
func applyWithoutChain(body []byte, withoutLabels []string, logql string) []byte {
	chain, ok := withoutAggregationChain(logql)
	if !ok {
		return applyWithoutGrouping(body, withoutLabels, withoutAggregationOp(logql))
	}
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
		body = applyWithoutGrouping(body, exclude, string(level.Op))
	}
	return body
}

// withoutAggregationChain returns the levels of a vector aggregation over
// another one, innermost first, when its innermost level is a `without` and
// every level is a sum, avg, min, max or count (the chain the translator keeps
// as series rows).
func withoutAggregationChain(logql string) ([]*logqlpkg.VectorAggregation, bool) {
	expr, err := logqlpkg.Parse(strings.TrimSpace(logql))
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
