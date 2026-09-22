package proxy

import (
	"context"
	"net/http"
	"strconv"
)

func (p *Proxy) handleEmptySumWithout(w http.ResponseWriter, r *http.Request, query string) bool {
	expr, count := unwrapEmptySumWithout(query)
	if count == 0 {
		return false
	}
	child := p.evaluateBinaryLogQLOperand(r, expr, "vector")
	if child.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(child.status)
		_, _ = w.Write(child.body.Bytes())
		return true
	}
	body := child.body.Bytes()
	for range count {
		var err error
		body, err = sumWithoutMetricName(child.ctx, body)
		if err != nil {
			p.writeError(w, http.StatusBadRequest, err.Error())
			return true
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
	return true
}

// Dropping the metric name can merge otherwise distinct input series. Retain
// the actual sum semantics, with the same decode/output bounds as binary work.
func sumWithoutMetricName(ctx context.Context, body []byte) ([]byte, error) {
	ctx = binaryEvaluationContext(ctx)
	if err := checkBinaryDecodeBudget(ctx, body); err != nil {
		return nil, err
	}
	samples, err := binarySamplesByTime(ctx, body)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*binaryMatchedSeries)
	for timestamp, points := range samples {
		for _, point := range points {
			if err := checkBinaryOutputSample(ctx); err != nil {
				return nil, err
			}
			delete(point.labels, "__name__")
			key := binaryLabelKey(point.labels)
			// Loki hashes the input label set before Builder.Reset removes
			// empty values from the returned labels. Preserve that distinction.
			for name, value := range point.labels {
				if value == "" {
					delete(point.labels, name)
				}
			}
			if existing := result[key]; existing != nil {
				value := parsePointValue(existing.points[0][1]) + point.value
				existing.points[0][1] = strconv.FormatFloat(value, 'f', -1, 64)
			} else {
				if err := checkBinaryOutputLabels(ctx, point.labels); err != nil {
					return nil, err
				}
				result[key] = &binaryMatchedSeries{labels: point.labels, points: [][]any{{timestamp, strconv.FormatFloat(point.value, 'f', -1, 64)}}}
			}
		}
	}
	return encodeBinarySeriesContext(ctx, result, "vector", executionLimitsFrom(ctx).BufferedBackendBodyBytes)
}
