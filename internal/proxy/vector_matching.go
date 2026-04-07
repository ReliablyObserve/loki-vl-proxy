package proxy

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// bufferedResponseWriter captures the response body for post-processing.
type bufferedResponseWriter struct {
	header http.Header
	body   []byte
	code   int
}

func (w *bufferedResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *bufferedResponseWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}
func (w *bufferedResponseWriter) WriteHeader(code int) {
	w.code = code
}

// applyWithoutGrouping removes excluded labels from metric results and re-aggregates.
// This implements proper `without(label1, label2)` semantics:
// - VL returns results with all labels
// - We remove the excluded labels and sum values for series that now share the same key
func applyWithoutGrouping(body []byte, excludeLabels []string) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string            `json:"resultType"`
			Result     []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" {
		return body
	}

	exclude := make(map[string]bool, len(excludeLabels))
	for _, l := range excludeLabels {
		exclude[strings.TrimSpace(l)] = true
	}

	if resp.Data.ResultType == "vector" {
		return applyWithoutVector(body, exclude)
	}
	if resp.Data.ResultType == "matrix" {
		return applyWithoutMatrix(body, exclude)
	}
	return body
}

func applyWithoutVector(body []byte, exclude map[string]bool) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}

	// Group by remaining labels (after excluding)
	type groupEntry struct {
		metric map[string]interface{}
		value  float64
		ts     interface{}
	}
	groups := make(map[string]*groupEntry)

	for _, series := range resp.Data.Result {
		// Strip excluded labels
		filtered := make(map[string]string)
		for k, v := range series.Metric {
			if !exclude[k] {
				filtered[k] = v
			}
		}

		key := metricKeyStr(filtered)
		val := 0.0
		var ts interface{}
		if len(series.Value) >= 2 {
			ts = series.Value[0]
			if s, ok := series.Value[1].(string); ok {
				val, _ = strconv.ParseFloat(s, 64)
			}
		}

		if existing, ok := groups[key]; ok {
			existing.value += val // sum aggregation
		} else {
			// Convert to map[string]interface{} for JSON marshaling
			m := make(map[string]interface{}, len(filtered))
			for k, v := range filtered {
				m[k] = v
			}
			groups[key] = &groupEntry{metric: m, value: val, ts: ts}
		}
	}

	// Build result
	var result []map[string]interface{}
	for _, g := range groups {
		result = append(result, map[string]interface{}{
			"metric": g.metric,
			"value":  []interface{}{g.ts, strconv.FormatFloat(g.value, 'f', -1, 64)},
		})
	}

	out, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     result,
		},
	})
	return out
}

func applyWithoutMatrix(body []byte, exclude map[string]bool) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}

	// For matrix, just strip the excluded labels (don't re-aggregate across time)
	var result []map[string]interface{}
	for _, series := range resp.Data.Result {
		filtered := make(map[string]string)
		for k, v := range series.Metric {
			if !exclude[k] {
				filtered[k] = v
			}
		}
		result = append(result, map[string]interface{}{
			"metric": filtered,
			"values": series.Values,
		})
	}

	out, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	})
	return out
}

// applyOnMatching joins two metric results by a specified label subset.
// on(label1, label2) means: match series where label1 and label2 are equal.
func applyOnMatching(leftBody, rightBody []byte, op string, onLabels []string, resultType string) []byte {
	leftSeries := parseMetricSeries(leftBody)
	rightSeries := parseMetricSeries(rightBody)

	// Build right-side index keyed by on-labels
	rightByKey := make(map[string][]metricSeries)
	for _, s := range rightSeries {
		key := subsetKey(s.metric, onLabels)
		rightByKey[key] = append(rightByKey[key], s)
	}

	var result []map[string]interface{}
	for _, left := range leftSeries {
		leftKey := subsetKey(left.metric, onLabels)
		matches := rightByKey[leftKey]
		for _, right := range matches {
			val := applyArithmeticOp(left.value, right.value, op)
			result = append(result, map[string]interface{}{
				"metric": left.metric,
				"value":  []interface{}{left.ts, strconv.FormatFloat(val, 'f', -1, 64)},
			})
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}
	out, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": resultType,
			"result":     result,
		},
	})
	return out
}

// applyIgnoringMatching joins two metric results ignoring specified labels.
// ignoring(label1) means: match on all labels EXCEPT label1.
func applyIgnoringMatching(leftBody, rightBody []byte, op string, ignoringLabels []string, resultType string) []byte {
	ignore := make(map[string]bool, len(ignoringLabels))
	for _, l := range ignoringLabels {
		ignore[strings.TrimSpace(l)] = true
	}

	leftSeries := parseMetricSeries(leftBody)
	rightSeries := parseMetricSeries(rightBody)

	// Build right-side index keyed by all labels except ignored ones
	rightByKey := make(map[string][]metricSeries)
	for _, s := range rightSeries {
		key := excludeKey(s.metric, ignore)
		rightByKey[key] = append(rightByKey[key], s)
	}

	var result []map[string]interface{}
	for _, left := range leftSeries {
		leftKey := excludeKey(left.metric, ignore)
		matches := rightByKey[leftKey]
		for _, right := range matches {
			val := applyArithmeticOp(left.value, right.value, op)
			result = append(result, map[string]interface{}{
				"metric": left.metric,
				"value":  []interface{}{left.ts, strconv.FormatFloat(val, 'f', -1, 64)},
			})
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}
	out, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": resultType,
			"result":     result,
		},
	})
	return out
}

type metricSeries struct {
	metric map[string]string
	value  float64
	ts     interface{}
}

func parseMetricSeries(body []byte) []metricSeries {
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}

	var series []metricSeries
	for _, r := range resp.Data.Result {
		val := 0.0
		var ts interface{}
		if len(r.Value) >= 2 {
			ts = r.Value[0]
			if s, ok := r.Value[1].(string); ok {
				val, _ = strconv.ParseFloat(s, 64)
			}
		}
		series = append(series, metricSeries{metric: r.Metric, value: val, ts: ts})
	}
	return series
}

func subsetKey(metric map[string]string, labels []string) string {
	var parts []string
	for _, l := range labels {
		l = strings.TrimSpace(l)
		parts = append(parts, l+"="+metric[l])
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func excludeKey(metric map[string]string, exclude map[string]bool) string {
	var parts []string
	for k, v := range metric {
		if !exclude[k] {
			parts = append(parts, k+"="+v)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func metricKeyStr(metric map[string]string) string {
	var parts []string
	for k, v := range metric {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func applyArithmeticOp(left, right float64, op string) float64 {
	switch op {
	case "/":
		if right == 0 {
			return 0
		}
		return left / right
	case "*":
		return left * right
	case "+":
		return left + right
	case "-":
		return left - right
	case "%":
		if right == 0 {
			return 0
		}
		return float64(int64(left) % int64(right))
	case "==":
		if left == right {
			return 1
		}
		return 0
	case "!=":
		if left != right {
			return 1
		}
		return 0
	case ">":
		if left > right {
			return 1
		}
		return 0
	case "<":
		if left < right {
			return 1
		}
		return 0
	case ">=":
		if left >= right {
			return 1
		}
		return 0
	case "<=":
		if left <= right {
			return 1
		}
		return 0
	default:
		return left
	}
}
