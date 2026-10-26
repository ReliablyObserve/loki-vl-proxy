package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
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

// withoutAggregationOp returns the outer aggregation of a `<op> without (...)`
// query, which decides how the series that share a label set are combined.
func withoutAggregationOp(logql string) string {
	expr, err := logqlpkg.Parse(strings.TrimSpace(logql))
	if err != nil {
		return ""
	}
	aggregation, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok {
		return ""
	}
	return strings.ToLower(string(aggregation.Op))
}

// withoutFoldOp returns the operator that combines the rows sharing a label set
// after a `without` marker's labels are dropped (see ParseWithoutMarker), and
// whether the query is one the proxy folds from a row per stream: a sum, min,
// max, avg or count without, the outermost aggregation of the query. The marker
// of any other query leaves the answer to the aggregation of the whole query
// (logql), as it was before the fold.
func withoutFoldOp(markerOp, logql string) (op string, fold bool) {
	if markerOp != "" {
		return markerOp, true
	}
	return withoutAggregationOp(logql), false
}

// withoutMerge is how the values of the series that share a label set after
// the excluded labels are dropped are combined: Loki's outer aggregation.
type withoutMerge struct {
	sum, count bool
	min, max   bool
	avg        bool
}

// withoutMergeForOp returns the merge for a `<op> without (...)` aggregation.
// sum is the default: it is what every caller did before the other operators
// were translated series by series.
func withoutMergeForOp(op string) withoutMerge {
	switch op {
	case "count":
		return withoutMerge{count: true}
	case "min":
		return withoutMerge{min: true}
	case "max":
		return withoutMerge{max: true}
	case "avg":
		return withoutMerge{avg: true}
	default:
		return withoutMerge{sum: true}
	}
}

// combine folds value into the accumulator of a group. n is how many values,
// including this one, the group has seen.
func (m withoutMerge) combine(acc, value float64, n int) float64 {
	switch {
	case n == 1 && !m.count:
		return value
	case m.count:
		return acc + 1
	case m.min:
		// A NaN accumulator is replaced and a later NaN never wins, as in
		// pkg/logql/evaluator.go VectorAggEvaluator.Next.
		if math.IsNaN(acc) || value < acc {
			return value
		}
		return acc
	case m.max:
		if math.IsNaN(acc) || value > acc {
			return value
		}
		return acc
	case m.avg:
		return acc + (value-acc)/float64(n)
	default:
		return acc + value
	}
}

// withWithoutFold marks a request whose series the proxy folds by `without` once
// VictoriaLogs has answered (applyWithoutGrouping): VictoriaLogs returns a row
// per stream, since a LogsQL `by` cannot name every label but the excluded ones,
// and Loki counts max_query_series on the folded answer
// (pkg/logql/engine.go:536-548 and :575-590 JoinSampleVector). It reuses
// foldedSumKey, the mark of an ungrouped sum folded afterwards: the reads before
// the fold are bounded by the raw-row budget (-manual-range-metric-row-limit)
// unless the tenant's limit is lower, the budget's error is what they answer,
// and capWithoutFold applies the tenant's limit to the answer.
func (p *Proxy) withWithoutFold(r *http.Request) *http.Request {
	if scope, _ := r.Context().Value(seriesLimitScopeKey{}).(*seriesLimitScope); scope != nil {
		// The budget bounds the reads before the fold unless the tenant's limit is
		// the lower of the two, which then stays the bound (equal limits: the
		// budget, which is what the request is reading under).
		if budget, err := p.manualMetricRowBudget(); err == nil && budget >= p.requestQueryLimits(r.Context()).MaxQuerySeries {
			scope.foldBudget = budget
		}
	}
	ctx := context.WithValue(r.Context(), withoutFoldKey{}, true)
	return r.WithContext(context.WithValue(ctx, foldedSumKey{}, true))
}

// withoutFoldKey marks a request whose `without` the proxy folds
// (withWithoutFold), unlike foldedSumKey, which an ungrouped sum sets too and
// the series limit checks clear. Only such a request names the labels an
// extraction list extracts in its series (withParsedGroupLabels).
type withoutFoldKey struct{}

// capWithoutFold applies max_query_series to the folded answer of a
// withWithoutFold request: Loki's error for a plain client, the busiest series
// with Loki's warning for Logs Drilldown.
func (p *Proxy) capWithoutFold(ctx context.Context, body []byte) ([]byte, error) {
	ctx = context.WithValue(ctx, foldedSumKey{}, false)
	return capSeriesToLimit(ctx, body, p.resolvedMaxStatsQuerySeries(ctx))
}

// withoutAllLabels, as an excluded label, drops every label: the series of the
// response fold into one, an aggregation with no by() or without().
const withoutAllLabels = "*"

// applyWithoutGrouping removes excluded labels from metric results and folds the
// series that now share a label set with op, the `without` aggregation:
//   - VL returns a row per stream, with all its labels
//   - the excluded labels are removed and the values of the series that share the
//     key are combined as op says (sum when it is unknown)
func applyWithoutGrouping(body []byte, excludeLabels []string, op string) []byte {
	if folded, ok := foldLevels(body, withoutFoldLevel(excludeLabels, op)); ok {
		return folded
	}
	return applyWithoutGroupingGeneric(body, excludeLabels, op)
}

// applyWithoutGroupingGeneric folds the answers foldLevels does not read.
func applyWithoutGroupingGeneric(body []byte, excludeLabels []string, op string) []byte {
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

	merge := withoutMergeForOp(op)
	if resp.Data.ResultType == "vector" {
		return applyWithoutVector(body, exclude, merge)
	}
	if resp.Data.ResultType == "matrix" {
		return applyWithoutMatrix(body, exclude, merge)
	}
	return body
}

func applyWithoutVector(body []byte, exclude map[string]bool, merge withoutMerge) []byte {
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
		count  int
		ts     interface{}
	}
	groups := make(map[string]*groupEntry)

	for _, series := range resp.Data.Result {
		// Strip excluded labels
		filtered := make(map[string]string)
		for k, v := range series.Metric {
			if !exclude[k] && !exclude[withoutAllLabels] {
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
			existing.count++
			existing.value = merge.combine(existing.value, val, existing.count)
		} else {
			// Convert to map[string]interface{} for JSON marshaling
			m := make(map[string]interface{}, len(filtered))
			for k, v := range filtered {
				m[k] = v
			}
			groups[key] = &groupEntry{metric: m, value: merge.combine(0, val, 1), count: 1, ts: ts}
		}
	}

	// Build result
	result := []map[string]interface{}{}
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

func applyWithoutMatrix(body []byte, exclude map[string]bool, merge withoutMerge) []byte {
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

	type groupedSeries struct {
		metric map[string]interface{}
		values map[string]float64
		counts map[string]int
		order  map[string]interface{}
	}
	groups := make(map[string]*groupedSeries)
	for _, series := range resp.Data.Result {
		filtered := make(map[string]string)
		for k, v := range series.Metric {
			if !exclude[k] && !exclude[withoutAllLabels] {
				filtered[k] = v
			}
		}
		key := metricKeyStr(filtered)
		group := groups[key]
		if group == nil {
			metric := make(map[string]interface{}, len(filtered))
			for k, v := range filtered {
				metric[k] = v
			}
			group = &groupedSeries{
				metric: metric,
				values: make(map[string]float64, len(series.Values)),
				counts: make(map[string]int, len(series.Values)),
				order:  make(map[string]interface{}, len(series.Values)),
			}
			groups[key] = group
		}
		for _, point := range series.Values {
			if len(point) < 2 {
				continue
			}
			tsKey := fmt.Sprintf("%v", point[0])
			group.order[tsKey] = point[0]
			value, ok := 0.0, false
			switch raw := point[1].(type) {
			case string:
				value, ok = 0, true
				value, _ = strconv.ParseFloat(raw, 64)
			case float64:
				value, ok = raw, true
			}
			if !ok {
				continue
			}
			group.counts[tsKey]++
			group.values[tsKey] = merge.combine(group.values[tsKey], value, group.counts[tsKey])
		}
	}

	result := []map[string]interface{}{}
	for _, group := range groups {
		type sampleTime struct {
			key     string
			seconds float64
			ok      bool
		}
		timestamps := make([]sampleTime, 0, len(group.values))
		for ts := range group.values {
			seconds, ok := sampleTimeSeconds(group.order[ts])
			timestamps = append(timestamps, sampleTime{ts, seconds, ok})
		}
		// Sample times order numerically: their text form ("1.79166e+09",
		// "1.7916642e+09") does not.
		sort.Slice(timestamps, func(i, j int) bool {
			a, b := timestamps[i], timestamps[j]
			if a.ok && b.ok && a.seconds != b.seconds {
				return a.seconds < b.seconds
			}
			return a.key < b.key
		})
		values := make([][]interface{}, 0, len(timestamps))
		for _, t := range timestamps {
			ts := t.key
			values = append(values, []interface{}{
				group.order[ts],
				strconv.FormatFloat(group.values[ts], 'f', -1, 64),
			})
		}
		result = append(result, map[string]interface{}{
			"metric": group.metric,
			"values": values,
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

type vectorMatchError string

func (e vectorMatchError) Error() string { return string(e) }

func validateVectorMatchCardinality(leftBody, rightBody []byte, onLabels []string, ignoringLabels []string, allowGroupLeft, allowGroupRight bool) error {
	return validateVectorMatchCardinalityContext(context.Background(), leftBody, rightBody, onLabels, ignoringLabels, allowGroupLeft, allowGroupRight)
}

func validateVectorMatchCardinalityContext(ctx context.Context, leftBody, rightBody []byte, onLabels []string, ignoringLabels []string, allowGroupLeft, allowGroupRight bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Loki checks each evaluation independently. Streams which never overlap
	// must not create a false many-to-one error across the whole range.
	type evaluationKey struct {
		labels string
		time   float64
	}
	var countErr error
	counts := func(body []byte) map[evaluationKey]int {
		var response struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Value  []interface{}     `json:"value"`
					Values [][]interface{}   `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		result := make(map[evaluationKey]int)
		if countErr = ctx.Err(); countErr != nil {
			return result
		}
		if json.Unmarshal(body, &response) != nil {
			return result
		}
		ignore := make(map[string]bool, len(ignoringLabels))
		for _, label := range ignoringLabels {
			ignore[label] = true
		}
		for _, series := range response.Data.Result {
			if countErr = ctx.Err(); countErr != nil {
				return result
			}
			labels := make(map[string]string, len(series.Metric))
			if onLabels != nil {
				for _, label := range onLabels {
					if value := series.Metric[label]; value != "" {
						labels[label] = value
					}
				}
			} else {
				for label, value := range series.Metric {
					if !ignore[label] && value != "" {
						labels[label] = value
					}
				}
			}
			encoded, _ := json.Marshal(labels)
			key := string(encoded)
			values := series.Values
			if len(series.Value) == 2 {
				values = append(values, series.Value)
			}
			for _, value := range values {
				if countErr = ctx.Err(); countErr != nil {
					return result
				}
				if len(value) != 2 {
					continue
				}
				if ts, ok := value[0].(float64); ok {
					result[evaluationKey{key, ts}]++
				}
			}
		}
		return result
	}
	leftCounts, rightCounts := counts(leftBody), counts(rightBody)
	if countErr != nil {
		return countErr
	}
	// The "one" side must be unique even without a matching sample.
	oneSide, side := rightCounts, "right"
	if allowGroupRight {
		oneSide, side = leftCounts, "left"
	}
	for _, count := range oneSide {
		if err := ctx.Err(); err != nil {
			return err
		}
		if count > 1 {
			return vectorMatchError(fmt.Sprintf("found duplicate series on the %s hand-side; many-to-many matching not allowed: matching labels must be unique on one side", side))
		}
	}
	if !allowGroupLeft && !allowGroupRight {
		for key, count := range leftCounts {
			if err := ctx.Err(); err != nil {
				return err
			}
			if count > 1 && rightCounts[key] > 0 {
				return vectorMatchError("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)")
			}
		}
	}
	return nil
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

// --- group() / label_replace() / label_join() post-processing ---

// matrixResponseRW is a minimal struct for parsing and re-encoding a Loki matrix response.
type matrixResponseRW struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// applyGroupNormalization sets every sample value in a matrix response to "1",
// implementing group() semantics (return 1 for each series that has data).
func applyGroupNormalization(body []byte) []byte {
	var resp matrixResponseRW
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "matrix" {
		return body
	}
	for i := range resp.Data.Result {
		for j := range resp.Data.Result[i].Values {
			if len(resp.Data.Result[i].Values[j]) >= 2 {
				resp.Data.Result[i].Values[j][1] = "1"
			}
		}
	}
	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

// applyLabelReplace applies label_replace semantics to all series in a matrix response.
// For each series: new_value = regexp.ReplaceAll(src_label_value, replacement).
// If new_value is non-empty, dst_label is set; otherwise dst_label is deleted.
func applyLabelReplace(body []byte, spec translator.LabelReplaceSpec) []byte {
	re, err := regexp.Compile("^(?:" + spec.Regex + ")$")
	if err != nil {
		return body
	}
	var resp matrixResponseRW
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "matrix" {
		return body
	}
	for i, s := range resp.Data.Result {
		src := s.Metric[spec.SrcLabel]
		// Prometheus semantics: if regex matches, apply replacement; if not, series unchanged.
		if re.MatchString(src) {
			newVal := re.ReplaceAllString(src, spec.Replacement)
			if newVal != "" {
				resp.Data.Result[i].Metric[spec.DstLabel] = newVal
			} else {
				delete(resp.Data.Result[i].Metric, spec.DstLabel)
			}
		}
	}
	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

// applyLabelJoin applies label_join semantics to all series in a matrix response.
// dst_label is set to the src_labels values joined by separator.
func applyLabelJoin(body []byte, spec translator.LabelJoinSpec) []byte {
	var resp matrixResponseRW
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "matrix" {
		return body
	}
	for i, s := range resp.Data.Result {
		parts := make([]string, 0, len(spec.SrcLabels))
		for _, src := range spec.SrcLabels {
			if v := s.Metric[src]; v != "" {
				parts = append(parts, v)
			}
		}
		resp.Data.Result[i].Metric[spec.DstLabel] = strings.Join(parts, spec.Separator)
	}
	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

// sampleTimeSeconds reads a sample time of a decoded response: a JSON number, or
// its text.
func sampleTimeSeconds(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	}
	return 0, false
}
