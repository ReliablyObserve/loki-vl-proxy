package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/http"
	"regexp"
	"slices"
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

// applyWithoutGrouping removes excluded labels from metric results and re-aggregates.
// This implements proper `without(label1, label2)` semantics:
// - VL returns results with all labels
// - We remove the excluded labels and sum values for series that now share the same key
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
		return math.Min(acc, value)
	case m.max:
		return math.Max(acc, value)
	case m.avg:
		return acc + (value-acc)/float64(n)
	default:
		return acc + value
	}
}

// withoutAllLabels, as an excluded label, drops every label: the series of the
// response fold into one, an aggregation with no by() or without().
const withoutAllLabels = "*"

func applyWithoutGrouping(body []byte, excludeLabels []string, op string) []byte {
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
	labelSets := make(map[string]map[string]string)

	for _, series := range resp.Data.Result {
		// Strip excluded labels
		filtered := make(map[string]string)
		for k, v := range series.Metric {
			if !exclude[k] && !exclude[withoutAllLabels] {
				filtered[k] = v
			}
		}

		key := binaryLabelKey(filtered) // escaped: `a="b,c=d"` is not `a="b",c="d"`
		labelSets[key] = filtered
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

	// Build result, in label order as Loki sorts the vector (pkg/logql/engine.go).
	result := []map[string]interface{}{}
	for _, key := range keysInLabelOrder(labelSets) {
		g := groups[key]
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
	labelSets := make(map[string]map[string]string)
	for _, series := range resp.Data.Result {
		filtered := make(map[string]string)
		for k, v := range series.Metric {
			if !exclude[k] && !exclude[withoutAllLabels] {
				filtered[k] = v
			}
		}
		key := binaryLabelKey(filtered) // escaped: `a="b,c=d"` is not `a="b",c="d"`
		labelSets[key] = filtered
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
	for _, key := range keysInLabelOrder(labelSets) { // as Loki's sort.Sort(matrix)
		group := groups[key]
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

// metricKeyStr is a label set as sorted `name=value` text joined by commas. It
// is not a unique key (a value may hold `,` or `=`): a fold keys by
// binaryLabelKey.
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

// transformSeriesLabels rewrites the label set of every series of a matrix or
// a vector answer with fn, keeping the rest of the answer as it is. Series
// that end with one label set are not an error in Loki: an instant vector
// keeps each sample, a range matrix files their points under one series, those
// of each step in turn (pkg/logql/engine.go JoinSampleVector, vectorsToSeries).
func transformSeriesLabels(body []byte, fn func(map[string]string) map[string]string) []byte {
	var top, data map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil || json.Unmarshal(top["data"], &data) != nil {
		return body
	}
	var resultType string
	var series []map[string]json.RawMessage
	_ = json.Unmarshal(data["resultType"], &resultType)
	if (resultType != "matrix" && resultType != "vector") || json.Unmarshal(data["result"], &series) != nil {
		return body
	}
	merged := make(map[string]int, len(series))
	out := series[:0]
	for _, s := range series {
		var metric map[string]string
		_ = json.Unmarshal(s["metric"], &metric)
		if metric == nil {
			metric = map[string]string{}
		}
		metric = fn(metric)
		s["metric"], _ = json.Marshal(metric)
		if resultType == "matrix" {
			key := binaryLabelKey(metric)
			if at, dup := merged[key]; dup {
				var more, have []json.RawMessage
				_ = json.Unmarshal(s["values"], &more)
				_ = json.Unmarshal(out[at]["values"], &have)
				all := append(have, more...)
				sort.SliceStable(all, func(i, j int) bool { return pointSeconds(all[i]) < pointSeconds(all[j]) })
				out[at]["values"], _ = json.Marshal(all)
				continue
			}
			merged[key] = len(out)
		}
		out = append(out, s)
	}
	data["result"], _ = json.Marshal(out)
	top["data"], _ = json.Marshal(data)
	b, err := json.Marshal(top)
	if err != nil {
		return body
	}
	return b
}

// sortSeriesByLabels orders the series of a matrix or vector answer as Loki
// does after a label_replace: a range result by label set (pkg/logql/engine.go
// JoinSampleVector, sort.Sort(result)), an instant vector the same way unless
// the query sorts by value (sort, sort_desc: evaluator.go Sortable), in which
// case sortable is set and the order is kept.
func sortSeriesByLabels(body []byte, sortable bool) []byte {
	var top, data map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil || json.Unmarshal(top["data"], &data) != nil {
		return body
	}
	var resultType string
	var series []struct {
		raw    json.RawMessage
		metric map[string]string
	}
	var raws []json.RawMessage
	_ = json.Unmarshal(data["resultType"], &resultType)
	if (resultType != "matrix" && resultType != "vector") || (resultType == "vector" && sortable) || json.Unmarshal(data["result"], &raws) != nil {
		return body
	}
	for _, raw := range raws {
		var m struct {
			Metric map[string]string `json:"metric"`
		}
		_ = json.Unmarshal(raw, &m)
		series = append(series, struct {
			raw    json.RawMessage
			metric map[string]string
		}{raw, m.Metric})
	}
	sort.SliceStable(series, func(i, j int) bool { return compareLabelSets(series[i].metric, series[j].metric) < 0 })
	for i := range series {
		raws[i] = series[i].raw
	}
	data["result"], _ = json.Marshal(raws)
	top["data"], _ = json.Marshal(data)
	if b, err := json.Marshal(top); err == nil {
		return b
	}
	return body
}

var sortCallRE = regexp.MustCompile(`(^|[^\w])sort(_desc)?\s*\(`)

// queryIsSortable reports whether a query orders by value (evaluator.go
// Sortable: a sort or sort_desc anywhere in it).
func queryIsSortable(logql string) bool { return sortCallRE.MatchString(logql) }

// keysInLabelOrder returns the keys of the label sets in Loki's series order.
func keysInLabelOrder(sets map[string]map[string]string) []string {
	return slices.SortedFunc(maps.Keys(sets), func(a, b string) int { return compareLabelSets(sets[a], sets[b]) })
}

// compareLabelSets is Prometheus' labels.Compare: label by label in name
// order, the name first and then the value, a shorter prefix first.
func compareLabelSets(a, b map[string]string) int {
	names := func(m map[string]string) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	na, nb := names(a), names(b)
	for i := 0; i < len(na) && i < len(nb); i++ {
		if na[i] != nb[i] {
			return strings.Compare(na[i], nb[i])
		}
		if c := strings.Compare(a[na[i]], b[nb[i]]); c != 0 {
			return c
		}
	}
	return len(na) - len(nb)
}

// pointSeconds is the time of a sample point `[time, "value"]`.
func pointSeconds(point json.RawMessage) float64 {
	var p []any
	if json.Unmarshal(point, &p) != nil || len(p) == 0 {
		return 0
	}
	t, _ := sampleTimeSeconds(p[0])
	return t
}

// applyLabelReplace applies label_replace to every series of a matrix or vector
// answer as Loki's LabelReplaceEvaluator does (pkg/logql/evaluator.go): the
// regexp is anchored, a series it does not match is left as it is, and the
// expanded replacement ($1, ${1}, $name) replaces the destination label, which
// an empty expansion removes.
func applyLabelReplace(body []byte, spec translator.LabelReplaceSpec) []byte {
	re, err := regexp.Compile("^(?:" + spec.Regex + ")$")
	if err != nil {
		return body
	}
	return transformSeriesLabels(body, func(in map[string]string) map[string]string {
		src := in[spec.SrcLabel]
		idx := re.FindStringSubmatchIndex(src)
		if idx == nil {
			return in
		}
		res := re.ExpandString(nil, spec.Replacement, src, idx)
		out := make(map[string]string, len(in)+1)
		for k, v := range in {
			out[k] = v
		}
		delete(out, spec.DstLabel)
		if len(res) > 0 {
			out[spec.DstLabel] = string(res)
		}
		return out
	})
}

// applyLabelJoin applies label_join to every series of a matrix or vector
// answer: the destination label is the values of the source labels, the
// absent ones skipped, joined by the separator. Loki v3.7.7 has no label_join;
// accepting it is the open gap semantics/proxy-accepts-operators-not-in-loki.
func applyLabelJoin(body []byte, spec translator.LabelJoinSpec) []byte {
	return transformSeriesLabels(body, func(in map[string]string) map[string]string {
		parts := make([]string, 0, len(spec.SrcLabels))
		for _, src := range spec.SrcLabels {
			if v := in[src]; v != "" {
				parts = append(parts, v)
			}
		}
		out := make(map[string]string, len(in)+1)
		for k, v := range in {
			out[k] = v
		}
		out[spec.DstLabel] = strings.Join(parts, spec.Separator)
		return out
	})
}

// labelTransforms are the label_replace and label_join calls of a query, in
// the order they apply: the innermost first.
type labelTransforms struct {
	replaces []translator.LabelReplaceSpec
	join     *translator.LabelJoinSpec
}

// parseLabelTransformMarkers strips the label_replace and label_join markers
// the translator left in a query. It returns Loki's parse error text when a
// label_replace regexp does not compile.
func parseLabelTransformMarkers(logsql string) (string, labelTransforms, string) {
	logsql, replaces := translator.ParseAllLabelReplaceMarkers(logsql)
	logsql, join := translator.ParseLabelJoinMarker(logsql)
	for _, spec := range replaces {
		if _, err := regexp.Compile("^(?:" + spec.Regex + ")$"); err != nil {
			return logsql, labelTransforms{}, fmt.Sprintf("parse error : invalid regex in label_replace: %v", err)
		}
	}
	return logsql, labelTransforms{replaces: replaces, join: join}, ""
}

// apply runs the transforms over a matrix or vector answer.
func (t labelTransforms) apply(body []byte) []byte {
	for _, spec := range t.replaces {
		body = applyLabelReplace(body, spec)
	}
	if t.join != nil {
		body = applyLabelJoin(body, *t.join)
	}
	return body
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
