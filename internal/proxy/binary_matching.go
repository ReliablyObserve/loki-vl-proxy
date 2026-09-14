package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

type binarySample struct {
	labels map[string]string
	value  float64
}

func binarySamplesByTime(ctx context.Context, body []byte) (map[float64][]binarySample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	result := make(map[float64][]binarySample)
	count := 0
	for _, series := range response.Data.Result {
		points := series.Values
		if len(series.Value) == 2 {
			points = append(points, series.Value)
		}
		for _, point := range points {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(point) != 2 {
				return nil, fmt.Errorf("invalid binary operand sample")
			}
			ts, ok := point[0].(float64)
			if !ok {
				return nil, fmt.Errorf("invalid binary operand timestamp")
			}
			count++
			if count > 1000000 {
				return nil, fmt.Errorf("binary operand exceeds 1000000 sample limit")
			}
			result[ts] = append(result[ts], binarySample{series.Metric, parsePointValue(point[1])})
		}
	}
	return result, nil
}

func binaryMatchingLabels(labels map[string]string, vm *translator.VectorMatchInfo) map[string]string {
	selected := make(map[string]string, len(labels))
	if vm != nil && (vm.MatchOn || len(vm.On) > 0) {
		for _, name := range vm.On {
			if v := labels[name]; v != "" {
				selected[name] = v
			}
		}
		return selected
	}
	for name, value := range labels {
		if value != "" {
			selected[name] = value
		}
	}
	if vm != nil {
		for _, name := range vm.Ignoring {
			delete(selected, strings.TrimSpace(name))
		}
	}
	return selected
}

func binaryLabelKey(labels map[string]string) string {
	b, _ := json.Marshal(labels)
	return string(b)
}

func binaryResultLabels(many, one map[string]string, vm *translator.VectorMatchInfo) map[string]string {
	if vm == nil {
		return many
	}
	grouped := vm.GroupSide != "" || len(vm.GroupLeft) > 0 || len(vm.GroupRight) > 0
	if !grouped {
		return binaryMatchingLabels(many, vm)
	}
	result := make(map[string]string, len(many))
	for k, v := range many {
		result[k] = v
	}
	include := vm.GroupLeft
	if vm.GroupSide == "group_right" || len(vm.GroupRight) > 0 {
		include = vm.GroupRight
	}
	for _, name := range include {
		if v := one[name]; v != "" {
			result[name] = v
		} else {
			delete(result, name)
		}
	}
	return result
}

func binarySampleValue(a, b float64, op string, returnBool bool) (float64, bool) {
	switch op {
	case "+":
		return a + b, true
	case "-":
		return a - b, true
	case "*":
		return a * b, true
	case "/":
		return a / b, true
	case "%":
		return math.Mod(a, b), true
	case "^":
		return math.Pow(a, b), true
	}
	match := false
	switch op {
	case "==":
		match = a == b
	case "!=":
		match = a != b
	case ">":
		match = a > b
	case "<":
		match = a < b
	case ">=":
		match = a >= b
	case "<=":
		match = a <= b
	}
	if returnBool {
		if match {
			return 1, true
		}
		return 0, true
	}
	return a, match
}

type binaryMatchedSeries struct {
	labels map[string]string
	points [][]any
}

func matchBinaryMetricResults(left, right []byte, op, resultType string, vm *translator.VectorMatchInfo, returnBool bool) ([]byte, error) {
	return matchBinaryMetricResultsContext(context.Background(), left, right, op, resultType, vm, returnBool)
}

func matchBinaryMetricResultsContext(ctx context.Context, left, right []byte, op, resultType string, vm *translator.VectorMatchInfo, returnBool bool) ([]byte, error) {
	ctx = binaryEvaluationContext(ctx)
	for _, body := range [][]byte{left, right} {
		if err := checkBinaryDecodeBudget(ctx, body); err != nil {
			return nil, err
		}
	}
	if err := validateBinaryVectorCardinality(ctx, left, right, op, vm, false, false); err != nil {
		return nil, err
	}
	lhs, err := binarySamplesByTime(ctx, left)
	if err != nil {
		return nil, err
	}
	rhs, err := binarySamplesByTime(ctx, right)
	if err != nil {
		return nil, err
	}
	axis := make([]float64, 0, len(lhs)+len(rhs))
	seen := make(map[float64]bool)
	for ts := range lhs {
		axis = append(axis, ts)
		seen[ts] = true
	}
	for ts := range rhs {
		if !seen[ts] {
			axis = append(axis, ts)
		}
	}
	sort.Float64s(axis)
	series := make(map[string]*binaryMatchedSeries)
	for _, ts := range axis {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		appendSample := func(sample binarySample) error {
			if err := checkBinaryOutputSample(ctx); err != nil {
				return err
			}
			key := binaryLabelKey(sample.labels)
			s := series[key]
			if s == nil {
				s = &binaryMatchedSeries{labels: sample.labels}
				series[key] = s
			}
			if n := len(s.points); n > 0 && s.points[n-1][0] == ts {
				return vectorMatchError("multiple matches for labels: grouping labels must ensure unique matches")
			}
			s.points = append(s.points, []any{ts, strconv.FormatFloat(sample.value, 'g', -1, 64)})
			return nil
		}
		if err := matchBinaryEvaluation(ctx, lhs[ts], rhs[ts], op, vm, returnBool, appendSample); err != nil {
			return nil, err
		}
	}
	return encodeBinarySeriesContext(ctx, series, resultType, maxBufferedBackendBodyBytes)
}

func matchBinaryScalarContext(ctx context.Context, body []byte, scalar float64, scalarLeft bool, op, resultType string, returnBool bool) ([]byte, error) {
	ctx = binaryEvaluationContext(ctx)
	if err := checkBinaryDecodeBudget(ctx, body); err != nil {
		return nil, err
	}
	points, err := binarySamplesByTime(ctx, body)
	if err != nil {
		return nil, err
	}
	axis := make([]float64, 0, len(points))
	for ts := range points {
		axis = append(axis, ts)
	}
	sort.Float64s(axis)
	series := make(map[string]*binaryMatchedSeries)
	for _, ts := range axis {
		for _, sample := range points[ts] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a, b := sample.value, scalar
			if scalarLeft {
				a, b = b, a
			}
			value, keep := binarySampleValue(a, b, op, returnBool)
			if !keep {
				continue
			}
			if err := checkBinaryOutputSample(ctx); err != nil {
				return nil, err
			}
			// Filtered vector/scalar comparisons retain the vector value,
			// including when the scalar is syntactically on the left.
			if !returnBool && (op == "==" || op == "!=" || op == ">" || op == "<" || op == ">=" || op == "<=") {
				value = sample.value
			}
			if err := checkBinaryOutputLabels(ctx, sample.labels); err != nil {
				return nil, err
			}
			key := binaryLabelKey(sample.labels)
			s := series[key]
			if s == nil {
				s = &binaryMatchedSeries{labels: sample.labels}
				series[key] = s
			}
			s.points = append(s.points, []any{ts, strconv.FormatFloat(value, 'g', -1, 64)})
		}
	}
	return encodeBinarySeriesContext(ctx, series, resultType, maxBufferedBackendBodyBytes)
}

func matchBinaryEvaluation(ctx context.Context, lhs, rhs []binarySample, op string, vm *translator.VectorMatchInfo, returnBool bool, emit func(binarySample) error) error {
	if op == "and" || op == "or" || op == "unless" {
		return matchBinarySet(ctx, lhs, rhs, op, vm, emit)
	}
	swap := vm != nil && (vm.GroupSide == "group_right" || len(vm.GroupRight) > 0)
	if swap {
		lhs, rhs = rhs, lhs
	}
	one := make(map[string]binarySample, len(rhs))
	outputLabels := make(map[string]bool)
	for _, sample := range rhs {
		if err := ctx.Err(); err != nil {
			return err
		}
		one[binaryLabelKey(binaryMatchingLabels(sample.labels, vm))] = sample
	}
	for _, sample := range lhs {
		if err := ctx.Err(); err != nil {
			return err
		}
		match, ok := one[binaryLabelKey(binaryMatchingLabels(sample.labels, vm))]
		if !ok {
			continue
		}
		labels := binaryResultLabels(sample.labels, match.labels, vm)
		if err := checkBinaryOutputLabels(ctx, labels); err != nil {
			return err
		}
		outputKey := binaryLabelKey(labels)
		if outputLabels[outputKey] {
			return vectorMatchError("multiple matches for labels: grouping labels must ensure unique matches")
		}
		outputLabels[outputKey] = true
		a, b := sample.value, match.value
		if swap {
			a, b = b, a
		}
		value, keep := binarySampleValue(a, b, op, returnBool)
		if !keep {
			continue
		}
		if err := emit(binarySample{labels, value}); err != nil {
			return err
		}
	}
	return nil
}

func matchBinarySet(ctx context.Context, lhs, rhs []binarySample, op string, vm *translator.VectorMatchInfo, emit func(binarySample) error) error {
	boundedEmit := func(sample binarySample) error {
		if err := checkBinaryOutputLabels(ctx, sample.labels); err != nil {
			return err
		}
		return emit(sample)
	}
	leftKeys, rightKeys := make(map[string]bool), make(map[string]bool)
	for _, sample := range rhs {
		if err := ctx.Err(); err != nil {
			return err
		}
		rightKeys[binaryLabelKey(binaryMatchingLabels(sample.labels, vm))] = true
	}
	for _, sample := range lhs {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := binaryLabelKey(binaryMatchingLabels(sample.labels, vm))
		leftKeys[key] = true
		if op == "or" || (op == "and" && rightKeys[key]) || (op == "unless" && !rightKeys[key]) {
			if err := boundedEmit(sample); err != nil {
				return err
			}
		}
	}
	if op == "or" {
		for _, sample := range rhs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !leftKeys[binaryLabelKey(binaryMatchingLabels(sample.labels, vm))] {
				if err := boundedEmit(sample); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
