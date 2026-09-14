package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func binaryScalarValue(expr logqlpkg.Expr, depth int) (float64, bool) {
	if depth >= 64 {
		return 0, false
	}
	switch e := expr.(type) {
	case *logqlpkg.LiteralExpr:
		return e.Value, true
	case *logqlpkg.BinOpExpr:
		if e.VectorMatching != nil || e.Op == "and" || e.Op == "or" || e.Op == "unless" {
			return 0, false
		}
		left, leftOK := binaryScalarValue(e.Left, depth+1)
		right, rightOK := binaryScalarValue(e.Right, depth+1)
		if !leftOK || !rightOK {
			return 0, false
		}
		value, _ := binarySampleValue(left, right, e.Op, true)
		return value, true
	default:
		return 0, false
	}
}

func binaryConstantVectorResponse(r *http.Request, value float64, resultType string) ([]byte, error) {
	if resultType == "matrix" {
		return binaryConstantResponse(r, value, resultType)
	}
	ts, ok := parseLokiTimeToUnixNano(r.FormValue("time"))
	if !ok {
		return nil, fmt.Errorf("invalid vector evaluation time")
	}
	ctx := binaryEvaluationContext(r.Context())
	if err := checkBinaryOutputSample(ctx); err != nil {
		return nil, err
	}
	points := [][]any{{float64(ts) / 1e9, strconv.FormatFloat(value, 'g', -1, 64)}}
	return encodeBinarySeriesContext(ctx, map[string]*binaryMatchedSeries{"{}": {labels: map[string]string{}, points: points}}, "vector", maxBufferedBackendBodyBytes)
}

func binaryConstantResponse(r *http.Request, value float64, resultType string) ([]byte, error) {
	encoded := strconv.FormatFloat(value, 'g', -1, 64)
	if resultType != "matrix" {
		ts, ok := parseLokiTimeToUnixNano(r.FormValue("time"))
		if !ok {
			return nil, fmt.Errorf("invalid scalar evaluation time")
		}
		// Loki's scalar instant result uses its model.Time millisecond value.
		return json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "scalar", "result": []any{ts / 1e6, encoded}}})
	}
	start, startOK := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, endOK := parseLokiTimeToUnixNano(r.FormValue("end"))
	step, stepOK := parsePositiveStepDuration(r.FormValue("step"))
	if !startOK || !endOK || !stepOK || end < start || (end-start)/int64(step) >= 1000000 {
		return nil, fmt.Errorf("invalid or excessive scalar evaluation range")
	}
	ctx := binaryEvaluationContext(r.Context())
	count := int(1 + (end-start)/int64(step))
	if count > ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.outputSamples {
		return nil, fmt.Errorf("binary expression output sample budget exceeded")
	}
	points := make([][]any, 0, count)
	for ts := start; ts <= end; {
		if err := checkBinaryOutputSample(ctx); err != nil {
			return nil, err
		}
		points = append(points, []any{float64(ts) / 1e9, encoded})
		if end-ts < int64(step) {
			break
		}
		ts += int64(step)
	}
	return encodeBinarySeriesContext(ctx, map[string]*binaryMatchedSeries{"{}": {labels: map[string]string{}, points: points}}, "matrix", maxBufferedBackendBodyBytes)
}
