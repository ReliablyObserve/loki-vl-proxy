package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// binaryScalarValue is the value of a scalar expression: literals and
// arithmetic of them, which Loki folds into one literal.
func binaryScalarValue(expr logqlpkg.Expr, depth int) (float64, bool) {
	value, isVector, ok := binaryConstantValue(expr, depth)
	return value, ok && !isVector
}

// binaryConstantValue folds an expression of literals and vector(n) into its
// value, and says whether any operand was a vector(n), making the result a
// one-sample vector with no labels instead of a scalar. A division by zero is
// NaN, as in Loki's MergeBinOp (pkg/logql/syntax/ast.go). A comparison with a
// vector operand needs the bool modifier and the generic binary path.
func binaryConstantValue(expr logqlpkg.Expr, depth int) (value float64, isVector, ok bool) {
	if depth >= 64 {
		return 0, false, false
	}
	switch e := expr.(type) {
	case *logqlpkg.LiteralExpr:
		return e.Value, false, true
	case *logqlpkg.OpaqueMetricExpr:
		if matches := vectorLiteralRE.FindStringSubmatch(e.Raw); len(matches) == 2 {
			if value, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64); err == nil {
				return value, true, true
			}
		}
	case *logqlpkg.BinOpExpr:
		if e.VectorMatching != nil || e.Op == "and" || e.Op == "or" || e.Op == "unless" {
			return 0, false, false
		}
		left, leftVector, leftOK := binaryConstantValue(e.Left, depth+1)
		right, rightVector, rightOK := binaryConstantValue(e.Right, depth+1)
		if !leftOK || !rightOK || ((leftVector || rightVector) && isComparisonOp(e.Op)) {
			return 0, false, false
		}
		if e.Op == "/" && right == 0 {
			return math.NaN(), leftVector || rightVector, true
		}
		value, _ := binarySampleValue(left, right, e.Op, true)
		return value, leftVector || rightVector, true
	}
	return 0, false, false
}

func isComparisonOp(op string) bool {
	switch op {
	case "==", "!=", ">", "<", ">=", "<=":
		return true
	}
	return false
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
	points := [][]any{{float64(ts) / 1e9, strconv.FormatFloat(value, 'f', -1, 64)}}
	return encodeBinarySeriesContext(ctx, map[string]*binaryMatchedSeries{"{}": {labels: map[string]string{}, points: points}}, "vector", executionLimitsFrom(ctx).BufferedBackendBodyBytes)
}

func binaryConstantResponse(r *http.Request, value float64, resultType string) ([]byte, error) {
	encoded := strconv.FormatFloat(value, 'f', -1, 64)
	if resultType != "matrix" {
		ts, ok := parseLokiTimeToUnixNano(r.FormValue("time"))
		if !ok {
			return nil, fmt.Errorf("invalid scalar evaluation time")
		}
		// Loki's frontend marshalScalar passes the millisecond timestamp to
		// model.TimeFromUnix (seconds), so a scalar is stamped in integer
		// milliseconds, unlike the vector and matrix samples.
		return json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "scalar", "result": []any{ts / int64(time.Millisecond), encoded}}})
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
	return encodeBinarySeriesContext(ctx, map[string]*binaryMatchedSeries{"{}": {labels: map[string]string{}, points: points}}, "matrix", executionLimitsFrom(ctx).BufferedBackendBodyBytes)
}
