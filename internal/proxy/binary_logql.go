package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

type binaryEvaluationKey struct{}
type binaryEvaluationState struct {
	depth     int
	remaining *int
	budget    *binaryExecutionBudget
}

func nextBinaryEvaluation(r *http.Request) (*http.Request, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	r = r.WithContext(binaryEvaluationContext(r.Context()))
	state := r.Context().Value(binaryEvaluationKey{}).(binaryEvaluationState)
	if state.depth >= 64 || *state.remaining <= 0 {
		return nil, fmt.Errorf("binary expression evaluation limit exceeded")
	}
	*state.remaining--
	state.depth++
	return r.WithContext(context.WithValue(r.Context(), binaryEvaluationKey{}, state)), nil
}

// Resolve original operands through the normal handlers: VL stats buckets do
// not implement Loki's trailing range windows or parser-error semantics.
func binaryExprForRequest(r *http.Request) *logqlpkg.BinOpExpr {
	query := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	// The outer handler has already shifted uniform offsets. Mixed offsets
	// remain on their individual operands and are applied by each child handler.
	if _, stripped, err := extractLogQLOffset(query); err == nil {
		query = stripped
	}
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return nil
	}
	for {
		switch x := expr.(type) {
		case *logqlpkg.BinOpExpr:
			return x
		case *logqlpkg.VectorAggregation:
			expr = x.Inner
		default:
			return nil
		}
	}
}

type binaryOperandResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
	err    error
	limit  int
	ctx    context.Context
	budget *binaryExecutionBudget
}

func (w *binaryOperandResponse) Header() http.Header { return w.header }
func (w *binaryOperandResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *binaryOperandResponse) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.ctx != nil {
		if err := w.ctx.Err(); err != nil {
			w.err = err
			return 0, err
		}
	}
	if w.budget != nil && len(data) > w.budget.bytes {
		w.err = fmt.Errorf("binary expression exceeds aggregate response byte budget")
		return 0, w.err
	}
	if len(data) > w.limit-w.body.Len() {
		w.err = fmt.Errorf("binary operand response exceeds %d byte limit", w.limit)
		return 0, w.err
	}
	w.WriteHeader(http.StatusOK)
	if w.budget != nil {
		w.budget.bytes -= len(data)
	}
	return w.body.Write(data)
}

func (p *Proxy) evaluateBinaryLogQLOperand(r *http.Request, expr logqlpkg.Expr, resultType string) *binaryOperandResponse {
	w := &binaryOperandResponse{header: make(http.Header), limit: maxBufferedBackendBodyBytes}
	child, err := nextBinaryEvaluation(r)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return w
	}
	child = cloneMetricQueryRequest(child, expr.String())
	w.ctx = child.Context()
	w.budget = child.Context().Value(binaryEvaluationKey{}).(binaryEvaluationState).budget
	if matches := vectorLiteralRE.FindStringSubmatch(expr.String()); len(matches) == 2 {
		if value, err := strconv.ParseFloat(matches[1], 64); err == nil {
			body, err := binaryConstantVectorResponse(child, value, resultType)
			if err != nil {
				p.writeError(w, http.StatusBadRequest, err.Error())
			} else {
				_, _ = w.Write(body)
			}
			p.finishBinaryOperandResponse(w)
			return w
		}
	}
	if resultType == "matrix" {
		p.handleQueryRange(w, child)
	} else {
		p.handleQuery(w, child)
	}
	p.finishBinaryOperandResponse(w)
	if w.status < 400 && resultType != "matrix" {
		p.restoreBinaryOperandResponse(w, expr)
	}
	return w
}

func (p *Proxy) restoreBinaryOperandResponse(w *binaryOperandResponse, expr logqlpkg.Expr) {
	body, changed, err := restoreBinaryOperandGrouping(w.ctx, w.body.Bytes(), expr)
	if err != nil {
		w.err = err
	} else if changed {
		w.body.Reset()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
	p.finishBinaryOperandResponse(w)
}

func (p *Proxy) finishBinaryOperandResponse(w *binaryOperandResponse) {
	if w.err != nil {
		w.body.Reset()
		w.status = 0
		err := w.err
		w.err = nil
		w.ctx, w.budget = nil, nil
		p.writeError(w, http.StatusBadRequest, err.Error())
	}
}

// Stats compatibility exposes VL level as detected_level. An explicitly
// requested by(level) operand must retain its LogQL key for matching.
func restoreBinaryOperandGrouping(ctx context.Context, body []byte, expr logqlpkg.Expr) ([]byte, bool, error) {
	aggregation, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || aggregation.Grouping == nil || aggregation.Grouping.Without {
		return body, false, nil
	}
	wantsLevel, wantsDetected := false, false
	for _, label := range aggregation.Grouping.Labels {
		wantsLevel = wantsLevel || label == "level"
		wantsDetected = wantsDetected || label == "detected_level"
	}
	if !wantsLevel {
		return body, false, nil
	}
	if !wantsDetected {
		return renameStatsBodyMetricKey(body, "detected_level", "level"), true, nil
	}
	if err := checkBinaryDecodeBudget(ctx, body); err != nil {
		return nil, false, err
	}
	points, err := binarySamplesByTime(ctx, body)
	if err != nil {
		return nil, false, err
	}
	series := make(map[string]*binaryMatchedSeries)
	for stamp, samples := range points {
		for _, sample := range samples {
			if _, exists := sample.labels["level"]; !exists {
				if value, ok := sample.labels["detected_level"]; ok {
					sample.labels["level"] = value
				}
			}
			if err := checkBinaryOutputSample(ctx); err != nil {
				return nil, false, err
			}
			if err := checkBinaryOutputLabels(ctx, sample.labels); err != nil {
				return nil, false, err
			}
			// Preserve duplicate input series for the cardinality validator.
			series[strconv.Itoa(len(series))] = &binaryMatchedSeries{labels: sample.labels, points: [][]any{{stamp, strconv.FormatFloat(sample.value, 'g', -1, 64)}}}
		}
	}
	encoded, err := encodeBinarySeriesContext(ctx, series, "vector", maxBufferedBackendBodyBytes)
	return encoded, true, err
}

func (p *Proxy) proxyBinaryLogQL(w http.ResponseWriter, r *http.Request, expr *logqlpkg.BinOpExpr, resultType string) {
	if resultType != "matrix" && r.FormValue("time") == "" {
		r = cloneMetricQueryRequest(r, r.FormValue("query"))
		now := strconv.FormatInt(time.Now().UnixNano(), 10)
		r.Form.Set("time", now)
		params := r.URL.Query()
		params.Set("time", now)
		r.URL.RawQuery = params.Encode()
	}
	// Share the total work budget across siblings as well as nested children.
	r = r.WithContext(binaryEvaluationContext(r.Context()))
	leftValue, leftScalar := binaryScalarValue(expr.Left, 0)
	rightValue, rightScalar := binaryScalarValue(expr.Right, 0)
	if (leftScalar || rightScalar) && (expr.Op == "and" || expr.Op == "or" || expr.Op == "unless") {
		p.writeError(w, http.StatusBadRequest, "unexpected literal for logical/set binary operation")
		return
	}
	if leftScalar && rightScalar {
		value, _ := binarySampleValue(leftValue, rightValue, expr.Op, true)
		body, err := binaryConstantResponse(r, value, resultType)
		if err != nil {
			p.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	var bodies [2][]byte
	for i, operand := range []logqlpkg.Expr{expr.Left, expr.Right} {
		if (i == 0 && leftScalar) || (i == 1 && rightScalar) {
			continue
		}
		response := p.evaluateBinaryLogQLOperand(r, operand, resultType)
		if response.status >= 400 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(response.status)
			_, _ = w.Write(response.body.Bytes())
			return
		}
		bodies[i] = response.body.Bytes()
	}
	var result []byte
	var err error
	if leftScalar {
		result, err = matchBinaryScalarContext(r.Context(), bodies[1], leftValue, true, expr.Op, resultType, expr.ReturnBool)
	} else if rightScalar {
		result, err = matchBinaryScalarContext(r.Context(), bodies[0], rightValue, false, expr.Op, resultType, expr.ReturnBool)
	} else {
		result, err = matchBinaryMetricResultsContext(r.Context(), bodies[0], bodies[1], expr.Op, resultType, binOpExprToVMInfo(expr), expr.ReturnBool)
	}
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result)
}

func validateBinaryVectorCardinality(ctx context.Context, left, right []byte, op string, vm *translator.VectorMatchInfo, leftScalar, rightScalar bool) error {
	if leftScalar || rightScalar || op == "and" || op == "or" || op == "unless" {
		return nil
	}
	if vm == nil {
		return validateVectorMatchCardinalityContext(ctx, left, right, nil, nil, false, false)
	}
	on := vm.On
	if vm.MatchOn && on == nil {
		on = []string{}
	}
	return validateVectorMatchCardinalityContext(ctx, left, right, on, vm.Ignoring,
		vm.GroupSide == "group_left" || len(vm.GroupLeft) > 0,
		vm.GroupSide == "group_right" || len(vm.GroupRight) > 0)
}
