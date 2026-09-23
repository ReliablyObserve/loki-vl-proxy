package proxy

import (
	"bytes"
	"context"
	"encoding/json"
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
	w := &binaryOperandResponse{header: make(http.Header), limit: p.limits().BufferedBackendBodyBytes}
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
	if w.status < 400 {
		// A Drilldown operand cut by the series limit carries Loki's warning on
		// its own answer, cached or not; the combined answer must carry it too.
		if limit := seriesLimitWarningLimit(w.body.Bytes()); limit > 0 && requestKeepsPartialSeries(r.Context()) {
			_ = seriesLimitReached(r.Context(), limit)
		}
		p.restoreBinaryOperandResponse(w, expr, resultType)
	}
	return w
}

func (p *Proxy) restoreBinaryOperandResponse(w *binaryOperandResponse, expr logqlpkg.Expr, resultType string) {
	body, changed, err := restoreBinaryOperandGrouping(w.ctx, w.body.Bytes(), expr, resultType)
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
// requested by(level) operand must retain its LogQL key for matching, on range
// and instant queries alike (a range operand without it joined on an empty
// level and lost the label from the result).
func restoreBinaryOperandGrouping(ctx context.Context, body []byte, expr logqlpkg.Expr, resultType string) ([]byte, bool, error) {
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
		return nil, false, err
	}
	// Keep every input series distinct (keyed by position) so the cardinality
	// validator still sees duplicates; only fill in a missing level.
	series := make(map[string]*binaryMatchedSeries, len(response.Data.Result))
	for index, item := range response.Data.Result {
		labels := item.Metric
		if labels == nil {
			labels = map[string]string{}
		}
		if _, exists := labels["level"]; !exists {
			if value, ok := labels["detected_level"]; ok {
				labels["level"] = value
			}
		}
		if err := checkBinaryOutputLabels(ctx, labels); err != nil {
			return nil, false, err
		}
		points := item.Values
		if len(item.Value) == 2 {
			points = append(points, item.Value)
		}
		for range points {
			if err := checkBinaryOutputSample(ctx); err != nil {
				return nil, false, err
			}
		}
		series[fmt.Sprintf("%09d", index)] = &binaryMatchedSeries{labels: labels, points: points}
	}
	encoded, err := encodeBinarySeriesContext(ctx, series, resultType, executionLimitsFrom(ctx).BufferedBackendBodyBytes)
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
	leftValue, leftScalar := binaryScalarValue(expr.Left, 0)
	rightValue, rightScalar := binaryScalarValue(expr.Right, 0)
	if resultType == "matrix" && !leftScalar && !rightScalar {
		// Only a join of two vector operands needs a shared axis.
		r = alignBinaryRangeRequest(r)
	}
	// Share the total work budget across siblings as well as nested children.
	r = r.WithContext(binaryEvaluationContext(r.Context()))
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
	if expr.Op == "or" {
		// A union is the only operation that can hold more series than either
		// operand, each of which the series limit already bounded.
		if result, err = capSeriesToLimit(r.Context(), result, p.resolvedMaxStatsQuerySeries()); err != nil {
			p.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result)
}

// alignBinaryRangeRequest moves start and end down to multiples of step before
// the operands are evaluated. Operands with different range windows take
// different execution paths: sliding windows are evaluated at start+k*step,
// tumbling windows come from VictoriaLogs buckets on the epoch-aligned grid.
// Sharing one aligned axis lets the join match samples, and mirrors Loki's
// query frontend with align_queries_with_step. Requests without a parseable
// start, end or step are returned unchanged.
func alignBinaryRangeRequest(r *http.Request) *http.Request {
	start, startOK := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, endOK := parseLokiTimeToUnixNano(r.FormValue("end"))
	step, stepOK := parsePositiveStepDuration(r.FormValue("step"))
	if !startOK || !endOK || !stepOK || end < start {
		return r
	}
	stepNs := int64(step)
	alignedStart, alignedEnd := start-start%stepNs, end-end%stepNs
	if alignedStart == start && alignedEnd == end {
		return r
	}
	aligned := cloneMetricQueryRequest(r, r.FormValue("query"))
	params := aligned.URL.Query()
	for key, value := range map[string]int64{"start": alignedStart, "end": alignedEnd} {
		encoded := strconv.FormatInt(value, 10)
		aligned.Form.Set(key, encoded)
		if aligned.Method == http.MethodPost {
			aligned.PostForm.Set(key, encoded)
		}
		params.Set(key, encoded)
	}
	aligned.URL.RawQuery = params.Encode()
	return aligned
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
