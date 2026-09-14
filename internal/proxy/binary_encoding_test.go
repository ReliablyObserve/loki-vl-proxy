package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestBinaryEncoderExactByteLimit(t *testing.T) {
	series := map[string]*binaryMatchedSeries{"a": {labels: map[string]string{"escaped": "<>&\n\x01\u2028\u2029\xffé"}, points: [][]any{{float64(12.5), "1"}, {float64(13), "+Inf"}}}}
	for _, resultType := range []string{"matrix", "vector"} {
		body, err := encodeBinarySeriesContext(context.Background(), series, resultType, 4096)
		if err != nil || !json.Valid(body) {
			t.Fatalf("invalid encoding %s: %v", body, err)
		}
		exact, err := encodeBinarySeriesContext(context.Background(), series, resultType, len(body))
		if err != nil || string(exact) != string(body) {
			t.Fatalf("exact limit rejected: %v", err)
		}
		if excess, err := encodeBinarySeriesContext(context.Background(), series, resultType, len(body)-1); err == nil || excess != nil {
			t.Fatal("over-limit response escaped or was returned truncated")
		}
	}
	for _, value := range []string{"", "normal", "<>&\n\x01\u2028\u2029\xffé", "\\\"\b\f\r\t"} {
		want, _ := json.Marshal(value)
		size, err := binaryJSONStringSize(context.Background(), value)
		if err != nil || size != len(want) {
			t.Fatalf("quoted size=%d want=%d error=%v", size, len(want), err)
		}
	}
}

func TestBinaryEncoderCancellationAndSampleLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := encodeBinarySeriesContext(ctx, nil, "matrix", 4096); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	ctx = binaryEvaluationContext(context.Background())
	w := &binaryOperandResponse{header: make(http.Header), ctx: ctx, budget: ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget, limit: 4096}
	e := binaryJSONEncoder{w: w, samples: 1000000}
	e.point([]any{float64(1), "1"})
	if w.err == nil || w.body.Len() != 0 {
		t.Fatal("sample cap checked after allocation")
	}
	ctx = binaryEvaluationContext(context.Background())
	ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.outputSamples = 1
	body := binaryTestBody([]map[string]string{{"app": "a"}}, [][]any{{1, "1"}, {2, "2"}})
	if result, err := matchBinaryMetricResultsContext(ctx, body, body, "+", "matrix", nil, false); err == nil || !strings.Contains(err.Error(), "output sample budget") || result != nil {
		t.Fatalf("sample construction bypassed cap: %s %v", result, err)
	}
}

func TestBinaryGroupedLabelAmplificationBound(t *testing.T) {
	many := binaryTestBody([]map[string]string{{"app": "a", "id": "1"}, {"app": "a", "id": "2"}}, [][]any{{1, "1"}}, [][]any{{1, "2"}})
	one := binaryTestBody([]map[string]string{{"app": "a", "included": strings.Repeat("x", 100)}}, [][]any{{1, "1"}})
	for _, side := range []string{"group_left", "group_right"} {
		ctx := binaryEvaluationContext(context.Background())
		ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.outputLabels = 150
		vm := &translator.VectorMatchInfo{On: []string{"app"}, GroupSide: side}
		left, right := many, one
		if side == "group_left" {
			vm.GroupLeft = []string{"included"}
		} else {
			vm.GroupRight = []string{"included"}
			left, right = one, many
		}
		body, err := matchBinaryMetricResultsContext(ctx, left, right, "+", "matrix", vm, false)
		if err == nil || !strings.Contains(err.Error(), "output label budget") || body != nil {
			t.Fatalf("%s amplification accepted: %s %v", side, body, err)
		}
	}
}

func TestBinaryGroupingRepairPreflightAndBoundedRewrite(t *testing.T) {
	expr, err := logqlpkg.Parse(`sum by(level,detected_level)(count_over_time({app="a"}[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := binaryEvaluationContext(context.Background())
	ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.arrays = 0
	if _, _, err := restoreBinaryOperandGrouping(ctx, []byte(`[[[`), expr, "vector"); err == nil || !strings.Contains(err.Error(), "sample/series budget") {
		t.Fatalf("repair decoded before preflight: %v", err)
	}
	ctx = binaryEvaluationContext(context.Background())
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"detected_level":"info"},"value":[1,"2"]}]}}`)
	w := &binaryOperandResponse{header: make(http.Header), ctx: ctx, budget: ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget, limit: len(body)}
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	p := newTestProxy(t, "http://127.0.0.1:1")
	p.restoreBinaryOperandResponse(w, expr, "vector")
	if w.status != http.StatusBadRequest {
		t.Fatalf("expanded repair bypassed writer cap: %d %s", w.status, w.body.String())
	}
	ctx = binaryEvaluationContext(context.Background())
	both := []byte(`{"data":{"result":[{"metric":{"level":"warn","detected_level":"error"},"value":[1,"2"]}]}}`)
	repaired, _, err := restoreBinaryOperandGrouping(ctx, both, expr, "vector")
	if err != nil || !strings.Contains(string(repaired), `"level":"warn"`) || !strings.Contains(string(repaired), `"detected_level":"error"`) {
		t.Fatalf("existing distinct grouping values changed: %s %v", repaired, err)
	}
}

func TestBinaryGroupingRepairKeepsJSONResponseContract(t *testing.T) {
	expr, err := logqlpkg.Parse(`sum by(level)(count_over_time({app="a"}[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	payload := `<img src=x onerror="alert(1)">'` + "\n"
	body, err := json.Marshal(map[string]any{"data": map[string]any{"result": []any{
		map[string]any{"metric": map[string]string{"detected_level": payload}, "value": []any{1, "2"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := binaryEvaluationContext(context.Background())
	w := &binaryOperandResponse{header: make(http.Header), ctx: ctx, budget: ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget, limit: 4096}
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	p := newTestProxy(t, "http://127.0.0.1:1")
	p.restoreBinaryOperandResponse(w, expr, "vector")
	if w.status != http.StatusOK || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("rewritten metric must remain JSON: status=%d headers=%v", w.status, w.Header())
	}
	var response struct {
		Data struct {
			Result []struct{ Metric map[string]string }
		}
	}
	if err := json.Unmarshal(w.body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data.Result) != 1 || response.Data.Result[0].Metric["level"] != payload {
		t.Fatalf("label value must remain inert JSON data: %s", w.body.String())
	}
}

// Output labels are serialized once per series, so a long range over a single
// series must not exhaust the label budget one timestamp at a time.
func TestBinaryLabelBudgetChargedPerSeries(t *testing.T) {
	points := make([][]any, 0, 200)
	for i := 0; i < 200; i++ {
		points = append(points, []any{1700000000 + i*60, "1"})
	}
	body := binaryTestBody([]map[string]string{{"app": strings.Repeat("a", 40)}}, points)
	ctx := binaryEvaluationContext(context.Background())
	// Enough for the series' labels a few times over, far less than 200 copies.
	ctx.Value(binaryEvaluationKey{}).(binaryEvaluationState).budget.outputLabels = 200
	result, err := matchBinaryMetricResultsContext(ctx, body, body, "+", "matrix", nil, false)
	if err != nil {
		t.Fatalf("single-series range exhausted the label budget per sample: %v", err)
	}
	if !strings.Contains(string(result), `"values":[[`) {
		t.Fatalf("expected populated matrix, got %s", result)
	}
}

// A by(level) range operand must keep Loki's level key: the stats path emits
// detected_level, and without the repair on(level) joined on an empty value and
// dropped the label from the result (instant queries were already repaired).
func TestBinaryGroupingRepairAppliesToRangeOperands(t *testing.T) {
	levelOnly, err := logqlpkg.Parse(`sum by(level)(count_over_time({app="a"}[1m]))`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := binaryEvaluationContext(context.Background())
	matrix := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"detected_level":"info"},"values":[[1700000040,"6"],[1700000100,"7"]]}]}}`)
	repaired, changed, err := restoreBinaryOperandGrouping(ctx, matrix, levelOnly, "matrix")
	if err != nil || !changed || !strings.Contains(string(repaired), `"level":"info"`) || !strings.Contains(string(repaired), `[1700000100,"7"]`) {
		t.Fatalf("range operand not repaired: %s %v", repaired, err)
	}

	both, err := logqlpkg.Parse(`sum by(level,detected_level)(count_over_time({app="a"}[1m]))`)
	if err != nil {
		t.Fatal(err)
	}
	// Two input series with identical labels must stay two series with all points.
	duplicate := []byte(`{"data":{"result":[` +
		`{"metric":{"detected_level":"warn"},"values":[[1700000040,"1"],[1700000100,"2"]]},` +
		`{"metric":{"detected_level":"warn"},"values":[[1700000040,"3"]]}]}}`)
	repaired, _, err = restoreBinaryOperandGrouping(ctx, duplicate, both, "matrix")
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(repaired, &response); err != nil {
		t.Fatalf("decode %s: %v", repaired, err)
	}
	if response.Data.ResultType != "matrix" || len(response.Data.Result) != 2 || len(response.Data.Result[0].Values) != 2 || len(response.Data.Result[1].Values) != 1 {
		t.Fatalf("series identity or points changed: %s", repaired)
	}
	for _, series := range response.Data.Result {
		if series.Metric["level"] != "warn" || series.Metric["detected_level"] != "warn" {
			t.Fatalf("level not filled from detected_level: %s", repaired)
		}
	}
}
