package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestScalarMatrix_ApplyScalarOp_AllOperators(t *testing.T) {
	body := scalarMatrixBody(4)
	cases := []struct {
		op   string
		want float64
	}{
		{op: "+", want: 6},
		{op: "-", want: 2},
		{op: "*", want: 8},
		{op: "/", want: 2},
		{op: "%", want: 0},
		{op: "^", want: 16},
		{op: "==", want: 0},
		{op: "!=", want: 1},
		{op: ">", want: 1},
		{op: "<", want: 0},
		{op: ">=", want: 1},
		{op: "<=", want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			out := applyScalarOp(body, tc.op, 2, "matrix")
			got := firstMatrixValue(t, out)
			assertApproxEqual(t, got, tc.want)
		})
	}
}

func TestScalarMatrix_ApplyScalarOpReverse_AllOperators(t *testing.T) {
	body := scalarMatrixBody(4)
	cases := []struct {
		op   string
		want float64
	}{
		{op: "+", want: 6},
		{op: "-", want: -2},
		{op: "*", want: 8},
		{op: "/", want: 0.5},
		{op: "%", want: 2},
		{op: "^", want: 16},
		{op: "==", want: 0},
		{op: "!=", want: 1},
		{op: ">", want: 0},
		{op: "<", want: 1},
		{op: ">=", want: 0},
		{op: "<=", want: 1},
	}

	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			out := applyScalarOpReverse(body, tc.op, 2, "matrix")
			got := firstMatrixValue(t, out)
			assertApproxEqual(t, got, tc.want)
		})
	}
}

func TestScalarMatrix_ApplyScalarOp_DivisionAndModuloByZero(t *testing.T) {
	t.Run("right_scalar_zero", func(t *testing.T) {
		body := scalarMatrixBody(4)

		outDiv := applyScalarOp(body, "/", 0, "matrix")
		assertApproxEqual(t, firstMatrixValue(t, outDiv), 0)

		outMod := applyScalarOp(body, "%", 0, "matrix")
		assertApproxEqual(t, firstMatrixValue(t, outMod), 0)
	})

	t.Run("reverse_zero_denominator", func(t *testing.T) {
		body := scalarMatrixBody(0)

		outDiv := applyScalarOpReverse(body, "/", 2, "matrix")
		assertApproxEqual(t, firstMatrixValue(t, outDiv), 0)

		outMod := applyScalarOpReverse(body, "%", 2, "matrix")
		assertApproxEqual(t, firstMatrixValue(t, outMod), 0)
	})
}

func TestScalarMatrix_CombineBinaryMetricResults_RoutesScalarSides(t *testing.T) {
	metricBody := scalarMatrixBody(4)

	rightScalarOut := combineBinaryMetricResults(metricBody, nil, "+", "matrix", false, true, "", "2")
	assertApproxEqual(t, firstMatrixValue(t, rightScalarOut), 6)

	leftScalarOut := combineBinaryMetricResults(nil, metricBody, "-", "matrix", true, false, "2", "")
	assertApproxEqual(t, firstMatrixValue(t, leftScalarOut), -2)
}

func TestScalarMatrix_ApplyScalarOp_InstantVectorSample(t *testing.T) {
	body := scalarVectorBody(4)

	out := applyScalarOp(body, "*", 2, "vector")
	assertApproxEqual(t, firstMatrixValue(t, out), 8)

	reverse := applyScalarOpReverse(body, "-", 2, "vector")
	assertApproxEqual(t, firstMatrixValue(t, reverse), -2)
}

func TestScalarMatrix_ApplyScalarOp_PromDataResultShape(t *testing.T) {
	body := scalarPromVectorBody(4)

	out := applyScalarOp(body, "*", 2, "vector")
	assertApproxEqual(t, firstMatrixValue(t, out), 8)

	reverse := applyScalarOpReverse(body, "-", 2, "vector")
	assertApproxEqual(t, firstMatrixValue(t, reverse), -2)
}

func TestScalarMatrix_CombineMetricResults_PromDataResultShape(t *testing.T) {
	left := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"a"},"value":[1,"4"]}]}}`)
	right := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"a"},"value":[1,"2"]}]}}`)

	out := combineBinaryMetricResults(left, right, "*", "vector", false, false, "", "")
	assertApproxEqual(t, firstMatrixValue(t, out), 8)
}

func TestScalarMatrix_ApplyScalarOp_InvalidBodyReturnsEmptySuccess(t *testing.T) {
	out := applyScalarOp([]byte("not-json"), "+", 2, "matrix")

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			Result []interface{} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("decode fallback response: %v", err)
	}
	if payload.Status != "success" {
		t.Fatalf("expected success fallback status, got %q", payload.Status)
	}
	if len(payload.Data.Result) != 0 {
		t.Fatalf("expected empty fallback result, got %#v", payload.Data.Result)
	}
}

func scalarMatrixBody(v float64) []byte {
	return []byte(fmt.Sprintf(
		`{"results":[{"metric":{"app":"scalar-matrix"},"values":[[1,"%s"]]}]}`,
		strconv.FormatFloat(v, 'f', -1, 64),
	))
}

func scalarVectorBody(v float64) []byte {
	return []byte(fmt.Sprintf(
		`{"results":[{"metric":{"app":"scalar-vector"},"value":[1,"%s"]}]}`,
		strconv.FormatFloat(v, 'f', -1, 64),
	))
}

func scalarPromVectorBody(v float64) []byte {
	return []byte(fmt.Sprintf(
		`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"scalar-prom-vector"},"value":[1,"%s"]}]}}`,
		strconv.FormatFloat(v, 'f', -1, 64),
	))
}

func firstMatrixValue(t *testing.T, body []byte) float64 {
	t.Helper()

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			Results []struct {
				Values [][]interface{} `json:"values"`
				Value  []interface{}   `json:"value"`
			} `json:"results"`
			Result []struct {
				Values [][]interface{} `json:"values"`
				Value  []interface{}   `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode wrapped scalar response: %v; body=%s", err, string(body))
	}
	if payload.Status != "success" {
		t.Fatalf("expected success response, got %q; body=%s", payload.Status, string(body))
	}
	results := payload.Data.Results
	if len(results) == 0 {
		results = payload.Data.Result
	}
	if len(results) == 0 {
		t.Fatalf("missing results in response: %s", string(body))
	}

	var raw interface{}
	if len(results[0].Values) > 0 && len(results[0].Values[0]) >= 2 {
		raw = results[0].Values[0][1]
	} else if len(results[0].Value) >= 2 {
		raw = results[0].Value[1]
	} else {
		t.Fatalf("missing sample in response: %s", string(body))
	}

	switch v := raw.(type) {
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatalf("parse sample value %q: %v", v, err)
		}
		return parsed
	case float64:
		return v
	default:
		t.Fatalf("unexpected sample value type %T (%v)", raw, raw)
		return 0
	}
}

func assertApproxEqual(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("unexpected value: got=%v want=%v", got, want)
	}
}
