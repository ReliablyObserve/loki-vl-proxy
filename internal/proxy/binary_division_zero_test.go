package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func sameFloat(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	return a == b
}

var binaryEdgeOperands = []float64{0, math.Copysign(0, -1), 1, -1, 2.5, -7, math.NaN(), math.Inf(1), math.Inf(-1)}

var binaryEdgeOps = []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">", ">=", "<", "<="}

// Every operator against zero, NaN and infinite operands gives Loki's recorded value, with and without bool: a
// zero divisor of / and % is NaN (Inf/0 and -1/0 included), not +Inf or -Inf.
// conformance: semantics/binary-division-by-zero-nan
func TestBinarySampleValueMatchesRecordedLokiAnswers(t *testing.T) {
	answers := readBinaryLokiAnswers(t)
	for _, op := range binaryEdgeOps {
		for i, a := range binaryEdgeOperands {
			for j, b := range binaryEdgeOperands {
				for _, returnBool := range []bool{false, true} {
					want := answers.vector(t, op, i, j, returnBool)
					got, keep := binarySampleValue(a, b, op, returnBool)
					if keep != want.kept || (keep && !sameFloat(got, want.value)) {
						t.Errorf("%v %s %v (bool=%v): got %v keep=%v, Loki %s keep=%v", a, op, b, returnBool, got, keep, want.text, want.kept)
					}
				}
			}
		}
	}
}

// The scalar-only literal path folds to Loki's bool value of the operator (the arithmetic result, or 1 or 0 for a
// comparison).
// conformance: semantics/binary-division-by-zero-nan
func TestBinaryScalarFoldMatchesRecordedLokiAnswers(t *testing.T) {
	answers := readBinaryLokiAnswers(t)
	for _, op := range binaryEdgeOps {
		for i, a := range binaryEdgeOperands {
			for j, b := range binaryEdgeOperands {
				want := answers.vector(t, op, i, j, true)
				got, _ := binarySampleValue(a, b, op, true)
				if !sameFloat(got, want.value) {
					t.Errorf("%v %s %v: got %v, Loki %s", a, op, b, got, want.text)
				}
			}
		}
	}
}

// The fallback arithmetic used when the query cannot be parsed as a binary
// expression has the same division rule (it returned 0).
// conformance: semantics/binary-division-by-zero-nan
func TestApplyOpDivisionAndModuloByZeroAreNaN(t *testing.T) {
	answers := readBinaryLokiAnswers(t)
	for _, op := range binaryEdgeOps {
		for i, a := range binaryEdgeOperands {
			for j, b := range binaryEdgeOperands {
				want := answers.vector(t, op, i, j, true)
				if got := applyOp(a, b, op); !sameFloat(got, want.value) {
					t.Errorf("applyOp(%v, %v, %q) = %v, Loki %s", a, b, op, got, want.text)
				}
			}
		}
	}
}

// vector / 0 on query_range and query: every sample is NaN, as Loki
// evaluates it.
// conformance: semantics/binary-division-by-zero-nan
func TestBinaryVectorDivisionByZeroHandlers(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			return
		}
		_ = r.ParseForm()
		value := "60"
		if strings.Contains(r.FormValue("query"), `"zero"`) {
			value = "0"
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000040,%q],[1700000100,%q]]}]}}`, value, value)
		case "/select/logsql/stats_query":
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000100,%q]}]}}`, value)
		default:
			http.Error(w, "unexpected endpoint "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)

	for _, tc := range []struct {
		name, query, want string
	}{
		{"vector_div_scalar_zero", `sum(rate({app="api"}[1m])) / 0`, "NaN"},
		{"vector_mod_scalar_zero", `sum(rate({app="api"}[1m])) % 0`, "NaN"},
		{"scalar_div_zero_vector", `5 / sum(rate({app="zero"}[1m]))`, "NaN"},
		{"vector_div_zero_vector", `sum(rate({app="api"}[1m])) / sum(rate({app="zero"}[1m]))`, "NaN"},
		{"zero_vector_div_zero_vector", `sum(rate({app="zero"}[1m])) / sum(rate({app="zero"}[1m]))`, "NaN"},
		{"nonzero_divisor_unchanged", `sum(rate({app="api"}[1m])) / 4`, "15"},
		{"zero_numerator_unchanged", `0 / sum(rate({app="api"}[1m]))`, "0"},
	} {
		for _, mode := range []string{"query_range", "query"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				params := url.Values{"query": {tc.query}}
				if mode == "query_range" {
					params.Set("start", "1700000040")
					params.Set("end", "1700000100")
					params.Set("step", "60")
				} else {
					params.Set("time", "1700000100")
				}
				rec := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+mode+"?"+params.Encode(), nil)
				if mode == "query_range" {
					p.handleQueryRange(rec, r)
				} else {
					p.handleQuery(rec, r)
				}
				if rec.Code != http.StatusOK {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
				var response struct {
					Data struct {
						Result []struct {
							Value  []any   `json:"value"`
							Values [][]any `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || len(response.Data.Result) != 1 {
					t.Fatalf("want one series: %v %s", err, rec.Body.String())
				}
				points := response.Data.Result[0].Values
				if mode == "query" {
					points = [][]any{response.Data.Result[0].Value}
				}
				if len(points) == 0 {
					t.Fatalf("no points: %s", rec.Body.String())
				}
				for _, point := range points {
					if got, _ := point[1].(string); got != tc.want {
						t.Fatalf("value %v, want %s: %s", point, tc.want, rec.Body.String())
					}
				}
			})
		}
	}
}

// The matchers that wrap binarySampleValue (scalar on either side, vector with
// vector) keep Loki's value for every operator and operand: a filtered
// comparison returns the vector side's own value and drops a false comparison,
// bool returns 1 or 0, and / and % by zero are NaN.
// conformance: semantics/binary-division-by-zero-nan
func TestBinaryMatchersMatchRecordedLokiAnswers(t *testing.T) {
	answers := readBinaryLokiAnswers(t)
	ctx := binaryEvaluationContext(context.Background())
	decode := func(t *testing.T, body []byte) (string, bool) {
		t.Helper()
		var response struct {
			Data struct {
				Result []struct{ Values [][]any } `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if len(response.Data.Result) == 0 {
			return "", false
		}
		return response.Data.Result[0].Values[0][1].(string), true
	}
	check := func(t *testing.T, what string, body []byte, want float64, wantKeep bool) {
		t.Helper()
		got, ok := decode(t, body)
		if ok != wantKeep {
			t.Errorf("%s: kept=%v, Loki kept=%v (%s)", what, ok, wantKeep, body)
			return
		}
		if !ok {
			return
		}
		value, err := strconv.ParseFloat(got, 64)
		if err != nil || !sameFloat(value, want) {
			t.Errorf("%s: got %s, Loki %v", what, got, want)
		}
	}
	for _, op := range binaryEdgeOps {
		for i, a := range binaryEdgeOperands {
			for j, b := range binaryEdgeOperands {
				for _, returnBool := range []bool{false, true} {
					filter := !returnBool && strings.ContainsAny(op, "=<>")
					join := answers.vector(t, op, i, j, returnBool)
					vecA := binaryTestBody([]map[string]string{{"app": "x"}}, [][]any{{1700000040, strconv.FormatFloat(a, 'f', -1, 64)}})
					vecB := binaryTestBody([]map[string]string{{"app": "x"}}, [][]any{{1700000040, strconv.FormatFloat(b, 'f', -1, 64)}})
					what := fmt.Sprintf("%v %s %v bool=%v", a, op, b, returnBool)

					// vector op scalar: the vector is the left operand.
					want, keep := join.value, join.kept
					body, err := matchBinaryScalarContext(ctx, vecA, b, false, op, "matrix", returnBool)
					if err != nil {
						t.Fatal(err)
					}
					check(t, "vector-scalar "+what, body, want, keep)

					// scalar op vector: Loki keeps the vector's value for a filtered
					// comparison (TestBinaryOperatorLiteralSidesMatchVectorJoin), so the
					// value kept is b's side here.
					want, keep = join.value, join.kept
					if filter && keep {
						want = b
					}
					body, err = matchBinaryScalarContext(ctx, vecB, a, true, op, "matrix", returnBool)
					if err != nil {
						t.Fatal(err)
					}
					check(t, "scalar-vector "+what, body, want, keep)

					// vector op vector, one-to-one.
					want, keep = join.value, join.kept
					body, err = matchBinaryMetricResultsContext(ctx, vecA, vecB, op, "matrix", nil, returnBool)
					if err != nil {
						t.Fatal(err)
					}
					check(t, "vector-vector "+what, body, want, keep)
				}
			}
		}
	}
}

// A constant vector divided by zero is NaN on the instant endpoint, like Loki's
// answer for two vector() selectors.
// conformance: semantics/binary-division-by-zero-nan
func TestBinaryConstantVectorDivisionByZeroInstant(t *testing.T) {
	p := newGapTestProxy(t, "http://127.0.0.1:1")
	for _, query := range []string{`vector(1)/vector(0)`, `vector(1) % vector(0)`} {
		rec := httptest.NewRecorder()
		p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {"1700000100"}}.Encode(), nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"NaN"`) {
			t.Errorf("%s: want a NaN sample: %d %s", query, rec.Code, rec.Body.String())
		}
	}
}
