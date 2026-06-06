package proxy

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestPopulationStddev_EmptyReturnsZero(t *testing.T) {
	if got := populationStddev(nil); got != 0 {
		t.Fatalf("expected 0 for empty slice, got %v", got)
	}
	if got := populationStddev([]float64{}); got != 0 {
		t.Fatalf("expected 0 for zero-length slice, got %v", got)
	}
}

func TestPopulationStddev_SingleValueIsZero(t *testing.T) {
	if got := populationStddev([]float64{42}); got != 0 {
		t.Fatalf("expected 0 for single-value slice, got %v", got)
	}
}

func TestPopulationStddev_KnownDataset(t *testing.T) {
	// Population stddev of {2,4,4,4,5,5,7,9} is 2.0 (classic textbook example).
	vals := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	got := populationStddev(vals)
	if math.Abs(got-2.0) > 1e-9 {
		t.Fatalf("populationStddev want 2.0, got %v", got)
	}
}

func TestPopulationVariance_IsStddevSquared(t *testing.T) {
	vals := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	if got := populationVariance(vals); math.Abs(got-4.0) > 1e-9 {
		t.Fatalf("populationVariance want 4.0, got %v", got)
	}
	if got := populationVariance(nil); got != 0 {
		t.Fatalf("populationVariance(nil) want 0, got %v", got)
	}
}

func TestApplyConstantBinaryOp_AllOps(t *testing.T) {
	tests := []struct {
		op     string
		l, r   float64
		want   float64
		wantOK bool
	}{
		{"+", 3, 4, 7, true},
		{"-", 10, 4, 6, true},
		{"*", 6, 7, 42, true},
		{"/", 10, 4, 2.5, true},
		{"/", 10, 0, 0, false}, // div-by-zero
		{"%", 10, 4, 0, false}, // unsupported op
		{"", 1, 1, 0, false},   // empty op
	}
	for _, tc := range tests {
		got, ok := applyConstantBinaryOp(tc.l, tc.r, tc.op)
		if ok != tc.wantOK {
			t.Errorf("op=%q: want ok=%v got ok=%v", tc.op, tc.wantOK, ok)
		}
		if ok && math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("op=%q: want %v got %v", tc.op, tc.want, got)
		}
	}
}

// --- applyMatrixStddevAgg ---

func TestApplyMatrixStddevAgg_PassesThroughOnDecodeError(t *testing.T) {
	bad := []byte(`not-json`)
	if got := applyMatrixStddevAgg(bad, "stddev"); string(got) != string(bad) {
		t.Fatalf("non-JSON input must pass through unchanged, got %s", got)
	}
}

func TestApplyMatrixStddevAgg_PassesThroughOnNonMatrix(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`)
	if got := applyMatrixStddevAgg(body, "stddev"); string(got) != string(body) {
		t.Fatalf("vector input must pass through unchanged")
	}
}

func TestApplyMatrixStddevAgg_PassesThroughOnNonSuccess(t *testing.T) {
	body := []byte(`{"status":"error","data":{"resultType":"matrix","result":[]}}`)
	if got := applyMatrixStddevAgg(body, "stddev"); string(got) != string(body) {
		t.Fatalf("non-success input must pass through unchanged")
	}
}

func TestApplyMatrixStddevAgg_ComputesPerTimestamp(t *testing.T) {
	// Two series, three timestamps; at ts=10 both have the same value (stddev=0),
	// at ts=20 they diverge.
	in := []byte(`{
	  "status":"success",
	  "data":{"resultType":"matrix","result":[
	    {"metric":{"app":"a"},"values":[[10,"1"],[20,"4"],[30,"4"]]},
	    {"metric":{"app":"b"},"values":[[10,"1"],[20,"8"],[30,"4"]]}
	  ]}
	}`)
	out := applyMatrixStddevAgg(in, "stddev")

	var parsed struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output not valid JSON: %v\nbody=%s", err, out)
	}
	if parsed.Data.ResultType != "matrix" {
		t.Fatalf("expected matrix output, got %s", parsed.Data.ResultType)
	}
	if got := len(parsed.Data.Result); got != 1 {
		t.Fatalf("expected exactly one aggregated series, got %d", got)
	}
	if got := len(parsed.Data.Result[0].Values); got != 3 {
		t.Fatalf("expected 3 timestamp samples, got %d", got)
	}
}

func TestApplyMatrixStddevAgg_StdvarBranch(t *testing.T) {
	in := []byte(`{
	  "status":"success",
	  "data":{"resultType":"matrix","result":[
	    {"metric":{"app":"a"},"values":[[10,"2"]]},
	    {"metric":{"app":"b"},"values":[[10,"6"]]}
	  ]}
	}`)
	out := applyMatrixStddevAgg(in, "stdvar")
	// populationVariance({2,6}) = ((2-4)^2 + (6-4)^2)/2 = 4
	if want := `"4"`; !strings.Contains(string(out), want) {
		t.Fatalf("expected stdvar=4 in output, got %s", out)
	}
}

// --- applyInstantStddevAgg ---

func TestApplyInstantStddevAgg_PassesThroughOnDecodeError(t *testing.T) {
	bad := []byte(`not-json`)
	if got := applyInstantStddevAgg(bad, "stddev"); string(got) != string(bad) {
		t.Fatalf("non-JSON input must pass through unchanged, got %s", got)
	}
}

func TestApplyInstantStddevAgg_PassesThroughOnNonVector(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	if got := applyInstantStddevAgg(body, "stddev"); string(got) != string(body) {
		t.Fatalf("matrix input must pass through unchanged")
	}
}

func TestApplyInstantStddevAgg_ComputesAcrossSeries(t *testing.T) {
	// populationStddev({2,4,4,4,5,5,7,9}) = 2.0
	in := []byte(`{
	  "status":"success",
	  "data":{"resultType":"vector","result":[
	    {"metric":{"a":"x"},"value":[10,"2"]},
	    {"metric":{"a":"y"},"value":[10,"4"]},
	    {"metric":{"a":"z"},"value":[10,"4"]},
	    {"metric":{"a":"w"},"value":[10,"4"]},
	    {"metric":{"a":"v"},"value":[10,"5"]},
	    {"metric":{"a":"u"},"value":[10,"5"]},
	    {"metric":{"a":"t"},"value":[10,"7"]},
	    {"metric":{"a":"s"},"value":[10,"9"]}
	  ]}
	}`)
	out := applyInstantStddevAgg(in, "stddev")
	if !strings.Contains(string(out), `"2"`) {
		t.Fatalf("expected stddev=2 in output, got %s", out)
	}
}

func TestApplyInstantStddevAgg_StdvarBranch(t *testing.T) {
	in := []byte(`{
	  "status":"success",
	  "data":{"resultType":"vector","result":[
	    {"metric":{},"value":[10,"2"]},
	    {"metric":{},"value":[10,"6"]}
	  ]}
	}`)
	out := applyInstantStddevAgg(in, "stdvar")
	if !strings.Contains(string(out), `"4"`) {
		t.Fatalf("expected stdvar=4 in output, got %s", out)
	}
}
