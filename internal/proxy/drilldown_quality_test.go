package proxy

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestZerofillStatsMatrix_FillsGaps(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"42"],[300,"17"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, got)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(resp.Data.Result))
	}
	vals := resp.Data.Result[0].Values
	if len(vals) != 3 {
		t.Fatalf("expected 3 values (100,200,300), got %d: %v", len(vals), vals)
	}
	ts200 := vals[1]
	if len(ts200) < 2 {
		t.Fatalf("expected [ts,val] pair at index 1, got %v", ts200)
	}
	tsVal, _ := ts200[0].(float64)
	countVal, _ := ts200[1].(string)
	if int64(tsVal) != 200 {
		t.Errorf("expected timestamp 200, got %v", ts200[0])
	}
	if countVal != "0" {
		t.Errorf("expected zero-filled count '0', got %q", countVal)
	}
}

func TestZerofillStatsMatrix_Passthrough_WhenNoGaps(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"10"],[200,"20"],[300,"30"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	vals := resp.Data.Result[0].Values
	if len(vals) != 3 {
		t.Fatalf("expected 3 values unchanged, got %d", len(vals))
	}
}

func TestZerofillStatsMatrix_MultiSeries(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"50"],[300,"30"]]},` +
		`{"metric":{"level":"error"},"values":[[200,"5"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, got)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected 2 series, got %d", len(resp.Data.Result))
	}
	for _, s := range resp.Data.Result {
		if len(s.Values) != 3 {
			t.Errorf("series %v: expected 3 values, got %d: %v", s.Metric, len(s.Values), s.Values)
		}
	}
}

func TestZerofillStatsMatrix_EmptyResult_Passthrough(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	got := zerofillStatsMatrix(input, 0, 300, 100)
	if string(got) != string(input) {
		t.Errorf("expected passthrough for empty result\ngot:  %s\nwant: %s", got, input)
	}
}

func TestZerofillStatsMatrix_ZeroStep_Passthrough(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"1"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 0)
	if string(got) != string(input) {
		t.Errorf("expected passthrough when stepSec=0\ngot: %s", got)
	}
}

func TestZerofillStatsMatrix_PreservesMetricKey(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"detected_level":"info"},"values":[[200,"9"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)
	if !bytes.Contains(got, []byte(`"detected_level":"info"`)) {
		t.Errorf("metric key lost after zerofill\nbody: %s", got)
	}
	if !bytes.Contains(got, []byte(`[100,"0"]`)) {
		t.Errorf("ts=100 not zero-filled\nbody: %s", got)
	}
	if !bytes.Contains(got, []byte(`[300,"0"]`)) {
		t.Errorf("ts=300 not zero-filled\nbody: %s", got)
	}
}
