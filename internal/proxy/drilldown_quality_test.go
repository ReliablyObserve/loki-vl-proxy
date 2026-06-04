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

// TestSynthesizeDrilldownMatrixSpread verifies the high-cardinality hybrid
// path's stub spreading. The single-point synthesizeDrilldownMatrix (stepSec=0)
// produces a single dot at endSec, which Grafana renders as an empty-looking
// chart with one point at the right edge. The spread variant distributes total
// hits across drilldownSynthesizeBuckets points so the chart shows a populated
// band.
func TestSynthesizeDrilldownMatrixSpread(t *testing.T) {
	entries := []drilldownFVEntry{
		{Value: "v1", Hits: 600},
		{Value: "v2", Hits: 30},
	}

	t.Run("single point at endSec when stepSec=0", func(t *testing.T) {
		got := synthesizeDrilldownMatrix("trace_id", entries, 1000)
		// Both series should have exactly one point at ts=1000.
		// First series: hits=600 → value="600".
		if !bytes.Contains(got, []byte(`[1000,"600"]`)) {
			t.Errorf("expected single point at 1000 with value 600\nbody: %s", got)
		}
		if !bytes.Contains(got, []byte(`[1000,"30"]`)) {
			t.Errorf("expected single point at 1000 with value 30\nbody: %s", got)
		}
	})

	t.Run("spread across range when stepSec>0", func(t *testing.T) {
		// 1000s range, stepSec=10. drilldownSynthesizeBuckets=120 gives bucketStep
		// rangeSec/120 = 8s, which is below the step floor (10s) so bucketStep = 10s.
		// nPoints = 1000/10 + 1 = 101. perBucket for v1: 600/101 = 5.
		got := synthesizeDrilldownMatrixSpread("trace_id", entries, 0, 1000, 10)

		// Last point of v1 should land at ts=1000 with value=5.
		if !bytes.Contains(got, []byte(`[1000,"5"]`)) {
			t.Errorf("expected v1 last point at ts=1000 with value=5\nbody: %s", got)
		}
		// For v2 with 30 hits / 101 buckets = 0, floor to 1.
		if !bytes.Contains(got, []byte(`[1000,"1"]`)) {
			t.Errorf("expected v2 last point with value=1 (floored)\nbody: %s", got)
		}
	})

	t.Run("zero hits do not produce floor-to-1 phantom data", func(t *testing.T) {
		zeroEntries := []drilldownFVEntry{{Value: "trunc", Hits: 0}}
		got := synthesizeDrilldownMatrixSpread("trace_id", zeroEntries, 0, 1000, 10)
		// All values should be "0" — the floor only applies when hits>0.
		if bytes.Contains(got, []byte(`,"1"]`)) {
			t.Errorf("expected no '1' values for zero-hit entry\nbody: %s", got)
		}
		if !bytes.Contains(got, []byte(`,"0"]`)) {
			t.Errorf("expected zero values for truncated-hit entry\nbody: %s", got)
		}
	})

	t.Run("empty entries returns empty matrix", func(t *testing.T) {
		got := synthesizeDrilldownMatrixSpread("trace_id", nil, 0, 1000, 10)
		if !bytes.Contains(got, []byte(`"result":[]`)) {
			t.Errorf("expected empty result for nil entries\nbody: %s", got)
		}
	})
}

// TestZerofillStatsMatrix_AlignsStartToStepGrid verifies that when the request
// start timestamp is not a multiple of step, the axis is aligned UP to the next
// step boundary so that VL's step-aligned timestamps actually match the axis.
//
// Regression guard: previously, an unaligned startSec (e.g. 1780405844 mod 300 = 44)
// produced an axis at 1780405844, +300, +600 … which never matched VL's
// 1780406100, 1780406400 … grid, so every VL data point was silently dropped and
// replaced by a zero-fill entry. The proxy reported a "non-sparse" series with
// every value "0".
func TestZerofillStatsMatrix_AlignsStartToStepGrid(t *testing.T) {
	// VL returns step-aligned timestamps (100, 200, 300). Request range starts
	// at 44 (not a multiple of 100) and ends at 344. The axis must align to
	// the step grid so VL's [100,"42"] and [300,"17"] are preserved.
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"api"},"values":[[100,"42"],[300,"17"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 44, 344, 100)
	if !bytes.Contains(got, []byte(`[100,"42"]`)) {
		t.Errorf("VL data point at ts=100 lost (overwritten by zero-fill)\nbody: %s", got)
	}
	if !bytes.Contains(got, []byte(`[300,"17"]`)) {
		t.Errorf("VL data point at ts=300 lost (overwritten by zero-fill)\nbody: %s", got)
	}
	if !bytes.Contains(got, []byte(`[200,"0"]`)) {
		t.Errorf("zero-fill for missing ts=200 not present\nbody: %s", got)
	}
	// Ensure no garbage points outside the step grid (e.g. ts=44 or ts=344).
	if bytes.Contains(got, []byte(`[44,`)) || bytes.Contains(got, []byte(`[344,`)) {
		t.Errorf("axis includes unaligned timestamps\nbody: %s", got)
	}
}
