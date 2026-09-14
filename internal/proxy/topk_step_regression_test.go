package proxy

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestTopK_RangeWinnersChangeAtEachStep(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1,"10"],[2,"-9"]]},{"metric":{"app":"b"},"values":[[1,"-5"],[2,"-1"]]},{"metric":{"app":"c"},"values":[[1,"NaN"],[2,"NaN"]]}]}}`)
	for _, descending := range []bool{true, false} {
		name := "bottomk"
		if descending {
			name = "topk"
		}
		if got := applyMatrixPostAggregation(input, instantMetricPostAgg{name: name, k: 1}); string(got) != string(applyTopKToMatrix(input, 1, descending)) {
			t.Fatal("query_range post-aggregation bypasses per-step ranking")
		}
		var response struct {
			Data struct {
				Result []struct {
					Metric map[string]string
					Values [][]any
				}
			}
		}
		if err := json.Unmarshal(applyTopKToMatrix(input, 1, descending), &response); err != nil {
			t.Fatal(err)
		}
		want := map[string]float64{"a": 1, "b": 2}
		if !descending {
			want = map[string]float64{"a": 2, "b": 1}
		}
		got := map[string]float64{}
		for _, series := range response.Data.Result {
			if len(series.Values) != 1 {
				t.Fatalf("losing samples retained: %+v", series)
			}
			got[series.Metric["app"]] = series.Values[0][0].(float64)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("descending=%v got=%v want=%v", descending, got, want)
		}
	}
}

func TestTopK_EmptyWindowsCannotBecomeWinners(t *testing.T) {
	start := time.Unix(1700000000, 0)
	series := map[string]manualSeriesSamples{
		"a": {Metric: map[string]string{"app": "a"}, Samples: []rangeMetricSample{
			{ts: start.UnixNano(), value: 0}, // VL can emit its own zero buckets.
			{ts: start.Add(time.Minute).UnixNano(), value: 9},
		}},
		"b": {Metric: map[string]string{"app": "b"}, Samples: []rangeMetricSample{
			{ts: start.Add(time.Minute).UnixNano(), value: 1},
		}},
	}
	for _, descending := range []bool{true, false} {
		body := buildHitsRangeMetricMatrixWithFill("count_over_time", series, start, start.Add(4*time.Minute), time.Minute, time.Minute, false)
		var response struct {
			Data struct {
				Result []struct {
					Metric map[string]string
					Values [][]any
				}
			}
		}
		if err := json.Unmarshal(applyTopKToMatrix(body, 1, descending), &response); err != nil {
			t.Fatal(err)
		}
		want := "a"
		if !descending {
			want = "b"
		}
		if len(response.Data.Result) != 1 {
			t.Fatalf("empty windows created extra winners: %+v", response.Data.Result)
		}
		got := response.Data.Result[0]
		if got.Metric["app"] != want || len(got.Values) != 1 || got.Values[0][0] != float64(start.Add(2*time.Minute).Unix()) {
			t.Fatalf("descending=%v unexpected winner/samples: %+v", descending, got)
		}
	}
	// Ordinary Drilldown charts still retain their full requested time axis.
	var chart struct {
		Data struct {
			Result []struct{ Values [][]any }
		}
	}
	if err := json.Unmarshal(buildHitsRangeMetricMatrix("count_over_time", series, start, start.Add(4*time.Minute), time.Minute, time.Minute), &chart); err != nil {
		t.Fatal(err)
	}
	for _, result := range chart.Data.Result {
		if len(result.Values) != 5 {
			t.Fatalf("chart lost its full time axis: %+v", result.Values)
		}
	}
}
