package proxy

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestTopK_RangeWinnersChangeAtEachStep(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1,"10"],[2,"-9"]]},{"metric":{"app":"b"},"values":[[1,"-5"],[2,"-1"]]},{"metric":{"app":"c"},"values":[[1,"NaN"],[2,"NaN"]]}]}}`)
	for _, descending := range []bool{true, false} {
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
