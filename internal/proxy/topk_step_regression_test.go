package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
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

func TestTopK_BytesRankingKeepsRealZeroAndUsesStats(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stats_query_range" {
			http.NotFound(w, r)
			return
		}
		if !strings.Contains(r.FormValue("query"), "count() as __sample_count") {
			t.Error("byte ranking did not request presence counts")
		}
		w.Header().Set("Content-Type", "application/json")
		// Presence may arrive before the byte metric; zero-filled buckets are
		// not evidence of input, while an actual empty line must remain eligible.
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[
{"metric":{"__name__":"__sample_count","app":"empty"},"values":[[1700000000,"0"],[1700000300,"1"],[1700000600,"0"]]},
{"metric":{"__name__":"c","app":"empty"},"values":[[1700000000,"0"],[1700000300,"0"],[1700000600,"0"]]},
{"metric":{"__name__":"c","app":"full"},"values":[[1700000000,"0"],[1700000300,"6000"],[1700000600,"0"]]},
{"metric":{"__name__":"__sample_count","app":"full"},"values":[[1700000000,"0"],[1700000300,"1"],[1700000600,"0"]]}
]}}`)
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
	p.maxStatsQuerySeries = 2                   // two logical series, four upstream metrics
	for _, tc := range []struct{ op, app, value string }{{"topk", "full", "20"}, {"bottomk", "empty", "0"}} {
		q := url.Values{"query": {tc.op + `(1, sum by(app)(bytes_rate({app=~".+"}[5m])))`}, "start": {"1700000000"}, "end": {"1700000900"}, "step": {"300"}}
		r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
		w := httptest.NewRecorder()
		p.handleQueryRange(w, r)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", tc.op, w.Code, w.Body)
		}
		var response struct {
			Data struct {
				Result []struct {
					Metric map[string]string
					Values [][]any
				}
			}
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Data.Result) != 1 {
			t.Fatalf("%s unexpected series: %s", tc.op, w.Body)
		}
		got := response.Data.Result[0]
		if got.Metric["app"] != tc.app || len(got.Values) != 1 || got.Values[0][0] != float64(1700000600) || got.Values[0][1] != tc.value {
			t.Fatalf("%s wrong values/presence: %s", tc.op, w.Body)
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
		body := mustBuildHitsRangeMetricMatrix(t, "count_over_time", series, start, start.Add(4*time.Minute), time.Minute, time.Minute)
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
	// Plain range queries follow Loki too: a step whose window (t-1m, t] holds
	// no log line is absent, not zero. Loki's range-vector evaluator emits no
	// sample for an empty window, so zero-filling here invented points that
	// Loki never returns (for Explore, dashboards and Drilldown alike).
	var chart struct {
		Data struct {
			Result []struct{ Values [][]any }
		}
	}
	if err := json.Unmarshal(mustBuildHitsRangeMetricMatrix(t, "count_over_time", series, start, start.Add(4*time.Minute), time.Minute, time.Minute), &chart); err != nil {
		t.Fatal(err)
	}
	for _, result := range chart.Data.Result {
		if len(result.Values) != 1 || result.Values[0][0] != float64(start.Add(2*time.Minute).Unix()) {
			t.Fatalf("chart must keep only the step whose window holds lines: %+v", result.Values)
		}
	}
}
