package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSumAllLogRangeUsesStatsFastPath(t *testing.T) {
	stamp := time.Unix(1700000400, 0)
	for _, function := range []string{"rate", "bytes_rate", "count_over_time", "bytes_over_time"} {
		for _, format := range []string{"sum(%s)", "sum by () (%s)", "topk(1, sum(%s))"} {
			t.Run(function+"/"+format, func(t *testing.T) {
				calls := 0
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/select/logsql/stats_query_range" {
						t.Errorf("aggregate-all must retain stats fast path, got %s", r.URL.Path)
						http.Error(w, "unexpected endpoint", http.StatusInternalServerError)
						return
					}
					calls++
					if strings.Contains(r.FormValue("query"), "stats by") {
						t.Errorf("intermediate stream grouping retained: %s", r.FormValue("query"))
					}
					fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c"},"values":[[%d,"2"]]}]}}`, stamp.Add(-time.Minute).Unix())
				}))
				defer backend.Close()
				p := newGapTestProxy(t, backend.URL)
				query := fmt.Sprintf(format, function+`({app="api"}[5m])`)
				params := url.Values{"query": {query}, "start": {stamp.UTC().Format(time.RFC3339Nano)}, "end": {stamp.UTC().Format(time.RFC3339Nano)}, "step": {"60"}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusOK || calls != 1 {
					t.Fatalf("status=%d stats calls=%d body=%s", rec.Code, calls, rec.Body.String())
				}
				var response struct {
					Data struct {
						Result []struct {
							Metric map[string]string
							Values [][]interface{}
						}
					}
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if len(response.Data.Result) != 1 || len(response.Data.Result[0].Metric) != 0 || len(response.Data.Result[0].Values) != 1 {
					t.Fatalf("expected one aggregate-all sample: %s", rec.Body.String())
				}
				point := response.Data.Result[0].Values[0]
				want := 2.0
				if function == "rate" || function == "bytes_rate" {
					want /= 300
				}
				value, err := strconv.ParseFloat(fmt.Sprint(point[1]), 64)
				if err != nil || math.Abs(value-want) > 1e-9 || point[0] != float64(stamp.Unix()) {
					t.Fatalf("got %v, want [%d %v]", point, stamp.Unix(), want)
				}
			})
		}
	}
}

func TestSumAllLogRangePreservesOtherAggregationSemantics(t *testing.T) {
	for _, query := range []string{
		`avg(rate({app="api"}[5m]))`, `sum by(app)(rate({app="api"}[5m]))`,
		`sum without()(rate({app="api"}[5m]))`, `sum(rate({app="api"} | unwrap count [5m]))`,
		`sum(quantile_over_time(0.5,{app="api"} | unwrap latency [5m]))`,
	} {
		if isSumAllLogRange(query) {
			t.Errorf("must not collapse %s", query)
		}
	}
}
