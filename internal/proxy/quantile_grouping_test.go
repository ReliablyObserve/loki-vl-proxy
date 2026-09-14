package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Loki's range_vector.go uses (evaluation-window, evaluation] and interpolates
// adjacent ranked samples. Distinct message/stream identities must merge before
// the quantile is calculated when the query has an explicit range grouping.
func TestGroupedQuantileExactWindows(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	for _, tc := range []struct {
		grouping string
		want     map[string][]string
	}{
		{"by (level)", map[string][]string{"info": {"30", "50", "50", "50", "70", "90"}, "error": {"150", "200", "200", "200", "300", "400"}}},
		{"by ()", map[string][]string{"": {"50", "95", "95", "95", "145", "245"}}},
	} {
		for _, step := range []int{60, 300, 600} {
			t.Run(fmt.Sprintf("%s/step=%d", tc.grouping, step), func(t *testing.T) {
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/select/logsql/query" {
						t.Errorf("unexpected backend endpoint %s", r.URL.Path)
						http.Error(w, "unexpected endpoint", http.StatusInternalServerError)
						return
					}
					for i, sample := range []struct {
						sec, value int
						level      string
					}{
						{-300, 1000, "info"}, // excluded lower boundary at first evaluation
						{-240, 10, "info"}, {-60, 30, "info"}, {0, 50, "info"}, {60, 90, "info"},
						{-60, 100, "error"}, {0, 200, "error"}, {60, 400, "error"},
					} {
						// Model VL's exclusive end instead of returning samples that
						// the real backend would exclude from an instant lookback.
						fetchEnd, err := parseTimestamp(r.FormValue("end"))
						if err != nil {
							t.Errorf("invalid backend end %q: %v", r.FormValue("end"), err)
							return
						}
						if !base.Add(time.Duration(sample.sec) * time.Second).Before(fetchEnd) {
							continue
						}
						_ = json.NewEncoder(w).Encode(map[string]interface{}{
							"_time":   base.Add(time.Duration(sample.sec) * time.Second).Format(time.RFC3339Nano),
							"_stream": fmt.Sprintf(`{app="api",pod="pod-%d",level=%q}`, i, sample.level),
							"_msg":    fmt.Sprintf(`{"latency":%d}`, sample.value), "latency": sample.value, "level": sample.level,
						})
					}
				}))
				defer backend.Close()
				p := newGapTestProxy(t, backend.URL)
				params := url.Values{
					"query": {`quantile_over_time(0.5, {app="api"} | json | unwrap latency [5m]) ` + tc.grouping},
					"start": {base.Format(time.RFC3339Nano)}, "end": {base.Add(10 * time.Minute).Format(time.RFC3339Nano)},
					"step": {strconv.Itoa(step)},
				}
				for repeat := 0; repeat < 2; repeat++ {
					rec := httptest.NewRecorder()
					p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
					if rec.Code != http.StatusOK {
						t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
					}
					var response struct {
						Data struct {
							Result []struct {
								Metric map[string]string `json:"metric"`
								Values [][]interface{}   `json:"values"`
							} `json:"result"`
						} `json:"data"`
					}
					if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
						t.Fatal(err)
					}
					if len(response.Data.Result) != len(tc.want) {
						t.Fatalf("wrong series: %s", rec.Body.String())
					}
					for _, result := range response.Data.Result {
						level := result.Metric["level"]
						if _, ok := tc.want[level]; !ok {
							t.Fatalf("unexpected group %q", level)
						}
						wantMetric := map[string]string{}
						if level != "" {
							wantMetric["level"] = level
						}
						if !reflect.DeepEqual(result.Metric, wantMetric) {
							t.Fatalf("unrequested labels: %v", result.Metric)
						}
						var want [][]interface{}
						for sec := 0; sec < 360; sec += step {
							want = append(want, []interface{}{float64(base.Unix() + int64(sec)), tc.want[level][sec/60]})
						}
						if !reflect.DeepEqual(result.Values, want) {
							t.Fatalf("%s points got %v, want %v", level, result.Values, want)
						}
					}
				}
				// The instant endpoint must use the same interpolated quantile and
				// grouping as the first range evaluation.
				params.Set("time", base.Format(time.RFC3339Nano))
				rec := httptest.NewRecorder()
				p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
				if rec.Code != http.StatusOK {
					t.Fatalf("instant status %d: %s", rec.Code, rec.Body.String())
				}
				var instant struct {
					Data struct {
						Result []struct {
							Metric map[string]string `json:"metric"`
							Value  []interface{}     `json:"value"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &instant); err != nil {
					t.Fatal(err)
				}
				if len(instant.Data.Result) != len(tc.want) {
					t.Fatalf("wrong instant groups: %s", rec.Body.String())
				}
				for _, result := range instant.Data.Result {
					level := result.Metric["level"]
					values, ok := tc.want[level]
					if !ok || len(result.Metric) > 1 {
						t.Fatalf("unexpected instant group %v", result.Metric)
					}
					want := []interface{}{float64(base.Unix()), values[0]}
					if !reflect.DeepEqual(result.Value, want) {
						t.Fatalf("instant %s got %v, want %v", level, result.Value, want)
					}
				}
			})
		}
	}
}
