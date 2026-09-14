package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// fakeAggregationVL answers stats_query like VictoriaLogs: a grouped final stats
// pipe yields no rows over an empty input, while an ungrouped one always yields
// one row per function (count()=0, max()/min()="", everything else NaN).
func fakeAggregationVL(t *testing.T, populated bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		query := r.FormValue("query")
		idx := strings.LastIndex(query, "| stats ")
		if r.URL.Path != "/select/logsql/stats_query" || idx < 0 {
			t.Errorf("unexpected upstream request %s %q", r.URL.Path, query)
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
			return
		}
		tail := query[idx+len("| stats "):]
		var series []string
		if strings.HasPrefix(tail, "by (") && !strings.HasPrefix(tail, "by ()") {
			if populated {
				for _, pod := range []string{"p0", "p1"} {
					series = append(series, fmt.Sprintf(`{"metric":{"pod":%q},"value":[1700000000,"0.01"]}`, pod))
				}
			}
		} else {
			for _, fn := range strings.Split(strings.TrimPrefix(tail, "by () "), ", ") {
				name := fn
				if i := strings.Index(fn, " as "); i >= 0 {
					name = fn[i+len(" as "):]
				}
				series = append(series, fmt.Sprintf(`{"metric":{"__name__":%q},"value":[1700000000,%q]}`, name, fakeAggregationValue(fn, populated)))
			}
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(series, ",") + `]}}`))
	}))
}

func fakeAggregationValue(fn string, populated bool) string {
	switch {
	case !populated && strings.HasPrefix(fn, "count("):
		return "0"
	case !populated && (strings.HasPrefix(fn, "max(") || strings.HasPrefix(fn, "min(")):
		return ""
	case !populated:
		return "NaN"
	case strings.HasPrefix(fn, "sum_len("):
		// A genuine zero: the window holds lines whose messages are empty.
		return "0"
	case strings.HasPrefix(fn, "count("):
		return "2"
	default:
		return "0.02"
	}
}

func instantAggregationResult(t *testing.T, p *Proxy, query string) []struct {
	Metric map[string]string `json:"metric"`
	Value  []json.RawMessage `json:"value"`
} {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?time=1700000000&query="+url.QueryEscape(query), nil)
	w := httptest.NewRecorder()
	p.handleQuery(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "vector" || resp.Data.Result == nil {
		t.Fatalf("invalid vector response (%v): %s", err, w.Body)
	}
	return resp.Data.Result
}

var emptyWindowAggregationQueries = map[string]float64{
	`sum(rate({app="api"}[5m]))`:                           0.02,
	`sum(bytes_rate({app="api"}[5m]))`:                     0.02,
	`sum(count_over_time({app="api"}[5m]))`:                2,
	`sum(bytes_over_time({app="api"}[5m]))`:                0,
	`sum by () (count_over_time({app="api"}[5m]))`:         2,
	`sum(count_over_time({app="api"} |= "x" [5m]))`:        2,
	`avg(rate({app="api"}[5m]))`:                           0.02,
	`max(rate({app="api"}[5m]))`:                           0.02,
	`count(rate({app="api"}[5m]))`:                         2,
	`stddev(rate({app="api"}[5m]))`:                        0,
	`stdvar(rate({app="api"}[5m]))`:                        0,
	`sum(count_over_time({app="api"}[5m])) / 2`:            1,
	`sum(rate({app="api"}[5m])) * 2`:                       0.04,
	`topk(3, sum(rate({app="api"}[5m])))`:                  0.02,
	`sort(sum(rate({app="api"}[5m])))`:                     0.02,
	`sum(sum by (pod) (count_over_time({app="api"}[5m])))`: 0.02,
}

// Loki aggregates an empty input vector into an empty vector; VictoriaLogs
// emits one zero or NaN row for an ungrouped stats pipe over no rows.
func TestInstantAggregationOverEmptyWindowReturnsEmptyVector(t *testing.T) {
	backend := fakeAggregationVL(t, false)
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for query := range emptyWindowAggregationQueries {
		t.Run(query, func(t *testing.T) {
			if result := instantAggregationResult(t, p, query); len(result) != 0 {
				t.Fatalf("want empty vector, got %+v", result)
			}
		})
	}
}

func TestInstantAggregationWithSamplesKeepsValue(t *testing.T) {
	backend := fakeAggregationVL(t, true)
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for query, want := range emptyWindowAggregationQueries {
		t.Run(query, func(t *testing.T) {
			result := instantAggregationResult(t, p, query)
			if len(result) != 1 || len(result[0].Metric) != 0 || len(result[0].Value) != 2 {
				t.Fatalf("want one unlabelled sample, got %+v", result)
			}
			var raw string
			if err := json.Unmarshal(result[0].Value[1], &raw); err != nil {
				t.Fatal(err)
			}
			got, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.Abs(got-want) > 1e-12 {
				t.Fatalf("value %q, want %g", raw, want)
			}
		})
	}
}

func TestEmptyInputGuardQueryRewrite(t *testing.T) {
	for query, want := range map[string]string{
		`app:="x" | stats count()`:                                  `app:="x" | stats count(), count() as __lvp_n`,
		`app:="x" | stats by () count()`:                            `app:="x" | stats by () count(), count() as __lvp_n`,
		`app:="x" | stats by (_stream) count() as n | stats sum(n)`: `app:="x" | stats by (_stream) count() as n | stats sum(n), count() as __lvp_n`,
		`app:="x" | stats by (pod) count()`:                         `app:="x" | stats by (pod) count()`,
		`app:="x" | stats by(pod) count()`:                          `app:="x" | stats by(pod) count()`,
		`app:="x" | stats count() | sort by (x)`:                    `app:="x" | stats count() | sort by (x)`,
		`app:="x"`:                                                  `app:="x"`,
	} {
		if got, _ := withEmptyInputGuard(query); got != want {
			t.Errorf("withEmptyInputGuard(%q) = %q, want %q", query, got, want)
		}
	}
}

func TestDropEmptyInputGuard(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"empty input": {
			in:   `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"count(*)"},"value":[1,"0"]},{"metric":{"__name__":"__lvp_n"},"value":[1,"0"]}]}}`,
			want: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		},
		"rows present": {
			in:   `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"sum(x)"},"value":[1,"0"]},{"metric":{"__name__":"__lvp_n"},"value":[1,"3"]}]}}`,
			want: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"sum(x)"},"value":[1,"0"]}]}}`,
		},
		"no guard": {
			in:   `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"sum(x)"},"value":[1,"NaN"]}]}}`,
			want: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"sum(x)"},"value":[1,"NaN"]}]}}`,
		},
		"invalid": {in: `not json`, want: `not json`},
	} {
		t.Run(name, func(t *testing.T) {
			got := dropEmptyInputGuard([]byte(tc.in))
			var gotValue, wantValue any
			if json.Unmarshal([]byte(tc.want), &wantValue) != nil {
				if string(got) != tc.want {
					t.Fatalf("got %s, want %s", got, tc.want)
				}
				return
			}
			if err := json.Unmarshal(got, &gotValue); err != nil || !reflect.DeepEqual(gotValue, wantValue) {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
