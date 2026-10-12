package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// An instant `sum(sum_over_time(... | json | unwrap x [r]))` is one series
// without labels, as Loki's bare outer sum makes it, and VictoriaLogs answers it
// whole: the translated `stats by (_stream, _msg)` before the outer sum is not a
// label of the result, and no raw rows are read.
//
// conformance: profiles/sanitized-json-key-grouping
func TestInstantUnwrapSumCollapsesStreams(t *testing.T) {
	var statsQueries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			_ = r.ParseForm()
			statsQueries = append(statsQueries, r.FormValue("query"))
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"sum(__lvp_inner)"},"value":[1767229200,"401"]}]}}`))
		case "/select/logsql/query":
			t.Errorf("raw rows read for an ungrouped sum: %s", r.URL.RawQuery)
			_, _ = w.Write([]byte(`{}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	q := url.Values{"query": {`sum(sum_over_time({app="web"} | json | unwrap code [1h]))`}, "time": {"1767229200"}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query?"+q.Encode(), nil)
	if res.Code != http.StatusOK {
		t.Fatalf("%d %s", res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Metric) != 0 || resp.Data.Result[0].Value[1] != "401" {
		t.Fatalf("want one unlabelled series worth 401: %s", res.Body)
	}
	if len(statsQueries) != 1 || !strings.Contains(statsQueries[0], "| stats by (__lvp_bad) sum(__lvp_inner)") {
		t.Fatalf("want one stats query ending in the outer sum (grouped by the conversion counter): %v", statsQueries)
	}
}

// Every outer operator but the additive sum reads one value per stream, and
// folds those values into the one series without labels that Loki's bare
// aggregation makes (pkg/logql/evaluator.go VectorAggEvaluator.Next): the number
// of streams for count, their largest and mean value for max and avg.
//
// conformance: profiles/sanitized-json-key-grouping, semantics/non-sum-vector-aggregation-unreduced
func TestInstantUnwrapOtherAggregationsFoldPerStreamValues(t *testing.T) {
	rows := `{"_time":"2026-01-01T00:59:00Z","_msg":"{\"code\":200}","_stream":"{app=\"web\",pod=\"a\"}","app":"web","pod":"a","code":"200"}` + "\n" +
		`{"_time":"2026-01-01T00:59:01Z","_msg":"{\"code\":201}","_stream":"{app=\"web\",pod=\"b\"}","app":"web","pod":"b","code":"201"}` + "\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(rows))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for query, want := range map[string]string{
		`count(sum_over_time({app="web"} | json | unwrap code [1h]))`: "2",
		`max(sum_over_time({app="web"} | json | unwrap code [1h]))`:   "201",
		`avg(sum_over_time({app="web"} | json | unwrap code [1h]))`:   "200.5",
	} {
		q := url.Values{"query": {query}, "time": {"1767229200"}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query?"+q.Encode(), nil)
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, res.Code, res.Body)
		}
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Value  []any             `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Metric) != 0 || resp.Data.Result[0].Value[1] != want {
			t.Fatalf("%s: want one unlabelled series worth %s: %s", query, want, res.Body)
		}
	}
}
