package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// A label_replace around a bare range function yields the series of the
// function under it (pkg/logql/evaluator.go LabelReplaceEvaluator passes the
// series through): one per stream on an instant query too. The grouping by the
// stream was added only for a query that itself starts with the function, so an
// instant `label_replace(count_over_time(...))` came back as one unlabeled
// total.
// conformance: semantics/label-replace-new-label, loki_api_v1_query, loki_api_v1_query_range
func TestLabelReplaceOverBareRangeFunctionKeepsOneSeriesPerStream(t *testing.T) {
	streams := []string{`{app="api-gateway"}`, `{app="auth-service"}`, `{app="worker"}`}
	var statsQueries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		q := r.FormValue("query")
		statsQueries = append(statsQueries, q)
		grouped := strings.Contains(q, "stats by (_stream")
		var out []string
		if grouped {
			for i, s := range streams {
				app := strings.Split(s, `"`)[1]
				metric, _ := json.Marshal(map[string]string{"_stream": s, "app": app})
				if r.URL.Path == "/select/logsql/stats_query" {
					out = append(out, fmt.Sprintf(`{"metric":%s,"value":[1704067200,"%d"]}`, metric, i+1))
				} else {
					out = append(out, fmt.Sprintf(`{"metric":%s,"values":[[1704067200,"%d"]]}`, metric, i+1))
				}
			}
		} else if r.URL.Path == "/select/logsql/stats_query" {
			out = append(out, `{"metric":{},"value":[1704067200,"6"]}`)
		} else {
			out = append(out, `{"metric":{},"values":[[1704067200,"6"]]}`)
		}
		kind := "vector"
		if r.URL.Path != "/select/logsql/stats_query" {
			kind = "matrix"
		}
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":%q,"result":[%s]}}`, kind, strings.Join(out, ","))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	for _, fn := range []string{"count_over_time", "bytes_over_time", "rate"} {
		for _, endpoint := range []string{"query", "query_range"} {
			q := fmt.Sprintf(`label_replace(%s({env="production"}[5m]), "app_short", "$1", "app", "(.*)-.*")`, fn)
			a := lrAsk(t, p, endpoint, q)
			if a.code != 200 || len(a.series) != len(streams) {
				t.Errorf("%s %s: HTTP %d, %d series, want one per stream (%d)\n%s", endpoint, fn, a.code, len(a.series), len(streams), a.body)
				continue
			}
			for _, s := range a.series {
				if want := map[string]string{"api-gateway": "api", "auth-service": "auth", "worker": ""}[s.Metric["app"]]; s.Metric["app_short"] != want {
					t.Errorf("%s %s: app=%s app_short=%q, want %q", endpoint, fn, s.Metric["app"], s.Metric["app_short"], want)
				}
			}
		}
	}
	if len(statsQueries) == 0 {
		t.Fatal("no query reached the backend")
	}
}

// A label_replace around a metric over `| json` is answered by the evaluator
// that owns JSON parser errors, like the metric without it: a sample whose
// line is not JSON fails the query with Loki's pipeline error unless the
// pipeline drops `__error__`, and the transform is applied to what the
// evaluator answered. Through the general path the pipeline was ignored and
// the query answered 200 with every stream.
// conformance: semantics/label-replace-new-label, semantics/pipeline-error-text-parity, loki_api_v1_query, loki_api_v1_query_range
func TestLabelReplaceOverJSONMetricUsesTheOrderedJSONEvaluator(t *testing.T) {
	var rowQueries, statsQueries atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			rowQueries.Add(1)
			fmt.Fprintln(w, `{"_time":"2024-01-01T00:00:30Z","_msg":"{\"k\":1}","_stream":"{app=\"api-gateway\"}","app":"api-gateway"}`)
			fmt.Fprintln(w, `{"_time":"2024-01-01T00:00:30Z","_msg":"{\"k\":1}","_stream":"{app=\"auth-service\"}","app":"auth-service"}`)
			fmt.Fprintln(w, `{"_time":"2024-01-01T00:00:30Z","_msg":"not json","_stream":"{app=\"worker\"}","app":"worker"}`)
		case "/select/logsql/stats_query", "/select/logsql/stats_query_range":
			statsQueries.Add(1)
			fmt.Fprintln(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
		default:
			fmt.Fprintln(w, `{}`)
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	ask := func(endpoint, filter string) lrAnswer {
		q := fmt.Sprintf(`label_replace(sum by (app) (count_over_time({env="production"} | json %s [5m])), "app_short", "$1", "app", "(.*)-.*")`, filter)
		return lrAsk(t, p, endpoint, q)
	}
	for _, endpoint := range []string{"query", "query_range"} {
		rowQueries.Store(0)
		statsQueries.Store(0)
		failed := ask(endpoint, "")
		if failed.code != 400 || !strings.Contains(failed.body, "JSONParserErr") {
			t.Errorf("%s without a filter: HTTP %d, want Loki's 400 JSONParserErr\n%s", endpoint, failed.code, failed.body)
		}
		if rowQueries.Load() == 0 || statsQueries.Load() != 0 {
			t.Errorf("%s: the ordered JSON evaluator was not used (row queries %d, stats queries %d)", endpoint, rowQueries.Load(), statsQueries.Load())
		}
		ok := ask(endpoint, `| __error__=""`)
		if ok.code != 200 || len(ok.series) != 2 {
			t.Errorf("%s with __error__=\"\": HTTP %d, %d series, want the two streams that parse\n%s", endpoint, ok.code, len(ok.series), ok.body)
			continue
		}
		for _, s := range ok.series {
			if want := map[string]string{"api-gateway": "api", "auth-service": "auth"}[s.Metric["app"]]; want == "" || s.Metric["app_short"] != want {
				t.Errorf("%s: app=%q app_short=%q, want the label_replace destination", endpoint, s.Metric["app"], s.Metric["app_short"])
			}
		}
	}
}
