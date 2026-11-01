package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestOrderedJSONVectorKeepsDirectCompilerContract(t *testing.T) {
	for _, query := range []string{
		`max(sum by(cluster)(rate({app="test"}|json[5m])))`,
		`topk(1,sum by(cluster)(count_over_time({app="test"}|json|__error__=""[5m])))`,
		`sum without(region)(avg by(cluster)(sum by(cluster,region)(rate({app="test"}|json[5m]))))`,
	} {
		if _, ok := compileOrderedJSONMetric(query); ok {
			t.Fatalf("nested query admitted by direct point-cache compiler: %s", query)
		}
		if queryStepsShape(query, time.Minute) {
			t.Fatalf("nested query admitted by generic settled-point cache: %s", query)
		}
		plan, outer, ok := orderedJSONVectorPlan(query)
		if !ok || len(outer) == 0 || plan.grouping == nil || plan.grouping.Labels[0] != "cluster" || plan.noLabels {
			t.Fatalf("lost innermost parser hints: %s plan=%+v outer=%v", query, plan, outer)
		}
	}
	for _, query := range []string{
		`sum by(cluster)(rate({app="test"}|json[5m]))`,
		`max(sum by(cluster)(rate({app="test"}|logfmt[5m])))`,
		`max(sum by(cluster)(rate({app="test"}|json|logfmt[5m])))`,
		`stddev(sum by(cluster)(rate({app="test"}|json[5m])))`,
		`max(sum by(cluster)(rate({app="test"}|json[5m] offset 1h)))`,
		`topk(0,sum by(cluster)(rate({app="test"}|json[5m])))`,
		`topk(-1,sum by(cluster)(rate({app="test"}|json[5m])))`,
	} {
		if _, _, ok := orderedJSONVectorPlan(query); ok {
			t.Fatalf("unexpected new ownership: %s", query)
		}
	}
}

// conformance: semantics/pipeline-error-text-parity
func TestOrderedJSONVectorErrorFilterAndRateUnits(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			fmt.Fprintln(w, `{}`)
			return
		}
		fmt.Fprintln(w, `{"_time":"2026-01-01T00:00:01Z","_msg":"invalid JSON","_stream":"{app=\"test\",cluster=\"east\"}"}`)
		fmt.Fprintln(w, `{"_time":"2026-01-01T00:00:01Z","_msg":"{\"message\":\"valid\"}","_stream":"{app=\"test\",cluster=\"east\"}"}`)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, mode := range []string{"query", "query_range"} {
		for _, wrap := range []string{"max(%s)", "avg(%s)", "topk(1,%s)", "bottomk(1,%s)", "sum without(cluster)(%s)"} {
			for _, filtered := range []bool{false, true} {
				pipeline := "|json"
				if filtered {
					pipeline += `|__error__=""`
				}
				query := fmt.Sprintf(wrap, `sum by(cluster)(rate({app="test"}`+pipeline+`[5m]))`)
				params := url.Values{"query": {query}, "time": {"2026-01-01T00:00:02Z"}, "start": {"2026-01-01T00:00:02Z"}, "end": {"2026-01-01T00:00:02Z"}, "step": {"60"}}
				result := doCompatProxyRequest(p, "/loki/api/v1/"+mode+"?"+params.Encode(), nil)
				if !filtered {
					if result.Code != 400 || !strings.Contains(result.Body.String(), "JSONParserErr") {
						t.Fatalf("%s %s status=%d body=%s", mode, query, result.Code, result.Body)
					}
				} else if result.Code != 200 || !strings.Contains(result.Body.String(), `"0.0033333333333333335"`) {
					t.Fatalf("lost filter or rate units: %s %s status=%d body=%s", mode, query, result.Code, result.Body)
				}
			}
		}
	}
}

func TestOrderedJSONVectorAppliesFinalSeriesLimit(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			fmt.Fprintln(w, `{}`)
			return
		}
		for _, cluster := range []string{"east", "west"} {
			fmt.Fprintf(w, "{\"_time\":\"2026-01-01T00:00:01Z\",\"_msg\":\"{\\\"message\\\":\\\"valid\\\"}\",\"_stream\":\"{app=\\\"test\\\",cluster=\\\"%s\\\"}\"}\n", cluster)
		}
	}))
	defer backend.Close()
	for _, mode := range []string{"query", "query_range"} {
		for _, tc := range []struct {
			wrap   string
			status int
		}{
			{"max(%s)", 200}, {"topk(1,%s)", 200}, {"sum by(cluster)(%s)", 400},
		} {
			p := newTestProxy(t, backend.URL)
			p.maxStatsQuerySeries = 1
			query := fmt.Sprintf(tc.wrap, `sum by(cluster)(rate({app="test"}|json|__error__=""[5m]))`)
			params := url.Values{"query": {query}, "time": {"2026-01-01T00:00:02Z"}, "start": {"2026-01-01T00:00:02Z"}, "end": {"2026-01-01T00:00:02Z"}, "step": {"60"}}
			result := doCompatProxyRequest(p, "/loki/api/v1/"+mode+"?"+params.Encode(), nil)
			if result.Code != tc.status {
				t.Fatalf("%s %s status=%d want=%d body=%s", mode, query, result.Code, tc.status, result.Body)
			}
			stats := p.queryTracker.TopByFrequency(2)
			wantErrors := int64(0)
			if tc.status >= 400 {
				wantErrors = 1
			}
			if p.queryTracker.TotalQueries.Load() != 1 || len(stats) != 1 || stats[0].Count != 1 || stats[0].Errors != wantErrors || stats[0].Query != query {
				t.Fatalf("final answer tracking: total=%d stats=%+v", p.queryTracker.TotalQueries.Load(), stats)
			}
			metricReply := httptest.NewRecorder()
			p.metrics.Handler(metricReply, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			requests, durations := 0, 0
			for _, line := range strings.Split(metricReply.Body.String(), "\n") {
				if !strings.Contains(line, `endpoint="`+mode+`"`) || !strings.Contains(line, `direction="downstream"`) {
					continue
				}
				if strings.HasSuffix(line, " 0") { // Request metrics preseed fixed status labels.
					continue
				}
				if strings.HasPrefix(line, "loki_vl_proxy_requests_total{") {
					requests++
					if !strings.Contains(line, fmt.Sprintf(`status="%d"`, tc.status)) || !strings.HasSuffix(line, " 1") {
						t.Fatalf("wrong final status metric: %s", line)
					}
				}
				if strings.HasPrefix(line, "loki_vl_proxy_request_duration_seconds_count{") {
					durations++
					if !strings.HasSuffix(line, " 1") {
						t.Fatalf("duplicate duration metric: %s", line)
					}
				}
			}
			if requests != 1 || durations != 1 {
				t.Fatalf("request families missing: requests=%d durations=%d", requests, durations)
			}
		}
	}
}

func TestOrderedJSONVectorDrilldownHistogramIsFolded(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			fmt.Fprintln(w, `{}`)
			return
		}
		for _, cluster := range []string{"east", "west"} {
			fmt.Fprintf(w, "{\"_time\":\"2026-01-01T00:00:01Z\",\"_msg\":\"{\\\"message\\\":\\\"valid\\\"}\",\"_stream\":\"{app=\\\"test\\\",cluster=\\\"%s\\\"}\"}\n", cluster)
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	query := `max(sum by(cluster)(count_over_time({app="test"}|json|drop __error__,__error_details__|cluster!=""[1m])))`
	params := url.Values{"query": {query}, "start": {"2026-01-01T00:00:02Z"}, "end": {"2026-01-01T00:00:02Z"}, "step": {"60"}}
	result := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"})
	if result.Code != 200 || lokiResultSeriesCount(result.Body.Bytes()) != 1 || !strings.Contains(result.Body.String(), `"metric":{}`) || !strings.Contains(result.Body.String(), `"1"`) {
		t.Fatalf("nested histogram fell through without its outer fold: %d %s", result.Code, result.Body)
	}
}
