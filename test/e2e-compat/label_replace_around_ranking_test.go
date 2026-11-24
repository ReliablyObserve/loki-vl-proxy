//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

// label_replace calls around a ranking rewrite the labels of the series the ranking answers
// (semantics/label-replace-around-ranking-and-vector): topk, bottomk, sort and sort_desc, under an aggregation, two
// calls, and a call that gives every ranked series one label set (a range answer holds each ranked sample of a step
// in that one series). The proxy answered a 400 on query and an empty streams answer on query_range.
//
// Every shape is asked of Loki and of the proxy on query_range and on query. Both must answer the same status and,
// when they answer, the same result type, the same label sets and, for each label set, the same samples at each step
// (in the order Loki writes them for a ranking at the top). The fixture's apps log at 5, 10 and 15 seconds, so no two
// counts of a window tie.
// conformance: semantics/label-replace-around-ranking-and-vector, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_LabelReplaceAroundRanking(t *testing.T) {
	fx := ensureLabelReplaceFixture(t)
	agg := fmt.Sprintf(`sum by (app) (count_over_time({probe=%q}[5m]))`, fx.probe)
	lr := func(inner, dst, repl, src, re string) string {
		return fmt.Sprintf(`label_replace(%s, %q, %q, %q, %q)`, inner, dst, repl, src, re)
	}
	shapes := []struct {
		name, query string
		// ordered compares the order of the samples of one label set at a step (a ranking at the top decides it).
		ordered bool
	}{
		{"topk", lr(`topk(2, `+agg+`)`, "app_short", "$1", "app", "(.*)-.*"), true},
		{"bottomk", lr(`bottomk(1, `+agg+`)`, "short", "$1", "app", "(.)"), true},
		{"sort_desc", lr(`sort_desc(`+agg+`)`, "a", "x-$1", "app", "(.*)"), true},
		{"sort, rewrites the ranked label", lr(`sort(`+agg+`)`, "app", "x-$1", "app", "(api.*|web)"), true},
		{"under sum", lr(`sum(topk(2, `+agg+`))`, "all", "yes", "", ""), false},
		{"two calls", lr(lr(`topk(2, `+agg+`)`, "a", "$1", "app", "(.*)"), "b", "x$1", "a", "(.*)"), true},
		{"one label set for every ranked series", lr(`topk(2, `+agg+`)`, "app", "", "", ""), true},
		{"over a binary operation", lr(`topk(2, `+agg+` / 2)`, "a", "$1", "app", "(.*)"), true},
		{"invalid regexp", lr(`topk(2, `+agg+`)`, "a", "$1", "app", "("), false},
	}
	// samples reads a decoded answer as {label set: the samples of each timestamp, in answer order}.
	samples := func(payload map[string]interface{}, ordered bool) map[string]string {
		out := map[string][]string{}
		data, _ := payload["data"].(map[string]interface{})
		result, _ := data["result"].([]interface{})
		for _, item := range result {
			series, _ := item.(map[string]interface{})
			metric, _ := series["metric"].(map[string]interface{})
			key := canonicalMetricKey(metric)
			points, _ := series["values"].([]interface{})
			if value, ok := series["value"].([]interface{}); ok {
				points = []interface{}{value}
			}
			for _, p := range points {
				pair, _ := p.([]interface{})
				if len(pair) == 2 {
					out[key] = append(out[key], fmt.Sprint(pair[0])+"="+fmt.Sprint(pair[1]))
				}
			}
		}
		rendered := map[string]string{}
		for k, v := range out {
			if !ordered {
				sort.Strings(v)
			}
			rendered[k] = strings.Join(v, " ")
		}
		return rendered
	}
	for _, shape := range shapes {
		for _, endpoint := range []string{"query_range", "query"} {
			t.Run(shape.name+"/"+endpoint, func(t *testing.T) {
				instant := endpoint == "query"
				at := time.Now().UTC().Truncate(time.Minute)
				params := url.Values{"query": {shape.query}}
				if instant {
					params.Set("time", fmt.Sprintf("%d", at.UnixNano()))
				} else {
					params.Set("start", fmt.Sprintf("%d", at.Add(-15*time.Minute).UnixNano()))
					params.Set("end", fmt.Sprintf("%d", at.UnixNano()))
					params.Set("step", "60")
				}
				headers := map[string]string{"X-Scope-OrgID": "0"}
				full := "/loki/api/v1/" + endpoint + "?" + params.Encode()
				lokiStatus, lokiBody, lokiPayload := doJSONGET(t, lokiURL+full, headers)
				proxyStatus, proxyBody, proxyPayload := doJSONGET(t, proxyURL+full, headers)
				if lokiStatus != proxyStatus {
					t.Fatalf("HTTP status loki=%d proxy=%d\nloki:  %s\nproxy: %s", lokiStatus, proxyStatus, lokiBody, proxyBody)
				}
				if lokiStatus != http.StatusOK {
					if !strings.Contains(proxyBody, "invalid regex in label_replace") {
						t.Errorf("the proxy's error is not Loki's invalid regex error\nloki:  %s\nproxy: %s", lokiBody, proxyBody)
					}
					return
				}
				lokiData, _ := lokiPayload["data"].(map[string]interface{})
				proxyData, _ := proxyPayload["data"].(map[string]interface{})
				if lt, pt := lokiData["resultType"], proxyData["resultType"]; lt != pt {
					t.Fatalf("resultType loki=%v proxy=%v", lt, pt)
				}
				l, p := samples(lokiPayload, shape.ordered && !instant), samples(proxyPayload, shape.ordered && !instant)
				if len(l) == 0 {
					t.Fatalf("Loki answered no series for %s", shape.query)
				}
				for k, v := range l {
					if p[k] != v {
						t.Errorf("%s: loki %s\nproxy %s", k, v, p[k])
					}
				}
				for k := range p {
					if _, ok := l[k]; !ok {
						t.Errorf("the proxy answers a series Loki does not: %s", k)
					}
				}
			})
		}
	}
}
