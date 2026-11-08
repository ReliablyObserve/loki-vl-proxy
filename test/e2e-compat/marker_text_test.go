//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCompat_MarkerTextInStrings: a line filter, a label filter value or a stream label value that holds text the
// proxy writes into its own translated queries (`__without__:`, `__lvp_lr:…__`) is matched as text, like Loki: the
// log query returns the lines, and a metric over it counts them, on query and query_range. The fixture is one stream
// of lines ingested alike into Loki and VictoriaLogs.
// conformance: semantics/marker-text-in-query-strings, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_MarkerTextInStrings(t *testing.T) {
	probe := fmt.Sprintf("marker-text-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	lines := []string{
		`{"msg":"saw __without__:pod in a log","k":"__without__:pod"}`,
		`{"msg":"plain line","k":"v"}`,
		`{"msg":"token __lvp_lr:YQ==__ here","k":"__lvp_lr:YQ==__"}`,
		`{"msg":"another __without__:pod","k":"x"}`,
	}
	var vlRows []string
	var values [][]string
	for i, line := range lines {
		ts := stamp.Add(time.Duration(i) * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "marker-text", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "marker-text", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	window := url.Values{
		"start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)},
		"limit": {"100"},
	}
	lineCount := func(resp map[string]interface{}) int {
		n := 0
		for _, s := range extractArray(extractMap(resp, "data"), "result") {
			v, _ := s.(map[string]interface{})["values"].([]interface{})
			n += len(v)
		}
		return n
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		params := url.Values{"query": {fmt.Sprintf(`{probe=%q}`, probe)}, "start": window["start"], "end": window["end"], "limit": window["limit"]}
		_, _, lp := queryRangeGET(t, lokiURL, params, headers)
		_, _, pp := queryRangeGET(t, proxyURL, params, headers)
		if lineCount(lp) == len(lines) && lineCount(pp) == len(lines) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", lineCount(lp), lineCount(pp))
		}
		time.Sleep(time.Second)
	}
	for _, tc := range []struct {
		expr string
		want int
	}{
		{`{probe=%q} |= "__without__:pod"`, 2},
		{`{probe=%q} |= "__lvp_lr:YQ==__"`, 1},
		{`{probe=%q} | json | k="__without__:pod"`, 1},
	} {
		query := fmt.Sprintf(tc.expr, probe)
		params := url.Values{"query": {query}, "start": window["start"], "end": window["end"], "limit": window["limit"]}
		ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		if lineCount(lp) != tc.want {
			t.Fatalf("%s: Loki answered %d lines, the fixture holds %d: %s", query, lineCount(lp), tc.want, lb)
		}
		if lineCount(pp) != lineCount(lp) {
			t.Errorf("%s: proxy %d lines, Loki %d", query, lineCount(pp), lineCount(lp))
		}
	}
	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	at := strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)
	for _, tc := range []struct{ expr, want string }{
		{`sum(count_over_time({probe=%q} |= "__without__:pod" [5m]))`, "2"},
		{`sum(count_over_time({probe=%q} |= "__lvp_lr:YQ==__" [5m]))`, "1"},
	} {
		query := fmt.Sprintf(tc.expr, probe)
		instant := url.Values{"query": {query}, "time": {at}}
		ls, lb, lp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pb, pp := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		lokiResult := extractArray(extractMap(lp, "data"), "result")
		if len(lokiResult) == 0 || lokiResult[0].(map[string]interface{})["value"].([]interface{})[1] != tc.want {
			t.Fatalf("%s: Loki answered %s, the fixture holds %s", query, lb, tc.want)
		}
		if !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), lokiResult) {
			t.Errorf("instant %s:\nproxy %s\n Loki %s", query, pb, lb)
		}
		rng := url.Values{"query": {query}, "start": {strconv.FormatInt(stamp.Add(-time.Minute).Unix(), 10)},
			"end": {strconv.FormatInt(stamp.Add(time.Minute).Unix(), 10)}, "step": {"15"}}
		ls, lb, lp = queryRangeGET(t, lokiURL, rng, headers)
		ps, pb, pp = queryRangeGET(t, proxyURL, rng, headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("range %s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		if l := extractArray(extractMap(lp, "data"), "result"); len(l) == 0 || !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), l) {
			t.Errorf("range %s:\nproxy %s\n Loki %s", query, pb, lb)
		}
	}
}
