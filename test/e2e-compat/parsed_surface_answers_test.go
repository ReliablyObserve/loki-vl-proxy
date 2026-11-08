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

// TestRangeMetricCompatibilityParsedSurfaceAnswers: three LogQL forms the parser accepts answer like Loki, not only
// parse: a line filter written after the range (`count_over_time({...}[1m] |= "error")`), ip() label filters on a
// parsed field (CIDR and range, = and !=), and topk over sum_over_time with unwrap as an instant query. The fixture
// is one stream of JSON lines ingested alike into Loki and VictoriaLogs.
// conformance: semantics/proxy-rejects-valid-logql, loki_api_v1_query_range, loki_api_v1_query
func TestRangeMetricCompatibilityParsedSurfaceAnswers(t *testing.T) {
	probe := fmt.Sprintf("parsed-surface-%d", time.Now().UnixNano())
	base := time.Now().Add(-20 * time.Minute).Truncate(time.Minute)
	var vlRows []string
	values := map[string][][]string{}
	for i := 0; i < 300; i += 3 {
		ts := base.Add(time.Duration(i) * time.Second)
		svc := []string{"a", "b", "c"}[i%3]
		msg := "ok"
		if i%4 == 0 {
			msg = "error timeout"
		} else if i%5 == 0 {
			msg = "error refused"
		}
		line, _ := json.Marshal(map[string]any{"msg": msg, "ip": fmt.Sprintf("10.%d.0.%d", i%4, i%250), "duration_ms": i % 97})
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": string(line), "app": "parsed-surface", "probe": probe, "svc": svc})
		vlRows = append(vlRows, string(row))
		values[svc] = append(values[svc], []string{strconv.FormatInt(ts.UnixNano(), 10), string(line)})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe,svc",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	var streams []any
	for svc, v := range values {
		streams = append(streams, map[string]any{"stream": map[string]string{"app": "parsed-surface", "probe": probe, "svc": svc}, "values": v})
	}
	push, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	sel := fmt.Sprintf(`{probe=%q}`, probe)
	waitForLokiMetricDataSelector(t, sel)
	rangeCases := []string{
		"count_over_time(" + sel + "[1m] |= \"error\")",
		"sum(count_over_time(" + sel + "[1m] |= \"error\" != \"timeout\"))",
		"sum by (svc) (count_over_time(" + sel + " | json | ip=ip(\"10.2.0.0/16\") [1m]))",
		"sum(count_over_time(" + sel + " | json | ip=ip(\"10.0.0.0-10.1.255.255\") [1m]))",
		"sum(count_over_time(" + sel + " | json | ip!=ip(\"10.2.0.0/16\") [1m]))",
	}
	for _, q := range rangeCases {
		rng := url.Values{"query": {q}, "start": {strconv.FormatInt(base.Unix(), 10)}, "end": {strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10)}, "step": {"60"}}
		ls, lb, lp := queryRangeGET(t, lokiURL, rng, headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, rng, headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%q: Loki=%d %s proxy=%d %s", q, ls, lb, ps, pb)
		}
		l := extractArray(extractMap(lp, "data"), "result")
		if len(l) == 0 {
			t.Fatalf("%q: Loki answered no series: %s", q, lb)
		}
		if !reflect.DeepEqual(sortedSeries(extractArray(extractMap(pp, "data"), "result")), sortedSeries(l)) {
			t.Errorf("%q:\nproxy %s\n Loki %s", q, pb, lb)
		}
	}
	at := strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10)
	for _, q := range []string{
		"topk(2, sum by (svc) (sum_over_time(" + sel + " | json | unwrap duration_ms [5m])))",
		"topk(1, sum by (svc) (sum_over_time(" + sel + " | json | unwrap duration_ms | __error__=\"\" [5m])))",
	} {
		instant := url.Values{"query": {q}, "time": {at}}
		ls, lb, lp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pb, pp := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%q: Loki=%d %s proxy=%d %s", q, ls, lb, ps, pb)
		}
		l := extractArray(extractMap(lp, "data"), "result")
		if len(l) == 0 {
			t.Fatalf("%q: Loki answered no series: %s", q, lb)
		}
		if !reflect.DeepEqual(sortedSeries(extractArray(extractMap(pp, "data"), "result")), sortedSeries(l)) {
			t.Errorf("%q:\nproxy %s\n Loki %s", q, pb, lb)
		}
	}
}

// sortedSeries orders a result by its labels: series order is not part of the contract.
func sortedSeries(r []interface{}) []string {
	out := make([]string, 0, len(r))
	for _, s := range r {
		b, _ := json.Marshal(s)
		out = append(out, string(b))
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
