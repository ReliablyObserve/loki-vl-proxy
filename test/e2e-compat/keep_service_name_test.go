//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// conformance: profiles/keep-removes-derived-service-name, loki_api_v1_query_range
func TestCompat_KeepRemovesDerivedServiceName(t *testing.T) {
	probe := fmt.Sprintf("keep-service-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	line := `{"level":"info","message":"keep marker"}`
	labels := map[string]string{"app": "checkout", "probe": probe}
	row, _ := json.Marshal(map[string]string{
		"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line,
		"app": "checkout", "probe": probe,
	})
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields=app,probe", string(row)+"\n",
		map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}},
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push),
		map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	params := url.Values{
		"query": {fmt.Sprintf(`{probe=%q} | json | keep level`, probe)},
		"start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)},
		"limit": {"10"},
	}
	for _, categorized := range []bool{false, true} {
		headers := map[string]string{"X-Scope-OrgID": "0"}
		if categorized {
			headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
		}
		var lokiResult, proxyResult []interface{}
		deadline := time.Now().Add(60 * time.Second)
		for {
			ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("query_range Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
			}
			lokiResult = extractArray(extractMap(lp, "data"), "result")
			proxyResult = extractArray(extractMap(pp, "data"), "result")
			if len(lokiResult) == 1 && len(proxyResult) == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("fixture not visible on both sides: Loki=%s proxy=%s", lb, pb)
			}
			time.Sleep(time.Second)
		}
		want := map[string]interface{}{"level": "info"}
		if categorized {
			want = map[string]interface{}{}
		}
		loki := lokiResult[0].(map[string]interface{})
		proxy := proxyResult[0].(map[string]interface{})
		if !reflect.DeepEqual(loki["stream"], want) {
			t.Fatalf("categorized=%v Loki labels=%v, expected fixture labels=%v", categorized, loki["stream"], want)
		}
		if !reflect.DeepEqual(proxy["stream"], loki["stream"]) || !reflect.DeepEqual(proxy["values"], loki["values"]) {
			t.Fatalf("categorized=%v proxy=%v Loki=%v", categorized, proxy, loki)
		}
	}
}
