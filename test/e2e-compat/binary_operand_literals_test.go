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

// TestCompat_BinaryOperandLiterals: a binary metric query whose operand holds
// a label filter with a compound duration (`1h5m`) or a number in exponent
// form (`1e3`), or a scalar the printer used to write as `1e+06`, answers like
// Loki on query and query_range. The proxy translates each operand from its
// printed text, which split those literals and answered 400. The fixture is
// one stream of JSON lines ingested alike into Loki and VictoriaLogs.
// conformance: semantics/binary-operand-literals-reprint, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_BinaryOperandLiterals(t *testing.T) {
	probe := fmt.Sprintf("binop-literals-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	lines := []string{
		`{"latency":"30m","n":500}`,
		`{"latency":"1h10m","n":1500}`,
		`{"latency":"2h","n":2500}`,
		`{"latency":"1h5m","n":900}`,
	}
	var vlRows []string
	var values [][]string
	for i, line := range lines {
		ts := stamp.Add(time.Duration(i) * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "binop-literals", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "binop-literals", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	at := strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)
	for _, tc := range []struct {
		expr string
		want string // Loki's value over the fixture
	}{
		{`sum(count_over_time({probe=%[1]q} | json | latency > 1h5m [5m])) / 2`, "1"},
		{`sum(count_over_time({probe=%[1]q} | json | n >= 1e3 [5m])) / sum(count_over_time({probe=%[1]q} [5m]))`, "0.5"},
		{`(sum(count_over_time({probe=%[1]q} | json | latency >= 1h5m [5m])) * 1000000) / 2`, "1500000"},
	} {
		query := fmt.Sprintf(tc.expr, probe)
		instant := url.Values{"query": {query}, "time": {at}}
		ls, lb, lp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pb, pp := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		lokiResult := extractArray(extractMap(lp, "data"), "result")
		if len(lokiResult) == 0 {
			t.Fatalf("%s: Loki answered no sample: %s", query, lb)
		}
		if got := lokiResult[0].(map[string]interface{})["value"].([]interface{})[1]; got != tc.want {
			t.Fatalf("%s: Loki answered %v, the fixture holds %s: %s", query, got, tc.want, lb)
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
