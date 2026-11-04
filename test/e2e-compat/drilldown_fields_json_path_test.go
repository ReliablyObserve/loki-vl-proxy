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

// TestCompat_DrilldownFieldsPillJSONPathExtracted: the Logs Drilldown Fields
// tab breaks a nested JSON field down with a JSON-path extraction list
// (pkg/logql/log/parser.go:671 JSONExpressionParser.Process), with a level
// pill before the parser or without it. The fixture is one stream of JSON
// lines with a level key and service.name nested (two values, or none),
// ingested alike into Loki and VictoriaLogs; the proxy answers Loki's series
// and values on query_range and query.
// conformance: drilldown/fields-pill-json-path-extracted-empty, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_DrilldownFieldsPillJSONPathExtracted(t *testing.T) {
	probe := fmt.Sprintf("dd-json-path-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-3 * time.Minute).Truncate(time.Minute)
	var vlRows []string
	var values [][]string
	for i := 0; i < 30; i++ {
		ts := stamp.Add(time.Duration(i) * 2 * time.Second)
		level := []string{"error", "info", "warn"}[i%3]
		line := fmt.Sprintf(`{"level":%q,"service":{"name":"api-gateway"},"n":%d}`, level, i)
		switch i % 5 {
		case 3:
			line = fmt.Sprintf(`{"level":%q,"service":{"name":"billing"},"n":%d}`, level, i)
		case 4:
			line = fmt.Sprintf(`{"level":%q,"n":%d}`, level, i)
		}
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "dd-json-path", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "dd-json-path", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	headers := map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": "Source=grafana-lokiexplore-app"}
	for _, pill := range []string{` | detected_level="error"`, ""} {
		query := fmt.Sprintf(`sum by (service_name_extracted) (count_over_time({probe=%q}%s | json service_name_extracted="[\"service\"][\"name\"]" | drop __error__, __error_details__ | service_name_extracted!="" [1m]))`, probe, pill)
		params := url.Values{"query": {query}, "step": {"60"},
			"start": {strconv.FormatInt(stamp.UnixNano(), 10)}, "end": {strconv.FormatInt(stamp.Add(2*time.Minute).UnixNano(), 10)}}
		var lokiResult []interface{}
		var lb string
		deadline := time.Now().Add(60 * time.Second)
		for {
			var lp map[string]interface{}
			_, lb, lp = queryRangeGET(t, lokiURL, params, headers)
			lokiResult = extractArray(extractMap(lp, "data"), "result")
			if len(lokiResult) == 2 || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Second)
		}
		if len(lokiResult) != 2 {
			t.Fatalf("%s: Loki answered %d series, the fixture holds 2: %s", query, len(lokiResult), lb)
		}
		ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
		if ps != http.StatusOK || !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), lokiResult) {
			t.Errorf("query_range %s:\nproxy %d %s\n Loki %s", query, ps, pb, lb)
		}
		instant := url.Values{"query": {strings.Replace(query, "[1m]", "[5m]", 1)}, "time": {strconv.FormatInt(stamp.Add(2*time.Minute).UnixNano(), 10)}}
		ls, lbi, lpi := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pbi, ppi := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK || !reflect.DeepEqual(extractArray(extractMap(ppi, "data"), "result"), extractArray(extractMap(lpi, "data"), "result")) {
			t.Errorf("query %s:\nproxy %d %s\n Loki %d %s", instant.Get("query"), ps, pbi, ls, lbi)
		}
	}
}
