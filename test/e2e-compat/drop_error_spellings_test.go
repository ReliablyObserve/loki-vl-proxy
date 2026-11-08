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

// TestRangeMetricCompatibilityDropErrorSpellings: `| json | drop __error__` keeps the lines the parser fails on, in
// every spelling Loki accepts for the pipeline of a range: inside the range, after it, and as a parenthesised log
// query. Each spelling answers like Loki on query_range, with the step equal to the range (the native stats route)
// and shorter than it, and all spellings answer the same. The fixture is one stream of JSON and non-JSON lines
// ingested alike into Loki and VictoriaLogs.
// conformance: semantics/drop-error-pipeline-spellings, loki_api_v1_query_range
func TestRangeMetricCompatibilityDropErrorSpellings(t *testing.T) {
	probe := fmt.Sprintf("drop-error-spellings-%d", time.Now().UnixNano())
	base := time.Now().Add(-6 * time.Minute).Truncate(time.Minute)
	var vlRows []string
	var values [][]string
	total := 0
	for minute := 0; minute < 4; minute++ {
		for i := 0; i <= minute; i++ {
			ts := base.Add(time.Duration(minute)*time.Minute + time.Duration(i+1)*time.Second)
			line := fmt.Sprintf(`{"msg":"ok %d-%d","status":200}`, minute, i)
			if i%2 == 1 {
				line = fmt.Sprintf("plain text %d-%d, not JSON", minute, i)
			}
			row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "drop-error-spellings", "probe": probe})
			vlRows = append(vlRows, string(row))
			values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
			total++
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "drop-error-spellings", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	selector := fmt.Sprintf(`{probe=%q}`, probe)
	waitForLokiMetricDataSelector(t, selector)

	sumValues := func(result []interface{}) float64 {
		n := 0.0
		for _, s := range result {
			for _, v := range s.(map[string]interface{})["values"].([]interface{}) {
				f, _ := strconv.ParseFloat(v.([]interface{})[1].(string), 64)
				n += f
			}
		}
		return n
	}
	spellings := []string{
		`sum(count_over_time(%s | json | drop __error__ [1m]))`,
		`sum(count_over_time(%s[1m] | json | drop __error__))`,
		`sum(count_over_time((%s | json | drop __error__)[1m]))`,
		`sum by (probe) (count_over_time(%s[1m] | json | drop __error__))`,
	}
	for _, step := range []time.Duration{time.Minute, 30 * time.Second} {
		var first []interface{}
		for i, spelling := range spellings {
			query := fmt.Sprintf(spelling, selector)
			rng := url.Values{"query": {query}, "start": {strconv.FormatInt(base.Unix(), 10)},
				"end": {strconv.FormatInt(base.Add(4*time.Minute).Unix(), 10)}, "step": {strconv.Itoa(int(step.Seconds()))}}
			ls, lb, lp := queryRangeGET(t, lokiURL, rng, headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, rng, headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("%s step %s: Loki=%d %s proxy=%d %s", query, step, ls, lb, ps, pb)
			}
			l := extractArray(extractMap(lp, "data"), "result")
			if len(l) == 0 {
				t.Fatalf("%s step %s: Loki answered no series: %s", query, step, lb)
			}
			if step == time.Minute && sumValues(l) != float64(total) {
				t.Fatalf("%s: Loki counts %v lines over the tumbling windows, the fixture holds %d (the non-JSON lines included)", query, sumValues(l), total)
			}
			if !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), l) {
				t.Errorf("%s step %s:\nproxy %s\n Loki %s", query, step, pb, lb)
			}
			if i == 0 {
				first = l
			} else if i < 3 && !reflect.DeepEqual(l, first) {
				t.Errorf("%s step %s: Loki answers it unlike the first spelling:\n%v\n%v", query, step, l, first)
			}
		}
	}
}
