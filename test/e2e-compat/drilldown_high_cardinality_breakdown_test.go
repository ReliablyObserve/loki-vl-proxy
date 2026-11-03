//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestDrilldown_HighCardinalityFieldBreakdownMatchesLoki: a Logs Drilldown
// field breakdown of a field with a new value on every line (a batch or job
// id) answers Loki's series and values, on the first view and on the view
// moved back a step. The answer holds 15,000 single-point series, more than
// the size the proxy transforms one series at a time, so it exercises the
// per-series trim, translation and tumbling relabel of a large stats answer.
// conformance: quality/stats-answer-transform-cost, loki_api_v1_query_range
func TestDrilldown_HighCardinalityFieldBreakdownMatchesLoki(t *testing.T) {
	probe := fmt.Sprintf("hicard%d", time.Now().UnixNano())
	s0 := time.Now().Add(-9 * time.Hour).Truncate(time.Hour)
	const lines = 15000
	var vlRows strings.Builder
	var values [][]string
	for i := 0; i < lines; i++ {
		at := s0.Add(time.Duration(i)*240*time.Millisecond + 70*time.Millisecond)
		line := fmt.Sprintf(`{"job_id":"job-%06d","msg":"done"}`, i)
		values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
		row, _ := json.Marshal(map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line, "probe": probe})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=probe", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"}); status != http.StatusOK {
		t.Fatalf("VL ingest: %d %.300s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"probe": probe}, "values": values}}})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %.300s", status, body)
	}
	forceVLFlush(t)
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %.300s", status, body)
	}

	query := fmt.Sprintf(`sum by (job_id) (count_over_time({probe=%q} | json job_id="job_id" | drop __error__, __error_details__ | job_id!="" [1m]))`, probe)
	read := func(base, query string, start, end time.Time) map[string]map[string]string {
		t.Helper()
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": "Source=grafana-lokiexplore-app"})
		if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil {
			t.Fatalf("%s: unhealthy response %d %.300s", base, status, body)
		}
		out := map[string]map[string]string{}
		for _, item := range extractArray(extractMap(resp, "data"), "result") {
			series, _ := item.(map[string]any)
			metric, _ := series["metric"].(map[string]any)
			key, _ := metric["job_id"].(string)
			if _, dup := out[key]; dup || key == "" {
				t.Fatalf("%s: a series without job_id or two for %q", base, key)
			}
			out[key] = map[string]string{}
			for _, raw := range series["values"].([]any) {
				pair, _ := raw.([]any)
				if len(pair) != 2 {
					t.Fatalf("%s: malformed sample %v", base, raw)
				}
				out[key][fmt.Sprint(pair[0])] = fmt.Sprint(pair[1])
			}
		}
		return out
	}
	from, to := s0, s0.Add(time.Hour)
	// Both backends hold every line before any comparison. Each is asked
	// through a query no cache holds (a filter every line passes, spelled
	// differently each time): Loki's results cache keeps an hours-old split
	// it answered while its ingester was still flushing, and would keep
	// answering it. The compared query is first sent once both answer every
	// series.
	deadline := time.Now().Add(240 * time.Second)
	for attempt := 0; ; attempt++ {
		probeQuery := strings.Replace(query, "| json", fmt.Sprintf(`!= "absent-%d" | json`, attempt), 1)
		loki, proxy := read(lokiURL, probeQuery, from, to), read(proxyURL, probeQuery, from, to)
		if len(loki) == lines && len(proxy) == lines {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki answers %d, VictoriaLogs %d of %d series", len(loki), len(proxy), lines)
		}
		time.Sleep(2 * time.Second)
	}
	for back := 0; back <= 1; back++ {
		start, end := from.Add(-time.Duration(back)*time.Minute), to.Add(-time.Duration(back)*time.Minute)
		loki, proxy := read(lokiURL, query, start, end), read(proxyURL, query, start, end)
		if len(loki) == 0 || len(proxy) != len(loki) {
			t.Fatalf("window moved back %dm: proxy %d series, Loki %d", back, len(proxy), len(loki))
		}
		for key, want := range loki {
			got := proxy[key]
			if len(got) != len(want) {
				t.Fatalf("window moved back %dm: %s has %d samples, Loki %d", back, key, len(got), len(want))
			}
			for ts, v := range want {
				if got[ts] != v {
					t.Fatalf("window moved back %dm: %s at %s: %q, Loki %q", back, key, ts, got[ts], v)
				}
			}
		}
	}
}
