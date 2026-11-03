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

// largeAnswerRange returns a matrix as the batch_id label -> timestamp ->
// value.
func largeAnswerRange(t *testing.T, base, query string, start, end time.Time) map[string]map[string]string {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"60"},
	}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": "Source=grafana-lokiexplore-app"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil {
		t.Fatalf("%s: unhealthy response %d %.300s", base, status, body)
	}
	out := map[string]map[string]string{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		series, _ := item.(map[string]any)
		metric, _ := series["metric"].(map[string]any)
		key, _ := metric["batch_id"].(string)
		if _, dup := out[key]; dup || key == "" {
			t.Fatalf("%s: a series without batch_id or two for one value", base)
		}
		out[key] = map[string]string{}
		values, _ := series["values"].([]any)
		for _, raw := range values {
			pair, _ := raw.([]any)
			if len(pair) != 2 {
				t.Fatalf("%s: malformed sample %v", base, raw)
			}
			out[key][fmt.Sprint(pair[0])] = fmt.Sprint(pair[1])
		}
	}
	return out
}

// TestCompat_MetricRangeExtentCacheLargeAnswerMatchesLoki: a Logs Drilldown
// field breakdown whose answer is larger than one proxy cache entry (a tenth
// of -cache-max-bytes) answers Loki's series and values on the first read and
// on the windows moved back a step at a time, as the Drilldown page is opened
// again; the proxy answers the moved windows from the buckets the first read
// kept split by split (the extent cache), not from a whole new read. It runs
// against the underscore proxy, whose 64 MiB cache (6.4 MB entries) is the
// benchmark proxy's. The fixture is 450 lines over six hours, each with its
// own long batch_id, so the answer (about 9 MB) is over one entry while each
// of its three splits is under it.
// conformance: quality/query-range-extent-cache-large-answer, quality/query-range-extent-cache, loki_api_v1_query_range
func TestCompat_MetricRangeExtentCacheLargeAnswerMatchesLoki(t *testing.T) {
	probe := fmt.Sprintf("extentlarge%d", time.Now().UnixNano())
	s0 := time.Now().Add(-9 * time.Hour).Truncate(time.Hour)
	labels := map[string]string{"probe": probe}
	value := strings.Repeat("batch-segment-", 1450) // 20 KB
	const lines = 450
	var vlRows strings.Builder
	var values [][]string
	for i := 0; i < lines; i++ {
		at := s0.Add(5*time.Second + time.Duration(i)*48*time.Second)
		line := fmt.Sprintf(`{"batch_id":"%s%04d","msg":"done"}`, value, i)
		values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
		row, _ := json.Marshal(map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line, "probe": probe})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=probe", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"}); status != http.StatusOK {
		t.Fatalf("VL ingest: %d %.300s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": values}}})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %.300s", status, body)
	}
	forceVLFlush(t)
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %.300s", status, body)
	}

	query := fmt.Sprintf(`sum by (batch_id) (count_over_time({probe=%q} | json batch_id="batch_id" | drop __error__, __error_details__ | batch_id!="" [1m]))`, probe)
	from, to := s0, s0.Add(6*time.Hour)
	// Both backends hold every line before any comparison. Each is asked
	// through a query no cache holds (a filter every line passes, spelled
	// differently each time): Loki's results cache keeps an hours-old split
	// it answered while its ingester was still flushing, and would keep
	// answering it, and the proxy must keep no bucket before it is whole.
	// The compared query is first sent once both answer every series.
	deadline := time.Now().Add(240 * time.Second)
	for attempt := 0; ; attempt++ {
		probeQuery := strings.Replace(query, "| json", fmt.Sprintf(`!= "absent-%d" | json`, attempt), 1)
		loki, proxy := largeAnswerRange(t, lokiURL, probeQuery, from, to), largeAnswerRange(t, proxyUnderscoreURL, probeQuery, from, to)
		if len(loki) == lines && len(proxy) == lines {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki answers %d, VictoriaLogs %d of %d series", len(loki), len(proxy), lines)
		}
		time.Sleep(2 * time.Second)
	}

	before := extentCacheOutcomes(t, proxyUnderscoreURL)
	for back := 0; back <= 3; back++ {
		start, end := from.Add(-time.Duration(back)*time.Minute), to.Add(-time.Duration(back)*time.Minute)
		loki := largeAnswerRange(t, lokiURL, query, start, end)
		proxy := largeAnswerRange(t, proxyUnderscoreURL, query, start, end)
		if len(loki) == 0 || len(proxy) != len(loki) {
			t.Fatalf("window moved back %dm: proxy %d series, Loki %d", back, len(proxy), len(loki))
		}
		for key, want := range loki {
			got := proxy[key]
			if len(got) != len(want) {
				t.Fatalf("window moved back %dm: series %.40s… has %d samples, Loki %d", back, key, len(got), len(want))
			}
			for ts, v := range want {
				if got[ts] != v {
					t.Fatalf("window moved back %dm: series %.40s… at %s: %q, Loki %q", back, key, ts, got[ts], v)
				}
			}
		}
	}
	after := extentCacheOutcomes(t, proxyUnderscoreURL)
	delta := map[string]int{}
	for outcome, n := range after {
		delta[outcome] = n - before[outcome]
	}
	if delta["stored"] < 1 || delta["hit"]+delta["partial"] < 3 || delta["unreadable"] != 0 {
		t.Fatalf("the moved windows must be answered from the splits the first read kept: outcomes %v", delta)
	}
}
