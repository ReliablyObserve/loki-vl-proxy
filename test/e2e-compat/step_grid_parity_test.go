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

// The stack's Loki runs with query_range.align_queries_with_step: true and
// every proxy variant in docker-compose.yml with -align-queries-with-step, so
// a metric range query with an odd step and an unaligned start has to land
// its samples on the same k*step grid on both sides. On an unaligned proxy
// the whole series is shifted by start mod step and the first point differs.
//
// conformance: loki_api_v1_query_range
func TestCompat_StepAlignedGridMatchesLoki(t *testing.T) {
	ensureDataIngested(t)

	const step = 137 * time.Second
	end := time.Now().Truncate(time.Second)
	start := end.Add(-30 * time.Minute)
	if start.Unix()%int64(step.Seconds()) == 0 {
		// An aligned start proves nothing; move it off the grid.
		start = start.Add(time.Second)
	}
	params := url.Values{}
	params.Set("query", `sum(count_over_time({app="api-gateway"}[5m]))`)
	params.Set("start", fmt.Sprintf("%d", start.UnixNano()))
	params.Set("end", fmt.Sprintf("%d", end.UnixNano()))
	params.Set("step", fmt.Sprintf("%ds", int(step.Seconds())))

	loki := stepGridTimestamps(t, lokiURL, params)
	proxy := stepGridTimestamps(t, proxyURL, params)
	if len(loki) == 0 {
		t.Fatalf("Loki returned no samples for %s", params.Get("query"))
	}
	for _, ts := range loki {
		if ts%int64(step.Seconds()) != 0 {
			t.Fatalf("Loki timestamp %d is not on the %s grid; is align_queries_with_step on in loki-local-config.yaml?", ts, step)
		}
	}
	if strings.Join(int64Strings(proxy), ",") != strings.Join(int64Strings(loki), ",") {
		t.Fatalf("evaluation grids differ (start %d, step %s)\n  loki:  %v\n  proxy: %v", start.Unix(), step, loki, proxy)
	}
}

// stepGridTimestamps returns the sorted, de-duplicated second timestamps of a
// matrix response.
func stepGridTimestamps(t *testing.T, baseURL string, params url.Values) []int64 {
	t.Helper()
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	if status != http.StatusOK || !checkStatus(resp) {
		t.Fatalf("%s: HTTP %d %s", baseURL, status, body)
	}
	seen := map[int64]struct{}{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		series, _ := item.(map[string]interface{})
		for _, point := range extractArray(series, "values") {
			pair, _ := point.([]interface{})
			if len(pair) != 2 {
				continue
			}
			ts, _ := pair[0].(float64)
			seen[int64(ts)] = struct{}{}
		}
	}
	out := make([]int64, 0, len(seen))
	for ts := range seen {
		out = append(out, ts)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func int64Strings(values []int64) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = fmt.Sprint(v)
	}
	return out
}
