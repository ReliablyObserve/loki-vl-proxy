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

// TestRangeMetricCompatibilityCommentsAndOffsets: `#` comments (to the end of their line, outside strings), a
// negative offset (offset -2m shifts the window forward), and compound and fractional durations (1m30s, 1.5m) answer
// like Loki on query_range. The fixture is one stream of lines over ten minutes ingested alike into Loki and
// VictoriaLogs; the windows read only inside it.
// conformance: semantics/comments-signed-compound-offsets, loki_api_v1_query_range
func TestRangeMetricCompatibilityCommentsAndOffsets(t *testing.T) {
	probe := fmt.Sprintf("comments-offsets-%d", time.Now().UnixNano())
	base := time.Now().Add(-20 * time.Minute).Truncate(time.Minute)
	var vlRows []string
	var values [][]string
	for i := 0; i < 600; i += 7 {
		ts := base.Add(time.Duration(i) * time.Second)
		line := fmt.Sprintf("line %d #%d", i, i%5)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "comments-offsets", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "comments-offsets", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	sel := fmt.Sprintf(`{probe=%q}`, probe)
	waitForLokiMetricDataSelector(t, sel)
	for _, tc := range []string{
		"sum(count_over_time(%s[1m])) # every line\n",
		"# all of them\nsum(count_over_time(%s |= \"#3\" [1m]))",
		"sum(count_over_time(%s[1m] offset -2m))",
		"sum(count_over_time(%s[1m] offset 1m30s))",
		"sum(count_over_time(%s[1m] offset 1.5m))",
		"sum(count_over_time(%s[1m30s] offset -1m))",
		"sum(count_over_time(%s[1m] offset -2m)) / sum(count_over_time(%s[1m] offset -2m)) # same shift both sides",
	} {
		query := strings.ReplaceAll(tc, "%s", sel)
		for _, step := range []int{60, 30} {
			rng := url.Values{"query": {query}, "start": {strconv.FormatInt(base.Add(3*time.Minute).Unix(), 10)},
				"end": {strconv.FormatInt(base.Add(7*time.Minute).Unix(), 10)}, "step": {strconv.Itoa(step)}}
			ls, lb, lp := queryRangeGET(t, lokiURL, rng, headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, rng, headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("%q step %d: Loki=%d %s proxy=%d %s", query, step, ls, lb, ps, pb)
			}
			l := extractArray(extractMap(lp, "data"), "result")
			if len(l) == 0 {
				t.Fatalf("%q step %d: Loki answered no series: %s", query, step, lb)
			}
			if !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), l) {
				t.Errorf("%q step %d:\nproxy %s\n Loki %s", query, step, pb, lb)
			}
		}
	}
}
