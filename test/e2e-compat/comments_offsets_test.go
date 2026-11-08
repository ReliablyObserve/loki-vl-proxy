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

// TestRangeMetricCompatibilityLogfmtFlags: `| logfmt --strict` and `--keep-empty` (in either order, with a field
// list) answer like Loki on lines that are well-formed logfmt without empty values, where the flags change nothing:
// the proxy reads the flags and answers the stage as `| logfmt` (malformed lines and empty values differ:
// semantics/logfmt-malformed-lines). Log query and metric, on query_range.
// conformance: semantics/proxy-rejects-valid-logql, loki_api_v1_query_range
func TestRangeMetricCompatibilityLogfmtFlags(t *testing.T) {
	probe := fmt.Sprintf("logfmt-flags-%d", time.Now().UnixNano())
	base := time.Now().Add(-20 * time.Minute).Truncate(time.Minute)
	var vlRows []string
	var values [][]string
	for i := 0; i < 300; i += 5 {
		ts := base.Add(time.Duration(i) * time.Second)
		line := fmt.Sprintf(`level=%s route=/api/%d status=%d msg="request %d"`, []string{"info", "warn", "error"}[i%3], i%4, 200+i%3, i)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "logfmt-flags", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "logfmt-flags", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	sel := fmt.Sprintf(`{probe=%q}`, probe)
	waitForLokiMetricDataSelector(t, sel)
	start, end := strconv.FormatInt(base.Unix(), 10), strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10)
	for _, flags := range []string{"--strict", "--keep-empty", "--keep-empty --strict"} {
		for _, tc := range []struct{ query, step string }{
			{"sum by (route) (count_over_time(" + sel + " | logfmt " + flags + " [1m]))", "60"},
			{"sum by (level) (count_over_time(" + sel + " | logfmt " + flags + " level, status [1m]))", "30"},
			{sel + " | logfmt " + flags + " | status=\"201\"", ""},
		} {
			params := url.Values{"query": {tc.query}, "start": {start}, "end": {end}}
			if tc.step != "" {
				params.Set("step", tc.step)
			} else {
				params.Set("limit", "1000")
				params.Set("direction", "forward")
			}
			ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("%q: Loki=%d %s proxy=%d %s", tc.query, ls, lb, ps, pb)
			}
			l := extractArray(extractMap(lp, "data"), "result")
			if len(l) == 0 {
				t.Fatalf("%q: Loki answered nothing: %s", tc.query, lb)
			}
			if tc.step == "" {
				// The log query: the same lines with the same parsed labels, compared as sets of stream labels and
				// line counts (stream order is not part of the contract).
				count := func(r []interface{}) map[string]int {
					out := map[string]int{}
					for _, s := range r {
						m := s.(map[string]interface{})
						k, _ := json.Marshal(m["stream"])
						out[string(k)] += len(m["values"].([]interface{}))
					}
					return out
				}
				if !reflect.DeepEqual(count(extractArray(extractMap(pp, "data"), "result")), count(l)) {
					t.Errorf("%q:\nproxy %s\n Loki %s", tc.query, pb, lb)
				}
				continue
			}
			if !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), l) {
				t.Errorf("%q:\nproxy %s\n Loki %s", tc.query, pb, lb)
			}
		}
	}
}
