//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A |= or != line filter whose text is not UTF-8 answers like Loki: 200, with no line for |= and every line for
// != (no ingestion API stores such bytes in a line), on log and metric queries. A regexp that is not UTF-8 is
// Loki's 400 on both.
// conformance: semantics/line-filter-invalid-utf8, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_LineFilterInvalidUTF8LikeLoki(t *testing.T) {
	probe := fmt.Sprintf("utf8-line-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	lines := []string{"level=info msg=ok", "level=warn msg=slow b", "level=error msg=\"bad byte\" b"}
	var vlRows []string
	var values [][]string
	for i, line := range lines {
		ts := stamp.Add(time.Duration(i) * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "utf8-line", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "utf8-line", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	start := strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)
	end := strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)
	linesOf := func(resp map[string]interface{}) []string {
		var out []string
		for _, s := range extractArray(extractMap(resp, "data"), "result") {
			vals, _ := s.(map[string]interface{})["values"].([]interface{})
			for _, v := range vals {
				if pair, _ := v.([]interface{}); len(pair) == 2 {
					out = append(out, fmt.Sprint(pair[1]))
				}
			}
		}
		sort.Strings(out)
		return out
	}
	query := func(base, q string) (int, string, []string) {
		st, b, resp := queryRangeGET(t, base, url.Values{"query": {q}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		return st, b, linesOf(resp)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, _, l := query(lokiURL, fmt.Sprintf(`{probe=%q}`, probe))
		_, _, p := query(proxyURL, fmt.Sprintf(`{probe=%q}`, probe))
		if len(l) == len(lines) && len(p) == len(lines) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", len(l), len(p))
		}
		time.Sleep(time.Second)
	}
	for _, tc := range []struct {
		filter string
		status int
		lines  int
	}{
		{`|= "\xff\xfe"`, http.StatusOK, 0},
		{`!= "\xff"`, http.StatusOK, len(lines)},
		{`|= "\xff" or " b"`, http.StatusOK, 2},
		{`|~ "\xff"`, http.StatusBadRequest, 0},
	} {
		q := fmt.Sprintf(`{probe=%q} %s`, probe, tc.filter)
		ls, lb, l := query(lokiURL, q)
		ps, pb, p := query(proxyURL, q)
		if ls != tc.status || len(l) != tc.lines {
			t.Fatalf("%s: Loki %d with %d lines, the fixture pins %d with %d: %s", q, ls, len(l), tc.status, tc.lines, lb)
		}
		if ps != ls {
			t.Errorf("%s: proxy %d %s, Loki %d %s", q, ps, pb, ls, lb)
			continue
		}
		if ls == http.StatusBadRequest {
			if strings.TrimSpace(pb) != strings.TrimSpace(lb) {
				t.Errorf("%s: proxy error %q, Loki %q", q, pb, lb)
			}
			continue
		}
		if strings.Join(p, "\n") != strings.Join(l, "\n") {
			t.Errorf("%s:\n proxy %v\n Loki  %v", q, p, l)
		}
	}
	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	for _, filter := range []string{`|= "\xff\xfe"`, `!= "\xff"`} {
		q := fmt.Sprintf(`sum(count_over_time({probe=%q} %s [5m]))`, probe, filter)
		instant := url.Values{"query": {q}, "time": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}}
		ls, lb, lr := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pb, pr := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", q, ls, lb, ps, pb)
		}
		if !reflect.DeepEqual(extractArray(extractMap(pr, "data"), "result"), extractArray(extractMap(lr, "data"), "result")) {
			t.Errorf("%s:\n proxy %s\n Loki  %s", q, pb, lb)
		}
	}
}
