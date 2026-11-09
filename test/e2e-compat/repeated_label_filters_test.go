//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// pushLabelFixture stores one line per label value (labels app, a and the extra labels of the row) in both Loki and
// VictoriaLogs, the rows' time one second apart, and waits until both sides answer the stream. It returns the
// window to query.
func pushLabelFixture(t *testing.T, app string, rows []map[string]string, lines []string) (start, end string) {
	t.Helper()
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	var vlRows []string
	var streams []any
	fields := map[string]bool{}
	for i, labels := range rows {
		ts := stamp.Add(time.Duration(i) * time.Second)
		row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": lines[i], "app": app}
		stream := map[string]string{"app": app}
		for k, v := range labels {
			row[k], stream[k] = v, v
			fields[k] = true
		}
		b, _ := json.Marshal(row)
		vlRows = append(vlRows, string(b))
		streams = append(streams, map[string]any{"stream": stream, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), lines[i]}}})
	}
	names := []string{"app"}
	for k := range fields {
		names = append(names, k)
	}
	sort.Strings(names)
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+strings.Join(names, ","),
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	start = strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)
	end = strconv.FormatInt(stamp.Add(time.Duration(len(rows))*time.Second+time.Minute).UnixNano(), 10)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, _, l := queryRangeGET(t, lokiURL, url.Values{"query": {fmt.Sprintf(`{app=%q}`, app)}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		_, _, p := queryRangeGET(t, proxyURL, url.Values{"query": {fmt.Sprintf(`{app=%q}`, app)}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		if len(resultLines(l)) == len(rows) && len(resultLines(p)) == len(rows) {
			return start, end
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", len(resultLines(l)), len(resultLines(p)))
		}
		time.Sleep(time.Second)
	}
}

// resultLines are the sorted lines of a query_range streams response.
func resultLines(resp map[string]interface{}) []string {
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

// Every label filter of a query applies, as in Loki: a second filter on the same label does not replace the first
// (`| a="1" | a="2"` keeps no line, `| a="1" | a!="1"` keeps none), and a later filter on a label keeps its place
// after the stages between the two.
// conformance: semantics/repeated-label-filter-replaced-across-label-format, loki_api_v1_query_range
func TestCompat_RepeatedLabelFiltersApplyAllLikeLoki(t *testing.T) {
	app := fmt.Sprintf("repeated-filter-%d", time.Now().UnixNano())
	start, end := pushLabelFixture(t, app,
		[]map[string]string{{"a": "1"}, {"a": "2"}, {"a": "3"}},
		[]string{"line one", "line two", "line three"})
	headers := map[string]string{"X-Scope-OrgID": "0"}
	for _, tc := range []struct {
		stages string
		want   []string
	}{
		{`| a="1" | a="2"`, nil},
		{`| a="1" | a!="1"`, nil},
		{`| a="1" | a!="2"`, []string{"line one"}},
		{`| a!="1" | a!="2"`, []string{"line three"}},
		{`| a=~"1|2" | a=~"2|3"`, []string{"line two"}},
		{`| a="1" | a="1"`, []string{"line one"}},
		{`| a="1" |= "one" | a="2"`, nil},
		{`| a!="1" |= "line" | a!="3"`, []string{"line two"}},
		{`| label_format x="{{.a}}" | a="1" | a="2"`, nil},
		{`| a="1" | label_format x="{{.a}}" | a="2"`, nil},
		{`| a!="3" | label_format x="{{.a}}" | a!="1"`, []string{"line two"}},
	} {
		q := fmt.Sprintf(`{app=%q} %s`, app, tc.stages)
		ls, lb, lresp := queryRangeGET(t, lokiURL, url.Values{"query": {q}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		ps, pb, presp := queryRangeGET(t, proxyURL, url.Values{"query": {q}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Errorf("%s: Loki %d %s, proxy %d %s", q, ls, lb, ps, pb)
			continue
		}
		l, p := resultLines(lresp), resultLines(presp)
		if strings.Join(l, "|") != strings.Join(tc.want, "|") {
			t.Fatalf("%s: Loki keeps %v, the fixture pins %v", q, l, tc.want)
		}
		if strings.Join(p, "|") != strings.Join(l, "|") {
			t.Errorf("%s:\n proxy %v\n Loki  %v", q, p, l)
		}
	}
}
