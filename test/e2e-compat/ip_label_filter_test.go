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

// TestCompat_IPLabelFilterAbsentLabel: Loki's ip() label filter drops a line
// whose label is absent under both = and != (pkg/logql/log/ip.go:123-145),
// keeps a whole address in the pattern under = and the others under !=. The
// fixture is one logfmt stream ingested alike into Loki and VictoriaLogs:
// lines without addr, with addr in and out of 1.2.3.0/24, an IPv6 address and
// a leading-zero address. Log queries and count_over_time on query and
// query_range answer like Loki.
// conformance: semantics/ip-label-filter-missing-label, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_IPLabelFilterAbsentLabel(t *testing.T) {
	probe := fmt.Sprintf("ip-filter-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	addrs := []string{"", "1.2.3.4", "", "1.2.3.200", "", "5.6.7.8", "", "2001:db8::1", "", "01.2.3.4"}
	var vlRows []string
	var values [][]string
	for i, addr := range addrs {
		ts := stamp.Add(time.Duration(i) * time.Second)
		line := fmt.Sprintf(`level=info msg="charge" n=%d`, i)
		if addr != "" {
			line += " addr=" + addr
		}
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "ip-filter", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "ip-filter", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	window := url.Values{
		"start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)},
		"limit": {"100"},
	}
	lineCount := func(resp map[string]interface{}) int {
		n := 0
		for _, s := range extractArray(extractMap(resp, "data"), "result") {
			v, _ := s.(map[string]interface{})["values"].([]interface{})
			n += len(v)
		}
		return n
	}
	// The selector alone: both sides hold every fixture line.
	deadline := time.Now().Add(60 * time.Second)
	for {
		params := url.Values{"query": {fmt.Sprintf(`{probe=%q}`, probe)}, "start": window["start"], "end": window["end"], "limit": window["limit"]}
		_, _, lp := queryRangeGET(t, lokiURL, params, headers)
		_, _, pp := queryRangeGET(t, proxyURL, params, headers)
		if lineCount(lp) == len(addrs) && lineCount(pp) == len(addrs) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", lineCount(lp), lineCount(pp))
		}
		time.Sleep(time.Second)
	}
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{`addr=ip("1.2.3.4")`, 1},
		{`addr!=ip("1.2.3.4")`, 4},
		{`addr=ip("1.2.3.0/24")`, 2},
		{`addr!=ip("1.2.3.0/24")`, 3},
		{`addr=ip("2001:db8::/32")`, 1},
		{`addr=ip("9.9.9.9")`, 0},
	} {
		query := fmt.Sprintf(`{probe=%q} | logfmt | %s`, probe, tc.filter)
		params := url.Values{"query": {query}, "start": window["start"], "end": window["end"], "limit": window["limit"]}
		ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		if lineCount(lp) != tc.want {
			t.Fatalf("%s: Loki answered %d lines, the fixture holds %d: %s", query, lineCount(lp), tc.want, lb)
		}
		if lineCount(pp) != lineCount(lp) {
			t.Errorf("%s: proxy %d lines, Loki %d", query, lineCount(pp), lineCount(lp))
		}
	}
	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	for _, filter := range []string{`addr=ip("1.2.3.0/24")`, `addr!=ip("1.2.3.0/24")`, `addr=ip("1.2.3.4")`} {
		query := fmt.Sprintf(`sum(count_over_time({probe=%q} | logfmt | %s [5m]))`, probe, filter)
		instant := url.Values{"query": {query}, "time": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}}
		ls, lb, lp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pb, pp := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		lokiResult := extractArray(extractMap(lp, "data"), "result")
		if len(lokiResult) == 0 {
			t.Fatalf("%s: Loki answered no sample: %s", query, lb)
		}
		if !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), lokiResult) {
			t.Errorf("%s:\nproxy %s\n Loki %s", query, pb, lb)
		}
	}
}
