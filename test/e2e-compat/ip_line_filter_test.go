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

// An IPv4 ip() line filter keeps the lines Loki's keeps: the address read as Loki's filter reads a line (the first
// octet is the last three digits before the first dot, a digit or a dot right after the address makes it another
// address, a single colon in front belongs to an IPv6 address after a hex digit), never a substring of a longer
// number. Log queries and count_over_time answer like Loki, and Loki's own answers are pinned so a Loki release that
// reads ip() differently is noticed.
// conformance: semantics/ip-filter-precision, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_IPLineFilterLikeLoki(t *testing.T) {
	probe := fmt.Sprintf("ip-line-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	lines := []string{
		"client 10.0.0.1 connected", "client 110.0.0.12 connected", "client 10.0.0.12 connected", "client 10.0.0.1:8080",
		"ip=10.0.0.1,next", "a10.0.0.1b", "10.0.0.1.5", "v1.10.0.0.1", "x-10.0.0.1-y", "10.0.0.255 end", "10.0.1.1 end",
		"10.0.0.3 mid", "10.0.0.9 high", "0010.0.0.1 padded", "10.00.0.1 zero", "1192.168.1.1 wide", "::ffff:10.0.0.1 mapped",
		"g:10.0.0.1 colon", "10.0.0.1. dot",
	}
	var vlRows []string
	var values [][]string
	for i, line := range lines {
		ts := stamp.Add(time.Duration(i) * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "ip-line", "probe": probe})
		vlRows = append(vlRows, string(row))
		values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "ip-line", "probe": probe}, "values": values,
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
				pair, _ := v.([]interface{})
				if len(pair) == 2 {
					out = append(out, fmt.Sprint(pair[1]))
				}
			}
		}
		sort.Strings(out)
		return out
	}
	query := func(base string, q string) ([]string, int, string) {
		status, body, resp := queryRangeGET(t, base, url.Values{"query": {q}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		return linesOf(resp), status, body
	}
	// The selector alone: both sides hold every fixture line.
	deadline := time.Now().Add(60 * time.Second)
	for {
		l, _, _ := query(lokiURL, fmt.Sprintf(`{probe=%q}`, probe))
		p, _, _ := query(proxyURL, fmt.Sprintf(`{probe=%q}`, probe))
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
		want   int // Loki's own answer on the fixture
	}{
		{`|= ip("10.0.0.1")`, 6},
		{`!= ip("10.0.0.1")`, len(lines) - 6},
		{`|= ip("10.0.0.0/24")`, 10},
		{`|= ip("10.0.0.1-10.0.0.5")`, 7},
		{`|= ip("192.168.1.1")`, 1},
		// In a positive `or` chain Loki reads the ip() head as its text (a substring).
		{`|= ip("10.0.0.1") or "end"`, 15},
	} {
		q := fmt.Sprintf(`{probe=%q} %s`, probe, tc.filter)
		l, ls, lb := query(lokiURL, q)
		p, ps, pb := query(proxyURL, q)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", q, ls, lb, ps, pb)
		}
		if len(l) != tc.want {
			t.Fatalf("%s: Loki answered %d lines, the fixture pins %d: %v", q, len(l), tc.want, l)
		}
		if strings.Join(p, "\n") != strings.Join(l, "\n") {
			t.Errorf("%s:\n proxy %v\n Loki  %v", q, p, l)
		}
	}
	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	for _, filter := range []string{`|= ip("10.0.0.1")`, `|= ip("10.0.0.0/24")`} {
		q := fmt.Sprintf(`sum(count_over_time({probe=%q} %s [5m]))`, probe, filter)
		instant := url.Values{"query": {q}, "time": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}}
		ls, lb, lr := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		ps, pb, pr := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%s: Loki=%d %s proxy=%d %s", q, ls, lb, ps, pb)
		}
		lokiResult := extractArray(extractMap(lr, "data"), "result")
		if len(lokiResult) == 0 {
			t.Fatalf("%s: Loki answered no sample: %s", q, lb)
		}
		if !reflect.DeepEqual(extractArray(extractMap(pr, "data"), "result"), lokiResult) {
			t.Errorf("%s:\n proxy %s\n Loki  %s", q, pb, lb)
		}
	}
}
