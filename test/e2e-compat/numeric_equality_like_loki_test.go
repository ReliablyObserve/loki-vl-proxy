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

// TestCompat_NumericEqualityLikeLoki: a numeric, duration or size equality after a json parser keeps the lines Loki
// keeps, for each class of value the query plan reads like Loki: the spellings of the value both read, a size or a
// duration where a number is asked (and the other way round), hexadecimal, binary and octal integers, the unit
// spellings VictoriaLogs does not read (`1kb`, `1 KB`, `5000000us`), and a value Loki marks with __error__. One stream
// of json lines, ingested alike into Loki and VictoriaLogs; each query is compared line by line. A present but empty
// value under != (Loki keeps it with __error__, VictoriaLogs stores no field) is the one class left out: the plan
// still differs there (semantics/label-filter-numeric-equality).
//
// conformance: semantics/label-filter-numeric-equality, loki_api_v1_query_range
func TestCompat_NumericEqualityLikeLoki(t *testing.T) {
	probe := fmt.Sprintf("numeric-eq-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	values := map[string][]string{
		"n": {"200", "200.0", "2e2", "0200", "2_00", "1KB", "1000ns", "0xC8", "0b11001000", "0o310", "abc", "201"},
		"d": {"5s", "5000ms", "5000000us", "5000000μs", "+5s", "05s", "5000000000", "1GB", "1m30s", "4s1000ms", "abc"},
		"b": {"1KB", "1kb", "1 KB", "1Kb", "1000", "1,000", "001KB", "1KB ", "1µs", "1_000", "0x3e8", "1e3", "2KB"},
	}
	var vlRows []string
	var lokiValues [][]any
	i := 0
	for _, key := range []string{"n", "d", "b"} {
		for _, v := range values[key] {
			ts := stamp.Add(time.Duration(i) * 100 * time.Millisecond)
			b, _ := json.Marshal(map[string]string{key: v, "row": strconv.Itoa(i)})
			line := string(b)
			row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "numeric-eq", "probe": probe})
			vlRows = append(vlRows, string(row))
			lokiValues = append(lokiValues, []any{strconv.FormatInt(ts.UnixNano(), 10), line})
			i++
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "numeric-eq", "probe": probe}, "values": lokiValues,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	window := func(query string) url.Values {
		return url.Values{"query": {query}, "start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}, "limit": {"1000"}}
	}
	lines := func(resp map[string]interface{}) []string {
		var out []string
		for _, s := range extractArray(extractMap(resp, "data"), "result") {
			v, _ := s.(map[string]interface{})["values"].([]interface{})
			for _, pair := range v {
				if p, _ := pair.([]interface{}); len(p) >= 2 {
					out = append(out, fmt.Sprint(p[1]))
				}
			}
		}
		sort.Strings(out)
		return out
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := window(fmt.Sprintf(`{probe=%q}`, probe))
		_, _, lp := queryRangeGET(t, lokiURL, all, headers)
		_, _, pp := queryRangeGET(t, proxyURL, all, headers)
		if len(lines(lp)) == i && len(lines(pp)) == i {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", len(lines(lp)), len(lines(pp)))
		}
		time.Sleep(time.Second)
	}
	for _, filter := range []string{
		`n = 200`, `n == 2e2`, `n != 200`, `d = 5s`, `d != 5s`, `d == 5000ms`, `b = 1KB`, `b != 1KB`, `b == 1000B`,
	} {
		t.Run(filter, func(t *testing.T) {
			query := fmt.Sprintf(`{probe=%q} | json | %s`, probe, filter)
			ls, lb, lp := queryRangeGET(t, lokiURL, window(query), headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, window(query), headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("%q: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
			}
			if l, p := strings.Join(lines(lp), "\n"), strings.Join(lines(pp), "\n"); l != p {
				t.Errorf("%q: proxy %d lines, Loki %d\nproxy %s\n Loki %s", query, len(lines(pp)), len(lines(lp)), p, l)
			}
		})
	}
}
