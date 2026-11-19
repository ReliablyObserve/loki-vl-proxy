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

// TestCompat_MarkerBytesLabelValueLikeLoki: a label filter whose value holds \x02 bytes, in front of a parser and a
// filter on an `_extracted` name, keeps the lines Loki keeps. The string translator cut such a value at the bytes it
// marks its stages with and filtered on another value; the query plan keeps the value. The fixture is one stream of
// json lines with a structured metadata (VictoriaLogs: field) `o` that holds the value, an empty one, none, or the
// value plus a byte, ingested alike into Loki and VictoriaLogs.
//
// conformance: semantics/translator-reads-marker-bytes-in-quoted-value, loki_api_v1_query_range
func TestCompat_MarkerBytesLabelValueLikeLoki(t *testing.T) {
	probe := fmt.Sprintf("marker-bytes-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	const value = "\x02a\x02"
	rows := []struct {
		o   string
		set bool
	}{{value, true}, {"", true}, {"", false}, {value + "x", true}}
	var vlRows []string
	var values [][]any
	for i, r := range rows {
		ts := stamp.Add(time.Duration(i) * time.Second)
		line := fmt.Sprintf(`{"K":"k%d","msg":"row %d"}`, i, i)
		vl := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "marker-bytes", "probe": probe}
		entry := []any{strconv.FormatInt(ts.UnixNano(), 10), line}
		if r.set {
			vl["o"] = r.o
			entry = append(entry, map[string]string{"o": r.o})
		}
		row, _ := json.Marshal(vl)
		vlRows = append(vlRows, string(row))
		values = append(values, entry)
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "marker-bytes", "probe": probe}, "values": values,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	window := func(query string) url.Values {
		return url.Values{"query": {query}, "start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}, "limit": {"100"}}
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
		return out
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := window(fmt.Sprintf(`{probe=%q}`, probe))
		_, _, lp := queryRangeGET(t, lokiURL, all, headers)
		_, _, pp := queryRangeGET(t, proxyURL, all, headers)
		if len(lines(lp)) == len(rows) && len(lines(pp)) == len(rows) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", len(lines(lp)), len(lines(pp)))
		}
		time.Sleep(time.Second)
	}
	for _, tc := range []struct {
		op   string
		want int // the lines Loki keeps
	}{{"=", 1}, {"!=", 3}} {
		query := fmt.Sprintf(`{probe=%q} | o%s%s | json | K_extracted!~"p|q"`, probe, tc.op, strconv.Quote(value))
		ls, lb, lp := queryRangeGET(t, lokiURL, window(query), headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, window(query), headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("%q: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		if got := lines(lp); len(got) != tc.want {
			t.Fatalf("%q: Loki answered %d lines, the fixture holds %d: %s", query, len(got), tc.want, lb)
		}
		if l, p := strings.Join(lines(lp), "\n"), strings.Join(lines(pp), "\n"); len(lines(pp)) != len(lines(lp)) {
			t.Errorf("%q: proxy %d lines, Loki %d\nproxy %s\n Loki %s", query, len(lines(pp)), len(lines(lp)), p, l)
		}
	}
}
