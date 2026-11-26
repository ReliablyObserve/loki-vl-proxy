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

// TestCompat_KeepNamesAndMatchersLikeLoki: a keep stage keeps a label listed
// bare or matched by one of its matchers on that name and removes every other
// one, on log entries and on the series of a metric query; later stages do not
// read a removed value. The fixture is two streams with different level and zone
// labels and JSON lines, ingested alike into Loki and VictoriaLogs.
// conformance: semantics/keep-names-and-matchers, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_KeepNamesAndMatchersLikeLoki(t *testing.T) {
	probe := fmt.Sprintf("keep-matchers-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	var vlRows []string
	var lokiStreams []any
	for i, f := range []struct{ level, zone, line string }{
		// The line's level key is renamed level_extracted beside the stream label.
		{"error", "z1", `{"msg":"boom","code":"500","level":"debug"}`},
		{"warn", "z2", `{"msg":"slow","code":"200"}`},
		{"warn", "z2", `{"msg":"busy","code":"500"}`},
	} {
		ts := stamp.Add(time.Duration(i) * time.Second)
		labels := map[string]string{"app": "keep-matchers", "probe": probe, "level": f.level, "zone": f.zone}
		row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": f.line}
		for k, v := range labels {
			row[k] = v
		}
		encoded, _ := json.Marshal(row)
		vlRows = append(vlRows, string(encoded))
		lokiStreams = append(lokiStreams, map[string]any{
			"stream": labels, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), f.line}},
		})
	}
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields=app,probe,level,zone", strings.Join(vlRows, "\n")+"\n",
		map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push),
		map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	sel := fmt.Sprintf(`{probe=%q}`, probe)
	start := strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)
	end := strconv.FormatInt(stamp.Add(time.Minute).Unix(), 10)
	// canonical is the answer's result as sorted JSON, one element per stream or series.
	canonical := func(parsed map[string]interface{}) []string {
		var out []string
		for _, item := range extractArray(extractMap(parsed, "data"), "result") {
			encoded, _ := json.Marshal(item)
			out = append(out, string(encoded))
		}
		sort.Strings(out)
		return out
	}
	compare := func(t *testing.T, path string, params url.Values, headers map[string]string, wantData bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for {
			ls, lb := hardeningRequest(t, http.MethodGet, lokiURL+path+"?"+params.Encode(), "", headers)
			ps, pb := hardeningRequest(t, http.MethodGet, proxyURL+path+"?"+params.Encode(), "", headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("%s Loki=%d %s proxy=%d %s", params.Get("query"), ls, lb, ps, pb)
			}
			var lp, pp map[string]interface{}
			if err := json.Unmarshal(lb, &lp); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(pb, &pp); err != nil {
				t.Fatal(err)
			}
			loki, proxy := canonical(lp), canonical(pp)
			if (len(loki) > 0) == wantData && reflect.DeepEqual(loki, proxy) {
				return
			}
			// Both sides may still be taking the fixture in (Loki's metric blank window on a fresh stack).
			if time.Now().After(deadline) {
				t.Fatalf("%s:\nLoki  %v\nproxy %v", params.Get("query"), loki, proxy)
			}
			time.Sleep(2 * time.Second)
		}
	}
	// The bare selector first: both sides hold the three lines.
	compare(t, "/loki/api/v1/query_range", url.Values{"query": {sel}, "start": {start}, "end": {end}, "limit": {"10"}},
		map[string]string{"X-Scope-OrgID": "0", "X-Loki-Response-Encoding-Flags": "categorize-labels"}, true)

	for _, tc := range []struct {
		pipeline string
		empty    bool
	}{
		{pipeline: `| keep level="error", app`},
		{pipeline: `| keep app, level!="error"`},
		{pipeline: `| keep level="error"`},
		{pipeline: `| keep level="warn", level="error", zone`},
		{pipeline: `| keep app, app="nope"`},
		{pipeline: `| keep app, level | keep level="warn"`},
		{pipeline: `| keep level="error", app | level="warn"`, empty: true},
		{pipeline: `| keep level="error", app | level!="warn"`},
		{pipeline: `| keep level="warn", app | drop level="warn"`},
		{pipeline: `| keep level="error", app | json`},
		{pipeline: `| keep app, level="a=b|!"`},
		{pipeline: `| json | keep code="500", msg`},
		{pipeline: `| json | keep level, app | level="error"`},
		{pipeline: `| json | keep level="error", app | level="error"`},
		{pipeline: `| json | keep level="warn", app | level!="warn"`},
		{pipeline: `| json | keep level="debug", app | level="debug"`, empty: true},
	} {
		for _, categorized := range []bool{false, true} {
			t.Run(fmt.Sprintf("logs/%s/categorized=%v", tc.pipeline, categorized), func(t *testing.T) {
				headers := map[string]string{"X-Scope-OrgID": "0"}
				if categorized {
					headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
				}
				compare(t, "/loki/api/v1/query_range",
					url.Values{"query": {sel + " " + tc.pipeline}, "start": {start}, "end": {end}, "limit": {"10"}}, headers, !tc.empty)
			})
		}
	}
	for _, query := range []string{
		`count_over_time(` + sel + ` | keep app, level="error" [5m])`,
		`rate(` + sel + ` | keep zone!="z1" [5m])`,
		`sum by (zone) (count_over_time(` + sel + ` | keep app, zone="z1" [5m]))`,
		`sum by (level) (count_over_time(` + sel + ` | keep level="error", app | level!="warn" [5m]))`,
		`count_over_time(` + sel + ` | keep zone="z1" | keep app [5m])`,
		`count_over_time(` + sel + ` | json | keep code="500", app [5m])`,
		`count(count_over_time(` + sel + ` | keep level="error" [5m]))`,
		`count(count_over_time(` + sel + ` | json | keep code="500" [5m]))`,
		`count_over_time(` + sel + ` | keep zone="z1", level="a=b" [5m])`,
	} {
		t.Run("instant/"+query, func(t *testing.T) {
			compare(t, "/loki/api/v1/query", url.Values{"query": {query}, "time": {end}},
				map[string]string{"X-Scope-OrgID": "0"}, true)
		})
		t.Run("range/"+query, func(t *testing.T) {
			compare(t, "/loki/api/v1/query_range",
				url.Values{"query": {query}, "start": {strconv.FormatInt(stamp.Add(-time.Minute).Unix(), 10)}, "end": {end}, "step": {"60"}},
				map[string]string{"X-Scope-OrgID": "0"}, true)
		})
	}
}
