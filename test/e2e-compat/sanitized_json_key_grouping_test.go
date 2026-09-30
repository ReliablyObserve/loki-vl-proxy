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

// Grouping by or unwrapping a dotted JSON key that is not a stored
// VictoriaLogs field must give Loki's series, through the stats pushdown and
// the raw evaluator, for range and instant queries and for Logs Drilldown's
// tagged requests: a json expression label (the form Drilldown builds from
// detected_fields' jsonPath), a sanitized key and unwrap.
//
// conformance: profiles/sanitized-json-key-grouping
func TestCompat_SanitizedJSONKeyGroupingMatchesLoki(t *testing.T) {
	app := fmt.Sprintf("compat-grouping-%d", time.Now().UnixNano())
	pushStream(t, time.Now().Add(-30*time.Second), streamDef{
		Labels: map[string]string{"app": app, "env": "grouping", "level": "info"},
		Lines: []string{
			`{"msg":"a","gk.verb":"GET","gk.code":200,"go":{"gi":"v1"},"gx-hdr-id":"h1"}`,
			`{"msg":"b","gk.verb":"POST","gk.code":201,"go":{"gi":"v2"},"gx-hdr-id":"h2"}`,
			`{"msg":"c","gk.verb":"GET","gk.code":404,"go":{"gi":"v1"},"gx-hdr-id":"h1"}`,
		},
	})
	forceVLFlush(t)
	sel := `{app="` + app + `"}`
	waitForLokiMetricDataSelector(t, sel)

	end := time.Now().Truncate(time.Minute).Add(time.Minute)
	series := func(base, path string, q url.Values, headers map[string]string) map[string]float64 {
		t.Helper()
		status, body := rejectedQueryGet(t, base, path, q, "0", headers)
		if status != http.StatusOK {
			t.Fatalf("%s %s %s: %d %s", base, path, q.Get("query"), status, body)
		}
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][]any           `json:"values"`
					Value  []any             `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, s := range resp.Data.Result {
			vals := s.Values
			if len(s.Value) == 2 {
				vals = [][]any{s.Value}
			}
			var sum float64
			for _, v := range vals {
				f, _ := strconv.ParseFloat(v[1].(string), 64)
				sum += f
			}
			keys := make([]string, 0, len(s.Metric))
			for k, v := range s.Metric {
				keys = append(keys, k+"="+v)
			}
			sort.Strings(keys)
			out[strings.Join(keys, ",")] = sum
		}
		return out
	}
	rangeQ := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(end.Add(-10*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
	}
	// An instant query looks back over an hour, so the fixture stays in its
	// window however long Loki takes to settle.
	instantAt := time.Now().Add(time.Minute)
	instantQ := func(q string) url.Values {
		return url.Values{"query": {strings.ReplaceAll(q, "[60s]", "[1h]")}, "time": {strconv.FormatInt(instantAt.UnixNano(), 10)}}
	}
	equal := func(a, b map[string]float64) bool {
		if len(a) != len(b) {
			return false
		}
		for k, v := range a {
			if w, ok := b[k]; !ok || w != v {
				return false
			}
		}
		return true
	}
	// Loki answers a fresh stream partially for a while: take its series once
	// two polls agree and they are not empty.
	settle := 7 * time.Minute // the first query waits out the fresh-stream window
	lokiSeries := func(path string, q url.Values, headers map[string]string) map[string]float64 {
		want := series(lokiURL, path, q, headers)
		for deadline := time.Now().Add(settle); time.Now().Before(deadline); {
			time.Sleep(time.Second)
			next := series(lokiURL, path, q, headers)
			if len(next) > 0 && equal(next, want) {
				settle = 90 * time.Second
				return want
			}
			want = next
		}
		t.Fatalf("Loki did not settle on a non-empty answer for %s", q.Get("query"))
		return nil
	}

	queries := []string{
		`sum by (gk_verb) (count_over_time(` + sel + ` | json | gk_verb!="" [60s]))`,
		`sum by (gk_verb) (count_over_time(` + sel + ` | json | drop __error__,__error_details__ | gk_verb!="" [60s]))`,
		`sum by (gk_verb) (count_over_time(` + sel + ` | json | gk_verb="GET" [60s]))`,
		`sum by (v) (count_over_time(` + sel + ` | json v="[\"gk.verb\"]" | drop __error__,__error_details__ | v!="" [60s]))`,
		`sum by (v) (count_over_time(` + sel + ` | json v="go.gi" | v!="" [60s]))`,
		`sum by (go_gi) (count_over_time(` + sel + ` | json | go_gi!="" [60s]))`,
		`sum by (gx_hdr_id) (count_over_time(` + sel + ` | json | gx_hdr_id!="" [60s]))`,
		`sum(sum_over_time(` + sel + ` | json | unwrap gk_code [60s]))`,
		`sum by (gk_verb) (sum_over_time(` + sel + ` | json | unwrap gk_code [60s]))`,
		`sum(sum_over_time(` + sel + ` | json c="[\"gk.code\"]" | unwrap c [60s]))`,
		`sum by (v) (sum_over_time(` + sel + ` | json v="[\"gk.verb\"]", c="[\"gk.code\"]" | unwrap c [60s]))`,
	}
	drilldown := map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}
	proxies := map[string]string{"parity": proxyURL, "loki-compat": proxyUnderscoreURL, "drilldown": patternsAutodetectProxyURL}
	for _, q := range queries {
		for _, mode := range []struct {
			name, path string
			params     func(string) url.Values
		}{{"range", "/loki/api/v1/query_range", rangeQ}, {"instant", "/loki/api/v1/query", instantQ}} {
			for hname, headers := range map[string]map[string]string{"plain": nil, "drilldown-tagged": drilldown} {
				want := lokiSeries(mode.path, mode.params(q), headers)
				for pname, base := range proxies {
					if got := series(base, mode.path, mode.params(q), headers); !equal(got, want) {
						t.Errorf("%s %s %s via %s: %v, Loki %v\n  %s", mode.name, hname, pname, base, got, want, q)
					}
				}
			}
		}
	}

	// The Drilldown fields page: detected_fields names the key, then the
	// breakdown groups by the returned name through its jsonPath.
	params := url.Values{"query": {sel}, "start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	status, body := rejectedQueryGet(t, proxyUnderscoreURL, "/loki/api/v1/detected_fields", params, "0", nil)
	var df struct {
		Fields []detectedFieldEntry `json:"fields"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &df) != nil {
		t.Fatalf("detected_fields: %d %s", status, body)
	}
	var label, path string
	for _, f := range df.Fields {
		if f.Label == "gk_verb" && len(f.JSONPath) == 1 {
			label, path = f.Label, f.JSONPath[0]
		}
	}
	if label == "" {
		t.Fatalf("detected_fields has no gk_verb with a jsonPath: %s", body)
	}
	bq := `sum by (` + label + `) (count_over_time(` + sel + ` | json ` + label + `="[\"` + path + `\"]" | drop __error__,__error_details__ | ` + label + `!="" [60s]))`
	want := lokiSeries("/loki/api/v1/query_range", rangeQ(bq), drilldown)
	for pname, base := range proxies {
		if got := series(base, "/loki/api/v1/query_range", rangeQ(bq), drilldown); !equal(got, want) {
			t.Errorf("fields page breakdown via %s: %v, Loki %v", pname, got, want)
		}
	}
}
