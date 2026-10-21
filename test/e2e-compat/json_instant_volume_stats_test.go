//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// instantVolumeVector asks /loki/api/v1/query and returns series (canonical
// label string) -> {sample time, value}, failing unless the answer is a
// successful vector without warnings.
func instantVolumeVector(t *testing.T, baseURL, query string, at time.Time) map[string][2]string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || resp["error"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", baseURL, query, status, body)
	}
	data := extractMap(resp, "data")
	if data["resultType"] != "vector" {
		t.Fatalf("%s %s: expected vector, got %s", baseURL, query, body)
	}
	out := map[string][2]string{}
	for _, item := range extractArray(data, "result") {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		keys := make([]string, 0, len(metric))
		for k, v := range metric {
			keys = append(keys, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(keys)
		key := strings.Join(keys, ",")
		pair, _ := series["value"].([]interface{})
		if len(pair) != 2 {
			t.Fatalf("%s %s: malformed sample %v", baseURL, query, series["value"])
		}
		ts, ok := pair[0].(float64)
		if !ok {
			t.Fatalf("%s %s: non-numeric timestamp %v", baseURL, query, pair[0])
		}
		if _, dup := out[key]; dup {
			t.Fatalf("%s %s: duplicate series {%s}", baseURL, query, key)
		}
		out[key] = [2]string{strconv.FormatFloat(ts, 'f', -1, 64), fmt.Sprint(pair[1])}
	}
	return out
}

// An instant `| json | drop __error__` volume (the form dashboards, alert rules
// and Explore's table view send to /loki/api/v1/query) is answered from one
// VictoriaLogs stats bucket (t-range, t] instead of every raw row of its
// window, and must equal Loki's vector: the same series, values and
// evaluation time, whichever window length, function and grouping.
//
// The in-process proxy records its backend requests: the answer is one stats
// answer with no raw-row read, counted as vl_stats_buckets/pushdown.
// conformance: semantics/json-instant-volume-stats
func TestRangeMetricCompatibilityInstantJSONVolumeStats(t *testing.T) {
	ensureDataIngested(t)
	waitForLokiMetricData(t)

	now := time.Now()
	app := fmt.Sprintf("json-instant-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: jsonFilterFixture(s0)})

	route, recorder := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "patterns-autodetect": patternsAutodetectProxyURL, "in-process": route}
	selector := `{app="` + app + `"}`
	for _, tc := range []struct {
		name, query string
		at          time.Time
	}{
		{"explore volume 5m", `sum by (level, detected_level) (count_over_time(` + selector + ` | json | drop __error__ [5m]))`, s0.Add(5 * time.Minute)},
		{"explore volume 5m, window cut by data", `sum by (level, detected_level) (count_over_time(` + selector + ` | json | drop __error__ [5m]))`, s0.Add(7 * time.Minute)},
		{"explore volume 1h", `sum by (level, detected_level) (count_over_time(` + selector + ` | json | drop __error__ [1h]))`, s0.Add(10 * time.Minute)},
		{"grouped rate", `sum by (level) (rate(` + selector + ` | json | drop __error__ [2m]))`, s0.Add(4 * time.Minute)},
		{"bytes volume", `sum by (level, detected_level) (bytes_over_time(` + selector + ` | json | drop __error__ [5m]))`, s0.Add(5 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := instantVolumeVector(t, lokiURL, tc.query, tc.at)
			if len(loki) < 2 {
				t.Fatalf("Loki sanity: expected at least 2 series, got %d: %v", len(loki), loki)
			}
			for name, base := range targets {
				recorder.reset()
				before := parserMetricEvaluations(t, route)
				got := instantVolumeVector(t, base, tc.query, tc.at)
				for key, want := range loki {
					if have, ok := got[key]; !ok || have != want {
						t.Errorf("%s [%s] {%s}: proxy %v (present=%v), Loki %v", tc.query, name, key, have, ok, want)
					}
				}
				for key, have := range got {
					if _, ok := loki[key]; !ok {
						t.Errorf("%s [%s]: proxy series {%s} = %v absent in Loki", tc.query, name, key, have)
					}
				}
				if name != "in-process" {
					continue
				}
				after := parserMetricEvaluations(t, route)
				raw := 0
				for _, call := range recorder.reset() {
					switch {
					case isStoredLevelAttempt(call), isLevelPresenceCall(call):
					case strings.HasPrefix(call, "/select/logsql/query ") && strings.HasSuffix(call, " | limit 1"): // a parse-risk probe
					case strings.HasPrefix(call, "/select/logsql/query "):
						raw++
					}
				}
				if statsDelta, rawDelta := after["vl_stats_buckets/pushdown"]-before["vl_stats_buckets/pushdown"], after["raw_rows/probe"]-before["raw_rows/probe"]+after["raw_rows/ineligible"]-before["raw_rows/ineligible"]+after["raw_rows/grid"]-before["raw_rows/grid"]; raw != 0 || statsDelta != 1 || rawDelta != 0 {
					t.Fatalf("expected one stats answer and no raw rows: raw reads=%d counter vl_stats_buckets+%d raw_rows+%d", raw, statsDelta, rawDelta)
				}
			}
		})
	}
}
