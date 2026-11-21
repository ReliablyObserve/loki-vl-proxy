//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// A Logs Drilldown field breakdown asks VictoriaLogs to parse only the lines
// that can hold the field (a stored value, the field's word in the line, or a
// JSON \u escape that may spell the key) and to keep a stored value over the
// line's key. The answer must stay Loki's, for the `| json` breakdown and the
// `| json field="[\"field\"]"` form alike: structured metadata wins over a
// body key of the same name (Loki renames the parsed one to _extracted), a key
// spelled with an escape still counts, and lines without the field, with an
// empty value, a key that only contains the name, or no JSON do not.
//
// conformance: semantics/two-parsers-keep-one, limits/drilldown-breakdown-exact, parser-json, loki_api_v1_query_range
func TestRangeMetricCompatibilityDrilldownFieldBreakdown(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("drilldown-field-breakdown-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	var lines []jsonVolumeStreamLine
	for i := 0; i < 60; i++ { // one tick every 10s for 10 minutes
		tick := s0.Add(time.Duration(i) * 10 * time.Second)
		at := func(k int) time.Time { return tick.Add(time.Duration(k) * time.Second) }
		meta := func(pipeline, user string) map[string]string {
			return map[string]string{"pipeline": pipeline, "user_id": user}
		}
		lines = append(lines,
			jsonVolumeStreamLine{ts: at(0), msg: fmt.Sprintf(`{"pipeline":"p-body","user_id":"u-body","n":%d}`, i)},
			// The keys are spelled with \u escapes: pipe\u006cine, user\u005fid.
			jsonVolumeStreamLine{ts: at(1), msg: fmt.Sprintf("{\"pipe\\u006cine\":\"p-escaped\",\"user\\u005fid\":\"u-escaped\",\"n\":%d}", i)},
			jsonVolumeStreamLine{ts: at(2), msg: fmt.Sprintf(`{"msg":"stored only","n":%d}`, i), meta: meta("p-meta", "u-meta"), vlMeta: meta("p-meta", "u-meta")},
			jsonVolumeStreamLine{ts: at(3), msg: fmt.Sprintf(`{"pipeline":"p-shadowed","user_id":"u-shadowed","n":%d}`, i), meta: meta("p-stored", "u-stored"), vlMeta: meta("p-stored", "u-stored")},
			jsonVolumeStreamLine{ts: at(4), msg: fmt.Sprintf(`{"pipeline" : "p-spaced", "user_id" : "u-spaced", "n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(5), msg: fmt.Sprintf(`{"pipeline":"","user_id":"","n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(6), msg: fmt.Sprintf(`{"pipelines":"p-other","my_user_id":"u-other","n":%d}`, i)},
			jsonVolumeStreamLine{ts: at(7), msg: fmt.Sprintf(`pipeline=p-logfmt user_id=u-logfmt n=%d`, i)},
			jsonVolumeStreamLine{ts: at(8), msg: fmt.Sprintf(`{"pipeline":%d,"user_id":%d}`, i%3, i%2)},
		)
	}
	storedApp := app + "-stored-level"
	bodyApp := app + "-body-level"
	const bodyLine = `level=error msg="r1" amount=1`
	meta := map[string]string{"fixture": "stored-level-window"}
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{
		app:     lines,
		bodyApp: {{ts: s0, msg: bodyLine}},
		storedApp: {
			{ts: s0.Add(-time.Minute), level: "error", msg: "error on open boundary"},
			{ts: s0, level: "info", msg: "info at first evaluation", meta: meta, vlMeta: meta},
			{ts: s0.Add(30*time.Second + time.Millisecond), level: "error", msg: "error after final evaluation"},
			{ts: s0.Add(45 * time.Second), level: "error", msg: "error at request end"},
		},
	})
	instantSeries := func(t *testing.T, base, query string, at time.Time, headers map[string]string) map[string]map[string]string {
		t.Helper()
		params := url.Values{"query": {query}, "time": {at.Format(time.RFC3339Nano)}}
		status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), headers)
		if status != 200 || resp["status"] != "success" || resp["warnings"] != nil || resp["error"] != nil || extractMap(resp, "data")["resultType"] != "vector" {
			t.Fatalf("instant query: %d %s", status, body)
		}
		return alignLimitMatrix(t, resp)
	}
	t.Run("stored_level_window_limit", func(t *testing.T) {
		for _, fn := range []string{"count_over_time", "rate"} {
			query := fmt.Sprintf(`sum by (detected_level) (%s({app=%q} | json | logfmt | drop __error__, __error_details__ [1m]))`, fn, storedApp)
			for _, client := range []string{"plain", "drilldown"} {
				headers := map[string]string{"X-Scope-OrgID": "0"}
				if client == "drilldown" {
					headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
				}
				for _, endpoint := range []string{"query", "query_range"} {
					t.Run(fn+"/"+client+"/"+endpoint, func(t *testing.T) {
						route, rec := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
							cfg.DisableQueryPlanLowering = false
							cfg.TenantLimits = map[string]map[string]any{"0": {"max_query_series": 1}}
						})
						end := s0
						points := 1
						if endpoint == "query_range" {
							end = s0.Add(45 * time.Second)
							points = 2
						}
						read := func(base string) map[string]map[string]string {
							if endpoint == "query" {
								return instantSeries(t, base, query, s0, headers)
							}
							return slidingRangeSeries(t, base, query, s0, end, 30*time.Second, headers)
						}
						want := read(lokiURL)
						if len(want) != 1 || len(want[metricJSONKey(map[string]string{"detected_level": "info"})]) != points {
							t.Fatalf("unexpected Loki fixture: %v", want)
						}
						rec.reset()
						assertSlidingParity(t, query, want, read(route))
						rec.mu.Lock()
						windows := append([][2]time.Time(nil), rec.windows...)
						rec.mu.Unlock()
						calls := rec.reset()
						stats, guards := 0, 0
						for _, call := range calls {
							switch {
							case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
								if !strings.Contains(call, "copy detected_level as __dl_d, level as __dl_l") || !strings.Contains(call, "stats by (__dl_d, __dl_l) count()") || strings.Contains(call, "unpack_") {
									t.Fatalf("unexpected stats aggregation: %s", call)
								}
								stats++
							case strings.HasPrefix(call, "/select/logsql/query "):
								if !strings.Contains(call, "severity:*") || !strings.HasSuffix(call, " | limit 1") {
									t.Fatalf("unexpected raw fetch: %s", call)
								}
								guards++
							}
						}
						if stats != 1 || guards != 1 || len(windows) != 1 || !windows[0][0].Equal(s0.Add(-time.Minute)) || !windows[0][1].Equal(end.Add(time.Nanosecond)) {
							t.Fatalf("expected guarded stored-level stats over the full request, calls %v guard windows %v", calls, windows)
						}
						t.Logf("%d visible points, one stored-level stats read, one foreign-field guard over %s", points, windows[0][1].Sub(windows[0][0]))
						if fn == "count_over_time" && client == "plain" && endpoint == "query" {
							// Advance one millisecond past the final grid point so both
							// groups are visible. This proves the tenant cap is enforced.
							second := s0.Add(30*time.Second + time.Millisecond)
							if len(instantSeries(t, lokiURL, query, second, headers)) != 2 {
								t.Fatal("expected both stored levels at the control instant")
							}
							params := url.Values{"query": {query}, "time": {second.Format(time.RFC3339Nano)}}
							status, body, _ := doJSONGET(t, route+"/loki/api/v1/query?"+params.Encode(), headers)
							if status != 400 || !strings.Contains(body, "maximum number of series (1)") {
								t.Fatalf("tenant limit control: %d %s", status, body)
							}
						}
					})
				}
			}
		}
	})

	route, recorder := newJSONVolumeRouteProxy(t)
	t.Run("body_derived_level_error", func(t *testing.T) {
		selector := fmt.Sprintf(`{app=%q}`, bodyApp)
		raw := url.Values{"query": {selector}, "start": {s0.Format(time.RFC3339Nano)}, "end": {s0.Add(time.Second).Format(time.RFC3339Nano)}}
		status, body, row := doJSONGET(t, vlURL+"/select/logsql/query?"+raw.Encode(), nil)
		if status != http.StatusOK || row["_msg"] != bodyLine || row["level"] != nil || row["detected_level"] != nil {
			t.Fatalf("unexpected raw VL fixture: %d %s", status, body)
		}
		query := "sum by (missing) (count_over_time(" + selector + " | logfmt | json [1m]))"
		headers := map[string]string{"X-Scope-OrgID": "0"}
		for _, endpoint := range []string{"query", "query_range"} {
			params := url.Values{"query": {query}}
			if endpoint == "query" {
				params.Set("time", s0.Format(time.RFC3339Nano))
			} else {
				params.Set("start", s0.Format(time.RFC3339Nano))
				params.Set("end", s0.Add(30*time.Second).Format(time.RFC3339Nano))
				params.Set("step", "30")
			}
			path := "/loki/api/v1/" + endpoint + "?" + params.Encode()
			wantStatus, want, _ := doJSONGET(t, lokiURL+path, headers)
			if wantStatus != http.StatusBadRequest || !strings.Contains(want, "JSONParserErr") || !strings.Contains(want, `detected_level="error"`) {
				t.Fatalf("%s: unexpected Loki fixture: %d %s", endpoint, wantStatus, want)
			}
			gotStatus, got, _ := doJSONGET(t, route+path, headers)
			if gotStatus != wantStatus || got != want {
				t.Errorf("%s: proxy %d %q, Loki %d %q", endpoint, gotStatus, got, wantStatus, want)
			}
		}
	})
	drilldown := map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": "Source=grafana-lokiexplore-app"}
	start, end := s0.Add(-2*time.Minute), s0.Add(12*time.Minute)
	for _, pipeline := range []string{`| json | logfmt`, `| json user_id="[\"user_id\"]" | logfmt`, `| logfmt | json`} {
		t.Run(pipeline, func(t *testing.T) {
			query := `sum by (user_id) (count_over_time({app="` + app + `"} ` + pipeline + ` | drop __error__, __error_details__ | user_id!="" [1m]))`
			want := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, drilldown)
			for _, value := range []string{"u-body", "u-logfmt", "u-escaped", "u-stored"} {
				if len(want[metricJSONKey(map[string]string{"user_id": value})]) == 0 {
					t.Fatalf("missing Loki cohort %s", value)
				}
			}
			assertSlidingParity(t, query, want, slidingRangeSeries(t, route, query, start, end, time.Minute, drilldown))
			want = instantSeries(t, lokiURL, query, s0.Add(5*time.Minute), drilldown)
			if len(want) == 0 {
				t.Fatal("empty Loki instant fixture")
			}
			assertSlidingParity(t, query, want, instantSeries(t, route, query, s0.Add(5*time.Minute), drilldown))
		})
	}
	for _, tc := range []struct{ field, query string }{
		{"pipeline", `sum by (pipeline) (count_over_time({app="` + app + `"} | json | drop __error__, __error_details__ | pipeline!="" [1m]))`},
		{"user_id", `sum by (user_id) (count_over_time({app="` + app + `"} | json | drop __error__, __error_details__ | user_id!="" [1m]))`},
		{"user_id", `sum by (user_id) (count_over_time({app="` + app + `"} | json user_id="[\"user_id\"]" | drop __error__, __error_details__ | user_id!="" [1m]))`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, drilldown)
			// body, escaped, meta, stored and spaced, plus the three numeric values.
			prefix := strings.Split(tc.field, "_")[0][:1]
			for _, value := range []string{"body", "escaped", "meta", "stored", "spaced"} {
				if _, ok := loki[metricJSONKey(map[string]string{tc.field: prefix + "-" + value})]; !ok {
					t.Fatalf("Loki sanity: expected the %s-%s series, got %v", prefix, value, loki)
				}
			}
			if len(loki) != 5+map[string]int{"pipeline": 3, "user_id": 2}[tc.field] {
				t.Fatalf("Loki sanity: unexpected series %v", loki)
			}
			recorder.reset()
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, route, tc.query, start, end, time.Minute, drilldown))
			var stats []string
			for _, call := range recorder.reset() {
				if strings.HasPrefix(call, "/select/logsql/stats_query_range ") {
					stats = append(stats, call)
				}
			}
			parse := fmt.Sprintf(`| filter (%[1]s:* or _msg:"%[1]s" or _msg:~"\\\\u") | unpack_json if (-%[1]s:*) fields (%[1]s) | filter %[1]s:!""`, tc.field)
			if len(stats) != 1 || !strings.Contains(stats[0], parse) {
				t.Fatalf("expected one stats_query_range request parsing only the lines that can hold %s, got %v", tc.field, stats)
			}
		})
	}
}
