//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Loki's stddev and stdvar are vector aggregations: each step they take the
// population standard deviation or variance of the VALUES of the series of a
// group (pkg/logql/evaluator.go VectorAggEvaluator.Next, Welford's update, 0 for
// a group of one series), the group being what `by` keeps or `without` leaves.
// They are not stddev_over_time, which works over the samples inside a window.
// The proxy answered the total line count of the group for `by` and no
// resultType for some instant `without` forms
// (semantics/stddev-stdvar-grouped-values).

type stddevGroupedFixtures struct {
	selector string
	s0       time.Time
}

var (
	stddevGroupedOnce sync.Once
	stddevGroupedFx   *stddevGroupedFixtures
)

// stddevGroupedStreams: three services, six pods; a service of one pod makes a
// single-series group (stddev 0). Pod p lines come every (p+1)*10s so the counts
// of the series differ and change with the window.
var stddevGroupedStreams = []struct{ service, pod string }{
	{"a", "p0"}, {"a", "p1"}, {"a", "p2"},
	{"b", "p3"},
	{"c", "p4"}, {"c", "p5"},
}

// ensureStddevGroupedFixtures ingests the same lines into Loki (with
// detected_level=unknown as structured metadata) and VictoriaLogs (as a stream
// field), so every label set the metrics carry is identical on both backends.
func ensureStddevGroupedFixtures(t *testing.T) *stddevGroupedFixtures {
	t.Helper()
	stddevGroupedOnce.Do(func() {
		now := time.Now()
		id := now.UnixNano()
		fx := &stddevGroupedFixtures{
			selector: fmt.Sprintf(`{service_name=~"stdgrp-%d-.+"}`, id),
			s0:       now.Add(-11 * time.Hour).Truncate(time.Hour),
		}
		var vlRows strings.Builder
		var streams []any
		total := 0
		for k, st := range stddevGroupedStreams {
			labels := map[string]string{"service_name": fmt.Sprintf("stdgrp-%d-%s", id, st.service), "pod": st.pod}
			values := [][]any{}
			// Off the whole-10s marks and each stream on its own offset, so no
			// line sits on a window edge.
			for i := 0; i < 180; i += k + 1 {
				ts := fx.s0.Add(time.Duration(3+2*k)*time.Second + time.Duration(i)*10*time.Second)
				msg := fmt.Sprintf("tick %d", i)
				fields := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": msg, "detected_level": "unknown"}
				for name, value := range labels {
					fields[name] = value
				}
				row, _ := json.Marshal(fields)
				vlRows.Write(row)
				vlRows.WriteByte('\n')
				values = append(values, []any{strconv.FormatInt(ts.UnixNano(), 10), msg, map[string]string{"detected_level": "unknown"}})
				total++
			}
			streams = append(streams, map[string]any{"stream": labels, "values": values})
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,pod,detected_level", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": streams})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		forceVLFlush(t)
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
			t.Fatalf("Loki flush: %d %s", status, body)
		}
		// Both backends must hold every line before any comparison.
		deadline := time.Now().Add(240 * time.Second)
		for {
			lokiCount, vlCount := 0, 0
			vlParams := url.Values{"query": {fx.selector}, "start": {fx.s0.UTC().Format(time.RFC3339Nano)}, "end": {fx.s0.Add(40 * time.Minute).UTC().Format(time.RFC3339Nano)}, "limit": {"100000"}}
			if status, body := hardeningRequest(t, http.MethodGet, vlURL+"/select/logsql/query?"+vlParams.Encode(), "", nil); status == http.StatusOK {
				vlCount = strings.Count(string(body), "\n")
			}
			lokiParams := url.Values{"query": {fmt.Sprintf("sum(count_over_time(%s[40m]))", fx.selector)}, "time": {strconv.FormatInt(fx.s0.Add(40*time.Minute).UnixNano(), 10)}}
			if _, _, resp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+lokiParams.Encode(), map[string]string{"X-Scope-OrgID": "0"}); resp["status"] == "success" && resp["warnings"] == nil {
				for _, item := range extractArray(extractMap(resp, "data"), "result") {
					if value, ok := item.(map[string]interface{})["value"].([]interface{}); ok && len(value) == 2 {
						lokiCount, _ = strconv.Atoi(fmt.Sprint(value[1]))
					}
				}
			}
			if lokiCount == total && vlCount == total {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("backends not healthy: fixture=%d loki=%d victorialogs=%d", total, lokiCount, vlCount)
			}
			time.Sleep(2 * time.Second)
		}
		stddevGroupedFx = fx
	})
	if stddevGroupedFx == nil {
		t.Fatal("the stddev fixtures were not ingested; see the first test that ran")
	}
	return stddevGroupedFx
}

// TestCompat_StddevStdvarGrouped: stddev and stdvar over a range aggregation,
// with `by`, `without`, either clause position and no grouping, answer Loki's
// groups, labels and values on a sliding range, a tumbling range and an instant
// query. The fixture has a service of one pod (a group of one series: 0), and
// Loki's answers are checked non-empty (parity would otherwise be empty-vs-empty).
// conformance: semantics/stddev-stdvar-grouped-values, metric-series-identity, window-bounds-and-step-alignment, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_StddevStdvarGrouped(t *testing.T) {
	fx := ensureStddevGroupedFixtures(t)
	type shape struct {
		name        string
		window      string
		step        time.Duration
		start, stop time.Duration
	}
	shapes := []shape{
		{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
		{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
		{"range longer than the step", "10m", 3 * time.Minute, 12 * time.Minute, 27 * time.Minute},
		{"instant", "5m", 0, 0, 20 * time.Minute},
	}
	groupings := []struct{ name, pre, post string }{
		{"ungrouped", "", ""},
		{"by service", "by (service_name)", ""},
		{"by service trailing", "", "by (service_name)"},
		{"by service and pod", "by (service_name, pod)", ""},
		{"by ()", "by ()", ""},
		{"without pod", "without (pod)", ""},
		{"without service trailing", "", "without (service_name)"},
		{"without ()", "without ()", ""},
	}
	// Loki waits for its first head block cut; a cold stack answers metrics empty.
	waitFor := fx.s0.Add(20 * time.Minute)
	deadline := time.Now().Add(240 * time.Second)
	for len(unwrapInstant(t, lokiURL, fmt.Sprintf("count_over_time(%s[5m])", fx.selector), waitFor)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Loki's metric path answered nothing for the fixture after 240s")
		}
		time.Sleep(2 * time.Second)
	}
	for _, sh := range shapes {
		for _, op := range []string{"stddev", "stdvar"} {
			for _, g := range groupings {
				operand := fmt.Sprintf("count_over_time(%s[%s])", fx.selector, sh.window)
				query := fmt.Sprintf("%s %s (%s) %s", op, g.pre, operand, g.post)
				t.Run(sh.name+"/"+op+"/"+g.name, func(t *testing.T) {
					run := func(base string) map[string]map[string]string {
						if sh.step == 0 {
							at := fx.s0.Add(sh.stop)
							out := map[string]map[string]string{}
							for key, value := range unwrapInstant(t, base, query, at) {
								out[key] = map[string]string{strconv.FormatInt(at.Unix(), 10): value}
							}
							return out
						}
						return slidingRangeSeries(t, base, query, fx.s0.Add(sh.start), fx.s0.Add(sh.stop), sh.step, nil)
					}
					loki, proxy := run(lokiURL), run(proxyURL)
					compareUnwrapSumSeries(t, query, loki, proxy)
					if g.name == "by service" && op == "stddev" {
						// The pod of service b is alone: its group is 0, not its count.
						for key, points := range loki {
							if !strings.Contains(key, "service_name=stdgrp-") || !strings.HasSuffix(key, "-b") {
								continue
							}
							for ts, v := range points {
								if v != "0" {
									t.Fatalf("%s {%s} t=%s: Loki sanity: a group of one series has stddev %s, want 0", query, key, ts, v)
								}
							}
						}
					}
				})
			}
		}
	}
}

// stddevInstantResultType returns data.resultType of an instant query, failing
// unless the answer is a successful one without warnings.
func stddevInstantResultType(t *testing.T, base, query string, at time.Time) string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil {
		t.Fatalf("%s %s: unhealthy answer %d %s", base, query, status, body)
	}
	resultType, _ := extractMap(resp, "data")["resultType"].(string)
	return resultType
}

// TestCompat_StddevStdvarGroupedInstantResultType: an instant stddev or stdvar
// answers a vector with its resultType on both backends, over a window that
// holds lines and over one that holds none (registry cluster ce65447e,
// `stdvar without (app) (count_over_time(({namespace="prod"} |~".+bar")[1m]))`,
// where the proxy answered no resultType).
// conformance: semantics/stddev-stdvar-grouped-values, loki_api_v1_query
func TestCompat_StddevStdvarGroupedInstantResultType(t *testing.T) {
	fx := ensureStddevGroupedFixtures(t)
	empty := fmt.Sprintf(`{service_name=~"stdgrp-%d-nothing"}`, time.Now().UnixNano())
	for _, tc := range []struct{ name, selector string }{{"lines", fx.selector}, {"no lines", empty}} {
		for _, op := range []string{"stddev", "stdvar"} {
			for _, grouping := range []string{"without (pod)", "without (app)", "by (service_name)", "by ()", ""} {
				query := fmt.Sprintf(`%s %s (count_over_time((%s |~".+")[1m]))`, op, grouping, tc.selector)
				t.Run(tc.name+"/"+op+"/"+grouping, func(t *testing.T) {
					at := fx.s0.Add(20 * time.Minute)
					for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
						if got := stddevInstantResultType(t, base, query, at); got != "vector" {
							t.Errorf("%s answered resultType %q for %s, want vector", name, got, query)
						}
					}
				})
			}
		}
	}
}

// TestCompat_StddevStdvarGroupedRegistryQueries runs the row's own request and
// the example queries of its parity clusters, on the generator's shared data,
// range and instant, and compares the proxy with Loki: series, labels, values.
// conformance: semantics/stddev-stdvar-grouped-values, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_StddevStdvarGroupedRegistryQueries(t *testing.T) {
	end := time.Now().Add(-time.Minute).Truncate(time.Minute)
	for _, query := range []string{
		`stddev by (app) (count_over_time({env="production"}[5m]))`,
		`stdvar by(app)(count_over_time({env="production"}[5m]))`,
		`stddev by(app)(count_over_time({env="production"}[5m]))`,
		`stddev by(level)(count_over_time({app="api-gateway",env="production"}[5m]))`,
	} {
		t.Run("range/"+query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, end.Add(-30*time.Minute), end, time.Minute, nil)
			proxy := slidingRangeSeries(t, proxyURL, query, end.Add(-30*time.Minute), end, time.Minute, nil)
			compareUnwrapSumSeries(t, query, loki, proxy)
		})
		t.Run("instant/"+query, func(t *testing.T) {
			loki := map[string]map[string]string{}
			proxy := map[string]map[string]string{}
			for key, value := range unwrapInstant(t, lokiURL, query, end) {
				loki[key] = map[string]string{strconv.FormatInt(end.Unix(), 10): value}
			}
			for key, value := range unwrapInstant(t, proxyURL, query, end) {
				proxy[key] = map[string]string{strconv.FormatInt(end.Unix(), 10): value}
			}
			compareUnwrapSumSeries(t, query, loki, proxy)
		})
	}
}
