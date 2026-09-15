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
	"sync"
	"testing"
	"time"
)

type slidingLiveLine struct {
	ts  time.Time
	msg string
}

type slidingLiveFixture struct {
	app   string
	lines []slidingLiveLine
	// service names the stream with a service_name label and stores
	// detected_level=unknown on both backends (Loki structured metadata, a
	// VictoriaLogs stream field), so bare range metrics, which keep every
	// label, have identical label sets. Otherwise the stream label is app.
	service bool
}

// streamLabels returns the fixture's stream labels as pushed to Loki.
func (fx slidingLiveFixture) streamLabels() map[string]string {
	if fx.service {
		return map[string]string{"service_name": fx.app}
	}
	return map[string]string{"app": fx.app}
}

// selector returns the LogQL stream selector for the fixture.
func (fx slidingLiveFixture) selector() string {
	for name, value := range fx.streamLabels() {
		return `{` + name + `="` + value + `"}`
	}
	return ""
}

type slidingLiveFixtures struct {
	gap, edge                        slidingLiveFixture
	bareLogfmt, bareJSON, bareUnwrap slidingLiveFixture
	hour0, s0                        time.Time
}

var (
	slidingFixturesOnce sync.Once
	slidingFixtures     *slidingLiveFixtures
)

// ensureSlidingFixtures ingests the sliding fixtures once per test binary,
// so every test shares one flush and one readiness poll.
func ensureSlidingFixtures(t *testing.T) *slidingLiveFixtures {
	t.Helper()
	slidingFixturesOnce.Do(func() {
		now := time.Now()
		fx := &slidingLiveFixtures{
			gap:        slidingLiveFixture{app: fmt.Sprintf("sliding-gap-%d", now.UnixNano())},
			edge:       slidingLiveFixture{app: fmt.Sprintf("sliding-edge-%d", now.UnixNano())},
			bareLogfmt: slidingLiveFixture{app: fmt.Sprintf("sliding-bare-logfmt-%d", now.UnixNano()), service: true},
			bareJSON:   slidingLiveFixture{app: fmt.Sprintf("sliding-bare-json-%d", now.UnixNano()), service: true},
			bareUnwrap: slidingLiveFixture{app: fmt.Sprintf("sliding-bare-unwrap-%d", now.UnixNano()), service: true},
			hour0:      now.Add(-20 * time.Hour).Truncate(time.Hour),
			s0:         now.Add(-10 * time.Hour).Truncate(time.Hour),
		}
		for _, h := range []int{0, 5} {
			for i := 0; i < 60; i++ {
				ts := fx.hour0.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Minute + 7*time.Second)
				fx.gap.lines = append(fx.gap.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("gap line %d %s", h*100+i, strings.Repeat("x", i%7))})
			}
		}
		for i := 0; i < 180; i++ { // one line every 10s for 30 minutes, on whole 10s marks
			ts := fx.s0.Add(time.Duration(i) * 10 * time.Second)
			fx.edge.lines = append(fx.edge.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("edge line %03d", i)})
			// Bare parser metrics keep parsed labels, so every line parses to the
			// same label set while line sizes vary: a logfmt key without a value
			// adds no label, and JSON whitespace adds no field.
			pad := strings.Repeat("x", i%4)
			fx.bareLogfmt.lines = append(fx.bareLogfmt.lines, slidingLiveLine{ts: ts, msg: "tick" + pad})
			fx.bareJSON.lines = append(fx.bareJSON.lines, slidingLiveLine{ts: ts, msg: `{` + strings.Repeat(" ", i%4) + `"msg":"tick"}`})
			fx.bareUnwrap.lines = append(fx.bareUnwrap.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("n=%d", i%7)})
		}
		ingestSlidingFixtures(t, fx.gap, fx.edge, fx.bareLogfmt, fx.bareJSON, fx.bareUnwrap)
		slidingFixtures = fx
	})
	if slidingFixtures == nil {
		t.Fatal("sliding fixtures were not ingested; see the first test that ran")
	}
	return slidingFixtures
}

// ingestSlidingFixtures writes byte-identical lines with one stream label per
// fixture into Loki and VictoriaLogs, flushes both, and proves both backends
// hold exactly every fixture before any comparison.
func ingestSlidingFixtures(t *testing.T, fixtures ...slidingLiveFixture) {
	t.Helper()
	var vlRows strings.Builder
	streams := make([]any, 0, len(fixtures))
	for _, fx := range fixtures {
		values := make([][]any, 0, len(fx.lines))
		for _, line := range fx.lines {
			fields := map[string]string{"_time": line.ts.UTC().Format(time.RFC3339Nano), "_msg": line.msg}
			for name, value := range fx.streamLabels() {
				fields[name] = value
			}
			value := []any{strconv.FormatInt(line.ts.UnixNano(), 10), line.msg}
			if fx.service {
				fields["detected_level"] = "unknown"
				value = append(value, map[string]string{"detected_level": "unknown"})
			}
			row, _ := json.Marshal(fields)
			vlRows.Write(row)
			vlRows.WriteByte('\n')
			values = append(values, value)
		}
		streams = append(streams, map[string]any{"stream": fx.streamLabels(), "values": values})
	}
	// Rows without a listed field leave it out of their stream.
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,service_name,detected_level", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	// The fixtures are older than query_ingesters_within, so Loki only serves
	// them after the ingester flushes them to the store.
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}

	deadline := time.Now().Add(180 * time.Second)
	for {
		pending := ""
		for _, fx := range fixtures {
			lokiCount, vlCount := slidingFixtureCounts(t, fx)
			if lokiCount != len(fx.lines) || vlCount != len(fx.lines) {
				pending += fmt.Sprintf(" %s: fixture=%d loki=%d victorialogs=%d;", fx.app, len(fx.lines), lokiCount, vlCount)
			}
		}
		if pending == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends not healthy:%s", pending)
		}
		time.Sleep(time.Second)
	}
}

// slidingFixtureCounts counts the fixture natively on VictoriaLogs and through
// a warning-free Loki instant query covering every line.
func slidingFixtureCounts(t *testing.T, fx slidingLiveFixture) (lokiCount, vlCount int) {
	t.Helper()
	first, last := fx.lines[0].ts, fx.lines[len(fx.lines)-1].ts
	selector := fx.selector()
	vlParams := url.Values{"query": {selector}, "start": {first.UTC().Format(time.RFC3339Nano)}, "end": {last.Add(time.Second).UTC().Format(time.RFC3339Nano)}, "limit": {"100000"}}
	status, body := hardeningRequest(t, http.MethodGet, vlURL+"/select/logsql/query?"+vlParams.Encode(), "", nil)
	if status != http.StatusOK {
		t.Fatalf("VL count query: %d %s", status, body)
	}
	vlCount = strings.Count(string(body), "\n")

	window := last.Sub(first) + time.Minute
	lokiParams := url.Values{"query": {fmt.Sprintf("sum(count_over_time(%s[%ds]))", selector, int(window.Seconds()))}, "time": {strconv.FormatInt(last.Add(time.Second).UnixNano(), 10)}}
	_, _, resp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+lokiParams.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if resp["status"] == "success" && resp["warnings"] == nil {
		for _, item := range extractArray(extractMap(resp, "data"), "result") {
			if value, ok := item.(map[string]interface{})["value"].([]interface{}); ok && len(value) == 2 {
				n, _ := strconv.Atoi(fmt.Sprint(value[1]))
				lokiCount += n
			}
		}
	}
	return lokiCount, vlCount
}

// slidingRangeSeries runs query_range and returns series → timestamp → value,
// failing unless the response is a successful matrix without warnings.
func slidingRangeSeries(t *testing.T, baseURL, query string, start, end time.Time, step time.Duration, headers map[string]string) map[string]map[string]string {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	}
	all := map[string]string{"X-Scope-OrgID": "0"}
	for k, v := range headers {
		all[k] = v
	}
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), all)
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || resp["error"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", baseURL, query, status, body)
	}
	data := extractMap(resp, "data")
	if data["resultType"] != "matrix" {
		t.Fatalf("%s %s: expected matrix, got %s", baseURL, query, body)
	}
	out := map[string]map[string]string{}
	for _, item := range extractArray(data, "result") {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		keys := make([]string, 0, len(metric))
		for k, v := range metric {
			keys = append(keys, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(keys)
		key := strings.Join(keys, ",")
		if out[key] == nil {
			out[key] = map[string]string{}
		}
		values, _ := series["values"].([]interface{})
		for _, raw := range values {
			pair, _ := raw.([]interface{})
			if len(pair) != 2 {
				t.Fatalf("%s %s: malformed sample %v", baseURL, query, raw)
			}
			ts, ok := pair[0].(float64)
			if !ok {
				t.Fatalf("%s %s: non-numeric timestamp %v", baseURL, query, pair[0])
			}
			out[key][strconv.FormatFloat(ts, 'f', -1, 64)] = fmt.Sprint(pair[1])
		}
	}
	return out
}

func assertSlidingParity(t *testing.T, query string, loki, proxy map[string]map[string]string) {
	t.Helper()
	lokiPoints := 0
	for _, points := range loki {
		lokiPoints += len(points)
	}
	if lokiPoints == 0 {
		t.Fatalf("%s: Loki returned no samples; parity would be empty-vs-empty", query)
	}
	for key, want := range loki {
		got := proxy[key]
		if len(got) != len(want) {
			t.Errorf("%s {%s}: proxy has %d samples, Loki %d", query, key, len(got), len(want))
		}
		for ts, wv := range want {
			if gv, ok := got[ts]; !ok || gv != wv {
				t.Errorf("%s {%s} t=%s: proxy %q (present=%v), Loki %q", query, key, ts, gv, ok, wv)
			}
		}
		for ts, gv := range got {
			if _, ok := want[ts]; !ok {
				t.Errorf("%s {%s} t=%s: proxy emitted %q where Loki has no sample", query, key, ts, gv)
			}
		}
	}
	for key := range proxy {
		if _, ok := loki[key]; !ok {
			t.Errorf("%s: proxy series {%s} absent in Loki", query, key)
		}
	}
}

// Loki emits a sliding-window sample only when (t-range, t] holds lines, so a
// data gap stays absent for every client, including Drilldown-tagged requests.
func TestRangeMetricCompatibilitySlidingGaps(t *testing.T) {
	fx := ensureSlidingFixtures(t)
	selector := `{app="` + fx.gap.app + `"}`
	start, end := fx.hour0, fx.hour0.Add(8*time.Hour)
	for _, tc := range []struct {
		query string
		step  time.Duration
	}{
		{`sum by (app) (count_over_time(` + selector + `[1h]))`, 300 * time.Second},
		{`sum by (app) (count_over_time(` + selector + `[5m]))`, time.Minute},
		{`sum by (app) (rate(` + selector + `[1h]))`, 300 * time.Second},
		{`sum by (app) (bytes_rate(` + selector + `[1h]))`, 300 * time.Second},
		{`sum by (app) (count_over_time(` + selector + `[1h])) / 3600`, 300 * time.Second},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, tc.step, nil)
			steps := int(end.Sub(start)/tc.step) + 1
			for _, points := range loki {
				if len(points) >= steps {
					t.Fatalf("Loki returned %d of %d steps; the fixture gap is not exercised", len(points), steps)
				}
			}
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, proxyURL, tc.query, start, end, tc.step, nil))
			assertSlidingParity(t, tc.query+" [drilldown]", loki, slidingRangeSeries(t, proxyURL, tc.query, start, end, tc.step, map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}))
		})
	}
}

// Windows that are not a multiple of the step, and lines that sit exactly on
// window edges, must be counted as Loki counts them.
func TestRangeMetricCompatibilitySlidingBucketEdges(t *testing.T) {
	fx := ensureSlidingFixtures(t)
	s0 := fx.s0
	selector := `{app="` + fx.edge.app + `"}`
	// Starts and ends are step-aligned: this stack's Loki aligns queries with the step.
	for _, tc := range []struct {
		query      string
		start, end time.Time
		wantValue  string
	}{
		{`sum(count_over_time(` + selector + `[90s]))`, s0.Add(5 * time.Minute), s0.Add(25 * time.Minute), "9"},
		{`sum(count_over_time(` + selector + `[2m]))`, s0, s0.Add(33 * time.Minute), ""},
		{`sum by (app) (count_over_time(` + selector + `[2m]))`, s0.Add(-2 * time.Minute), s0.Add(32 * time.Minute), ""},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, tc.start, tc.end, time.Minute, nil)
			if tc.wantValue != "" {
				for _, points := range loki {
					for ts, v := range points {
						if v != tc.wantValue {
							t.Fatalf("Loki sanity: expected %s at every step, got %s at %s", tc.wantValue, v, ts)
						}
					}
				}
			}
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, proxyURL, tc.query, tc.start, tc.end, time.Minute, nil))
		})
	}
}
