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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// A `| json` metric whose range is shorter than the step (a `[1m]` panel at
// Grafana's 120 s step over 48 hours) is answered from VictoriaLogs stats over
// the lines inside the windows, as Loki evaluates it: each step reads
// (t-range, t] and the lines between two windows are never read, so a line
// Loki's JSON parser rejects there does not fail the strict count
// (pkg/logql/range_vector.go batchRangeVectorIterator.load, evaluator.go
// RangeVectorEvaluator.Next). The shapes are the strict detected_level count
// (Logs Drilldown and the DLV3 A/B shape), its rate and byte forms, the
// `__error__!=""` and `__error__=""` splits and the `drop __error__` pushdown,
// over steps equal to, longer than and not a multiple of the range, with a
// start on the step grid and an unaligned millisecond start (which the
// stack's align_queries_with_step and the proxy's -align-queries-with-step
// both move onto the grid; the unaligned evaluation of Loki's defaults is the
// unit differential's). The in-process proxy must answer from
// stats_query_range with no raw scan of the selected lines.
// conformance: semantics/json-metric-step-wider-than-range, semantics/json-detected-level-error-split
func TestRangeMetricCompatibilityJSONStepWiderThanRange(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-8 * time.Hour).Truncate(time.Hour)
	clean := fmt.Sprintf("json-step-wider-%d", now.UnixNano())
	mixed := fmt.Sprintf("json-step-wider-mixed-%d", now.UnixNano())
	gap := fmt.Sprintf("json-step-wider-gap-%d", now.UnixNano())
	inWindow := fmt.Sprintf("json-step-wider-window-%d", now.UnixNano())
	levels := []string{"info", "error", "warn", "debug"}
	statuses := []string{"200", "404", "500"}
	// Lines Loki's JSON parser rejects, for the mixed app: unbraced, a brace
	// line cut short, a value VictoriaLogs' parser accepts and Loki's does not.
	rejected := []string{"plain text %d", `{"status":"500","msg":"cut %d`, `{"status":"404","v":NaN,"n":%d}`}
	var lines, mixedLines []jsonVolumeStreamLine
	for i := 0; i < 360; i++ {
		// One line every 5 s plus a millisecond offset; every twelfth line sits
		// exactly on a minute mark (an evaluation time or a window start of the
		// aligned grids).
		ts := s0.Add(time.Duration(i)*5*time.Second + time.Duration(i%7)*time.Millisecond)
		msg := fmt.Sprintf(`{"status":%q,"n":%d}`, statuses[i%3], i)
		if i%5 == 0 {
			msg = fmt.Sprintf(`{"status":%q,"nested":{"a":%d}}`, statuses[i%3], i)
		}
		line := jsonVolumeStreamLine{ts: ts, level: levels[i%len(levels)], msg: msg}
		lines = append(lines, line)
		if i%4 == 1 {
			// Rejected lines spread over windows and gaps alike.
			line.msg = fmt.Sprintf(rejected[(i/4)%len(rejected)], i)
		}
		mixedLines = append(mixedLines, line)
	}
	start, end := s0.Add(3*time.Minute), s0.Add(28*time.Minute)
	// The strict count's error placement, on the aligned [1m] windows of a 2 m
	// step: an unbraced line in the gap before an evaluation time, and one
	// inside its window.
	gridStart := time.Unix(0, start.UnixNano()/int64(2*time.Minute)*int64(2*time.Minute))
	eval := gridStart.Add(4 * 2 * time.Minute)
	withGap := append(append([]jsonVolumeStreamLine(nil), lines...), jsonVolumeStreamLine{ts: eval.Add(-90 * time.Second), level: "warn", msg: "plain text in the gap"})
	withWindow := append(append([]jsonVolumeStreamLine(nil), lines...), jsonVolumeStreamLine{ts: eval.Add(-30 * time.Second), level: "warn", msg: "plain text in a window"})
	// In time order: the ingest check counts each app from its first to its
	// last line.
	for _, fixture := range [][]jsonVolumeStreamLine{withGap, withWindow} {
		sort.Slice(fixture, func(i, j int) bool { return fixture[i].ts.Before(fixture[j].ts) })
	}
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{clean: lines, mixed: mixedLines, gap: withGap, inWindow: withWindow})

	// The stack's Loki and proxy align the bounds to the step; the in-process
	// proxy does too.
	route, rec := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) { cfg.AlignQueriesWithStep = true })
	targets := map[string]string{"proxy": proxyURL, "in-process": route}
	// The strict shapes over the app whose lines Loki's parser all accepts; the
	// error splits and the drop over the app holding rejected lines.
	shapes := []struct{ app, fn, pipeline, group string }{
		{clean, "count_over_time", `| json`, "detected_level"},
		{clean, "rate", `| json`, "detected_level"},
		{clean, "bytes_over_time", `| json`, "detected_level"},
		{mixed, "count_over_time", `| json | __error__!=""`, "detected_level"},
		{mixed, "bytes_rate", `| json | __error__!=""`, "detected_level"},
		{mixed, "count_over_time", `| json | __error__=""`, "status"},
		{mixed, "count_over_time", `| json | drop __error__`, "detected_level"},
	}
	for _, grid := range []struct {
		window string
		step   time.Duration
	}{{"1m", time.Minute}, {"1m", 90 * time.Second}, {"1m", 2 * time.Minute}, {"1m", 5 * time.Minute}, {"30s", time.Minute}, {"1m", 61 * time.Second}} {
		for _, aligned := range []bool{true, false} {
			from := start.Add(1234 * time.Millisecond)
			if aligned {
				from = time.Unix(0, start.UnixNano()/int64(grid.step)*int64(grid.step))
			}
			for _, shape := range shapes {
				query := fmt.Sprintf(`sum by (%s) (%s({app=%q} %s [%s]))`, shape.group, shape.fn, shape.app, shape.pipeline, grid.window)
				t.Run(fmt.Sprintf("%s step %v aligned %v", query, grid.step, aligned), func(t *testing.T) {
					loki := slidingRangeSeries(t, lokiURL, query, from, end, grid.step, nil)
					if len(loki) == 0 {
						t.Fatalf("Loki sanity: no series for %s", query)
					}
					rec.reset()
					for name, base := range targets {
						assertSlidingParity(t, query+" ["+name+"]", loki, slidingRangeSeries(t, base, query, from, end, grid.step, nil))
					}
					assertStatsOnly(t, rec, query)
				})
			}
		}
	}
	// The strict count with an unbraced line: in the gap Loki answers 200 and
	// the proxy answers it from stats; inside a window both fail it with the
	// pipeline error.
	for _, tc := range []struct {
		app    string
		failed bool
	}{{gap, false}, {inWindow, true}} {
		query := fmt.Sprintf(`sum by (detected_level) (count_over_time({app=%q} | json [1m]))`, tc.app)
		t.Run(fmt.Sprintf("unbraced line, in a window %v", tc.failed), func(t *testing.T) {
			lokiStatus, lokiBody := stepWiderStatus(t, lokiURL, query, gridStart, end, 2*time.Minute)
			if tc.failed && (lokiStatus != http.StatusBadRequest || !strings.Contains(lokiBody, "pipeline error")) || !tc.failed && lokiStatus != http.StatusOK {
				t.Fatalf("Loki sanity: status %d %s", lokiStatus, lokiBody)
			}
			rec.reset()
			for name, base := range targets {
				status, body := stepWiderStatus(t, base, query, gridStart, end, 2*time.Minute)
				if status != lokiStatus || tc.failed && !strings.Contains(body, "pipeline error") {
					t.Fatalf("%s: got %d %q, Loki %d %q", name, status, body, lokiStatus, lokiBody)
				}
			}
			if !tc.failed {
				assertStatsOnly(t, rec, query)
				assertSlidingParity(t, query, slidingRangeSeries(t, lokiURL, query, gridStart, end, 2*time.Minute, nil), slidingRangeSeries(t, route, query, gridStart, end, 2*time.Minute, nil))
			}
		})
	}
}

// assertStatsOnly fails when the in-process proxy read the selected lines as
// raw rows: every /select/logsql/query it sent is a `| limit 1` probe or
// carries the window filter and the split's raw-row condition.
func assertStatsOnly(t *testing.T, rec *vlQueryRecorder, query string) {
	t.Helper()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	stats := 0
	for _, call := range rec.calls {
		switch {
		case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
			stats++
		case strings.HasPrefix(call, "/select/logsql/query "):
			if !strings.HasSuffix(call, " | limit 1") && !strings.Contains(call, "-__vlp:*") && !isLevelPresenceCall(call) {
				t.Fatalf("%s: raw scan %s", query, call)
			}
		}
	}
	if stats == 0 {
		t.Fatalf("%s: no stats_query_range request: %v", query, rec.calls)
	}
}

// stepWiderStatus returns the status and, for an error, the body of a range
// query.
func stepWiderStatus(t *testing.T, base, query string, start, end time.Time, step time.Duration) (int, string) {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.Itoa(int(step.Seconds()))}}
	status, body, _ := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status == http.StatusOK {
		return status, ""
	}
	return status, strings.TrimSpace(body)
}
