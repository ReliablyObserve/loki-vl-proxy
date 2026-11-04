//go:build e2e

package e2e_compat

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// generatorProbeFixture mirrors the log generator's api-gateway lines: every
// body holds level (Python's json.dumps spacing) and every stream carries the
// level label, so a name_extracted label reads every line and the parse-risk
// probes find nothing. From the second hour on, one line in fifty is a kind
// the probes must still find (Loki and unpack_json read it differently).
func generatorProbeFixture(s0 time.Time, lines int, every time.Duration) []jsonVolumeStreamLine {
	risky := []string{
		`{"level":"error","msg": truncated`,
		`{"level":"info","level":"error"}`,
		`{"level":["error"]}`,
		`{" level ":"error","method":"GET"}`,
		`{"level-extracted":"x","method":"GET"}`,
		`{"":"blank","level":"warn"}`,
	}
	out := make([]jsonVolumeStreamLine, 0, lines)
	for i := 0; i < lines; i++ {
		ts := s0.Add(time.Duration(i) * every)
		level := []string{"info", "info", "warn", "error"}[i%4]
		msg := fmt.Sprintf(`{"service": {"name": "api-gateway"}, "method": %q, "status": %d, "user_id": "user_%d", "level": %q, "region": "us-east-1"}`,
			[]string{"GET", "POST"}[i%2], []int{200, 200, 404, 500}[i%4], i%97, level)
		if ts.Sub(s0) >= time.Hour && i%50 == 7 {
			msg = risky[(i/50)%len(risky)]
		}
		out = append(out, jsonVolumeStreamLine{ts: ts, level: level, msg: msg})
	}
	return out
}

// The json metrics over a name_extracted label and over parsed labels equal
// Loki's on generator lines, answered from VictoriaLogs stats after the
// parse-risk probes found no line (one stats pushdown, no raw rows), and equal
// Loki's over a window holding the lines the probes must find, which keep the
// exact raw evaluator.
// conformance: quality/json-label-probe-gates, semantics/json-extracted-metric-pushdown, semantics/extracted-suffix-collision
func TestCompat_JSONLabelProbeGatesLikeLoki(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("probe-gates-%d", now.UnixNano())
	// 22 h back, beside the other long-range fixtures.
	s0 := now.Add(-22 * time.Hour).Truncate(time.Hour)
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: generatorProbeFixture(s0, 720, 10*time.Second)})

	pushdown := vlSupportsStatsRangeOffset(t)
	route, recorder := newJSONVolumeRouteProxyWithRowLimit(t, 0)
	targets := map[string]string{"proxy": proxyURL, "in-process": route}
	selector := `{app="` + app + `"}`
	queries := []struct{ name, query string }{
		{"X1 filter level_extracted, by level", `sum by (level) (count_over_time(` + selector + ` | json | level_extracted!="" [1m]))`},
		{"E7 count by level_extracted", `sum by (level_extracted) (count_over_time(` + selector + ` | json | drop __error__ [1m]))`},
		{"LS8 count by level_extracted, equality", `sum by (level_extracted) (count_over_time(` + selector + ` | json | level_extracted="error" | drop __error__ [1m]))`},
		{"X3 rate, level_extracted equality", `sum(rate(` + selector + ` | json | level_extracted="error" [1m]))`},
		{"parsed method", `sum by (method) (count_over_time(` + selector + ` | json | drop __error__ [1m]))`},
		{"parsed user_id", `sum by (user_id) (count_over_time(` + selector + ` | json | drop __error__ [1m]))`},
	}
	for _, window := range []struct {
		name       string
		start, end time.Time
		clean      bool
	}{
		{"clean hour", s0.Add(time.Minute), s0.Add(59 * time.Minute), true},
		{"hour with risky lines", s0.Add(time.Hour), s0.Add(2*time.Hour - time.Minute), false},
	} {
		for _, tc := range queries {
			t.Run(window.name+"/"+tc.name, func(t *testing.T) {
				loki := slidingRangeSeries(t, lokiURL, tc.query, window.start, window.end, time.Minute, nil)
				for name, base := range targets {
					recorder.reset()
					before := parserMetricEvaluations(t, route)
					got := slidingRangeSeries(t, base, tc.query, window.start, window.end, time.Minute, nil)
					assertSlidingParity(t, tc.query+" ["+name+"]", loki, got)
					if name != "in-process" || !pushdown || !window.clean {
						continue
					}
					after := parserMetricEvaluations(t, route)
					// The probe verdicts of a window are remembered, so a shape
					// after another over the same fields may send none.
					raw := 0
					for _, call := range recorder.reset() {
						if strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1") && !isLevelPresenceCall(call) {
							raw++
						}
					}
					if raw != 0 || after["vl_stats_buckets/pushdown"]-before["vl_stats_buckets/pushdown"] != 1 {
						t.Fatalf("expected one stats pushdown and no raw rows: raw requests=%d counters before=%v after=%v", raw, before, after)
					}
				}
			})
		}
	}
}
