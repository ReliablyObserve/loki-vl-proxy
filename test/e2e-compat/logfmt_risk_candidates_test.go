//go:build e2e

package e2e_compat

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// logfmtCandidateCallRE matches the candidate probe sent beside the logfmt
// parse-risk probe: clauses of absent stored fields, each with the key's
// expression or none, and no line-shape expression.
var logfmtCandidateCallRE = regexp.MustCompile(`^/select/logsql/query .* \| filter \(-\S+:\*(?: -\S+:\*)*(?: _msg:~"(?:[^"\\]|\\.)*")?\)(?: or \(-\S+:\*(?: -\S+:\*)*(?: _msg:~"(?:[^"\\]|\\.)*")?\))* \| limit 1$`)

func isLogfmtCandidateCall(call string) bool {
	return logfmtCandidateCallRE.MatchString(call)
}

// Grafana's logs volume for a `| logfmt` selector, asked again for windows
// moved by refreshes, answers what Loki answers for every window: over lines
// whose level is a stream label (no line is a candidate, so a refresh of the
// stack's proxy checks its moved part with the candidate probe alone) and over
// lines whose level is only in the body (the line-shape probe decides, a risky
// tab-separated line keeps the raw evaluator). The in-process proxy has no
// cache, so every request sends both probes.
// conformance: quality/logfmt-parse-risk-candidates, parser-logfmt, severity-detected-level-derivation, loki_api_v1_query_range
func TestRangeMetricCompatibilityLogfmtVolumeRefresh(t *testing.T) {
	now := time.Now()
	stored := fmt.Sprintf("logfmt-refresh-stored-%d", now.UnixNano())
	body := fmt.Sprintf("logfmt-refresh-body-%d", now.UnixNano())
	risky := fmt.Sprintf("logfmt-refresh-risky-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	fixture := map[string][]jsonVolumeStreamLine{}
	for i := 0; i < 90; i++ { // one tick every 10s for 15 minutes
		tick := s0.Add(time.Duration(i) * 10 * time.Second)
		level := []string{"info", "warn", "error"}[i%3]
		fixture[stored] = append(fixture[stored],
			jsonVolumeStreamLine{ts: tick, level: level, msg: fmt.Sprintf("level=%s op=update n=%d", level, i)},
			jsonVolumeStreamLine{ts: tick.Add(time.Second), level: "debug", msg: fmt.Sprintf("n=%d\tlevel=error", i)},
		)
		fixture[body] = append(fixture[body],
			jsonVolumeStreamLine{ts: tick, msg: fmt.Sprintf(`level=%s op=read msg="slow query" n=%d`, level, i)},
			jsonVolumeStreamLine{ts: tick.Add(time.Second), msg: fmt.Sprintf("op=select n=%d", i)},
		)
		line := fmt.Sprintf("level=%s op=read n=%d", level, i)
		if i == 45 {
			line = fmt.Sprintf("n=%d\tlevel=warn", i) // Loki's decoder ends a token at a tab
		}
		fixture[risky] = append(fixture[risky], jsonVolumeStreamLine{ts: tick, msg: line})
	}
	ingestJSONVolumeFixture(t, fixture)

	route, recorder := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "patterns-autodetect": patternsAutodetectProxyURL, "in-process": route}
	for _, tc := range []struct {
		app        string
		candidates bool // a window holds a line lacking a stored level with the key
		raw        bool // some window keeps the raw evaluator
	}{
		{stored, false, false},
		{body, true, false},
		{risky, true, true},
	} {
		t.Run(tc.app, func(t *testing.T) {
			query := `sum by (level, detected_level) (count_over_time({app="` + tc.app + `"} | logfmt | drop __error__[1m]))`
			for _, shift := range []time.Duration{0, -time.Minute, -2 * time.Minute, time.Minute} {
				end := s0.Add(14*time.Minute + shift)
				start := end.Add(-10 * time.Minute)
				loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
				if len(loki) == 0 {
					t.Fatalf("Loki sanity: no series for %s over %v..%v", query, start, end)
				}
				for name, base := range targets {
					recorder.reset()
					assertSlidingParity(t, fmt.Sprintf("%s [%s, shift %v]", query, name, shift), loki, slidingRangeSeries(t, base, query, start, end, time.Minute, nil))
					if name != "in-process" {
						continue
					}
					candidates, rows := 0, 0
					for _, call := range recorder.reset() {
						switch {
						case isLogfmtCandidateCall(call):
							candidates++
						case isLevelPresenceCall(call), strings.HasSuffix(call, " | limit 1"):
						case strings.HasPrefix(call, "/select/logsql/query "):
							rows++
						}
					}
					// A risky verdict does not wait for the candidate probe.
					if candidates == 0 && !tc.raw {
						t.Fatalf("shift %v: expected the candidate probe beside the logfmt probe", shift)
					}
					if !tc.raw && rows != 0 {
						t.Fatalf("shift %v: expected stats buckets without raw rows, got %d row reads", shift, rows)
					}
				}
			}
		})
	}
}
