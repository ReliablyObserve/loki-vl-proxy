//go:build e2e

package e2e_compat

import (
	"fmt"
	"testing"
	"time"
)

// detected_level is the level Loki sets at ingest, normalized
// (distributor/field_detection.go): a level stream label, else a level key of
// the JSON body, whatever the spelling (info, INFO, inf, information). A
// `sum by (detected_level)` over `| json` is answered by the row evaluator, or
// by the stats pushdown with the `drop __error__` form, and both must group
// the spellings of one level into one series like Loki, with the level in the
// body, as a stream label, or both (the stream's wins).
// conformance: semantics/json-metric-detected-level-derivation
func TestRangeMetricCompatibilityJSONDetectedLevel(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	body := fmt.Sprintf("json-level-body-%d", now.UnixNano())
	stream := fmt.Sprintf("json-level-stream-%d", now.UnixNano())
	both := fmt.Sprintf("json-level-both-%d", now.UnixNano())
	spellings := []string{"info", "INFO", "Info", "inf", "information", "warn", "WARN", "warning", "error", "ERR", "debug"}
	var bodyLines, streamLines, bothLines []jsonVolumeStreamLine
	for i := 0; i < 66; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		level := spellings[i%len(spellings)]
		other := spellings[(i+3)%len(spellings)]
		bodyLines = append(bodyLines, jsonVolumeStreamLine{ts: ts, msg: fmt.Sprintf(`{"level":%q,"n":%d}`, level, i)})
		streamLines = append(streamLines, jsonVolumeStreamLine{ts: ts, level: level, msg: fmt.Sprintf(`{"n":%d}`, i)})
		bothLines = append(bothLines, jsonVolumeStreamLine{ts: ts, level: level, msg: fmt.Sprintf(`{"level":%q,"n":%d}`, other, i)})
	}
	// A line without any level, the last of each fixture.
	last := s0.Add(331 * time.Second)
	bodyLines = append(bodyLines, jsonVolumeStreamLine{ts: last, msg: `{"n":-1}`})
	streamLines = append(streamLines, jsonVolumeStreamLine{ts: last, msg: `{"n":-1}`})
	bothLines = append(bothLines, jsonVolumeStreamLine{ts: last, msg: `{"n":-1}`})
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{body: bodyLines, stream: streamLines, both: bothLines})

	route, _ := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "in-process": route}
	start, end := s0.Add(-2*time.Minute), s0.Add(9*time.Minute)
	for _, tc := range []struct{ name, app string }{
		{"level in the body", body},
		{"level as a stream label with mixed spellings", stream},
		{"level in both, the stream's wins", both},
	} {
		for _, pipeline := range []string{`| json`, `| json | drop __error__`} {
			for _, function := range []string{"count_over_time", "bytes_over_time"} {
				query := `sum by (detected_level) (` + function + `({app="` + tc.app + `"} ` + pipeline + ` [1m]))`
				t.Run(tc.name+"/"+function+" "+pipeline, func(t *testing.T) {
					loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
					if len(loki) < 4 {
						t.Fatalf("Loki sanity: expected several detected_level series, got %d: %v", len(loki), loki)
					}
					for name, base := range targets {
						assertSlidingParity(t, query+" ["+name+"]", loki, slidingRangeSeries(t, base, query, start, end, time.Minute, nil))
					}
				})
			}
		}
	}
}
