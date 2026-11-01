//go:build e2e

package e2e_compat

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// A field parsed from the logfmt line is absent from VictoriaLogs' stored
// columns. The field-presence burst path must preserve the parsed value when
// answering an ungrouped count, including when the request is alone.
// conformance: semantics/logfmt-existence-filter-ungrouped-sum, parser-logfmt, loki_api_v1_query_range
func TestCompat_LogfmtExistenceFilterUngroupedSum(t *testing.T) {
	app := fmt.Sprintf("logfmt-presence-%d", time.Now().UnixNano())
	s0 := time.Now().Add(-9 * time.Hour).Truncate(time.Hour)
	var lines []jsonVolumeStreamLine
	for i := 0; i < 60; i++ {
		at := s0.Add(time.Duration(i) * 10 * time.Second)
		lines = append(lines,
			jsonVolumeStreamLine{ts: at, msg: fmt.Sprintf("op=hget key=k%d", i)},
			jsonVolumeStreamLine{ts: at.Add(time.Second), msg: fmt.Sprintf("op=set key=k%d", i)},
			jsonVolumeStreamLine{ts: at.Add(2 * time.Second), msg: fmt.Sprintf("key=k%d", i)},
		)
	}
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: lines})
	route, recorder := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
		cfg.DrilldownBurstWindowMs = 1
	})
	start, end := s0.Add(-time.Minute), s0.Add(11*time.Minute)
	selector := `{app="` + app + `"} | logfmt | op!=""`
	for _, query := range []string{
		`sum(count_over_time(` + selector + ` [5m]))`,
		`sum(count_over_time(` + selector + ` | drop __error__ [5m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
			if len(loki) != 1 {
				t.Fatalf("Loki fixture should return one series, got %v", loki)
			}
			recorder.reset()
			got := slidingRangeSeries(t, route, query, start, end, time.Minute, nil)
			assertSlidingParity(t, query, loki, got)
			var statsCalls, parsedStats int
			for _, call := range recorder.reset() {
				if strings.HasPrefix(call, "/select/logsql/stats_query_range ") {
					statsCalls++
					if strings.Contains(call, "| unpack_logfmt") {
						parsedStats++
					}
				}
			}
			if statsCalls != 1 || parsedStats != 1 {
				t.Fatalf("expected one VictoriaLogs stats read retaining logfmt, got %d stats calls, %d parsed", statsCalls, parsedStats)
			}
		})
	}
}
