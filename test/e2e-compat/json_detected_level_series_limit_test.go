//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// Loki counts the series of the answer: the spellings of one level are one
// series (the stream levels `error`, `ERR` and `Err` all give
// detected_level=error), and a filter on detected_level narrows the series
// before they are counted. The stats pushdown reads those spellings as
// separate VictoriaLogs groups, so the series limit applies to the merged
// answer: a query whose answer fits the limit gets the 200 and the series Loki
// gives, even when its groups do not, and one more series than the limit is
// refused with Loki's message. The stack's Loki has a limit far above this
// fixture, so the limit under test is the in-process proxy's
// (-max-stats-query-series) and the series are compared with Loki's.
// conformance: semantics/json-metric-detected-level-derivation
// conformance: limits/series-limit-error
func TestRangeMetricCompatibilityJSONDetectedLevelSeriesLimit(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	app := fmt.Sprintf("json-level-limit-%d", now.UnixNano())
	const pods = 12
	spellings := []string{"error", "ERR", "Err"} // three spellings of one level
	var lines []jsonVolumeStreamLine
	for p := 0; p < pods; p++ {
		pod := fmt.Sprintf("p%02d", p)
		for i, level := range append(spellings, "info") {
			lines = append(lines, jsonVolumeStreamLine{
				ts: s0.Add(time.Duration(p*4+i) * time.Second), level: level,
				labels: map[string]string{"pod": pod}, msg: fmt.Sprintf(`{"n":%d}`, p*4+i),
			})
		}
	}
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: lines})
	start, end := s0.Add(-time.Minute), s0.Add(2*time.Minute)

	// every pod x (error, info) is a series of its own: 24 answered by
	// sum by (pod, detected_level); the error filter keeps the 12 pods.
	byPodLevel := `sum by (pod, detected_level) (count_over_time({app="` + app + `"} | json | drop __error__ [1m]))`
	filtered := `sum by (pod) (count_over_time({app="` + app + `"} | json | drop __error__ | detected_level="error" [1m]))`
	for _, tc := range []struct {
		name, query string
		series      int
	}{{"all levels", byPodLevel, 2 * pods}, {"filtered to error", filtered, pods}} {
		loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, nil)
		if len(loki) != tc.series {
			t.Fatalf("%s: Loki returned %d series, want %d", tc.name, len(loki), tc.series)
		}
		// at the limit, one above it and far above it: Loki's answer, not a 400.
		// VictoriaLogs holds 48 groups for the first query (three spellings of
		// error and info per pod) and 12 for the second (its filter runs there):
		// more than the limit in the first, so a plan counting groups fails.
		for _, limit := range []int{tc.series, tc.series + 1, 10 * tc.series} {
			t.Run(fmt.Sprintf("%s/limit %d", tc.name, limit), func(t *testing.T) {
				route, _ := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) { cfg.MaxStatsQuerySeries = limit })
				assertSlidingParity(t, tc.query+fmt.Sprintf(" [limit %d]", limit), loki, slidingRangeSeries(t, route, tc.query, start, end, time.Minute, nil))
			})
		}
		// one series past the limit: Loki's error
		t.Run(tc.name+"/over the limit", func(t *testing.T) {
			limit := tc.series - 1
			route, _ := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) { cfg.MaxStatsQuerySeries = limit })
			status, body, _ := alignLimitRangeQuery(t, route, tc.query, start, end, nil)
			if status != http.StatusBadRequest || !strings.Contains(body, fmt.Sprintf("maximum number of series (%d) reached for a single query", limit)) {
				t.Fatalf("expected Loki's series limit error for %d series over a limit of %d, got %d %s", tc.series, limit, status, body)
			}
		})
	}
}
