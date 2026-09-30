package proxy

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The breakdown contract over the grid Grafana actually sends: Logs Drilldown's
// $__auto steps (range == step), millisecond and minute start offsets, ends on
// and off the step grid, from Logs Drilldown and from a plain client. Every
// cell must equal Loki's answer: samples at start+k*step covering
// (t-step, t], no sample where a window holds no line, no series without
// samples.
//
// conformance: limits/drilldown-breakdown-exact, loki_api_v1_query_range
func TestDrilldownBreakdown_LokiGridMatrix(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := drilldownBreakdownFixture(base)
	shapes := append([]struct {
		name, query, field string
		parsed             bool
		busiest            []string
	}{}, drilldownBreakdownShapes...)
	shapes = append(shapes, struct {
		name, query, field string
		parsed             bool
		busiest            []string
	}{"grouped count", `sum by (pod) (count_over_time({app="tumble"}[5m]))`, "pod", false, drilldownBreakdownShapes[0].busiest})
	for _, shape := range shapes {
		for _, drilldown := range []bool{true, false} {
			stack := newDrilldownBreakdownStack(t, Config{}, lines)
			for _, step := range []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute} {
				query := strings.Replace(shape.query, "5m]", fmt.Sprintf("%dm]", int(step/time.Minute)), 1)
				for _, offset := range []time.Duration{0, 123 * time.Millisecond, 7 * time.Second, 150 * time.Second} {
					for _, tail := range []time.Duration{0, 37 * time.Second} {
						start := base.Add(2*time.Minute + offset)
						end := start.Add(20*time.Minute + tail)
						name := fmt.Sprintf("%s/drilldown=%v/step=%s/start+%s/end+%s", shape.name, drilldown, step, offset, tail)
						t.Run(name, func(t *testing.T) {
							want := lokiTumblingReferenceParsed(lines, "count_over_time", []string{shape.field}, start, end, step, step, shape.parsed)
							code, body, calls := stack.run(tumblingRangeParams(query, start, end, step), "tenant-a", drilldown)
							if code != http.StatusOK || strings.Contains(body, `"warnings"`) {
								t.Fatalf("expected every series without a warning, got %d: %s", code, body)
							}
							assertTumblingEqual(t, query, want, decodeTumblingSeries(t, query, []byte(body)))
							if len(calls) != 1 {
								t.Fatalf("expected one stats_query_range call, got %+v", calls)
							}
						})
					}
				}
			}
		}
	}
}

// Over every tenant limit below the number of values, Logs Drilldown gets the
// busiest series, each equal to Loki's, with Loki's warning, and a plain
// client gets Loki's 400, whatever the start offset.
//
// conformance: limits/drilldown-breakdown-exact, limits/drilldown-partial-with-warning, limits/series-limit-error
func TestDrilldownBreakdown_OverLimitMatrix(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := drilldownBreakdownFixture(base)
	step := 5 * time.Minute
	for _, shape := range drilldownBreakdownShapes {
		for limit := 1; limit < len(shape.busiest); limit++ {
			cfg := Config{TenantLimits: map[string]map[string]any{"tenant-a": {"max_query_series": float64(limit)}}}
			stack := newDrilldownBreakdownStack(t, cfg, lines)
			for _, offset := range []time.Duration{0, 123 * time.Millisecond, 150 * time.Second} {
				start := base.Add(2*time.Minute + offset)
				end := start.Add(20 * time.Minute)
				t.Run(fmt.Sprintf("%s/limit=%d/start+%s", shape.name, limit, offset), func(t *testing.T) {
					params := tumblingRangeParams(shape.query, start, end, step)
					code, body, _ := stack.run(params, "tenant-a", false)
					if code != http.StatusBadRequest || !strings.Contains(body, fmt.Sprintf("maximum number of series (%d) reached for a single query; consider reducing query cardinality", limit)) {
						t.Fatalf("plain client: expected Loki's series limit error, got %d: %s", code, body)
					}
					code, body, _ = stack.run(params, "tenant-a", true)
					if code != http.StatusOK || !strings.Contains(body, fmt.Sprintf(`"warnings":["maximum number of series (%d) reached for a single query; returning partial results"]`, limit)) {
						t.Fatalf("drilldown: expected a partial result with Loki's warning, got %d: %s", code, body)
					}
					want := lokiTumblingReferenceParsed(lines, "count_over_time", []string{shape.field}, start, end, step, step, shape.parsed)
					kept := map[string]map[int64]string{}
					for _, value := range shape.busiest[:limit] {
						key := canonicalLabelsKey(map[string]string{shape.field: value})
						kept[key] = want[key]
					}
					assertTumblingEqual(t, shape.query, kept, decodeTumblingSeries(t, shape.query, []byte(body)))
				})
			}
		}
	}
}
