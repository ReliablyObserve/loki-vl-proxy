//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// TestCompat_AlignQueriesWithStep: the stack's Loki runs with
// query_range.align_queries_with_step and its proxies with
// -align-queries-with-step. A metric range query with a start and end off the
// step grid (step 137s) is answered on Loki's grid: every sample at a multiple
// of the step, the first at the start moved down, the last at the end moved
// down (pkg/querier/queryrange/queryrangebase/step_align.go:20-24), with
// Loki's values; the same after a refresh that moved the window by less than
// a step, and for a Grafana-shaped request (start on the grid, raw end). A log
// query keeps the bounds it was sent.
// conformance: semantics/query-range-step-alignment, loki_api_v1_query_range
func TestCompat_AlignQueriesWithStep(t *testing.T) {
	fx := ensureNonSumFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	step := 137 * time.Second
	stepMs := step.Milliseconds()
	gridSecond := func(at time.Time) int64 { return at.UnixMilli() / stepMs * stepMs / 1000 }
	start := fx.s0.Add(4*time.Minute + 17*time.Second + 300*time.Millisecond)
	end := fx.s0.Add(27*time.Minute + 41*time.Second + 700*time.Millisecond)
	if gridSecond(start)*1000 == start.UnixMilli() || gridSecond(end)*1000 == end.UnixMilli() {
		t.Fatalf("the window [%s, %s] must start and end off the %s grid", start, end, step)
	}
	windows := []struct {
		name       string
		start, end time.Time
	}{
		{"bounds off the grid", start, end},
		{"refresh moved by less than a step", start.Add(31 * time.Second), end.Add(31 * time.Second)},
		{"start on the grid, raw end", time.Unix(gridSecond(start), 0), end.Add(50 * time.Second)},
	}
	queries := []string{
		fmt.Sprintf(`sum(count_over_time(%s[5m]))`, selector),
		fmt.Sprintf(`sum by (pod) (rate(%s[2m]))`, selector),
		fmt.Sprintf(`sum by (zone) (count_over_time(%s[137s]))`, selector),
		fmt.Sprintf(`max by (zone) (count_over_time(%s[90s]))`, selector),
		fmt.Sprintf(`sum(rate(%s[5m])) - sum(rate(%s[1m]))`, selector, selector),
	}
	for _, w := range windows {
		for i, query := range queries {
			loki := nonSumRange(t, lokiURL, query, w.start, w.end, step)
			proxy := nonSumRange(t, proxyURL, query, w.start, w.end, step)
			first, last := int64(-1), int64(-1)
			for key, series := range loki {
				for raw := range series {
					ts, err := strconv.ParseInt(raw, 10, 64)
					if err != nil || ts%int64(step/time.Second) != 0 {
						t.Fatalf("%s: Loki's sample {%s} at %s is off the %s grid: the stack's Loki must run with align_queries_with_step", query, key, raw, step)
					}
					if first < 0 || ts < first {
						first = ts
					}
					if ts > last {
						last = ts
					}
				}
			}
			if i == 0 && (first != gridSecond(w.start) || last != gridSecond(w.end)) {
				t.Fatalf("%s %s: Loki's samples span [%d, %d], want the bounds moved down [%d, %d]", w.name, query, first, last, gridSecond(w.start), gridSecond(w.end))
			}
			compareNonSum(t, w.name+": "+query, loki, proxy)
		}
	}

	// A log query keeps its bounds: the same lines on both, none outside them.
	for _, w := range windows[:1] {
		lokiLines, lokiFirst, lokiLast := alignLogLines(t, lokiURL, selector, w.start, w.end, step)
		proxyLines, proxyFirst, proxyLast := alignLogLines(t, proxyURL, selector, w.start, w.end, step)
		if lokiLines == 0 || lokiLines != proxyLines || lokiFirst != proxyFirst || lokiLast != proxyLast {
			t.Fatalf("log query: Loki %d lines in [%d, %d], proxy %d in [%d, %d]", lokiLines, lokiFirst, lokiLast, proxyLines, proxyFirst, proxyLast)
		}
		if lokiFirst < w.start.UnixNano() || lokiLast > w.end.UnixNano() {
			t.Fatalf("log query: lines in [%d, %d] outside the bounds sent [%d, %d]", lokiFirst, lokiLast, w.start.UnixNano(), w.end.UnixNano())
		}
	}
}

// alignLogLines counts the lines of a log range query and returns the
// earliest and latest line timestamps (Unix nanoseconds).
func alignLogLines(t *testing.T, base, query string, start, end time.Time, step time.Duration) (lines int, first, last int64) {
	t.Helper()
	params := url.Values{
		"query":     {query},
		"start":     {strconv.FormatInt(start.UnixNano(), 10)},
		"end":       {strconv.FormatInt(end.UnixNano(), 10)},
		"step":      {strconv.Itoa(int(step.Seconds()))},
		"limit":     {"5000"},
		"direction": {"forward"},
	}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || extractMap(resp, "data")["resultType"] != "streams" {
		t.Fatalf("%s %s: unhealthy response %d %s", base, query, status, body)
	}
	first = -1
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		stream, _ := item.(map[string]any)
		values, _ := stream["values"].([]any)
		for _, raw := range values {
			pair, _ := raw.([]any)
			ts, err := strconv.ParseInt(fmt.Sprint(pair[0]), 10, 64)
			if len(pair) < 2 || err != nil {
				t.Fatalf("%s %s: malformed entry %v", base, query, raw)
			}
			lines++
			if first < 0 || ts < first {
				first = ts
			}
			if ts > last {
				last = ts
			}
		}
	}
	return lines, first, last
}
