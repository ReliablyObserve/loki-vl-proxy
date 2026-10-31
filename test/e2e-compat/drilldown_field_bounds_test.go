//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// A byte limit may be reached before the streaming reader observes the series
// limit. Drilldown must still rank the field values, visibly report the partial
// answer, and retain Loki's exact points for every selected value. A plain
// client must continue to receive the honest resource-limit error.
//
// conformance: limits/drilldown-breakdown-exact, limits/top-n-ranking-not-silent, loki_api_v1_query_range
func TestDrilldown_FieldBoundsByteOverflowMatchesLoki(t *testing.T) {
	app := fmt.Sprintf("drilldown-field-bytes-%d", time.Now().UnixNano())
	anchor := time.Now().UTC().Add(-12 * time.Hour).Truncate(time.Minute)
	var lines []jsonVolumeStreamLine
	fixtureCounts := map[string]map[string]int{}
	values := []string{"z-quote\"eye", "z-backslash\\nose", "z-newline\near"}
	for i := 0; i < 24; i++ {
		values = append(values, fmt.Sprintf("a%02d-", i)+strings.Repeat("x", 1600))
	}
	for i, value := range values {
		count := 1
		if i < 3 {
			count = 30 - i*5 // unique totals, independent of series ordering
		}
		for j := 0; j < count; j++ {
			msg, err := json.Marshal(map[string]string{"val": value})
			if err != nil {
				t.Fatal(err)
			}
			ts := anchor.Add(time.Duration(j)*time.Second + time.Duration(i)*time.Millisecond)
			lines = append(lines, jsonVolumeStreamLine{ts: ts, msg: string(msg)})
			// A line belongs to the one-minute sample whose window is (t-1m,t].
			// All queried ranges below contain these samples, including the
			// line exactly on the first minute boundary.
			sample := ts.Truncate(time.Minute)
			if ts.After(sample) {
				sample = sample.Add(time.Minute)
			}
			key := "val=" + value
			if fixtureCounts[key] == nil {
				fixtureCounts[key] = map[string]int{}
			}
			fixtureCounts[key][strconv.FormatInt(sample.Unix(), 10)]++
		}
	}
	expected := map[string]map[string]string{}
	for key, counts := range fixtureCounts {
		expected[key] = map[string]string{}
		for ts, count := range counts {
			expected[key][ts] = strconv.Itoa(count)
		}
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i].ts.Before(lines[j].ts) })
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: lines})
	query := fmt.Sprintf(`sum by (val) (count_over_time({app=%q} | json | drop __error__, __error_details__ | val!="" [1m]))`, app)
	const byteLimit = 2500
	bounded, recorder := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
		cfg.MaxStatsQuerySeries = 2
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = byteLimit
	})
	full, _ := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) { cfg.MaxStatsQuerySeries = 100 })
	end := anchor.Add(11 * time.Hour).Add(2 * time.Minute)

	for _, span := range []time.Duration{24 * time.Hour, 48 * time.Hour} {
		t.Run(span.String(), func(t *testing.T) {
			for _, shift := range []time.Duration{0, time.Minute} {
				start, stop := end.Add(-span).Add(shift), end.Add(shift)
				// Log/instant visibility does not prove Loki's range store is
				// ready on a fresh stack. Gate the actual historical query on
				// every fixture value and independently calculated sample.
				loki := fieldBoundsWaitLokiRange(t, query, start, stop, expected)
				assertSlidingParity(t, query, loki, slidingRangeSeries(t, full, query, start, stop, time.Minute, nil))
				want := fieldBoundsBusiest(t, loki, 2)
				// The same byte/series budgets must also preserve a complete
				// answer, with no partial-result warning, when all values fit.
				small := fmt.Sprintf(`sum by (val) (count_over_time({app=%q} | json | drop __error__, __error_details__ | val=~"z-(quote|backslash).*" [1m]))`, app)
				smallLoki := slidingRangeSeries(t, lokiURL, small, start, stop, time.Minute, nil)
				if len(smallLoki) != 2 {
					t.Fatalf("under-limit Loki fixture must have two values, got %d", len(smallLoki))
				}
				assertSlidingParity(t, small, smallLoki, slidingRangeSeries(t, bounded, small, start, stop, time.Minute, map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}))
				params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(stop.UnixNano(), 10)}, "step": {"60"}}
				target := bounded + "/loki/api/v1/query_range?" + params.Encode()
				status, body, _ := doJSONGET(t, target, map[string]string{"X-Scope-OrgID": "0"})
				if !((status == http.StatusBadRequest && strings.Contains(string(body), "maximum number of series (2)")) ||
					(status == http.StatusInternalServerError && strings.Contains(string(body), fmt.Sprintf("response exceeds %d bytes", byteLimit)))) {
					t.Fatalf("plain clients must receive an honest resource-limit error: HTTP %d %.500s", status, body)
				}
				for repeat := 0; repeat < 2; repeat++ {
					recorder.reset()
					status, body, envelope := doJSONGET(t, target, map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": "Source=grafana-lokiexplore-app"})
					if status != http.StatusOK || envelope["status"] != "success" || len(extractArray(envelope, "warnings")) == 0 {
						t.Fatalf("Drilldown must return a visibly partial 200 after byte overflow: HTTP %d %.1000s", status, body)
					}
					got := fieldBoundsSeries(t, envelope)
					assertSlidingParity(t, query, want, got)
					ranked := false
					for _, call := range recorder.reset() {
						ranked = ranked || strings.Contains(call, "| sort by (__lvp_rank desc, val) | limit 3 | fields val)")
					}
					if !ranked {
						t.Fatal("byte overflow did not retry with the bounded whole-range ranking")
					}
				}
			}
		})
	}
}

func fieldBoundsWaitLokiRange(t *testing.T, query string, start, end time.Time, expected map[string]map[string]string) map[string]map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	last := "no range response"
	for {
		reply := longrangeFieldsFetch(ctx, client, lokiURL, query, start, end)
		if reply.err != nil {
			// Keep the last useful response if the deadline cancels this
			// attempt; a bare context error would hide the readiness cause.
			if ctx.Err() == nil {
				last = reply.err.Error()
			}
		} else if reflect.DeepEqual(expected, reply.series) {
			return reply.series
		} else {
			last = longrangeFieldsDifference(expected, reply.series)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Loki range fixture never became complete for %s [%s,%s]: %v; last response: %s", query, start.Format(time.RFC3339), end.Format(time.RFC3339), ctx.Err(), last)
			return nil
		case <-ticker.C:
		}
	}
}

func fieldBoundsBusiest(t *testing.T, all map[string]map[string]string, limit int) map[string]map[string]string {
	t.Helper()
	type rank struct {
		key   string
		total float64
	}
	var ranks []rank
	for key, points := range all {
		var total float64
		for _, value := range points {
			n, err := strconv.ParseFloat(value, 64)
			if err != nil {
				t.Fatal(err)
			}
			total += n
		}
		ranks = append(ranks, rank{key, total})
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i].total > ranks[j].total })
	if len(ranks) <= limit || ranks[limit-1].total == ranks[limit].total {
		t.Fatal("fixture must have an unambiguous ranking boundary")
	}
	out := map[string]map[string]string{}
	for _, item := range ranks[:limit] {
		out[item.key] = all[item.key]
	}
	return out
}

func fieldBoundsSeries(t *testing.T, envelope map[string]interface{}) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	data := extractMap(envelope, "data")
	if data["resultType"] != "matrix" {
		t.Fatalf("expected a matrix: %v", envelope)
	}
	for _, item := range extractArray(data, "result") {
		series, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("invalid series: %v", item)
		}
		metric := extractMap(series, "metric")
		if len(metric) != 1 {
			t.Fatalf("expected only the val grouping label: %v", metric)
		}
		key := "val=" + fmt.Sprint(metric["val"])
		if _, exists := out[key]; exists {
			t.Fatalf("duplicate series: %s", key)
		}
		points := map[string]string{}
		for _, raw := range extractArray(series, "values") {
			pair, ok := raw.([]interface{})
			if !ok || len(pair) != 2 {
				t.Fatalf("invalid point: %v", raw)
			}
			ts, ok := pair[0].(float64)
			if !ok {
				t.Fatalf("invalid timestamp: %v", pair[0])
			}
			points[strconv.FormatFloat(ts, 'f', -1, 64)] = fmt.Sprint(pair[1])
		}
		out[key] = points
	}
	return out
}
