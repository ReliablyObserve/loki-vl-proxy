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

	"github.com/ReliablyObserve/Loki-VL-proxy/test/metricparity"
)

// A byte limit may be reached before the streaming reader observes the series
// limit. Drilldown must still rank the field values, visibly report the partial
// answer, and retain Loki's exact points for every selected value. A plain
// client must continue to receive the honest resource-limit error.
//
// conformance: limits/drilldown-breakdown-exact, limits/top-n-ranking-not-silent, loki_api_v1_query_range
func TestDrilldown_FieldBoundsByteOverflowMatchesLoki(t *testing.T) {
	app, anchor, query, expected := fieldBoundsByteOverflowFixture(t)
	const byteLimit = 2500
	bounded, recorder := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
		cfg.MaxStatsQuerySeries = 2
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = byteLimit
		// These checks hold the busiest-series mode's whole-range ranking (count desc); the default Loki mode ranks
		// by first line and is held to Loki in internal/proxy (TestDrilldownSeriesLimitMatchesLoki).
		cfg.DrilldownSeriesLimitMode = "busiest"
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

// In the default Loki mode the same byte overflow retries with the first-met ranking, per split of Loki's query
// frontend (-drilldown-series-limit-split-interval, 1h here as Loki's default): a 200 with Loki's warning holding,
// in every split, the series met first up to the step where the series met so far pass the limit, with Loki's
// points for them (Loki's own answer over the fixture, cut by that rule). Every line falls in one minute, so one
// split holds data: z-quote alone at the anchor (its first line is on the minute) and the 26 other values new one
// minute later, where the limit of 2 trips and one of them fills it: Loki's order among series new at one step is
// not fixed, so any one is Loki's answer (the ranked read keeps those with the earliest lines, z-backslash here). A
// repeat is the first answer.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/top-n-ranking-not-silent, loki_api_v1_query_range
func TestDrilldown_FieldBoundsByteOverflowLokiMode(t *testing.T) {
	_, anchor, query, expected := fieldBoundsByteOverflowFixture(t)
	// Three of the 1,600-byte values fit; the whole breakdown does not.
	const byteLimit = 8000
	bounded, recorder := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
		cfg.MaxStatsQuerySeries = 2
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = byteLimit
		cfg.DrilldownSeriesLimitSplitInterval = time.Hour
	})
	end := anchor.Add(11 * time.Hour).Add(2 * time.Minute)
	for _, span := range []time.Duration{24 * time.Hour, 48 * time.Hour} {
		t.Run(span.String(), func(t *testing.T) {
			start, stop := end.Add(-span), end
			loki := fieldBoundsWaitLokiRange(t, query, start, stop, expected)
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(stop.UnixNano(), 10)}, "step": {"60"}}
			target := bounded + "/loki/api/v1/query_range?" + params.Encode()
			var first map[string]map[string]string
			for repeat := 0; repeat < 2; repeat++ {
				recorder.reset()
				status, body, envelope := doJSONGET(t, target, map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": "Source=grafana-lokiexplore-app"})
				if status != http.StatusOK || envelope["status"] != "success" || len(extractArray(envelope, "warnings")) == 0 {
					t.Fatalf("Drilldown must return a visibly partial 200 after byte overflow: HTTP %d %.1000s", status, body)
				}
				got := fieldBoundsSeries(t, envelope)
				assertFirstMetPerHour(t, query, loki, got, start, stop, 2)
				if repeat == 0 {
					first = got
				} else {
					assertSlidingParity(t, query+" (repeat)", first, got)
				}
				ranked := false
				for _, call := range recorder.reset() {
					ranked = ranked || strings.Contains(call, "min(_time) as __lvp_rank | sort by (__lvp_rank, val) partition by (_time) limit 3 | fields val)")
				}
				if repeat == 0 && !ranked {
					t.Fatal("byte overflow did not retry with the first-met ranking per split")
				}
			}
		})
	}
}

// assertFirstMetPerHour checks got is what Loki keeps of its whole answer all over the limit: per split of the hour
// grid inside (start, end), the series by their first sample in it, cut at the step where the series met so far pass
// the limit. Every series met before that step is kept with Loki's points up to it and none after; of the series new
// at that step, as many as fill the limit are kept, any of them (Loki's order among them is not fixed); no series met
// after it is kept.
func assertFirstMetPerHour(t *testing.T, query string, all, got map[string]map[string]string, start, end time.Time, limit int) {
	t.Helper()
	splitOf := func(ts int64) int64 {
		first := start.Unix() - start.Unix()%3600 + 3600
		if ts < first || first >= end.Unix() {
			return 0
		}
		return min((ts-first)/3600, (end.Unix()-1-first)/3600) + 1
	}
	seconds := func(ts string) int64 {
		n, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	firsts := map[int64]map[string]int64{} // split -> series -> first sample
	for key, points := range all {
		for ts := range points {
			n, split := seconds(ts), splitOf(seconds(ts))
			if firsts[split] == nil {
				firsts[split] = map[string]int64{}
			}
			if cur, ok := firsts[split][key]; !ok || n < cur {
				firsts[split][key] = n
			}
		}
	}
	for key := range got {
		if all[key] == nil {
			t.Fatalf("%s: proxy series %s is not in Loki's answer", query, key)
		}
	}
	for split, met := range firsts {
		times := make([]int64, 0, len(met))
		for _, first := range met {
			times = append(times, first)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		trip := int64(1 << 62)
		if len(times) > limit {
			trip = times[limit]
		}
		kept := 0
		for key, first := range met {
			inSplit := false
			for ts := range got[key] {
				if splitOf(seconds(ts)) == split {
					inSplit = true
				}
			}
			switch {
			case first < trip && !inSplit:
				t.Fatalf("%s: split %d: Loki keeps %s (first at %d, the trip at %d), the proxy does not", query, split, key, first, trip)
			case first > trip && inSplit:
				t.Fatalf("%s: split %d: %s is first met at %d, after the trip at %d, yet kept", query, split, key, first, trip)
			case !inSplit:
				continue
			}
			kept++
			for ts, v := range all[key] {
				if n := seconds(ts); splitOf(n) == split && n <= trip && got[key][ts] != v {
					t.Fatalf("%s: split %d: %s at %s: proxy %q, Loki %q", query, split, key, ts, got[key][ts], v)
				}
			}
			for ts := range got[key] {
				if n := seconds(ts); splitOf(n) == split && (n > trip || all[key][ts] == "") {
					t.Fatalf("%s: split %d: %s has a point at %s, past the trip at %d or where Loki has none", query, split, key, ts, trip)
				}
			}
		}
		if len(times) > limit && kept != limit {
			t.Fatalf("%s: split %d: the proxy keeps %d series, Loki the limit %d", query, split, kept, limit)
		}
	}
}

// fieldBoundsByteOverflowFixture ingests the byte-overflow fixture on both backends: three short values with
// unique counts and 24 values of 1,600 bytes, every line within the anchor's first minute. It returns the app, the
// anchor, the breakdown query and the samples Loki must hold.
func fieldBoundsByteOverflowFixture(t *testing.T) (string, time.Time, string, map[string]map[string]string) {
	t.Helper()
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
	return app, anchor, query, expected
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
			return fieldBoundsCanonicalSeries(t, reply.series)
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

func fieldBoundsCanonicalSeries(t *testing.T, legacy map[string]map[string]string) map[string]map[string]string {
	t.Helper()
	out := make(map[string]map[string]string, len(legacy))
	for key, points := range legacy {
		// This reference has exactly one grouping label. Its entire value may
		// contain commas or equals signs, so do not split it into label pairs.
		value, ok := strings.CutPrefix(key, "val=")
		if !ok {
			t.Fatalf("expected the val grouping label: %s", key)
		}
		out[metricJSONKey(map[string]string{"val": value})] = points
	}
	return out
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
	data := extractMap(envelope, "data")
	if data["resultType"] != "matrix" {
		t.Fatalf("expected a matrix: %v", envelope)
	}
	out, err := metricparity.DecodeSamples(envelope)
	if err != nil {
		t.Fatal(err)
	}
	for key := range out {
		metric := metricKeyLabels(key)
		_, hasValue := metric["val"]
		if len(metric) != 1 || !hasValue {
			t.Fatalf("expected only the val grouping label: %v", metric)
		}
	}
	return out
}

func TestDrilldown_FieldBoundsCanonicalKeys(t *testing.T) {
	values := []string{"quoted\"comma,other=label\\\n", "second", "third"}
	legacy := map[string]map[string]string{}
	var result []interface{}
	for i, value := range values {
		count := strconv.Itoa(9 - i*2)
		legacy["val="+value] = map[string]string{"1.25": count}
		result = append(result, map[string]interface{}{
			"metric": map[string]interface{}{"val": value},
			"values": []interface{}{[]interface{}{json.Number("1.25"), count}},
		})
	}
	envelope := map[string]interface{}{
		"status": "success", "warnings": []interface{}{"partial result"},
		"data": map[string]interface{}{"resultType": "matrix", "result": result},
	}
	reference := fieldBoundsCanonicalSeries(t, legacy)
	got := fieldBoundsSeries(t, envelope)
	if !reflect.DeepEqual(got, reference) {
		t.Fatalf("partial-response labels or decimal samples differ: got %v want %v", got, reference)
	}
	top := fieldBoundsBusiest(t, reference, 2)
	if len(top) != 2 || top[metricJSONKey(map[string]string{"val": values[0]})]["1.25"] != "9" || top[metricJSONKey(map[string]string{"val": values[1]})]["1.25"] != "7" {
		t.Fatalf("canonicalization changed the unambiguous ranking: %v", top)
	}
}
