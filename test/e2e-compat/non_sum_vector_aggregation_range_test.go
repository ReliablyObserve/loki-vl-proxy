//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Loki folds a vector aggregation to one series per group at every step:
// max(count_over_time({...}[5m])) is one series, max by (pod) one per pod, and
// the value is the extreme (count: the number of series, avg: their mean) of
// the per-stream values (pkg/logql/evaluator.go newVectorAggEvaluator /
// VectorAggEvaluator.Next, pkg/logql/range_vector.go for the window (t-range, t]).
// The proxy answered one series per stream on query_range for a window that is
// not the step (semantics/non-sum-vector-aggregation-unreduced).

type nonSumFixtures struct {
	probe string
	s0    time.Time
}

var (
	nonSumOnce      sync.Once
	nonSumFixtures_ *nonSumFixtures
)

type nonSumStream struct {
	labels   map[string]string
	interval time.Duration
	offset   time.Duration
}

// ensureNonSumFixtures ingests four streams in two zones, a line every 5, 9, 13
// and 17 seconds for 30 minutes, each stream on its own sub-second offset so no
// line sits on a window edge and the per-stream counts differ at every step.
func ensureNonSumFixtures(t *testing.T) *nonSumFixtures {
	t.Helper()
	nonSumOnce.Do(func() {
		now := time.Now()
		probe := fmt.Sprintf("nonsum%d", now.UnixNano())
		fx := &nonSumFixtures{probe: probe, s0: now.Add(-9 * time.Hour).Truncate(time.Hour)}
		streams := []nonSumStream{
			{map[string]string{"probe": probe, "zone": "x", "pod": "a"}, 5 * time.Second, 370 * time.Millisecond},
			{map[string]string{"probe": probe, "zone": "x", "pod": "b"}, 9 * time.Second, 500 * time.Millisecond},
			{map[string]string{"probe": probe, "zone": "y", "pod": "c"}, 13 * time.Second, 630 * time.Millisecond},
			{map[string]string{"probe": probe, "zone": "y", "pod": "d"}, 17 * time.Second, 760 * time.Millisecond},
		}
		var vlRows strings.Builder
		var lokiStreams []any
		total := 0
		for _, s := range streams {
			var values [][]string
			for at := fx.s0.Add(s.offset); at.Before(fx.s0.Add(30 * time.Minute)); at = at.Add(s.interval) {
				line := fmt.Sprintf("tick %s %d", s.labels["pod"], at.UnixNano())
				values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
				row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line}
				for k, v := range s.labels {
					row[k] = v
				}
				encoded, _ := json.Marshal(row)
				vlRows.Write(encoded)
				vlRows.WriteByte('\n')
			}
			total += len(values)
			lokiStreams = append(lokiStreams, map[string]any{"stream": s.labels, "values": values})
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,zone,pod"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		forceVLFlush(t)
		// The fixture is older than query_ingesters_within, so Loki serves it
		// from the store once its ingester flushed it.
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
			t.Fatalf("Loki flush: %d %s", status, body)
		}
		// Both backends must hold every line before any comparison, and Loki's
		// metric path must answer (it answers empty for a few minutes after a
		// fresh stack starts).
		deadline := time.Now().Add(240 * time.Second)
		for {
			at := fx.s0.Add(31 * time.Minute)
			query := fmt.Sprintf(`sum(count_over_time({probe=%q}[%ds]))`, probe, 31*60)
			lokiTotal := nonSumInstant(t, lokiURL, query, at)[""]
			proxyTotal := nonSumInstant(t, proxyURL, query, at)[""]
			if lokiTotal == float64(total) && proxyTotal == float64(total) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("fixture not ready: %d lines, Loki counts %v, proxy %v", total, lokiTotal, proxyTotal)
			}
			time.Sleep(2 * time.Second)
		}
		nonSumFixtures_ = fx
	})
	if nonSumFixtures_ == nil {
		t.Fatal("the non-sum aggregation fixtures were not ingested; see the first test that ran")
	}
	return nonSumFixtures_
}

// nonSumKey keeps the labels the aggregations here group by. Series otherwise
// carry labels each backend adds on its own (service_name, detected_level).
func nonSumKey(metric map[string]any) string {
	var keys []string
	for _, name := range []string{"zone", "pod"} {
		if v, ok := metric[name]; ok {
			keys = append(keys, fmt.Sprintf("%s=%v", name, v))
		}
	}
	return strings.Join(keys, ",")
}

// nonSumInstant returns an instant vector as grouping labels -> value.
func nonSumInstant(t *testing.T, base, query string, at time.Time) map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || extractMap(resp, "data")["resultType"] != "vector" {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	out := map[string]float64{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		sample, _ := item.(map[string]any)
		metric, _ := sample["metric"].(map[string]any)
		value, _ := sample["value"].([]any)
		if len(value) != 2 {
			t.Fatalf("%s %s: unexpected sample in %s", base, query, body)
		}
		v, err := strconv.ParseFloat(fmt.Sprint(value[1]), 64)
		if err != nil {
			t.Fatalf("%s %s: invalid value in %s", base, query, body)
		}
		if _, dup := out[nonSumKey(metric)]; dup {
			t.Fatalf("%s %s: two series for {%s}: %s", base, query, nonSumKey(metric), body)
		}
		out[nonSumKey(metric)] = v
	}
	return out
}

// nonSumRange returns a matrix as grouping labels -> timestamp -> value, failing
// on a group answered by more than one series.
func nonSumRange(t *testing.T, base, query string, start, end time.Time, step time.Duration) map[string]map[string]float64 {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || extractMap(resp, "data")["resultType"] != "matrix" {
		t.Fatalf("%s %s: unhealthy response %d %s", base, query, status, body)
	}
	out := map[string]map[string]float64{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		series, _ := item.(map[string]any)
		metric, _ := series["metric"].(map[string]any)
		key := nonSumKey(metric)
		if _, dup := out[key]; dup {
			t.Fatalf("%s %s: two series for {%s}: %d series in all", base, query, key, len(extractArray(extractMap(resp, "data"), "result")))
		}
		out[key] = map[string]float64{}
		values, _ := series["values"].([]any)
		for _, raw := range values {
			pair, _ := raw.([]any)
			ts, _ := pair[0].(float64)
			v, err := strconv.ParseFloat(fmt.Sprint(pair[1]), 64)
			if len(pair) != 2 || err != nil {
				t.Fatalf("%s %s: malformed sample %v", base, query, raw)
			}
			out[key][strconv.FormatFloat(ts, 'f', -1, 64)] = v
		}
	}
	return out
}

func compareNonSum(t *testing.T, query string, loki, proxy map[string]map[string]float64) {
	t.Helper()
	points := 0
	for _, series := range loki {
		points += len(series)
	}
	if points == 0 {
		t.Fatalf("%s: Loki answered no sample; parity would be empty-vs-empty", query)
	}
	var lokiKeys, proxyKeys []string
	for k := range loki {
		lokiKeys = append(lokiKeys, k)
	}
	for k := range proxy {
		proxyKeys = append(proxyKeys, k)
	}
	sort.Strings(lokiKeys)
	sort.Strings(proxyKeys)
	if fmt.Sprint(lokiKeys) != fmt.Sprint(proxyKeys) {
		t.Errorf("%s: Loki's groups %v, the proxy's %v", query, lokiKeys, proxyKeys)
	}
	for key, want := range loki {
		got := proxy[key]
		if len(got) != len(want) {
			t.Errorf("%s {%s}: proxy has %d samples, Loki %d", query, key, len(got), len(want))
		}
		for ts, w := range want {
			g, ok := got[ts]
			if !ok {
				t.Errorf("%s {%s} t=%s: proxy has no sample, Loki %v", query, key, ts, w)
			} else if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Errorf("%s {%s} t=%s: proxy %v, Loki %v", query, key, ts, g, w)
			}
		}
	}
}

// nonSumShapes: a sliding window, a tumbling one, a range shorter than the
// step, a range longer than it, one that is not a multiple of the step, and an
// instant query.
var nonSumShapes = []struct {
	name        string
	window      string
	step        time.Duration
	start, stop time.Duration // offsets from the fixture start; an instant query evaluates at stop
}{
	{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"tumbling", "1m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"range shorter than the step", "2m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range longer than the step", "10m", 3 * time.Minute, 12 * time.Minute, 27 * time.Minute},
	{"range that is not a multiple of the step", "90s", time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"instant", "5m", 0, 0, 20 * time.Minute},
}

// TestCompat_NonSumVectorAggregationRange: count, min, max and avg of
// count_over_time and rate answer Loki's series and values per step and at an
// instant: ungrouped, by labels the streams have (zone, pod), by a label no
// stream has (baz), `by ()`, and nested under another aggregation.
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_NonSumVectorAggregationRange(t *testing.T) {
	fx := ensureNonSumFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	var forms []func(function, window string) string
	for _, op := range []string{"count", "min", "max", "avg"} {
		for _, grouping := range []string{"", " by (zone)", " by (pod)", " by (zone, pod)", " by (baz)", " by ()", " without (pod)"} {
			forms = append(forms, func(function, window string) string {
				return fmt.Sprintf("%s%s (%s(%s [%s]))", op, grouping, function, selector, window)
			})
		}
	}
	for _, nested := range []string{
		"sum(max by (pod) (%s))",
		"sum(max by (zone) (%s))",
		"max(count by (zone) (%s))",
		"max(sum by (pod) (%s))",
		"count(max by (pod) (%s))",
	} {
		forms = append(forms, func(function, window string) string {
			return fmt.Sprintf(nested, fmt.Sprintf("%s(%s [%s])", function, selector, window))
		})
	}
	for _, shape := range nonSumShapes {
		for _, function := range []string{"count_over_time", "rate"} {
			for _, form := range forms {
				query := form(function, shape.window)
				t.Run(shape.name+"/"+query, func(t *testing.T) {
					if shape.step == 0 {
						at := fx.s0.Add(shape.stop)
						loki := nonSumInstant(t, lokiURL, query, at)
						proxy := nonSumInstant(t, proxyURL, query, at)
						compareNonSum(t, query, nonSumAsSeries(loki), nonSumAsSeries(proxy))
						return
					}
					start, end := fx.s0.Add(shape.start), fx.s0.Add(shape.stop)
					loki := nonSumRange(t, lokiURL, query, start, end, shape.step)
					proxy := nonSumRange(t, proxyURL, query, start, end, shape.step)
					compareNonSum(t, query, loki, proxy)
				})
			}
		}
	}
}

func nonSumAsSeries(instant map[string]float64) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	for k, v := range instant {
		out[k] = map[string]float64{"t": v}
	}
	return out
}
