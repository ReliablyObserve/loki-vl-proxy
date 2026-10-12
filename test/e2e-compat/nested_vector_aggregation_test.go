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

// Loki folds a vector aggregation over another one level by level, and each
// level keeps exactly its own grouping labels (pkg/logql/evaluator.go
// VectorAggEvaluator.Next: `by` keeps the listed labels a series has, `without`
// drops the listed ones): min by (zone) (avg by (pod, zone) (count_over_time(...)))
// is one series per zone, valued by the lowest of the per-pod averages. The
// proxy sent the whole nest to VictoriaLogs and dropped the outer `by`, so every
// zone folded into one series without labels, at an instant and over a tumbling
// window; a sliding window folds the levels in the proxy
// (semantics/nested-aggregation-outer-by-native-routes).

type nestedAggFixtures struct {
	probe string
	s0    time.Time
}

var (
	nestedAggOnce      sync.Once
	nestedAggFixtures_ *nestedAggFixtures
)

type nestedAggStream struct {
	labels   map[string]string
	interval time.Duration
	offset   time.Duration
}

// ensureNestedAggFixtures ingests four streams in two zones and two services, a
// line every 5, 9, 13 and 17 seconds for 30 minutes, each stream on its own
// sub-second offset so no line sits on a window edge and the per-stream counts
// differ at every step.
func ensureNestedAggFixtures(t *testing.T) *nestedAggFixtures {
	t.Helper()
	nestedAggOnce.Do(func() {
		now := time.Now()
		probe := fmt.Sprintf("nestedagg%d", now.UnixNano())
		fx := &nestedAggFixtures{probe: probe, s0: now.Add(-9 * time.Hour).Truncate(time.Hour)}
		streams := []nestedAggStream{
			{map[string]string{"probe": probe, "zone": "x", "pod": "a", "svc": "s1"}, 5 * time.Second, 370 * time.Millisecond},
			{map[string]string{"probe": probe, "zone": "x", "pod": "b", "svc": "s2"}, 9 * time.Second, 500 * time.Millisecond},
			{map[string]string{"probe": probe, "zone": "y", "pod": "c", "svc": "s1"}, 13 * time.Second, 630 * time.Millisecond},
			{map[string]string{"probe": probe, "zone": "y", "pod": "d", "svc": "s2"}, 17 * time.Second, 760 * time.Millisecond},
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
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,zone,pod,svc"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
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
			lokiTotal := nestedAggInstant(t, lokiURL, query, at)[""]
			proxyTotal := nestedAggInstant(t, proxyURL, query, at)[""]
			if lokiTotal == float64(total) && proxyTotal == float64(total) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("fixture not ready: %d lines, Loki counts %v, proxy %v", total, lokiTotal, proxyTotal)
			}
			time.Sleep(2 * time.Second)
		}
		nestedAggFixtures_ = fx
	})
	if nestedAggFixtures_ == nil {
		t.Fatal("the nested aggregation fixtures were not ingested; see the first test that ran")
	}
	return nestedAggFixtures_
}

// nestedAggKey keeps the labels the aggregations here group by. Series
// otherwise carry labels each backend adds on its own (service_name,
// detected_level).
func nestedAggKey(metric map[string]any) string {
	var keys []string
	for _, name := range []string{"probe", "zone", "pod", "svc"} {
		if v, ok := metric[name]; ok {
			keys = append(keys, fmt.Sprintf("%s=%v", name, v))
		}
	}
	return strings.Join(keys, ",")
}

// nestedAggInstant returns an instant vector as grouping labels -> value.
func nestedAggInstant(t *testing.T, base, query string, at time.Time) map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || extractMap(resp, "data")["resultType"] != "vector" {
		t.Fatalf("%s %s: unhealthy response %d %s", base, query, status, body)
	}
	out := map[string]float64{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		sample, _ := item.(map[string]any)
		metric, _ := sample["metric"].(map[string]any)
		value, _ := sample["value"].([]any)
		v, err := strconv.ParseFloat(fmt.Sprint(value[1]), 64)
		if len(value) != 2 || err != nil {
			t.Fatalf("%s %s: malformed sample in %s", base, query, body)
		}
		if _, dup := out[nestedAggKey(metric)]; dup {
			t.Fatalf("%s %s: two series for {%s}: %s", base, query, nestedAggKey(metric), body)
		}
		out[nestedAggKey(metric)] = v
	}
	return out
}

// nestedAggRange returns a matrix as grouping labels -> timestamp -> value,
// failing on a group answered by more than one series.
func nestedAggRange(t *testing.T, base, query string, start, end time.Time, step time.Duration) map[string]map[string]float64 {
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
		key := nestedAggKey(metric)
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

func compareNestedAgg(t *testing.T, query string, loki, proxy map[string]map[string]float64) {
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

// nestedAggShapes: a tumbling window (range == step, answered by the
// VictoriaLogs buckets), an instant query, and sliding windows (range != step,
// evaluated by the proxy over the series of the innermost level).
var nestedAggShapes = []struct {
	name        string
	window      string
	step        time.Duration
	start, stop time.Duration // offsets from the fixture start; an instant query evaluates at stop
}{
	{"tumbling", "1m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"tumbling 5m", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"instant", "5m", 0, 0, 20 * time.Minute},
	{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"sliding range shorter than step", "1m", 2 * time.Minute, 6 * time.Minute, 26 * time.Minute},
}

// nestedAggForms are the nested queries, innermost range function as %s.
var nestedAggForms = []string{
	// every pair of operators, the inner group several series, the outer
	// group several inner groups
	"sum by (zone) (sum by (pod, zone) (%s))",
	"min by (zone) (sum by (pod, zone) (%s))",
	"max by (zone) (sum by (pod, zone) (%s))",
	"avg by (zone) (sum by (pod, zone) (%s))",
	"count by (zone) (sum by (pod, zone) (%s))",
	"sum by (zone) (avg by (pod, zone) (%s))",
	"min by (zone) (avg by (pod, zone) (%s))",
	"max by (zone) (avg by (pod, zone) (%s))",
	"avg by (zone) (avg by (pod, zone) (%s))",
	"count by (zone) (avg by (pod, zone) (%s))",
	"sum by (zone) (max by (pod, zone) (%s))",
	"min by (zone) (max by (pod, zone) (%s))",
	"max by (zone) (min by (pod, zone) (%s))",
	"avg by (zone) (min by (pod, zone) (%s))",
	"sum by (zone) (count by (pod, zone) (%s))",
	"max by (zone) (count by (pod, zone) (%s))",
	// an outer label the inner group lacks, by (), no grouping, the trailing spelling
	"min by (zone) (avg by (pod) (%s))",
	"count by (pod, baz) (max by (pod, zone) (%s))",
	"min by () (avg by (pod, zone) (%s))",
	"min(avg by (pod, zone) (%s))",
	"min(avg by (pod, zone) (%s)) by (zone)",
	"avg(max (%s))",
	// the outer level keeps the labels both name
	"max by (pod, zone) (avg by (pod, zone, svc) (%s))",
	"avg by (zone, svc) (sum by (pod, zone) (%s))",
	// without over a level whose labels are named
	"min without (pod) (avg by (pod, zone) (%s))",
	"count without (svc) (max by (pod, zone, svc) (%s))",
	// three levels
	"max by (zone) (min by (pod, zone) (max by (pod, zone, svc) (%s)))",
	"sum by (zone) (count by (pod, zone) (max by (pod, zone, svc) (%s)))",
	"avg by (zone) (avg by (pod, zone) (avg by (pod, zone, svc) (%s)))",
	"count by (zone) (avg without (svc) (min by (pod, zone, svc) (%s)))",
	// an innermost without: the proxy folds every level over the series
	"min by (zone) (avg without (pod) (%s))",
	"max(sum without (pod, svc) (%s))",
	"sum by (zone) (count by (pod, zone) (max without (svc) (%s)))",
}

// TestCompat_NestedVectorAggregation: a vector aggregation over another one
// answers Loki's series and values per step and at an instant, one stats chain
// to VictoriaLogs with a pipe per level (a sliding window: the levels folded by
// the proxy over the series of the innermost one). The forms with a `without`
// over a level that names its labels, and with an innermost `without`, are the
// ones of semantics/without-over-inner-aggregation-collapses and
// semantics/without-nested-under-aggregation.
// conformance: semantics/nested-aggregation-outer-by-native-routes, semantics/without-over-inner-aggregation-collapses, semantics/without-nested-under-aggregation, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_NestedVectorAggregation(t *testing.T) {
	fx := ensureNestedAggFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	for _, shape := range nestedAggShapes {
		for _, function := range []string{"count_over_time", "rate"} {
			for _, form := range nestedAggForms {
				if function == "rate" && strings.HasPrefix(form, "max(sum without") {
					// `sum without (...) (rate(...))` alone is a separate gap.
					continue
				}
				query := fmt.Sprintf(form, fmt.Sprintf("%s(%s [%s])", function, selector, shape.window))
				t.Run(shape.name+"/"+query, func(t *testing.T) {
					if shape.step == 0 {
						at := fx.s0.Add(shape.stop)
						loki := nestedAggInstant(t, lokiURL, query, at)
						proxy := nestedAggInstant(t, proxyURL, query, at)
						compareNestedAgg(t, query, nestedAggAsSeries(loki), nestedAggAsSeries(proxy))
						return
					}
					start, end := fx.s0.Add(shape.start), fx.s0.Add(shape.stop)
					loki := nestedAggRange(t, lokiURL, query, start, end, shape.step)
					proxy := nestedAggRange(t, proxyURL, query, start, end, shape.step)
					compareNestedAgg(t, query, loki, proxy)
				})
			}
		}
	}
}

func nestedAggAsSeries(instant map[string]float64) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	for k, v := range instant {
		out[k] = map[string]float64{"t": v}
	}
	return out
}
