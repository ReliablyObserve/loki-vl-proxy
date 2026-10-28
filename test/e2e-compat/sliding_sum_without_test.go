//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// `sum without (pod) (count_over_time(...))` on a sliding window (range !=
// step) is, at every step, the sum over each group of streams that share every
// label but pod of the stream's count in (t-range, t] (pkg/logql/range_vector.go
// :102-160, pkg/logql/evaluator.go:435-529). The proxy now has VictoriaLogs
// fold the stream rows of each bucket by the stream text with the pod pair cut
// out and computes the windows of the folded series only
// (semantics/without-sliding-sum-folded-buckets). The fixture is short-lived pods
// in two apps, some of them in a zone, so pod is the only label that tells
// most streams apart and zone is on some streams only.

type slidingWithoutFixtures struct {
	probe string
	s0    time.Time
}

var (
	slidingWithoutOnce sync.Once
	slidingWithoutFx   *slidingWithoutFixtures
)

// ensureSlidingWithoutFixtures ingests 48 pod streams, each living two to
// four minutes with a line every 7 to 11 seconds on its own sub-second offset
// (no line on a window edge), over 30 minutes.
func ensureSlidingWithoutFixtures(t *testing.T) *slidingWithoutFixtures {
	t.Helper()
	slidingWithoutOnce.Do(func() {
		now := time.Now()
		probe := fmt.Sprintf("slidingwithout%d", now.UnixNano())
		fx := &slidingWithoutFixtures{probe: probe, s0: now.Add(-9 * time.Hour).Truncate(time.Hour)}
		var vlRows strings.Builder
		var lokiStreams []any
		total := 0
		for i := 0; i < 48; i++ {
			labels := map[string]string{"probe": probe, "app": []string{"api", "worker"}[i%2], "pod": fmt.Sprintf("p%02d", i)}
			if i%3 == 0 {
				labels["zone"] = "x"
			}
			born := fx.s0.Add(time.Duration(i) * 35 * time.Second).Add(time.Duration(137+i*11) * time.Millisecond)
			var values [][]string
			for at := born; at.Before(born.Add(time.Duration(2+i%3) * time.Minute)); at = at.Add(time.Duration(7+i%5) * time.Second) {
				line := fmt.Sprintf("tick %s %s", labels["pod"], strings.Repeat("x", i%7))
				values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
				row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line}
				for k, v := range labels {
					row[k] = v
				}
				encoded, _ := json.Marshal(row)
				vlRows.Write(encoded)
				vlRows.WriteByte('\n')
			}
			total += len(values)
			lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,app,pod,zone"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		forceVLFlush(t)
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
			t.Fatalf("Loki flush: %d %s", status, body)
		}
		deadline := time.Now().Add(240 * time.Second)
		for {
			at := fx.s0.Add(35 * time.Minute)
			query := fmt.Sprintf(`sum(count_over_time({probe=%q}[%ds]))`, probe, 35*60)
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
		slidingWithoutFx = fx
	})
	if slidingWithoutFx == nil {
		t.Fatal("the sliding without fixtures were not ingested; see the first test that ran")
	}
	return slidingWithoutFx
}

// slidingWithoutRange returns a matrix as labels (probe, app, zone, pod) ->
// timestamp -> value, failing on two series with the same labels.
func slidingWithoutRange(t *testing.T, base, query string, start, end time.Time, step time.Duration) map[string]map[string]float64 {
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
		var keys []string
		for _, name := range []string{"probe", "app", "zone", "pod"} {
			if v, ok := metric[name]; ok {
				keys = append(keys, fmt.Sprintf("%s=%v", name, v))
			}
		}
		sort.Strings(keys)
		key := strings.Join(keys, ",")
		if _, dup := out[key]; dup {
			t.Fatalf("%s %s: two series for {%s}", base, query, key)
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

// TestCompat_SlidingSumWithoutStreamFold: a `sum without` over count_over_time
// and bytes_over_time of short-lived pod streams on sliding windows (range over
// the step, and under it) answers Loki's series and values at every step:
// excluding pod (the only label that tells most streams apart), pod and zone
// (zone on a third of the streams), a label no stream has, with a line
// filter, and in the trailing spelling.
// conformance: semantics/without-sliding-sum-folded-buckets, metric-series-identity, loki_api_v1_query_range
func TestCompat_SlidingSumWithoutStreamFold(t *testing.T) {
	fx := ensureSlidingWithoutFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	forms := []string{
		"sum without (pod) (%s)",
		"sum without (pod, zone) (%s)",
		"sum without (baz) (%s)",
		"sum(%s) without (pod)",
	}
	shapes := []struct {
		name   string
		window string
		step   time.Duration
	}{
		{"range over step", "1m", 20 * time.Second},
		{"range 5m", "5m", time.Minute},
		{"range under step", "1m", 2 * time.Minute},
	}
	for _, shape := range shapes {
		for _, function := range []string{"count_over_time", "bytes_over_time"} {
			for _, filter := range []string{"", ` |= "tick"`} {
				for _, form := range forms {
					query := fmt.Sprintf(form, fmt.Sprintf("%s(%s%s [%s])", function, selector, filter, shape.window))
					t.Run(shape.name+"/"+query, func(t *testing.T) {
						start, end := fx.s0.Add(2*time.Minute), fx.s0.Add(32*time.Minute)
						loki := slidingWithoutRange(t, lokiURL, query, start, end, shape.step)
						proxy := slidingWithoutRange(t, proxyURL, query, start, end, shape.step)
						if strings.Contains(form, "(pod") || strings.Contains(form, "without (pod)") {
							if len(loki) < 2 || len(loki) > 4 {
								t.Fatalf("%s: Loki sanity: %d series, want one per app and zone", query, len(loki))
							}
						}
						compareNestedAgg(t, query, loki, proxy)
					})
				}
			}
		}
	}
}
