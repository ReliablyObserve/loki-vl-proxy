//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCompat_QueryRangeSettledSteps: the window evaluator's shapes asked
// again with the window moved forward one and two steps, moved back, re-run,
// widened, and refreshed the way Grafana does (the start on the step grid,
// the end a few seconds on) answer Loki's series and values every time, while
// the proxy keeps their settled steps (quality/query-range-settled-steps):
// `sum without (pod)` over max_over_time with unwrap and over
// count_over_time (folded by VictoriaLogs bucket by bucket), topk over it,
// quantile_over_time by a label and a grouped rate. The fixture rotates its
// pod every two minutes, as a Kubernetes deployment does, in two zones.
// conformance: quality/query-range-settled-steps, semantics/without-sliding-sum-folded-buckets, semantics/unwrap-sum-without-collapses, semantics/unwrap-over-time-range-windows, loki_api_v1_query_range
func TestCompat_QueryRangeSettledSteps(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("settled-steps-%d", now.UnixNano())
	s0 := now.Add(-5 * time.Hour).Truncate(time.Hour)
	type line struct {
		ts        time.Time
		pod, zone string
		msg       string
	}
	var lines []line
	for i := 0; i < 360; i++ { // a line every 10s for an hour, 3s past each mark
		zone := []string{"a", "b"}[i%2]
		pod := fmt.Sprintf("%s-%02d", zone, (i/12)%30) // a new pod every two minutes
		// Zone b's amounts are 100 above zone a's: a topk never meets a tie.
		amount := i%13 + 1
		if zone == "b" {
			amount += 100
		}
		lines = append(lines, line{ts: s0.Add(3*time.Second + time.Duration(i)*10*time.Second), pod: pod, zone: zone, msg: fmt.Sprintf("amount=%d", amount)})
	}
	// VictoriaLogs: service_name, pod, zone and detected_level are stream
	// fields; Loki: service_name, pod and zone are stream labels and
	// detected_level=unknown is structured metadata, so both keep the same
	// series labels (a stream that names its service gets none derived).
	var vlRows strings.Builder
	byStream := map[string][][]any{}
	labelsOf := map[string]map[string]string{}
	for _, l := range lines {
		row, _ := json.Marshal(map[string]string{"_time": l.ts.UTC().Format(time.RFC3339Nano), "_msg": l.msg, "service_name": app, "pod": l.pod, "zone": l.zone, "detected_level": "unknown"})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		key := l.pod + "/" + l.zone
		labelsOf[key] = map[string]string{"service_name": app, "pod": l.pod, "zone": l.zone}
		byStream[key] = append(byStream[key], []any{strconv.FormatInt(l.ts.UnixNano(), 10), l.msg, map[string]string{"detected_level": "unknown"}})
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,pod,zone,detected_level", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"}); status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	var streams []any
	for key, values := range byStream {
		streams = append(streams, map[string]any{"stream": labelsOf[key], "values": values})
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}
	sel := `{service_name="` + app + `"}`
	// Both backends hold every line before any comparison.
	control := `sum(count_over_time(` + sel + `[2h]))`
	deadline := time.Now().Add(180 * time.Second)
	for {
		l := settledStepsInstant(t, lokiURL, control, s0.Add(time.Hour))
		p := settledStepsInstant(t, proxyURL, control, s0.Add(time.Hour))
		if l == len(lines) && p == len(lines) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends not ready: Loki %d, proxy %d lines of %d", l, p, len(lines))
		}
		time.Sleep(2 * time.Second)
	}

	step := 30 * time.Second
	type window struct{ from, to time.Duration }
	base := window{10 * time.Minute, 40 * time.Minute}
	windows := []window{
		base,
		{base.from + step, base.to + step},
		{base.from + 2*step, base.to + 2*step},
		{base.from + time.Minute, base.to + time.Minute},
		{base.from - step, base.to - step},
		base,
		{base.from - 5*time.Minute, base.to + 2*time.Minute},
		// Grafana's refresh: the start stays on the step grid, the end moves on.
		{base.from, base.to + 10*time.Second},
		{base.from, base.to + 20*time.Second},
	}
	for _, query := range []string{
		`sum without (pod) (max_over_time(` + sel + ` | logfmt amount | unwrap amount [2m]))`,
		`sum without (pod) (count_over_time(` + sel + `[2m]))`,
		`topk(1, sum without (pod) (sum_over_time(` + sel + ` | logfmt amount | unwrap amount [2m])))`,
		`quantile_over_time(0.9, ` + sel + ` | logfmt amount | unwrap amount [2m]) by (zone)`,
		`sum by (zone) (rate(` + sel + `[2m]))`,
	} {
		for i, w := range windows {
			t.Run(fmt.Sprintf("%s/%d", query, i), func(t *testing.T) {
				from, to := s0.Add(w.from), s0.Add(w.to)
				lv := slidingRangeSeries(t, lokiURL, query, from, to, step, nil)
				pv := slidingRangeSeries(t, proxyURL, query, from, to, step, nil)
				if len(lv) == 0 {
					t.Fatalf("Loki answers no series")
				}
				if len(pv) != len(lv) {
					t.Fatalf("Loki has %d series %v, proxy %d %v", len(lv), settledStepsKeys(lv), len(pv), settledStepsKeys(pv))
				}
				for key, want := range lv {
					got, ok := pv[key]
					if !ok || len(got) != len(want) {
						t.Fatalf("{%s}: Loki %d points, proxy %d (series present %v)", key, len(want), len(got), ok)
					}
					for ts, wv := range want {
						a, _ := strconv.ParseFloat(wv, 64)
						b, err := strconv.ParseFloat(got[ts], 64)
						if err != nil || math.Abs(a-b) > 1e-9*math.Max(1, math.Abs(a)) {
							t.Fatalf("{%s} at %s: proxy %q, Loki %q", key, ts, got[ts], wv)
						}
					}
				}
			})
		}
	}
}

func settledStepsInstant(t *testing.T, base, query string, at time.Time) int {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	status, _, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" {
		return -1
	}
	total := 0
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		if value, ok := item.(map[string]interface{})["value"].([]interface{}); ok && len(value) == 2 {
			n, _ := strconv.Atoi(fmt.Sprint(value[1]))
			total += n
		}
	}
	return total
}

func settledStepsKeys(series map[string]map[string]string) []string {
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	return keys
}
