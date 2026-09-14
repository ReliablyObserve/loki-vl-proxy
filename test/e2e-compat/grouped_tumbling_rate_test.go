//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// groupedTumblingLine has a fixed length so bytes_over_time/bytes_rate are
// deterministic on both backends.
const (
	groupedTumblingLine      = `level=info msg="grouped tumbling fixture"`
	groupedTumblingLineBytes = float64(len(groupedTumblingLine))
)

// TestRangeMetricCompatibilityGroupedTumblingLongRange compares Loki and the
// proxy for single-label grouped range metrics whose range window equals the
// step over a range of at least two hours. Such requests reach the proxy's
// high-cardinality single-field count paths; Loki returns per-second values for
// rate and bytes_rate there, never the raw per-window counts.
func TestRangeMetricCompatibilityGroupedTumblingLongRange(t *testing.T) {
	namespace := fmt.Sprintf("e2e-grouped-tumbling-%d", time.Now().UnixNano())
	now := time.Now().UTC()
	// Lines start two seconds past an hour boundary so none sits exactly on a
	// window edge, and run up to now so Loki's recent-data probe sees them.
	base := now.Add(-5*time.Hour - 30*time.Minute).Truncate(time.Hour).Add(2 * time.Second)
	intervals := map[string]time.Duration{"fast": 10 * time.Second, "slow": 20 * time.Second}
	total := 0
	for _, app := range []string{"fast", "slow"} {
		n := int(now.Sub(base) / intervals[app])
		lines := make([]string, n)
		for i := range lines {
			lines[i] = groupedTumblingLine
		}
		total += n
		pushStream(t, base, streamDef{
			Labels:   map[string]string{"namespace": namespace, "app": app},
			Lines:    lines,
			Interval: intervals[app],
		})
	}
	selector := `{namespace="` + namespace + `"}`
	waitForLokiMetricDataSelector(t, selector)
	waitForGroupedTumblingTotal(t, selector, total, now)

	// Step-aligned window fully inside the data: the earliest [1h] window
	// starts one hour before start, still after the first line.
	end := now.Add(-15 * time.Minute).Truncate(time.Hour)
	start := end.Add(-3 * time.Hour)
	cases := []struct {
		name  string
		query string
		step  time.Duration
		want  map[string]float64 // Loki's per-step value for every app
	}{
		// Per window, fast logs 30 lines per 5m (360 per 1h) and slow half that;
		// rate-like values are the window total divided by the window seconds.
		{"rate_5m_step_300", `sum by (app) (rate(` + selector + `[5m]))`, 5 * time.Minute, map[string]float64{"fast": 30.0 / 300, "slow": 15.0 / 300}},
		{"rate_1h_step_3600", `sum by (app) (rate(` + selector + `[1h]))`, time.Hour, map[string]float64{"fast": 360.0 / 3600, "slow": 180.0 / 3600}},
		{"bytes_rate_5m_step_300", `sum by (app) (bytes_rate(` + selector + `[5m]))`, 5 * time.Minute, map[string]float64{"fast": 30 * groupedTumblingLineBytes / 300, "slow": 15 * groupedTumblingLineBytes / 300}},
		{"count_over_time_5m_step_300", `sum by (app) (count_over_time(` + selector + `[5m]))`, 5 * time.Minute, map[string]float64{"fast": 30, "slow": 15}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			startRaw, endRaw := strconv.FormatInt(start.Unix(), 10), strconv.FormatInt(end.Unix(), 10)
			stepRaw := strconv.FormatInt(int64(tc.step/time.Second), 10)
			loki := queryRangeResult(t, lokiURL, tc.query, startRaw, endRaw, stepRaw)
			proxy := queryRangeResult(t, proxyURL, tc.query, startRaw, endRaw, stepRaw)
			if loki.StatusCode != http.StatusOK || proxy.StatusCode != http.StatusOK {
				t.Fatalf("status loki=%d proxy=%d\nloki=%s\nproxy=%s", loki.StatusCode, proxy.StatusCode, loki.Body, proxy.Body)
			}
			lokiData, _ := loki.JSON["data"].(map[string]interface{})
			proxyData, _ := proxy.JSON["data"].(map[string]interface{})
			lokiFlat := flattenMatrixResult(t, lokiData["result"])
			proxyFlat := flattenMatrixResult(t, proxyData["result"])

			wantPoints := int(end.Sub(start)/tc.step) + 1
			for app, value := range tc.want {
				for i := 0; i < wantPoints; i++ {
					key := fmt.Sprintf("{app=%s}@%d", app, start.Add(time.Duration(i)*tc.step).Unix())
					if got, ok := lokiFlat[key]; !ok || got != value {
						t.Fatalf("Loki fixture not fully queryable: %s = %v (present=%v), want %v; body=%s", key, got, ok, value, loki.Body)
					}
				}
			}
			if len(lokiFlat) != len(tc.want)*wantPoints {
				t.Fatalf("Loki returned %d samples, want %d: %s", len(lokiFlat), len(tc.want)*wantPoints, loki.Body)
			}

			var diffs []string
			for key, lv := range lokiFlat {
				if pv, ok := proxyFlat[key]; !ok {
					diffs = append(diffs, fmt.Sprintf("%s: loki=%v proxy=missing", key, lv))
				} else if pv != lv {
					diffs = append(diffs, fmt.Sprintf("%s: loki=%v proxy=%v", key, lv, pv))
				}
			}
			for key, pv := range proxyFlat {
				if _, ok := lokiFlat[key]; !ok {
					diffs = append(diffs, fmt.Sprintf("%s: loki=missing proxy=%v", key, pv))
				}
			}
			if len(diffs) > 0 {
				sort.Strings(diffs)
				if len(diffs) > 10 {
					diffs = append(diffs[:10], fmt.Sprintf("... %d more", len(diffs)-10))
				}
				t.Fatalf("proxy differs from Loki for %s:\n%s", tc.query, strings.Join(diffs, "\n"))
			}
		})
	}
}

// waitForGroupedTumblingTotal polls instant totals until both Loki and the
// proxy see every pushed line. The probe query differs from the compared ones,
// so a partially indexed answer never lands in Loki's results cache for them.
func waitForGroupedTumblingTotal(t *testing.T, selector string, total int, at time.Time) {
	t.Helper()
	params := url.Values{}
	params.Set("query", `sum(count_over_time(`+selector+`[7h]))`)
	params.Set("time", strconv.FormatInt(at.Add(time.Minute).Unix(), 10))
	want := strconv.Itoa(total)
	deadline := time.Now().Add(90 * time.Second)
	var lastLoki, lastProxy string
	for time.Now().Before(deadline) {
		_, lokiBody, lokiResp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+params.Encode(), nil)
		_, proxyBody, proxyResp := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+params.Encode(), nil)
		lastLoki, lastProxy = lokiBody, proxyBody
		if instantScalarValue(lokiResp) == want && instantScalarValue(proxyResp) == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("fixture of %d lines not fully queryable after 90s\nloki=%s\nproxy=%s", total, lastLoki, lastProxy)
}

func instantScalarValue(resp map[string]interface{}) string {
	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) != 1 {
		return ""
	}
	series, _ := result[0].(map[string]interface{})
	value, _ := series["value"].([]interface{})
	if len(value) != 2 {
		return ""
	}
	return fmt.Sprint(value[1])
}
