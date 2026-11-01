//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// conformance: loki_api_v1_query_range, window-bounds-and-step-alignment
// The ordinary Drilldown CI shard selects this test. Its fixed, historical
// windows exercise actual parser fallback without hiding it behind Tier0.
func TestDrilldown_OrderedJSONSettledWindows(t *testing.T) {
	end := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Minute)
	base := end.Add(-49 * time.Hour)
	app := fmt.Sprintf("json-steps-%d", time.Now().UnixNano())
	var rows, errors []jsonVolumeStreamLine
	for at, n := base, 0; !at.After(end.Add(5 * time.Minute)); at, n = at.Add(time.Minute), n+1 {
		rows = append(rows,
			jsonVolumeStreamLine{ts: at.Add(10 * time.Second), level: "info", msg: fmt.Sprintf(`{"level":"info","batch_id":"batch-%d"}`, n%4)},
			jsonVolumeStreamLine{ts: at.Add(20 * time.Second), level: "warn", msg: `{"level":"warn","batch_id":"batch-warn"}`})
		errors = append(errors, jsonVolumeStreamLine{ts: at.Add(20 * time.Second), level: "warn", msg: "malformed JSON fixture"})
	}
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{app: rows, app + "-errors": errors})
	for _, hours := range []int{24, 48} {
		for _, shape := range []struct {
			name, pipeline, group string
			tagged                bool
		}{
			{"DLV1", "| json", "detected_level", true},
			{"DLV2", `| json | __error__!=""`, "detected_level", false},
			{"DLV3", "| json", "detected_level", false},
			{"field", `| json batch_id="batch_id" | drop __error__, __error_details__ | batch_id!=""`, "batch_id", true},
		} {
			t.Run(fmt.Sprintf("%dh/%s", hours, shape.name), func(t *testing.T) {
				c := cache.NewWithMaxBytes(5*time.Minute, 10000, 64<<20)
				t.Cleanup(c.Close)
				route, recorder := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
					cfg.Cache = c
					cfg.QueryRangeExtentCache = true
					cfg.QueryRangeExtentCacheTTL = time.Hour
				})
				selectedApp := app
				if shape.name == "DLV2" {
					selectedApp += "-errors"
				}
				query := fmt.Sprintf(`sum by (%s) (count_over_time({app=%q} %s [1m]))`, shape.group, selectedApp, shape.pipeline)
				headers := map[string]string{}
				if shape.tagged {
					headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
				}
				for i, shift := range []time.Duration{0, 0, time.Minute, 2 * time.Minute, -time.Minute} {
					to := end.Add(shift)
					from := to.Add(-time.Duration(hours) * time.Hour)
					reference := slidingRangeSeries(t, lokiURL, query, from, to, time.Minute, headers)
					if len(reference) == 0 {
						t.Fatal("Loki fixture is empty")
					}
					recorder.reset()
					began := time.Now()
					got := slidingRangeSeries(t, route, query, from, to, time.Minute, headers)
					recorder.mu.Lock()
					windows := append([][2]time.Time(nil), recorder.windows...)
					recorder.mu.Unlock()
					calls := recorder.reset()
					t.Logf("MEASURE hours=%d shape=%s sample=%d shift=%s duration=%s vl_requests=%d raw_windows=%v", hours, shape.name, i, shift, time.Since(began), len(calls), windows)
					if !reflect.DeepEqual(got, reference) {
						t.Fatalf("sample %d differs from Loki", i)
					}
					if i == 1 && len(calls) != 0 {
						t.Errorf("exact repeat read backend: %v", calls)
					}
					if i > 1 && shape.name == "DLV2" {
						for _, window := range windows {
							if span := window[1].Sub(window[0]); span > 2*time.Minute+time.Nanosecond {
								t.Errorf("shifted window reread settled history: %s", span)
							}
						}
					}
					if i == 0 && shape.name == "DLV2" {
						// A full-response hit does not prove background steps have
						// published. Wait for observed admission before moving bounds.
						deadline := time.Now().Add(10 * time.Second)
						for {
							status, metrics := hardeningRequest(t, http.MethodGet, route+"/metrics", "", nil)
							stored := false
							for _, line := range strings.Split(string(metrics), "\n") {
								if value, ok := strings.CutPrefix(line, `loki_vl_proxy_internal_operation_total{operation="query_steps_cache",outcome="stored"} `); ok {
									n, _ := strconv.ParseFloat(value, 64)
									stored = n > 0
								}
							}
							if status == 200 && stored {
								break
							}
							if time.Now().After(deadline) {
								t.Fatal("settled-step admission did not finish")
							}
							select {
							case <-time.After(10 * time.Millisecond):
							case <-t.Context().Done():
								t.Fatal(t.Context().Err())
							}
						}
					}
				}
			})
		}
	}
}
