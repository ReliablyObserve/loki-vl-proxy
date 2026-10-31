//go:build e2e

package e2e_compat

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// The in-process proxy deliberately has no Tier0 cache: an outer response hit
// would conceal a missing write on the ordered JSON dispatch path. The existing
// TestDrilldown_ CI shard runs this against real Loki and VictoriaLogs.
// conformance: loki_api_v1_query_range, parser-error-and-label-collision
func TestDrilldown_OrderedJSONExactResponseCache(t *testing.T) {
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Minute)
	validApp := fmt.Sprintf("json-cache-valid-%d", time.Now().UnixNano())
	errorApp := validApp + "-errors"
	fixtures := map[string][]jsonVolumeStreamLine{}
	for minute := range 4 {
		at := base.Add(time.Duration(minute)*time.Minute + 10*time.Second)
		fixtures[validApp] = append(fixtures[validApp],
			jsonVolumeStreamLine{ts: at, level: "info", msg: `{"level":"info","message":"served"}`},
			jsonVolumeStreamLine{ts: at.Add(time.Second), level: "warn", msg: `{"level":"warn","message":"served"}`})
		fixtures[errorApp] = append(fixtures[errorApp],
			jsonVolumeStreamLine{ts: at, level: "info", msg: `{"level":"info","message":"served"}`},
			jsonVolumeStreamLine{ts: at.Add(time.Second), level: "warn", msg: "malformed JSON fixture"})
	}
	ingestJSONVolumeFixture(t, fixtures)
	start, end := base.Add(time.Minute), base.Add(3*time.Minute)
	for _, shape := range []struct {
		name, app, pipeline string
		tagged              bool
	}{
		{"DLV1", validApp, "| json", true},
		{"DLV2", errorApp, `| json | __error__!=""`, false},
		{"DLV3", validApp, "| json", false},
		{"native", validApp, "", false},
	} {
		t.Run(shape.name, func(t *testing.T) {
			query := fmt.Sprintf(`sum by (detected_level) (count_over_time({app=%q} %s [1m]))`, shape.app, shape.pipeline)
			headers := map[string]string{}
			if shape.tagged {
				headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
			}
			want := map[string]map[string]string{}
			for _, level := range []string{"info", "warn"} {
				if shape.name == "DLV2" && level == "info" {
					continue
				}
				points := map[string]string{}
				for at := start; !at.After(end); at = at.Add(time.Minute) {
					points[strconv.FormatInt(at.Unix(), 10)] = "1"
				}
				want["detected_level="+level] = points
			}
			deadline := time.Now().Add(180 * time.Second)
			for {
				reference := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, headers)
				if reflect.DeepEqual(reference, want) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("Loki never exposed the complete fixture: got %v want %v", reference, want)
				}
				select {
				case <-time.After(time.Second):
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				}
			}
			c := cache.NewWithMaxBytes(5*time.Minute, 256, 8<<20)
			t.Cleanup(c.Close)
			route, recorder := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) { cfg.Cache = c })
			recorder.reset()
			assertSlidingParity(t, query, want, slidingRangeSeries(t, route, query, start, end, time.Minute, headers))
			if calls := recorder.reset(); len(calls) == 0 {
				t.Fatal("cold response did not exercise the actual backend")
			}
			assertSlidingParity(t, query, want, slidingRangeSeries(t, route, query, start, end, time.Minute, headers))
			if calls := recorder.reset(); len(calls) != 0 {
				t.Fatalf("exact repeat read VictoriaLogs again: %v", calls)
			}
		})
	}
}
