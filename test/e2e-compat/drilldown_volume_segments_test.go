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

// TestDrilldown_IndexVolumeSegmentsMatchLokiOnRepeat asks Logs Drilldown's
// landing volume (`{service_name=~...}`, targetLabels=service_name,
// limit=5000) over a window that reaches now, then again with the window
// moved by a few seconds each time, as a Grafana refresh does. The proxy
// answers the repeats from settled volume segments and reads only the edges
// from VictoriaLogs (quality/index-volume-settled-segments). Every answer must
// have Loki's series, order, timestamp and resultType, the fixture's exact
// line bytes, and Loki's bytes within what Loki adds on top of line bytes;
// rows written into the last minutes between two requests must show at once.
// conformance: quality/index-volume-settled-segments, loki_api_v1_index_volume
func TestDrilldown_IndexVolumeSegmentsMatchLokiOnRepeat(t *testing.T) {
	tag := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := func(suffix string) string { return "volseg-" + tag + "-" + suffix }
	streams := []volumeBytesStream{
		{labels: map[string]string{"app": app("a"), "pod": "p1"}, lineSize: 200, every: 5 * time.Second},
		{labels: map[string]string{"app": app("a"), "pod": "p2"}, lineSize: 500, every: 9 * time.Second},
		{labels: map[string]string{"app": app("b")}, lineSize: 300, every: 7 * time.Second},
		{labels: map[string]string{"app": app("c"), "team": "t1"}, lineSize: 1000, every: 13 * time.Second},
	}
	// 50 minutes of lines up to two minutes ago: the older part is sealed
	// (older than -max-stats-cache-freshness), the newer part is the live edge.
	now := time.Now()
	base := now.Add(-50 * time.Minute).Truncate(time.Minute)
	fixtureEnd := now.Add(-2 * time.Minute)
	var lines []volumeBytesLine
	push := func(from, to time.Time) {
		t.Helper()
		var vlRows strings.Builder
		var lokiStreams []map[string]any
		pushed := 0
		for i, s := range streams {
			var values [][]string
			for ts := from; ts.Before(to); ts = ts.Add(s.every) {
				msg := fmt.Sprintf("volume segment line %s", ts.Format(time.RFC3339))
				msg += strings.Repeat("y", s.lineSize-len(msg))
				values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), msg})
				row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": msg}
				for k, v := range s.labels {
					row[k] = v
				}
				encoded, _ := json.Marshal(row)
				vlRows.Write(encoded)
				vlRows.WriteByte('\n')
				lines = append(lines, volumeBytesLine{stream: i, ts: ts})
				pushed++
			}
			if len(values) > 0 {
				lokiStreams = append(lokiStreams, map[string]any{"stream": s.labels, "values": values})
			}
		}
		if pushed == 0 {
			return
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,pod,team", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		forceVLFlush(t)
	}
	push(base, fixtureEnd)
	selector := fmt.Sprintf(`{service_name=~"volseg-%s-.+"}`, tag)
	waitForFixtureOnBothBackends(t, fmt.Sprintf(`app:~"volseg-%s-.+"`, tag), fmt.Sprintf(`{app=~"volseg-%s-.+"}`, tag), base.Add(-time.Second), fixtureEnd, len(lines))

	byService := func(labels map[string]string) []string {
		return []string{volumeBytesName(map[string]string{"service_name": labels["app"]}, false)}
	}
	segmentsCached := func() int64 {
		return proxyMetricValue(t, proxyURL, `loki_vl_proxy_internal_operation_total{operation="volume_segments",outcome="cached"} `)
	}
	ask := func(round int, start, end time.Time) {
		t.Helper()
		params := url.Values{
			"query":        {selector},
			"targetLabels": {"service_name"},
			"limit":        {"5000"},
			"aggregateBy":  {"series"},
			"start":        {strconv.FormatInt(start.UnixNano(), 10)},
			"end":          {strconv.FormatInt(end.UnixNano(), 10)},
		}
		path := "/loki/api/v1/index/volume?" + params.Encode()
		loki := fetchVolumeBytes(t, lokiURL+path)
		proxy := fetchVolumeBytes(t, proxyURL+path)
		if loki.Data.ResultType != "vector" || proxy.Data.ResultType != "vector" {
			t.Fatalf("round %d: resultType loki=%s proxy=%s", round, loki.Data.ResultType, proxy.Data.ResultType)
		}
		if len(loki.Data.Result) != 3 || len(proxy.Data.Result) != 3 {
			t.Fatalf("round %d: series loki=%d proxy=%d, want 3\nloki=%+v\nproxy=%+v", round, len(loki.Data.Result), len(proxy.Data.Result), loki.Data.Result, proxy.Data.Result)
		}
		expected := expectedVolumeBytes(streams, lines, start, end, 0, byService)
		for i := range loki.Data.Result {
			lokiSeries, proxySeries := loki.Data.Result[i], proxy.Data.Result[i]
			if fmt.Sprint(lokiSeries.Metric) != fmt.Sprint(proxySeries.Metric) {
				t.Fatalf("round %d series %d: metric loki=%v proxy=%v (order must match)", round, i, lokiSeries.Metric, proxySeries.Metric)
			}
			lokiSamples, proxySamples := volumeSamples(t, lokiSeries), volumeSamples(t, proxySeries)
			if len(lokiSamples) != 1 || len(proxySamples) != 1 || lokiSamples[0].ts != proxySamples[0].ts {
				t.Fatalf("round %d %v: samples loki=%v proxy=%v", round, proxySeries.Metric, lokiSamples, proxySamples)
			}
			name := volumeBytesName(proxySeries.Metric, false)
			want, ok := expected[name][proxySamples[0].ts]
			if !ok {
				t.Fatalf("round %d %s: unexpected sample at %v; expected %v", round, name, proxySamples[0].ts, expected[name])
			}
			if proxySamples[0].bytes != float64(want.bytes) {
				t.Fatalf("round %d %s: proxy=%v, fixture line bytes=%d", round, name, proxySamples[0].bytes, want.bytes)
			}
			if diff := math.Abs(lokiSamples[0].bytes - proxySamples[0].bytes); diff > float64(want.allowance) {
				t.Fatalf("round %d %s: loki=%v proxy=%v differ by %v, beyond the documented structured-metadata allowance %d", round, name, lokiSamples[0].bytes, proxySamples[0].bytes, diff, want.allowance)
			}
		}
	}

	// An unaligned start: the window has a left edge, sealed segments and the
	// live edge of the last -max-stats-cache-freshness.
	start, end := base.Add(-7*time.Minute+23*time.Second), time.Now()
	ask(0, start, end)
	cachedBefore := segmentsCached()
	for round := 1; round <= 3; round++ {
		if round == 2 {
			// Rows written into the live edge between two refreshes show at
			// once, as in Loki, which reads that window live.
			push(fixtureEnd, time.Now().Add(-5*time.Second))
			waitForFixtureOnBothBackends(t, fmt.Sprintf(`app:~"volseg-%s-.+"`, tag), fmt.Sprintf(`{app=~"volseg-%s-.+"}`, tag), base.Add(-time.Second), time.Now(), len(lines))
		}
		start, end = start.Add(6*time.Second), time.Now()
		ask(round, start, end)
	}
	if got := segmentsCached(); got <= cachedBefore {
		t.Fatalf("the moved windows were not answered from cached volume segments (cached outcome %d -> %d)", cachedBefore, got)
	}
}
