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
	"testing"
	"time"
)

// volumeBytesStream is one stream of the volume fixture: every line has the
// same length and cadence, so each side's bytes are easy to derive.
type volumeBytesStream struct {
	labels   map[string]string
	lineSize int
	every    time.Duration
}

type volumeBytesLine struct {
	stream int
	ts     time.Time
}

type volumeBytesSeries struct {
	Metric map[string]string   `json:"metric"`
	Value  []json.RawMessage   `json:"value"`
	Values [][]json.RawMessage `json:"values"`
}

type volumeBytesResponse struct {
	Status   string   `json:"status"`
	Warnings []string `json:"warnings"`
	Data     struct {
		ResultType string              `json:"resultType"`
		Result     []volumeBytesSeries `json:"result"`
	} `json:"data"`
}

type volumeBytesSample struct {
	ts    float64
	bytes float64
}

// TestDrilldown_IndexVolumeBytesMatchLoki compares /index/volume and
// /index/volume_range against Loki on a unique fixture. Loki reports volume in
// bytes: labels, timestamps, result type and ordering must match exactly, the
// proxy must report the fixture's exact line bytes, and Loki must agree within
// what Loki adds on top of line bytes (structured metadata such as
// detected_level, chunk-level size estimates and KB rounding of flushed chunks).
// conformance: loki_api_v1_index_volume
func TestDrilldown_IndexVolumeBytesMatchLoki(t *testing.T) {
	tag := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := func(suffix string) string { return "volbytes-" + tag + "-" + suffix }
	streams := []volumeBytesStream{
		{labels: map[string]string{"app": app("a"), "pod": "p1"}, lineSize: 200, every: 2 * time.Second},
		{labels: map[string]string{"app": app("a"), "pod": "p2"}, lineSize: 500, every: 4 * time.Second},
		{labels: map[string]string{"app": app("b"), "pod": "p1"}, lineSize: 300, every: 6 * time.Second},
		{labels: map[string]string{"app": app("b")}, lineSize: 160, every: 3 * time.Second},
		{labels: map[string]string{"app": app("c"), "pod": "p3", "team": "t1"}, lineSize: 1000, every: 12 * time.Second},
	}
	const step = 300 * time.Second
	base := time.Now().Add(-20 * time.Minute).Truncate(step)
	fixtureEnd := base.Add(18 * time.Minute)

	var lines []volumeBytesLine
	var vlRows strings.Builder
	var lokiStreams []map[string]any
	for i, s := range streams {
		var values [][]string
		for ts := base; ts.Before(fixtureEnd); ts = ts.Add(s.every) {
			msg := fmt.Sprintf("volume line %s", ts.Format(time.RFC3339))
			msg += strings.Repeat("x", s.lineSize-len(msg))
			values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), msg})
			row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": msg}
			for k, v := range s.labels {
				row[k] = v
			}
			encoded, _ := json.Marshal(row)
			vlRows.Write(encoded)
			vlRows.WriteByte('\n')
			lines = append(lines, volumeBytesLine{stream: i, ts: ts})
		}
		lokiStreams = append(lokiStreams, map[string]any{"stream": s.labels, "values": values})
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

	selector := fmt.Sprintf(`{app=~"volbytes-%s-.+"}`, tag)
	waitForFixtureOnBothBackends(t, fmt.Sprintf(`app:~"volbytes-%s-.+"`, tag), selector, base.Add(-time.Second), fixtureEnd, len(lines))

	cases := []struct {
		name       string
		rangeQuery bool
		params     url.Values
		// key names the Loki volume a stream contributes to (nil = skipped).
		keys func(labels map[string]string) []string
		// wantSeries is the exact number of series Loki returns.
		wantSeries int
	}{
		{name: "series_by_selector_label", params: url.Values{"query": {selector}},
			keys: seriesKey("app"), wantSeries: 3},
		{name: "series_by_all_selector_labels", params: url.Values{"query": {fmt.Sprintf(`{app=~"volbytes-%s-.+", pod=~".+"}`, tag)}},
			keys: requiredSeriesKey("app", "pod"), wantSeries: 4},
		{name: "target_label", params: url.Values{"query": {selector}, "targetLabels": {"pod"}},
			keys: requiredSeriesKey("pod"), wantSeries: 3},
		{name: "target_labels_all_required", params: url.Values{"query": {selector}, "targetLabels": {"pod,team"}},
			keys: requiredSeriesKey("pod", "team"), wantSeries: 1},
		{name: "aggregate_by_labels", params: url.Values{"query": {selector}, "aggregateBy": {"labels"}},
			keys: labelsKey(nil), wantSeries: 4},
		{name: "aggregate_by_labels_target", params: url.Values{"query": {selector}, "aggregateBy": {"labels"}, "targetLabels": {"pod"}},
			keys: labelsKey([]string{"pod"}), wantSeries: 1},
		{name: "limit", params: url.Values{"query": {selector}, "limit": {"2"}},
			keys: seriesKey("app"), wantSeries: 2},
		{name: "range_series", rangeQuery: true, params: url.Values{"query": {selector}, "step": {"300"}},
			keys: seriesKey("app"), wantSeries: 3},
		{name: "range_target_label", rangeQuery: true, params: url.Values{"query": {selector}, "step": {"300"}, "targetLabels": {"pod"}},
			keys: requiredSeriesKey("pod"), wantSeries: 3},
		{name: "range_aggregate_by_labels_limit", rangeQuery: true, params: url.Values{"query": {selector}, "step": {"300"}, "aggregateBy": {"labels"}, "limit": {"2"}},
			keys: labelsKey(nil), wantSeries: 2},
		{name: "range_single_bucket_is_vector", rangeQuery: true, params: url.Values{"query": {selector}, "step": {"3600"}},
			keys: seriesKey("app"), wantSeries: 3},
	}

	// The fixture stays in Loki's ingester, which sizes chunks exactly. Loki's
	// store path rounds flushed chunks to whole KiB and sums a chunk once per
	// TSDB index file that lists it, so flushed volumes are not compared here.
	// Unaligned bounds between line timestamps exercise Loki's partial first
	// and last buckets.
	start := base.Add(35 * time.Second)
	end := fixtureEnd.Add(-5 * time.Second)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := url.Values{}
			for k, v := range tc.params {
				params[k] = v
			}
			params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
			params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
			path := "/loki/api/v1/index/volume"
			if tc.rangeQuery {
				path = "/loki/api/v1/index/volume_range"
			}
			loki := fetchVolumeBytes(t, lokiURL+path+"?"+params.Encode())
			proxy := fetchVolumeBytes(t, proxyURL+path+"?"+params.Encode())

			if loki.Data.ResultType != proxy.Data.ResultType {
				t.Fatalf("resultType loki=%s proxy=%s", loki.Data.ResultType, proxy.Data.ResultType)
			}
			if len(loki.Data.Result) != tc.wantSeries || len(proxy.Data.Result) != tc.wantSeries {
				t.Fatalf("series loki=%d proxy=%d want %d\nloki=%+v\nproxy=%+v", len(loki.Data.Result), len(proxy.Data.Result), tc.wantSeries, loki.Data.Result, proxy.Data.Result)
			}
			stepFor := time.Duration(0)
			if tc.rangeQuery {
				secs, _ := strconv.Atoi(tc.params.Get("step"))
				stepFor = time.Duration(secs) * time.Second
			}
			expected := expectedVolumeBytes(streams, lines, start, end, stepFor, tc.keys)
			for i := range loki.Data.Result {
				lokiSeries, proxySeries := loki.Data.Result[i], proxy.Data.Result[i]
				if fmt.Sprint(lokiSeries.Metric) != fmt.Sprint(proxySeries.Metric) {
					t.Fatalf("series %d metric loki=%v proxy=%v (ordering must match)", i, lokiSeries.Metric, proxySeries.Metric)
				}
				lokiSamples, proxySamples := volumeSamples(t, lokiSeries), volumeSamples(t, proxySeries)
				if len(lokiSamples) != len(proxySamples) {
					t.Fatalf("%v: samples loki=%v proxy=%v", lokiSeries.Metric, lokiSamples, proxySamples)
				}
				name := volumeBytesName(proxySeries.Metric, tc.params.Get("aggregateBy") == "labels")
				for j := range lokiSamples {
					if lokiSamples[j].ts != proxySamples[j].ts {
						t.Fatalf("%s sample %d timestamp loki=%v proxy=%v", name, j, lokiSamples[j].ts, proxySamples[j].ts)
					}
					want, ok := expected[name][proxySamples[j].ts]
					if !ok {
						t.Fatalf("%s: unexpected sample at %v; expected %v", name, proxySamples[j].ts, expected[name])
					}
					if proxySamples[j].bytes != float64(want.bytes) {
						t.Fatalf("%s at %v: proxy=%v, fixture line bytes=%d", name, proxySamples[j].ts, proxySamples[j].bytes, want.bytes)
					}
					if j == 0 {
						t.Logf("%s at %v: loki=%v proxy=%v allowance=%d", name, proxySamples[j].ts, lokiSamples[j].bytes, proxySamples[j].bytes, want.allowance)
					}
					if diff := math.Abs(lokiSamples[j].bytes - proxySamples[j].bytes); diff > float64(want.allowance) {
						t.Fatalf("%s at %v: loki=%v proxy=%v differ by %v, beyond the documented structured-metadata allowance %d", name, proxySamples[j].ts, lokiSamples[j].bytes, proxySamples[j].bytes, diff, want.allowance)
					}
				}
			}
		})
	}
}

func fetchVolumeBytes(t *testing.T, target string) volumeBytesResponse {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, target, "", map[string]string{"X-Scope-OrgID": "0"})
	var resp volumeBytesResponse
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil || resp.Status != "success" || len(resp.Warnings) != 0 {
		t.Fatalf("%s: %d %s", target, status, body)
	}
	return resp
}

func volumeSamples(t *testing.T, series volumeBytesSeries) []volumeBytesSample {
	t.Helper()
	pairs := series.Values
	if len(series.Value) == 2 {
		pairs = append(pairs, series.Value)
	}
	samples := make([]volumeBytesSample, 0, len(pairs))
	for _, pair := range pairs {
		var ts float64
		var raw string
		if len(pair) != 2 || json.Unmarshal(pair[0], &ts) != nil || json.Unmarshal(pair[1], &raw) != nil {
			t.Fatalf("malformed sample %v", pair)
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("malformed value %q", raw)
		}
		samples = append(samples, volumeBytesSample{ts: ts, bytes: v})
	}
	return samples
}

// seriesKey names a volume by the listed labels a stream has.
func seriesKey(names ...string) func(map[string]string) []string {
	return func(labels map[string]string) []string {
		metric := map[string]string{}
		for _, n := range names {
			if v := labels[n]; v != "" {
				metric[n] = v
			}
		}
		return []string{volumeBytesName(metric, false)}
	}
}

// requiredSeriesKey skips streams that lack any of the labels.
func requiredSeriesKey(names ...string) func(map[string]string) []string {
	return func(labels map[string]string) []string {
		for _, n := range names {
			if labels[n] == "" {
				return nil
			}
		}
		return seriesKey(names...)(labels)
	}
}

// labelsKey names one volume per label name (service_name is Loki's
// discovered label, derived from app on both sides).
func labelsKey(targets []string) func(map[string]string) []string {
	return func(labels map[string]string) []string {
		all := map[string]string{"service_name": labels["app"]}
		for k, v := range labels {
			all[k] = v
		}
		var names []string
		if len(targets) == 0 {
			for k := range all {
				names = append(names, k)
			}
			return names
		}
		for _, n := range targets {
			if all[n] == "" {
				return nil
			}
		}
		return targets
	}
}

func volumeBytesName(metric map[string]string, byLabels bool) string {
	keys := make([]string, 0, len(metric))
	for k := range metric {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if byLabels {
		return strings.Join(keys, ",")
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Quote(metric[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

type volumeBytesExpectation struct {
	bytes     int
	allowance int
}

// expectedVolumeBytes derives each volume sample from the fixture: exact line
// bytes, and the allowance for what Loki adds per sample. Loki counts 8 bytes of
// structured-metadata symbol references per line (discover_log_levels adds
// detected_level), plus per-chunk symbol bytes, time-proportional chunk size
// estimates.
func expectedVolumeBytes(streams []volumeBytesStream, lines []volumeBytesLine, start, end time.Time, step time.Duration, keys func(map[string]string) []string) map[string]map[float64]volumeBytesExpectation {
	type acc struct {
		bytes   int
		lines   int
		streams map[int]bool
		maxLine int
	}
	sums := map[string]map[float64]*acc{}
	endMs := end.UnixMilli()
	for _, line := range lines {
		if line.ts.Before(start) || !line.ts.Before(end) {
			continue
		}
		stampMs := endMs
		if step > 0 {
			bucketEnd := line.ts.Truncate(step).Add(step)
			if bucketEnd.Before(end) {
				stampMs = bucketEnd.UnixMilli() - 1
			}
		}
		stamp := float64(stampMs) / 1e3
		s := streams[line.stream]
		for _, name := range keys(s.labels) {
			if sums[name] == nil {
				sums[name] = map[float64]*acc{}
			}
			a := sums[name][stamp]
			if a == nil {
				a = &acc{streams: map[int]bool{}}
				sums[name][stamp] = a
			}
			a.bytes += s.lineSize
			a.lines++
			a.streams[line.stream] = true
			if s.lineSize > a.maxLine {
				a.maxLine = s.lineSize
			}
		}
	}
	out := map[string]map[float64]volumeBytesExpectation{}
	for name, byStamp := range sums {
		out[name] = map[float64]volumeBytesExpectation{}
		for stamp, a := range byStamp {
			out[name][stamp] = volumeBytesExpectation{
				bytes:     a.bytes,
				allowance: 8*a.lines + len(a.streams)*(2*a.maxLine+1088),
			}
		}
	}
	return out
}
