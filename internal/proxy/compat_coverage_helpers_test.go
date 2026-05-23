package proxy

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestCompatHelpers_ParseQuantileAndUnwrapErrorName(t *testing.T) {
	if got := unwrapErrorFuncName(""); got != "range_aggregation" {
		t.Fatalf("expected default unwrap error func, got %q", got)
	}
	if got := unwrapErrorFuncName(" sum_over_time "); got != "sum_over_time" {
		t.Fatalf("expected trimmed unwrap func name, got %q", got)
	}

	phi, field, ok := parseStatsQuantileSpec("0.95, latency_ms")
	if !ok {
		t.Fatal("expected quantile spec to parse")
	}
	if phi != 0.95 || field != "latency_ms" {
		t.Fatalf("unexpected quantile spec parse: phi=%v field=%q", phi, field)
	}

	if _, _, ok := parseStatsQuantileSpec("bad"); ok {
		t.Fatal("expected invalid quantile spec without comma to fail")
	}
	if _, _, ok := parseStatsQuantileSpec("x,latency"); ok {
		t.Fatal("expected invalid quantile phi to fail")
	}

	field, conv := parseUnwrapExpression(`duration("latency_ms")`)
	if field != "latency_ms" || conv != "duration" {
		t.Fatalf("expected duration unwrap parse, got field=%q conv=%q", field, conv)
	}
	field, conv = parseUnwrapExpression("bytes(size)")
	if field != "size" || conv != "bytes" {
		t.Fatalf("expected bytes unwrap parse, got field=%q conv=%q", field, conv)
	}
	field, conv = parseUnwrapExpression("`custom.value`")
	if field != "custom.value" || conv != "" {
		t.Fatalf("expected passthrough unwrap parse, got field=%q conv=%q", field, conv)
	}
	if !metricFuncRequiresUnwrap("sum_over_time") {
		t.Fatal("expected sum_over_time to require unwrap")
	}
	if metricFuncRequiresUnwrap("count_over_time") {
		t.Fatal("expected count_over_time not to require unwrap")
	}
	if shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "avg", false) {
		t.Fatal("expected parser-stage avg to use VL stats_query_range, not manual NDJSON fallback")
	}
	if shouldUseManualRangeMetricCompat(`{app="api"}`, "avg", false) {
		t.Fatal("expected non-parser query not to use manual fallback for avg")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"}`, "rate_counter", false) {
		t.Fatal("expected rate_counter to always use manual fallback")
	}

	// rate / bytes_rate: sliding window (range != step) always requires manual path.
	// Tumbling window (range == step) also requires manual path for parser-stage queries:
	// Loki excludes parse-failed lines from metric aggregation; VL native stats counts all.
	// Callers that explicitly add "| drop __error__" opt in to VL's count-all semantics;
	// that opt-in is checked at the call site (handleStatsCompatRange) before this function.
	if !shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "rate", false) {
		t.Fatal("expected parser-stage rate to use manual fallback when range != step")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "rate", true) {
		t.Fatal("expected parser-stage rate to use manual fallback even when range == step (no drop-error opt-in)")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"}`, "rate", false) {
		t.Fatal("expected non-parser rate to use manual fallback when range != step (sliding window)")
	}
	if shouldUseManualRangeMetricCompat(`{app="api"}`, "rate", true) {
		t.Fatal("expected non-parser rate to use VL native stats when range == step")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"}`, "bytes_rate", false) {
		t.Fatal("expected non-parser bytes_rate to use manual fallback when range != step")
	}
	if shouldUseManualRangeMetricCompat(`{app="api"}`, "bytes_rate", true) {
		t.Fatal("expected non-parser bytes_rate to use VL native stats when range == step")
	}

	// count_over_time / bytes_over_time: for sliding windows (range != step) VL native
	// stats buckets by step while LogQL evaluates each point over [T-range, T]. With
	// non-uniform data distributions the two diverge — route to manual path regardless
	// of whether parser stages are present.
	if !shouldUseManualRangeMetricCompat(`{app="api"}`, "count_over_time", false) {
		t.Fatal("expected non-parser count_over_time to use manual fallback when range != step")
	}
	if shouldUseManualRangeMetricCompat(`{app="api"}`, "count_over_time", true) {
		t.Fatal("expected non-parser count_over_time to use VL native stats when range == step")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"}`, "bytes_over_time", false) {
		t.Fatal("expected non-parser bytes_over_time to use manual fallback when range != step")
	}
	if shouldUseManualRangeMetricCompat(`{app="api"}`, "bytes_over_time", true) {
		t.Fatal("expected non-parser bytes_over_time to use VL native stats when range == step")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "count_over_time", false) {
		t.Fatal("expected parser-stage count_over_time to use manual fallback when range != step")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "count_over_time", true) {
		t.Fatal("expected parser-stage count_over_time to use manual fallback even when range == step (no drop-error opt-in)")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "bytes_over_time", false) {
		t.Fatal("expected parser-stage bytes_over_time to use manual fallback when range != step")
	}
	if !shouldUseManualRangeMetricCompat(`{app="api"} | unpack_json`, "bytes_over_time", true) {
		t.Fatal("expected parser-stage bytes_over_time to use manual fallback even when range == step (no drop-error opt-in)")
	}
}

func TestCompatHelpers_AddParsedEntryLabels(t *testing.T) {
	metricLabels := map[string]string{
		"service_name": "api",
		"level":        "info",
	}
	entry := map[string]interface{}{
		"_time":        "ignored",
		"_stream_id":   "ignored",
		"latency.ms":   "10",
		"latency_ms":   "11",
		"service_name": "existing",
		"trace_id":     "abc123",
		"status":       200,
		"empty":        "   ",
	}

	addParsedEntryLabels(metricLabels, entry, "latency.ms")

	if got := metricLabels["service_name"]; got != "api" {
		t.Fatalf("expected existing label to remain unchanged, got %q", got)
	}
	if _, exists := metricLabels["latency.ms"]; exists {
		t.Fatal("expected unwrap field to be excluded from parsed labels")
	}
	if _, exists := metricLabels["latency_ms"]; exists {
		t.Fatal("expected unwrap underscore alias to be excluded from parsed labels")
	}
	if got := metricLabels["trace_id"]; got != "abc123" {
		t.Fatalf("expected trace_id to be added, got %q", got)
	}
	if got := metricLabels["status"]; got != "200" {
		t.Fatalf("expected numeric parsed value to stringify, got %q", got)
	}
	if _, exists := metricLabels["_time"]; exists {
		t.Fatal("expected internal _time field to remain excluded")
	}
}

func TestCompatHelpers_AggregateManualWindow(t *testing.T) {
	samples := []rangeMetricSample{
		{ts: 0, value: 1},
		{ts: 10, value: 2},
		{ts: 20, value: 3},
	}

	assertClose := func(name string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 0.000001 {
			t.Fatalf("%s: expected %v, got %v", name, want, got)
		}
	}

	if got, ok := aggregateManualWindow("count_over_time", 0, samples, 0, 20, 20); !ok || got != 3 {
		t.Fatalf("count_over_time: expected 3,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("rate", 0, samples, 0, 20, 20); !ok {
		t.Fatal("rate: expected success")
	} else {
		assertClose("rate", got, 0.15)
	}
	if got, ok := aggregateManualWindow("bytes_over_time", 0, samples, 0, 20, 20); !ok || got != 6 {
		t.Fatalf("bytes_over_time: expected 6,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("bytes_rate", 0, samples, 0, 20, 20); !ok {
		t.Fatal("bytes_rate: expected success")
	} else {
		assertClose("bytes_rate", got, 0.3)
	}
	if got, ok := aggregateManualWindow("sum", 0, samples, 0, 20, 20); !ok || got != 6 {
		t.Fatalf("sum: expected 6,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("avg", 0, samples, 0, 20, 20); !ok || got != 2 {
		t.Fatalf("avg: expected 2,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("min", 0, samples, 0, 20, 20); !ok || got != 1 {
		t.Fatalf("min: expected 1,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("max", 0, samples, 0, 20, 20); !ok || got != 3 {
		t.Fatalf("max: expected 3,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("stddev", 0, samples, 0, 20, 20); !ok {
		t.Fatal("stddev: expected success")
	} else {
		assertClose("stddev", got, math.Sqrt(2.0/3.0))
	}
	if got, ok := aggregateManualWindow("stdvar", 0, samples, 0, 20, 20); !ok {
		t.Fatal("stdvar: expected success")
	} else {
		assertClose("stdvar", got, 2.0/3.0)
	}
	if got, ok := aggregateManualWindow("quantile", 0.5, samples, 0, 20, 20); !ok || got != 2 {
		t.Fatalf("quantile: expected 2,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("first", 0, samples, 0, 20, 20); !ok || got != 1 {
		t.Fatalf("first: expected 1,true got %v,%v", got, ok)
	}
	if got, ok := aggregateManualWindow("last", 0, samples, 0, 20, 20); !ok || got != 3 {
		t.Fatalf("last: expected 3,true got %v,%v", got, ok)
	}

	counterSamples := []rangeMetricSample{
		{ts: 0, value: 100},
		{ts: 10, value: 130},
		{ts: 20, value: 10},
		{ts: 30, value: 30},
	}
	if got, ok := aggregateManualWindow("rate_counter", 0, counterSamples, 0, 30, 30); !ok {
		t.Fatal("rate_counter: expected success")
	} else {
		assertClose("rate_counter", got, 2.0)
	}

	if _, ok := aggregateManualWindow("unknown", 0, samples, 0, 20, 20); ok {
		t.Fatal("expected unknown aggregate function to fail")
	}
	if _, ok := aggregateManualWindow("rate", 0, samples, 0, 20, 0); ok {
		t.Fatal("expected rate with non-positive windowSeconds to fail")
	}
	if _, ok := aggregateManualWindow("sum", 0, samples, 999, 1000, 1); ok {
		t.Fatal("expected aggregate on empty sample window to fail")
	}
}

func TestCompatHelpers_BuildManualRangeResponses(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(2 * time.Minute)
	step := time.Minute
	window := time.Minute
	series := map[string]manualSeriesSamples{
		"{app=\"api\"}": {
			Metric: map[string]string{"app": "api"},
			Samples: []rangeMetricSample{
				{ts: start.UnixNano(), value: 1},
				{ts: start.Add(time.Minute).UnixNano(), value: 2},
				{ts: end.UnixNano(), value: 3},
			},
		},
	}

	var matrixResp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(buildManualRangeMetricMatrix("count_over_time", 0, series, start, end, step, window), &matrixResp); err != nil {
		t.Fatalf("decode matrix response: %v", err)
	}
	if matrixResp.Status != "success" || matrixResp.Data.ResultType != "matrix" {
		t.Fatalf("unexpected matrix response envelope: %+v", matrixResp)
	}
	if len(matrixResp.Data.Result) != 1 || len(matrixResp.Data.Result[0].Values) == 0 {
		t.Fatalf("expected non-empty matrix points, got %+v", matrixResp.Data.Result)
	}

	var vectorResp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(buildManualRangeMetricVector("last", 0, series, end, window), &vectorResp); err != nil {
		t.Fatalf("decode vector response: %v", err)
	}
	if vectorResp.Status != "success" || vectorResp.Data.ResultType != "vector" {
		t.Fatalf("unexpected vector response envelope: %+v", vectorResp)
	}
	if len(vectorResp.Data.Result) != 1 || len(vectorResp.Data.Result[0].Value) != 2 {
		t.Fatalf("expected single vector point, got %+v", vectorResp.Data.Result)
	}

	var emptyMatrix struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(buildManualRangeMetricMatrix("sum", 0, series, end, start, step, window), &emptyMatrix); err != nil {
		t.Fatalf("decode empty matrix response: %v", err)
	}
	if len(emptyMatrix.Data.Result) != 0 {
		t.Fatalf("expected empty matrix result for end<start, got %+v", emptyMatrix.Data.Result)
	}
}

func TestCompatHelpers_TimeParsingAndDurationFormatting(t *testing.T) {
	ref := time.Date(2026, time.April, 23, 15, 0, 0, 0, time.UTC)

	if got, ok := parseFlexibleUnixNanos(ref.Format(time.RFC3339Nano)); !ok || got != ref.UnixNano() {
		t.Fatalf("expected RFC3339 nanos parse, got %v,%v", got, ok)
	}
	if got, ok := parseFlexibleUnixNanos("1700000000"); !ok || got != 1700000000*int64(time.Second) {
		t.Fatalf("expected seconds integer to normalize to nanos, got %v,%v", got, ok)
	}
	if got, ok := parseFlexibleUnixNanos("1700000000000"); !ok || got != 1700000000000*int64(time.Millisecond) {
		t.Fatalf("expected millis integer to normalize to nanos, got %v,%v", got, ok)
	}
	if got, ok := parseFlexibleUnixNanos("1700000000.5"); !ok || got != 1700000000500000000 {
		t.Fatalf("expected float seconds to normalize to nanos, got %v,%v", got, ok)
	}
	if _, ok := parseFlexibleUnixNanos("not-a-time"); ok {
		t.Fatal("expected invalid nanos parse to fail")
	}

	if got, ok := parseFlexibleUnixSeconds(ref.Format(time.RFC3339)); !ok || got != ref.Unix() {
		t.Fatalf("expected RFC3339 seconds parse, got %v,%v", got, ok)
	}
	if got := normalizeUnixNanos(1700000000); got != 1700000000*int64(time.Second) {
		t.Fatalf("expected normalizeUnixNanos(seconds) to scale, got %v", got)
	}
	if got := normalizeUnixNanos(1700000000000); got != 1700000000000*int64(time.Millisecond) {
		t.Fatalf("expected normalizeUnixNanos(millis) to scale, got %v", got)
	}

	if got := formatLogQLDuration(0); got != "1s" {
		t.Fatalf("expected non-positive duration to clamp to 1s, got %q", got)
	}
	if got := formatLogQLDuration(2 * time.Hour); got != "2h" {
		t.Fatalf("expected hour formatting, got %q", got)
	}
	if got := formatLogQLDuration(3 * time.Minute); got != "3m" {
		t.Fatalf("expected minute formatting, got %q", got)
	}
	if got := formatLogQLDuration(4 * time.Second); got != "4s" {
		t.Fatalf("expected second formatting, got %q", got)
	}
	if got := formatLogQLDuration(1500 * time.Millisecond); got != "1500ms" {
		t.Fatalf("expected millisecond formatting, got %q", got)
	}

	if d, ok := parsePositiveStepDuration("30s"); !ok || d != 30*time.Second {
		t.Fatalf("expected duration step parse, got %v,%v", d, ok)
	}
	if d, ok := parsePositiveStepDuration("2.5"); !ok || d != 2500*time.Millisecond {
		t.Fatalf("expected float step parse, got %v,%v", d, ok)
	}
	if d, ok := parsePositiveStepDuration("1d"); !ok || d != 24*time.Hour {
		t.Fatalf("expected day step parse, got %v,%v", d, ok)
	}
	if d, ok := parsePositiveStepDuration("1w2d3h"); !ok || d != (9*24+3)*time.Hour {
		t.Fatalf("expected week/day/hour step parse, got %v,%v", d, ok)
	}
	if _, ok := parsePositiveStepDuration("-1"); ok {
		t.Fatal("expected negative step parse to fail")
	}
	if _, ok := parsePositiveStepDuration("not-a-step"); ok {
		t.Fatal("expected invalid step parse to fail")
	}

	if d, ok := resolveGrafanaTemplateTokenDuration("$__rate_interval", "1700000000", "1700000600", "30s"); !ok || d != time.Minute*2 {
		t.Fatalf("expected $__rate_interval to resolve to 2m, got %v,%v", d, ok)
	}
	if d, ok := resolveGrafanaTemplateTokenDuration("${__rate_interval_ms}", "1700000000", "1700000600", "30s"); !ok || d != time.Minute*2 {
		t.Fatalf("expected ${__rate_interval_ms} to resolve to 2m, got %v,%v", d, ok)
	}
	if d, ok := resolveGrafanaTemplateTokenDuration("$__range", "1700000000", "1700000600", "30s"); !ok || d != 10*time.Minute {
		t.Fatalf("expected $__range to resolve to 10m, got %v,%v", d, ok)
	}
	if d, ok := resolveGrafanaTemplateTokenDuration("$__interval", "1700000000", "1700000600", "30s"); !ok || d != 30*time.Second {
		t.Fatalf("expected $__interval to resolve to step, got %v,%v", d, ok)
	}
	if d, ok := resolveGrafanaTemplateTokenDuration("${__interval_ms}", "1700000000", "1700000600", "30s"); !ok || d != 30*time.Second {
		t.Fatalf("expected ${__interval_ms} to resolve to step, got %v,%v", d, ok)
	}
	if !isGrafanaRangeTemplateSelector("${__range_s}") {
		t.Fatal("expected braced $__range_s selector to be recognized")
	}
	if isGrafanaRangeTemplateSelector("${__unknown}") {
		t.Fatal("expected unknown braced selector to be rejected")
	}
	if _, ok := resolveGrafanaTemplateTokenDuration("$__unknown", "1700000000", "1700000600", "30s"); ok {
		t.Fatal("expected unknown grafana token to fail resolution")
	}
	if got := resolveGrafanaRangeTemplateTokens(`rate({app="api"}[${__interval}]) + count_over_time({app="api"}[$__auto])`, "1700000000", "1700000600", "30s"); got != `rate({app="api"}[30s]) + count_over_time({app="api"}[30s])` {
		t.Fatalf("expected Grafana token replacement, got %q", got)
	}

	if bucketRange, ok := parseRequestedBucketRange("1700000000", "1700000060", "30s"); !ok {
		t.Fatal("expected requested bucket range to parse")
	} else {
		if bucketRange.count != 3 {
			t.Fatalf("expected bucket count 3, got %d", bucketRange.count)
		}
		if bucket, ok := bucketRange.bucketFor(1700000045); !ok || bucket != 1700000030 {
			t.Fatalf("expected bucket alignment to 1700000030, got %d,%v", bucket, ok)
		}
		if _, ok := bucketRange.bucketFor(1699999999); ok {
			t.Fatal("expected out-of-range bucket lookup to fail")
		}
	}
}
