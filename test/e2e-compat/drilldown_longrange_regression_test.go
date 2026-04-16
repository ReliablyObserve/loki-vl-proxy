//go:build e2e

package e2e_compat

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type longRangeDrilldownSeedConfig struct {
	rangeWindow             time.Duration
	step                    time.Duration
	patternCount            int
	repeatsPerPatternBucket int
	batchSize               int
}

const (
	longRangePatternsService = "drilldown-longrange-patterns"
	longRangeVolumeService   = "drilldown-longrange-volume"
)

func TestDrilldown_LongRangePatterns_MaintainPerPatternCoverage(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	waitForReady(t, grafanaURL+"/api/health", 30*time.Second)

	cfg := longRangeDrilldownSeedConfig{
		rangeWindow:             48 * time.Hour,
		step:                    5 * time.Minute,
		patternCount:            12,
		repeatsPerPatternBucket: 20,
		batchSize:               4000,
	}
	serviceName := longRangeServiceName(longRangePatternsService)
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(cfg.step)
	end := start.Add(cfg.rangeWindow)
	t.Logf("long-range patterns fixture service=%s start=%s end=%s", serviceName, start.Format(time.RFC3339), end.Format(time.RFC3339))
	seedLongRangeDrilldownServiceData(t, serviceName, start, end, cfg)
	waitForDrilldownServiceVisible(t, serviceName, start, end)

	dsUID := grafanaDatasourceUID(t, "Loki (via VL proxy patterns autodetect)")
	query := fmt.Sprintf(`{service_name="%s"}`, serviceName)

	var entries []densePatternEntry
	deadline := time.Now().Add(60 * time.Second)
	for {
		fetched, err := fetchPatternsViaGrafanaDatasource(dsUID, query, start, end, cfg.step, cfg.patternCount)
		if err == nil && len(fetched) > 0 {
			entries = fetched
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("patterns long-range repro did not stabilize in time; last_err=%v entries=%d", err, len(fetched))
		}
		time.Sleep(1500 * time.Millisecond)
	}

	if len(entries) == 0 {
		t.Fatalf("expected long-range patterns to be detected, got %d", len(entries))
	}

	expectedBuckets := int(end.Sub(start)/cfg.step) + 1
	minCoverageBuckets := (expectedBuckets * 4) / 5
	checkCount := min(4, len(entries))
	for i := 0; i < checkCount; i++ {
		nonzero := countNonZeroPatternBuckets(entries[i].Samples)
		if nonzero < minCoverageBuckets {
			t.Fatalf(
				"expected pattern %q to cover at least %d/%d buckets, got %d samples: %#v",
				entries[i].Pattern,
				minCoverageBuckets,
				expectedBuckets,
				nonzero,
				entries[i].Samples,
			)
		}
	}
}

func TestDrilldown_LongRangeVolume_SplitQueriesStayDense(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	waitForReady(t, grafanaURL+"/api/health", 30*time.Second)

	cfg := longRangeDrilldownSeedConfig{
		rangeWindow:             48 * time.Hour,
		step:                    5 * time.Minute,
		patternCount:            6,
		repeatsPerPatternBucket: 4,
		batchSize:               4000,
	}
	serviceName := longRangeServiceName(longRangeVolumeService)
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(cfg.step)
	end := start.Add(cfg.rangeWindow)
	t.Logf("long-range volume fixture service=%s start=%s end=%s", serviceName, start.Format(time.RFC3339), end.Format(time.RFC3339))
	seedLongRangeDrilldownServiceData(t, serviceName, start, end, cfg)
	waitForDrilldownServiceVisible(t, serviceName, start, end)

	expr := fmt.Sprintf(`sum by (detected_level) (count_over_time({service_name="%s"}[%ds]))`, serviceName, int(cfg.step/time.Second))
	result := queryRangeMatrixResult(t, expr, start, end, cfg.step)
	if len(result) < 2 {
		t.Fatalf("expected at least info/error series for long-range volume query, got %d: %v", len(result), result)
	}

	startBucket, endBucket, expectedBuckets := denseExpectedBuckets(start, end, cfg.step)
	minPoints := expectedBuckets - 2
	if minPoints < 1 {
		minPoints = 1
	}
	stepSeconds := int64(cfg.step / time.Second)
	seenLevels := map[string]bool{}

	for _, item := range result {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		level, _ := metric["detected_level"].(string)
		values, _ := series["values"].([]interface{})
		if level == "" {
			t.Fatalf("expected detected_level label on long-range volume series: %v", series)
		}
		if len(values) < minPoints {
			t.Fatalf("expected dense volume series for detected_level=%s, got %d/%d points", level, len(values), expectedBuckets)
		}

		var prevTS int64
		for i, raw := range values {
			pair, _ := raw.([]interface{})
			if len(pair) != 2 {
				t.Fatalf("expected [ts,value] pair for detected_level=%s, got %v", level, raw)
			}
			ts, ok := denseSampleTs(pair[0])
			if !ok {
				t.Fatalf("expected numeric timestamp for detected_level=%s, got %T", level, pair[0])
			}
			if i == 0 && ts > startBucket+(2*stepSeconds) {
				t.Fatalf("expected early coverage for detected_level=%s, first_ts=%d start_bucket=%d", level, ts, startBucket)
			}
			if i > 0 && ts-prevTS > (2*stepSeconds) {
				t.Fatalf("expected contiguous long-range volume buckets for detected_level=%s, prev=%d current=%d", level, prevTS, ts)
			}
			prevTS = ts
		}
		if prevTS < endBucket-(2*stepSeconds) {
			t.Fatalf("expected late coverage for detected_level=%s, last_ts=%d end_bucket=%d", level, prevTS, endBucket)
		}
		seenLevels[level] = true
	}

	for _, want := range []string{"info", "error"} {
		if !seenLevels[want] {
			t.Fatalf("expected long-range volume result to include detected_level=%s, got %v", want, seenLevels)
		}
	}
}

func TestDrilldown_LongRangeVolume_GrafanaStyleSplitQueriesStayDense(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	cfg := longRangeDrilldownSeedConfig{
		rangeWindow:             48 * time.Hour,
		step:                    5 * time.Minute,
		patternCount:            6,
		repeatsPerPatternBucket: 4,
		batchSize:               4000,
	}
	serviceName := longRangeServiceName(longRangeVolumeService)
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(cfg.step)
	end := start.Add(cfg.rangeWindow)
	seedLongRangeDrilldownServiceData(t, serviceName, start, end, cfg)
	waitForDrilldownServiceVisible(t, serviceName, start, end)

	queryStep := time.Hour
	expr := fmt.Sprintf(`sum by (detected_level) (count_over_time({service_name="%s"}[1h]))`, serviceName)

	type splitWindow struct {
		start time.Time
		end   time.Time
	}

	windows := make([]splitWindow, 0, 3)
	for chunkStart := start; !chunkStart.After(end); {
		chunkEnd := chunkStart.Add(23 * queryStep)
		if chunkEnd.After(end) {
			chunkEnd = end
		}
		windows = append(windows, splitWindow{start: chunkStart, end: chunkEnd})
		if !chunkEnd.Before(end) {
			break
		}
		chunkStart = chunkEnd.Add(queryStep)
	}
	if len(windows) < 2 {
		t.Fatalf("expected multi-window long-range split, got %#v", windows)
	}

	merged := map[string][]int64{}
	for _, window := range windows {
		windowResult := queryRangeMatrixResult(t, expr, window.start, window.end, queryStep)
		for _, item := range windowResult {
			series, _ := item.(map[string]interface{})
			metric, _ := series["metric"].(map[string]interface{})
			level, _ := metric["detected_level"].(string)
			values, _ := series["values"].([]interface{})
			for _, raw := range values {
				pair, _ := raw.([]interface{})
				if len(pair) != 2 {
					t.Fatalf("expected [ts,value] pair for split volume series, got %v", raw)
				}
				ts, ok := denseSampleTs(pair[0])
				if !ok {
					t.Fatalf("expected numeric timestamp for detected_level=%s, got %T", level, pair[0])
				}
				merged[level] = append(merged[level], ts)
			}
		}
	}

	startBucket, endBucket, expectedBuckets := denseExpectedBuckets(start, end, queryStep)
	minPoints := expectedBuckets - 1
	for _, level := range []string{"info", "error"} {
		series := merged[level]
		if len(series) < minPoints {
			t.Fatalf("expected dense merged split coverage for detected_level=%s, got %d/%d points", level, len(series), expectedBuckets)
		}
		series = uniqueSortedInt64s(series)
		if len(series) < minPoints {
			t.Fatalf("expected dense unique split coverage for detected_level=%s, got %d/%d points", level, len(series), expectedBuckets)
		}
		if series[0] > startBucket+(2*int64(queryStep/time.Second)) {
			t.Fatalf("expected early split coverage for detected_level=%s, first_ts=%d start_bucket=%d", level, series[0], startBucket)
		}
		for i := 1; i < len(series); i++ {
			if gap := series[i] - series[i-1]; gap > int64(queryStep/time.Second) {
				t.Fatalf("expected contiguous merged split buckets for detected_level=%s, prev=%d current=%d gap=%d", level, series[i-1], series[i], gap)
			}
		}
		if series[len(series)-1] < endBucket-(2*int64(queryStep/time.Second)) {
			t.Fatalf("expected late split coverage for detected_level=%s, last_ts=%d end_bucket=%d", level, series[len(series)-1], endBucket)
		}
	}
}

func seedLongRangeDrilldownServiceData(t *testing.T, serviceName string, start, end time.Time, cfg longRangeDrilldownSeedConfig) {
	t.Helper()

	if cfg.step <= 0 {
		cfg.step = 5 * time.Minute
	}
	if cfg.patternCount <= 0 {
		cfg.patternCount = 4
	}
	if cfg.repeatsPerPatternBucket <= 0 {
		cfg.repeatsPerPatternBucket = 1
	}
	if cfg.batchSize <= 0 {
		cfg.batchSize = 2000
	}

	insertURL := vlURL + "/insert/jsonline?_stream_fields=" + url.QueryEscape("app,service_name,cluster,namespace,level,detected_level")
	linesInBatch := 0
	var body strings.Builder
	body.Grow(cfg.batchSize * 256)
	flush := func() {
		if linesInBatch == 0 {
			return
		}
		resp, err := http.Post(insertURL, "application/stream+json", strings.NewReader(body.String()))
		if err != nil {
			t.Fatalf("long-range VL push failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("long-range VL push failed with status=%d", resp.StatusCode)
		}
		body.Reset()
		linesInBatch = 0
	}

	bucketIndex := 0
	for bucket := start; !bucket.After(end); bucket = bucket.Add(cfg.step) {
		for patternID := 0; patternID < cfg.patternCount; patternID++ {
			level := "info"
			if patternID%3 == 0 {
				level = "error"
			}
			method := []string{"GET", "POST", "PUT", "DELETE"}[patternID%4]
			status := []int{200, 201, 204, 400, 404, 500}[patternID%6]
			path := fmt.Sprintf("/api/v1/route/%02d", patternID)
			for repeat := 0; repeat < cfg.repeatsPerPatternBucket; repeat++ {
				ts := bucket.Add(time.Duration((patternID*cfg.repeatsPerPatternBucket)+repeat) * time.Second).UTC().Format(time.RFC3339Nano)
				msg := fmt.Sprintf(
					"pattern_%02d method=%s route=%s status=%d latency_ms=%d request_id=req-%03d-%06d-%02d user_id=user-%02d",
					patternID,
					method,
					path,
					status,
					((bucketIndex+repeat)%900)+10,
					patternID,
					bucketIndex,
					repeat,
					patternID%17,
				)
				body.WriteString("{\"_time\":\"")
				body.WriteString(ts)
				body.WriteString("\",\"_msg\":")
				body.WriteString(strconv.Quote(msg))
				body.WriteString(",\"app\":\"")
				body.WriteString(serviceName)
				body.WriteString("\",\"service_name\":\"")
				body.WriteString(serviceName)
				body.WriteString("\",\"cluster\":\"drilldown-longrange\",\"namespace\":\"drilldown-longrange\",\"level\":\"")
				body.WriteString(level)
				body.WriteString("\",\"detected_level\":\"")
				body.WriteString(level)
				body.WriteString("\"}\n")
				linesInBatch++
				if linesInBatch >= cfg.batchSize {
					flush()
				}
			}
		}
		bucketIndex++
	}
	flush()
	time.Sleep(5 * time.Second)
}

func longRangeServiceName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func waitForDrilldownServiceVisible(t *testing.T, serviceName string, start, end time.Time) {
	t.Helper()

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{service_name="%s"}`, serviceName))
	params.Set("start", start.Format(time.RFC3339Nano))
	params.Set("end", end.Format(time.RFC3339Nano))

	deadline := time.Now().Add(45 * time.Second)
	var last map[string]interface{}
	for {
		last = getJSON(t, proxyURL+"/loki/api/v1/index/stats?"+params.Encode())
		if entries, ok := last["entries"].(float64); ok && entries > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seeded service %q never became visible in index/stats: %v", serviceName, last)
		}
		time.Sleep(1500 * time.Millisecond)
	}
}

func countNonZeroPatternBuckets(samples [][]interface{}) int {
	count := 0
	for _, sample := range samples {
		if len(sample) < 2 {
			continue
		}
		v, ok := sample[1].(float64)
		if ok && v > 0 {
			count++
			continue
		}
		switch raw := sample[1].(type) {
		case int:
			if raw > 0 {
				count++
			}
		case int64:
			if raw > 0 {
				count++
			}
		case string:
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				count++
			}
		}
	}
	return count
}

func queryRangeMatrixResult(t *testing.T, expr string, start, end time.Time, step time.Duration) []interface{} {
	t.Helper()

	params := url.Values{}
	params.Set("query", expr)
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	params.Set("step", strconv.Itoa(int(step/time.Second)))

	resp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	data := extractMap(resp, "data")
	if data == nil {
		t.Fatalf("expected query_range data for %q, got %v", expr, resp)
	}
	result := extractArray(data, "result")
	if result == nil {
		t.Fatalf("expected query_range matrix result for %q, got %v", expr, resp)
	}
	return result
}

func uniqueSortedInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	out := sorted[:1]
	for _, value := range sorted[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
