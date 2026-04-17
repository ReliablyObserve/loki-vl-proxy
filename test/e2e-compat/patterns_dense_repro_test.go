//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type densePatternsReproConfig struct {
	totalLines   int
	patternCount int
	batchSize    int
	rangeWindow  time.Duration
	step         time.Duration
	limit        int
	refreshLoops int
}

func TestPatternsDenseRepro_FullRangeAndRefreshStability(t *testing.T) {
	if strings.TrimSpace(os.Getenv("LVP_DENSE_PATTERNS_REPRO")) != "1" {
		t.Skip("set LVP_DENSE_PATTERNS_REPRO=1 to run dense patterns reproducibility harness")
	}

	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	waitForReady(t, grafanaURL+"/api/health", 30*time.Second)

	cfg := densePatternsReproConfig{
		totalLines:   denseEnvInt("DENSE_PATTERNS_TOTAL_LINES", 80000),
		patternCount: denseEnvInt("DENSE_PATTERNS_PATTERN_COUNT", 49),
		batchSize:    denseEnvInt("DENSE_PATTERNS_BATCH_SIZE", 4000),
		rangeWindow:  time.Duration(denseEnvInt("DENSE_PATTERNS_RANGE_MINUTES", 90)) * time.Minute,
		step:         time.Duration(denseEnvInt("DENSE_PATTERNS_STEP_SECONDS", 60)) * time.Second,
		limit:        denseEnvInt("DENSE_PATTERNS_LIMIT", 50),
		refreshLoops: denseEnvInt("DENSE_PATTERNS_REFRESH_LOOPS", 6),
	}
	if cfg.patternCount < 2 {
		cfg.patternCount = 2
	}
	if cfg.totalLines < cfg.patternCount*100 {
		cfg.totalLines = cfg.patternCount * 100
	}
	if cfg.batchSize <= 0 {
		cfg.batchSize = 1000
	}
	if cfg.limit <= 0 {
		cfg.limit = 50
	}
	if cfg.refreshLoops < 2 {
		cfg.refreshLoops = 2
	}
	if cfg.step < time.Second {
		cfg.step = 60 * time.Second
	}
	if cfg.rangeWindow < 5*time.Minute {
		cfg.rangeWindow = 30 * time.Minute
	}

	appName := fmt.Sprintf("pattern-dense-repro-%d", time.Now().UnixNano())
	seedStart := time.Now().UTC().Add(-cfg.rangeWindow)
	seedEnd := seedStart.Add(cfg.rangeWindow)
	t.Logf(
		"dense repro seed start app=%s total_lines=%d patterns=%d range=%s step=%s batch=%d loops=%d",
		appName, cfg.totalLines, cfg.patternCount, cfg.rangeWindow, cfg.step, cfg.batchSize, cfg.refreshLoops,
	)
	pushDensePatternData(t, appName, seedStart, seedEnd, cfg)

	dsUID := grafanaDatasourceUID(t, "Loki (via VL proxy patterns autodetect)")
	query := fmt.Sprintf(`{cluster="dense-cluster", namespace="dense-ns", app="%s"}`, appName)
	expectedStartBucket, expectedEndBucket, expectedBucketCount := denseExpectedBuckets(seedStart, seedEnd, cfg.step)

	var baseline densePatternCoverage
	var baselineCount int
	deadline := time.Now().Add(45 * time.Second)
	for {
		entries, err := fetchPatternsViaGrafanaDatasource(dsUID, query, seedStart, seedEnd, cfg.step, cfg.limit)
		if err == nil && len(entries) > 0 {
			coverage, ok := summarizeDensePatternCoverage(entries)
			if ok {
				baseline = coverage
				baselineCount = len(entries)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("dense repro query did not yield pattern coverage in time")
		}
		time.Sleep(1500 * time.Millisecond)
	}

	if baseline.sampleBuckets < expectedBucketCount-1 {
		t.Fatalf("expected dense patterns to cover nearly full bucket count=%d, got=%d", expectedBucketCount, baseline.sampleBuckets)
	}
	if baseline.minTs > expectedStartBucket+int64(cfg.step/time.Second) {
		t.Fatalf("expected min sample timestamp near range start bucket=%d, got=%d", expectedStartBucket, baseline.minTs)
	}
	if baseline.maxTs < expectedEndBucket-int64(cfg.step/time.Second) {
		t.Fatalf("expected max sample timestamp near range end bucket=%d, got=%d", expectedEndBucket, baseline.maxTs)
	}

	minAcceptedCount := max(1, baselineCount/2)
	stepSeconds := int64(cfg.step / time.Second)
	for i := 0; i < cfg.refreshLoops; i++ {
		entries, err := fetchPatternsViaGrafanaDatasource(dsUID, query, seedStart, seedEnd, cfg.step, cfg.limit)
		if err != nil {
			t.Fatalf("refresh %d patterns query failed: %v", i+1, err)
		}
		if len(entries) < minAcceptedCount {
			t.Fatalf("refresh %d returned too few patterns: got=%d baseline=%d", i+1, len(entries), baselineCount)
		}
		coverage, ok := summarizeDensePatternCoverage(entries)
		if !ok {
			t.Fatalf("refresh %d returned no usable samples", i+1)
		}
		if coverage.maxTs < expectedEndBucket-stepSeconds {
			t.Fatalf("refresh %d collapsed to short recent tail: max_ts=%d expected_end_bucket=%d", i+1, coverage.maxTs, expectedEndBucket)
		}
		if coverage.minTs > expectedStartBucket+stepSeconds {
			t.Fatalf("refresh %d lost range start coverage: min_ts=%d expected_start_bucket=%d", i+1, coverage.minTs, expectedStartBucket)
		}
	}
}

type densePatternEntry struct {
	Pattern string          `json:"pattern"`
	Samples [][]interface{} `json:"samples"`
}

type densePatternCoverage struct {
	minTs         int64
	maxTs         int64
	sampleBuckets int
}

func summarizeDensePatternCoverage(entries []densePatternEntry) (densePatternCoverage, bool) {
	minTs := int64(math.MaxInt64)
	maxTs := int64(math.MinInt64)
	buckets := map[int64]struct{}{}
	for _, entry := range entries {
		for _, sample := range entry.Samples {
			if len(sample) < 2 {
				continue
			}
			ts, ok := denseSampleTs(sample[0])
			if !ok {
				continue
			}
			buckets[ts] = struct{}{}
			if ts < minTs {
				minTs = ts
			}
			if ts > maxTs {
				maxTs = ts
			}
		}
	}
	if len(buckets) == 0 || minTs == int64(math.MaxInt64) || maxTs == int64(math.MinInt64) {
		return densePatternCoverage{}, false
	}
	return densePatternCoverage{
		minTs:         minTs,
		maxTs:         maxTs,
		sampleBuckets: len(buckets),
	}, true
}

func denseExpectedBuckets(start, end time.Time, step time.Duration) (int64, int64, int) {
	stepSeconds := int64(step / time.Second)
	if stepSeconds <= 0 {
		stepSeconds = 60
	}
	startBucket := (start.Unix() / stepSeconds) * stepSeconds
	endBucket := (end.Unix() / stepSeconds) * stepSeconds
	if endBucket < startBucket {
		endBucket = startBucket
	}
	count := int((endBucket-startBucket)/stepSeconds) + 1
	return startBucket, endBucket, count
}

func TestDenseLinearTimestamp_MonotonicAndBounded(t *testing.T) {
	startNs := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC).UnixNano()
	windowNs := int64((7 * 24 * time.Hour).Nanoseconds())
	denom := int64(100000 - 1)

	prev := denseLinearTimestamp(startNs, windowNs, 0, denom)
	if prev != startNs {
		t.Fatalf("expected first timestamp=%d, got %d", startNs, prev)
	}
	for i := int64(1); i <= denom; i += 257 {
		ts := denseLinearTimestamp(startNs, windowNs, i, denom)
		if ts < prev {
			t.Fatalf("expected monotonic timestamps, prev=%d current=%d i=%d", prev, ts, i)
		}
		if ts > startNs+windowNs {
			t.Fatalf("expected timestamp <= end bound, got=%d end=%d", ts, startNs+windowNs)
		}
		prev = ts
	}
	if got := denseLinearTimestamp(startNs, windowNs, denom, denom); got != startNs+windowNs {
		t.Fatalf("expected last timestamp=%d, got=%d", startNs+windowNs, got)
	}
}

func fetchPatternsViaGrafanaDatasource(dsUID, query string, start, end time.Time, step time.Duration, limit int) ([]densePatternEntry, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", start.Format(time.RFC3339Nano))
	params.Set("end", end.Format(time.RFC3339Nano))
	params.Set("step", fmt.Sprintf("%ds", int(step/time.Second)))
	params.Set("limit", strconv.Itoa(limit))

	reqURL := grafanaURL + "/api/datasources/uid/" + url.PathEscape(dsUID) + "/resources/patterns?" + params.Encode()
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload struct {
		Status string              `json:"status"`
		Data   []densePatternEntry `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	if payload.Status != "success" {
		return nil, fmt.Errorf("unexpected status=%q", payload.Status)
	}
	return payload.Data, nil
}

func pushDensePatternData(t *testing.T, appName string, start, end time.Time, cfg densePatternsReproConfig) {
	t.Helper()

	streamFields := url.QueryEscape("app,service_name,cluster,namespace,level")
	insertURL := vlURL + "/insert/jsonline?_stream_fields=" + streamFields
	windowNs := end.UnixNano() - start.UnixNano()
	if windowNs <= 0 {
		windowNs = int64(cfg.rangeWindow)
	}
	if windowNs <= 0 {
		windowNs = int64(30 * time.Minute)
	}

	for offset := 0; offset < cfg.totalLines; offset += cfg.batchSize {
		batchEnd := min(cfg.totalLines, offset+cfg.batchSize)
		var b strings.Builder
		b.Grow((batchEnd - offset) * 256)
		for i := offset; i < batchEnd; i++ {
			relative := int64(i)
			denom := int64(max(1, cfg.totalLines-1))
			tsNs := denseLinearTimestamp(start.UnixNano(), windowNs, relative, denom)
			ts := time.Unix(0, tsNs).UTC().Format(time.RFC3339Nano)
			patternID := i % cfg.patternCount
			method := []string{"GET", "POST", "PUT", "DELETE"}[patternID%4]
			status := []int{200, 201, 204, 400, 403, 404, 500, 502}[patternID%8]
			service := fmt.Sprintf("dense-svc-%02d", patternID%25)
			msg := fmt.Sprintf(
				"dense_pattern_%02d method=%s route=/api/p/%02d status=%d latency_ms=%d request_id=req-%08d worker=w%02d",
				patternID,
				method,
				patternID,
				status,
				(i%900)+10,
				i,
				patternID%17,
			)
			b.WriteString("{\"_time\":\"")
			b.WriteString(ts)
			b.WriteString("\",\"_msg\":")
			b.WriteString(strconv.Quote(msg))
			b.WriteString(",\"app\":\"")
			b.WriteString(appName)
			b.WriteString("\",\"service_name\":\"")
			b.WriteString(service)
			b.WriteString("\",\"cluster\":\"dense-cluster\",\"namespace\":\"dense-ns\",\"level\":\"info\"}\n")
		}

		resp, err := http.Post(insertURL, "application/stream+json", strings.NewReader(b.String()))
		if err != nil {
			t.Fatalf("dense VL push failed at offset=%d: %v", offset, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("dense VL push failed at offset=%d status=%d", offset, resp.StatusCode)
		}
	}
}

// denseLinearTimestamp maps [0..denom] to [startNs..startNs+windowNs] while
// avoiding int64 overflow for very large windows and line counts.
func denseLinearTimestamp(startNs, windowNs, relative, denom int64) int64 {
	if denom <= 0 || windowNs <= 0 {
		return startNs
	}
	if relative <= 0 {
		return startNs
	}
	if relative >= denom {
		return startNs + windowNs
	}
	quotient := windowNs / denom
	remainder := windowNs % denom
	return startNs + relative*quotient + (relative*remainder)/denom
}

func denseEnvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func denseSampleTs(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed, true
		}
		if parsedFloat, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(parsedFloat), true
		}
	}
	return 0, false
}
