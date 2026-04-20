//go:build e2e

package e2e_compat

import (
	"encoding/json"
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

type stablePatternABConfig struct {
	rangeWindow time.Duration
	step        time.Duration
	batchSize   int
}

type stablePatternSummary struct {
	pattern        string
	sampleBuckets  int
	nonZeroBuckets int
	firstTs        int64
	lastTs         int64
	maxGapSeconds  int64
}

var stablePatternMessages = []string{
	`stable_pattern_alpha component=collector action=scrape outcome=ok`,
	`stable_pattern_bravo component=collector action=transform outcome=ok`,
	`stable_pattern_charlie component=collector action=export outcome=retry`,
	`stable_pattern_delta component=collector action=ship outcome=ok`,
}

var burstyPatternMessages = []string{
	`bursty_pattern_base component=api method=GET route=/orders status=200`,
	`bursty_pattern_auth component=api method=POST route=/login status=401`,
	`bursty_pattern_ship component=worker action=ship outcome=retry`,
}

func waitForPatternABReady(t *testing.T) {
	t.Helper()

	const readyTimeout = 60 * time.Second
	waitForReady(t, lokiURL+"/ready", readyTimeout)
	waitForReady(t, proxyURL+"/ready", readyTimeout)
	waitForReady(t, grafanaURL+"/api/health", readyTimeout)
}

func TestDrilldown_Patterns_NativeLokiAndProxyBurstyMixedPatternsStayAligned(t *testing.T) {
	waitForPatternABReady(t)

	cfg := stablePatternABConfig{
		rangeWindow: 2 * time.Hour,
		step:        30 * time.Second,
		batchSize:   1000,
	}
	serviceName := fmt.Sprintf("stable-pattern-bursty-%d", time.Now().UTC().UnixNano())
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(cfg.step)
	end := start.Add(cfg.rangeWindow)

	seedPatternMessagesToLokiAndVL(t, serviceName, start, end, cfg, func(bucket time.Time) []string {
		bucketIndex := int(bucket.Sub(start) / cfg.step)
		messages := []string{burstyPatternMessages[0]}
		if bucketIndex%3 == 0 {
			messages = append(messages, burstyPatternMessages[1])
		}
		if bucketIndex%7 == 0 {
			messages = append(messages, burstyPatternMessages[2])
		}
		return messages
	})

	query := fmt.Sprintf(`{service_name="%s"}`, serviceName)
	directUID := grafanaDatasourceUID(t, "Loki (direct)")
	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy patterns autodetect)")

	directEntries := waitForPatternsViaGrafanaDatasource(t, directUID, query, start, end, cfg.step, len(burstyPatternMessages))
	proxyEntries := waitForPatternsViaGrafanaDatasource(t, proxyUID, query, start, end, cfg.step, len(burstyPatternMessages))

	directSummary := summarizePatternEntriesByPattern(t, "direct", directEntries, len(burstyPatternMessages), int64(cfg.step/time.Second))
	proxySummary := summarizePatternEntriesByPattern(t, "proxy", proxyEntries, len(burstyPatternMessages), int64(cfg.step/time.Second))
	assertPatternSet(t, "direct", directSummary, burstyPatternMessages)
	assertPatternSet(t, "proxy", proxySummary, burstyPatternMessages)
	assertPatternSignalComparable(t, directSummary, proxySummary, int64(cfg.step/time.Second), 1)
}

func TestDrilldown_Patterns_NativeLokiAndProxyHighCardinalityVariablesStayAligned(t *testing.T) {
	waitForPatternABReady(t)

	cfg := stablePatternABConfig{
		rangeWindow: 90 * time.Minute,
		step:        30 * time.Second,
		batchSize:   1000,
	}
	serviceName := fmt.Sprintf("stable-pattern-hc-%d", time.Now().UTC().UnixNano())
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(cfg.step)
	end := start.Add(cfg.rangeWindow)

	seedPatternMessagesToLokiAndVL(t, serviceName, start, end, cfg, func(bucket time.Time) []string {
		seq := bucket.Unix()
		return []string{
			fmt.Sprintf("hc_uuid request trace_id=550e8400-e29b-41d4-a716-%012d user=user-%d", seq, seq%7),
			fmt.Sprintf("hc_path request path=/api/v1/orders/%d/items/%d status=500", seq%1000, (seq/3)%1000),
		}
	})

	query := fmt.Sprintf(`{service_name="%s"}`, serviceName)
	directUID := grafanaDatasourceUID(t, "Loki (direct)")
	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy patterns autodetect)")

	directEntries := waitForPatternsViaGrafanaDatasource(t, directUID, query, start, end, cfg.step, 2)
	proxyEntries := waitForPatternsViaGrafanaDatasource(t, proxyUID, query, start, end, cfg.step, 2)

	directSummary := summarizePatternEntriesByPattern(t, "direct", directEntries, 2, int64(cfg.step/time.Second))
	proxySummary := summarizePatternEntriesByPattern(t, "proxy", proxyEntries, 2, int64(cfg.step/time.Second))
	assertPatternSignalComparable(t, directSummary, proxySummary, int64(cfg.step/time.Second), 1)
}

func TestDrilldown_Patterns_NativeLokiAndProxyStayDenseOnSameStableData(t *testing.T) {
	waitForPatternABReady(t)

	cfg := stablePatternABConfig{
		rangeWindow: 2 * time.Hour,
		step:        30 * time.Second,
		batchSize:   1000,
	}
	serviceName := fmt.Sprintf("stable-pattern-ab-%d", time.Now().UTC().UnixNano())
	start := time.Now().UTC().Add(-cfg.rangeWindow).Truncate(cfg.step)
	end := start.Add(cfg.rangeWindow)
	t.Logf("stable pattern A/B fixture service=%s start=%s end=%s step=%s", serviceName, start.Format(time.RFC3339), end.Format(time.RFC3339), cfg.step)

	seedStablePatternsToLokiAndVL(t, serviceName, start, end, cfg)

	query := fmt.Sprintf(`{service_name="%s"}`, serviceName)
	directUID := grafanaDatasourceUID(t, "Loki (direct)")
	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy patterns autodetect)")

	directEntries := waitForPatternsViaGrafanaDatasource(t, directUID, query, start, end, cfg.step, len(stablePatternMessages))
	proxyEntries := waitForPatternsViaGrafanaDatasource(t, proxyUID, query, start, end, cfg.step, 1)

	_, _, expectedBuckets := denseExpectedBuckets(start, end, cfg.step)
	stepSeconds := int64(cfg.step / time.Second)

	directSummary := summarizeStablePatternEntries(t, "direct", directEntries, len(stablePatternMessages), expectedBuckets, stepSeconds)
	proxySummary := summarizeStablePatternEntries(t, "proxy", proxyEntries, len(stablePatternMessages), expectedBuckets, stepSeconds)

	if len(directSummary) != len(proxySummary) {
		t.Fatalf("expected same pattern count for direct and proxy, direct=%d proxy=%d direct=%v proxy=%v", len(directSummary), len(proxySummary), directSummary, proxySummary)
	}
	for _, message := range stablePatternMessages {
		directItem, ok := directSummary[message]
		if !ok {
			t.Fatalf("direct Loki missing stable pattern %q in %v", message, directSummary)
		}
		proxyItem, ok := proxySummary[message]
		if !ok {
			t.Fatalf("proxy missing stable pattern %q in %v", message, proxySummary)
		}
		if directItem.sampleBuckets != proxyItem.sampleBuckets || directItem.nonZeroBuckets != proxyItem.nonZeroBuckets || directItem.maxGapSeconds != proxyItem.maxGapSeconds {
			t.Fatalf(
				"stable pattern %q diverged between direct Loki and proxy: direct=%+v proxy=%+v",
				message,
				directItem,
				proxyItem,
			)
		}
	}
}

func seedStablePatternsToLokiAndVL(t *testing.T, serviceName string, start, end time.Time, cfg stablePatternABConfig) {
	t.Helper()
	seedPatternMessagesToLokiAndVL(t, serviceName, start, end, cfg, func(time.Time) []string {
		return stablePatternMessages
	})
}

func seedPatternMessagesToLokiAndVL(t *testing.T, serviceName string, start, end time.Time, cfg stablePatternABConfig, messagesForBucket func(time.Time) []string) {
	t.Helper()

	if cfg.step <= 0 {
		cfg.step = 30 * time.Second
	}
	if cfg.batchSize <= 0 {
		cfg.batchSize = 1000
	}

	streamLabels := map[string]string{
		"app":          serviceName,
		"service_name": serviceName,
		"cluster":      "stable-pattern-ab",
		"namespace":    "stable-pattern-ab",
		"level":        "info",
	}

	type lokiStream struct {
		Stream map[string]string `json:"stream"`
		Values [][]string        `json:"values"`
	}

	insertVLURL := vlURL + "/insert/jsonline?_stream_fields=" + url.QueryEscape("app,service_name,cluster,namespace,level")
	var lokiValues [][]string
	var vlBody strings.Builder
	linesInBatch := 0

	flush := func() {
		if len(lokiValues) == 0 {
			return
		}

		lokiPayload := map[string]interface{}{
			"streams": []lokiStream{{
				Stream: streamLabels,
				Values: lokiValues,
			}},
		}
		body, err := json.Marshal(lokiPayload)
		if err != nil {
			t.Fatalf("marshal Loki stable patterns payload failed: %v", err)
		}
		lokiResp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Fatalf("stable pattern Loki push failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, lokiResp.Body)
		lokiResp.Body.Close()
		if lokiResp.StatusCode/100 != 2 {
			t.Fatalf("stable pattern Loki push failed with status=%d", lokiResp.StatusCode)
		}

		vlResp, err := http.Post(insertVLURL, "application/stream+json", strings.NewReader(vlBody.String()))
		if err != nil {
			t.Fatalf("stable pattern VL push failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, vlResp.Body)
		vlResp.Body.Close()
		if vlResp.StatusCode/100 != 2 {
			t.Fatalf("stable pattern VL push failed with status=%d", vlResp.StatusCode)
		}

		lokiValues = lokiValues[:0]
		vlBody.Reset()
		linesInBatch = 0
	}

	for bucket := start; !bucket.After(end); bucket = bucket.Add(cfg.step) {
		for _, message := range messagesForBucket(bucket) {
			ts := bucket.UTC()
			lokiValues = append(lokiValues, []string{fmt.Sprintf("%d", ts.UnixNano()), message})
			vlBody.WriteString("{\"_time\":\"")
			vlBody.WriteString(ts.Format(time.RFC3339Nano))
			vlBody.WriteString("\",\"_msg\":")
			vlBody.WriteString(strconv.Quote(message))
			vlBody.WriteString(",\"app\":\"")
			vlBody.WriteString(serviceName)
			vlBody.WriteString("\",\"service_name\":\"")
			vlBody.WriteString(serviceName)
			vlBody.WriteString("\",\"cluster\":\"stable-pattern-ab\",\"namespace\":\"stable-pattern-ab\",\"level\":\"info\"}\n")
			linesInBatch++
			if linesInBatch >= cfg.batchSize {
				flush()
			}
		}
	}
	flush()
}

func waitForPatternsViaGrafanaDatasource(t *testing.T, dsUID, query string, start, end time.Time, step time.Duration, minPatterns int) []densePatternEntry {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	poll := 200 * time.Millisecond
	maxPoll := 2 * time.Second
	for {
		entries, err := fetchPatternsViaGrafanaDatasource(dsUID, query, start, end, step, max(minPatterns, 20))
		if err == nil && len(entries) >= minPatterns {
			return entries
		}
		now := time.Now()
		if now.After(deadline) {
			t.Fatalf("patterns did not stabilize for datasource=%s query=%s: err=%v entries=%d", dsUID, query, err, len(entries))
		}
		sleepFor := poll
		remaining := time.Until(deadline)
		if sleepFor > remaining {
			sleepFor = remaining
		}
		time.Sleep(sleepFor)
		if poll < maxPoll {
			poll *= 2
			if poll > maxPoll {
				poll = maxPoll
			}
		}
	}
}

func summarizeStablePatternEntries(t *testing.T, source string, entries []densePatternEntry, expectedPatterns, expectedBuckets int, stepSeconds int64) map[string]stablePatternSummary {
	t.Helper()

	if len(entries) < expectedPatterns {
		t.Fatalf("%s returned too few patterns: got=%d want_at_least=%d entries=%v", source, len(entries), expectedPatterns, entries)
	}

	summaries := make(map[string]stablePatternSummary, len(entries))
	for _, entry := range entries {
		sampleBuckets := len(entry.Samples)
		nonZeroBuckets := 0
		firstTs := int64(0)
		lastTs := int64(0)
		maxGapSeconds := int64(0)
		var prevTs int64
		for i, sample := range entry.Samples {
			if len(sample) < 2 {
				continue
			}
			ts, ok := denseSampleTs(sample[0])
			if !ok {
				t.Fatalf("%s pattern %q has non-numeric timestamp sample=%v", source, entry.Pattern, sample)
			}
			value, ok := denseSampleTs(sample[1])
			if !ok {
				t.Fatalf("%s pattern %q has non-numeric value sample=%v", source, entry.Pattern, sample)
			}
			if value > 0 {
				nonZeroBuckets++
			}
			if i == 0 {
				firstTs = ts
			}
			if prevTs != 0 && ts-prevTs > maxGapSeconds {
				maxGapSeconds = ts - prevTs
			}
			prevTs = ts
			lastTs = ts
		}
		summaries[entry.Pattern] = stablePatternSummary{
			pattern:        entry.Pattern,
			sampleBuckets:  sampleBuckets,
			nonZeroBuckets: nonZeroBuckets,
			firstTs:        firstTs,
			lastTs:         lastTs,
			maxGapSeconds:  maxGapSeconds,
		}
	}

	if len(summaries) != expectedPatterns {
		keys := make([]string, 0, len(summaries))
		for key := range summaries {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		t.Fatalf("%s returned unexpected stable pattern set: got=%d want=%d keys=%v", source, len(summaries), expectedPatterns, keys)
	}

	minCoverage := max(1, expectedBuckets-1)
	for _, message := range stablePatternMessages {
		summary, ok := summaries[message]
		if !ok {
			t.Fatalf("%s missing stable pattern %q in %v", source, message, summaries)
		}
		if summary.sampleBuckets < minCoverage {
			t.Fatalf("%s pattern %q lost bucket coverage: got=%d want_at_least=%d summary=%+v", source, message, summary.sampleBuckets, minCoverage, summary)
		}
		if summary.nonZeroBuckets < minCoverage {
			t.Fatalf("%s pattern %q lost non-zero coverage: got=%d want_at_least=%d summary=%+v", source, message, summary.nonZeroBuckets, minCoverage, summary)
		}
		if summary.maxGapSeconds > stepSeconds {
			t.Fatalf("%s pattern %q has bucket gap=%ds larger than step=%ds summary=%+v", source, message, summary.maxGapSeconds, stepSeconds, summary)
		}
	}

	return summaries
}
func summarizePatternEntriesByPattern(t *testing.T, source string, entries []densePatternEntry, expectedMinPatterns int, _ int64) map[string]stablePatternSummary {
	t.Helper()

	if len(entries) < expectedMinPatterns {
		t.Fatalf("%s returned too few patterns: got=%d want_at_least=%d entries=%v", source, len(entries), expectedMinPatterns, entries)
	}

	summaries := make(map[string]stablePatternSummary, len(entries))
	for _, entry := range entries {
		sampleBuckets := len(entry.Samples)
		nonZeroBuckets := 0
		firstTs := int64(0)
		lastTs := int64(0)
		maxGapSeconds := int64(0)
		var prevTs int64
		for i, sample := range entry.Samples {
			if len(sample) < 2 {
				continue
			}
			ts, ok := denseSampleTs(sample[0])
			if !ok {
				t.Fatalf("%s pattern %q has non-numeric timestamp sample=%v", source, entry.Pattern, sample)
			}
			value, ok := denseSampleTs(sample[1])
			if !ok {
				t.Fatalf("%s pattern %q has non-numeric value sample=%v", source, entry.Pattern, sample)
			}
			if value > 0 {
				nonZeroBuckets++
			}
			if i == 0 {
				firstTs = ts
			}
			if prevTs != 0 && ts-prevTs > maxGapSeconds {
				maxGapSeconds = ts - prevTs
			}
			prevTs = ts
			lastTs = ts
		}
		summaries[entry.Pattern] = stablePatternSummary{
			pattern:        entry.Pattern,
			sampleBuckets:  sampleBuckets,
			nonZeroBuckets: nonZeroBuckets,
			firstTs:        firstTs,
			lastTs:         lastTs,
			maxGapSeconds:  maxGapSeconds,
		}
	}
	return summaries
}

func assertPatternSummariesMatch(t *testing.T, directSummary, proxySummary map[string]stablePatternSummary) {
	t.Helper()

	if len(directSummary) != len(proxySummary) {
		t.Fatalf("expected same pattern count for direct and proxy, direct=%d proxy=%d direct=%v proxy=%v", len(directSummary), len(proxySummary), directSummary, proxySummary)
	}
	for pattern, directItem := range directSummary {
		proxyItem, ok := proxySummary[pattern]
		if !ok {
			t.Fatalf("proxy missing pattern %q in %v", pattern, proxySummary)
		}
		if directItem.sampleBuckets != proxyItem.sampleBuckets || directItem.nonZeroBuckets != proxyItem.nonZeroBuckets || directItem.maxGapSeconds != proxyItem.maxGapSeconds {
			t.Fatalf("pattern %q diverged between direct Loki and proxy: direct=%+v proxy=%+v", pattern, directItem, proxyItem)
		}
	}
}

func assertPatternSignalComparable(t *testing.T, directSummary, proxySummary map[string]stablePatternSummary, stepSeconds int64, maxCountDrift int) {
	t.Helper()

	if len(directSummary) != len(proxySummary) {
		t.Fatalf("expected same pattern count for direct and proxy, direct=%d proxy=%d direct=%v proxy=%v", len(directSummary), len(proxySummary), directSummary, proxySummary)
	}
	for pattern, directItem := range directSummary {
		proxyItem, ok := proxySummary[pattern]
		if !ok {
			t.Fatalf("proxy missing pattern %q in %v", pattern, proxySummary)
		}
		// Bursty grouped patterns are sparse by design, so compare preserved signal
		// instead of demanding near-identical bucket-for-bucket coverage.
		allowedCountDrift := max(maxCountDrift, directItem.nonZeroBuckets/3)
		minProxyCoverage := max(1, directItem.nonZeroBuckets-(allowedCountDrift))
		if proxyItem.nonZeroBuckets < minProxyCoverage {
			t.Fatalf(
				"pattern %q lost too much non-zero coverage: direct=%+v proxy=%+v min_proxy_non_zero=%d allowed_drift=%d",
				pattern,
				directItem,
				proxyItem,
				minProxyCoverage,
				allowedCountDrift,
			)
		}
		allowedTimeDrift := stepSeconds
		if directItem.maxGapSeconds*2 > allowedTimeDrift {
			allowedTimeDrift = directItem.maxGapSeconds * 2
		}
		if absInt64(directItem.firstTs-proxyItem.firstTs) > allowedTimeDrift || absInt64(directItem.lastTs-proxyItem.lastTs) > allowedTimeDrift {
			t.Fatalf(
				"pattern %q diverged in time bounds: direct=%+v proxy=%+v allowed_time_drift=%ds",
				pattern,
				directItem,
				proxyItem,
				allowedTimeDrift,
			)
		}
	}
}

func assertPatternSet(t *testing.T, source string, summaries map[string]stablePatternSummary, expected []string) {
	t.Helper()

	if len(summaries) != len(expected) {
		t.Fatalf("%s returned unexpected pattern count: got=%d want=%d summaries=%v", source, len(summaries), len(expected), summaries)
	}
	for _, pattern := range expected {
		if _, ok := summaries[pattern]; !ok {
			t.Fatalf("%s missing expected pattern %q in %v", source, pattern, summaries)
		}
	}
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
