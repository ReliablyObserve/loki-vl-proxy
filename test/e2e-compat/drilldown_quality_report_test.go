//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

type drilldownQualityField struct {
	lokiName       string
	filter         string
	isStreamLabel  bool
	mustBeNonEmpty bool
}

type drilldownQualityRange struct {
	label string
	dur   time.Duration
	step  time.Duration
}

type drilldownQualityResult struct {
	field          string
	rangeLabel     string
	seriesCount    int
	nonZeroBuckets int
	totalBuckets   int
	totalCount     int64
	latencyMs      int64
	pathLabel      string
	errMsg         string
}

func TestDrilldown_QualityMatrix(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	svc := fmt.Sprintf("drilldown-quality-%d", time.Now().UnixNano()/1e9)

	seedDrilldownQualityData(t, svc)
	// Give VL time to index the seeded data.
	time.Sleep(3 * time.Second)

	fields := []drilldownQualityField{
		{lokiName: "level", filter: `| level != ""`, isStreamLabel: true, mustBeNonEmpty: true},
		{lokiName: "http_method", filter: `|json|drop __error__,__error_details__| http_method!=""`, mustBeNonEmpty: false},
		{lokiName: "http_status", filter: `|json|drop __error__,__error_details__| http_status!=""`, mustBeNonEmpty: false},
		{lokiName: "duration_ms", filter: `|json|drop __error__,__error_details__| duration_ms!=""`, mustBeNonEmpty: false},
		{lokiName: "trace_id", filter: `|json|drop __error__,__error_details__| trace_id!=""`, mustBeNonEmpty: false},
	}

	ranges := []drilldownQualityRange{
		{"1h", 1 * time.Hour, 60 * time.Second},
		{"3h", 3 * time.Hour, 3 * time.Minute},
		{"6h", 6 * time.Hour, 6 * time.Minute},
		{"12h", 12 * time.Hour, 12 * time.Minute},
		{"24h", 24 * time.Hour, 48 * time.Minute},
		{"2d", 48 * time.Hour, 96 * time.Minute},
		{"7d", 7 * 24 * time.Hour, 336 * time.Minute},
	}

	now := time.Now()
	var results []drilldownQualityResult

	for _, f := range fields {
		for _, r := range ranges {
			start := now.Add(-r.dur)
			res := measureDrilldownQuality(t, svc, f, r, start, now)
			results = append(results, res)

			if res.errMsg != "" {
				t.Errorf("HARD FAIL field=%s range=%s: %s", f.lokiName, r.label, res.errMsg)
				continue
			}

			if f.mustBeNonEmpty && res.seriesCount == 0 {
				t.Errorf("HARD FAIL field=%s range=%s returned 0 series", f.lokiName, r.label)
			}
		}
	}

	t.Logf("\n=== DRILLDOWN QUALITY MATRIX ===")
	t.Logf("%-14s %-14s %8s %12s %13s %10s %10s %s",
		"field", "range", "series", "nonZeroBkts", "totalBuckets", "totalCount", "latencyMs", "path")
	for _, res := range results {
		if res.errMsg != "" {
			t.Logf("%-14s %-14s ERROR: %s", res.field, res.rangeLabel, res.errMsg)
			continue
		}
		pct := 0.0
		if res.totalBuckets > 0 {
			pct = float64(res.nonZeroBuckets) / float64(res.totalBuckets) * 100
		}
		t.Logf("%-14s %-14s %8d %11d/%d (%.0f%%) %10d %9dms %s",
			res.field, res.rangeLabel,
			res.seriesCount,
			res.nonZeroBuckets, res.totalBuckets, pct,
			res.totalCount,
			res.latencyMs,
			res.pathLabel)
	}
}

func seedDrilldownQualityData(t *testing.T, svc string) {
	t.Helper()

	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	statuses := []int{200, 201, 204, 400, 401, 403, 404, 500, 502, 503}
	levels := []string{"info", "warn", "error"}

	insertURL := vlURL + "/insert/jsonline?_stream_fields=" +
		url.QueryEscape("service_name,app,level")

	var buf strings.Builder
	buf.Grow(1200 * 200)
	rng := rand.New(rand.NewSource(42))
	now := time.Now()
	for i := 0; i < 1200; i++ {
		ts := now.Add(-2*time.Hour + time.Duration(i)*6*time.Second)
		level := levels[i%len(levels)]
		method := methods[i%len(methods)]
		status := statuses[i%len(statuses)]
		durationMs := rng.Intn(4999) + 1
		traceID := fmt.Sprintf("t-%016x-%016x", rng.Int63(), rng.Int63())
		entry, _ := json.Marshal(map[string]interface{}{
			"_time":        ts.Format(time.RFC3339Nano),
			"_msg":         fmt.Sprintf("%s /api/resource status=%d duration=%dms", method, status, durationMs),
			"service_name": svc,
			"app":          svc,
			"level":        level,
			"http_method":  method,
			"http_status":  strconv.Itoa(status),
			"duration_ms":  strconv.Itoa(durationMs),
			"trace_id":     traceID,
		})
		buf.Write(entry)
		buf.WriteByte('\n')
	}

	resp, err := http.Post(insertURL, "application/stream+json", strings.NewReader(buf.String()))
	if err != nil {
		t.Fatalf("VL seed push failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("VL seed push returned status=%d", resp.StatusCode)
	}
	// VL keeps recent ingest in in-memory buffers; without /internal/force_flush
	// the subsequent stats_query_range against `level` returns 0 series until
	// the next periodic flush (5 s default; 1 s in our compose). On hosted GHA
	// runners this race causes `HARD FAIL field=level range=*` even though
	// the data is committed — verified locally: same query returns 5 levels
	// after flush, 0 series without it. force_flush is the documented
	// test-harness hook for this exact case.
	// https://docs.victoriametrics.com/victorialogs/#forced-flush
	forceVLFlush(t)
	t.Logf("seeded %s with 1200 entries over 2h", svc)
}

func measureDrilldownQuality(t *testing.T, svc string, f drilldownQualityField, r drilldownQualityRange, start, end time.Time) drilldownQualityResult {
	t.Helper()

	res := drilldownQualityResult{field: f.lokiName, rangeLabel: r.label}

	startStr := strconv.FormatInt(start.UnixNano(), 10)
	endStr := strconv.FormatInt(end.UnixNano(), 10)
	stepStr := strconv.FormatInt(int64(r.step/time.Second), 10)

	// LogQL requires the range bracket to be a single duration unit ([60s] or
	// [1m]). time.Duration.String() emits compound forms with trailing zero
	// components (60s → "1m0s", 5760s → "96m0s") which the parser rejects:
	//   parse error : logql: expected ], got DURATION ("0s")
	// Format the range as integer seconds — same representation as the step.
	rangeStr := stepStr + "s"
	query := fmt.Sprintf(`sum by (%s) (count_over_time({service_name=%q}%s [%s]))`,
		f.lokiName, svc, f.filter, rangeStr)

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", startStr)
	params.Set("end", endStr)
	params.Set("step", stepStr)

	req, _ := http.NewRequest(http.MethodGet,
		proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	req.Header.Set("X-Scope-OrgID", "fake")

	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	res.latencyMs = time.Since(t0).Milliseconds()

	if err != nil {
		res.errMsg = fmt.Sprintf("HTTP error: %v", err)
		return res
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		body, _ := io.ReadAll(resp.Body)
		res.errMsg = fmt.Sprintf("proxy 5xx status=%d body=%s", resp.StatusCode, body)
		return res
	}

	res.pathLabel = resp.Header.Get("X-Proxy-Drilldown-Path")

	var payload struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &payload); err != nil {
		res.errMsg = fmt.Sprintf("JSON decode: %v body=%s", err, body[:min(200, len(body))])
		return res
	}

	res.seriesCount = len(payload.Data.Result)
	for _, s := range payload.Data.Result {
		for _, pair := range s.Values {
			res.totalBuckets++
			if len(pair) < 2 {
				continue
			}
			var cnt int64
			switch v := pair[1].(type) {
			case string:
				cnt, _ = strconv.ParseInt(v, 10, 64)
			case float64:
				cnt = int64(v)
			}
			if cnt > 0 {
				res.nonZeroBuckets++
			}
			res.totalCount += cnt
		}
	}
	return res
}
