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

type lokiCompareField struct {
	lokiName      string
	filterClause  string
	compareStrict bool
}

type lokiCompareResult struct {
	field          string
	rangeLabel     string
	lokiSeries     int
	proxySeries    int
	lokiTotal      int64
	proxyTotal     int64
	lokiNonZero    int
	proxyNonZero   int
	lokiLatencyMs  int64
	proxyLatencyMs int64
}

type matrixSummary struct {
	series  int
	total   int64
	nonZero int
}

// TestDrilldown_LokiCompare_FieldQuality seeds the same logs into both Loki and VL,
// then compares the proxy response against the Loki response for field quality.
//
// Acceptance thresholds (per field, per range):
//   - proxy series count >= loki series count
//   - |proxy_total - loki_total| / loki_total <= 0.15  (+-15%)
//   - proxy non-zero bucket count >= loki non-zero count * 0.90  (>=90% coverage)
func TestDrilldown_LokiCompare_FieldQuality(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	waitForReady(t, lokiURL+"/ready", 30*time.Second)

	svc := fmt.Sprintf("drilldown-compare-%d", time.Now().UnixNano()/1e9)

	seedDrilldownCompareData(t, svc)
	// Give both backends time to index the seeded data.
	time.Sleep(3 * time.Second)

	fields := []lokiCompareField{
		{lokiName: "level", filterClause: `| level != ""`, compareStrict: true},
		{lokiName: "http_method", filterClause: `|json|drop __error__,__error_details__| http_method!=""`, compareStrict: true},
		{lokiName: "http_status", filterClause: `|json|drop __error__,__error_details__| http_status!=""`, compareStrict: true},
		{lokiName: "duration_ms", filterClause: `|json|drop __error__,__error_details__| duration_ms!=""`, compareStrict: false},
		{lokiName: "trace_id", filterClause: `|json|drop __error__,__error_details__| trace_id!=""`, compareStrict: false},
	}

	testRanges := []drilldownQualityRange{
		{"1h", 1 * time.Hour, 60 * time.Second},
		{"3h", 3 * time.Hour, 3 * time.Minute},
		{"6h", 6 * time.Hour, 6 * time.Minute},
	}

	now := time.Now()
	var results []lokiCompareResult

	for _, f := range fields {
		for _, r := range testRanges {
			start := now.Add(-r.dur)
			res := runLokiCompare(t, svc, f, r, start, now)
			results = append(results, res)

			if !f.compareStrict {
				continue
			}

			if res.proxySeries < res.lokiSeries {
				t.Errorf("field=%s range=%s: proxy returned fewer series (%d) than Loki (%d)",
					f.lokiName, r.label, res.proxySeries, res.lokiSeries)
			}

			if res.lokiTotal > 0 {
				diff := res.proxyTotal - res.lokiTotal
				if diff < 0 {
					diff = -diff
				}
				pct := float64(diff) / float64(res.lokiTotal)
				if pct > 0.15 {
					t.Errorf("field=%s range=%s: count gap %.1f%% exceeds 15%% threshold (loki=%d proxy=%d)",
						f.lokiName, r.label, pct*100, res.lokiTotal, res.proxyTotal)
				}
			}

			if res.lokiNonZero > 0 {
				coverage := float64(res.proxyNonZero) / float64(res.lokiNonZero)
				if coverage < 0.90 {
					t.Errorf("field=%s range=%s: proxy non-zero bucket coverage %.0f%% < 90%% (loki=%d proxy=%d)",
						f.lokiName, r.label, coverage*100, res.lokiNonZero, res.proxyNonZero)
				}
			}
		}
	}

	t.Logf("\n=== LOKI vs PROXY COMPARISON ===")
	t.Logf("%-14s %-6s %10s %12s %12s %14s %14s",
		"field", "range", "series(L/P)", "total(L/P)", "nzBkts(L/P)", "lokiLat", "proxyLat")
	for _, res := range results {
		t.Logf("%-14s %-6s %5d/%-5d %6d/%-6d %6d/%-6d %8dms %8dms",
			res.field, res.rangeLabel,
			res.lokiSeries, res.proxySeries,
			res.lokiTotal, res.proxyTotal,
			res.lokiNonZero, res.proxyNonZero,
			res.lokiLatencyMs, res.proxyLatencyMs)
	}
}

func seedDrilldownCompareData(t *testing.T, svc string) {
	t.Helper()

	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	statuses := []int{200, 201, 204, 400, 404, 500}
	levels := []string{"info", "warn", "error"}
	rng := rand.New(rand.NewSource(99))

	now := time.Now()

	type entry struct {
		ts       time.Time
		level    string
		method   string
		status   int
		duration int
		traceID  string
		msg      string
	}
	var entries []entry
	for i := 0; i < 540; i++ {
		ts := now.Add(-90*time.Minute + time.Duration(i)*10*time.Second)
		lv := levels[i%len(levels)]
		m := methods[i%len(methods)]
		s := statuses[i%len(statuses)]
		d := rng.Intn(4999) + 1
		tid := fmt.Sprintf("t-%016x-%016x", rng.Int63(), rng.Int63())
		msg := fmt.Sprintf("%s /api/v1/resource status=%d duration=%dms trace=%s", m, s, d, tid)
		entries = append(entries, entry{ts: ts, level: lv, method: m, status: s, duration: d, traceID: tid, msg: msg})
	}

	// Push to Loki.
	lokiStreams := make(map[string][][2]string)
	for _, e := range entries {
		line, _ := json.Marshal(map[string]interface{}{
			"http_method": e.method,
			"http_status": strconv.Itoa(e.status),
			"duration_ms": strconv.Itoa(e.duration),
			"trace_id":    e.traceID,
			"msg":         e.msg,
		})
		lokiStreams[e.level] = append(lokiStreams[e.level],
			[2]string{strconv.FormatInt(e.ts.UnixNano(), 10), string(line)})
	}
	var lokiStreamList []interface{}
	for lv, vals := range lokiStreams {
		values := make([][]string, len(vals))
		for i, v := range vals {
			values[i] = []string{v[0], v[1]}
		}
		lokiStreamList = append(lokiStreamList, map[string]interface{}{
			"stream": map[string]string{
				"service_name": svc, "app": svc, "level": lv,
			},
			"values": values,
		})
	}
	lokiPayload, _ := json.Marshal(map[string]interface{}{"streams": lokiStreamList})
	resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json",
		strings.NewReader(string(lokiPayload)))
	if err != nil {
		t.Fatalf("Loki compare seed push failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	t.Logf("Loki seed: status=%d", resp.StatusCode)

	// Push to VL (jsonline format).
	insertURL := vlURL + "/insert/jsonline?_stream_fields=" +
		url.QueryEscape("service_name,app,level")
	var vlBuf strings.Builder
	vlBuf.Grow(len(entries) * 200)
	for _, e := range entries {
		row, _ := json.Marshal(map[string]interface{}{
			"_time":        e.ts.Format(time.RFC3339Nano),
			"_msg":         e.msg,
			"service_name": svc,
			"app":          svc,
			"level":        e.level,
			"http_method":  e.method,
			"http_status":  strconv.Itoa(e.status),
			"duration_ms":  strconv.Itoa(e.duration),
			"trace_id":     e.traceID,
		})
		vlBuf.Write(row)
		vlBuf.WriteByte('\n')
	}
	resp2, err := http.Post(insertURL, "application/stream+json",
		strings.NewReader(vlBuf.String()))
	if err != nil {
		t.Fatalf("VL compare seed push failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	t.Logf("VL seed: status=%d", resp2.StatusCode)
}

func runLokiCompare(t *testing.T, svc string, f lokiCompareField, r drilldownQualityRange, start, end time.Time) lokiCompareResult {
	t.Helper()

	res := lokiCompareResult{field: f.lokiName, rangeLabel: r.label}

	startStr := strconv.FormatInt(start.UnixNano(), 10)
	endStr := strconv.FormatInt(end.UnixNano(), 10)
	stepStr := strconv.FormatInt(int64(r.step/time.Second), 10)
	query := fmt.Sprintf(`sum by (%s) (count_over_time({service_name=%q}%s [%s]))`,
		f.lokiName, svc, f.filterClause, r.step)

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", startStr)
	params.Set("end", endStr)
	params.Set("step", stepStr)
	queryStr := "/loki/api/v1/query_range?" + params.Encode()

	t0 := time.Now()
	lokiResp, err := http.Get(lokiURL + queryStr)
	res.lokiLatencyMs = time.Since(t0).Milliseconds()
	if err != nil {
		t.Logf("Loki query error field=%s range=%s: %v", f.lokiName, r.label, err)
	} else {
		defer lokiResp.Body.Close()
		lok := parseLokiMatrixSummary(t, lokiResp.Body)
		res.lokiSeries = lok.series
		res.lokiTotal = lok.total
		res.lokiNonZero = lok.nonZero
	}

	req, _ := http.NewRequest(http.MethodGet, proxyURL+queryStr, nil)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	req.Header.Set("X-Scope-OrgID", "fake")
	t0 = time.Now()
	proxyResp, err := http.DefaultClient.Do(req)
	res.proxyLatencyMs = time.Since(t0).Milliseconds()
	if err != nil {
		t.Logf("proxy query error field=%s range=%s: %v", f.lokiName, r.label, err)
	} else {
		defer proxyResp.Body.Close()
		prx := parseLokiMatrixSummary(t, proxyResp.Body)
		res.proxySeries = prx.series
		res.proxyTotal = prx.total
		res.proxyNonZero = prx.nonZero
	}

	return res
}

func parseLokiMatrixSummary(t *testing.T, body io.Reader) matrixSummary {
	t.Helper()
	var payload struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	raw, _ := io.ReadAll(body)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Logf("parseLokiMatrixSummary: decode failed: %v (body=%s)", err, raw[:min(100, len(raw))])
		return matrixSummary{}
	}
	var s matrixSummary
	s.series = len(payload.Data.Result)
	for _, series := range payload.Data.Result {
		for _, pair := range series.Values {
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
				s.nonZero++
			}
			s.total += cnt
		}
	}
	return s
}
