//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func doJSONGET(t *testing.T, rawURL string, headers map[string]string) (int, string, map[string]interface{}) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("build request %s: %v", rawURL, err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s failed: %v", rawURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s failed: %v", rawURL, err)
	}

	var payload map[string]interface{}
	_ = json.Unmarshal(body, &payload)
	return resp.StatusCode, string(body), payload
}

func queryRangeGET(t *testing.T, baseURL string, params url.Values, headers map[string]string) (int, string, map[string]interface{}) {
	t.Helper()

	cloned := url.Values{}
	for key, values := range params {
		for _, value := range values {
			cloned.Add(key, value)
		}
	}

	now := time.Now()
	if cloned.Get("start") == "" {
		cloned.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
	}
	if cloned.Get("end") == "" {
		cloned.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	}
	if cloned.Get("limit") == "" {
		cloned.Set("limit", "1000")
	}

	return doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+cloned.Encode(), headers)
}

func responseStreamTimestamps(resp map[string]interface{}) [][]int64 {
	data := extractMap(resp, "data")
	result := extractArray(data, "result")
	streams := make([][]int64, 0, len(result))
	for _, item := range result {
		stream, _ := item.(map[string]interface{})
		values, _ := stream["values"].([]interface{})
		ts := make([]int64, 0, len(values))
		for _, value := range values {
			pair, _ := value.([]interface{})
			if len(pair) == 0 {
				continue
			}
			switch raw := pair[0].(type) {
			case string:
				parsed, err := strconv.ParseInt(raw, 10, 64)
				if err == nil {
					ts = append(ts, parsed)
				}
			case float64:
				ts = append(ts, int64(raw))
			}
		}
		if len(ts) > 0 {
			streams = append(streams, ts)
		}
	}
	return streams
}

func expectMonotonicTimestamps(t *testing.T, timestamps [][]int64, ascending bool) {
	t.Helper()
	if len(timestamps) == 0 {
		t.Fatal("expected at least one stream with timestamps")
	}
	for _, stream := range timestamps {
		for i := 1; i < len(stream); i++ {
			if ascending && stream[i-1] > stream[i] {
				t.Fatalf("timestamps not ascending: %v", stream)
			}
			if !ascending && stream[i-1] < stream[i] {
				t.Fatalf("timestamps not descending: %v", stream)
			}
		}
	}
}

func TestExplore_HTTPQueryRangeContracts(t *testing.T) {
	ensureDataIngested(t)

	t.Run("line_filters_match_loki_counts", func(t *testing.T) {
		query := `{app="api-gateway"} |= "payments" != "timeout"`
		proxyResp := queryProxy(t, query)
		lokiResp := queryLoki(t, query)

		if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
			t.Fatalf("expected successful responses, proxy=%v loki=%v", proxyResp, lokiResp)
		}
		proxyLines := countLogLines(proxyResp)
		lokiLines := countLogLines(lokiResp)
		if proxyLines == 0 || lokiLines == 0 {
			t.Fatalf("expected filtered lines from both backends, proxy=%d loki=%d", proxyLines, lokiLines)
		}
		if proxyLines != lokiLines {
			t.Fatalf("line-filter parity mismatch, proxy=%d loki=%d", proxyLines, lokiLines)
		}
	})

	t.Run("json_pipeline_matches_loki_counts", func(t *testing.T) {
		query := `{app="api-gateway"} | json | method="GET"`
		proxyResp := queryProxy(t, query)
		lokiResp := queryLoki(t, query)

		if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
			t.Fatalf("expected successful json pipeline responses, proxy=%v loki=%v", proxyResp, lokiResp)
		}
		proxyLines := countLogLines(proxyResp)
		lokiLines := countLogLines(lokiResp)
		if proxyLines == 0 || lokiLines == 0 {
			t.Fatalf("expected json parser results from both backends, proxy=%d loki=%d", proxyLines, lokiLines)
		}
		if proxyLines != lokiLines {
			t.Fatalf("json pipeline parity mismatch, proxy=%d loki=%d", proxyLines, lokiLines)
		}
	})

	t.Run("logfmt_pipeline_matches_loki_counts", func(t *testing.T) {
		query := `{app="payment-service"} | logfmt | level="error"`
		proxyResp := queryProxy(t, query)
		lokiResp := queryLoki(t, query)

		if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
			t.Fatalf("expected successful logfmt pipeline responses, proxy=%v loki=%v", proxyResp, lokiResp)
		}
		proxyLines := countLogLines(proxyResp)
		lokiLines := countLogLines(lokiResp)
		if proxyLines == 0 || lokiLines == 0 {
			t.Fatalf("expected logfmt parser results from both backends, proxy=%d loki=%d", proxyLines, lokiLines)
		}
		if proxyLines != lokiLines {
			t.Fatalf("logfmt pipeline parity mismatch, proxy=%d loki=%d", proxyLines, lokiLines)
		}
	})

	t.Run("direction_forward_and_backward_keep_stream_ordering", func(t *testing.T) {
		forwardParams := url.Values{}
		forwardParams.Set("query", `{app="api-gateway",level="info"}`)
		forwardParams.Set("direction", "forward")
		forwardParams.Set("limit", "100")

		status, body, proxyForward := queryRangeGET(t, proxyURL, forwardParams, nil)
		if status != http.StatusOK || !checkStatus(proxyForward) {
			t.Fatalf("expected 200 forward query, status=%d body=%s", status, body)
		}
		expectMonotonicTimestamps(t, responseStreamTimestamps(proxyForward), true)

		backwardParams := url.Values{}
		backwardParams.Set("query", `{app="api-gateway",level="info"}`)
		backwardParams.Set("direction", "backward")
		backwardParams.Set("limit", "100")

		status, body, proxyBackward := queryRangeGET(t, proxyURL, backwardParams, nil)
		if status != http.StatusOK || !checkStatus(proxyBackward) {
			t.Fatalf("expected 200 backward query, status=%d body=%s", status, body)
		}
		expectMonotonicTimestamps(t, responseStreamTimestamps(proxyBackward), false)
	})

	t.Run("metric_queries_keep_matrix_shape", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `sum by (level) (count_over_time({app="api-gateway"}[5m]))`)
		params.Set("step", "60")

		proxyStatus, proxyBody, proxyResp := queryRangeGET(t, proxyURL, params, nil)
		lokiStatus, lokiBody, lokiResp := queryRangeGET(t, lokiURL, params, nil)

		if proxyStatus != http.StatusOK || lokiStatus != http.StatusOK {
			t.Fatalf("expected 200 matrix responses, proxy=%d loki=%d proxyBody=%s lokiBody=%s", proxyStatus, lokiStatus, proxyBody, lokiBody)
		}

		proxyData := extractMap(proxyResp, "data")
		lokiData := extractMap(lokiResp, "data")
		if proxyData["resultType"] != "matrix" || lokiData["resultType"] != "matrix" {
			t.Fatalf("expected matrix resultType, proxy=%v loki=%v", proxyData["resultType"], lokiData["resultType"])
		}
		if len(extractArray(proxyData, "result")) == 0 || len(extractArray(lokiData, "result")) == 0 {
			t.Fatalf("expected non-empty matrix results, proxy=%v loki=%v", proxyResp, lokiResp)
		}
	})

	t.Run("label_format_keeps_browser_visible_stream_labels", func(t *testing.T) {
		query := `{app="api-gateway"} | json | label_format method_alias="{{.method}}", status_code="{{.status}}"`
		proxyResp := queryProxy(t, query)
		lokiResp := queryLoki(t, query)

		if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
			t.Fatalf("expected successful label_format responses, proxy=%v loki=%v", proxyResp, lokiResp)
		}

		proxyLines := countLogLines(proxyResp)
		lokiLines := countLogLines(lokiResp)
		if proxyLines == 0 || lokiLines == 0 {
			t.Fatalf("expected label_format to keep log results visible, proxy=%d loki=%d", proxyLines, lokiLines)
		}
		if proxyLines != lokiLines {
			t.Fatalf("label_format parity mismatch, proxy=%d loki=%d", proxyLines, lokiLines)
		}
	})

	t.Run("invalid_queries_fail_as_4xx_not_5xx", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="api-gateway"`)

		status, body, _ := queryRangeGET(t, proxyURL, params, nil)
		if status < 400 || status >= 500 {
			t.Fatalf("expected invalid query to fail as 4xx, got %d body=%s", status, body)
		}
		if !strings.Contains(strings.ToLower(body), "error") {
			t.Fatalf("expected invalid query body to explain the failure, got %s", body)
		}
	})
}
