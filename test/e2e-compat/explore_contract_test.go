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

	// sharedWindow builds url.Values with shared start/end timestamps so that
	// proxy and Loki query the identical time window. Key design choices:
	//   - end = now-30s  → both backends have fully committed in-flight writes
	//   - start = ingestionAnchor-2m → anchored to when ingestRichTestData ran,
	//     so the window always covers ingested data regardless of test duration;
	//     using time.Now()-5m could miss data if setup + tests take >2 minutes
	//   - testdata is pushed at ingestionAnchor so it always falls inside this window
	sharedWindow := func(query string) url.Values {
		p := url.Values{}
		p.Set("query", query)
		p.Set("start", fmt.Sprintf("%d", ingestionAnchor.Add(-2*time.Minute).UnixNano()))
		p.Set("end", fmt.Sprintf("%d", time.Now().Add(-30*time.Second).UnixNano()))
		p.Set("limit", "1000")
		return p
	}
	queryBoth := func(t *testing.T, query string) (map[string]interface{}, map[string]interface{}) {
		t.Helper()
		p := sharedWindow(query)
		_, _, proxy := queryRangeGET(t, proxyURL, p, nil)
		_, _, loki := queryRangeGET(t, lokiURL, p, nil)
		return proxy, loki
	}

	t.Run("line_filters_match_loki_counts", func(t *testing.T) {
		// Use the fixed pod label from ingestRichTestData to isolate test data
		// from background log-generator traffic (which uses random pod names).
		proxyResp, lokiResp := queryBoth(t, `{app="api-gateway", pod="api-gateway-7f8d9c6b4-x2k9m"} |= "payments" != "timeout"`)

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
		// Use the fixed pod label from ingestRichTestData to isolate test data
		// from background log-generator traffic (which uses random pod names).
		proxyResp, lokiResp := queryBoth(t, `{app="api-gateway", pod="api-gateway-7f8d9c6b4-x2k9m"} | json | method="GET"`)

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
		// Use the fixed pod label from ingestRichTestData to isolate test data
		// from background log-generator traffic (which uses random pod names).
		proxyResp, lokiResp := queryBoth(t, `{app="payment-service", pod="payment-svc-5c8f7d9a2-q7w3e"} | logfmt | level="error"`)

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
		// Use the fixed pod label from ingestRichTestData to isolate test data
		// from background log-generator traffic (which uses random pod names).
		proxyResp, lokiResp := queryBoth(t, `{app="api-gateway", pod="api-gateway-7f8d9c6b4-x2k9m"} | json | label_format method_alias="{{.method}}", status_code="{{.status}}"`)

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
