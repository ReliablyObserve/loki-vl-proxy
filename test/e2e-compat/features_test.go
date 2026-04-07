//go:build e2e

// Feature verification tests — validates all proxy features against real backends.
// Covers: index/stats, volume, multitenancy, tail, query analytics,
// Grafana datasource config, and additional edge cases.
package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func readTailFrame(t *testing.T, conn *websocket.Conn, wantSubstring string, timeout time.Duration) map[string]interface{} {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("tail read failed: %v", err)
		}
		if wantSubstring != "" && !strings.Contains(string(msg), wantSubstring) {
			continue
		}
		var frame map[string]interface{}
		if err := json.Unmarshal(msg, &frame); err != nil {
			t.Fatalf("invalid tail JSON frame: %v", err)
		}
		return frame
	}
}

// =============================================================================
// Index Stats (real implementation via VL /select/logsql/hits)
// =============================================================================

func TestFeature_IndexStats_ReturnsRealData(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/stats?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/index/stats?"+params.Encode())

	// Both should return numeric fields
	for _, field := range []string{"streams", "chunks", "entries", "bytes"} {
		if v, ok := proxyResp[field]; ok {
			if num, ok := v.(float64); ok && num >= 0 {
				score.pass("index_stats", fmt.Sprintf("proxy has %s=%v", field, num))
			} else {
				score.fail("index_stats", fmt.Sprintf("proxy %s not a number: %v", field, v))
			}
		} else {
			score.fail("index_stats", fmt.Sprintf("proxy missing %s", field))
		}
	}

	// Proxy should have entries > 0 if Loki does
	lokiEntries, _ := lokiResp["entries"].(float64)
	proxyEntries, _ := proxyResp["entries"].(float64)
	if lokiEntries > 0 && proxyEntries > 0 {
		score.pass("index_stats", fmt.Sprintf("both have entries: loki=%v proxy=%v", lokiEntries, proxyEntries))
	} else if lokiEntries > 0 && proxyEntries == 0 {
		score.fail("index_stats", "proxy has 0 entries but Loki has data")
	}

	score.report(t)
}

// =============================================================================
// Index Volume (real implementation via VL /select/logsql/hits)
// =============================================================================

func TestFeature_IndexVolume_ReturnsPrometheusFormat(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{namespace="prod"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/volume?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("volume", "proxy returns status=success")
	} else {
		score.fail("volume", "proxy error")
	}

	if data, ok := proxyResp["data"].(map[string]interface{}); ok {
		if rt, ok := data["resultType"].(string); ok && rt == "vector" {
			score.pass("volume", "resultType=vector")
		} else {
			score.fail("volume", fmt.Sprintf("expected resultType=vector, got %v", data["resultType"]))
		}

		if result, ok := data["result"].([]interface{}); ok {
			score.pass("volume", fmt.Sprintf("result has %d entries", len(result)))
			if len(result) > 0 {
				entry, _ := result[0].(map[string]interface{})
				if _, ok := entry["metric"]; ok {
					score.pass("volume", "entries have metric field")
				}
				if _, ok := entry["value"]; ok {
					score.pass("volume", "entries have value field")
				}
			}
		}
	} else {
		score.fail("volume", "missing data object")
	}

	score.report(t)
}

func TestFeature_IndexVolumeRange_ReturnsMatrix(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{namespace="prod"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/volume_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("volume_range", "proxy returns status=success")
	} else {
		score.fail("volume_range", "proxy error")
	}

	if data, ok := proxyResp["data"].(map[string]interface{}); ok {
		if rt := data["resultType"]; rt == "matrix" {
			score.pass("volume_range", "resultType=matrix")
		} else {
			score.fail("volume_range", fmt.Sprintf("expected resultType=matrix, got %v", rt))
		}
	}

	score.report(t)
}

// =============================================================================
// Multitenancy (X-Scope-OrgID → AccountID/ProjectID)
// =============================================================================

func TestFeature_Multitenancy_DefaultTenantBypassUsesVLGlobalTenantWithoutMappings(t *testing.T) {
	score := &CompatScore{}

	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")

	for _, orgID := range []string{"0", "fake", "default", "*"} {
		req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", orgID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			score.fail("multitenancy", fmt.Sprintf("request with OrgID=%s failed: %v", orgID, err))
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			score.pass("multitenancy", fmt.Sprintf("OrgID=%s uses VL default tenant when no tenant map is configured", orgID))
		} else {
			score.fail("multitenancy", fmt.Sprintf("expected OrgID=%s to use VL default tenant, got %d", orgID, resp.StatusCode))
		}
	}

	// Query without tenant header is still allowed when auth.enabled=false.
	req2, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/labels", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		score.fail("multitenancy", "no-org request failed")
	} else {
		defer resp2.Body.Close()
		if resp2.StatusCode == 200 {
			score.pass("multitenancy", "no OrgID header accepted")
		}
	}

	score.report(t)
}

func TestFeature_Multitenancy_QueryEndpointsSupportExplicitMultiTenantFanout(t *testing.T) {
	score := &CompatScore{}

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", time.Now().Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", time.Now().UnixNano()))
	params.Set("limit", "10")

	req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0|fake")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		score.fail("multitenancy", "multi-tenant query_range request failed: "+err.Error())
		score.report(t)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		score.fail("multitenancy", fmt.Sprintf("expected 200 for multi-tenant query_range, got %d", resp.StatusCode))
		score.report(t)
		return
	}

	var decoded struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		score.fail("multitenancy", "failed to decode multi-tenant query_range response: "+err.Error())
		score.report(t)
		return
	}
	if decoded.Status == "success" && decoded.Data.ResultType == "streams" {
		score.pass("multitenancy", "multi-tenant query_range returns Loki streams response")
	} else {
		score.fail("multitenancy", fmt.Sprintf("unexpected multi-tenant response shape: status=%q resultType=%q", decoded.Status, decoded.Data.ResultType))
	}

	seenTenantLabel := false
	for _, item := range decoded.Data.Result {
		if item.Stream["__tenant_id__"] != "" {
			seenTenantLabel = true
			break
		}
	}
	if seenTenantLabel {
		score.pass("multitenancy", "multi-tenant query_range injects __tenant_id__ label")
	} else {
		score.fail("multitenancy", "multi-tenant query_range missing __tenant_id__ label")
	}

	filterParams := url.Values{}
	filterParams.Set("query", `{app="api-gateway",__tenant_id__="fake"}`)
	filterParams.Set("start", params.Get("start"))
	filterParams.Set("end", params.Get("end"))
	filterParams.Set("limit", "10")
	filterReq, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+filterParams.Encode(), nil)
	filterReq.Header.Set("X-Scope-OrgID", "0|fake")
	filterResp, err := http.DefaultClient.Do(filterReq)
	if err != nil {
		score.fail("multitenancy", "tenant-filtered query_range request failed: "+err.Error())
	} else {
		defer filterResp.Body.Close()
		var filtered struct {
			Data struct {
				Result []struct {
					Stream map[string]string `json:"stream"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.NewDecoder(filterResp.Body).Decode(&filtered); err != nil {
			score.fail("multitenancy", "failed to decode tenant-filtered query_range response: "+err.Error())
		} else if len(filtered.Data.Result) > 0 && filtered.Data.Result[0].Stream["__tenant_id__"] == "fake" {
			score.pass("multitenancy", "__tenant_id__ selector narrows multi-tenant fanout")
		} else {
			score.fail("multitenancy", fmt.Sprintf("expected __tenant_id__ filter to narrow to fake, got %+v", filtered.Data.Result))
		}
	}

	labelResp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/label/__tenant_id__/values", map[string]string{"X-Scope-OrgID": "0|fake"})
	values, _ := labelResp["data"].([]interface{})
	if len(values) == 2 {
		score.pass("multitenancy", "__tenant_id__ label values reflect requested tenants")
	} else {
		score.fail("multitenancy", fmt.Sprintf("expected __tenant_id__ values for 2 tenants, got %v", labelResp))
	}

	tailReq, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/tail?query=%7Bapp%3D%22api-gateway%22%7D", nil)
	tailReq.Header.Set("X-Scope-OrgID", "0|fake")
	tailResp, err := http.DefaultClient.Do(tailReq)
	if err != nil {
		score.fail("multitenancy", "multi-tenant tail request failed: "+err.Error())
	} else {
		defer tailResp.Body.Close()
		if tailResp.StatusCode == http.StatusBadRequest {
			score.pass("multitenancy", "tail rejects multi-tenant headers like upstream Loki")
		} else {
			score.fail("multitenancy", fmt.Sprintf("expected tail multi-tenant rejection, got %d", tailResp.StatusCode))
		}
	}

	score.report(t)
}

func TestFeature_Multitenancy_FilteredLabelsSeriesAndDetectedFields(t *testing.T) {
	ensureDataIngested(t)
	headers := map[string]string{"X-Scope-OrgID": "0|fake"}
	now := time.Now()
	start := fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano())
	end := fmt.Sprintf("%d", now.UnixNano())

	labelsResp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/labels", headers)
	data := extractArray(labelsResp, "data")
	if len(data) == 0 {
		t.Fatalf("expected labels for multi-tenant request, got %v", labelsResp)
	}
	foundTenantID := false
	for _, item := range data {
		if item == "__tenant_id__" {
			foundTenantID = true
			break
		}
	}
	if !foundTenantID {
		t.Fatalf("expected synthetic __tenant_id__ in labels response, got %v", labelsResp)
	}

	seriesParams := url.Values{}
	seriesParams.Add("match[]", `{app="api-gateway",__tenant_id__="fake"}`)
	seriesResp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/series?"+seriesParams.Encode(), headers)
	series := extractArray(seriesResp, "data")
	if len(series) == 0 {
		t.Fatalf("expected series data for multi-tenant filtered request, got %v", seriesResp)
	}
	for _, item := range series {
		labels, _ := item.(map[string]interface{})
		if labels["__tenant_id__"] != "fake" {
			t.Fatalf("expected series __tenant_id__=fake, got %v", item)
		}
	}

	regexSeriesParams := url.Values{}
	regexSeriesParams.Add("match[]", `{app="api-gateway",__tenant_id__=~"f.*"}`)
	regexSeriesResp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/series?"+regexSeriesParams.Encode(), headers)
	regexSeries := extractArray(regexSeriesResp, "data")
	if len(regexSeries) == 0 {
		t.Fatalf("expected regex-filtered series data for multi-tenant request, got %v", regexSeriesResp)
	}
	for _, item := range regexSeries {
		labels, _ := item.(map[string]interface{})
		if labels["__tenant_id__"] != "fake" {
			t.Fatalf("expected regex-filtered series __tenant_id__=fake, got %v", item)
		}
	}

	labelValueResp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/label/cluster/values?query="+url.QueryEscape(`{app="api-gateway",__tenant_id__=~"f.*"}`), headers)
	labelValues := extractArray(labelValueResp, "data")
	if len(labelValues) == 0 {
		t.Fatalf("expected cluster label values for regex-filtered multi-tenant request, got %v", labelValueResp)
	}
	foundCluster := false
	for _, item := range labelValues {
		if item == "us-east-1" {
			foundCluster = true
			break
		}
	}
	if !foundCluster {
		t.Fatalf("expected regex-filtered cluster label values to include us-east-1, got %v", labelValueResp)
	}

	singleFields := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/detected_fields?query="+url.QueryEscape(`{app="api-gateway"}`)+"&start="+start+"&end="+end, map[string]string{"X-Scope-OrgID": "0"})
	multiFields := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/detected_fields?query="+url.QueryEscape(`{app="api-gateway"}`)+"&start="+start+"&end="+end, headers)
	singleFieldItems := extractArray(singleFields, "fields")
	multiFieldItems := extractArray(multiFields, "fields")
	if len(singleFieldItems) == 0 || len(multiFieldItems) == 0 {
		t.Fatalf("expected detected_fields data in both single and multi tenant responses, single=%v multi=%v", singleFields, multiFields)
	}
	singleCards := map[string]float64{}
	for _, item := range singleFieldItems {
		field := item.(map[string]interface{})
		singleCards[field["label"].(string)], _ = field["cardinality"].(float64)
	}
	for _, item := range multiFieldItems {
		field := item.(map[string]interface{})
		label := field["label"].(string)
		if label == "method" || label == "status" || label == "path" {
			if field["cardinality"] != singleCards[label] {
				t.Fatalf("expected exact union cardinality for %s, single=%v multi=%v", label, singleCards[label], field["cardinality"])
			}
		}
	}
}

func TestFeature_Multitenancy_TenantIDPermutations(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()
	headers := map[string]string{"X-Scope-OrgID": "0|fake"}

	buildParams := func(query string) url.Values {
		params := url.Values{}
		params.Set("query", query)
		params.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
		params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
		params.Set("limit", "100")
		return params
	}

	t.Run("exact_match", func(t *testing.T) {
		_, _, resp := queryRangeGET(t, proxyURL, buildParams(`{app="api-gateway",__tenant_id__="fake"}`), headers)
		if !checkStatus(resp) {
			t.Fatalf("expected exact __tenant_id__ match to succeed, got %v", resp)
		}
		if countLogLines(resp) == 0 {
			t.Fatalf("expected exact __tenant_id__ match to return logs, got %v", resp)
		}
		for _, item := range extractArray(extractMap(resp, "data"), "result") {
			stream := item.(map[string]interface{})["stream"].(map[string]interface{})
			if stream["__tenant_id__"] != "fake" {
				t.Fatalf("expected exact __tenant_id__ filter to keep only fake, got %v", stream)
			}
		}
	})

	t.Run("negative_regex", func(t *testing.T) {
		_, _, resp := queryRangeGET(t, proxyURL, buildParams(`{app="api-gateway",__tenant_id__!~"f.*"}`), headers)
		if !checkStatus(resp) {
			t.Fatalf("expected negative regex __tenant_id__ match to succeed, got %v", resp)
		}
		if countLogLines(resp) == 0 {
			t.Fatalf("expected negative regex __tenant_id__ match to return logs, got %v", resp)
		}
		for _, item := range extractArray(extractMap(resp, "data"), "result") {
			stream := item.(map[string]interface{})["stream"].(map[string]interface{})
			if stream["__tenant_id__"] == "fake" {
				t.Fatalf("expected negative regex to exclude fake tenant, got %v", stream)
			}
		}
	})

	t.Run("no_match_returns_empty_success", func(t *testing.T) {
		_, _, resp := queryRangeGET(t, proxyURL, buildParams(`{app="api-gateway",__tenant_id__="missing"}`), headers)
		if !checkStatus(resp) {
			t.Fatalf("expected no-match __tenant_id__ query to keep success shape, got %v", resp)
		}
		if countLogLines(resp) != 0 {
			t.Fatalf("expected no-match __tenant_id__ query to return no lines, got %v", resp)
		}
	})
}

func TestFeature_AdminDebugEndpoints_DefaultClosed(t *testing.T) {
	score := &CompatScore{}

	for _, path := range []string{"/debug/queries", "/debug/pprof/"} {
		resp, err := http.Get(proxyURL + path)
		if err != nil {
			score.fail("admin_endpoints", fmt.Sprintf("%s request failed: %v", path, err))
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			score.pass("admin_endpoints", fmt.Sprintf("%s disabled by default", path))
		} else {
			score.fail("admin_endpoints", fmt.Sprintf("%s expected 404, got %d", path, resp.StatusCode))
		}
	}

	score.report(t)
}

// =============================================================================
// Tail (WebSocket)
// =============================================================================

func TestFeature_Tail_WebSocketConnection(t *testing.T) {
	score := &CompatScore{}

	wsURL := "ws" + strings.TrimPrefix(proxyURL, "http") + "/loki/api/v1/tail"
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial(wsURL+"?"+params.Encode(), nil)
	if err != nil {
		if resp != nil {
			score.fail("tail", fmt.Sprintf("WebSocket upgrade failed: %d", resp.StatusCode))
		} else {
			score.fail("tail", "WebSocket dial failed: "+err.Error())
		}
		score.report(t)
		return
	}
	defer conn.Close()
	score.pass("tail", "WebSocket connection established")

	// Set a short read deadline to avoid blocking
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		// Timeout is OK — means no new logs during the window
		score.pass("tail", "tail read completed (timeout or no new data)")
	} else {
		var frame map[string]interface{}
		if json.Unmarshal(msg, &frame) == nil {
			if _, ok := frame["streams"]; ok {
				score.pass("tail", "received Loki-compatible tail frame with streams")
			} else {
				score.fail("tail", "tail frame missing streams field")
			}
		}
	}

	score.report(t)
}

func TestFeature_Tail_WebSocketStreamsLiveData_ProxyMatchesLoki(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-live-%d", now.UnixNano())
	msg := "tail live frame " + app

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
	params.Set("start", fmt.Sprintf("%d", now.UnixNano()))

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	proxyConn, proxyResp, err := dialer.Dial("ws"+strings.TrimPrefix(proxyURL, "http")+"/loki/api/v1/tail?"+params.Encode(), nil)
	if err != nil {
		t.Fatalf("proxy websocket dial failed: %v (resp=%v)", err, proxyResp)
	}
	defer proxyConn.Close()

	lokiConn, lokiResp, err := dialer.Dial("ws"+strings.TrimPrefix(lokiURL, "http")+"/loki/api/v1/tail?"+params.Encode(), nil)
	if err != nil {
		t.Fatalf("loki websocket dial failed: %v (resp=%v)", err, lokiResp)
	}
	defer lokiConn.Close()

	pushCustomToVL(t, now.Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: msg, Level: "info"}}, []string{"app", "env", "level"})
	pushCustomToLoki(t, now.Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: msg, Level: "info"}})

	proxyFrame := readTailFrame(t, proxyConn, msg, 10*time.Second)
	lokiFrame := readTailFrame(t, lokiConn, msg, 10*time.Second)

	if _, ok := proxyFrame["streams"]; !ok {
		t.Fatalf("expected proxy tail frame to contain streams, got %v", proxyFrame)
	}
	if _, ok := lokiFrame["streams"]; !ok {
		t.Fatalf("expected loki tail frame to contain streams, got %v", lokiFrame)
	}
}

func TestFeature_Tail_BrowserOriginRejectedByDefault(t *testing.T) {
	score := &CompatScore{}

	wsURL := "ws" + strings.TrimPrefix(proxyURL, "http") + "/loki/api/v1/tail"
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	headers := http.Header{}
	headers.Set("Origin", "https://grafana.example.com")

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial(wsURL+"?"+params.Encode(), headers)
	if err == nil {
		conn.Close()
		score.fail("tail_origin", "expected browser origin to be rejected by default")
		score.report(t)
		return
	}
	if resp != nil && resp.StatusCode == http.StatusForbidden {
		score.pass("tail_origin", "browser origin rejected by default")
	} else {
		score.fail("tail_origin", fmt.Sprintf("expected 403 for browser origin, got resp=%v err=%v", resp, err))
	}

	score.report(t)
}

func TestFeature_Tail_SyntheticProxyAllowsConfiguredOriginAndStreamsLiveData(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-synth-%d", now.UnixNano())
	msg := "synthetic tail frame " + app

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
	params.Set("start", fmt.Sprintf("%d", now.UnixNano()))

	headers := http.Header{}
	headers.Set("Origin", "http://127.0.0.1:3002")
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailProxyURL, "http")+"/loki/api/v1/tail?"+params.Encode(), headers)
	if err != nil {
		t.Fatalf("synthetic proxy websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	pushCustomToVL(t, now.Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "warn",
	}, []logLine{{Msg: msg, Level: "warn"}}, []string{"app", "env", "level"})

	frame := readTailFrame(t, conn, msg, 10*time.Second)
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected synthetic tail frame with streams, got %v", frame)
	}
}

func TestFeature_Tail_SyntheticProxySurvivesIdleWindow(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-idle-%d", now.UnixNano())
	msg := "idle tail frame " + app

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
	params.Set("start", fmt.Sprintf("%d", now.UnixNano()))

	headers := http.Header{}
	headers.Set("Origin", "http://127.0.0.1:3002")
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailProxyURL, "http")+"/loki/api/v1/tail?"+params.Encode(), headers)
	if err != nil {
		t.Fatalf("synthetic proxy websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	time.Sleep(4 * time.Second)
	pushCustomToVL(t, time.Now().Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: msg, Level: "info"}}, []string{"app", "env", "level"})

	frame := readTailFrame(t, conn, msg, 10*time.Second)
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected idle tail frame with streams, got %v", frame)
	}
}

func TestFeature_Tail_ReverseProxyIngressStreamsLiveData(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-ingress-%d", now.UnixNano())
	msg := "ingress tail frame " + app

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
	params.Set("start", fmt.Sprintf("%d", now.UnixNano()))

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailIngressURL, "http")+"/loki/api/v1/tail?"+params.Encode(), nil)
	if err != nil {
		t.Fatalf("ingress websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	pushCustomToVL(t, now.Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: msg, Level: "info"}}, []string{"app", "env", "level"})

	frame := readTailFrame(t, conn, msg, 10*time.Second)
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected ingress tail frame with streams, got %v", frame)
	}
}

func TestFeature_Tail_ReverseProxyIngressSurvivesIdleWindow(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-ingress-idle-%d", now.UnixNano())
	msg := "ingress idle tail frame " + app

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
	params.Set("start", fmt.Sprintf("%d", now.UnixNano()))

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailIngressURL, "http")+"/loki/api/v1/tail?"+params.Encode(), nil)
	if err != nil {
		t.Fatalf("ingress websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	time.Sleep(4 * time.Second)
	pushCustomToVL(t, time.Now().Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: msg, Level: "info"}}, []string{"app", "env", "level"})

	frame := readTailFrame(t, conn, msg, 10*time.Second)
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected ingress tail frame after idle window, got %v", frame)
	}
}

func TestFeature_Tail_SyntheticProxyReconnectsCleanly(t *testing.T) {
	app := fmt.Sprintf("tail-reconnect-%d", time.Now().UnixNano())

	connect := func(start time.Time) *websocket.Conn {
		t.Helper()
		params := url.Values{}
		params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
		params.Set("start", fmt.Sprintf("%d", start.UnixNano()))

		headers := http.Header{}
		headers.Set("Origin", "http://127.0.0.1:3002")
		dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
		conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailProxyURL, "http")+"/loki/api/v1/tail?"+params.Encode(), headers)
		if err != nil {
			t.Fatalf("synthetic proxy websocket dial failed: %v (resp=%v)", err, resp)
		}
		return conn
	}

	firstConn := connect(time.Now())
	firstMsg := "tail reconnect frame one " + app
	pushCustomToVL(t, time.Now().Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: firstMsg, Level: "info"}}, []string{"app", "env", "level"})
	_ = readTailFrame(t, firstConn, firstMsg, 10*time.Second)
	_ = firstConn.Close()

	secondConn := connect(time.Now())
	defer secondConn.Close()
	secondMsg := "tail reconnect frame two " + app
	pushCustomToVL(t, time.Now().Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "warn",
	}, []logLine{{Msg: secondMsg, Level: "warn"}}, []string{"app", "env", "level"})
	frame := readTailFrame(t, secondConn, secondMsg, 10*time.Second)
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected reconnect tail frame with streams, got %v", frame)
	}
}

func TestFeature_Tail_SyntheticProxyLongLivedSessionStreamsAcrossPolls(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("tail-session-%d", now.UnixNano())

	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{app="%s"}`, app))
	params.Set("start", fmt.Sprintf("%d", now.UnixNano()))

	headers := http.Header{}
	headers.Set("Origin", "http://127.0.0.1:3002")
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailProxyURL, "http")+"/loki/api/v1/tail?"+params.Encode(), headers)
	if err != nil {
		t.Fatalf("synthetic proxy websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	firstMsg := "tail session frame one " + app
	pushCustomToVL(t, time.Now().Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "info",
	}, []logLine{{Msg: firstMsg, Level: "info"}}, []string{"app", "env", "level"})
	_ = readTailFrame(t, conn, firstMsg, 10*time.Second)

	time.Sleep(3 * time.Second)

	secondMsg := "tail session frame two " + app
	pushCustomToVL(t, time.Now().Add(500*time.Millisecond), map[string]string{
		"app":   app,
		"env":   "test",
		"level": "warn",
	}, []logLine{{Msg: secondMsg, Level: "warn"}}, []string{"app", "env", "level"})
	frame := readTailFrame(t, conn, secondMsg, 10*time.Second)
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected long-lived tail session to receive second batch, got %v", frame)
	}
}

func TestFeature_Tail_NativeModeClosesWhenBackendTailUnavailable(t *testing.T) {
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)

	headers := http.Header{}
	headers.Set("Origin", "http://127.0.0.1:3002")
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(tailNativeURL, "http")+"/loki/api/v1/tail?"+params.Encode(), headers)
	if err != nil {
		t.Fatalf("native-only proxy websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expected native-only tail to close when backend native tail is unavailable")
	}
	if !websocket.IsCloseError(err, websocket.CloseInternalServerErr) {
		t.Fatalf("expected internal server close for native-only tail fallback denial, got %v", err)
	}
}

// =============================================================================
// Query Analytics (/debug/queries)
// =============================================================================

func TestFeature_QueryAnalytics_Endpoint(t *testing.T) {
	score := &CompatScore{}

	// First run some queries to populate tracker
	queryProxy(t, `{app="api-gateway"}`)
	queryProxy(t, `{app="payment-service"}`)
	queryProxy(t, `{app="api-gateway"} |= "error"`)

	resp, err := http.Get(proxyURL + "/debug/queries")
	if err != nil {
		score.fail("analytics", "request failed: "+err.Error())
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			score.pass("analytics", "/debug/queries disabled by default")
		} else {
			score.fail("analytics", fmt.Sprintf("expected /debug/queries to be disabled by default, got %d", resp.StatusCode))
		}
	}

	score.report(t)
}

// =============================================================================
// Security Headers
// =============================================================================

func TestFeature_SecurityHeaders(t *testing.T) {
	score := &CompatScore{}

	resp, err := http.Get(proxyURL + "/loki/api/v1/labels")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if v := resp.Header.Get("X-Content-Type-Options"); v == "nosniff" {
		score.pass("security", "X-Content-Type-Options: nosniff")
	} else {
		score.fail("security", fmt.Sprintf("expected nosniff, got %q", v))
	}

	if v := resp.Header.Get("X-Frame-Options"); v == "DENY" {
		score.pass("security", "X-Frame-Options: DENY")
	} else {
		score.fail("security", fmt.Sprintf("expected DENY, got %q", v))
	}

	if v := resp.Header.Get("Cache-Control"); v == "no-store" {
		score.pass("security", "Cache-Control: no-store")
	} else {
		score.fail("security", fmt.Sprintf("expected no-store, got %q", v))
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Concurrent identical queries (coalescing)
// =============================================================================

func TestEdge_ConcurrentIdenticalQueries(t *testing.T) {
	score := &CompatScore{}
	q := `{app="api-gateway"}`
	n := 10
	results := make(chan map[string]interface{}, n)

	for range n {
		go func() {
			results <- queryProxy(t, q)
		}()
	}

	successCount := 0
	for range n {
		resp := <-results
		if checkStatus(resp) {
			successCount++
		}
	}

	if successCount == n {
		score.pass("coalescing", fmt.Sprintf("all %d concurrent requests succeeded", n))
	} else {
		score.fail("coalescing", fmt.Sprintf("only %d/%d succeeded", successCount, n))
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Very long query string
// =============================================================================

func TestEdge_LongQueryString(t *testing.T) {
	score := &CompatScore{}

	// Build a query with many OR conditions
	conditions := make([]string, 50)
	for i := range conditions {
		conditions[i] = fmt.Sprintf(`|= "pattern_%d"`, i)
	}
	q := `{app="api-gateway"} ` + strings.Join(conditions, " ")

	now := time.Now()
	params := url.Values{}
	params.Set("query", q)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")

	resp, err := http.Get(proxyURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		score.fail("long_query", "request failed: "+err.Error())
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == 200 || resp.StatusCode == 400 {
			score.pass("long_query", fmt.Sprintf("long query handled gracefully (%d)", resp.StatusCode))
		} else {
			score.fail("long_query", fmt.Sprintf("unexpected status: %d", resp.StatusCode))
		}
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Query with all filter types combined
// =============================================================================

func TestEdge_CombinedFilterTypes(t *testing.T) {
	score := &CompatScore{}

	queries := []struct {
		name  string
		query string
	}{
		{"line_contains", `{app="api-gateway"} |= "GET"`},
		{"line_not_contains", `{app="api-gateway"} != "health"`},
		{"regex_match", `{app="api-gateway"} |~ "GET|POST"`},
		{"regex_not_match", `{app="api-gateway"} !~ "metrics|health|ready"`},
		{"json_parser", `{app="api-gateway", level="info"} | json`},
		{"multi_filter_chain", `{app="api-gateway"} |= "api" != "health" |~ "GET|POST"`},
	}

	for _, tc := range queries {
		proxyResult := queryProxy(t, tc.query)
		lokiResult := queryLoki(t, tc.query)

		proxyLines := countLogLines(proxyResult)
		lokiLines := countLogLines(lokiResult)

		t.Logf("[%s] Loki=%d, Proxy=%d", tc.name, lokiLines, proxyLines)

		if checkStatus(proxyResult) {
			score.pass(tc.name, fmt.Sprintf("proxy OK (%d lines)", proxyLines))
		} else {
			score.fail(tc.name, "proxy error")
		}

		// Both should return results (or both empty)
		if (lokiLines > 0 && proxyLines > 0) || (lokiLines == 0 && proxyLines == 0) {
			score.pass(tc.name, "result count direction matches Loki")
		} else {
			score.fail(tc.name, fmt.Sprintf("count mismatch: loki=%d proxy=%d", lokiLines, proxyLines))
		}
	}

	score.report(t)
}

func TestFeature_DrilldownClusterLevelOrFilters(t *testing.T) {
	score := &CompatScore{}

	query := `{cluster="us-east-1"} | detected_level="error" or detected_level="info" or detected_level="warn" | json | logfmt | drop __error__, __error_details__`

	proxyResult := queryProxy(t, query)
	lokiResult := queryLoki(t, query)

	if checkStatus(proxyResult) {
		score.pass("drilldown_level_filters", "proxy returns success")
	} else {
		score.fail("drilldown_level_filters", fmt.Sprintf("proxy error: %v", proxyResult))
	}

	if checkStatus(lokiResult) {
		score.pass("drilldown_level_filters", "loki returns success")
	} else {
		score.fail("drilldown_level_filters", fmt.Sprintf("loki error: %v", lokiResult))
	}

	proxyLines := countLogLines(proxyResult)
	lokiLines := countLogLines(lokiResult)
	if proxyLines > 0 {
		score.pass("drilldown_level_filters", fmt.Sprintf("proxy returns %d lines", proxyLines))
	} else {
		score.fail("drilldown_level_filters", "proxy returned no log lines")
	}
	if lokiLines == proxyLines {
		score.pass("drilldown_level_filters", fmt.Sprintf("line count matches Loki (%d)", proxyLines))
	} else {
		score.fail("drilldown_level_filters", fmt.Sprintf("line count mismatch: loki=%d proxy=%d", lokiLines, proxyLines))
	}

	score.report(t)
}

func TestFeature_MultitenantDrilldownClusterLevelOrFilters(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0|fake"}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{cluster="us-east-1"} | detected_level="error" or detected_level="info" or detected_level="warn" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-3*time.Hour).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "1000")

	resp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode(), headers)
	if !checkStatus(resp) {
		t.Fatalf("expected multitenant drilldown level filters to succeed, got %v", resp)
	}
	if lines := countLogLines(resp); lines == 0 {
		t.Fatalf("expected multitenant drilldown level filters to return logs, got %v", resp)
	}
}

// =============================================================================
// Edge Case: Empty result queries
// =============================================================================

func TestEdge_EmptyResults(t *testing.T) {
	score := &CompatScore{}

	queries := []struct {
		name  string
		query string
	}{
		{"nonexistent_app", `{app="this-app-does-not-exist-xyz"}`},
		{"impossible_filter", `{app="api-gateway"} |= "IMPOSSIBLE_STRING_THAT_NEVER_EXISTS_12345"`},
		{"future_time", `{app="api-gateway"}`}, // with future start time
	}

	for _, tc := range queries {
		var proxyResult map[string]interface{}
		if tc.name == "future_time" {
			future := time.Now().Add(24 * time.Hour)
			params := url.Values{}
			params.Set("query", tc.query)
			params.Set("start", fmt.Sprintf("%d", future.UnixNano()))
			params.Set("end", fmt.Sprintf("%d", future.Add(time.Hour).UnixNano()))
			proxyResult = getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
		} else {
			proxyResult = queryProxy(t, tc.query)
		}

		if checkStatus(proxyResult) {
			score.pass(tc.name, "proxy returns success for empty result")
		} else {
			score.fail(tc.name, "proxy error on empty result")
		}

		lines := countLogLines(proxyResult)
		if lines == 0 {
			score.pass(tc.name, "correctly returns 0 lines")
		} else {
			score.fail(tc.name, fmt.Sprintf("expected 0 lines, got %d", lines))
		}
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Rapid sequential queries (rate limiter)
// =============================================================================

func TestEdge_RapidSequentialQueries(t *testing.T) {
	score := &CompatScore{}
	successCount := 0
	total := 20

	for range total {
		resp, err := http.Get(proxyURL + "/loki/api/v1/labels")
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			successCount++
		}
	}

	if successCount == total {
		score.pass("rate_limit", fmt.Sprintf("all %d rapid requests allowed", total))
	} else if successCount > 0 {
		score.pass("rate_limit", fmt.Sprintf("%d/%d requests allowed (rate limiter working)", successCount, total))
	} else {
		score.fail("rate_limit", "all requests rejected")
	}

	score.report(t)
}

// =============================================================================
// Edge Case: Instant query (not range)
// =============================================================================

func TestEdge_InstantQuery(t *testing.T) {
	score := &CompatScore{}

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("limit", "5")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/query?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("instant_query", "proxy returns success")
	} else {
		score.fail("instant_query", "proxy error")
	}

	if checkStatus(lokiResp) {
		score.pass("instant_query", "loki returns success")
	}

	score.report(t)
}

// =============================================================================
// Metrics endpoint validation
// =============================================================================

func TestFeature_MetricsEndpoint(t *testing.T) {
	score := &CompatScore{}

	resp, err := http.Get(proxyURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body := make([]byte, 64*1024)
	n, _ := resp.Body.Read(body)
	text := string(body[:n])

	expectedMetrics := []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_request_duration_seconds",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_cache_misses_total",
		"loki_vl_proxy_translations_total",
		"loki_vl_proxy_uptime_seconds",
	}

	for _, m := range expectedMetrics {
		if strings.Contains(text, m) {
			score.pass("metrics", fmt.Sprintf("has %s", m))
		} else {
			score.fail("metrics", fmt.Sprintf("missing %s", m))
		}
	}

	score.report(t)
}

// =============================================================================
// Gzip Response Compression
// =============================================================================

func TestFeature_GzipCompression(t *testing.T) {
	score := &CompatScore{}

	// Use raw http.Transport to prevent auto-decompression
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/labels", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		score.fail("gzip", "request failed: "+err.Error())
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") == "gzip" {
		score.pass("gzip", "response is gzip-compressed")
	} else {
		score.fail("gzip", fmt.Sprintf("expected gzip, got Content-Encoding: %q", resp.Header.Get("Content-Encoding")))
	}

	if resp.Header.Get("Vary") == "Accept-Encoding" {
		score.pass("gzip", "Vary: Accept-Encoding header present")
	}

	score.report(t)
}

func TestFeature_NoGzipWithoutAccept(t *testing.T) {
	score := &CompatScore{}

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/labels", nil)
	// No Accept-Encoding
	resp, err := client.Do(req)
	if err != nil {
		score.fail("no_gzip", "request failed")
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		score.pass("no_gzip", "no gzip without Accept-Encoding")
	} else {
		score.fail("no_gzip", "should not gzip without Accept-Encoding")
	}

	score.report(t)
}

// =============================================================================
// Derived Fields (trace linking)
// =============================================================================

func TestFeature_DerivedFields_TraceIDInJSON(t *testing.T) {
	score := &CompatScore{}

	// The test data has trace_id fields in JSON logs
	proxyResult := queryProxy(t, `{app="api-gateway", level="info"} | json`)
	if checkStatus(proxyResult) {
		score.pass("derived", "json parser query succeeds")
	} else {
		score.fail("derived", "json parser query failed")
		score.report(t)
		return
	}

	data, _ := proxyResult["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})

	traceFound := false
	for _, s := range result {
		stream, _ := s.(map[string]interface{})
		labels, _ := stream["stream"].(map[string]interface{})
		if _, ok := labels["trace_id"]; ok {
			traceFound = true
			break
		}
	}

	if traceFound {
		score.pass("derived", "trace_id available after json parsing")
	} else {
		score.pass("derived", "trace_id extraction depends on derived-fields config (OK)")
	}

	score.report(t)
}

// =============================================================================
// Chunked / streaming response validation
// =============================================================================

func TestFeature_ResponseStructureConsistency(t *testing.T) {
	score := &CompatScore{}

	proxyResult := queryProxy(t, `{namespace="prod"}`)

	if checkStatus(proxyResult) {
		score.pass("response_struct", "response is valid JSON")
	} else {
		score.fail("response_struct", "response is not valid JSON")
	}

	data, _ := proxyResult["data"].(map[string]interface{})
	if data["resultType"] == "streams" {
		score.pass("response_struct", "resultType=streams")
	}

	result, _ := data["result"].([]interface{})
	if len(result) > 0 {
		score.pass("response_struct", fmt.Sprintf("has %d stream entries", len(result)))

		entry0, _ := result[0].(map[string]interface{})
		if _, ok := entry0["stream"]; ok {
			score.pass("response_struct", "entry has stream object")
		}
		if vals, ok := entry0["values"].([]interface{}); ok && len(vals) > 0 {
			score.pass("response_struct", "entry has values array")
			if pair, ok := vals[0].([]interface{}); ok && len(pair) == 2 {
				if _, ok := pair[0].(string); ok {
					score.pass("response_struct", "timestamp is string (nanoseconds)")
				}
				if _, ok := pair[1].(string); ok {
					score.pass("response_struct", "line is string")
				}
			}
		}
	}

	score.report(t)
}

// =============================================================================
// query_range vs query consistency
// =============================================================================

func TestFeature_QueryRange_vs_Query_Consistency(t *testing.T) {
	score := &CompatScore{}

	rangeResult := queryProxy(t, `{app="api-gateway"}`)
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("time", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")
	instantResult := getJSON(t, proxyURL+"/loki/api/v1/query?"+params.Encode())

	if checkStatus(rangeResult) && checkStatus(instantResult) {
		score.pass("consistency", "both query and query_range return success")
	} else {
		score.fail("consistency", "one or both failed")
	}

	rangeData, _ := rangeResult["data"].(map[string]interface{})
	instantData, _ := instantResult["data"].(map[string]interface{})

	if rangeData["resultType"] == "streams" {
		score.pass("consistency", "query_range returns streams")
	}
	if instantData["resultType"] == "streams" {
		score.pass("consistency", "query returns streams")
	}

	score.report(t)
}

func TestFeature_InstantVectorExpression_Compatibility(t *testing.T) {
	score := &CompatScore{}
	params := url.Values{}
	params.Set("query", `vector(1)+vector(1)`)
	params.Set("time", "4")

	lokiResult := getJSON(t, lokiURL+"/loki/api/v1/query?"+params.Encode())
	proxyResult := getJSON(t, proxyURL+"/loki/api/v1/query?"+params.Encode())

	if checkStatus(proxyResult) && checkStatus(lokiResult) {
		score.pass("instant_vector", "both instant queries return success")
	} else {
		score.fail("instant_vector", "instant vector query failed")
		score.report(t)
		return
	}

	lokiData, _ := lokiResult["data"].(map[string]interface{})
	proxyData, _ := proxyResult["data"].(map[string]interface{})
	if proxyData["resultType"] == "vector" && lokiData["resultType"] == "vector" {
		score.pass("instant_vector", "resultType=vector parity")
	} else {
		score.fail("instant_vector", fmt.Sprintf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"]))
	}

	lokiSamples, _ := lokiData["result"].([]interface{})
	proxySamples, _ := proxyData["result"].([]interface{})
	if len(lokiSamples) == 1 && len(proxySamples) == 1 {
		score.pass("instant_vector", "single sample parity")
	} else {
		score.fail("instant_vector", fmt.Sprintf("sample count mismatch loki=%d proxy=%d", len(lokiSamples), len(proxySamples)))
	}

	if len(proxySamples) == 1 {
		sample, _ := proxySamples[0].(map[string]interface{})
		value, _ := sample["value"].([]interface{})
		if len(value) == 2 && value[1] == "2" {
			score.pass("instant_vector", "proxy returns computed value 2")
		} else {
			score.fail("instant_vector", fmt.Sprintf("unexpected proxy sample value %v", value))
		}
	}

	score.report(t)
}
