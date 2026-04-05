//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestLokiTrackScore(t *testing.T) {
	ensureDataIngested(t)
	ensureOTelData(t)
	score := &CompatScore{}
	now := time.Now()

	labelsResp := getJSON(t, proxyURL+"/loki/api/v1/labels")
	labels := extractStrings(labelsResp, "data")
	if checkStatus(labelsResp) {
		score.pass("labels", "proxy labels endpoint returns success")
	} else {
		score.fail("labels", "proxy labels endpoint does not return success")
	}
	for _, want := range []string{"app", "namespace", "service_name"} {
		if contains(labels, want) {
			score.pass("labels", fmt.Sprintf("proxy exposes %s", want))
		} else {
			score.fail("labels", fmt.Sprintf("proxy missing %s", want))
		}
	}

	clusterVals := getJSON(t, proxyURL+"/loki/api/v1/label/cluster/values?query=%7Bservice_name%3D%22api-gateway%22%7D")
	values := extractStrings(clusterVals, "data")
	for _, want := range []string{"us-east-1", "us-west-2"} {
		if contains(values, want) {
			score.pass("label_values", fmt.Sprintf("cluster value %s present", want))
		} else {
			score.fail("label_values", fmt.Sprintf("cluster value %s missing", want))
		}
	}

	streams := queryRange(t, proxyURL, `{service_name="api-gateway"} | json | status >= 400`)
	if len(streams) > 0 {
		score.pass("query_range", "json parser and numeric field filter return results")
	} else {
		score.fail("query_range", "json parser and numeric field filter returned no results")
	}

	params := url.Values{}
	params.Set("query", `sum by (detected_level) (count_over_time({service_name="api-gateway"} | json | logfmt | drop __error__, __error_details__ [1m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-2*time.Hour).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")
	levelResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	levelData := extractMap(levelResp, "data")
	levelResult := extractArray(levelData, "result")
	foundInfo := false
	foundError := false
	for _, item := range levelResult {
		metric := item.(map[string]interface{})["metric"].(map[string]interface{})
		switch metric["detected_level"] {
		case "info":
			foundInfo = true
		case "error":
			foundError = true
		}
	}
	if foundInfo {
		score.pass("metrics", "detected_level=info series exists")
	} else {
		score.fail("metrics", "detected_level=info series missing")
	}
	if foundError {
		score.pass("metrics", "detected_level=error series exists")
	} else {
		score.fail("metrics", "detected_level=error series missing")
	}

	otelStreams := queryRange(t, proxyUnderscoreURL, `{service_name="otel-auth-service"}`)
	if len(otelStreams) > 0 {
		score.pass("otel", "underscore proxy resolves OTel service_name selector")
	} else {
		score.fail("otel", "underscore proxy failed OTel service_name selector")
	}

	seriesParams := url.Values{}
	seriesParams.Add("match[]", `{service_name="api-gateway"}`)
	seriesResp := getJSON(t, proxyURL+"/loki/api/v1/series?"+seriesParams.Encode())
	series := extractArray(seriesResp, "data")
	if len(series) > 0 {
		score.pass("series", "series endpoint returns translated labels")
	} else {
		score.fail("series", "series endpoint returned no translated labels")
	}

	score.report(t)
}

func TestDrilldownTrackScore(t *testing.T) {
	ensureDataIngested(t)
	ingestPatternData(t)
	score := &CompatScore{}
	now := time.Now()
	start := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	end := now.Format(time.RFC3339Nano)
	dsUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")

	volumeParams := url.Values{}
	volumeParams.Set("query", "{service_name=~`.+`}")
	volumeParams.Set("start", start)
	volumeParams.Set("end", end)
	volumeParams.Set("targetLabels", "service_name")
	volumeResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume?"+volumeParams.Encode())
	volumeData := extractMap(volumeResp, "data")
	volumeResult := extractArray(volumeData, "result")
	foundService := false
	for _, item := range volumeResult {
		metric := item.(map[string]interface{})["metric"].(map[string]interface{})
		if metric["service_name"] == "api-gateway" {
			foundService = true
			break
		}
	}
	if foundService {
		score.pass("service_selection", "service volume lists api-gateway")
	} else {
		score.fail("service_selection", "service volume missing api-gateway")
	}

	levelParams := url.Values{}
	levelParams.Set("query", `{service_name="api-gateway"}`)
	levelParams.Set("start", start)
	levelParams.Set("end", end)
	levelParams.Set("step", "60")
	levelParams.Set("targetLabels", "detected_level")
	levelResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume_range?"+levelParams.Encode())
	levelData := extractMap(levelResp, "data")
	levelResult := extractArray(levelData, "result")
	levelInfo := false
	levelError := false
	for _, item := range levelResult {
		metric := item.(map[string]interface{})["metric"].(map[string]interface{})
		switch metric["detected_level"] {
		case "info":
			levelInfo = true
		case "error":
			levelError = true
		}
	}
	if levelInfo {
		score.pass("level_volume", "detected_level info series present")
	} else {
		score.fail("level_volume", "detected_level info series missing")
	}
	if levelError {
		score.pass("level_volume", "detected_level error series present")
	} else {
		score.fail("level_volume", "detected_level error series missing")
	}

	fieldsParams := url.Values{}
	fieldsParams.Set("query", `{service_name="api-gateway"}`)
	fieldsParams.Set("start", start)
	fieldsParams.Set("end", end)
	fieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?"+fieldsParams.Encode())
	fields, _ := fieldsResp["fields"].([]interface{})
	seen := map[string]bool{}
	for _, field := range fields {
		seen[field.(map[string]interface{})["label"].(string)] = true
	}
	for _, want := range []string{"method", "path", "status", "duration_ms"} {
		if seen[want] {
			score.pass("detected_fields", fmt.Sprintf("field %s present", want))
		} else {
			score.fail("detected_fields", fmt.Sprintf("field %s missing", want))
		}
	}
	for _, forbidden := range []string{"app", "cluster", "namespace", "service_name", "duration_ms_extracted"} {
		if seen[forbidden] {
			score.fail("detected_fields", fmt.Sprintf("forbidden field %s leaked into drilldown", forbidden))
		} else {
			score.pass("detected_fields", fmt.Sprintf("forbidden field %s absent", forbidden))
		}
	}

	otelFieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?query=%7Bservice_name%3D%22otel-auth-service%22%7D&start="+url.QueryEscape(start)+"&end="+url.QueryEscape(end))
	otelFields, _ := otelFieldsResp["fields"].([]interface{})
	otelSeen := map[string]bool{}
	for _, field := range otelFields {
		otelSeen[field.(map[string]interface{})["label"].(string)] = true
	}
	for _, want := range []string{"service.name", "service_name"} {
		if otelSeen[want] {
			score.pass("detected_fields", fmt.Sprintf("hybrid field %s present", want))
		} else {
			score.fail("detected_fields", fmt.Sprintf("hybrid field %s missing", want))
		}
	}

	clusterVals := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/cluster/values?query=%7Bservice_name%3D%22api-gateway%22%7D&start="+url.QueryEscape(start)+"&end="+url.QueryEscape(end))
	clusterData := extractStrings(clusterVals, "data")
	if contains(clusterData, "us-east-1") {
		score.pass("label_values", "cluster label values include us-east-1")
	} else {
		score.fail("label_values", "cluster label values missing us-east-1")
	}

	streams := queryRange(t, proxyURL, `{service_name="api-gateway"}`)
	if len(streams) > 0 {
		score.pass("service_logs", "service detail query returns log streams")
	} else {
		score.fail("service_logs", "service detail query returned no log streams")
	}

	patternParams := url.Values{}
	patternParams.Set("query", `{app="pattern-test"}`)
	patternParams.Set("start", start)
	patternParams.Set("end", end)
	patternParams.Set("step", "60s")
	patternResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/patterns?"+patternParams.Encode())
	patterns, _ := patternResp["data"].([]interface{})
	if len(patterns) > 0 {
		score.pass("patterns", "patterns resource returns grouped Drilldown patterns")
	} else {
		score.fail("patterns", "patterns resource returned no grouped patterns")
	}

	score.report(t)
}

func TestVLTrackScore(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	streams := queryRange(t, proxyURL, `{service_name="api-gateway"}`)
	if len(streams) > 0 {
		score.pass("stream_translation", "query_range returns translated streams from VictoriaLogs")
	} else {
		score.fail("stream_translation", "query_range returned no translated streams")
	}

	rawFields := getDetectedFieldsForQuery(t, proxyURL, `{service_name="api-gateway"}`)
	for _, want := range []string{"method", "path", "status", "duration_ms"} {
		if contains(rawFields, want) {
			score.pass("detected_fields", fmt.Sprintf("VictoriaLogs field %s exposed via proxy", want))
		} else {
			score.fail("detected_fields", fmt.Sprintf("VictoriaLogs field %s missing via proxy", want))
		}
	}

	serviceVals := getJSON(t, proxyURL+"/loki/api/v1/label/service_name/values?query=*")
	serviceNames := extractStrings(serviceVals, "data")
	if contains(serviceNames, "api-gateway") {
		score.pass("synthetic_labels", "derived service_name label values are exposed")
	} else {
		score.fail("synthetic_labels", "derived service_name label values missing")
	}

	indexStats := getJSON(t, proxyURL+"/loki/api/v1/index/stats?query=%7Bservice_name%3D%22api-gateway%22%7D")
	if indexStats["entries"] != nil && indexStats["streams"] != nil {
		score.pass("index_stats", "index stats endpoint returns Loki-shaped data")
	} else {
		score.fail("index_stats", "index stats endpoint missing data payload")
	}

	volParams := url.Values{}
	volParams.Set("query", `{service_name="api-gateway"}`)
	volParams.Set("start", fmt.Sprintf("%d", now.Add(-2*time.Hour).UnixNano()))
	volParams.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	volParams.Set("step", "60")
	volRange := getJSON(t, proxyURL+"/loki/api/v1/index/volume_range?"+volParams.Encode())
	volData := extractMap(volRange, "data")
	volResult := extractArray(volData, "result")
	if len(volResult) > 0 {
		score.pass("volume_range", "volume_range is backed by VictoriaLogs hits data")
	} else {
		score.fail("volume_range", "volume_range returned no series")
	}

	methods := getDetectedFieldValuesForQuery(t, proxyURL, "method", `{service_name="api-gateway"}`)
	for _, want := range []string{"GET", "POST", "DELETE"} {
		if contains(methods, want) {
			score.pass("field_values", fmt.Sprintf("field value %s present", want))
		} else {
			score.fail("field_values", fmt.Sprintf("field value %s missing", want))
		}
	}

	score.report(t)
}
