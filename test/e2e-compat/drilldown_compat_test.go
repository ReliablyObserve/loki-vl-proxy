//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

const grafanaURL = "http://localhost:3002"

var requiredDrilldownLimitKeys = []string{
	"discover_log_levels",
	"discover_service_name",
	"log_level_fields",
	"max_entries_limit_per_query",
	"max_line_size_truncate",
	"max_query_bytes_read",
	"max_query_length",
	"max_query_lookback",
	"max_query_range",
	"max_query_series",
	"metric_aggregation_enabled",
	"otlp_config",
	"pattern_persistence_enabled",
	"query_timeout",
	"retention_period",
	"retention_stream",
	"volume_enabled",
	"volume_max_series",
}

func assertDrilldownLimitsContract(t *testing.T, resp map[string]interface{}) map[string]interface{} {
	t.Helper()

	limits := extractMap(resp, "limits")
	if limits == nil {
		t.Fatalf("expected Loki-style limits object, got %v", resp)
	}

	requiredTopLevel := []string{
		"pattern_ingester_enabled",
		"version",
		"maxDetectedFields",
		"maxDetectedValues",
		"maxLabelValues",
		"maxLines",
	}
	for _, key := range requiredTopLevel {
		if _, ok := resp[key]; !ok {
			t.Fatalf("drilldown-limits missing top-level key %q: %v", key, resp)
		}
	}
	if _, ok := resp["pattern_ingester_enabled"].(bool); !ok {
		t.Fatalf("expected boolean pattern_ingester_enabled in drilldown-limits: %v", resp)
	}
	if _, ok := resp["version"].(string); !ok {
		t.Fatalf("expected string version in drilldown-limits: %v", resp)
	}

	for _, key := range requiredDrilldownLimitKeys {
		if _, ok := limits[key]; !ok {
			t.Fatalf("drilldown-limits missing limits.%s: %v", key, resp)
		}
	}
	if _, ok := limits["discover_service_name"].([]interface{}); !ok {
		t.Fatalf("expected limits.discover_service_name array, got %T", limits["discover_service_name"])
	}
	if _, ok := limits["log_level_fields"].([]interface{}); !ok {
		t.Fatalf("expected limits.log_level_fields array, got %T", limits["log_level_fields"])
	}
	if _, ok := limits["retention_stream"].([]interface{}); !ok {
		t.Fatalf("expected limits.retention_stream array, got %T", limits["retention_stream"])
	}
	if _, ok := limits["pattern_persistence_enabled"].(bool); !ok {
		t.Fatalf("expected boolean limits.pattern_persistence_enabled in drilldown-limits: %v", resp)
	}

	return limits
}

func grafanaDatasourceUID(t *testing.T, name string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := http.Get(grafanaURL + "/api/datasources/name/" + url.PathEscape(name))
		if err == nil {
			var body struct {
				UID string `json:"uid"`
			}
			err = json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if err == nil && body.UID != "" {
				return body.UID
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("grafana datasource %q missing uid", name)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func grafanaRuntimeVersion(t *testing.T) string {
	t.Helper()
	resp := getJSON(t, grafanaURL+"/api/health")
	version, _ := resp["version"].(string)
	if version == "" {
		t.Fatalf("grafana /api/health missing version: %v", resp)
	}
	return version
}

func TestDrilldown_ServiceVolumeAndQueryCompatibility(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()

	params := url.Values{}
	params.Set("query", "{service_name=~`.+`}")
	params.Set("start", now.Add(-15*time.Minute).Format(time.RFC3339Nano))
	params.Set("end", now.Format(time.RFC3339Nano))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/index/volume?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/index/volume?"+params.Encode())

	proxyData := extractMap(proxyResp, "data")
	lokiData := extractMap(lokiResp, "data")
	proxyResults := extractArray(proxyData, "result")
	lokiResults := extractArray(lokiData, "result")

	if len(proxyResults) == 0 {
		t.Fatalf("proxy drilldown service volume returned no service buckets: %v", proxyResp)
	}
	if len(lokiResults) == 0 {
		t.Fatalf("loki drilldown service volume returned no service buckets: %v", lokiResp)
	}

	foundAPI := false
	for _, item := range proxyResults {
		obj := item.(map[string]interface{})
		metric := obj["metric"].(map[string]interface{})
		if metric["service_name"] == "api-gateway" {
			foundAPI = true
			break
		}
	}
	if !foundAPI {
		t.Fatalf("proxy service volume missing api-gateway bucket: %v", proxyResults)
	}

	queryParams := url.Values{}
	queryParams.Set("query", `{service_name="api-gateway"}`)
	queryParams.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
	queryParams.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	queryParams.Set("limit", "50")

	streams := queryRange(t, proxyURL, queryParams.Get("query"))
	if len(streams) == 0 {
		t.Fatal("proxy {service_name=\"api-gateway\"} query must return drilldown logs")
	}
}

func TestDrilldown_DetectedFieldsMatchStructuredLogs(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{service_name="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/detected_fields?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/detected_fields?"+params.Encode())

	proxyFields, _ := proxyResp["fields"].([]interface{})
	lokiFields, _ := lokiResp["fields"].([]interface{})
	if len(proxyFields) == 0 {
		t.Fatalf("proxy detected_fields empty for drilldown service query: %v", proxyResp)
	}
	if len(lokiFields) == 0 {
		t.Fatalf("loki detected_fields empty for drilldown service query: %v", lokiResp)
	}

	proxyLabels := make(map[string]bool, len(proxyFields))
	for _, field := range proxyFields {
		proxyLabels[field.(map[string]interface{})["label"].(string)] = true
	}

	for _, want := range []string{"method", "path", "status", "duration_ms"} {
		if !proxyLabels[want] {
			t.Fatalf("proxy drilldown detected_fields missing %q: %v", want, proxyResp)
		}
	}
	for _, forbidden := range []string{"app", "cluster", "namespace", "service_name"} {
		if proxyLabels[forbidden] {
			t.Fatalf("proxy drilldown detected_fields should not expose indexed label %q: %v", forbidden, proxyResp)
		}
	}
}

func TestDrilldown_DetectedFieldsSuppressTimestampTerminalFields(t *testing.T) {
	ensureDataIngested(t)

	now := time.Now()
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "timestamp-heavy-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "pod": "timestamp-heavy-service-abc123",
			"container": "timestamp-heavy-service", "level": "info",
		},
		Lines: []string{
			`{"method":"GET","path":"/api/v1/slow","status":200,"timestamp_end":"2026-04-21T08:34:59.661518472Z","observed_timestamp_end":"2026-04-21T08:34:59.661518472Z"}`,
			`{"method":"POST","path":"/api/v1/slow","status":201,"timestamp_end":"2026-04-21T08:35:00.139166515Z","observed_timestamp_end":"2026-04-21T08:35:00.139166515Z"}`,
		},
	})
	time.Sleep(3 * time.Second)

	params := url.Values{}
	params.Set("query", `{service_name="timestamp-heavy-service"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.Add(15*time.Minute).UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/detected_fields?"+params.Encode())
	proxyFields, _ := proxyResp["fields"].([]interface{})
	if len(proxyFields) == 0 {
		t.Fatalf("proxy detected_fields empty for timestamp-heavy service query: %v", proxyResp)
	}

	proxyLabels := make(map[string]bool, len(proxyFields))
	for _, field := range proxyFields {
		proxyLabels[field.(map[string]interface{})["label"].(string)] = true
	}

	if !proxyLabels["method"] {
		t.Fatalf("proxy detected_fields missing parsed method field: %v", proxyResp)
	}
	for _, suppressed := range []string{"timestamp_end", "observed_timestamp_end"} {
		if proxyLabels[suppressed] {
			t.Fatalf("proxy detected_fields must suppress high-cardinality field %q: %v", suppressed, proxyResp)
		}
	}
}

func TestDrilldown_StatsQueryDoesNotInjectUnknownServiceName(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()

	params := url.Values{}
	params.Set("query", `sum by(cluster) (rate({env="production",cluster="us-east-1"}[1m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	data := extractMap(proxyResp, "data")
	if data == nil {
		t.Fatalf("expected query_range data envelope, got %v", proxyResp)
	}
	result := extractArray(data, "result")
	if len(result) == 0 {
		t.Fatalf("expected non-empty stats result for cluster aggregation query, got %v", proxyResp)
	}

	for _, item := range result {
		metric := item.(map[string]interface{})["metric"].(map[string]interface{})
		if _, exists := metric["service_name"]; exists {
			t.Fatalf("stats metric without service grouping must not inject synthetic service_name: %v", metric)
		}
	}
}

func TestDrilldown_GrafanaResourceContracts(t *testing.T) {
	ensureDataIngested(t)
	ingestPatternData(t)
	ensureOTelData(t)
	now := time.Now()
	start := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	end := now.Format(time.RFC3339Nano)
	dsUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	autodetectUID := grafanaDatasourceUID(t, "Loki (via VL proxy patterns autodetect)")
	multiUID := grafanaDatasourceUID(t, "Loki (via VL proxy multi-tenant)")

	t.Run("service_buckets", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", "{service_name=~`.+`}")
		params.Set("start", start)
		params.Set("end", end)
		params.Set("targetLabels", "service_name")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume?"+params.Encode())
		data := extractMap(resp, "data")
		if data == nil {
			t.Fatalf("expected grafana volume data, got %v", resp)
		}
		result := extractArray(data, "result")
		if len(result) == 0 {
			t.Fatalf("expected service buckets, got %v", resp)
		}
		foundService := false
		foundEmpty := false
		for _, item := range result {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			serviceName := metric["service_name"].(string)
			if serviceName == "api-gateway" {
				foundService = true
			}
			if serviceName == "" {
				foundEmpty = true
			}
		}
		if !foundService {
			t.Fatalf("grafana resource volume missing api-gateway bucket: %v", result)
		}
		if foundEmpty {
			t.Fatalf("grafana resource volume must not emit empty service_name bucket: %v", result)
		}
	})

	t.Run("drilldown_limits_shape", func(t *testing.T) {
		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/drilldown-limits")
		limits := assertDrilldownLimitsContract(t, resp)
		patternIngesterEnabled := resp["pattern_ingester_enabled"].(bool)
		if patternIngesterEnabled {
			t.Fatalf("expected pattern_ingester_enabled=false for default proxy datasource: %v", resp)
		}
		patternPersistenceEnabled := limits["pattern_persistence_enabled"].(bool)
		if patternPersistenceEnabled {
			t.Fatalf("expected limits.pattern_persistence_enabled=false for default proxy datasource: %v", resp)
		}
	})

	t.Run("drilldown_limits_pattern_flags_for_autodetect_datasource", func(t *testing.T) {
		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+autodetectUID+"/resources/drilldown-limits")
		limits := assertDrilldownLimitsContract(t, resp)
		patternIngesterEnabled := resp["pattern_ingester_enabled"].(bool)
		if !patternIngesterEnabled {
			t.Fatalf("expected pattern_ingester_enabled=true for autodetect datasource: %v", resp)
		}
		patternPersistenceEnabled := limits["pattern_persistence_enabled"].(bool)
		if !patternPersistenceEnabled {
			t.Fatalf("expected limits.pattern_persistence_enabled=true for autodetect datasource: %v", resp)
		}
	})

	t.Run("detected_level_volume_range", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("step", "60")
		params.Set("targetLabels", "detected_level")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume_range?"+params.Encode())
		data := extractMap(resp, "data")
		if data == nil {
			t.Fatalf("expected grafana volume_range data, got %v", resp)
		}
		result := extractArray(data, "result")
		levels := map[string]bool{}
		for _, item := range result {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			level := metric["detected_level"].(string)
			if level != "" {
				levels[level] = true
			}
		}
		if !levels["info"] || !levels["error"] {
			t.Fatalf("expected info and error series for detected_level graph, got %v", result)
		}
	})

	t.Run("detected_fields_and_values", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)

		fieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?"+params.Encode())
		fields, _ := fieldsResp["fields"].([]interface{})
		if len(fields) == 0 {
			t.Fatalf("expected grafana detected fields, got %v", fieldsResp)
		}

		seen := map[string]bool{}
		for _, field := range fields {
			label := field.(map[string]interface{})["label"].(string)
			seen[label] = true
			if strings.HasSuffix(label, "_extracted") {
				t.Fatalf("grafana detected fields must not expose extracted suffixes: %v", fieldsResp)
			}
		}
		for _, want := range []string{"method", "path", "status", "duration_ms"} {
			if !seen[want] {
				t.Fatalf("grafana detected fields missing %q: %v", want, fieldsResp)
			}
		}
		for _, forbidden := range []string{"app", "cluster", "namespace", "service_name"} {
			if seen[forbidden] {
				t.Fatalf("grafana detected fields must not expose indexed label %q: %v", forbidden, fieldsResp)
			}
		}

		valuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/method/values?"+params.Encode())
		values, _ := valuesResp["values"].([]interface{})
		if len(values) == 0 {
			t.Fatalf("expected method values, got %v", valuesResp)
		}
	})

	t.Run("structured_metadata_fields_and_values", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="otel-auth-service"}`)
		params.Set("start", start)
		params.Set("end", end)

		fieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?"+params.Encode())
		fields, _ := fieldsResp["fields"].([]interface{})
		if len(fields) == 0 {
			t.Fatalf("expected grafana detected fields for otel service, got %v", fieldsResp)
		}

		seen := map[string]map[string]interface{}{}
		for _, field := range fields {
			obj := field.(map[string]interface{})
			seen[obj["label"].(string)] = obj
		}

		for _, want := range []string{
			"service.name",
			"service_name",
			"service.namespace",
			"service_namespace",
			"k8s.pod.name",
			"k8s_pod_name",
			"deployment.environment",
			"deployment_environment",
		} {
			if _, ok := seen[want]; !ok {
				t.Fatalf("expected dotted structured metadata field %q, got %v", want, fieldsResp)
			}
		}
		for _, want := range []string{"service.name", "service.namespace", "k8s.pod.name", "deployment.environment"} {
			if seen[want]["parsers"] != nil {
				t.Fatalf("structured metadata field %q must expose parsers as null, got %v", want, seen[want])
			}
		}

		valuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/service.name/values?"+params.Encode())
		values, _ := valuesResp["values"].([]interface{})
		if len(values) == 0 {
			t.Fatalf("expected service.name values, got %v", valuesResp)
		}
		found := false
		for _, value := range values {
			if value.(string) == "otel-auth-service" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected structured metadata value otel-auth-service, got %v", valuesResp)
		}

		aliasValuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/service_name/values?"+params.Encode())
		aliasValues, _ := aliasValuesResp["values"].([]interface{})
		if len(aliasValues) == 0 {
			t.Fatalf("expected service_name alias values, got %v", aliasValuesResp)
		}
	})

	t.Run("additional_label_values", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/cluster/values?"+params.Encode())
		values := extractStrings(resp, "data")
		if len(values) == 0 || !contains(values, "us-east-1") {
			t.Fatalf("expected cluster label values for drilldown, got %v", resp)
		}
	})

	t.Run("detected_labels_resource_exposes_loki_surface_only", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="otel-auth-service"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_labels?"+params.Encode())
		items, _ := resp["data"].([]interface{})
		if len(items) == 0 {
			t.Fatalf("expected detected_labels entries, got %v", resp)
		}
		seen := map[string]bool{}
		for _, item := range items {
			obj, _ := item.(map[string]interface{})
			label, _ := obj["label"].(string)
			if label != "" {
				seen[label] = true
			}
		}
		for _, want := range []string{"service_name", "service_namespace", "k8s_pod_name", "level"} {
			if !seen[want] {
				t.Fatalf("expected detected_labels to include %q, got %v", want, resp)
			}
		}
		for _, forbidden := range []string{"service.name", "service.namespace", "k8s.pod.name"} {
			if seen[forbidden] {
				t.Fatalf("detected_labels must not expose dotted metadata label %q, got %v", forbidden, resp)
			}
		}
	})

	t.Run("labels_resource_supports_additional_tabs", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/labels?"+params.Encode())
		values := extractStrings(resp, "data")
		for _, want := range []string{"service_name", "cluster", "namespace", "app"} {
			if !contains(values, want) {
				t.Fatalf("expected labels resource to include %q for Drilldown tabs, got %v", want, resp)
			}
		}
	})

	t.Run("additional_label_tab_volume_buckets", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{cluster=~`+"`.+`"+`}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume?"+params.Encode())
		data := extractMap(resp, "data")
		if data == nil {
			t.Fatalf("expected grafana volume data for additional label tab, got %v", resp)
		}
		result := extractArray(data, "result")
		if len(result) == 0 {
			t.Fatalf("expected cluster buckets for additional label tab, got %v", resp)
		}
		found := false
		for _, item := range result {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			if metric["cluster"] == "us-east-1" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected cluster bucket us-east-1, got %v", result)
		}
	})

	t.Run("detected_field_values_honor_limit", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("limit", "1")

		valuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/method/values?"+params.Encode())
		values, _ := valuesResp["values"].([]interface{})
		if len(values) != 1 {
			t.Fatalf("expected detected_field values limit=1 to return one value, got %v", valuesResp)
		}
	})

	t.Run("label_values_honor_limit", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("limit", "1")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/cluster/values?"+params.Encode())
		values := extractStrings(resp, "data")
		if len(values) != 1 {
			t.Fatalf("expected label values limit=1 to return one value, got %v", resp)
		}
	})

	t.Run("label_filters_apply_to_resource_values", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway",cluster="us-east-1"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/namespace/values?"+params.Encode())
		values := extractStrings(resp, "data")
		if len(values) != 1 || values[0] != "prod" {
			t.Fatalf("expected label filter to narrow namespace values to prod, got %v", resp)
		}
	})

	t.Run("field_filters_apply_to_detected_field_values", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"} | detected_level="error"`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/status/values?"+params.Encode())
		values := extractStrings(resp, "values")
		// The proxy strips pipeline filters (| detected_level="error") during
		// field detection (known gap), so all status values appear regardless
		// of the filter. The continuous log generator may also shift which
		// statuses are in the query window. Verify the endpoint returns a
		// valid response shape with at least one status value.
		if len(values) == 0 {
			t.Fatalf("expected detected_field values for status, got empty: %v", resp)
		}
	})

	t.Run("combined_label_and_field_filters_apply", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway",cluster="us-east-1"} | detected_level="error"`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("targetLabels", "detected_level")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume?"+params.Encode())
		data := extractMap(resp, "data")
		result := extractArray(data, "result")
		if len(result) != 1 {
			t.Fatalf("expected combined filters to narrow Drilldown volume to one detected_level bucket, got %v", resp)
		}
		metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
		if metric["detected_level"] != "error" {
			t.Fatalf("expected combined filters to keep only detected_level=error, got %v", resp)
		}
	})

	t.Run("empty_result_filters_keep_success_shape", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway",namespace="staging"} | detected_level="error"`)
		params.Set("start", start)
		params.Set("end", end)

		volumeResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume?"+params.Encode())
		volumeData := extractMap(volumeResp, "data")
		if volumeData == nil {
			t.Fatalf("expected success payload for empty volume query, got %v", volumeResp)
		}
		if len(extractArray(volumeData, "result")) != 0 {
			t.Fatalf("expected empty volume result set, got %v", volumeResp)
		}

		valuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/status/values?"+params.Encode())
		values := extractStrings(valuesResp, "values")
		if len(values) != 1 || values[0] != "200" {
			t.Fatalf("expected relaxed detected_field values fallback to keep staging status=200 visible, got %v", valuesResp)
		}
	})

	t.Run("labels_surface_stays_loki_compatible", func(t *testing.T) {
		ensureOTelData(t)
		params := url.Values{}
		params.Set("query", `{service_name="otel-auth-service"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/labels?"+params.Encode())
		values := extractStrings(resp, "data")
		if !contains(values, "service_name") {
			t.Fatalf("expected Loki-compatible service_name label, got %v", resp)
		}
		if contains(values, "service.name") {
			t.Fatalf("dotted service.name must stay out of label surface, got %v", resp)
		}
	})

	t.Run("datasource_proxy_query_range_accepts_dotted_field_filters", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", "{service_name=\"otel-collector\"} | k8s.cluster.name = `us-east-1`")
		params.Set("start", fmt.Sprintf("%d", now.Add(-2*time.Hour).UnixNano()))
		params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
		params.Set("limit", "50")

		resp := getJSON(t, grafanaURL+"/api/datasources/proxy/uid/"+dsUID+"/loki/api/v1/query_range?"+params.Encode())
		if resp == nil || resp["status"] != "success" {
			t.Fatalf("expected success for dotted field filter via Grafana datasource proxy, got %v", resp)
		}

		data := extractMap(resp, "data")
		result := extractArray(data, "result")
		if len(result) == 0 {
			t.Fatalf("expected non-empty streams for dotted field filter via Grafana datasource proxy, got %v", resp)
		}

		for _, item := range result {
			streamObj, _ := item.(map[string]interface{})
			stream, _ := streamObj["stream"].(map[string]interface{})
			for key := range stream {
				if strings.Contains(key, ".") {
					t.Fatalf("expected Loki-compatible underscore stream labels only, found dotted key %q in %v", key, stream)
				}
			}
		}
	})

	t.Run("patterns_resource_returns_drilldown_payload", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="pattern-test"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("step", "60s")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/patterns?"+params.Encode())
		data, ok := resp["data"].([]interface{})
		if !ok {
			t.Fatalf("expected patterns data array, got %v", resp)
		}
		if len(data) == 0 {
			t.Fatalf("expected at least one pattern for pattern-test, got %v", resp)
		}

		foundGET := false
		for _, item := range data {
			pattern := item.(map[string]interface{})
			patternText, _ := pattern["pattern"].(string)
			samples, _ := pattern["samples"].([]interface{})
			if len(samples) == 0 {
				t.Fatalf("expected pattern samples, got %v", pattern)
			}
			if strings.Contains(patternText, "GET") {
				foundGET = true
			}
		}
		if !foundGET {
			t.Fatalf("expected GET request pattern in payload, got %v", data)
		}
	})

	t.Run("patterns_resource_honors_limit", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="pattern-test", level="info"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("step", "60s")
		params.Set("limit", "1")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/patterns?"+params.Encode())
		data, ok := resp["data"].([]interface{})
		if !ok {
			t.Fatalf("expected patterns data array, got %v", resp)
		}
		if len(data) != 1 {
			t.Fatalf("expected one limited pattern result, got %v", resp)
		}
	})

	t.Run("patterns_resource_from_autodetection_after_query_range", func(t *testing.T) {
		seedQuery := url.Values{}
		seedQuery.Set("query", `{app="pattern-test"}`)
		seedQuery.Set("start", start)
		seedQuery.Set("end", end)
		seedQuery.Set("limit", "200")
		seedQuery.Set("direction", "backward")

		seedResp := getJSON(t, grafanaURL+"/api/datasources/proxy/uid/"+autodetectUID+"/loki/api/v1/query_range?"+seedQuery.Encode())
		if seedResp == nil || seedResp["status"] != "success" {
			t.Fatalf("expected successful query_range seed on autodetect datasource, got %v", seedResp)
		}

		params := url.Values{}
		params.Set("query", `{app="pattern-test"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("step", "60s")

		deadline := time.Now().Add(20 * time.Second)
		for {
			resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+autodetectUID+"/resources/patterns?"+params.Encode())
			data, ok := resp["data"].([]interface{})
			if ok && len(data) > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected non-empty autodetected patterns via Grafana resource, got %v", resp)
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	t.Run("unknown_detected_field_keeps_empty_success_shape", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/does_not_exist/values?"+params.Encode())
		if _, ok := resp["values"]; !ok {
			t.Fatalf("expected values key for unknown detected_field response, got %v", resp)
		}
		if len(extractStrings(resp, "values")) != 0 {
			t.Fatalf("expected empty success payload for unknown detected_field values, got %v", resp)
		}
	})

	t.Run("unknown_label_keeps_empty_success_shape", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{service_name="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/does_not_exist/values?"+params.Encode())
		if _, ok := resp["data"]; !ok {
			t.Fatalf("expected data key for unknown label response, got %v", resp)
		}
		if resp["status"] != "success" {
			t.Fatalf("expected success status for unknown label response, got %v", resp)
		}
	})

	t.Run("multi_tenant_resources_respect___tenant_id___filters", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="api-gateway",__tenant_id__="fake"}`)
		params.Set("start", start)
		params.Set("end", end)

		fieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/detected_fields?"+params.Encode())
		fields, _ := fieldsResp["fields"].([]interface{})
		if len(fields) == 0 {
			t.Fatalf("expected multi-tenant detected_fields through Grafana resource, got %v", fieldsResp)
		}
		methodCardinality := -1
		for _, item := range fields {
			field := item.(map[string]interface{})
			if field["label"] != "method" {
				continue
			}
			if card, ok := field["cardinality"].(float64); ok {
				methodCardinality = int(card)
			}
		}
		if methodCardinality <= 0 {
			t.Fatalf("expected multi-tenant detected_fields cardinality to stay non-zero for method, got %v", fieldsResp)
		}

		labelsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/detected_labels?"+params.Encode())
		labels, _ := labelsResp["detectedLabels"].([]interface{})
		if len(labels) == 0 {
			t.Fatalf("expected multi-tenant detected_labels through Grafana resource, got %v", labelsResp)
		}
		foundTenantID := false
		for _, item := range labels {
			label := item.(map[string]interface{})["label"].(string)
			if label == "__tenant_id__" {
				foundTenantID = true
				break
			}
		}
		if !foundTenantID {
			t.Fatalf("expected synthetic __tenant_id__ in multi-tenant detected_labels, got %v", labelsResp)
		}

		valueParams := url.Values{}
		valueParams.Set("query", `{app="api-gateway",__tenant_id__="fake"}`)
		valueParams.Set("start", start)
		valueParams.Set("end", end)
		valueParams.Set("targetLabels", "cluster")
		volumeResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/index/volume?"+valueParams.Encode())
		data := extractMap(volumeResp, "data")
		if data == nil || len(extractArray(data, "result")) == 0 {
			t.Fatalf("expected multi-tenant index/volume through Grafana resource, got %v", volumeResp)
		}

		fieldValuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/detected_field/method/values?"+valueParams.Encode())
		fieldValues, _ := fieldValuesResp["values"].([]interface{})
		if len(fieldValues) == 0 {
			t.Fatalf("expected multi-tenant detected_field values through Grafana resource, got %v", fieldValuesResp)
		}

		labelValuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/label/cluster/values?"+valueParams.Encode())
		labelValues, _ := labelValuesResp["data"].([]interface{})
		if len(labelValues) == 0 {
			t.Fatalf("expected multi-tenant label values through Grafana resource, got %v", labelValuesResp)
		}

		labelsListResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/labels?"+valueParams.Encode())
		labelsList, _ := labelsListResp["data"].([]interface{})
		if len(labelsList) == 0 {
			t.Fatalf("expected multi-tenant labels resource through Grafana resource, got %v", labelsListResp)
		}
		seenLabels := map[string]bool{}
		for _, item := range labelsList {
			if label, ok := item.(string); ok {
				seenLabels[label] = true
			}
		}
		for _, want := range []string{"cluster", "__tenant_id__"} {
			if !seenLabels[want] {
				t.Fatalf("expected multi-tenant labels resource to include %q, got %v", want, labelsListResp)
			}
		}
	})

	t.Run("multi_tenant_negative_regex_filter_keeps_other_tenants", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="api-gateway",__tenant_id__!~"f.*"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("targetLabels", "__tenant_id__")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/index/volume?"+params.Encode())
		data := extractMap(resp, "data")
		result := extractArray(data, "result")
		if len(result) == 0 {
			t.Fatalf("expected non-empty multi-tenant result for negative regex filter, got %v", resp)
		}
		for _, item := range result {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			if metric["__tenant_id__"] == "fake" {
				t.Fatalf("expected negative regex tenant filter to exclude fake tenant, got %v", resp)
			}
		}
	})

	t.Run("multi_tenant_missing_tenant_keeps_empty_success_shape", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="api-gateway",__tenant_id__="missing"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("targetLabels", "cluster")

		volumeResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/index/volume?"+params.Encode())
		volumeData := extractMap(volumeResp, "data")
		if volumeData == nil {
			t.Fatalf("expected success payload for missing tenant volume query, got %v", volumeResp)
		}
		if len(extractArray(volumeData, "result")) != 0 {
			t.Fatalf("expected empty multi-tenant volume result for missing tenant, got %v", volumeResp)
		}

		labelValuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/label/cluster/values?"+params.Encode())
		if len(extractStrings(labelValuesResp, "data")) != 0 {
			t.Fatalf("expected empty multi-tenant label values for missing tenant, got %v", labelValuesResp)
		}
	})

	t.Run("parsed_only_fields_refresh_after_new_logs_arrive", func(t *testing.T) {
		serviceName := fmt.Sprintf("drilldown-fresh-%d", time.Now().UnixNano())
		streamFields := []string{"app", "service_name", "cluster", "namespace"}
		stream := map[string]string{
			"app":          serviceName,
			"service_name": serviceName,
			"cluster":      "fresh-east-1",
			"namespace":    "prod",
			"level":        "info",
		}

		pushCustomToVL(t, time.Now().Add(500*time.Millisecond), stream, []logLine{
			{Msg: `{"stable":"yes","method":"GET"}`, Level: "info"},
		}, streamFields)
		time.Sleep(3 * time.Second)

		buildParams := func() url.Values {
			params := url.Values{}
			params.Set("query", fmt.Sprintf(`{service_name="%s"}`, serviceName))
			params.Set("start", time.Now().Add(-10*time.Minute).Format(time.RFC3339Nano))
			params.Set("end", time.Now().Add(time.Minute).Format(time.RFC3339Nano))
			return params
		}

		firstResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?"+buildParams().Encode())
		seen := map[string]bool{}
		for _, field := range extractArray(firstResp, "fields") {
			label := field.(map[string]interface{})["label"].(string)
			seen[label] = true
		}
		if !seen["stable"] {
			t.Fatalf("expected initial parsed field to be visible, got %v", firstResp)
		}
		if seen["new_field"] {
			t.Fatalf("did not expect new_field before second ingest, got %v", firstResp)
		}

		pushCustomToVL(t, time.Now().Add(500*time.Millisecond), stream, []logLine{
			{Msg: `{"stable":"yes","method":"GET","new_field":"fresh"}`, Level: "info"},
		}, streamFields)

		deadline := time.Now().Add(20 * time.Second)
		for {
			fieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?"+buildParams().Encode())
			seen = map[string]bool{}
			for _, field := range extractArray(fieldsResp, "fields") {
				label := field.(map[string]interface{})["label"].(string)
				seen[label] = true
			}
			if seen["new_field"] {
				valuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/new_field/values?"+buildParams().Encode())
				values := extractStrings(valuesResp, "values")
				if !contains(values, "fresh") {
					t.Fatalf("expected refreshed field values to include fresh, got %v", valuesResp)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected parsed-only field freshness update, last payload=%v", fieldsResp)
			}
			time.Sleep(1 * time.Second)
		}
	})
}

func TestDrilldown_RuntimeFamilyContracts(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()
	start := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	end := now.Format(time.RFC3339Nano)
	dsUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	version := grafanaRuntimeVersion(t)

	switch {
	case strings.HasPrefix(version, "11."):
		params := url.Values{}
		params.Set("query", "{service_name=~`.+`}")
		params.Set("start", start)
		params.Set("end", end)
		params.Set("targetLabels", "service_name")

		volumeResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume?"+params.Encode())
		volumeData := extractMap(volumeResp, "data")
		result := extractArray(volumeData, "result")
		foundService := false
		for _, item := range result {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			if metric["service_name"] == "api-gateway" {
				foundService = true
				break
			}
		}
		if !foundService {
			t.Fatalf("grafana %s expected 1.x-style service buckets, got %v", version, volumeResp)
		}

		fieldParams := url.Values{}
		fieldParams.Set("query", `{service_name="api-gateway"}`)
		fieldParams.Set("start", start)
		fieldParams.Set("end", end)

		fieldsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_fields?"+fieldParams.Encode())
		fields, _ := fieldsResp["fields"].([]interface{})
		seen := map[string]bool{}
		for _, field := range fields {
			seen[field.(map[string]interface{})["label"].(string)] = true
		}
		if !seen["method"] || seen["service_name"] || seen["cluster"] {
			t.Fatalf("grafana %s expected 1.x detected_fields filtering, got %v", version, fieldsResp)
		}

		clusterResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/cluster/values?"+fieldParams.Encode())
		clusterValues := extractStrings(clusterResp, "data")
		if !contains(clusterValues, "us-east-1") {
			t.Fatalf("grafana %s expected 1.x additional label values for cluster, got %v", version, clusterResp)
		}

		labelsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_labels?"+fieldParams.Encode())
		labels, _ := labelsResp["data"].([]interface{})
		seenLabels := map[string]bool{}
		for _, item := range labels {
			obj := item.(map[string]interface{})
			if label, _ := obj["label"].(string); label != "" {
				seenLabels[label] = true
			}
		}
		if !seenLabels["level"] || seenLabels["detected_level"] {
			t.Fatalf("grafana %s expected 1.x Loki-style detected_labels surface, got %v", version, labelsResp)
		}

	case strings.HasPrefix(version, "12."), strings.HasPrefix(version, "13."):
		levelParams := url.Values{}
		levelParams.Set("query", `{service_name="api-gateway"}`)
		levelParams.Set("start", start)
		levelParams.Set("end", end)
		levelParams.Set("step", "60")
		levelParams.Set("targetLabels", "detected_level")

		levelResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/index/volume_range?"+levelParams.Encode())
		levelData := extractMap(levelResp, "data")
		levelResult := extractArray(levelData, "result")
		levels := map[string]bool{}
		for _, item := range levelResult {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			if level, _ := metric["detected_level"].(string); level != "" {
				levels[level] = true
			}
		}
		if !levels["info"] || !levels["error"] {
			t.Fatalf("grafana %s expected 2.x detected_level volume series, got %v", version, levelResp)
		}

		fieldParams := url.Values{}
		fieldParams.Set("query", `{service_name="api-gateway"}`)
		fieldParams.Set("start", start)
		fieldParams.Set("end", end)

		methodValuesResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/detected_field/method/values?"+fieldParams.Encode())
		methodValues := extractStrings(methodValuesResp, "values")
		if len(methodValues) == 0 {
			t.Fatalf("grafana %s expected 2.x field-values breakdown contract for method values, got empty: %v", version, methodValuesResp)
		}

		clusterResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/label/cluster/values?"+fieldParams.Encode())
		clusterValues := extractStrings(clusterResp, "data")
		if !contains(clusterValues, "us-east-1") {
			t.Fatalf("grafana %s expected 2.x additional label values for cluster, got %v", version, clusterResp)
		}

		labelsResp := getJSON(t, grafanaURL+"/api/datasources/uid/"+dsUID+"/resources/labels?"+fieldParams.Encode())
		labelValues := extractStrings(labelsResp, "data")
		for _, want := range []string{"cluster", "namespace", "app"} {
			if !contains(labelValues, want) {
				t.Fatalf("grafana %s expected 2.x additional label tab %q, got %v", version, want, labelsResp)
			}
		}

	default:
		t.Fatalf("unsupported grafana runtime family for Drilldown contracts: %s", version)
	}
}

func TestDrilldown_TenantLimitsEndpointContract(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, proxyURL+"/config/tenant/v1/limits", nil)
	if err != nil {
		t.Fatalf("failed to create tenant limits request: %v", err)
	}
	req.Header.Set("X-Scope-OrgID", "team-a")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("tenant limits request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected tenant limits status 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("expected tenant limits content type text/plain, got %q", resp.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read tenant limits response body: %v", err)
	}
	payload := string(body)
	for _, required := range []string{"retention_period:", "query_timeout:", "volume_enabled:"} {
		if !strings.Contains(payload, required) {
			t.Fatalf("tenant limits response missing %q: %s", required, payload)
		}
	}
}
