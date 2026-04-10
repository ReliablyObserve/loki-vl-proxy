//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	proxyNativeMetadataURL = envOrOtel("PROXY_NATIVE_METADATA_URL", "http://localhost:3106")
	structuredMetadataOnce sync.Once
)

func TestStructuredMetadata_HybridModeExposesNativeAndTranslatedAliases(t *testing.T) {
	ensureStructuredMetadataData(t)

	resp := queryRangeCategorized(t, proxyUnderscoreURL, `{service_name="structured-metadata-e2e",level="info"}`)
	labels := firstStreamLabels(t, resp)
	metadata := firstStreamStructuredMetadata(t, resp)

	for _, want := range []string{"service_name", "k8s_pod_name"} {
		if _, ok := labels[want]; !ok {
			t.Fatalf("hybrid/underscore labels missing %q: %+v", want, labels)
		}
	}
	for _, forbidden := range []string{"service.name", "k8s.pod.name"} {
		if _, ok := labels[forbidden]; ok {
			t.Fatalf("hybrid/underscore labels unexpectedly exposed dotted key %q: %+v", forbidden, labels)
		}
	}

	for _, want := range []string{
		"http.target", "http_target",
		"cloud.region", "cloud_region",
	} {
		if _, ok := metadata[want]; !ok {
			t.Fatalf("hybrid structuredMetadata missing %q: %+v", want, metadata)
		}
	}
}

func TestStructuredMetadata_NativeModeKeepsDottedMetadataWithUnderscoreLabels(t *testing.T) {
	ensureStructuredMetadataData(t)

	resp := queryRangeCategorized(t, proxyNativeMetadataURL, `{service_name="structured-metadata-e2e",level="info"}`)
	labels := firstStreamLabels(t, resp)
	metadata := firstStreamStructuredMetadata(t, resp)

	for _, want := range []string{"service_name", "k8s_pod_name"} {
		if _, ok := labels[want]; !ok {
			t.Fatalf("native metadata mode labels missing %q: %+v", want, labels)
		}
	}
	for _, forbidden := range []string{"service.name", "k8s.pod.name"} {
		if _, ok := labels[forbidden]; ok {
			t.Fatalf("native metadata mode labels unexpectedly exposed dotted key %q: %+v", forbidden, labels)
		}
	}

	for _, want := range []string{"http.target", "cloud.region"} {
		if _, ok := metadata[want]; !ok {
			t.Fatalf("native structuredMetadata missing dotted key %q: %+v", want, metadata)
		}
	}
	for _, forbidden := range []string{"http_target", "cloud_region"} {
		if _, ok := metadata[forbidden]; ok {
			t.Fatalf("native structuredMetadata unexpectedly exposed translated key %q: %+v", forbidden, metadata)
		}
	}
}

func TestStructuredMetadata_LabelShapeMatchesLokiExpectations(t *testing.T) {
	ensureStructuredMetadataData(t)

	for name, baseURL := range map[string]string{
		"loki_direct":             lokiURL,
		"proxy_hybrid_underscore": proxyUnderscoreURL,
		"proxy_native_metadata":   proxyNativeMetadataURL,
	} {
		resp := queryRangeCategorized(t, baseURL, `{service_name="structured-metadata-e2e",level="info"}`)
		labels := firstStreamLabels(t, resp)

		if _, ok := labels["service_name"]; !ok {
			t.Fatalf("%s missing Loki-compatible service_name label: %+v", name, labels)
		}
		for key := range labels {
			if strings.Contains(key, ".") {
				t.Fatalf("%s returned dotted stream label %q (expected Loki-compatible label keys): %+v", name, key, labels)
			}
		}
	}
}

func ensureStructuredMetadataData(t *testing.T) {
	t.Helper()
	structuredMetadataOnce.Do(func() {
		now := time.Now().UTC()
		tsNanos := fmt.Sprintf("%d", now.UnixNano())
		tsRFC3339 := now.Format(time.RFC3339Nano)

		// Loki-compatible reference stream for label-shape parity checks.
		lokiPayload := map[string]interface{}{
			"streams": []map[string]interface{}{
				{
					"stream": map[string]string{
						"service_name": "structured-metadata-e2e",
						"k8s_pod_name": "structured-metadata-e2e-pod",
						"level":        "info",
					},
					"values": [][]string{{tsNanos, `{"msg":"structured metadata compatibility event"}`}},
				},
			},
		}
		body, _ := json.Marshal(lokiPayload)
		resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Fatalf("push structured metadata stream to Loki: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("push structured metadata stream to Loki failed with status %d", resp.StatusCode)
		}

		// VictoriaLogs stream with extra dotted fields that should surface in structuredMetadata.
		vlLine := fmt.Sprintf(
			`{"_time":"%s","_msg":"structured metadata compatibility event","service.name":"structured-metadata-e2e","k8s.pod.name":"structured-metadata-e2e-pod","level":"info","http.target":"/api/login","cloud.region":"eu-west-1"}`,
			tsRFC3339,
		)
		vlURLWithFields := vlURL + "/insert/jsonline?_stream_fields=service.name,k8s.pod.name,level"
		resp, err = http.Post(vlURLWithFields, "application/stream+json", strings.NewReader(vlLine))
		if err != nil {
			t.Fatalf("push structured metadata stream to VictoriaLogs: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("push structured metadata stream to VictoriaLogs failed with status %d", resp.StatusCode)
		}

		time.Sleep(3 * time.Second)
	})
}

func queryRangeCategorized(t *testing.T, baseURL, query string) map[string]interface{} {
	t.Helper()
	now := time.Now()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%d", now.Add(-20*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	req, err := http.NewRequest(http.MethodGet, baseURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	if err != nil {
		t.Fatalf("create query_range request: %v", err)
	}
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute query_range request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected query_range 200 from %s, got %d", baseURL, resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode query_range response: %v", err)
	}
	return out
}

func firstStreamLabels(t *testing.T, response map[string]interface{}) map[string]interface{} {
	t.Helper()

	data := extractMap(response, "data")
	result := extractArray(data, "result")
	if len(result) == 0 {
		t.Fatalf("expected at least one stream result, got %v", response)
	}
	stream, _ := result[0].(map[string]interface{})
	labels, _ := stream["stream"].(map[string]interface{})
	if len(labels) == 0 {
		t.Fatalf("expected stream labels, got %v", stream)
	}
	return labels
}

func firstStreamStructuredMetadata(t *testing.T, response map[string]interface{}) map[string]string {
	t.Helper()

	data := extractMap(response, "data")
	result := extractArray(data, "result")
	if len(result) == 0 {
		t.Fatalf("expected at least one stream result, got %v", response)
	}
	stream, _ := result[0].(map[string]interface{})
	values, _ := stream["values"].([]interface{})
	if len(values) == 0 {
		t.Fatalf("expected tuple values, got %v", stream)
	}
	firstTuple, _ := values[0].([]interface{})
	if len(firstTuple) < 3 {
		t.Fatalf("expected 3-tuple with metadata, got %#v", firstTuple)
	}
	meta, _ := firstTuple[2].(map[string]interface{})
	if meta == nil {
		t.Fatalf("expected metadata object in tuple[2], got %#v", firstTuple[2])
	}
	if _, ok := meta["structured_metadata"]; ok {
		t.Fatalf("non-Loki structured_metadata alias must not be emitted: %#v", meta)
	}
	structuredRaw, ok := meta["structuredMetadata"].([]interface{})
	if !ok {
		t.Fatalf("expected structuredMetadata key in metadata object, got %#v", meta)
	}

	structured := make(map[string]string, len(structuredRaw))
	for _, item := range structuredRaw {
		pair, ok := item.([]interface{})
		if !ok {
			t.Fatalf("expected structured metadata pair tuple, got %#v", item)
		}
		if len(pair) < 2 {
			t.Fatalf("expected [name,value] structured metadata pair, got %#v", pair)
		}
		name, _ := pair[0].(string)
		if name == "" {
			t.Fatalf("expected non-empty structured metadata name in %#v", pair)
		}
		v := pair[1]
		switch typed := v.(type) {
		case string:
			structured[name] = typed
		case float64:
			structured[name] = fmt.Sprintf("%g", typed)
		case bool:
			structured[name] = fmt.Sprintf("%t", typed)
		default:
			structured[name] = fmt.Sprintf("%v", typed)
		}
	}
	return structured
}
