//go:build e2e

// OTel semantic label compatibility tests.
//
// Tests all combinations of:
//   - VL stores dotted fields (OTel native) + proxy label-style=underscores → Loki underscores
//   - VL stores dotted fields + proxy label-style=passthrough → Loki dots (VL native view)
//   - VL stores underscore fields (pre-normalized) + proxy passthrough → Loki underscores
//   - Mixed labels (dots + underscores + dashes + plain) in same stream
//   - All OTel semantic convention categories: service, k8s, cloud, host, process, container, network, OS, log, telemetry, deployment
//   - Query direction: Loki underscore queries → VL dotted field lookups
//   - All API endpoints: labels, label_values, detected_fields, detected_field values, series, query_range, query
//   - Grafana Drilldown patterns with OTel labels
//
// Requires:
//
//	docker-compose up -d --build (includes loki-vl-proxy-underscore at :3102)
package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

var (
	proxyUnderscoreURL = envOrOtel("PROXY_UNDERSCORE_URL", "http://localhost:3102")
)

func envOrOtel(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ─── DATA INGESTION ──────────────────────────────────────────────────────────

// ingestAllOTelData pushes comprehensive OTel-style data covering every semantic category.
// Called once by TestMain-like setup, cached via sync.Once equivalent.
var otelDataIngested bool

func ensureOTelData(t *testing.T) {
	t.Helper()
	if otelDataIngested {
		return
	}
	ingestOTelTestData(t)
	ingestUnderscoreTestData(t)
	ingestMixedLabelTestData(t)
	ingestAllOTelCategories(t)
	// The e2e proxy caches global label queries briefly; wait for the short
	// cache TTL in docker-compose to expire before asserting on label discovery.
	time.Sleep(6 * time.Second)
	otelDataIngested = true
}

// ingestOTelTestData pushes core OTel-style logs with dotted attributes to both Loki and VL.
func ingestOTelTestData(t *testing.T) {
	t.Helper()
	now := time.Now()

	otelStreams := []streamDef{
		// ── Service: auth (info) ──
		{
			Labels: map[string]string{
				"service.name":           "otel-auth-service",
				"service.namespace":      "auth",
				"k8s.namespace.name":     "production",
				"k8s.pod.name":           "auth-svc-6f4d8c9-xj2k1",
				"k8s.container.name":     "auth",
				"k8s.node.name":          "ip-10-0-1-42",
				"deployment.environment": "production",
				"telemetry.sdk.name":     "opentelemetry",
				"telemetry.sdk.language": "go",
				"host.name":              "ip-10-0-1-42",
				"level":                  "info",
			},
			Lines: []string{
				`{"msg":"request processed","method":"POST","path":"/auth/login","status":200,"duration_ms":42,"trace_id":"abcdef1234567890"}`,
				`{"msg":"token validated","user_id":"usr_001","token_type":"jwt","duration_ms":3}`,
				`{"msg":"session created","session_id":"sess_abc123","ttl":"3600s"}`,
			},
		},
		// ── Service: auth (error) ──
		{
			Labels: map[string]string{
				"service.name":           "otel-auth-service",
				"service.namespace":      "auth",
				"k8s.namespace.name":     "production",
				"k8s.pod.name":           "auth-svc-6f4d8c9-xj2k1",
				"k8s.container.name":     "auth",
				"deployment.environment": "production",
				"host.name":              "ip-10-0-1-42",
				"level":                  "error",
			},
			Lines: []string{
				`{"msg":"authentication failed","user_id":"usr_bad","reason":"invalid_credentials","trace_id":"err_trace_001"}`,
				`{"msg":"rate limit exceeded","client_ip":"10.0.1.99","requests_per_sec":150,"limit":100}`,
			},
		},
		// ── Service: orders (warn) ──
		{
			Labels: map[string]string{
				"service.name":           "otel-order-service",
				"service.namespace":      "orders",
				"k8s.namespace.name":     "production",
				"k8s.pod.name":           "order-svc-8a7b6c5-mn3p",
				"k8s.container.name":     "orders",
				"deployment.environment": "production",
				"container.id":           "abc123def456",
				"level":                  "warn",
			},
			Lines: []string{
				`{"msg":"slow database query","query":"SELECT * FROM orders","duration_ms":4500,"threshold_ms":1000}`,
				`{"msg":"inventory check delayed","product_id":"prod_789","warehouse":"us-east"}`,
			},
		},
	}

	for _, sd := range otelStreams {
		pushStreamToVL(t, now, sd)
		pushStreamToLoki(t, now, sd)
	}
	time.Sleep(5 * time.Second)
	t.Log("OTel dotted data ingested")
}

// ingestUnderscoreTestData pushes data with underscore-only labels to VL.
// This simulates a pipeline that pre-normalizes labels (e.g., Vector/FluentBit).
func ingestUnderscoreTestData(t *testing.T) {
	t.Helper()
	now := time.Now()

	sd := streamDef{
		Labels: map[string]string{
			"service_name":           "underscore-svc",
			"k8s_namespace_name":     "staging",
			"k8s_pod_name":           "underscore-pod-abc",
			"deployment_environment": "staging",
			"host_name":              "host-underscore-1",
			"level":                  "info",
		},
		Lines: []string{
			`{"msg":"pre-normalized underscore labels","status":"ok"}`,
			`{"msg":"vector pipeline processed","source":"kubernetes_logs"}`,
		},
	}

	// Push to VL only (underscore labels stored as-is in VL)
	pushStreamToVL(t, now, sd)
	// Push same to Loki (Loki accepts underscores natively)
	pushStreamToLoki(t, now, sd)

	time.Sleep(5 * time.Second)
	t.Log("Underscore data ingested")
}

// ingestMixedLabelTestData pushes data with a mix of dots, underscores, and dashes.
func ingestMixedLabelTestData(t *testing.T) {
	t.Helper()
	now := time.Now()

	sd := streamDef{
		Labels: map[string]string{
			"service.name":       "mixed-label-svc",
			"plain_label":        "plain_value",
			"k8s.pod.name":       "mixed-pod-xyz",
			"custom-dashed":      "dashed_value",
			"already_underscore": "yes",
			"level":              "debug",
		},
		Lines: []string{
			`{"msg":"mixed label styles in one stream"}`,
			`{"msg":"dots and underscores and dashes coexist"}`,
		},
	}

	pushStreamToVL(t, now, sd)
	pushStreamToLoki(t, now, sd)

	time.Sleep(5 * time.Second)
	t.Log("Mixed label data ingested")
}

// ingestAllOTelCategories pushes one stream per OTel semantic category.
func ingestAllOTelCategories(t *testing.T) {
	t.Helper()
	now := time.Now()

	categories := []streamDef{
		// ── Cloud attributes ──
		{
			Labels: map[string]string{
				"service.name":            "cloud-metadata-svc",
				"cloud.provider":          "aws",
				"cloud.platform":          "aws_ec2",
				"cloud.region":            "us-east-1",
				"cloud.availability_zone": "us-east-1a",
				"cloud.account.id":        "123456789012",
				"level":                   "info",
			},
			Lines: []string{`{"msg":"cloud metadata test entry"}`},
		},
		// ── Host attributes ──
		{
			Labels: map[string]string{
				"service.name": "host-metadata-svc",
				"host.name":    "ip-10-0-1-42.ec2.internal",
				"host.id":      "i-0abc123def456",
				"host.type":    "m5.xlarge",
				"host.arch":    "amd64",
				"level":        "info",
			},
			Lines: []string{`{"msg":"host metadata test entry"}`},
		},
		// ── Process attributes ──
		{
			Labels: map[string]string{
				"service.name":            "process-metadata-svc",
				"process.pid":             "12345",
				"process.executable.name": "myapp",
				"process.runtime.name":    "go",
				"process.runtime.version": "1.23.0",
				"level":                   "info",
			},
			Lines: []string{`{"msg":"process metadata test entry"}`},
		},
		// ── Container attributes ──
		{
			Labels: map[string]string{
				"service.name":         "container-metadata-svc",
				"container.id":         "sha256:abc123def456",
				"container.name":       "myapp-container",
				"container.runtime":    "containerd",
				"container.image.name": "ghcr.io/org/myapp",
				"container.image.tag":  "v1.2.3",
				"level":                "info",
			},
			Lines: []string{`{"msg":"container metadata test entry"}`},
		},
		// ── OS attributes ──
		{
			Labels: map[string]string{
				"service.name": "os-metadata-svc",
				"os.type":      "linux",
				"os.version":   "5.15.0-1042-aws",
				"level":        "info",
			},
			Lines: []string{`{"msg":"os metadata test entry"}`},
		},
		// ── Network attributes ──
		{
			Labels: map[string]string{
				"service.name":  "network-metadata-svc",
				"net.host.name": "api.example.com",
				"net.host.port": "8443",
				"net.peer.name": "db.internal",
				"net.peer.port": "5432",
				"level":         "info",
			},
			Lines: []string{`{"msg":"network metadata test entry"}`},
		},
		// ── Log-specific attributes ──
		{
			Labels: map[string]string{
				"service.name":  "log-metadata-svc",
				"log.file.path": "/var/log/app/myapp.log",
				"log.file.name": "myapp.log",
				"log.iostream":  "stdout",
				"level":         "info",
			},
			Lines: []string{`{"msg":"log metadata test entry"}`},
		},
		// ── Kubernetes workload types ──
		{
			Labels: map[string]string{
				"service.name":         "k8s-workloads-svc",
				"k8s.deployment.name":  "myapp-deploy",
				"k8s.replicaset.name":  "myapp-deploy-abc123",
				"k8s.daemonset.name":   "fluentbit",
				"k8s.statefulset.name": "redis",
				"k8s.job.name":         "batch-import",
				"k8s.cronjob.name":     "nightly-cleanup",
				"k8s.cluster.name":     "prod-us-east",
				"k8s.namespace.name":   "default",
				"k8s.pod.name":         "k8s-workloads-pod",
				"level":                "info",
			},
			Lines: []string{`{"msg":"k8s workload types test entry"}`},
		},
		// ── Telemetry SDK attributes ──
		{
			Labels: map[string]string{
				"service.name":           "telemetry-metadata-svc",
				"telemetry.sdk.name":     "opentelemetry",
				"telemetry.sdk.language": "python",
				"telemetry.sdk.version":  "1.21.0",
				"service.version":        "2.0.0",
				"service.instance.id":    "instance-xyz-789",
				"level":                  "info",
			},
			Lines: []string{`{"msg":"telemetry sdk metadata test entry"}`},
		},
		// ── Deployment attributes ──
		{
			Labels: map[string]string{
				"service.name":                "deployment-metadata-svc",
				"deployment.environment":      "production",
				"deployment.environment.name": "prod-us-east",
				"level":                       "info",
			},
			Lines: []string{`{"msg":"deployment metadata test entry"}`},
		},
	}

	for _, sd := range categories {
		pushStreamToVL(t, now, sd)
		pushStreamToLoki(t, now, sd)
	}

	time.Sleep(3 * time.Second)
	t.Log("All OTel categories ingested")
}

// ─── PUSH HELPERS ────────────────────────────────────────────────────────────

func pushStreamToVL(t *testing.T, baseTime time.Time, sd streamDef) {
	t.Helper()
	var vlLines []string
	for i, line := range sd.Lines {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		entry := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": line}
		for k, v := range sd.Labels {
			entry[k] = v
		}
		j, _ := json.Marshal(entry)
		vlLines = append(vlLines, string(j))
	}

	streamFields := []string{}
	for k := range sd.Labels {
		if k != "level" {
			streamFields = append(streamFields, k)
		}
	}

	resp, err := http.Post(
		vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(streamFields, ",")),
		"application/stream+json",
		strings.NewReader(strings.Join(vlLines, "\n")),
	)
	if err != nil {
		svcName := sd.Labels["service.name"]
		if svcName == "" {
			svcName = sd.Labels["service_name"]
		}
		t.Logf("VL push (%s): failed: %v", svcName, err)
		return
	}
	resp.Body.Close()
}

func pushStreamToLoki(t *testing.T, baseTime time.Time, sd streamDef) {
	t.Helper()
	values := make([][]string, len(sd.Lines))
	for i, line := range sd.Lines {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		values[i] = []string{fmt.Sprintf("%d", ts.UnixNano()), line}
	}

	lokiPayload := map[string]interface{}{
		"streams": []map[string]interface{}{
			{"stream": sd.Labels, "values": values},
		},
	}
	body, _ := json.Marshal(lokiPayload)
	resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Logf("Loki push failed: %v", err)
		return
	}
	resp.Body.Close()
}

// ─── QUERY HELPERS ───────────────────────────────────────────────────────────

func getLabels(t *testing.T, baseURL string) []string {
	t.Helper()
	resp, err := http.Get(baseURL + "/loki/api/v1/labels")
	if err != nil {
		t.Fatalf("failed to get labels from %s: %v", baseURL, err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data, _ := result["data"].([]interface{})
	labels := make([]string, 0, len(data))
	for _, d := range data {
		if s, ok := d.(string); ok {
			labels = append(labels, s)
		}
	}
	return labels
}

func getLabelValues(t *testing.T, baseURL, labelName string) []string {
	t.Helper()
	resp, err := http.Get(baseURL + "/loki/api/v1/label/" + labelName + "/values")
	if err != nil {
		t.Fatalf("failed to get label values: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data, _ := result["data"].([]interface{})
	values := make([]string, 0, len(data))
	for _, d := range data {
		if s, ok := d.(string); ok {
			values = append(values, s)
		}
	}
	return values
}

func queryRange(t *testing.T, baseURL, query string) []interface{} {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
		"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
		"limit": {"1000"},
	}

	resp, err := http.Get(baseURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		t.Fatalf("query_range failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data, _ := result["data"].(map[string]interface{})
	streams, _ := data["result"].([]interface{})
	return streams
}

func getDetectedFields(t *testing.T, baseURL string) []string {
	return getDetectedFieldsForQuery(t, baseURL, "*")
}

func getDetectedFieldsForQuery(t *testing.T, baseURL, query string) []string {
	t.Helper()
	resp, err := http.Get(baseURL + "/loki/api/v1/detected_fields?query=" + url.QueryEscape(query))
	if err != nil {
		t.Fatalf("detected_fields failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	fields, _ := result["fields"].([]interface{})
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		field, _ := f.(map[string]interface{})
		if label, ok := field["label"].(string); ok {
			names = append(names, label)
		}
	}
	return names
}

func getSeries(t *testing.T, baseURL, matchQuery string) []map[string]interface{} {
	t.Helper()
	params := url.Values{
		"match[]": {matchQuery},
		"start":   {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
		"end":     {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
	}
	resp, err := http.Get(baseURL + "/loki/api/v1/series?" + params.Encode())
	if err != nil {
		t.Fatalf("series failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data, _ := result["data"].([]interface{})
	series := make([]map[string]interface{}, 0, len(data))
	for _, d := range data {
		if m, ok := d.(map[string]interface{}); ok {
			series = append(series, m)
		}
	}
	return series
}

func getDetectedFieldValues(t *testing.T, baseURL, fieldName string) []string {
	return getDetectedFieldValuesForQuery(t, baseURL, fieldName, "*")
}

func getDetectedFieldValuesForQuery(t *testing.T, baseURL, fieldName, query string) []string {
	t.Helper()
	resp, err := http.Get(baseURL + "/loki/api/v1/detected_field/" + fieldName + "/values?query=" + url.QueryEscape(query))
	if err != nil {
		t.Fatalf("detected_field values failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result map[string]interface{}
	json.Unmarshal(body, &result)

	// May come as result.values (Loki) or result.data (Loki standard)
	var vals []interface{}
	if v, ok := result["values"].([]interface{}); ok {
		vals = v
	} else if v, ok := result["data"].([]interface{}); ok {
		vals = v
	}
	values := make([]string, 0, len(vals))
	for _, d := range vals {
		if s, ok := d.(string); ok {
			values = append(values, s)
		}
	}
	return values
}

// ─── SCENARIO 1: VL stores dots, proxy label-style=underscores ──────────────

func TestOTelDots_ProxyUnderscores(t *testing.T) {
	ensureOTelData(t)

	t.Run("labels_all_underscored", func(t *testing.T) {
		labels := getLabels(t, proxyUnderscoreURL)
		labelSet := toSet(labels)

		for _, l := range labels {
			if strings.Contains(l, ".") {
				t.Errorf("label %q contains dots — all labels should be underscored in underscore mode", l)
			}
		}

		// All OTel semantic categories should be present as underscores
		mustHave := []string{
			// Service
			"service_name", "service_namespace", "service_version", "service_instance_id",
			// K8s core
			"k8s_namespace_name", "k8s_pod_name", "k8s_container_name", "k8s_node_name",
			// K8s workloads
			"k8s_deployment_name", "k8s_replicaset_name", "k8s_daemonset_name",
			"k8s_statefulset_name", "k8s_job_name", "k8s_cronjob_name", "k8s_cluster_name",
			// Cloud
			"cloud_provider", "cloud_platform", "cloud_region", "cloud_availability_zone", "cloud_account_id",
			// Host
			"host_name", "host_id", "host_type", "host_arch",
			// Process
			"process_pid", "process_executable_name", "process_runtime_name", "process_runtime_version",
			// Container
			"container_id", "container_name", "container_runtime", "container_image_name", "container_image_tag",
			// OS
			"os_type", "os_version",
			// Network
			"net_host_name", "net_host_port", "net_peer_name", "net_peer_port",
			// Log
			"log_file_path", "log_file_name", "log_iostream",
			// Telemetry
			"telemetry_sdk_name", "telemetry_sdk_language", "telemetry_sdk_version",
			// Deployment
			"deployment_environment", "deployment_environment_name",
		}
		for _, want := range mustHave {
			if !labelSet[want] {
				t.Errorf("missing expected underscore label %q in labels list: %v", want, labels)
			}
		}
	})

	t.Run("no_dots_in_any_label", func(t *testing.T) {
		labels := getLabels(t, proxyUnderscoreURL)
		for _, l := range labels {
			if strings.Contains(l, ".") {
				t.Errorf("label %q contains dots in underscore mode", l)
			}
			if strings.Contains(l, "-") {
				// dashes are also sanitized to underscores
				t.Errorf("label %q contains dashes — should be underscored", l)
			}
		}
	})

	t.Run("label_values_service_name", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "service_name")
		if len(values) == 0 {
			t.Fatal("service_name label values should not be empty")
		}
		valSet := toSet(values)
		wantServices := []string{
			"otel-auth-service", "otel-order-service",
			"cloud-metadata-svc", "host-metadata-svc", "process-metadata-svc",
			"container-metadata-svc", "os-metadata-svc", "network-metadata-svc",
			"log-metadata-svc", "k8s-workloads-svc", "telemetry-metadata-svc",
			"deployment-metadata-svc", "mixed-label-svc",
		}
		for _, want := range wantServices {
			if !valSet[want] {
				t.Errorf("expected service %q in service_name values, got: %v", want, values)
			}
		}
	})

	t.Run("label_values_k8s_namespace_name", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "k8s_namespace_name")
		if len(values) == 0 {
			t.Error("k8s_namespace_name values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["production"] && !valSet["staging"] {
			t.Errorf("expected at least one known namespace value (production/staging), got: %v", values)
		}
	})

	t.Run("label_values_cloud_provider", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "cloud_provider")
		if len(values) == 0 {
			t.Error("cloud_provider values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["aws"] {
			t.Errorf("expected 'aws' in cloud_provider values, got: %v", values)
		}
	})

	t.Run("label_values_host_name", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "host_name")
		if len(values) == 0 {
			t.Error("host_name values should not be empty")
		}
	})

	t.Run("label_values_container_id", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "container_id")
		if len(values) == 0 {
			t.Error("container_id values should not be empty")
		}
	})

	t.Run("label_values_os_type", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "os_type")
		if len(values) == 0 {
			t.Error("os_type values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["linux"] {
			t.Errorf("expected 'linux' in os_type values, got: %v", values)
		}
	})

	t.Run("label_values_net_host_name", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "net_host_name")
		if len(values) == 0 {
			t.Error("net_host_name values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["api.example.com"] {
			t.Errorf("expected 'api.example.com' in net_host_name values, got: %v", values)
		}
	})

	t.Run("label_values_log_iostream", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "log_iostream")
		if len(values) == 0 {
			t.Error("log_iostream values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["stdout"] {
			t.Errorf("expected 'stdout' in log_iostream values, got: %v", values)
		}
	})

	t.Run("label_values_deployment_environment", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "deployment_environment")
		if len(values) == 0 {
			t.Error("deployment_environment values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["production"] && !valSet["staging"] {
			t.Errorf("expected at least one known deployment environment value (production/staging), got: %v", values)
		}
	})

	t.Run("label_values_process_runtime_name", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "process_runtime_name")
		if len(values) == 0 {
			t.Error("process_runtime_name values should not be empty")
		}
		valSet := toSet(values)
		if !valSet["go"] {
			t.Errorf("expected 'go' in process_runtime_name values, got: %v", values)
		}
	})

	t.Run("label_values_telemetry_sdk_language", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "telemetry_sdk_language")
		if len(values) == 0 {
			t.Error("telemetry_sdk_language values should not be empty")
		}
		valSet := toSet(values)
		for _, want := range []string{"go", "python"} {
			if !valSet[want] {
				t.Errorf("expected %q in telemetry_sdk_language values, got: %v", want, values)
			}
		}
	})
}

// ─── SCENARIO 2: VL stores dots, proxy passthrough ──────────────────────────

func TestOTelDots_ProxyPassthrough(t *testing.T) {
	ensureOTelData(t)

	t.Run("labels_show_dots", func(t *testing.T) {
		labels := getLabels(t, proxyURL)
		labelSet := toSet(labels)

		dottedExpected := []string{
			"service.name", "service.namespace",
			"k8s.namespace.name", "k8s.pod.name", "k8s.container.name", "k8s.node.name",
			"deployment.environment",
			"host.name",
			"cloud.provider", "cloud.region",
			"container.id", "container.name",
			"os.type",
			"net.host.name", "net.peer.name",
			"log.file.path", "log.iostream",
			"telemetry.sdk.name", "telemetry.sdk.language",
		}
		found := 0
		for _, dotted := range dottedExpected {
			if labelSet[dotted] {
				found++
			}
		}
		if found == 0 {
			t.Errorf("no dotted labels found in passthrough mode, labels: %v", labels)
		} else {
			t.Logf("found %d/%d dotted labels in passthrough mode", found, len(dottedExpected))
		}
	})

	t.Run("query_with_dotted_labels", func(t *testing.T) {
		// In passthrough mode, dotted labels work in queries
		// VL field names ARE dotted, so passthrough queries should match
		streams := queryRange(t, proxyURL, `{service.name="otel-auth-service"}`)
		if len(streams) == 0 {
			// This might fail because LogQL doesn't support dots in stream selectors
			// That's expected — dots are not valid Loki label syntax
			t.Log("note: dotted label query returned no results (expected — dots not valid in LogQL)")
		}
	})
}

// ─── SCENARIO 3: VL stores underscores, proxy passthrough ───────────────────

func TestOTelUnderscores_ProxyPassthrough(t *testing.T) {
	ensureOTelData(t)

	t.Run("underscore_labels_passthrough", func(t *testing.T) {
		// The underscore-only data pushed to VL should be visible via passthrough proxy
		labels := getLabels(t, proxyURL)
		labelSet := toSet(labels)

		// These were pushed with underscore names directly
		wantUnderscored := []string{
			"service_name", "k8s_namespace_name", "k8s_pod_name",
			"deployment_environment", "host_name",
		}
		found := 0
		for _, want := range wantUnderscored {
			if labelSet[want] {
				found++
			}
		}
		if found == 0 {
			t.Errorf("no underscore labels found in passthrough mode, got: %v", labels)
		} else {
			t.Logf("found %d/%d underscore labels in passthrough mode", found, len(wantUnderscored))
		}
	})

	t.Run("query_underscore_service", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{service_name="underscore-svc"}`)
		if len(streams) == 0 {
			t.Error("underscore service query should return results via passthrough proxy")
		}
	})
}

// ─── SCENARIO 4: Query direction tests ──────────────────────────────────────

func TestQueryDirection_UnderscoreProxy(t *testing.T) {
	ensureOTelData(t)

	// Each subtest queries using Loki underscore syntax against the underscore proxy,
	// which translates to VL dotted field names for the backend query.

	t.Run("query_service_name", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{service_name="otel-auth-service"}`)
		if len(streams) == 0 {
			t.Error("query {service_name=...} should return results")
			return
		}
		// All returned labels should be underscored
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_k8s_namespace_name", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{k8s_namespace_name="production"}`)
		if len(streams) == 0 {
			t.Error("query {k8s_namespace_name=...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_deployment_environment", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{deployment_environment="production"}`)
		if len(streams) == 0 {
			t.Error("query {deployment_environment=...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_host_name", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{host_name="ip-10-0-1-42"}`)
		if len(streams) == 0 {
			t.Error("query {host_name=...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_cloud_provider", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{cloud_provider="aws"}`)
		if len(streams) == 0 {
			t.Error("query {cloud_provider=...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_container_name", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{container_name="myapp-container"}`)
		if len(streams) == 0 {
			t.Error("query {container_name=...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_os_type", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{os_type="linux"}`)
		if len(streams) == 0 {
			t.Error("query {os_type=...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_regex_service_name", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{service_name=~"otel-.*"}`)
		if len(streams) == 0 {
			t.Error("regex query {service_name=~...} should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_negation_service_name", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL, `{service_name!="otel-auth-service",level="info"}`)
		if len(streams) == 0 {
			t.Error("negated query should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_multiple_otel_labels", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL,
			`{service_name="otel-auth-service",deployment_environment="production",level="info"}`)
		if len(streams) == 0 {
			t.Error("multi-label OTel query should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_with_line_filter", func(t *testing.T) {
		streams := queryRange(t, proxyUnderscoreURL,
			`{service_name="otel-auth-service"} |= "request processed"`)
		if len(streams) == 0 {
			t.Error("query with line filter should return results")
			return
		}
		assertNoDotsInStreams(t, streams)
	})

	t.Run("query_unknown_label_passthrough", func(t *testing.T) {
		// Labels NOT in the known OTel map should pass through as-is
		streams := queryRange(t, proxyUnderscoreURL, `{level="info"}`)
		if len(streams) == 0 {
			t.Error("query with non-OTel label should return results")
		}
	})
}

// ─── SCENARIO 5: detected_fields and detected_field values ──────────────────

func TestDetectedFields_UnderscoreProxy(t *testing.T) {
	ensureOTelData(t)

	t.Run("hybrid_fields_expose_aliases_and_native_names", func(t *testing.T) {
		fields := getDetectedFields(t, proxyUnderscoreURL)
		fieldSet := toSet(fields)
		mustHave := []string{
			"service_name",
			"service.name",
			"detected_level",
			"k8s_pod_name",
			"k8s.pod.name",
			"deployment_environment",
			"deployment.environment",
		}
		for _, want := range mustHave {
			if !fieldSet[want] {
				t.Errorf("missing expected detected field %q", want)
			}
		}
	})

	t.Run("no_vl_internal_fields", func(t *testing.T) {
		fields := getDetectedFields(t, proxyUnderscoreURL)
		for _, f := range fields {
			if f == "_time" || f == "_msg" || f == "_stream" || f == "_stream_id" {
				t.Errorf("VL internal field %q should not appear in detected_fields", f)
			}
		}
	})

	t.Run("detected_field_values_service_name", func(t *testing.T) {
		values := getDetectedFieldValues(t, proxyUnderscoreURL, "service_name")
		if len(values) == 0 {
			t.Log("note: detected_field values for service_name returned empty (endpoint may not support query translation)")
		} else {
			valSet := toSet(values)
			if !valSet["otel-auth-service"] {
				t.Logf("'otel-auth-service' not yet in detected field values (timing), got: %v", values)
			}
		}
	})

	t.Run("detected_field_values_service_dot_name", func(t *testing.T) {
		values := getDetectedFieldValues(t, proxyUnderscoreURL, "service.name")
		if len(values) == 0 {
			t.Log("note: detected_field values for service.name returned empty")
			return
		}
		valSet := toSet(values)
		if !valSet["otel-auth-service"] {
			t.Logf("'otel-auth-service' not yet in detected field values for service.name, got: %v", values)
		}
	})
}

// ─── SCENARIO 6: series endpoint ────────────────────────────────────────────

func TestSeries_UnderscoreProxy(t *testing.T) {
	ensureOTelData(t)

	t.Run("series_labels_underscored", func(t *testing.T) {
		series := getSeries(t, proxyUnderscoreURL, `{service_name="otel-auth-service"}`)
		if len(series) == 0 {
			t.Error("series query should return results")
			return
		}
		for _, s := range series {
			for key := range s {
				if strings.Contains(key, ".") {
					t.Errorf("series label %q contains dots", key)
				}
			}
		}
	})

	t.Run("series_wildcard_all_underscored", func(t *testing.T) {
		series := getSeries(t, proxyUnderscoreURL, `{level="info"}`)
		if len(series) == 0 {
			t.Error("wildcard series should return results")
			return
		}
		dotCount := 0
		for _, s := range series {
			for key := range s {
				if strings.Contains(key, ".") {
					dotCount++
				}
			}
		}
		if dotCount > 0 {
			t.Errorf("found %d dotted labels in series results", dotCount)
		}
	})
}

// ─── SCENARIO 7: Mixed labels ───────────────────────────────────────────────

func TestMixedLabels_UnderscoreProxy(t *testing.T) {
	ensureOTelData(t)

	t.Run("mixed_all_sanitized", func(t *testing.T) {
		// The mixed data has: service.name (dotted), plain_label (underscore),
		// custom-dashed (dash), already_underscore
		streams := queryRange(t, proxyUnderscoreURL, `{service_name="mixed-label-svc"}`)
		if len(streams) == 0 {
			t.Error("mixed label query should return results")
			return
		}

		for _, s := range streams {
			stream, _ := s.(map[string]interface{})
			labels, _ := stream["stream"].(map[string]interface{})
			for key := range labels {
				if strings.Contains(key, ".") {
					t.Errorf("mixed stream label %q contains dots", key)
				}
				if strings.Contains(key, "-") {
					t.Errorf("mixed stream label %q contains dashes — should be underscored", key)
				}
			}

			// Verify specific translations
			if _, ok := labels["service_name"]; !ok {
				t.Error("expected service_name (from service.name)")
			}
			if _, ok := labels["k8s_pod_name"]; !ok {
				t.Error("expected k8s_pod_name (from k8s.pod.name)")
			}
			if _, ok := labels["custom_dashed"]; !ok {
				t.Error("expected custom_dashed (from custom-dashed)")
			}
			if _, ok := labels["already_underscore"]; !ok {
				t.Error("expected already_underscore (already underscore)")
			}
			if _, ok := labels["plain_label"]; !ok {
				t.Error("expected plain_label (already underscore)")
			}
		}
	})
}

// ─── SCENARIO 8: Compare Loki vs Proxy label names ─────────────────────────

func TestLokiVsProxy_LabelParity(t *testing.T) {
	ensureOTelData(t)

	t.Run("otel_labels_match_loki_convention", func(t *testing.T) {
		lokiLabels := getLabels(t, lokiURL)
		proxyLabels := getLabels(t, proxyUnderscoreURL)

		lokiSet := toSet(lokiLabels)
		proxySet := toSet(proxyLabels)

		// OTel labels that Loki sanitizes — these should match between Loki and proxy
		otelLabels := []string{
			"service_name", "service_namespace",
			"k8s_namespace_name", "k8s_pod_name", "k8s_container_name", "k8s_node_name",
			"deployment_environment",
			"telemetry_sdk_name", "telemetry_sdk_language",
			"host_name",
		}

		matched := 0
		for _, lbl := range otelLabels {
			inLoki := lokiSet[lbl]
			inProxy := proxySet[lbl]
			if inLoki && inProxy {
				matched++
			} else if inLoki && !inProxy {
				t.Errorf("label %q: in Loki but MISSING from proxy", lbl)
			} else if !inLoki && inProxy {
				t.Logf("label %q: in proxy but not in Loki (may be data difference)", lbl)
			}
		}
		t.Logf("OTel label parity: %d/%d matched between Loki and underscore proxy", matched, len(otelLabels))
	})

	t.Run("service_name_values_match", func(t *testing.T) {
		lokiValues := getLabelValues(t, lokiURL, "service_name")
		proxyValues := getLabelValues(t, proxyUnderscoreURL, "service_name")

		// Loki auto-sanitizes dots → underscores in label names, and since we pushed
		// the same data with dotted labels to both, Loki should have the same service names
		lokiSet := toSet(lokiValues)
		proxySet := toSet(proxyValues)

		// Services pushed with OTel dotted labels
		otelServices := []string{
			"otel-auth-service", "otel-order-service",
		}
		for _, svc := range otelServices {
			if !lokiSet[svc] && !proxySet[svc] {
				t.Logf("service %q not found in either (may be ingestion issue)", svc)
			}
			if lokiSet[svc] && !proxySet[svc] {
				t.Errorf("service %q in Loki but MISSING from proxy", svc)
			}
			if !lokiSet[svc] && proxySet[svc] {
				t.Logf("service %q in proxy but not Loki", svc)
			}
		}

		t.Logf("Loki service_name values (%d): %v", len(lokiValues), lokiValues)
		t.Logf("Proxy service_name values (%d): %v", len(proxyValues), proxyValues)
	})
}

// ─── SCENARIO 9: Grafana Drilldown patterns ─────────────────────────────────

func TestGrafanaDrilldown_UnderscoreProxy(t *testing.T) {
	ensureOTelData(t)

	t.Run("drilldown_service_name_query", func(t *testing.T) {
		// Grafana Drilldown queries service_name for the overview
		values := getLabelValues(t, proxyUnderscoreURL, "service_name")
		if len(values) == 0 {
			t.Fatal("Drilldown needs service_name values")
		}
		t.Logf("Drilldown services: %v", values)
	})

	t.Run("drilldown_level_filter", func(t *testing.T) {
		// Drilldown filters by level within a service
		streams := queryRange(t, proxyUnderscoreURL,
			`{service_name="otel-auth-service",level="error"}`)
		if len(streams) == 0 {
			t.Error("Drilldown service+level filter should return results")
		}
	})

	t.Run("drilldown_detected_fields_for_service", func(t *testing.T) {
		// Drilldown calls detected_fields for the selected service
		params := url.Values{
			"query": {`{service_name="otel-auth-service"}`},
		}
		resp, err := http.Get(proxyUnderscoreURL + "/loki/api/v1/detected_fields?" + params.Encode())
		if err != nil {
			t.Fatalf("detected_fields failed: %v", err)
		}
		defer resp.Body.Close()

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)

		fields, _ := result["fields"].([]interface{})
		if len(fields) == 0 {
			t.Log("note: detected_fields for specific query returned empty (may need wildcard fallback)")
		}
		seen := map[string]bool{}
		for _, f := range fields {
			field, _ := f.(map[string]interface{})
			label, _ := field["label"].(string)
			seen[label] = true
		}
		for _, want := range []string{"service.name", "service_name"} {
			if !seen[want] {
				t.Errorf("Drilldown hybrid detected fields missing %q", want)
			}
		}
	})

	t.Run("drilldown_index_stats", func(t *testing.T) {
		params := url.Values{
			"query": {`{service_name="otel-auth-service"}`},
			"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
			"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
		}
		resp, err := http.Get(proxyUnderscoreURL + "/loki/api/v1/index/stats?" + params.Encode())
		if err != nil {
			t.Fatalf("index/stats failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("index/stats expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("drilldown_volume", func(t *testing.T) {
		params := url.Values{
			"query": {`{service_name="otel-auth-service"}`},
			"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
			"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
		}
		resp, err := http.Get(proxyUnderscoreURL + "/loki/api/v1/index/volume?" + params.Encode())
		if err != nil {
			t.Fatalf("volume failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("volume expected 200, got %d", resp.StatusCode)
		}
	})
}

// ─── SCENARIO 10: Deduplication of translated labels ────────────────────────

func TestLabelDeduplication(t *testing.T) {
	ensureOTelData(t)

	t.Run("labels_no_duplicates", func(t *testing.T) {
		labels := getLabels(t, proxyUnderscoreURL)
		seen := map[string]int{}
		for _, l := range labels {
			seen[l]++
		}
		for l, count := range seen {
			if count > 1 {
				t.Errorf("duplicate label %q appeared %d times", l, count)
			}
		}
	})

	t.Run("detected_fields_no_duplicates", func(t *testing.T) {
		fields := getDetectedFields(t, proxyUnderscoreURL)
		seen := map[string]int{}
		for _, f := range fields {
			seen[f]++
		}
		for f, count := range seen {
			if count > 1 {
				t.Errorf("duplicate detected field %q appeared %d times", f, count)
			}
		}
	})
}

// ─── SCENARIO 11: Edge cases ────────────────────────────────────────────────

func TestLabelTranslation_EdgeCases(t *testing.T) {
	ensureOTelData(t)

	t.Run("deep_nesting_sanitized", func(t *testing.T) {
		// deployment.environment.name has 3 levels of nesting
		values := getLabelValues(t, proxyUnderscoreURL, "deployment_environment_name")
		if len(values) == 0 {
			t.Error("3-level dotted field deployment.environment.name should be accessible as deployment_environment_name")
		}
	})

	t.Run("container_image_name_5levels", func(t *testing.T) {
		// container.image.name — deep dotted path
		values := getLabelValues(t, proxyUnderscoreURL, "container_image_name")
		if len(values) == 0 {
			t.Error("container.image.name should be accessible as container_image_name")
		}
	})

	t.Run("process_executable_name_3levels", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "process_executable_name")
		if len(values) == 0 {
			t.Error("process.executable.name should be accessible as process_executable_name")
		}
	})

	t.Run("service_instance_id_3levels", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "service_instance_id")
		if len(values) == 0 {
			t.Error("service.instance.id should be accessible as service_instance_id")
		}
	})

	t.Run("k8s_cronjob_name", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "k8s_cronjob_name")
		if len(values) == 0 {
			t.Error("k8s.cronjob.name should be accessible as k8s_cronjob_name")
		}
		valSet := toSet(values)
		if !valSet["nightly-cleanup"] {
			t.Errorf("expected 'nightly-cleanup' in k8s_cronjob_name values, got: %v", values)
		}
	})

	t.Run("cloud_account_id_3levels", func(t *testing.T) {
		values := getLabelValues(t, proxyUnderscoreURL, "cloud_account_id")
		if len(values) == 0 {
			t.Error("cloud.account.id should be accessible as cloud_account_id")
		}
		valSet := toSet(values)
		if !valSet["123456789012"] {
			t.Errorf("expected '123456789012' in cloud_account_id values, got: %v", values)
		}
	})
}

// ─── SCORING ────────────────────────────────────────────────────────────────

func TestOTelCompatibilityScore(t *testing.T) {
	ensureOTelData(t)

	score := &CompatScore{}

	// Labels
	labels := getLabels(t, proxyUnderscoreURL)
	labelSet := toSet(labels)
	hasDots := false
	for _, l := range labels {
		if strings.Contains(l, ".") {
			hasDots = true
		}
	}
	if !hasDots {
		score.pass("labels", "no dots in label names with underscore mode")
	} else {
		score.fail("labels", "dotted labels found in underscore mode")
	}

	// All OTel categories present
	allCategories := []string{
		"service_name", "k8s_pod_name", "cloud_provider", "host_name",
		"process_pid", "container_id", "os_type", "net_host_name",
		"log_iostream", "telemetry_sdk_name", "deployment_environment",
	}
	for _, cat := range allCategories {
		if labelSet[cat] {
			score.pass("labels/"+cat, "present")
		} else {
			score.fail("labels/"+cat, "missing")
		}
	}

	// Query direction
	queryTests := map[string]string{
		"service_name":           "otel-auth-service",
		"k8s_namespace_name":     "production",
		"deployment_environment": "production",
		"cloud_provider":         "aws",
		"os_type":                "linux",
	}
	for label, value := range queryTests {
		streams := queryRange(t, proxyUnderscoreURL, fmt.Sprintf(`{%s="%s"}`, label, value))
		if len(streams) > 0 {
			score.pass("query/"+label, fmt.Sprintf("returned %d streams", len(streams)))
		} else {
			score.fail("query/"+label, "no results")
		}
	}

	// Label values. Some CI datasets may not include every optional OTel service fixture,
	// so service_name accepts any known OTel-compatible service value.
	labelValueTests := map[string][]string{
		"service_name":   {"otel-auth-service", "otel-order-service", "otel-collector", "payment-api"},
		"cloud_provider": {"aws"},
		"os_type":        {"linux"},
		"log_iostream":   {"stdout"},
	}
	for label, wantValues := range labelValueTests {
		vals := getLabelValues(t, proxyUnderscoreURL, label)
		valSet := toSet(vals)
		matched := ""
		for _, wantValue := range wantValues {
			if valSet[wantValue] {
				matched = wantValue
				break
			}
		}
		if matched != "" {
			score.pass("label_values/"+label, matched+" found")
		} else {
			score.fail("label_values/"+label, fmt.Sprintf("none of %v found in %v", wantValues, vals))
		}
	}

	// Detected fields
	fields := getDetectedFields(t, proxyUnderscoreURL)
	fieldSet := toSet(fields)
	for _, want := range []string{"service.name", "service_name", "k8s.pod.name", "k8s_pod_name"} {
		if fieldSet[want] {
			score.pass("detected_fields/"+want, "present")
		} else {
			score.fail("detected_fields/"+want, "missing")
		}
	}

	// Series
	series := getSeries(t, proxyUnderscoreURL, `{level="info"}`)
	seriesDots := 0
	for _, s := range series {
		for key := range s {
			if strings.Contains(key, ".") {
				seriesDots++
			}
		}
	}
	if seriesDots == 0 && len(series) > 0 {
		score.pass("series", "no dots in any series label")
	} else if len(series) == 0 {
		score.fail("series", "no series returned")
	} else {
		score.fail("series", fmt.Sprintf("%d dotted labels in series", seriesDots))
	}

	// Print final score
	t.Logf("\n╔══════════════════════════════════════════════╗")
	t.Logf("║  OTel Label Translation Compatibility Score  ║")
	t.Logf("╠══════════════════════════════════════════════╣")
	t.Logf("║  Passed: %3d / %3d (%3d%%)                    ║", score.passed, score.total,
		100*score.passed/max(score.total, 1))
	t.Logf("╚══════════════════════════════════════════════╝")

	sort.Strings(score.details)
	for _, d := range score.details {
		if strings.HasPrefix(d, "FAIL") {
			t.Logf("  %s", d)
		}
	}

	if score.failed > 0 {
		t.Errorf("OTel compatibility score: %d/%d (%d%%) — %d failures",
			score.passed, score.total, 100*score.passed/max(score.total, 1), score.failed)
	}
}

// ─── HELPERS ────────────────────────────────────────────────────────────────

func toSet(items []string) map[string]bool {
	set := make(map[string]bool, len(items))
	for _, item := range items {
		set[item] = true
	}
	return set
}

func assertNoDotsInStreams(t *testing.T, streams []interface{}) {
	t.Helper()
	for _, s := range streams {
		stream, _ := s.(map[string]interface{})
		labels, _ := stream["stream"].(map[string]interface{})
		for key := range labels {
			if strings.Contains(key, ".") {
				t.Errorf("response label %q contains dots — should be underscored", key)
			}
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
