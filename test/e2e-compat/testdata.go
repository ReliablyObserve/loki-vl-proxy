//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ingestRichTestData pushes realistic production-like logs into both
// Loki and VictoriaLogs for comprehensive compatibility testing.
func ingestRichTestData(t *testing.T) {
	t.Helper()
	now := time.Now()

	// ── Service: api-gateway ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "api-gateway", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "pod": "api-gateway-7f8d9c6b4-x2k9m",
			"container": "api-gateway", "level": "info",
		},
		Lines: []string{
			`{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":15,"trace_id":"abc123def456","user_id":"usr_42"}`,
			`{"method":"POST","path":"/api/v1/orders","status":201,"duration_ms":142,"trace_id":"def789abc012","user_id":"usr_99"}`,
			`{"method":"GET","path":"/api/v1/products","status":200,"duration_ms":8,"trace_id":"ghi345jkl678","user_id":"usr_42"}`,
			`{"method":"DELETE","path":"/api/v1/orders/123","status":403,"duration_ms":3,"trace_id":"mno901pqr234","user_id":"usr_01"}`,
			`{"method":"GET","path":"/health","status":200,"duration_ms":1}`,
			`{"method":"GET","path":"/ready","status":200,"duration_ms":1}`,
			`{"method":"GET","path":"/metrics","status":200,"duration_ms":2}`,
		},
	})

	// ── Service: api-gateway errors ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "api-gateway", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "pod": "api-gateway-7f8d9c6b4-x2k9m",
			"container": "api-gateway", "level": "error",
		},
		Lines: []string{
			`{"method":"POST","path":"/api/v1/payments","status":500,"duration_ms":5023,"error":"connection refused","trace_id":"err001","upstream":"payment-service"}`,
			`{"method":"POST","path":"/api/v1/payments","status":502,"duration_ms":30000,"error":"gateway timeout","trace_id":"err002","upstream":"payment-service"}`,
			`{"method":"GET","path":"/api/v1/users/999","status":404,"duration_ms":12,"error":"user not found","trace_id":"err003"}`,
		},
	})

	// ── Service: payment-service ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "payment-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "pod": "payment-svc-5c8f7d9a2-q7w3e",
			"container": "payment", "level": "error",
		},
		Lines: []string{
			`level=error msg="database connection pool exhausted" db=payments max_conns=50 active=50 waiting=127`,
			`level=error msg="transaction rollback" tx_id=tx_98765 reason="deadlock detected" table=orders`,
			`level=error msg="stripe API rate limited" status=429 retry_after=30s endpoint=/v1/charges`,
		},
	})

	// ── Service: payment-service warnings (logfmt) ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "payment-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "pod": "payment-svc-5c8f7d9a2-q7w3e",
			"container": "payment", "level": "warn",
		},
		Lines: []string{
			`level=warn msg="high latency on DB query" query="SELECT * FROM orders WHERE user_id=$1" duration=2.3s threshold=1s`,
			`level=warn msg="retry attempt" attempt=3 max_retries=5 service=stripe operation=charge`,
		},
	})

	// ── Service: nginx ingress (access log format) ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "nginx-ingress", "namespace": "ingress-nginx", "env": "production",
			"cluster": "us-east-1", "pod": "nginx-ingress-controller-abc123",
			"container": "nginx", "level": "info",
		},
		Lines: []string{
			`10.0.1.42 - admin [03/Apr/2026:10:30:00 +0000] "GET /api/v1/users HTTP/1.1" 200 1234 "-" "Mozilla/5.0" 0.015`,
			`10.0.1.43 - - [03/Apr/2026:10:30:01 +0000] "POST /api/v1/orders HTTP/1.1" 201 567 "-" "curl/7.88" 0.142`,
			`192.168.1.100 - - [03/Apr/2026:10:30:02 +0000] "GET /api/v1/products HTTP/1.1" 200 8901 "-" "Python/3.11" 0.008`,
			`10.0.2.55 - - [03/Apr/2026:10:30:03 +0000] "POST /api/v1/payments HTTP/1.1" 500 234 "-" "Java/17" 5.023`,
			`10.0.2.55 - attacker [03/Apr/2026:10:30:04 +0000] "GET /admin/../../../etc/passwd HTTP/1.1" 400 0 "-" "sqlmap/1.7" 0.001`,
		},
	})

	// ── Service: staging environment ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "api-gateway", "namespace": "staging", "env": "staging",
			"cluster": "us-west-2", "pod": "api-gateway-staging-abc",
			"container": "api-gateway", "level": "info",
		},
		Lines: []string{
			`{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":25}`,
			`{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":30}`,
		},
	})

	// ── Service: otel-auth-service (OTel with full semantic conventions) ──
	// VL-only: Loki does not natively support dotted stream labels in push API.
	// OTel data reaches VL directly (via collector or jsonline), not via Loki push.
	pushStream(t, now, streamDef{
		VLOnly: true,
		Labels: map[string]string{
			"service.name":           "otel-auth-service",
			"service.namespace":      "auth-namespace",
			"k8s.cluster.name":       "us-east-1",
			"k8s.namespace.name":     "prod",
			"k8s.pod.name":           "auth-svc-xyz789",
			"k8s.pod.uid":            "pod-uid-12345",
			"k8s.container.name":     "auth-container",
			"k8s.node.name":          "worker-node-1",
			"deployment.name":        "auth-service",
			"deployment.environment": "production",
			"deployment.version":     "v2.1.0",
			"host.name":              "host-auth-1",
			"host.arch":              "amd64",
			"telemetry.sdk.name":     "opentelemetry",
			"telemetry.sdk.language": "go",
			"telemetry.sdk.version":  "1.21.0",
			"level":                  "info",
		},
		Lines: []string{
			`{"client_ip":"10.0.1.1","method":"POST","path":"/auth/login","status":200,"duration_ms":45,"trace_id":"otel001trace","span_id":"otel001span","user":"alice","request_id":"req-001"}`,
			`{"client_ip":"10.0.1.2","method":"POST","path":"/auth/verify","status":200,"duration_ms":12,"trace_id":"otel002trace","span_id":"otel002span","user":"bob","request_id":"req-002"}`,
			`{"client_ip":"10.0.1.3","method":"POST","path":"/auth/logout","status":200,"duration_ms":8,"trace_id":"otel003trace","span_id":"otel003span","user":"charlie","request_id":"req-003"}`,
		},
	})

	// ── Service: otel-api-service (OTel data with minimal stream labels) ──
	// VL-only: OTel attributes in message JSON, not via Loki push
	pushStream(t, now, streamDef{
		VLOnly: true,
		Labels: map[string]string{
			"app": "otel-api-service", "namespace": "prod",
			"cluster": "us-east-1", "level": "info",
		},
		Lines: []string{
			`{"service.name":"otel-api-service","k8s.pod.name":"api-xyz123","span_id":"span-001","trace_id":"trace-001","http.method":"GET","http.status_code":200,"http.url":"/api/endpoint","duration_ms":25}`,
			`{"service.name":"otel-api-service","k8s.pod.name":"api-xyz123","span_id":"span-002","trace_id":"trace-002","http.method":"POST","http.status_code":201,"http.url":"/api/resource","duration_ms":145}`,
			`{"service.name":"otel-api-service","k8s.pod.name":"api-xyz123","span_id":"span-003","trace_id":"trace-003","http.method":"GET","http.status_code":404,"http.url":"/api/notfound","duration_ms":3}`,
		},
	})

	// ── Service: otel-collector-native (OTel collector with underscore-translated labels) ──
	// VL-only: Pre-translated underscore conventions from OTel collector
	pushStream(t, now, streamDef{
		VLOnly: true,
		Labels: map[string]string{
			"service_name":           "otel-collector",
			"service_namespace":      "observability",
			"k8s_cluster_name":       "us-east-1",
			"k8s_namespace_name":     "observability",
			"k8s_pod_name":           "otel-collector-uvw456",
			"k8s_container_name":     "otel-collector",
			"deployment_environment": "prod",
			"telemetry_sdk_name":     "opentelemetry",
			"telemetry_sdk_language": "go",
			"level":                  "info",
		},
		Lines: []string{
			`{"spans_received":1337,"spans_exported":1334,"spans_dropped":3,"exporter":"otlp","backend":"victorialogs","duration_ms":42}`,
			`{"metrics_received":5000,"metrics_exported":4998,"metrics_dropped":2,"exporter":"prometheus","backend":"victoriametrics","duration_ms":31}`,
			`{"logs_received":2500,"logs_exported":2500,"logs_dropped":0,"exporter":"loki","backend":"loki","duration_ms":18}`,
		},
	})

	// ── Service: with dots in labels (OTel-style) ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "otel-collector", "namespace": "observability",
			"k8s.cluster.name": "us-east-1", "k8s.namespace.name": "prod",
			"k8s.pod.name": "otel-collector-abc", "level": "info",
		},
		Lines: []string{
			`{"msg":"exporting traces","exporter":"otlp","spans":42,"duration":"15ms"}`,
			`{"msg":"exporting metrics","exporter":"prometheus","metrics":1337,"duration":"8ms"}`,
		},
	})

	// ── Multiline / special chars ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "java-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "error",
		},
		Lines: []string{
			"java.lang.NullPointerException: Cannot invoke method on null object\n\tat com.example.Service.process(Service.java:42)\n\tat com.example.Handler.handle(Handler.java:15)",
			"com.example.TimeoutException: Request timed out after 30s\n\tat com.example.Client.call(Client.java:88)",
		},
	})

	// ── Duration/bytes logs (for unwrap duration() / unwrap bytes() tests) ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "duration-bytes-test", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		Lines: []string{
			`{"endpoint":"/api/users","response_time":"15ms","body_size":"1024B","status":200}`,
			`{"endpoint":"/api/orders","response_time":"142ms","body_size":"2048B","status":201}`,
			`{"endpoint":"/api/products","response_time":"8ms","body_size":"512B","status":200}`,
			`{"endpoint":"/api/payments","response_time":"5023ms","body_size":"256B","status":500}`,
		},
	})

	// ── Pattern-matchable logs (for |> / !> pattern match line filter tests) ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "pattern-filter-test", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		Lines: []string{
			`user=alice action=login ip=10.0.1.1 result=success`,
			`user=bob action=purchase ip=10.0.1.2 result=success`,
			`user=charlie action=login ip=10.0.1.3 result=failure`,
			`user=alice action=logout ip=10.0.1.1 result=success`,
			`user=bob action=login ip=10.0.1.4 result=success`,
			`user=charlie action=purchase ip=10.0.1.3 result=success`,
		},
	})

	// ── Unpack parser test logs (standard JSON, unpack is equivalent to json for non-packed lines) ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "unpack-test", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		Lines: []string{
			`{"method":"GET","path":"/api/v1/items","status":200,"latency_ms":12}`,
			`{"method":"POST","path":"/api/v1/items","status":201,"latency_ms":45}`,
			`{"method":"DELETE","path":"/api/v1/items/7","status":404,"latency_ms":3}`,
			`{"method":"PUT","path":"/api/v1/items/1","status":200,"latency_ms":18}`,
		},
	})

	time.Sleep(5 * time.Second) // VL needs time to index, especially in CI
	t.Log("Rich test data ingested successfully")
}

type streamDef struct {
	Labels map[string]string
	Lines  []string
	VLOnly bool // If true, push only to VL (not Loki) — for OTel data with dotted labels
}

func pushStream(t *testing.T, baseTime time.Time, sd streamDef) {
	t.Helper()

	// Push to Loki
	values := make([][]string, len(sd.Lines))
	for i, line := range sd.Lines {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		values[i] = []string{fmt.Sprintf("%d", ts.UnixNano()), line}
	}

	if !sd.VLOnly {
		lokiPayload := map[string]interface{}{
			"streams": []map[string]interface{}{
				{"stream": sd.Labels, "values": values},
			},
		}
		body, _ := json.Marshal(lokiPayload)
		resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Logf("Loki push (%s): failed: %v", sd.Labels["app"], err)
		} else {
			resp.Body.Close()
		}
	}

	// Push to VictoriaLogs
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

	// Build stream fields from labels
	streamFields := []string{}
	for k := range sd.Labels {
		if k != "level" { // level varies within app, not a good stream field
			streamFields = append(streamFields, k)
		}
	}

	resp, err := http.Post(
		vlURL+"/insert/jsonline?_stream_fields="+strings.Join(streamFields, ","),
		"application/stream+json",
		strings.NewReader(strings.Join(vlLines, "\n")),
	)
	if err != nil {
		t.Logf("VL push (%s): failed: %v", sd.Labels["app"], err)
	} else {
		resp.Body.Close()
	}
}
