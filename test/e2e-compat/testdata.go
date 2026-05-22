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
	// Use a past timestamp so all entries are guaranteed to be in Loki's chunk
	// storage (not the ingester head block) by the time metric queries run.
	now := time.Now().Add(-3 * time.Minute)

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
			"cluster": "us-east-1", "level": "info", "env": "internal",
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

	// ── OTel vs Non-OTel comparison pair: frontend service ──
	//
	// OTel path: data arrives via OTel collector into VL directly.
	// Stream labels use dotted OTel conventions. Extra OTel span attributes
	// (trace_id, span_id, http.method etc.) are pushed as extra VL fields
	// and surface as structuredMetadata through the proxy.
	pushStream(t, now, streamDef{
		VLOnly: true,
		Labels: map[string]string{
			"service.name":           "otel-frontend",
			"service.namespace":      "web",
			"k8s.cluster.name":       "us-east-1",
			"k8s.namespace.name":     "prod",
			"k8s.pod.name":           "otel-frontend-abc123",
			"k8s.container.name":     "frontend",
			"deployment.environment": "production",
			"deployment.version":     "v3.2.1",
			"telemetry.sdk.name":     "opentelemetry",
			"telemetry.sdk.language": "javascript",
			"level":                  "info",
		},
		StructuredMetadata: map[string]string{
			"trace_id":         "otel-trace-fe-001",
			"span_id":          "otel-span-fe-001",
			"http.method":      "GET",
			"http.target":      "/dashboard",
			"http.status_code": "200",
			"net.peer.ip":      "10.0.1.50",
		},
		Lines: []string{
			`{"event":"page_load","route":"/dashboard","user_id":"usr-01","duration_ms":320,"cache_hit":false,"assets_loaded":12}`,
			`{"event":"api_call","endpoint":"/api/v1/metrics","user_id":"usr-01","duration_ms":45,"status":200,"retries":0}`,
			`{"event":"page_load","route":"/settings","user_id":"usr-02","duration_ms":180,"cache_hit":true,"assets_loaded":8}`,
			`{"event":"error","route":"/reports","user_id":"usr-03","error":"timeout","duration_ms":5000,"retries":3}`,
			`{"event":"api_call","endpoint":"/api/v1/users","user_id":"usr-02","duration_ms":22,"status":200,"retries":0}`,
		},
	})

	// Non-OTel path: equivalent frontend service using Loki push (underscore labels).
	// Trace context is structured metadata, not embedded in stream labels.
	// Both Loki and VL receive identical data for side-by-side comparison.
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "frontend", "service_name": "frontend",
			"namespace": "prod", "cluster": "us-east-1",
			"version": "v3.2.1", "env": "production", "level": "info",
		},
		StructuredMetadata: map[string]string{
			"trace_id":   "nonotel-trace-fe-001",
			"span_id":    "nonotel-span-fe-001",
			"request_id": "req-nonotel-001",
			"user_agent": "Mozilla/5.0 (compatible; test)",
		},
		Lines: []string{
			`{"event":"page_load","route":"/dashboard","user_id":"usr-01","duration_ms":310,"cache_hit":false,"assets_loaded":11}`,
			`{"event":"api_call","endpoint":"/api/v1/metrics","user_id":"usr-01","duration_ms":48,"status":200,"retries":0}`,
			`{"event":"page_load","route":"/settings","user_id":"usr-02","duration_ms":175,"cache_hit":true,"assets_loaded":8}`,
			`{"event":"error","route":"/reports","user_id":"usr-03","error":"timeout","duration_ms":5001,"retries":3}`,
			`{"event":"api_call","endpoint":"/api/v1/users","user_id":"usr-02","duration_ms":25,"status":200,"retries":0}`,
		},
	})

	// ── OTel vs Non-OTel: backend worker service ──
	//
	// OTel path: background job worker instrumented with OTel SDK.
	// Rich resource attributes + span attributes as structured metadata.
	pushStream(t, now, streamDef{
		VLOnly: true,
		Labels: map[string]string{
			"service.name":           "otel-worker",
			"service.namespace":      "jobs",
			"k8s.cluster.name":       "us-east-1",
			"k8s.namespace.name":     "prod",
			"k8s.pod.name":           "otel-worker-def456",
			"k8s.container.name":     "worker",
			"deployment.environment": "production",
			"deployment.version":     "v1.8.0",
			"telemetry.sdk.name":     "opentelemetry",
			"telemetry.sdk.language": "python",
			"level":                  "info",
		},
		StructuredMetadata: map[string]string{
			"trace_id":         "otel-trace-worker-001",
			"span_id":          "otel-span-worker-001",
			"job.type":         "email",
			"job.queue":        "notifications",
			"messaging.system": "rabbitmq",
		},
		Lines: []string{
			`{"event":"job_started","job_id":"job-001","type":"email","recipient":"alice@example.com","priority":"high"}`,
			`{"event":"job_completed","job_id":"job-001","duration_ms":234,"emails_sent":1,"status":"success"}`,
			`{"event":"job_started","job_id":"job-002","type":"report","report_id":"rpt-042","priority":"low"}`,
			`{"event":"job_failed","job_id":"job-002","error":"disk_full","retryable":true,"attempt":2}`,
			`{"event":"job_started","job_id":"job-003","type":"cleanup","target":"temp_files","files_count":1024}`,
			`{"event":"job_completed","job_id":"job-003","duration_ms":8901,"files_deleted":987,"status":"success"}`,
		},
	})

	// Non-OTel equivalent worker (both Loki + VL).
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "worker", "service_name": "worker",
			"namespace": "prod", "cluster": "us-east-1",
			"version": "v1.8.0", "env": "production", "level": "info",
		},
		StructuredMetadata: map[string]string{
			"trace_id":   "nonotel-trace-worker-001",
			"span_id":    "nonotel-span-worker-001",
			"queue_name": "notifications",
			"worker_id":  "worker-node-7",
		},
		Lines: []string{
			`{"event":"job_started","job_id":"job-101","type":"email","recipient":"bob@example.com","priority":"high"}`,
			`{"event":"job_completed","job_id":"job-101","duration_ms":198,"emails_sent":1,"status":"success"}`,
			`{"event":"job_started","job_id":"job-102","type":"report","report_id":"rpt-043","priority":"low"}`,
			`{"event":"job_failed","job_id":"job-102","error":"db_timeout","retryable":true,"attempt":1}`,
			`{"event":"job_started","job_id":"job-103","type":"cleanup","target":"logs","files_count":512}`,
			`{"event":"job_completed","job_id":"job-103","duration_ms":4521,"files_deleted":498,"status":"success"}`,
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

	// ── Drop-error edge stream (used by TestEdge_* tests in semantics group) ──
	// Must be here so ensureDataIngested covers it without requiring
	// TestSetup_IngestEdgeCaseData to run first.
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-drop-error", "namespace": "edge-tests", "level": "info",
		},
		Lines: []string{
			`{"msg":"request processed","method":"GET","status":200}`,
			`{"msg":"request processed","method":"POST","status":201}`,
			`{"msg":"request failed","method":"POST","status":500,"error":"timeout"}`,
		},
	})

	// ── Structured metadata: checkout-service with trace/span context ──
	// Uses a unique app name to avoid cardinality collision with the existing
	// api-gateway streams (which carry a pod label that would cause many-to-one
	// matching errors in binary metric queries).
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "checkout-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		StructuredMetadata: map[string]string{
			"trace_id":   "abc123def456789",
			"span_id":    "span001xyz",
			"request_id": "req-001-structured",
			"user_agent": "Mozilla/5.0 (compatible; e2e-test)",
		},
		Lines: []string{
			`{"method":"GET","path":"/api/v1/dashboard","status":200,"duration_ms":22}`,
			`{"method":"POST","path":"/api/v1/checkout","status":200,"duration_ms":315}`,
			`{"method":"GET","path":"/api/v1/cart","status":200,"duration_ms":11}`,
		},
	})

	// ── Structured metadata: billing-service with cloud context ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "billing-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		StructuredMetadata: map[string]string{
			"cloud.region":    "us-east-1",
			"cloud.provider":  "aws",
			"k8s.node.name":   "node-worker-42",
			"deployment.name": "billing-service-v3",
		},
		Lines: []string{
			`level=info msg="invoice generated" amount=99.99 currency=USD invoice_id=inv_12345`,
			`level=info msg="invoice generated" amount=49.50 currency=EUR invoice_id=inv_12346`,
			`level=info msg="credit issued" amount=15.00 currency=USD invoice_id=inv_12347`,
		},
	})

	// ── Structured metadata: auth-service with OTel-style resource attributes ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "auth-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		StructuredMetadata: map[string]string{
			"telemetry.sdk.name":     "opentelemetry",
			"telemetry.sdk.language": "go",
			"telemetry.sdk.version":  "1.21.0",
			"deployment.version":     "v1.5.2",
			"host.name":              "auth-host-1",
		},
		Lines: []string{
			`{"event":"login","user":"alice@example.com","ip":"10.0.1.1","mfa":true,"duration_ms":45}`,
			`{"event":"token_refresh","user":"bob@example.com","ip":"10.0.1.2","mfa":false,"duration_ms":12}`,
			`{"event":"logout","user":"charlie@example.com","ip":"10.0.1.3","mfa":true,"duration_ms":8}`,
		},
	})

	// ── Parsed fields: rich JSON logs for | json pipeline testing ──
	// These JSON logs contain many nested fields that can be extracted via | json.
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "order-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		Lines: []string{
			`{"event":"order_created","order_id":"ord-001","user_id":"usr-42","items":3,"total":149.99,"currency":"USD","payment_method":"card","shipping":"express","region":"us-east"}`,
			`{"event":"order_shipped","order_id":"ord-001","tracking_id":"TRK123456","carrier":"fedex","estimated_delivery":"2026-05-25","weight_kg":2.5}`,
			`{"event":"order_delivered","order_id":"ord-001","delivery_time_ms":345678,"signature_required":true,"delivered_to":"alice"}`,
			`{"event":"order_created","order_id":"ord-002","user_id":"usr-99","items":1,"total":29.99,"currency":"EUR","payment_method":"paypal","shipping":"standard","region":"eu-west"}`,
			`{"event":"order_failed","order_id":"ord-003","user_id":"usr-01","error":"payment_declined","code":4001,"retryable":false}`,
		},
	})

	// ── Parsed fields: logfmt logs for | logfmt pipeline testing ──
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "inventory-service", "namespace": "prod", "env": "production",
			"cluster": "us-east-1", "level": "info",
		},
		Lines: []string{
			`level=info msg="stock check" sku=SKU-001 quantity=150 warehouse=east-1 reserved=12`,
			`level=info msg="stock updated" sku=SKU-002 delta=-5 new_quantity=95 reason=sale`,
			`level=warn msg="low stock alert" sku=SKU-003 quantity=3 threshold=10 auto_reorder=true`,
			`level=info msg="restock received" sku=SKU-001 added=500 supplier=acme delivery_id=DEL-789`,
			`level=error msg="stock sync failed" warehouse=west-2 error="connection timeout" retry_in=30s`,
		},
	})

	time.Sleep(5 * time.Second) // VL needs time to index, especially in CI
	t.Log("Rich test data ingested successfully")
}

type streamDef struct {
	Labels             map[string]string
	Lines              []string
	StructuredMetadata map[string]string // applied to all lines; exposed via categorize-labels
	VLOnly             bool              // push only to VL (not Loki) — for OTel data with dotted labels
}

func pushStream(t *testing.T, baseTime time.Time, sd streamDef) {
	t.Helper()

	if !sd.VLOnly {
		// Loki: use 3-element values when structured metadata is set.
		var lokiValues []interface{}
		for i, line := range sd.Lines {
			ts := baseTime.Add(time.Duration(i) * time.Second)
			tsStr := fmt.Sprintf("%d", ts.UnixNano())
			if len(sd.StructuredMetadata) > 0 {
				lokiValues = append(lokiValues, []interface{}{tsStr, line, sd.StructuredMetadata})
			} else {
				lokiValues = append(lokiValues, []string{tsStr, line})
			}
		}
		lokiPayload := map[string]interface{}{
			"streams": []map[string]interface{}{
				{"stream": sd.Labels, "values": lokiValues},
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

	// VictoriaLogs: structured metadata fields are pushed as extra non-stream fields.
	var vlLines []string
	for i, line := range sd.Lines {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		entry := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": line}
		for k, v := range sd.Labels {
			entry[k] = v
		}
		for k, v := range sd.StructuredMetadata {
			entry[k] = v
		}
		j, _ := json.Marshal(entry)
		vlLines = append(vlLines, string(j))
	}

	// Stream fields: only label keys (not structured metadata keys) — this
	// ensures metadata fields appear as structuredMetadata in categorize-labels.
	streamFields := make([]string, 0, len(sd.Labels))
	for k := range sd.Labels {
		streamFields = append(streamFields, k)
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
