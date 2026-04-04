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

	time.Sleep(5 * time.Second) // VL needs time to index, especially in CI
	t.Log("Rich test data ingested successfully")
}

type streamDef struct {
	Labels map[string]string
	Lines  []string
}

func pushStream(t *testing.T, baseTime time.Time, sd streamDef) {
	t.Helper()

	// Push to Loki
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
		t.Logf("Loki push (%s): failed: %v", sd.Labels["app"], err)
	} else {
		resp.Body.Close()
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

	resp, err = http.Post(
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
