---
sidebar_label: Logging for Compatibility
description: Deep-dive guide on structuring logs for simultaneous Loki and VictoriaLogs compatibility — covering JSON, logfmt, OTel SDK, OTel Collector, stream labels, and the _msg field.
---

# Logging for Loki + VictoriaLogs Compatibility

This guide covers every format decision that affects whether queries against VictoriaLogs (via loki-vl-proxy) and Loki return identical results. It is aimed at platform engineers writing ingestion configs, application developers choosing log formats, and observability teams migrating from Loki to VictoriaLogs.

---

## 1. How the Two Systems Differ

Understanding the one fundamental difference is the key to everything else.

### Loki: line-oriented storage

Loki stores the raw log line verbatim — exactly the bytes pushed by the client. Text filters (`|= "text"`, `!= "text"`, `|~ "regex"`) search those raw bytes. There is no separate "message" field; the whole line is the message.

```
Line:  {"method":"POST","path":"/api/v1/payments","status":502,"error":"gateway timeout"}
Query: {app="api"} |= "timeout"   →  searches all bytes above → MATCH
```

### VictoriaLogs: document-oriented storage

VL stores logs as structured documents where `_msg` is the designated "primary" message field. Text phrase filters in LogsQL (translated from LogQL by the proxy) always search `_msg`:

```
LogQL:   {app="api"} |= "timeout"
LogsQL:  app:=api -"timeout"         # phrase filter on _msg
```

If `_msg` does not contain the string "timeout", the filter finds nothing — even if "timeout" appears in other stored fields.

### What VL uses as `_msg` — by delivery path

| Delivery path | VL `_msg` value |
|---|---|
| Loki push API — plain text | Full raw line |
| Loki push API — JSON, **no** `_msg` field | Full JSON string (same bytes Loki stores) ✓ |
| Loki push API — JSON, **with** `_msg` field | Only the `_msg` value — other fields invisible to text filters ✗ |
| OTel OTLP (`/insert/opentelemetry/v1/logs`) | `body` string attribute |
| VL JSON insert (`/insert/jsonline`) | Explicit `_msg` field value |

The single source of divergence: explicitly setting `_msg` in JSON logs to a short summary while searchable content (error messages, upstream names, status codes) lives in other fields.

---

## 2. VictoriaLogs JSON Parsing Behavior

### Auto-extraction of JSON fields

When VL receives a JSON log line via the Loki push API, it automatically parses the JSON and stores each key as a searchable VL field — in addition to keeping the full JSON string as `_msg`. This gives you both text filter parity with Loki AND structured field access:

```json
{"method":"POST","path":"/api/v1/payments","status":502,"error":"gateway timeout","trace_id":"err002"}
```

VL stores:
```
_msg     = '{"method":"POST","path":"/api/v1/payments","status":502,"error":"gateway timeout","trace_id":"err002"}'
method   = POST
path     = /api/v1/payments
status   = 502
error    = gateway timeout
trace_id = err002
```

Result:
- `|= "timeout"` searches `_msg` → full JSON → **finds "gateway timeout"** ✓
- `| json | error="gateway timeout"` uses the extracted `error` field ✓
- Both Loki and VL return this line for both query forms ✓

This auto-parsing is enabled by default. It can be disabled with `-loki.disableMessageParsing` on the VL binary (rarely needed).

### What breaks parity: explicit `_msg`

```json
{"_msg":"POST /api/v1/payments 502 30000ms","method":"POST","path":"/api/v1/payments","status":502,"error":"gateway timeout"}
```

VL stores `_msg` = `"POST /api/v1/payments 502 30000ms"` — the explicit value wins. The "gateway timeout" string is in the `error` field but not in `_msg`. So:

- `|= "timeout"` searches `_msg` = `"POST /api/v1/payments 502 30000ms"` → **no match** ✗
- Loki searches the full JSON raw line → **match** ✓
- Result: 3 lines VL returns that Loki excludes (or vice versa)

---

## 3. JSON Logs via Loki Push API

### Rule: Never set `_msg` in JSON log entries

Keep `_msg` out of the payload. VL uses the full JSON string and both systems agree.

**Correct format:**
```json
{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":42,"trace_id":"abc123def456","user_id":"usr_42","level":"info"}
```

**For error entries, include error details in the JSON body:**
```json
{"method":"POST","path":"/api/v1/payments","status":502,"duration_ms":30000,"error":"gateway timeout","upstream":"payment-service","trace_id":"err002","level":"error"}
```

Both `|= "timeout"` and `|= "gateway"` search the full JSON — both systems agree.

**Do not do:**
```json
{"_msg":"POST /api/v1/payments 502 30000ms","error":"gateway timeout"}
```

### Stream label cardinality — the `level` rule

Stream labels in Loki create separate chunk files per unique combination. `level` is bounded and high-signal — promote it to a stream label so metric queries (`count_over_time` by level) are efficient.

**Correct:** One stream per level
```
{app="api-gateway", namespace="prod", cluster="us-east-1", level="info"}
{app="api-gateway", namespace="prod", cluster="us-east-1", level="error"}
{app="api-gateway", namespace="prod", cluster="us-east-1", level="warn"}
```

**Do not index unbounded fields** as stream labels:
```
# Wrong — stream label cardinality explodes:
{app="api-gateway", trace_id="abc123def456"}  # millions of unique trace IDs
{app="api-gateway", user_id="usr_42"}          # millions of users
```

Keep high-cardinality values in the JSON body, not as stream labels.

---

## 4. logfmt Logs via Loki Push API

logfmt is the most common source of Loki/VL parity failures. The issue stems from how Loki handles the `level` field.

### Why parity fails with logfmt

```
Stream label:  {app="payment-service", level="info"}
Log line:      level=error msg="payment failed" tx_id=tx_98765 error_code=card_declined
```

**Loki** evaluates `{app="payment-service"} | logfmt | level="error"`:
1. `{app="payment-service"}` — selects this stream ✓
2. `| logfmt` — parses the line; finds `level=error` but stream label is `level="info"`
3. `level="error"` — Loki checks the **stream label** `level`, which is `"info"` → **no match** ✗
4. The parsed `level` field becomes `level_extracted` (Loki convention for field/label name collisions)

**VictoriaLogs** translates `| logfmt | level="error"` to `unpack_logfmt | filter level:=error`:
1. `unpack_logfmt` — parses the logfmt line; extracts `level=error`
2. `filter level:=error` — checks the **parsed field** `level="error"` → **match** ✓

Result: VL returns the line; Loki does not. **Filter results diverge.**

### The fix: one stream per level, always

Split logfmt logs into per-level streams so the stream label `level` always matches the `level=...` field in the log line:

**Correct:**
```
Stream: {app="payment-service", namespace="prod", cluster="us-east-1", level="error"}
Line:   level=error msg="payment failed" tx_id=tx_98765 error_code=card_declined amount=99.99 currency=USD

Stream: {app="payment-service", namespace="prod", cluster="us-east-1", level="warn"}
Line:   level=warn msg="payment retry" attempt=3 max_retries=5 provider=stripe tx_id=tx_11111

Stream: {app="payment-service", namespace="prod", cluster="us-east-1", level="info"}
Line:   level=info msg="payment processed" tx_id=tx_22222 amount=149.99 currency=EUR provider=paypal
```

### Field name collision avoidance

When a logfmt field has the same name as a stream label, Loki renames the parsed field to `<field>_extracted`. Avoid stream labels that conflict with commonly parsed fields:

| Stream label | Risk | Parsed field | Loki name |
|---|---|---|---|
| `level` | High | `level=error` in logfmt | `level_extracted` |
| `method` | Medium | `method=GET` in logfmt | `method_extracted` |
| `status` | Medium | `status=200` in logfmt | `status_extracted` |

This is why `level` MUST be a stream label that matches the logfmt content — not a static "info" label applied to a mixed-level stream.

---

## 5. OTel Semantic Conventions

OpenTelemetry defines a structured log data model with distinct fields that map to VL and Loki storage concepts.

### OTel Log Record Structure

```
LogRecord {
  Timestamp:      time when the event occurred
  ObservedTimestamp: time the log was received by the collector
  SeverityNumber: numeric severity (TRACE=1 ... FATAL=24)
  SeverityText:   human-readable ("DEBUG","INFO","WARN","ERROR","FATAL")
  Body:           AnyValue — the primary log message (maps to _msg in VL)
  Attributes:     key-value fields (maps to VL log fields)
  Resource:       service identity (maps to VL stream fields)
  InstrumentationScope: library/module that emitted the log
  TraceId:        W3C trace context
  SpanId:         W3C span context
}
```

### The `Body` field is `_msg`

VL maps the OTel `Body` string directly to `_msg`. Text filters in LogQL (`|= "timeout"`) become phrase filters on `_msg` in LogsQL. Therefore:

- **Put the human-readable log message in `Body`** — everything users will search for
- **Do NOT put only a short code like `"http_request"` in Body** — that makes text filtering useless

**Body that enables text filter parity:**
```
Body: "payment failed: gateway timeout (upstream: payment-service, amount: 99.99 USD)"
```

Text filter `|= "timeout"` finds this in both Loki (raw line contains it in structured format) and VL (body → `_msg`).

**Body that breaks text filter parity:**
```
Body: "http.request"  # only a type code
Attributes: {error: "gateway timeout", upstream: "payment-service"}
```

`|= "timeout"` searches `_msg` = `"http.request"` → no match in VL. Loki reconstructs a raw line that includes attributes → may match.

### Resource attributes → stream fields

OTel resource attributes describe the service and infrastructure. These map to VL stream fields and Loki stream labels. The proxy translates between dotted VL names and underscore Loki names.

**Standard resource attributes to always include:**

| OTel Resource Attribute | VL stream field | Loki label (proxy) | Example |
|---|---|---|---|
| `service.name` | `service.name` | `service_name` | `"payment-service"` |
| `service.namespace` | `service.namespace` | `service_namespace` | `"prod"` |
| `service.version` | `service.version` | `service_version` | `"v2.3.1"` |
| `deployment.environment` | `deployment.environment` | `deployment_environment` | `"production"` |
| `k8s.namespace.name` | `k8s.namespace.name` | `k8s_namespace_name` | `"prod"` |
| `k8s.pod.name` | `k8s.pod.name` | `k8s_pod_name` | `"payment-svc-7f8d9c6b4"` |
| `k8s.container.name` | `k8s.container.name` | `k8s_container_name` | `"payment"` |
| `k8s.cluster.name` | `k8s.cluster.name` | `k8s_cluster_name` | `"us-east-1"` |
| `host.name` | `host.name` | `host_name` | `"ip-10-0-1-42"` |

### Log attributes → VL fields (not stream fields)

Log-level attributes (per-record, high-cardinality) stay as VL log fields, not stream fields:

```python
# These go in Attributes, not Resource:
logger.error("payment failed",
    # Log attributes — per-request, high cardinality
    trace_id="abc123",          # W3C trace context
    span_id="def456",
    user_id="usr_42",
    transaction_id="tx_98765",
    amount=99.99,
    currency="USD",
    error="gateway timeout",
    upstream="payment-service",
)
```

VL stores these as log fields accessible via `| unpack_json` or field filters, not as stream selectors.

### Severity → level

OTel `SeverityText` maps to the `level` field in VL. For Loki label parity, promote `level` (derived from `SeverityText`) to a stream label via the OTel Collector Loki exporter:

```yaml
exporters:
  loki:
    labels:
      attributes:
        level: ""  # Promotes SeverityText as "level" stream label
```

This ensures `{level="error"}` works as a stream selector in Loki and VL equally.

---

## 6. OTel SDK Examples

### Go

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os"

    "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
    sdklog "go.opentelemetry.io/otel/sdk/log"
    "go.opentelemetry.io/otel/sdk/resource"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
    "go.opentelemetry.io/contrib/bridges/otelslog"
)

func initLogger(ctx context.Context) (*sdklog.LoggerProvider, error) {
    // OTLP exporter — sends to OTel Collector which dual-writes to Loki + VL
    exp, err := otlploghttp.New(ctx,
        otlploghttp.WithEndpoint("otel-collector:4318"),
        otlploghttp.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    // Resource: always include service.name + k8s attributes
    res, err := resource.New(ctx,
        resource.WithAttributes(
            semconv.ServiceName("payment-service"),
            semconv.ServiceNamespace("prod"),
            semconv.ServiceVersion("v2.3.1"),
            semconv.DeploymentEnvironment("production"),
            semconv.K8SNamespaceName("prod"),
            semconv.K8SPodName(os.Getenv("HOSTNAME")),
            semconv.K8SContainerName("payment"),
        ),
    )
    if err != nil {
        return nil, err
    }

    provider := sdklog.NewLoggerProvider(
        sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
        sdklog.WithResource(res),
    )
    return provider, nil
}

func main() {
    ctx := context.Background()
    provider, _ := initLogger(ctx)
    defer provider.Shutdown(ctx)

    // Bridge to slog — Body = formatted message string
    logger := otelslog.NewLogger("payment-service",
        otelslog.WithLoggerProvider(provider),
    )

    // Body should contain full searchable text
    // Attributes carry structured context
    logger.ErrorContext(ctx,
        // Body — maps to _msg in VL — must contain searchable content
        fmt.Sprintf("payment failed: %s (upstream: %s)", "gateway timeout", "payment-service"),
        slog.String("error", "gateway timeout"),
        slog.String("upstream", "payment-service"),
        slog.String("tx_id", "tx_98765"),
        slog.Float64("amount", 99.99),
        slog.String("currency", "USD"),
        slog.String("provider", "stripe"),
    )
}
```

### Python

```python
import logging
import opentelemetry.sdk.logs as otel_logs
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk.logs import LoggerProvider
from opentelemetry.sdk.logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

# Resource — always set these attributes
resource = Resource.create({
    ResourceAttributes.SERVICE_NAME: "payment-service",
    ResourceAttributes.SERVICE_NAMESPACE: "prod",
    ResourceAttributes.SERVICE_VERSION: "v2.3.1",
    ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "production",
    ResourceAttributes.K8S_NAMESPACE_NAME: "prod",
    ResourceAttributes.K8S_POD_NAME: "payment-svc-7f8d9c6b4",
})

provider = LoggerProvider(resource=resource)
provider.add_log_record_processor(
    BatchLogRecordProcessor(
        OTLPLogExporter(endpoint="http://otel-collector:4318/v1/logs")
    )
)

# Hook into Python logging
handler = otel_logs.LoggingHandler(level=logging.NOTSET, logger_provider=provider)
logging.basicConfig(handlers=[handler])
log = logging.getLogger("payment-service")

# Body = full human-readable message (maps to _msg in VL)
# Extra kwargs become log attributes (structured fields)
log.error(
    "payment failed: gateway timeout (upstream: payment-service)",
    extra={
        "error": "gateway timeout",
        "upstream": "payment-service",
        "tx_id": "tx_98765",
        "amount": 99.99,
        "currency": "USD",
    }
)
```

### Java (Logback + OTel agent)

```xml
<!-- logback.xml -->
<configuration>
  <appender name="OTel" class="io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender">
    <!-- Body = formatted log message string -->
    <!-- OTel agent captures MDC fields as log attributes -->
    <captureCodeAttributes>true</captureCodeAttributes>
    <captureMdcAttributes>*</captureMdcAttributes>
  </appender>

  <root level="INFO">
    <appender-ref ref="OTel" />
  </root>
</configuration>
```

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    public void processPayment(String txId, double amount) {
        // MDC fields become log attributes (not body)
        MDC.put("tx_id", txId);
        MDC.put("amount", String.valueOf(amount));
        MDC.put("currency", "USD");

        try {
            // ... payment processing
        } catch (Exception e) {
            // Body = full human-readable message — searchable in VL and Loki
            // Include error details in body string for text filter parity
            log.error("payment failed: {} (upstream: payment-service, tx: {})",
                e.getMessage(), txId, e);
            MDC.put("error", e.getMessage());
            MDC.put("upstream", "payment-service");
        } finally {
            MDC.clear();
        }
    }
}
```

---

## 7. OTel Collector Configuration

### Full dual-write pipeline: Loki + VictoriaLogs

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
        cors:
          allowed_origins:
            - "http://*"
            - "https://*"

processors:
  batch:
    timeout: 5s
    send_batch_size: 1000
    send_batch_max_size: 2000

  resource:
    attributes:
      # Ensure service.name is always set
      - action: insert
        key: service.name
        value: "unknown-service"

  transform/logs:
    log_statements:
      # Map SeverityText to log attribute "level" — required for Loki label parity
      - context: log
        statements:
          - set(attributes["level"], ConvertCase(severity_text, "lower"))
            where severity_text != ""

  attributes/promote_level:
    actions:
      # Promote level to resource for Loki exporter label promotion
      - action: insert
        key: level
        from_attribute: level

exporters:
  # ── VictoriaLogs: native OTLP ────────────────────────────────────────────────
  # VL receives OTLP format, preserves OTel semantic conventions as dotted labels.
  # Body → _msg, resource attributes → stream fields (indexed by VL automatically).
  # Proxy translates dotted names to underscore for Grafana.
  otlphttp/victorialogs:
    endpoint: http://victorialogs:9428/insert/opentelemetry
    encoding: json
    tls:
      insecure: true
    headers:
      # Control which resource attributes become VL stream fields (indexed for fast filtering)
      # Default: VL indexes all resource attributes as stream fields
      # Uncomment to restrict:
      # VL-Stream-Fields: service.name,k8s.namespace.name,deployment.environment,level

  # ── Loki: Loki push format ───────────────────────────────────────────────────
  # OTel body → Loki log line. Resource attributes → stream labels (via labels config).
  # Attributes → Loki structured metadata or log line embedding.
  loki:
    endpoint: http://loki:3100/loki/api/v1/push
    default_labels_enabled:
      exporter: false
      job: true       # Keep job for Loki compatibility
      instance: false
      level: false    # We set level manually below

    labels:
      resource:
        # Bounded cardinality — safe as Loki stream labels
        service.name: "service_name"       # Mapped to "service_name" label key
        service.namespace: ""              # Keeps original key
        service.version: ""
        deployment.environment: ""
        k8s.namespace.name: ""
        k8s.pod.name: ""
        k8s.container.name: ""
        k8s.cluster.name: ""
      attributes:
        # Promote log-level to Loki stream label — critical for logfmt parity
        level: ""

  # ── Debug (optional) ─────────────────────────────────────────────────────────
  debug:
    verbosity: basic

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch, resource, transform/logs]
      exporters: [otlphttp/victorialogs, loki]
  telemetry:
    logs:
      level: warn
```

### Why `level` must be a stream label in Loki

The `labels.attributes.level: ""` directive promotes the OTel `level` log attribute to a Loki stream label. Without this:

```
# All logs go to one stream regardless of severity:
{service_name="payment-service", k8s_namespace_name="prod"}

# Query: {service_name="payment-service"} | logfmt | level="error"
# Loki: level="error" checks stream label → not present → no results or wrong results
# VL: level="error" checks parsed field → correct results
# DIVERGES ✗
```

With `level` as a stream label:
```
# Per-severity streams:
{service_name="payment-service", k8s_namespace_name="prod", level="error"}
{service_name="payment-service", k8s_namespace_name="prod", level="info"}

# Query: {service_name="payment-service"} | logfmt | level="error"
# Loki: level="error" checks stream label → matches error stream → correct
# VL: level="error" checks parsed field → correct
# AGREES ✓
```

---

## 8. Promtail / Grafana Alloy (Direct Loki Push)

### Grafana Alloy — dual-write with level promotion

```alloy
// alloy-config.alloy

discovery.kubernetes "pods" {
  role = "pod"
}

loki.source.kubernetes "pods" {
  targets    = discovery.kubernetes.pods.targets
  forward_to = [loki.process.extract_level.receiver]
}

loki.process "extract_level" {
  // 1. Try JSON parse to extract level
  stage.json {
    expressions = {
      level  = "level",
      msg    = "msg",
    }
  }

  // 2. Fall back to logfmt parse
  stage.logfmt {
    mapping = {
      level  = "level",
    }
  }

  // 3. Promote level to stream label — critical for filter parity
  stage.labels {
    values = {
      level = ""
    }
  }

  // 4. Drop high-cardinality labels that should not be stream labels
  stage.label_drop {
    values = ["trace_id", "span_id", "user_id", "request_id"]
  }

  forward_to = [
    loki.write.loki_backend.receiver,
    loki.write.victorialogs_backend.receiver,
  ]
}

loki.write "loki_backend" {
  endpoint {
    url = "http://loki:3100/loki/api/v1/push"
  }
}

loki.write "victorialogs_backend" {
  endpoint {
    // VictoriaLogs Loki-compatible push endpoint
    url = "http://victorialogs:9428/insert/loki/api/v1/push"
  }
}
```

### Promtail — dual-write with level extraction

```yaml
# promtail-config.yaml
clients:
  - url: http://loki:3100/loki/api/v1/push
  - url: http://victorialogs:9428/insert/loki/api/v1/push

scrape_configs:
  - job_name: kubernetes-pods
    kubernetes_sd_configs:
      - role: pod

    pipeline_stages:
      # Extract level from JSON logs
      - json:
          expressions:
            level: level

      # Extract level from logfmt logs (harmless if not logfmt)
      - logfmt:
          mapping:
            level: level

      # Promote level to stream label — must match log content
      - labels:
          level:

      # Drop high-cardinality fields from stream labels
      - label_drop:
          - trace_id
          - span_id
          - user_id
          - request_id

    relabel_configs:
      # Keep bounded-cardinality Kubernetes labels as stream labels
      - source_labels: [__meta_kubernetes_pod_label_app]
        target_label: app
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_pod_container_name]
        target_label: container
      # Drop all other __meta_ labels
      - regex: __meta_.*
        action: drop
```

---

## 9. Direct VictoriaLogs Push (`/insert/jsonline`)

When pushing directly to VL (not via proxy), use the JSON line endpoint:

```
POST http://victorialogs:9428/insert/jsonline?_stream_fields=service.name,namespace,level,environment
Content-Type: application/stream+json
```

**JSON line format:**
```json
{"_time":"2026-05-01T10:00:00.123Z","_msg":"payment failed: gateway timeout (upstream: payment-service)","service.name":"payment-service","namespace":"prod","level":"error","environment":"production","error":"gateway timeout","upstream":"payment-service","tx_id":"tx_98765","amount":99.99,"currency":"USD","trace_id":"abc123def456"}
```

**Rules:**
- `_time`: RFC3339 or Unix nanoseconds (required)
- `_msg`: full human-readable message with all searchable content
- `_stream_fields`: comma-separated list of fields to index as stream identifiers
- All other fields: stored as log fields, searchable via `| json` or field filters
- `level` MUST be in `_stream_fields` and match the log content

**Multi-line batch (JSON Lines format — one document per line):**
```
{"_time":"2026-05-01T10:00:00Z","_msg":"payment processed","service.name":"payment-service","namespace":"prod","level":"info","tx_id":"tx_11111","amount":149.99}
{"_time":"2026-05-01T10:00:01Z","_msg":"payment failed: card_declined","service.name":"payment-service","namespace":"prod","level":"error","tx_id":"tx_22222","error":"card_declined"}
{"_time":"2026-05-01T10:00:02Z","_msg":"payment retry attempt 2/5","service.name":"payment-service","namespace":"prod","level":"warn","tx_id":"tx_33333","attempt":2}
```

---

## 10. Verification Queries

After configuring your pipeline, use these queries in Grafana to verify parity. Run each against the Loki datasource (routed through the proxy) and directly against VL. Results must be identical.

### Text filter parity
```logql
# Both must return the same lines
{app="payment-service"} |= "timeout"
{app="payment-service"} !~ "processed|refunded"
{app="payment-service"} |= "failed" != "test"
```

### Pipeline filter parity
```logql
# logfmt: only works correctly if level is a stream label
{app="payment-service"} | logfmt | level="error"

# JSON: works if _msg is full JSON
{app="api-gateway"} | json | status >= 500

# Chained
{app="api-gateway"} |= "payments" | json | status > 400 | duration_ms > 1000
```

### Label filter parity
```logql
# Stream label selectors — same for both
{service_name="payment-service", level="error"}
{k8s_namespace_name="prod", deployment_environment="production"}
```

### Metric query parity
```logql
# Both must return same time series
sum by (level) (count_over_time({app="payment-service"}[5m]))
rate({app="api-gateway"} |= "error" [1m])
```

---

## 11. Summary Checklist

| Format | Critical rule | Why |
|---|---|---|
| JSON via Loki push | No explicit `_msg` field | VL uses full JSON → same as Loki raw line |
| JSON with errors | Include error text in JSON body | `_msg` = what VL searches; errors in separate fields are invisible to text filters |
| logfmt | One stream per `level` value | Loki checks stream label; VL checks parsed field — only match when they agree |
| OTel body | Put searchable text in body string | body → `_msg`; attributes are structured fields, not searched by text filters |
| OTel resource | Include `service.name`, `k8s.*`, `deployment.*` | Proxy translates to/from underscore for Grafana compatibility |
| OTel SeverityText | Promote to `level` stream label | Enables `{level="error"}` selector and logfmt-level filter parity |
| High-cardinality fields | Never as stream labels | Trace IDs, user IDs — keep in log body/attributes |
| Dual-write | Same pipeline, same labels, same body | Diverging structure = diverging queries |

---

## What the Proxy Translates Automatically

Once logs are formatted correctly, loki-vl-proxy handles all translation:

| Loki (LogQL) | VictoriaLogs (LogsQL) | Note |
|---|---|---|
| `{service_name="api"}` | `{service.name="api"}` | Underscore ↔ dotted label translation |
| `\|= "timeout"` | `-"timeout"` | Phrase filter on `_msg` |
| `!= "ok"` | `not "ok"` | Negated phrase filter |
| `\|~ "err.*"` | `~"err.*"` | Regex filter on `_msg` |
| `\| json` | `\| unpack_json` | JSON parser stage |
| `\| logfmt` | `\| unpack_logfmt` | logfmt parser stage |
| `\| label_format x="{{.y}}"` | `\| rename y as x` | Label renaming |
| `count_over_time(... [5m])` | `stats count(*)` | Metric aggregation |
| `rate(... [1m])` | `stats count(*) / 60` | Rate over window |

You do not need to change queries or Grafana configuration — only the log format at the source.
