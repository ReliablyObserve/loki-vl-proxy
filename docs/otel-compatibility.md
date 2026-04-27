---
sidebar_label: OpenTelemetry Compatibility
description: How OpenTelemetry labels (dotted and underscore-translated) flow through loki-vl-proxy and appear in Grafana.
---

# OpenTelemetry Compatibility

The proxy provides full OpenTelemetry (OTel) label compatibility between VictoriaLogs and Grafana's Loki API surface. This document explains how OTel data flows through the system, what the proxy detects, and how it maintains Loki compatibility.

## Why OTel Support Matters

VictoriaLogs stores OTel semantic convention labels natively with dotted names (`service.name`, `k8s.pod.name`, `deployment.environment`). Grafana's Loki datasource and Logs Drilldown expect underscore-based label names (`service_name`, `k8s_pod_name`, `deployment_environment`). The proxy bridges this gap by:

1. **Translating queries**: Loki-style underscore labels in LogQL are converted to dotted names for VL
2. **Translating responses**: VL's dotted labels are converted back to underscore names for Grafana
3. **Exposing alias pairs**: Both forms appear in `detected_fields` so Grafana field pickers show complete data
4. **Suppressing synthetic labels**: Non-OTel data gets a synthetic `service_name` that must not leak into `detected_fields`

## How OTel Data Reaches VictoriaLogs

### Delivery Mechanism 1: Direct OTel Collector Push

OTel collectors push data to VL's `/insert/jsonline` endpoint with dotted semantic convention labels as stream fields:

```
POST /insert/jsonline?_stream_fields=service.name,k8s.pod.name,k8s.namespace.name,deployment.environment

{"_time":"2025-04-25T10:30:00Z","_msg":"request processed","service.name":"auth-svc","k8s.pod.name":"auth-pod-xyz","deployment.environment":"production"}
```

VL indexes the declared `_stream_fields` and stores them in the `_stream` metadata field. When queried, the response includes:

```json
{
  "_stream": "{service.name=\"auth-svc\",k8s.pod.name=\"auth-pod-xyz\",deployment.environment=\"production\"}",
  "_msg": "request processed",
  "service.name": "auth-svc",
  "k8s.pod.name": "auth-pod-xyz"
}
```

### Delivery Mechanism 2: OTel Attributes in Log Messages

Some applications embed OTel attributes in structured log messages rather than stream labels:

```json
{"service.name":"api-svc","k8s.pod.name":"api-xyz","trace_id":"abc123","span_id":"def456","http.method":"GET","status":200}
```

These attributes are parsed from the message body during field detection, not from stream labels.

### Delivery Mechanism 3: Pre-Translated Underscore Labels

Some OTel systems pre-translate semantic conventions to underscores before storage:

```
_stream_fields=service_name,k8s_pod_name,deployment_environment

{"service_name":"collector","k8s_pod_name":"otel-uvw456","deployment_environment":"prod"}
```

The proxy detects these via OTel underscore prefix patterns (`k8s_`, `deployment_`, `telemetry_`).

## OTel Detection Hierarchy

_Introduced in v1.15.0._

The proxy uses a three-layer hierarchy to detect whether log data is OTel-instrumented. Detection happens per-entry by inspecting stream labels from the `_stream` field.

### Priority 1: Dotted Semantic Conventions (Strongest Signal)

Check stream labels for OTel dotted field names:

| Field | OTel Convention |
|-------|----------------|
| `service.name` | Service identity |
| `service.namespace` | Service namespace |
| `k8s.pod.name` | Kubernetes pod |
| `k8s.namespace.name` | Kubernetes namespace |
| `k8s.node.name` | Kubernetes node |
| `k8s.container.name` | Container name |
| `deployment.environment` | Deployment environment |
| `deployment.name` | Deployment name |
| `deployment.version` | Deployment version |
| `host.name`, `host.id`, `host.arch` | Host attributes |
| `telemetry.sdk.name` | SDK identity |
| `telemetry.sdk.language` | SDK language |
| `telemetry.sdk.version` | SDK version |

If any of these exist in stream labels, the entry is OTel-instrumented.

### Priority 2: Underscore OTel Prefixes (Medium Signal)

Check stream labels for underscore-translated OTel prefixes:

- `k8s_` (e.g., `k8s_pod_name`, `k8s_namespace_name`)
- `deployment_` (e.g., `deployment_environment`)
- `telemetry_` (e.g., `telemetry_sdk_name`)
- `host_` (e.g., `host_name`)

Special case: `service_name` alone is NOT an OTel indicator (it could be synthetic). It's only treated as OTel if paired with `service.name` in the same stream.

### Priority 3: Message Content (Weak Signal)

Check log message for `trace_id` AND `span_id` — but only if stream labels also contain `k8s.*` or `deployment.*` fields for confirmation. Message-only OTel attributes without stream label confirmation are not treated as OTel.

## Label Translation

### Response Direction (VL → Loki)

When returning data to Grafana, dotted VL fields are converted to underscore Loki labels:

| VL Field | Loki Label |
|----------|-----------|
| `service.name` | `service_name` |
| `k8s.pod.name` | `k8s_pod_name` |
| `k8s.namespace.name` | `k8s_namespace_name` |
| `deployment.environment` | `deployment_environment` |
| `telemetry.sdk.name` | `telemetry_sdk_name` |

The full mapping covers 50+ OTel semantic conventions (see `knownUnderscoreToDot` in `internal/proxy/labels.go`).

### Query Direction (Loki → VL)

When Grafana sends a query with underscore labels, the proxy translates back to dots:

```
LogQL:  {service_name="auth-svc", k8s_namespace_name="prod"}
LogsQL: {service.name="auth-svc", k8s.namespace.name="prod"}
```

### Metadata Field Modes

The proxy supports three metadata field exposure modes:

| Mode | Detected Fields Output | Use Case |
|------|----------------------|----------|
| **Hybrid** (default) | Both `service.name` AND `service_name` | Maximum compatibility — Explore and Drilldown see both forms |
| **Translated** | Only `service_name` | Strict Loki compatibility — no dots in field names |
| **Native** | Only `service.name` | VL-native mode — expose original field names |

## Service Name Handling

### Non-OTel Data (Standard Kubernetes)

For standard Kubernetes logs pushed via Loki (labels: `app`, `cluster`, `namespace`):

- The proxy synthesizes `service_name` from the `app` label for stream identification
- `service_name` is a **synthetic indexed label** — it must NOT appear in `detected_fields`
- `detected_fields` only shows parsed message fields: `method`, `status`, `duration_ms`, etc.

### OTel Data (Semantic Conventions)

_Alias pair exposure introduced in v1.15.0._

For OTel-instrumented services with `service.name` in stream labels:

- `service.name` is a **real stream label** — it appears in `detected_fields`
- `service_name` is exposed as an **alias** of `service.name` — also in `detected_fields`
- Both forms appear so Grafana Explore field picker and Logs Drilldown can use either
- `service_name` is suppressed for non-OTel data (e.g., `api-gateway` with only standard K8s labels) but exposed as the alias pair when real `service.name` exists in stream labels

### Implementation

`service_name` is unconditionally suppressed in the `suppressedDetectedFieldNames` list. After scanning all entries, if any entry has OTel `service.name` in its stream labels, `service_name` is explicitly re-added as an alias with matching values and cardinality.

This two-phase approach ensures:
- Non-OTel data never leaks synthetic `service_name` into `detected_fields`
- OTel data correctly exposes the alias pair
- Native field merging cannot re-introduce suppressed fields

### Logfmt Job Fields (v1.17.1)

Logfmt-parsed document fields in the log body (e.g., `job=sync-users` from a logfmt-formatted message) are no longer picked up as service names when stream label inventory is available.

When stream labels are present, only dotted OTel-style names (e.g., `service.name`) are read from the full field inventory as service name candidates. This prevents logfmt fields like `job` from polluting the Drilldown service list for non-OTel data.

## Test Coverage

### Test Data Services

| Service | Delivery | Labels | Purpose |
|---------|----------|--------|---------|
| `api-gateway` | Loki + VL | Standard K8s (`app`, `namespace`, `cluster`) | Non-OTel baseline — verify service_name suppressed |
| `otel-auth-service` | VL only | Full OTel dotted (`service.name`, `k8s.*`, `deployment.*`, `telemetry.*`) | OTel detection, alias pair exposure |
| `otel-api-service` | VL only | Standard K8s stream + OTel in message JSON | Mixed signal detection |
| `otel-collector-native` | VL only | Underscore OTel (`service_name`, `k8s_pod_name`, `deployment_environment`) | Pre-translated underscore detection |

### What Each Service Tests

**api-gateway (non-OTel)**:
- `detected_fields` must NOT include: `app`, `cluster`, `namespace`, `service_name`
- `detected_fields` must include: `method`, `path`, `status`, `duration_ms` (parsed from JSON)

**otel-auth-service (full OTel)**:
- `detected_fields` must include BOTH forms: `service.name` + `service_name`, `k8s.pod.name` + `k8s_pod_name`
- `detected_labels` must include only underscore forms (Loki surface)
- `detected_labels` must NOT include dotted forms

**otel-api-service (mixed)**:
- OTel attributes detected from message content (lower priority)
- Standard K8s stream labels treated as non-OTel

**otel-collector-native (underscore)**:
- Underscore OTel prefixes detected as Priority 2 signal
- Labels translated back to dotted forms for VL query direction

### CI Test Matrix

| CI Job | Tests | Scope |
|--------|-------|-------|
| `e2e-compat (otel-edge)` | `TestOTelDots_ProxyUnderscores`, `TestDetectedFields_UnderscoreProxy`, `TestOTelCompatibilityScore` | Full OTel label translation and field exposure |
| `drilldown-grafana-pr-matrix` | `structured_metadata_fields_and_values`, `detected_labels_resource_exposes_loki_surface_only` | Grafana integration with OTel alias pairs |
| `test` (unit) | `TestDrilldown_DetectedFields_TranslatedModeExposesOnlyAliases` | Metadata field mode handling |
| `loki-pinned` | `TestQuerySemanticsMatrix` | Query parity with env isolation |

## Configuration

OTel label handling is controlled by the proxy's label style and metadata field mode:

```yaml
# Label style: how dots are translated
label-style: underscores  # dots → underscores (default, recommended)

# Metadata field mode: what detected_fields exposes
metadata-field-mode: hybrid  # both forms (default, recommended for OTel)
```

For environments without OTel data, these defaults work correctly — synthetic `service_name` is suppressed, and only parsed message fields appear in `detected_fields`.