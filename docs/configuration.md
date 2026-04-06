# Configuration

All flags follow VictoriaMetrics naming conventions (`-flagName=value`).

## Server

| Flag | Env | Default | Description |
|---|---|---|---|
| `-listen` | `LISTEN_ADDR` | `:3100` | Listen address |
| `-backend` | `VL_BACKEND_URL` | `http://localhost:9428` | VictoriaLogs backend URL |
| `-ruler-backend` | `RULER_BACKEND_URL` | — | Optional rules backend for legacy Loki YAML rules routes and Prometheus-style `/prometheus/api/v1/rules` passthrough |
| `-alerts-backend` | `ALERTS_BACKEND_URL` | — | Optional alerts backend for Loki and Prometheus-style alerts endpoints (defaults to `-ruler-backend`) |
| `-log-level` | — | `info` | Log level: debug, info, warn, error |
| `-tls-cert-file` | — | — | TLS certificate file for HTTPS |
| `-tls-key-file` | — | — | TLS key file for HTTPS |
| `-tls-client-ca-file` | — | — | CA file for verifying HTTPS client certificates |
| `-tls-require-client-cert` | — | `false` | Require and verify HTTPS client certificates |

## Label Translation

| Flag | Env | Default | Description |
|---|---|---|---|
| `-label-style` | `LABEL_STYLE` | `passthrough` | `passthrough` or `underscores` |
| `-metadata-field-mode` | `METADATA_FIELD_MODE` | `hybrid` | `native`, `translated`, or `hybrid` for `detected_fields` and structured metadata exposure |
| `-field-mapping` | `FIELD_MAPPING` | — | JSON custom field mappings |

### Label Style Modes

| Mode | When to Use | Response | Query |
|---|---|---|---|
| `passthrough` | VL stores underscore labels (Vector/FluentBit normalize) | No translation | No translation |
| `underscores` | VL stores OTel dotted labels (OTLP direct) | `service.name` → `service_name` | `{service_name="x"}` → VL `"service.name":"x"` |

### Custom Field Mappings

```bash
./loki-vl-proxy -label-style=underscores \
  -field-mapping='[{"vl_field":"my_trace_id","loki_label":"traceID"}]'
```

### Metadata Field Modes

| Mode | When to Use | Field APIs |
|---|---|---|
| `native` | You want only raw VictoriaLogs field names | `service.name`, `k8s.pod.name` |
| `translated` | You want strict Loki-style field names only | `service_name`, `k8s_pod_name` |
| `hybrid` | Default. You need Loki compatibility plus OTel-native correlation | Both native dotted names and translated aliases |

`-metadata-field-mode=hybrid` keeps the label surface Loki-compatible while making field-oriented APIs like `detected_fields` and `detected_field/{name}/values` expose both `service.name` and `service_name` when they differ.

## Cache (L1 In-Memory)

| Flag | Env | Default | Description |
|---|---|---|---|
| `-cache-ttl` | — | `60s` | Default cache TTL |
| `-cache-max` | — | `10000` | Maximum cache entries |

### Per-Endpoint TTLs

| Endpoint | TTL |
|---|---|
| `labels`, `label_values` | 60s |
| `series`, `detected_fields` | 30s |
| `query_range`, `query` | 10s |

## Cache (L2 On-Disk)

| Flag | Env | Default | Description |
|---|---|---|---|
| `-disk-cache-path` | — | — | Path to bbolt DB file (empty = disabled) |
| `-disk-cache-compress` | — | `true` | Gzip compression for disk cache |
| `-disk-cache-flush-size` | — | `100` | Flush write buffer after N entries |
| `-disk-cache-flush-interval` | — | `5s` | Write buffer flush interval |


## Multitenancy

| Flag | Env | Default | Description |
|---|---|---|---|
| `-tenant-map` | `TENANT_MAP` | — | JSON string→int tenant mapping |
| `-auth.enabled` | — | `false` | Require `X-Scope-OrgID` on query requests |
| `-tenant.allow-global` | — | `false` | Allow `X-Scope-OrgID: *` to bypass tenant scoping and use the backend default tenant even when a tenant map is configured |

### Tenant Resolution Order

1. **No header** — when `-auth.enabled=false`, requests without `X-Scope-OrgID` use VictoriaLogs' backend default tenant, which is `AccountID=0` and `ProjectID=0`
2. **Tenant map lookup** — if `-tenant-map` is configured and the org ID matches a key, use the mapped `AccountID`/`ProjectID`
3. **Explicit tenant map override** — if a tenant map contains an exact key such as `"0"` or `"fake"`, that explicit mapping wins
4. **Default-tenant aliases** — `X-Scope-OrgID` values `0`, `fake`, and `default` map to VictoriaLogs' built-in `0:0` tenant without rewriting headers
5. **Wildcard global bypass** — `X-Scope-OrgID: *` is a proxy-specific convenience. It uses the backend default tenant only when no tenant map is configured, or when `-tenant.allow-global=true`
6. **Numeric passthrough** — if the org ID is a number other than the default-tenant alias case (for example `"42"`), pass it directly as `AccountID` with `ProjectID: 0`
7. **Fail closed** — unmapped non-numeric org IDs are rejected with `403 Forbidden`

### Multi-Tenant Query Headers

The proxy accepts Loki-style multi-tenant query headers on read/query endpoints by separating tenant IDs with `|`, for example `X-Scope-OrgID: team-a|team-b`.

- Supported on query-style endpoints such as `/query`, `/query_range`, `/labels`, `/label/.../values`, `/series`, `/index/*`, and Drilldown metadata endpoints
- Rejected on `/tail`, delete, and write paths
- Query results inject a synthetic `__tenant_id__` label per tenant, matching Loki's documented query behavior
- `__tenant_id__` matchers in the leading selector narrow the tenant fanout set before backend requests are sent
- multi-tenant `detected_fields` and `detected_labels` use exact merged value unions, so cardinality does not double-count identical values across tenants
- Wildcard `*` is not allowed inside a multi-tenant header; use explicit tenant IDs

### Configuration Examples

```bash
# Via flag (JSON)
./loki-vl-proxy -tenant-map='{"team-alpha":{"account_id":"100","project_id":"1"},"team-beta":{"account_id":"200","project_id":"2"}}'

# Via environment variable
export TENANT_MAP='{"ops-prod":{"account_id":"300","project_id":"0"}}'
./loki-vl-proxy
```

### Grafana Datasource per Tenant

```yaml
datasources:
  - name: Logs (team-alpha)
    type: loki
    url: http://loki-vl-proxy:3100
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: team-alpha
```

### Grafana Datasource for Explicit Multi-Tenant Reads

```yaml
datasources:
  - name: Logs (team-a + team-b)
    type: loki
    url: http://loki-vl-proxy:3100
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: team-a|team-b
```

Use `__tenant_id__` in LogQL when the datasource fans out to more than one tenant:

```logql
{app="api-gateway", __tenant_id__="team-b"}
{service_name="checkout", __tenant_id__=~"team-(a|b)"}
```

### Grafana Datasource for Single-Tenant VictoriaLogs

If the backend only uses VictoriaLogs' default tenant, you can keep Grafana simple:

```yaml
datasources:
  - name: Logs (global tenant)
    type: loki
    url: http://loki-vl-proxy:3100
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: "0"
```

`X-Scope-OrgID: "0"`, `X-Scope-OrgID: "fake"`, and `X-Scope-OrgID: "default"` resolve to VL's default `0:0` tenant in Loki-compatible single-tenant mode. `X-Scope-OrgID: "*"` remains a proxy-specific wildcard convenience, and if you later introduce a tenant map you keep that wildcard behavior only by explicitly setting `-tenant.allow-global=true`.

### Hot Reload

Tenant mappings and field mappings can be reloaded without restart via SIGHUP:
```bash
kill -HUP $(pidof loki-vl-proxy)
```

## OTLP Telemetry

| Flag | Env | Default | Description |
|---|---|---|---|
| `-otlp-endpoint` | `OTLP_ENDPOINT` | — | OTLP HTTP endpoint for proxy metrics |
| `-otlp-interval` | — | `30s` | Push interval |
| `-otlp-compression` | `OTLP_COMPRESSION` | `none` | `none`, `gzip`, `zstd` |
| `-otlp-headers` | `OTLP_HEADERS` | — | Comma-separated OTLP HTTP headers in `key=value` form |
| `-otlp-timeout` | — | `10s` | HTTP request timeout |
| `-otlp-tls-skip-verify` | — | `false` | Skip TLS verification |
| `-otel-service-name` | `OTEL_SERVICE_NAME` | `loki-vl-proxy` | `service.name` for OTLP metrics and JSON logs |
| `-otel-service-namespace` | `OTEL_SERVICE_NAMESPACE` | — | `service.namespace` for OTLP metrics and JSON logs |
| `-otel-service-instance-id` | `OTEL_SERVICE_INSTANCE_ID` | — | `service.instance.id` for OTLP metrics and JSON logs |
| `-deployment-environment` | `DEPLOYMENT_ENVIRONMENT` | — | `deployment.environment.name` for OTLP metrics and JSON logs |

## HTTP Hardening

| Flag | Env | Default | Description |
|---|---|---|---|
| `-http-read-timeout` | — | `30s` | Server read timeout |
| `-http-write-timeout` | — | `120s` | Server write timeout |
| `-http-idle-timeout` | — | `120s` | Server idle timeout |
| `-http-max-header-bytes` | — | `1MB` | Maximum header size |
| `-http-max-body-bytes` | — | `10MB` | Maximum request body size |

## Grafana Compatibility

| Flag | Env | Default | Description |
|---|---|---|---|
| `-max-lines` | — | `1000` | Default max lines per query |
| `-backend-timeout` | — | `120s` | Timeout for non-streaming VL backend requests |
| `-stream-response` | — | `false` | Stream via chunked transfer |
| `-response-gzip` | — | `true` | Compress responses |
| `-derived-fields` | — | — | JSON derived fields for trace linking |
| `-forward-headers` | — | — | HTTP headers to forward to VL |
| `-forward-cookies` | — | — | Cookie names to forward to VL |
| `-backend-basic-auth` | — | — | `user:password` for VL basic auth |
| `-backend-tls-skip-verify` | — | `false` | Skip TLS on VL connection |
| `-tail.allowed-origins` | — | — | Comma-separated WebSocket Origin allowlist for `/loki/api/v1/tail` |

## Observability and Admin Surfaces

| Flag | Env | Default | Description |
|---|---|---|---|
| `-server.register-instrumentation` | — | `true` | Register `/metrics` and related instrumentation handlers |
| `-server.enable-pprof` | — | `false` | Expose `/debug/pprof/*` |
| `-server.enable-query-analytics` | — | `false` | Expose `/debug/queries` |
| `-server.admin-auth-token` | — | — | Bearer token accepted on admin/debug endpoints |
| `-metrics.max-tenants` | — | `256` | Max unique tenant labels retained in `/metrics` before using `__overflow__` |
| `-metrics.max-clients` | — | `256` | Max unique client labels retained in `/metrics` before using `__overflow__` |
| `-metrics.trust-proxy-headers` | — | `false` | Trust `X-Grafana-User` and `X-Forwarded-For` for client metrics, logs, and backend client-context forwarding |

## Grafana Datasource Mapping

These Grafana Loki datasource settings now have a direct proxy-side mapping:

| Grafana Setting | Proxy Support |
|---|---|
| URL | `-listen`, optional `-tls-cert-file` / `-tls-key-file` |
| No Authentication | Supported by default |
| TLS Client Authentication | `-tls-client-ca-file`, `-tls-require-client-cert` |
| Skip TLS certificate validation | Grafana-side only |
| Add self-signed certificate | Grafana-side only |
| HTTP headers (`X-Scope-OrgID`, auth headers) | `-forward-headers`, `-tenant-map`, `-auth.enabled` |
| Allowed cookies | `-forward-cookies` |
| Timeout | `-backend-timeout` for proxy→VL, Grafana datasource timeout for Grafana→proxy |
| Maximum lines | `-max-lines` |

When `-metrics.trust-proxy-headers=true`, the proxy also forwards `X-Grafana-User` plus derived `X-Loki-VL-Client-ID` and `X-Loki-VL-Client-Source` headers to the backend so downstream logs and stats can attribute expensive queries to real Grafana users instead of only source IPs.

Alerting datasource integration is still partial: the proxy supports legacy Loki YAML rules reads and Prometheus-style JSON rules/alerts reads against a configured backend, but it does not yet implement the full Loki ruler write API surface.
