# Configuration

All flags follow VictoriaMetrics naming conventions (`-flagName=value`).

## Server

| Flag | Env | Default | Description |
|---|---|---|---|
| `-listen` | `LISTEN_ADDR` | `:3100` | Listen address |
| `-backend` | `VL_BACKEND_URL` | `http://localhost:9428` | VictoriaLogs backend URL |
| `-log-level` | — | `info` | Log level: debug, info, warn, error |
| `-tls-cert-file` | — | — | TLS certificate file for HTTPS |
| `-tls-key-file` | — | — | TLS key file for HTTPS |

## Label Translation

| Flag | Env | Default | Description |
|---|---|---|---|
| `-label-style` | `LABEL_STYLE` | `passthrough` | `passthrough` or `underscores` |
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

### Tenant Resolution Order

1. **Tenant map lookup** — if `-tenant-map` is configured and the org ID matches a key, use the mapped `AccountID`/`ProjectID`
2. **Numeric passthrough** — if the org ID is a number (e.g., `"42"`), pass it directly as `AccountID` with `ProjectID: 0`
3. **Default** — unmapped non-numeric org IDs get `AccountID: 0`, `ProjectID: 0`

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
| `-otlp-timeout` | — | `10s` | HTTP request timeout |
| `-otlp-tls-skip-verify` | — | `false` | Skip TLS verification |

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
| `-stream-response` | — | `false` | Stream via chunked transfer |
| `-response-gzip` | — | `true` | Compress responses |
| `-derived-fields` | — | — | JSON derived fields for trace linking |
| `-forward-headers` | — | — | HTTP headers to forward to VL |
| `-backend-basic-auth` | — | — | `user:password` for VL basic auth |
| `-backend-tls-skip-verify` | — | `false` | Skip TLS on VL connection |
