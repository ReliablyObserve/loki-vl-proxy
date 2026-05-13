---
sidebar_label: Configuration Reference
description: All loki-vl-proxy flags, environment variables, and Helm values. Covers cache, circuit breaker, multi-tenancy, label translation, and OTel.
---

# Configuration

All flags follow VictoriaMetrics naming conventions (`-flagName=value`).

## Server

| Flag | Env | Default | Description |
|---|---|---|---|
| `-listen` | `LISTEN_ADDR` | `:3100` | Listen address |
| `-backend` | `VL_BACKEND_URL` | `http://localhost:9428` | VictoriaLogs backend URL |
| `-proc-root` | `PROC_ROOT` | `/proc` | Proc filesystem root used for CPU/memory/disk/network/PSI metrics (`/proc` for container scope, `/host/proc` for host scope) |
| `-ruler-backend` | `RULER_BACKEND_URL` | — | Optional rules backend for legacy Loki YAML rules routes and Prometheus-style `/prometheus/api/v1/rules` passthrough |
| `-alerts-backend` | `ALERTS_BACKEND_URL` | — | Optional alerts backend for Loki and Prometheus-style alerts endpoints (defaults to `-ruler-backend`) |
| `-log-level` | — | `info` | Log level: debug, info, warn, error |
| `-tls-cert-file` | — | — | TLS certificate file for HTTPS |
| `-tls-key-file` | — | — | TLS key file for HTTPS |
| `-tls-client-ca-file` | — | — | CA file for verifying HTTPS client certificates |
| `-tls-require-client-cert` | — | `false` | Require and verify HTTPS client certificates |
| `-log-request-sample-rate` | — | `0` | Log one in every N requests for access logging (`0` = disabled) |
| `-cache-disabled` | — | `false` | Disable the in-memory L1 cache entirely (useful for benchmarking raw translation overhead) |

## Label Translation

See [Translation Modes Guide](translation-modes.md) for mode-selection profiles and exact underscore vs dotted exposure behavior across labels, field APIs, and structured metadata.

| Flag | Env | Default | Description |
|---|---|---|---|
| `-label-style` | `LABEL_STYLE` | `passthrough` | `passthrough` or `underscores` |
| `-metadata-field-mode` | `METADATA_FIELD_MODE` | `hybrid` | `native`, `translated`, or `hybrid` for `detected_fields` and structured metadata exposure |
| `-emit-structured-metadata` | — | `true` | Enable Loki `categorize-labels` response encoding: requests with `X-Loki-Response-Encoding-Flags: categorize-labels` emit 3-tuples `[timestamp, line, metadata]`, while default/no-flag requests stay canonical 2-tuples |
| `-patterns-enabled` | — | `true` | Enable `GET /loki/api/v1/patterns` (Grafana Logs Drilldown patterns view). When `false`, the endpoint returns `404 not_found` |
| `-patterns-autodetect-from-queries` | — | `false` | Warm `/loki/api/v1/patterns` cache from successful `query` and `query_range` responses (global autodetect mode, opt-in) |
| `-patterns-custom` | — | — | Static custom patterns prepended to every `/patterns` response. Accepts JSON string array or newline-separated text payload |
| `-patterns-custom-file` | — | — | File path for static custom patterns prepended to every `/patterns` response. Supports JSON string array or newline-separated text with optional `#` comments |
| `-patterns-persist-path` | — | — | Disk path for persisted patterns snapshot JSON file (empty disables persistence) |
| `-patterns-persist-interval` | — | `30s` | Periodic flush interval for in-memory patterns snapshot |
| `-patterns-startup-stale-threshold` | — | `60s` | Freshness threshold used by startup warm logic/peer snapshot cache |
| `-patterns-startup-peer-warm-timeout` | — | `5s` | Startup timeout for peer warm merge of pattern snapshots |
| `-field-mapping` | `FIELD_MAPPING` | — | JSON custom field mappings |
| `-stream-fields` | — | — | Comma-separated `_stream_fields` labels used for stream selector optimization and label-surface hints |
| `-extra-label-fields` | `EXTRA_LABEL_FIELDS` | — | Comma-separated additional VL fields to expose on label-facing APIs and alias resolution paths (for example `host.id,k8s.cluster.name`) |

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

### Custom Drilldown Patterns

Examples:

```bash
# Inline JSON list
./loki-vl-proxy \
  -patterns-custom='["time=\"<_>\" level=info msg=\"finished unary call\"","grpc.code=<_> grpc.method=<_>"]'

# File-backed list (JSON array or newline-separated text)
./loki-vl-proxy \
  -patterns-custom-file=/etc/loki-vl-proxy/custom-patterns.json
```

### Extra Label Fields (`-extra-label-fields`)

Use this when you need explicit label exposure and alias resolution for fields that are not always discoverable from `stream_field_names`.

What it affects:

- `GET /loki/api/v1/labels`: appends configured fields to the label set.
- `GET /loki/api/v1/label/{name}/values`: resolves underscore↔dot aliases for custom fields.
- `GET /loki/api/v1/index/volume` and `GET /loki/api/v1/index/volume_range`: resolves `targetLabels` aliases (for example `host_id` -> `host.id`) before backend grouping.

How values are handled:

- Accepts a comma-separated list.
- Entries are normalized to canonical VL field names by translator rules.
- Duplicates are removed.
- It extends exposure and aliasing only; it does not create or index fields in VictoriaLogs.

Caching and limits:

- It reuses existing label-path caches (`/labels` and `/label/{name}/values`), so repeated lookups do not repeatedly rescan backend metadata.
- There is no dedicated hard cap on how many entries you can set in `-extra-label-fields`; behavior is intentionally open to match VictoriaLogs field discovery.
- Existing proxy safeguards still apply (endpoint TTLs, backend timeouts, and standard response-size protections).
- Practical guidance: keep this list focused on fields you want Loki users to filter on frequently; very large lists can add UI noise in Grafana.

Examples:

```bash
# Conservative Loki-facing UX + explicit custom fields
./loki-vl-proxy \
  -label-style=underscores \
  -metadata-field-mode=translated \
  -emit-structured-metadata=true \
  -extra-label-fields='host.id,k8s.cluster.name,custom.pipeline.processing'
```

```bash
# Equivalent via env
export EXTRA_LABEL_FIELDS='host.id,k8s.cluster.name,custom.pipeline.processing'
./loki-vl-proxy -label-style=underscores -metadata-field-mode=translated
```

When to also use `-field-mapping`:

- Use `-extra-label-fields` to extend discovery and alias resolution.
- Use `-field-mapping` when you need a non-default alias name (for example `custom.pipeline.processing` &lt;-&gt; `pipeline_proc`).

### Indexed Label Values Browse Cache (Optional)

This mode is designed for very high-cardinality labels (for example `k8s_pod_name`) where returning every value on first browse is expensive.

| Flag | Env | Default | Description |
|---|---|---|---|
| `-label-values-indexed-cache` | — | `false` | Enable indexed hotset browsing for `GET /loki/api/v1/label/{name}/values` |
| `-label-values-hot-limit` | — | `200` | Default values returned for empty-query browse when `limit` is not specified |
| `-label-values-index-max-entries` | — | `200000` | Maximum indexed values retained per tenant+label in memory |
| `-label-values-index-persist-path` | — | — | Disk path for persisted label-values index snapshot (JSON) |
| `-label-values-index-persist-interval` | — | `30s` | Periodic snapshot flush interval |
| `-label-values-index-startup-stale-threshold` | — | `60s` | Disk snapshot freshness threshold before peer warm fallback |
| `-label-values-index-startup-peer-warm-timeout` | — | `5s` | Startup timeout for peer warm fallback |

Behavior when enabled:

- Empty-query browse (`query` omitted or `query=*`) serves a hot subset first.
- Supports optional pagination-style parameters on the same endpoint: `limit` and `offset`.
- Supports optional in-proxy value filtering with `search` (alias `q`), without forcing a full backend refetch when index is warm.
- Query-scoped requests (`query={...}`) keep standard behavior and still update index state.
- Startup warm order: restore disk snapshot first, then warm from peer cache when disk snapshot is stale/missing.
- Rolling update safety: graceful shutdown writes a final snapshot before exit.
- Readiness behavior: `/ready` stays `503` until label-values startup warm is finished.

Example:

```bash
./loki-vl-proxy \
  -label-values-indexed-cache=true \
  -label-values-hot-limit=200 \
  -label-values-index-max-entries=200000 \
  -label-values-index-persist-path=/cache/label-values-index.json \
  -label-values-index-persist-interval=30s \
  -label-values-index-startup-stale-threshold=60s \
  -label-values-index-startup-peer-warm-timeout=5s

Sizing guidance:

- RAM estimate per label key: `label-values-index-max-entries * ~96 bytes`.
- Disk snapshot estimate per label key: roughly `~60%` of RAM estimate.
- Total footprint scales with `(tenant_count * indexed_label_count)`.
```

### Patterns Persistence (Optional, Recommended For Drilldown)

Enable this to keep detected `/loki/api/v1/patterns` results warm across rolling restarts and fleet members.

Behavior:

- Pattern cache entries are retained long-term and updated in-place by cache key.
- Every detected pattern response is appended to the in-memory snapshot map (no periodic TTL-based pruning in snapshot state).
- Optional global autodetect (`-patterns-autodetect-from-queries=true`) passively mines successful `query` and `query_range` responses and pre-warms matching `/patterns` cache keys.
- Snapshot is persisted to disk periodically and on graceful shutdown.
- Startup restore order: disk snapshot first, then peer merge (newest entry wins per cache key).
- Readiness stays `503` during startup warm when patterns persistence is configured.

Operational guidance:

- For restart-safe persistence, run StatefulSet + PVC and set `-patterns-persist-path` to a writable mounted path.
- If `-patterns-persist-path` is set but not writable, proxy startup fails fast with a clear error.
- If `-patterns-persist-path` is not set, patterns still work, but persistence is disabled (cold start after restart).

Sizing guidance:

- Endpoint clamp: max returned patterns per request is `1000`.
- Approximate persisted bytes:
  - `snapshot_size ~= sum(pattern_response_payload_bytes_per_cached_query_key)`
  - Rule of thumb per pattern entry: `~(120 bytes base + pattern length + ~24 bytes per sample bucket)`
- Real footprint depends on:
  - number of unique `(tenant, rawQuery)` keys,
  - sample bucket count per pattern (`step`, query range),
  - pattern text length distribution.

References:

- Loki patterns API: https://grafana.com/docs/loki/latest/reference/loki-http-api/#patterns-detection
- Grafana Logs patterns UI: https://grafana.com/docs/grafana/latest/visualizations/simplified-exploration/logs/patterns/

### Metadata Field Modes

| Mode | When to Use | Field APIs |
|---|---|---|
| `native` | You want only raw VictoriaLogs field names | `service.name`, `k8s.pod.name` |
| `translated` | You want strict Loki-style field names only | `service_name`, `k8s_pod_name` |
| `hybrid` | Default. You need Loki compatibility plus OTel-native correlation | Both native dotted names and translated aliases |

`-metadata-field-mode=hybrid` keeps the label surface Loki-compatible while making field-oriented APIs like `detected_fields` and `detected_field/{name}/values` expose both `service.name` and `service_name` when they differ.

### Compatibility Profiles

The three flags below define the compatibility profile:

- `-label-style`
- `-metadata-field-mode`
- `-emit-structured-metadata`

| Profile | Settings | Best For |
|---|---|---|
| Loki/Grafana conservative | `label-style=underscores`, `metadata-field-mode=translated`, `emit-structured-metadata=true` | Strict Loki-style field naming plus Explore/Drilldown event metadata |
| Drilldown/OTel mixed mode | `label-style=underscores`, `metadata-field-mode=hybrid`, `emit-structured-metadata=true` | Grafana + OTel correlation where both dotted and translated field names are useful |
| Native VL field surface | `label-style=passthrough`, `metadata-field-mode=native`, `emit-structured-metadata=true` | Consumers that prefer raw VictoriaLogs field names and structured metadata |

For tuple behavior and endpoint-level details, see [API Reference](api-reference.md).
For support scope by product/version track, see [Compatibility Matrix](compatibility-matrix.md).

If you must interoperate with legacy clients that reject metadata objects in `categorize-labels` mode, explicitly set `-emit-structured-metadata=false`.

## Cache (L1 In-Memory)

| Flag | Env | Default | Description |
|---|---|---|---|
| `-cache-ttl` | — | `60s` | Default cache TTL |
| `-cache-max` | — | `10000` | Maximum cache entries |
| `-cache-max-bytes` | — | `268435456` | Maximum in-memory L1 cache size in bytes (256 MiB by default) |
| `-compat-cache-enabled` | — | `true` | Enable the Tier0 compatibility-edge response cache for safe GET read endpoints |
| `-compat-cache-max-percent` | — | `10` | Percent of `-cache-max-bytes` reserved for Tier0 (`0` disables, max `50`) |

### Tier0 Compatibility-Edge Cache

Tier0 is a separate in-memory cache instance that reuses the same cache implementation as the deeper L1/L2/L3 stack, but stores only final Loki-shaped response bodies.

- It runs only after tenant validation and route classification.
- It only applies to safe `GET` read endpoints such as `query`, `query_range`, `series`, labels, volume, patterns, and Drilldown metadata endpoints.
- It does not apply to `/tail`, websocket upgrades, writes, deletes, admin/debug paths, or non-JSON responses.
- Its budget is derived from `-cache-max-bytes`, so `-compat-cache-max-percent=10` means Tier0 gets 10% of the primary L1 memory budget.
- Tenant-map and field-mapping reloads invalidate Tier0 immediately.

### Per-Endpoint TTLs

| Endpoint | TTL |
|---|---|
| `labels`, `label_values` | 60s |
| `series`, `detected_fields`, `detected_field_values`, `detected_labels` | 30s |
| `patterns` | `100y` (effectively persistent; update-on-write) |
| `query_range`, `query` | 10s |
| `index_stats`, `volume`, `volume_range` | 10s |

## Cache (L2 On-Disk)

| Flag | Env | Default | Description |
|---|---|---|---|
| `-disk-cache-path` | — | — | Path to bbolt DB file (empty = disabled) |
| `-disk-cache-compress` | — | `true` | Gzip compression for disk cache |
| `-disk-cache-flush-size` | — | `100` | Flush write buffer after N entries |
| `-disk-cache-flush-interval` | — | `5s` | Write buffer flush interval |
| `-disk-cache-min-ttl` | — | `30s` | Minimum TTL required before an entry is eligible for L2 disk-cache writes |
| `-disk-cache-max-bytes` | — | `0` | Maximum on-disk L2 cache size in bytes (`0` = unlimited) |

## Cold Storage Backend

Route queries for old data to a separate cold backend (e.g. Victoria Lakehouse or a second VictoriaLogs instance). Queries spanning both hot and cold data are split and merged transparently.

| Flag | Env | Default | Description |
|---|---|---|---|
| `-cold-enabled` | — | `false` | Enable cold storage backend routing |
| `-cold-backend` | — | — | Cold storage backend URL (e.g. `http://lakehouse:9428`) |
| `-cold-boundary` | — | `168h` | Data older than this age is routed to the cold backend (default 7 days) |
| `-cold-overlap` | — | `1h` | Overlap window around the cold boundary — queries spanning the boundary include this overlap on both sides to avoid gaps |
| `-cold-manifest-refresh` | — | `5m` | How often to refresh the cold backend capability manifest |
| `-cold-timeout` | — | `30s` | Timeout for cold backend requests |

```bash
./loki-vl-proxy \
  -backend=http://victorialogs:9428 \
  -cold-enabled \
  -cold-backend=http://lakehouse:9428 \
  -cold-boundary=336h  # 14 days
```

## Query Range Window Cache

These flags control Loki-compatible `query_range` split/merge execution with per-window cache reuse.

| Flag | Env | Default | Description |
|---|---|---|---|
| `-query-range-windowing` | — | `true` | Enable window split/merge path for log `query_range` |
| `-query-range-split-interval` | — | `1h` | Per-window time span used for split/merge |
| `-query-range-max-parallel` | — | `2` | Static maximum window fetch parallelism (used when adaptive mode is disabled) |
| `-query-range-adaptive-parallel` | — | `true` | Enable adaptive window fetch parallelism |
| `-query-range-adaptive-min-parallel` | — | `2` | Lower bound for adaptive parallelism |
| `-query-range-adaptive-max-parallel` | — | `8` | Upper bound for adaptive parallelism |
| `-query-range-latency-target` | — | `1.5s` | Target backend window fetch latency for safe adaptive increase |
| `-query-range-latency-backoff` | — | `3s` | Backoff threshold; adaptive mode reduces parallelism when EWMA latency exceeds this |
| `-query-range-adaptive-cooldown` | — | `30s` | Minimum interval between adaptive parallelism changes |
| `-query-range-error-backoff-threshold` | — | `0.02` | Backoff threshold for EWMA backend error ratio (0-1) |
| `-query-range-freshness` | — | `10m` | Near-now freshness boundary |
| `-query-range-recent-cache-ttl` | — | `0s` | Cache TTL for windows newer than `now-freshness` (`0s` disables near-now cache) |
| `-query-range-history-cache-ttl` | — | `24h` | Cache TTL for historical windows older than `now-freshness` |
| `-query-range-prefilter-index-stats` | — | `true` | Use `/select/logsql/hits` preflight to skip empty split windows before expensive log fanout |
| `-query-range-prefilter-min-windows` | — | `8` | Minimum split-window count required before prefilter is enabled |
| `-query-range-stream-aware-batching` | — | `true` | Reduce parallelism for windows estimated as expensive from prefilter hits |
| `-query-range-expensive-hit-threshold` | — | `2000` | Prefilter hit threshold above which a window is treated as expensive |
| `-query-range-expensive-max-parallel` | — | `1` | Maximum parallel fetches for expensive windows |
| `-query-range-align-windows` | — | `true` | Align split windows to fixed interval boundaries for overlap-cache reuse |
| `-query-range-window-timeout` | — | `20s` | Per-window backend timeout budget (`0` disables) |
| `-query-range-partial-responses` | — | `false` | Allow partial `query_range` responses on retryable backend failures |
| `-query-range-background-warm` | — | `true` | Continue warming failed windows in background after a partial response |
| `-query-range-background-warm-max-windows` | — | `24` | Cap background warm fanout after a partial response |
| `-recent-tail-refresh-enabled` | — | `true` | Enable near-now stale-cache bypass for `query_range`, `index/volume`, and `index/volume_range` |
| `-recent-tail-refresh-window` | — | `2m` | Treat requests ending within this window from `now` as near-now |
| `-recent-tail-refresh-max-staleness` | — | `15s` | Maximum acceptable cache age for near-now requests before forcing a fresh backend fetch |

### Loki-Aligned Defaults

- `split-interval=1h` mirrors the common Loki split-by-interval setup.
- `freshness=10m` keeps the latest time range uncached by default, matching typical Loki freshness posture.
- bounded `max-parallel=2` protects VictoriaLogs from fanout bursts while still reducing latency for long ranges.
- `history-cache-ttl=24h` keeps older windows warm for repeated drilldowns and expanded-range queries.
- adaptive mode (`min=2`, `max=8`) raises parallelism only when EWMA latency/error remain healthy and backs off quickly when backend pressure rises.

### Cache Sizing Guidance

Use this approximation for disk planning:

`required_bytes ~= unique_window_keys_per_day * avg_compressed_window_bytes * ttl_days`

From in-repo sizing tests (`internal/cache/sizing_test.go`), a practical compressed average is around `56 KiB` per entry, which yields:

- `1 GiB` -> about `18k` cached windows
- `10 GiB` -> about `189k` cached windows
- `50 GiB` -> about `948k` cached windows

Set `-disk-cache-max-bytes` when you want deterministic upper bounds instead of relying only on PVC limits.

### Practical Tuning Profiles

| Profile | Suggested Settings | When to Use |
|---|---|---|
| Conservative | `adaptive-max=4`, `latency-target=2s`, `latency-backoff=4s`, `history-ttl=24h` | Shared VictoriaLogs clusters with strict backend protection goals |
| Balanced (default) | `adaptive-max=8`, `latency-target=1.5s`, `latency-backoff=3s`, `history-ttl=24h` | Most Grafana + Drilldown workloads |
| Aggressive | `adaptive-max=12`, `latency-target=1s`, `latency-backoff=2s`, `history-ttl=72h` + larger disk cache | Large repeated long-range analytics where backend headroom is proven |

### Tuning Visibility Metrics

Use these metrics to tune safely:

- `loki_vl_proxy_window_adaptive_parallel_current` (gauge)
- `loki_vl_proxy_window_adaptive_latency_ewma_seconds` (gauge)
- `loki_vl_proxy_window_adaptive_error_ewma` (gauge)
- `loki_vl_proxy_window_cache_hit_total`, `loki_vl_proxy_window_cache_miss_total` (counters)
- `loki_vl_proxy_window_fetch_seconds` and `loki_vl_proxy_window_merge_seconds` (histograms)
- `loki_vl_proxy_window_prefilter_attempt_total`, `loki_vl_proxy_window_prefilter_error_total` (counters)
- `loki_vl_proxy_window_prefilter_kept_total`, `loki_vl_proxy_window_prefilter_skipped_total` (counters)
- `loki_vl_proxy_window_prefilter_duration_seconds` (histogram)

Recommended loop:

1. Increase `-query-range-adaptive-max-parallel` only if latency EWMA stays below target and error EWMA stays near zero during peak load.
2. Increase `-query-range-history-cache-ttl` only if disk utilization and cache hit ratio justify it.
3. Set `-disk-cache-max-bytes` before increasing TTL windows beyond 24h.

### Metadata vs Live Query Freshness

The proxy keeps faster-changing paths conservative and slower-changing metadata slightly warmer:

- `query` and `query_range` stay on short TTLs so new log lines remain visible quickly
- `labels`, `label_values`, `series`, `patterns`, `detected_fields`, and `detected_labels` can cache longer because they change more slowly and are more expensive to rebuild
- Drilldown now prefers native VictoriaLogs metadata (`field_names`, `field_values`, `streams`) where possible, which reduces the amount of raw log rescanning needed on cache misses


## Multitenancy

| Flag | Env | Default | Description |
|---|---|---|---|
| `-tenant-map` | `TENANT_MAP` | — | JSON string→int tenant mapping |
| `-tenant-limits-allow-publish` | `TENANT_LIMITS_ALLOW_PUBLISH` | built-in allowlist | Comma-separated limit fields exposed on `/config/tenant/v1/limits` and `/loki/api/v1/drilldown-limits` |
| `-tenant-default-limits` | `TENANT_DEFAULT_LIMITS` | — | JSON map of default published-limit overrides |
| `-tenant-limits` | `TENANT_LIMITS` | — | JSON map of per-tenant published-limit overrides keyed by `X-Scope-OrgID` |
| `-auth.enabled` | — | `false` | Require `X-Scope-OrgID` on query requests |
| `-tenant.allow-global` | — | `false` | Allow `X-Scope-OrgID: *` to bypass tenant scoping and use the backend default tenant |

### Tenant Resolution Order

1. **No header** — when `-auth.enabled=false`, requests without `X-Scope-OrgID` use VictoriaLogs' backend default tenant, which is `AccountID=0` and `ProjectID=0`
2. **Tenant map lookup** — if `-tenant-map` is configured and the org ID matches a key, use the mapped `AccountID`/`ProjectID`
3. **Explicit tenant map override** — if a tenant map contains an exact key such as `"0"` or `"fake"`, that explicit mapping wins
4. **Default-tenant aliases** — `X-Scope-OrgID` values `0`, `fake`, and `default` map to VictoriaLogs' built-in `0:0` tenant without rewriting headers
5. **Wildcard global bypass** — `X-Scope-OrgID: *` is a proxy-specific convenience. It uses the backend default tenant only when `-tenant.allow-global=true`
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
- fanout is safety-capped to prevent one request from exploding into an unbounded number of backend queries
- merged multi-tenant response bodies are also size-capped before they are returned to the client

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

`X-Scope-OrgID: "0"`, `X-Scope-OrgID: "fake"`, and `X-Scope-OrgID: "default"` resolve to VL's default `0:0` tenant in Loki-compatible single-tenant mode. `X-Scope-OrgID: "*"` remains a proxy-specific wildcard convenience and always requires explicit `-tenant.allow-global=true`.

### Hot Reload

Tenant mappings and field mappings can be reloaded without restart via SIGHUP:
```bash
kill -HUP $(pidof loki-vl-proxy)
```

### Published Tenant Limits Compatibility Surface

The proxy also exposes tenant-limit compatibility endpoints for Grafana and Logs Drilldown bootstrap:

- `GET /config/tenant/v1/limits` returns YAML
- `GET /loki/api/v1/drilldown-limits` returns JSON

The base payload is generated in-proxy and then filtered/overridden by:

- `-tenant-limits-allow-publish`
- `-tenant-default-limits`
- `-tenant-limits`

Operational notes:

- multi-tenant `X-Scope-OrgID: a|b` is rejected on `/config/tenant/v1/limits`
- `X-Scope-OrgID` omitted on these endpoints uses the default published limits surface
- these flags only shape the published compatibility payload; they do not enforce backend quotas

## OTLP Telemetry

| Flag | Env | Default | Description |
|---|---|---|---|
| `-otlp-endpoint` | `OTLP_ENDPOINT` | — | OTLP HTTP endpoint for proxy metrics |
| `-otlp-interval` | — | `30s` | Push interval |
| `-otlp-compression` | `OTLP_COMPRESSION` | `none` | `none`, `gzip`, `zstd` |
| `-otlp-headers` | `OTLP_HEADERS` | — | Comma-separated OTLP HTTP headers in `key=value` form |
| `-otlp-timeout` | — | `10s` | HTTP request timeout |
| `-otlp-tls-skip-verify` | — | `false` | Skip TLS verification |
| `-otel-service-name` | `OTEL_SERVICE_NAME` | `loki-vl-proxy` | `service.name` resource metadata for OTLP metrics/logs (not duplicated per JSON log line) |
| `-otel-service-namespace` | `OTEL_SERVICE_NAMESPACE` | — | `service.namespace` resource metadata for OTLP metrics/logs (not duplicated per JSON log line) |
| `-otel-service-instance-id` | `OTEL_SERVICE_INSTANCE_ID` | — | `service.instance.id` resource metadata for OTLP metrics/logs (not duplicated per JSON log line) |
| `-deployment-environment` | `DEPLOYMENT_ENVIRONMENT` | — | `deployment.environment.name` resource metadata for OTLP metrics/logs (not duplicated per JSON log line) |

## Go Runtime Tuning

| Flag | Env | Default | Description |
|---|---|---|---|
| `-go-mem-limit` | — | `0` | Explicit `GOMEMLIMIT` in bytes. Overrides `-go-mem-limit-percent` when set (`0` = disabled) |
| `-go-mem-limit-percent` | — | `85` | `GOMEMLIMIT` as a percentage of the detected container memory limit (from `/proc` cgroup). Ignored when `-go-mem-limit` is set |
| `-go-gc-percent` | — | `200` | `GOGC` target percentage. The proxy default (`200`) halves GC frequency versus Go's built-in default of `100`, trading higher peak RSS for lower GC CPU. Set `-1` to disable GC target entirely |

See [Performance — Go Runtime Tuning](performance.md#go-runtime-tuning) for guidance on sizing these for your deployment.

## HTTP Hardening

| Flag | Env | Default | Description |
|---|---|---|---|
| `-http-read-timeout` | — | `30s` | Server read timeout |
| `-http-read-header-timeout` | — | `10s` | Server read header timeout |
| `-http-write-timeout` | — | `120s` | Server write timeout |
| `-http-idle-timeout` | — | `120s` | Server idle timeout |
| `-http-max-header-bytes` | — | `1MB` | Maximum header size |
| `-http-max-body-bytes` | — | `10MB` | Maximum request body size |
| `-http-conn-max-age` | — | `10m` | Maximum lifetime for downstream HTTP/1.x keepalive connections |
| `-http-conn-max-age-jitter` | — | `2m` | Jitter applied to downstream connection age rotation |
| `-http-conn-max-requests` | — | `256` | Maximum requests per keepalive connection |
| `-http-conn-overload-max-age` | — | `90s` | Shorter connection lifetime during backpressure |

## Grafana Compatibility

| Flag | Env | Default | Description |
|---|---|---|---|
| `-max-lines` | — | `1000` | Default max lines per query |
| `-backend-timeout` | — | `120s` | Timeout for non-streaming VL backend requests |
| `-backend-min-version` | — | `v1.30.0` | Minimum VictoriaLogs version considered fully supported at startup compatibility gate |
| `-backend-allow-unsupported-version` | — | `false` | Allow startup when detected backend version is lower than `-backend-min-version` (unsafe override) |
| `-backend-version-check-timeout` | — | `5s` | Timeout for startup backend version compatibility check (`/health`) |
| `-stream-response` | — | `false` | Stream via chunked transfer |
| `-response-compression` | — | `auto` | Response compression codec: `auto`, `gzip`, `none` |
| `-response-compression-min-bytes` | — | `1024` | Minimum response size before frontend compression starts; smaller responses stay identity |
| `-response-gzip` | — | `true` | Deprecated compatibility switch; `false` disables response compression when `-response-compression` is unset |
| `-derived-fields` | — | — | JSON derived fields for trace linking |
| `-forward-headers` | — | — | HTTP headers to forward to VL |
| `-forward-authorization` | — | `false` | Forward client `Authorization` header to VL backend (adds `Authorization` to forwarded headers list) |
| `-forward-cookies` | — | — | Cookie names to forward to VL |
| `-backend-basic-auth` | — | — | `user:password` for VL basic auth |
| `-backend-compression` | — | `auto` | Upstream compression preference: `auto`, `gzip`, `zstd`, `none`. `auto` detects loopback backends (localhost/127.x/::1) and disables compression for co-located VL, otherwise advertises `zstd, gzip`. |
| `-backend-tls-skip-verify` | — | `false` | Skip TLS on VL connection |
| `-backend-read-buffer-size` | — | `65536` | Per-connection read buffer size (bytes) for VL HTTP responses. Default 64 KB — reduces syscall count 7× for typical responses vs Go default 4 KB. Decrease (e.g. `8192`) to save memory in high-pod-count environments; increase (e.g. `131072`) when responses routinely exceed 64 KB. |
| `-backend-write-buffer-size` | — | `65536` | Per-connection write buffer size (bytes) for VL HTTP requests. Default 64 KB. Tune alongside `-backend-read-buffer-size` for memory-constrained pods. |
| `-tail.allowed-origins` | — | — | Comma-separated WebSocket Origin allowlist for `/loki/api/v1/tail` |
| `-tail.mode` | — | `auto` | `auto`, `native`, or `synthetic` for `/tail` streaming mode |

`-tail.mode=auto` prefers native backend tailing and falls back to synthetic polling when native streaming is unavailable. `native` disables the fallback, and `synthetic` forces the polling bridge even when the backend can stream natively.

Compression notes:

- `-response-compression=auto` keeps the optimized gzip path enabled for clients that advertise `gzip`.
- `-response-compression-min-bytes=1024` keeps small control-plane responses uncompressed and the proxy applies higher effective thresholds on metadata-heavy routes.
- peer-cache `/_cache/get` fetches follow the same preference order: `zstd`, then `gzip`, then identity.
- `-backend-compression=auto` auto-detects the backend address at startup: when the host is a loopback address (`localhost`, `127.x.x.x`, `::1`) it sends `Accept-Encoding: identity`, eliminating 25–35% decompression CPU with no bandwidth cost (loopback has no network overhead); when the host is remote it advertises `zstd, gzip` and the proxy safely decodes either on the way back. Use `none` to force identity for non-loopback local deployments (e.g. same host via a non-loopback IP).
- current VictoriaLogs docs describe HTTP response compression in general terms, not a guaranteed `zstd` select-query path, so in practice many deployments will still observe `gzip` or identity from stock VictoriaLogs today.
- Grafana `12.4.2` datasource proxy requests advertised `Accept-Encoding: deflate, gzip`, not `zstd`, in local verification, so the proxy keeps the frontend surface on `gzip`/identity only.
- The Helm chart now pins `-response-compression=gzip` and `-response-compression-min-bytes=1024` by default.

Backend version gate notes:

- On startup, proxy probes backend `/health` and inspects response headers for a VictoriaLogs semver.
- If detected version is below `-backend-min-version`, startup is blocked by default.
- Set `-backend-allow-unsupported-version=true` to bypass the gate at your own risk.
- If version cannot be detected from headers, proxy logs a warning and continues startup.

## Built-In Protection Defaults

All protection controls are tunable via CLI flags:

| Flag | Env | Default | Description |
|---|---|---|---|
| `-max-concurrent` | — | `100` | Maximum concurrent backend queries allowed globally |
| `-rate-limit-per-second` | — | `50` | Per-client request rate limit (requests/second) |
| `-rate-limit-burst` | — | `100` | Per-client burst allowance above the rate limit |
| `-cb-fail-threshold` | — | `5` | Number of backend failures within the sliding window required to open the circuit breaker |
| `-cb-open-duration` | — | `10s` | How long the circuit breaker stays open before entering half-open state |
| `-cb-window-duration` | — | `30s` | Sliding window duration for failure counting; sporadic failures outside the window do not accumulate |
| `-coalescer-disabled` | — | `false` | Disable request coalescing (singleflight); every concurrent request makes its own backend call — useful with `-cache-disabled` to measure raw translation overhead |

Shape per-client and global traffic at Grafana, ingress, or an outer proxy layer for additional control beyond these flags.

## Observability and Admin Surfaces

| Flag | Env | Default | Description |
|---|---|---|---|
| `-server.register-instrumentation` | — | `true` | Register `/metrics` and related instrumentation handlers |
| `-server.enable-pprof` | — | `false` | Expose `/debug/pprof/*` |
| `-server.enable-query-analytics` | — | `false` | Expose `/debug/queries` |
| `-server.admin-auth-token` | — | — | Bearer token accepted on admin/debug endpoints |
| `-server.metrics-max-concurrency` | — | `1` | Maximum concurrent `/metrics` scrapes served at once (`0` disables the cap) |
| `-metrics.max-tenants` | — | `256` | Max unique tenant labels retained in `/metrics` before using `__overflow__` |
| `-metrics.max-clients` | — | `256` | Max unique client labels retained in `/metrics` before using `__overflow__` |
| `-metrics.export-sensitive-labels` | — | `false` | Export per-tenant and per-client identity metrics on `/metrics` and OTLP |
| `-metrics.trust-proxy-headers` | — | `false` | Trust user/proxy headers (`X-Grafana-User`, `X-Forwarded-User`, `X-Webauth-User`, `X-Auth-Request-User`, `X-Forwarded-*`) for client metrics/log attribution and backend context forwarding |

## Peer Cache

| Flag | Env | Default | Description |
|---|---|---|---|
| `-peer-self` | — | — | This instance address used for peer-cache ownership and fetches |
| `-peer-discovery` | — | — | Peer discovery mode: `dns` or `static` |
| `-peer-dns` | — | — | Headless service DNS name used when `-peer-discovery=dns` |
| `-peer-static` | — | — | Comma-separated peer list used when `-peer-discovery=static` |
| `-peer-timeout` | — | `2s` | Timeout applied to peer-cache owner fetches (`/_cache/get`) before falling back locally |
| `-peer-auth-token` | — | — | Shared token used on `/_cache/get` and `/_cache/set` peer-cache requests. Strongly recommended for fleets so peer auth does not depend only on transient discovery/IP membership during startup. |
| `-peer-write-through` | — | `true` | Push eligible non-owner cache writes to the owner peer (`/_cache/set`) to keep owner shards warm under skewed traffic |
| `-peer-write-through-min-ttl` | — | `30s` | Minimum TTL required to push a write-through copy to the owner peer |
| `-peer-hot-read-ahead-enabled` | — | `false` | Enable bounded periodic hot-read-ahead prefetch from peer hot indexes |
| `-peer-hot-read-ahead-interval` | — | `30s` | Base interval for hot-index pull cycles |
| `-peer-hot-read-ahead-jitter` | — | `5s` | Random jitter added to the read-ahead interval |
| `-peer-hot-read-ahead-top-n` | — | `256` | Number of hot keys requested per peer hot-index pull |
| `-peer-hot-read-ahead-max-keys-per-interval` | — | `64` | Max prefetched keys per read-ahead cycle |
| `-peer-hot-read-ahead-max-bytes-per-interval` | — | `8388608` | Max prefetched bytes per read-ahead cycle |
| `-peer-hot-read-ahead-max-concurrency` | — | `4` | Max concurrent hot-index and prefetch peer requests |
| `-peer-hot-read-ahead-min-ttl` | — | `30s` | Minimum remaining TTL required for a prefetch candidate |
| `-peer-hot-read-ahead-max-object-bytes` | — | `262144` | Max object size eligible for read-ahead prefetch |
| `-peer-hot-read-ahead-tenant-fair-share` | — | `50` | Per-tenant first-pass selection cap (% of key budget) |
| `-peer-hot-read-ahead-error-backoff` | — | `15s` | Base cooldown after read-ahead/index pull failures |

Peer-cache notes:

- the Helm chart manages `-peer-self`, `-peer-discovery`, and `-peer-dns` automatically when `peerCache.enabled=true`
- peer-cache fetches preserve owner TTL and can compress larger `/_cache/get` responses with `zstd` or `gzip`
- `loki_vl_proxy_peer_cache_error_reason_total{reason=...}` breaks opaque peer fetch failures into low-cardinality reasons like `timeout`, `transport`, `status_502`, `body_read`, and `decode`
- with `-peer-write-through=true` (default), non-owner writes with TTL above threshold are pushed to owners and stored locally as short-lived shadows to reduce hot-pod disk skew
- set `-peer-auth-token` fleet-wide whenever peer cache is enabled; it avoids transient startup or discovery-flap `403` responses caused by IP-membership-only peer auth
- when `-peer-auth-token` is set, all peers must share the same token or peer-cache reuse will fail closed

### Current Tuning For Higher Fleet Reuse

Use these knobs first:

- keep `-peer-write-through=true` (default) to warm owner shards under skewed traffic
- tune `-peer-write-through-min-ttl` so only stable/hot entries are replicated
- keep `-response-compression=gzip` for explicit Loki/Grafana-safe frontend behavior, or `auto` if you want the same gzip behavior through the legacy default
- keep `-response-compression-min-bytes` around `1-4KiB` to avoid wasting CPU on small metadata/control responses
- keep `-backend-compression=auto` for smart upstream negotiation: loopback backends get `identity` (no decompression overhead), remote backends get `zstd`/`gzip`
- keep `query-range-windowing` enabled with long history TTL and near-now freshness controls for mixed historical/live workloads

### Bounded Hot Read-Ahead

A bounded peer hot-read-ahead mode is implemented and can be enabled with:

```bash
-peer-hot-read-ahead-enabled=true
```

It keeps traffic bounded via key/byte/concurrency caps, jitter, tenant fairness, and error backoff.
See [Fleet Cache Architecture](fleet-cache.md#hot-read-ahead-bounded).

The owner hot-index endpoint (`/_cache/hot`) is internal peer-cache surface area and should not be exposed publicly.

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
| Forward `Authorization` specifically | `-forward-authorization=true` (or `-forward-headers=Authorization`) |
| Allowed cookies | `-forward-cookies` |
| Timeout | `-backend-timeout` for proxy→VL, Grafana datasource timeout for Grafana→proxy |
| Maximum lines | `-max-lines` |

When `-metrics.trust-proxy-headers=true`, the proxy forwards trusted user headers and proxy-chain headers to the backend, plus derived context headers `X-Loki-VL-Client-ID` / `X-Loki-VL-Client-Source`.

Datasource auth credentials are forwarded separately as `X-Loki-VL-Auth-User` / `X-Loki-VL-Auth-Source` and are not used as `enduser.id` client identity.

### Grafana User Header Forwarding (Recommended)

To attribute requests to real Grafana users instead of datasource service credentials:

1. Enable trusted proxy headers on Loki-VL-proxy:

```bash
-metrics.trust-proxy-headers=true
```

2. Configure Grafana server dataproxy to forward the logged-in user header:

```ini
[dataproxy]
send_user_header = true
```

3. Keep Loki datasource in proxy mode (`access: proxy`) and point it to Loki-VL-proxy:

```yaml
datasources:
  - name: Loki (VL Proxy)
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100
```

4. If Grafana sits behind an auth/reverse proxy, preserve user/proxy-chain headers end to end (`X-Forwarded-User`, `X-Grafana-User`, `X-Forwarded-*`, `Forwarded`).

Expected request-log behavior with this setup:

- `enduser.id` reflects the trusted Grafana/auth user.
- `enduser.source` indicates user-header source (for example `grafana_user` or `forwarded_user`).
- `auth.principal` reflects datasource auth identity when present (for example basic-auth datasource user).

Alerting datasource integration is still partial: the proxy supports legacy Loki YAML rules reads and Prometheus-style JSON rules/alerts reads against a configured backend, but it does not yet implement the full Loki ruler write API surface.