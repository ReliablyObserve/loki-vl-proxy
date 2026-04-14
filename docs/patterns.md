# Patterns

This document covers Loki-compatible patterns behavior in Loki-VL-proxy, including persistence and sizing guidance.

## Compatibility Goal

Loki-VL-proxy exposes `GET /loki/api/v1/patterns` for Grafana Explore/Drilldown compatibility.

- API route and payload shape follow Loki patterns expectations.
- Endpoint can be gated via `-patterns-enabled` for strict environment control.
- Proxy-side response cache keeps repeated Drilldown pattern loads fast.

## How Detection Works

Proxy behavior:

- The proxy fetches matching logs and runs Drain-like token clustering.
- Patterns are derived from live query results, not from a static dictionary.
- Optional global autodetect mode (`-patterns-autodetect-from-queries=true`) additionally warms pattern cache from successful `query` and `query_range` responses.
- Maximum patterns returned per request are clamped to `1000`.

Loki behavior reference:

- Loki also treats patterns as dynamic detection from ingested/queryable logs.
- There is no built-in static predefined pattern catalog shipped for all tenants.

References:

- Loki HTTP API patterns endpoint: <https://grafana.com/docs/loki/latest/reference/loki-http-api/#patterns-detection>
- Grafana Logs patterns UX: <https://grafana.com/docs/grafana/latest/visualizations/simplified-exploration/logs/patterns/>

## Persistence Model

When `-patterns-persist-path` is set:

- Every detected pattern response is tracked in an in-memory snapshot map.
- Snapshot is written to disk periodically (`-patterns-persist-interval`) and on graceful shutdown.
- On startup, proxy restores disk snapshot first, then merges peer snapshots (newest entry wins per cache key).
- Readiness remains `503` until startup warm completes.

Retention model:

- Pattern cache entries are kept with effectively long-lived TTL (`100y`) and updated in-place.
- Snapshot state is append/update oriented and does not run periodic pruning by age.

## Disk And Startup Flags

- `-patterns-enabled`
- `-patterns-autodetect-from-queries`
- `-patterns-persist-path`
- `-patterns-persist-interval`
- `-patterns-startup-stale-threshold`
- `-patterns-startup-peer-warm-timeout`

If persistence path is invalid/unwritable, startup fails fast with a clear error.

## Deployment Recommendation

For restart-safe pattern persistence:

- run as `StatefulSet`
- mount a PVC
- set `-patterns-persist-path` to a file on that volume

If persistence path is not set, patterns endpoint still works, but warm state is lost on restart.

## Sizing Guidance

Upper bound characteristics:

- response pattern cap: `1000` patterns per request
- each pattern carries:
  - `pattern` string
  - optional `level`
  - `samples` time/count buckets

Approximation:

- `payload_bytes ~= sum(pattern_entry_bytes)` for each cached `(tenant, rawQuery)` key
- rough pattern entry size:
  - `~120 bytes base + len(pattern) + ~24 bytes per sample bucket`

Total snapshot footprint depends on:

- number of unique cached pattern queries
- average pattern string length
- sample bucket density (`step`, query range)
